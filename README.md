# NIDH prototype: National Immunisation Data Hub (DSC3219)

A scaled-down, fault-tolerant data pipeline for vaccination records and fridge sensors,
built for the DSC3219 Cloud and Distributed Computing project.

- **Test machine:** HP EliteBook 840 G6, Intel i5-8365U (4 cores / 8 threads), 8 GB RAM
  (Docker/WSL capped at 4.8 GB), Windows 11 Pro with Docker Desktop (WSL 2).
- **Scale divisor:** k = 20 (Annex A standard is a 16 GB machine; this one has 8 GB).
- **Synthetic data only** (Annex B): child IDs start with `SYN-`, no real people.

## Single start command

```
docker compose up -d --build
```

First run downloads images and builds the API, so it takes a few minutes.
Stop with `docker compose down` (keeps data) or `docker compose down -v` (wipes data).

## What is built so far (steps 1 to 3 of Milestone 3)

| Service | Containers | Zone | Purpose |
| --- | --- | --- | --- |
| Redpanda | `redpanda-0`, `-1`, `-2` | zone-a, b, c | The stream: 3 brokers, every topic replicated 3 times (Raft) |
| Topic setup | `topics-init` (runs once, then exits) | edge | Creates `vaccinations` (6 partitions), `sensor-readings`, `rejects`, `dlq` |
| Ingestion API | `ingest-api-1`, `-2` | zone-a, b | FastAPI batch endpoint `POST /v1/batches`, up to 500 records |
| Load balancer | `nginx` (port 8080) | edge | Spreads uploads over both APIs; per-device rate limit (429) |
| Redpanda Console | `console` (port 8088) | edge | Web page to watch messages arrive |
| CockroachDB | `roach-0`, `-1`, `-2` | zone-a, b, c | The database: 3 nodes, every range copied 3 times, majority (Raft) commits. Admin page http://localhost:8089 |
| Database setup | `db-init` (runs once, then exits) | edge | Joins the nodes into one cluster, loads `db/schema.sql` and `db/seed.sql` (3,200 synthetic facilities, 2 users) |
| Processor | `processor-1`, `-2` | zone-a, b | Reads `vaccinations`; validates, flags clock skew, skips duplicates, stores in one transaction, then commits the offset |
| Cache | `cache` (Valkey) | zone-c | Ready-made dashboard answers, kept 45 s, refreshed every 15 s. Memory only |
| Aggregator | `aggregator-1`, `-2` | zone-a, b | Every 15 s counts doses per district, vaccine, dose and week into `district_coverage` and the cache. Only the lease holder writes |
| Read API | `read-api-1`, `-2` | zone-a, b | `GET /v1/coverage/...` behind Nginx; two roles; every read written to `audit_log` |
| Prometheus | `prometheus` (port 9090) | edge | Collects the metrics of every service, Redpanda and CockroachDB every 10 s |
| Grafana | `grafana` (port 3000) | edge | The dashboard "NIDH pipeline": http://localhost:3000 (view without login) |

### How the ingestion API behaves

1. Checks the batch: 1 to 500 records, each with a UUID `record_id` (the idempotency key).
2. Rate limits each device; when busy it answers **429** with a `Retry-After` header.
3. Sends every record to the `vaccinations` topic, keyed by `record_id`, adding the server's
   `received_at` time (the tablet's `device_ts` is kept unchanged).
4. Replies **200 only after Redpanda confirms the whole batch with `acks=all`**, meaning a
   majority (2 of 3 brokers) has written it. This is the acknowledgement point.
5. If Redpanda cannot confirm, it replies **503**, so the tablet keeps the batch and resends.
   Resending is safe because duplicates are skipped later by `record_id`.

### How the processor behaves (the checks in the data-flow diagram)

1. **Valid format?** Wrong IDs, unknown vaccine, dose outside 1-5, unknown facility, or a
   vaccination dated before the child's birth: sent to the `rejects` topic and `rejects` table
   with the exact reason. Messages that are not JSON go to the `dlq` topic.
2. **Clock skewed?** The tablet sends its own clock (`device_clock`) with each batch. If it differs
   from the server's `received_at` by more than 5 minutes, the record is **kept** with
   `clock_skew_flag = true` and `skew_seconds`. A wrong clock is not wrong data.
3. **Already stored?** `INSERT ... ON CONFLICT DO NOTHING`. A re-sent record (same `record_id`)
   is skipped. A new `record_id` for the same child, vaccine and dose is sent to rejects as
   `duplicate_dose`.
4. The whole batch is written in **one transaction**, retried on serialization conflicts (40001)
   or if a database node dies (the connection string lists all three nodes).
5. The Kafka offset is committed **only after** the database commit. A crash replays the batch,
   and step 3 makes the replay harmless.

## Try it (Windows PowerShell or Command Prompt, from the repo folder)

```
copy .env.example .env
docker compose up -d --build
docker compose ps
python scripts/send_test_batch.py
```

Expected: two lines starting with `200 {'status': 'acknowledged', ...}`. The second is the
same batch sent again, as a tablet retry would.

**See the records in Redpanda:** start the optional Console with `docker compose --profile tools up -d console`,
open http://localhost:8088 and choose Topics → vaccinations, or run

```
docker exec -it redpanda-0 rpk topic consume vaccinations -n 5
```

**Check the API copies are both working:** `docker compose logs ingest-api-1 ingest-api-2`
shows which instance acknowledged each batch.

**Fault F6 (restart an API during uploads):**

```
docker stop ingest-api-1
python scripts/send_test_batch.py
docker start ingest-api-1
```

Uploads still get 200, because Nginx sends them to `ingest-api-2`.

**Check the database (step 2):**

```
docker compose logs processor-1 processor-2
docker exec -it roach-0 ./cockroach sql --insecure -d nidh -e "SELECT count(*) FROM vaccination_records;"
docker exec -it roach-0 ./cockroach sql --insecure -d nidh -e "SELECT reason, detail FROM rejects;"
docker exec -it roach-0 ./cockroach sql --insecure -d nidh -e "SELECT child_id, clock_skew_flag, skew_seconds FROM vaccination_records WHERE clock_skew_flag;"
```

Each run of `send_test_batch.py` sends its batch twice, so the record count should grow by
the batch size only once. Try `--skew-minutes 120` (records kept and flagged) and `--bad`
(three records rejected: dose 7, vaccinated before birth, duplicate dose).

**CockroachDB licence:** version 24.3+ throttles a multi-node cluster 7 days after it is first
started unless a licence key is set. Request the free Enterprise Free licence (students qualify)
and put it in `.env` as `COCKROACH_LICENSE=...`. `docker compose down -v` starts a new cluster.

### How the read path behaves (step 3)

1. **Aggregator.** Every 15 s, the copy holding the lease in `aggregator_lease` counts doses
   per district, vaccine, dose and ISO week (Uganda time; the measured clock skew is removed
   from flagged records first) and UPSERTs the counts into `district_coverage` in one
   transaction. It then writes one JSON answer per district plus a national summary into the
   cache, each expiring after 45 s. If the lease holder dies, the other copy takes over
   within 30 s. A full recount is used because it can never drift; at national scale it
   would be replaced by an incremental update from a CockroachDB changefeed.
2. **Read API.** The caller is identified by the `X-User` header (a prototype stand-in for
   government single sign-on). `officer_d001` (district officer) may read only D001;
   `analyst_national` may read every district and the national summary.
3. **Audit first.** Every request by a known user, including refused ones, is written to
   `audit_log` *before* data is returned. If the audit row cannot be written, the API returns
   503 and no data.
4. **Cache first.** Answers come from the cache (`"source": "cache"`). On a miss, or if the
   cache is down, the API reads `district_coverage` (`"source": "database"`) and refills
   the cache. The national summary holds totals only, never child-level data.

**Try the read path:**

```
python scripts/check_read_path.py
curl -H "X-User: officer_d001" http://localhost:8080/v1/coverage/districts/D001
curl -H "X-User: analyst_national" "http://localhost:8080/v1/coverage/national?week=2026-W47"
docker exec -it roach-0 ./cockroach sql --insecure -d nidh -e "SELECT u.username, a.action, a.resource, a.at FROM audit_log a JOIN users u USING (user_id) ORDER BY a.at DESC LIMIT 10;"
docker exec -it roach-0 ./cockroach sql --insecure -d nidh -e "SELECT * FROM aggregator_lease;"
```

(In PowerShell, use `curl.exe` instead of `curl`.)

**Dashboard:** open http://localhost:3000. Panels: services up, live database nodes,
unavailable ranges, newest record in the dashboard, batches and records per second,
acknowledgement time (p95), backlog, rejects and DLQ, transaction retries, read requests,
cache hit ratio, aggregator lease holder, and national doses by vaccine.
Prometheus itself is at http://localhost:9090 (Status → Targets shows every service).

**Lease failover:** `docker stop aggregator-1` (if it holds the lease), wait 30 s, and
`aggregator-2` logs "took the lease". Start it again with `docker start aggregator-1`.

## Running the tests (no Docker needed)

```
cd ingest_api
pip install -r requirements-dev.txt
pytest -q
cd ../processor
pip install -r requirements-dev.txt
pytest -q
cd ../aggregator
pip install -r requirements-dev.txt
pytest -q
cd ../read_api
pip install -r requirements-dev.txt
pytest -q
```

The tests replace Redpanda with a fake producer and check: acknowledgement only after the
stream confirms, 503 when it cannot, record_id and batch-size checks, the 429 rate limit,
and that records with bad content are forwarded rather than dropped.

## Memory budget so far

Docker on the test laptop has 4.8 GB, so every container has a memory cap. Total caps:
3 x 448 MB Redpanda (320 MB each for Redpanda itself), 3 x 512 MB CockroachDB (64 MiB cache,
96 MiB SQL memory), 2 x 160 MB API, 2 x 160 MB processor, 64 MB Nginx: about 3.6 GB.
Step 3 adds 48 MB cache, 2 x 96 MB aggregator, 2 x 128 MB read API, 192 MB Prometheus and
160 MB Grafana: about 4.4 GB of caps in all. Caps are ceilings, not real use.
The first version (about 5 GB of caps) froze the laptop, so the caps were lowered and the
Console made optional.
Check real use with `docker stats --no-stream`.

**Database CPU:** by default CockroachDB stores its own internal metrics in its own tables.
On the test laptop this kept every node at 100-136% CPU with no traffic. `db-init` turns it
off (`timeseries.storage.enabled = false`), which brought idle CPU down to 8-11% per node.
Prometheus still collects the same metrics, but CockroachDB's own admin page graphs are empty.

## Repository layout (target)

```
ingest_api/        FastAPI batch endpoint            (built)
processor/         validate, skew flag, dedup, insert (built)
db/                schema.sql + seed.sql (built)
aggregator/        district_coverage + cache, with lease (built)
read_api/          roles, audit log, cache first (built)
sensor_bridge/  sensor_aggregator/  sms_mock/  generator/
infra/             nginx, prometheus, grafana (built), mosquitto
loadtest/  faults/  reconcile/  results/  docs/
```
