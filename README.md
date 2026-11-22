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

## What is built so far (step 1 of Milestone 3)

| Service | Containers | Zone | Purpose |
| --- | --- | --- | --- |
| Redpanda | `redpanda-0`, `-1`, `-2` | zone-a, b, c | The stream: 3 brokers, every topic replicated 3 times (Raft) |
| Topic setup | `topics-init` (runs once, then exits) | edge | Creates `vaccinations` (6 partitions), `sensor-readings`, `rejects`, `dlq` |
| Ingestion API | `ingest-api-1`, `-2` | zone-a, b | FastAPI batch endpoint `POST /v1/batches`, up to 500 records |
| Load balancer | `nginx` (port 8080) | edge | Spreads uploads over both APIs; per-device rate limit (429) |
| Redpanda Console | `console` (port 8088) | edge | Web page to watch messages arrive |

### How the ingestion API behaves

1. Checks the batch: 1 to 500 records, each with a UUID `record_id` (the idempotency key).
2. Rate limits each device; when busy it answers **429** with a `Retry-After` header.
3. Sends every record to the `vaccinations` topic, keyed by `record_id`, adding the server's
   `received_at` time (the tablet's `device_ts` is kept unchanged).
4. Replies **200 only after Redpanda confirms the whole batch with `acks=all`**, meaning a
   majority (2 of 3 brokers) has written it. This is the acknowledgement point.
5. If Redpanda cannot confirm, it replies **503**, so the tablet keeps the batch and resends.
   Resending is safe because duplicates are skipped later by `record_id`.

Full content checks (impossible dates, dose numbers, clock skew, duplicate doses) are done
by the processor in the next step, which sends bad records to the `rejects` topic.

## Try it (Windows PowerShell or Command Prompt, from the repo folder)

```
copy .env.example .env
docker compose up -d --build
docker compose ps
python scripts/send_test_batch.py
```

Expected: two lines starting with `200 {'status': 'acknowledged', ...}`. The second is the
same batch sent again, as a tablet retry would.

**See the records in Redpanda:** open http://localhost:8088, choose Topics → vaccinations, or run

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

**Metrics:** each API exposes Prometheus metrics at `/metrics` (batches acknowledged,
429s, 503s, acknowledgement time). Prometheus and Grafana are added in a later step.

## Running the API tests (no Docker needed)

```
cd ingest_api
pip install -r requirements-dev.txt
pytest -q
```

The tests replace Redpanda with a fake producer and check: acknowledgement only after the
stream confirms, 503 when it cannot, record_id and batch-size checks, the 429 rate limit,
and that records with bad content are forwarded rather than dropped.

## Memory budget so far

About 2.8 GB of container limits (3 × 640 MB Redpanda, 2 × 256 MB API, 256 MB Console,
64 MB Nginx). Check real use with `docker stats`. CockroachDB is added next.

## Repository layout (target)

```
ingest_api/        FastAPI batch endpoint            (built)
processor/         validate, skew flag, dedup, insert (next)
db/schema.sql      tables from the data model (M1-d) (next)
sensor_bridge/  sensor_aggregator/  aggregator/  read_api/  sms_mock/  generator/
infra/             nginx (built), mosquitto, prometheus, grafana configs
loadtest/  faults/  reconcile/  results/  docs/
```
