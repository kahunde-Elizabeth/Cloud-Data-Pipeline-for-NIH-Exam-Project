"""NIDH coverage aggregator.

Every AGG_INTERVAL_S seconds:
  1. Try to take (or renew) the lease in `aggregator_lease`. Only the lease holder writes,
     so two copies can run (one per zone) without double-writing. If the holder dies, its
     lease expires after LEASE_TTL_S and the other copy takes over.
  2. Count doses per district, vaccine, dose and ISO week from `vaccination_records`.
     The week is taken from the event time in Uganda time; for a record whose tablet clock
     was wrong (clock_skew_flag) the measured skew is removed first.
  3. Write the counts into `district_coverage` (UPSERT, so a re-run changes nothing).
  4. Push ready-made JSON for every district, plus a national summary, into the cache
     (Valkey/Redis) with a TTL, so the read API can answer without touching the database.

A full recount is simple and always correct (it cannot drift). It reads every record,
which is fine at prototype scale; at national scale this would be replaced by an
incremental update from a CockroachDB changefeed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from datetime import datetime, timezone

import psycopg
import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .payloads import build_payloads, by_iso_week

DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
CACHE_URL = os.getenv("CACHE_URL", "redis://cache:6379/0")
INTERVAL_S = float(os.getenv("AGG_INTERVAL_S", "15"))
LEASE_TTL_S = int(os.getenv("LEASE_TTL_S", "30"))
CACHE_TTL_S = int(os.getenv("CACHE_TTL_S", "45"))
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
LEASE_NAME = "district_coverage"

log = logging.getLogger("aggregator")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s aggregator %(message)s")

RUNS = Counter("aggregator_runs_total", "Aggregation runs", ["outcome"])
IS_LEADER = Gauge("aggregator_is_leader", "1 if this copy holds the lease")
RUN_TIME = Histogram("aggregator_run_seconds", "Time for one aggregation run",
                     buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10))
LAST_OK = Gauge("aggregator_last_success_timestamp_seconds", "When the last run finished")
NEWEST_AGE = Gauge("aggregator_newest_record_age_seconds",
                   "Age of the newest record included in the last run (data freshness)")
NATIONAL = Gauge("coverage_national_doses", "National doses given (all weeks)", ["vaccine", "dose"])
CACHE_ERRORS = Counter("aggregator_cache_errors_total", "Failed cache refreshes")

# Event time, corrected for a known tablet clock error, as a calendar day in Uganda time
# (UTC+3, no daylight saving). The ISO week is worked out from the day in Python.
EVENT_TS = ("CASE WHEN clock_skew_flag AND skew_seconds IS NOT NULL "
            "THEN device_ts - skew_seconds * INTERVAL '1 second' ELSE device_ts END")
EVENT_DAY = f"(({EVENT_TS}) AT TIME ZONE 'UTC' + INTERVAL '3 hours')::DATE"

COUNT_SQL = f"""
SELECT district_id, vaccine_code, dose_no, {EVENT_DAY} AS event_day, count(*) AS doses
FROM vaccination_records AS OF SYSTEM TIME '-1s'
GROUP BY 1, 2, 3, 4
"""
NEWEST_SQL = "SELECT max(received_at) FROM vaccination_records AS OF SYSTEM TIME '-1s'"

LEASE_SQL = """
INSERT INTO aggregator_lease (lease_name, holder, expires_at)
VALUES (%(name)s, %(me)s, now() + %(ttl)s::INT * INTERVAL '1 second')
ON CONFLICT (lease_name) DO UPDATE
    SET holder = excluded.holder, expires_at = excluded.expires_at
    WHERE aggregator_lease.holder = excluded.holder OR aggregator_lease.expires_at < now()
RETURNING holder
"""

UPSERT_SQL = """
UPSERT INTO district_coverage (district_id, vaccine_code, dose_no, iso_week, doses, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
"""


async def connect_db() -> psycopg.AsyncConnection:
    while True:
        try:
            conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True, connect_timeout=5)
            log.info("%s connected to the database", INSTANCE)
            return conn
        except psycopg.Error as exc:
            log.warning("database not ready (%s); retrying in 3 s", exc)
            await asyncio.sleep(3)


async def take_lease(conn) -> bool:
    cur = await conn.execute(LEASE_SQL, {"name": LEASE_NAME, "me": INSTANCE, "ttl": LEASE_TTL_S})
    return (await cur.fetchone()) is not None


async def run_once(conn, cache) -> int:
    rows = by_iso_week(await (await conn.execute(COUNT_SQL)).fetchall())
    newest = (await (await conn.execute(NEWEST_SQL)).fetchone())[0]

    # One transaction for the whole table, so readers never see half a refresh.
    async with conn.transaction():
        async with conn.cursor() as cur:
            if rows:
                await cur.executemany(UPSERT_SQL, rows)

    now = datetime.now(timezone.utc)
    districts, national, by_vaccine = build_payloads(rows, now)
    for (vaccine, dose), doses in by_vaccine.items():
        NATIONAL.labels(vaccine=vaccine, dose=str(dose)).set(doses)
    if newest is not None:
        NEWEST_AGE.set(max(0.0, (now - newest).total_seconds()))

    try:
        pipe = cache.pipeline(transaction=False)
        for district_id, payload in districts.items():
            pipe.set(f"cov:district:{district_id}", json.dumps(payload), ex=CACHE_TTL_S)
        pipe.set("cov:national", json.dumps(national), ex=CACHE_TTL_S)
        await pipe.execute()
    except Exception as exc:          # the cache is a speed-up, never the source of truth
        CACHE_ERRORS.inc()
        log.warning("cache refresh failed (%s); read API will use the database", exc)
    return len(rows)


async def main() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    cache = aioredis.from_url(CACHE_URL, socket_timeout=2, socket_connect_timeout=2)
    conn = await connect_db()
    leader = False
    while True:
        started = time.perf_counter()
        try:
            now_leader = await take_lease(conn)
            if now_leader != leader:
                log.info("%s %s the lease", INSTANCE, "took" if now_leader else "does not hold")
            leader = now_leader
            IS_LEADER.set(1 if leader else 0)
            if leader:
                groups = await run_once(conn, cache)
                elapsed = time.perf_counter() - started
                RUN_TIME.observe(elapsed)
                LAST_OK.set(time.time())
                RUNS.labels(outcome="ok").inc()
                log.info("refreshed %d coverage rows (%.0f ms)", groups, elapsed * 1000)
            else:
                RUNS.labels(outcome="standby").inc()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            RUNS.labels(outcome="error").inc()
            leader = False
            IS_LEADER.set(0)
            log.warning("lost the database (%s); reconnecting", exc)
            try:
                await conn.close()
            except Exception:
                pass
            conn = await connect_db()
        except Exception as exc:
            RUNS.labels(outcome="error").inc()
            log.exception("run failed: %s", exc)
        await asyncio.sleep(max(0.0, INTERVAL_S - (time.perf_counter() - started)))


if __name__ == "__main__":
    asyncio.run(main())
