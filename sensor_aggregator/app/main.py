"""NIDH sensor aggregator.

Consumer side (two copies, one consumer group, readings keyed by sensor_id so each sensor
belongs to one copy at a time):
  1. Check each message (known facility, sane temperature, timestamps). Bad -> dlq.
  2. Alert rules per sensor: outside 2-8 C -> too_warm / too_cold; back inside -> back_in_range;
     Last Will -> offline; a reading from an offline sensor -> back_online.
  3. 1-minute windows (min / avg / max / count) -> sensor_readings_1min.
  4. State, alerts and finished windows are saved in ONE transaction, and only then is the
     offset committed (the oldest message still in an open window, so a crash rebuilds it).

Watcher side (only the copy holding the 'sensor_watch' lease), every WATCH_INTERVAL_S:
  5. Silence check: a sensor with no reading for 5 minutes -> offline alert.
  6. Outbox: alerts not yet published are sent to the `alerts` topic (for SMS, next step),
     then marked published. A crash in between only means an alert is sent twice, and the
     SMS service skips repeats by alert_id. No alert can be lost.
  7. Gauges for the dashboard: sensors online / offline / too warm / too cold, last temperatures.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections import defaultdict
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .rules import Invalid, Reading, Windows, check
from .store import Store

BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
TOPIC = os.getenv("SENSOR_TOPIC", "sensor-readings")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq")
DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
SILENCE_S = int(os.getenv("SILENCE_LIMIT_S", "300"))
WATCH_INTERVAL_S = float(os.getenv("WATCH_INTERVAL_S", "15"))
LEASE_TTL_S = int(os.getenv("LEASE_TTL_S", "30"))

log = logging.getLogger("sensor_aggregator")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s sensor-agg %(message)s")

READINGS = Counter("sensor_readings_total", "Readings processed")
INVALID = Counter("sensor_invalid_total", "Sensor messages sent to the DLQ")
ALERTS = Counter("sensor_alerts_total", "Alerts raised", ["kind"])
WINDOWS = Counter("sensor_windows_written_total", "1-minute windows saved")
PUBLISHED = Counter("sensor_alerts_published_total", "Alerts sent to the alerts topic")
BATCH = Histogram("sensor_batch_seconds", "Time to process one fetch",
                  buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
LAG = Gauge("sensor_lag_messages", "Messages waiting per partition", ["partition"])
IS_LEADER = Gauge("sensor_watch_is_leader", "1 if this copy runs the silence check")
SENSORS = Gauge("sensors_by_state", "Sensors by status and temperature band",
                ["status", "temp_state"])
LAST_TEMP = Gauge("sensor_last_temp_celsius", "Last temperature reported", ["sensor"])


class Rebalance(ConsumerRebalanceListener):
    def __init__(self, windows: Windows):
        self.windows = windows

    async def on_partitions_revoked(self, revoked):
        self.windows.drop_partitions({tp.partition for tp in revoked})

    async def on_partitions_assigned(self, assigned):
        log.info("%s now owns partitions %s", INSTANCE, sorted(tp.partition for tp in assigned))


async def kafka():
    while True:
        consumer = AIOKafkaConsumer(bootstrap_servers=BROKERS, group_id="sensor-agg",
                                    enable_auto_commit=False, auto_offset_reset="earliest")
        producer = AIOKafkaProducer(bootstrap_servers=BROKERS, acks="all",
                                    enable_idempotence=True)
        try:
            await consumer.start()
            await producer.start()
            return consumer, producer
        except Exception as exc:
            log.warning("Redpanda not ready (%s); retrying in 3 s", exc)
            await consumer.stop()
            await producer.stop()
            await asyncio.sleep(3)


async def consume(store: Store, consumer, producer) -> None:
    windows = Windows()
    consumer.subscribe([TOPIC], listener=Rebalance(windows))
    while True:
        fetched = await consumer.getmany(timeout_ms=1000, max_records=1000)
        started, now = time.perf_counter(), datetime.now(timezone.utc)
        events, processed, dlq = defaultdict(list), {}, []
        for tp, messages in fetched.items():
            for m in messages:
                processed[tp.partition] = m.offset
                try:
                    ev = check(json.loads(m.value), store.facilities)
                except (Invalid, ValueError, TypeError) as exc:
                    dlq.append({"source": "sensor_aggregator", "error": str(exc),
                                "raw": m.value.decode("utf-8", "replace")[:500]})
                    continue
                events[ev.sensor_id].append(ev)
                if isinstance(ev, Reading):
                    windows.add(ev, tp.partition, m.offset, now)
                    READINGS.inc()

        closed = windows.closed(now)
        if events or closed:
            alerts = await store.process(dict(events), [w.row() for w in closed])
            for a in alerts:
                ALERTS.labels(kind=a.kind).inc()
                log.info("ALERT %s %s (%s): %s", a.kind, a.sensor_id, a.facility_id, a.detail)
            WINDOWS.inc(len(closed))
        if dlq:
            await asyncio.gather(*[await producer.send(DLQ_TOPIC, json.dumps(d).encode())
                                   for d in dlq])
            INVALID.inc(len(dlq))
        if processed:
            # Only now, after the database commit, does the offset move forward.
            try:
                await consumer.commit({TopicPartition(TOPIC, p): off
                                       for p, off in windows.commit_offsets(processed).items()})
            except CommitFailedError as exc:      # a rebalance took the partition away;
                log.warning("commit skipped (%s); the new owner replays", exc)   # harmless
            BATCH.observe(time.perf_counter() - started)
        for tp in consumer.assignment():
            high = consumer.highwater(tp)
            if high is not None:
                LAG.labels(partition=str(tp.partition)).set(max(0, high - await consumer.position(tp)))


async def watch(store: Store, producer) -> None:
    leader = False
    while True:
        started = time.perf_counter()
        try:
            now_leader = await store.take_lease("sensor_watch", INSTANCE, LEASE_TTL_S)
            if now_leader != leader:
                log.info("%s %s the sensor_watch lease", INSTANCE,
                         "took" if now_leader else "does not hold")
            leader = now_leader
            IS_LEADER.set(1 if leader else 0)
            if leader:
                for a in await store.mark_silent(SILENCE_S):
                    ALERTS.labels(kind=a.kind).inc()
                    log.info("ALERT offline %s (%s): %s", a.sensor_id, a.facility_id, a.detail)
                pending = await store.unpublished()
                if pending:
                    sends = [await producer.send(ALERTS_TOPIC, key=r[1].encode(), value=json.dumps({
                        "alert_id": str(r[0]), "sensor_id": r[1], "facility_id": r[2],
                        "kind": r[3], "detail": r[4],
                        "temp_c": float(r[5]) if r[5] is not None else None,
                        "at": r[6].isoformat()}).encode()) for r in pending]
                    await asyncio.gather(*sends)
                    await store.mark_published([r[0] for r in pending])
                    PUBLISHED.inc(len(pending))
                counts, temps = await store.summary()
                SENSORS.clear()
                for status, temp_state, n in counts:
                    SENSORS.labels(status=status, temp_state=temp_state).set(n)
                for sensor, temp in temps:
                    LAST_TEMP.labels(sensor=sensor).set(float(temp))
        except Exception as exc:
            log.exception("watch run failed: %s", exc)
        await asyncio.sleep(max(0.0, WATCH_INTERVAL_S - (time.perf_counter() - started)))


async def main() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    consumer_store, watch_store = Store(DSN), Store(DSN)      # one connection each
    await consumer_store.connect()
    await watch_store.connect()
    consumer, producer = await kafka()
    log.info("%s ready: reading %s", INSTANCE, TOPIC)
    try:
        await asyncio.gather(consume(consumer_store, consumer, producer),
                             watch(watch_store, producer))
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
