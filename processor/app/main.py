"""NIDH vaccination processor.

Reads the `vaccinations` topic and, for each batch of messages:
  1. Valid format?   no  -> rejects topic (+ rejects table) with a reason
  2. Clock skewed?   yes -> keep the record, set clock_skew_flag and skew_seconds
  3. Already stored? yes -> skip (ON CONFLICT DO NOTHING); same child+vaccine+dose under a
                            new record_id -> rejects as duplicate_dose
  4. Insert the batch in ONE transaction (retry on 40001 / lost node)
  5. Commit the Kafka offset ONLY after the database commit, so a crash replays the batch
     and step 3 makes the replay harmless (at-least-once + idempotent = effectively once).
A message that is not JSON, or a record that keeps failing in the database, goes to the DLQ.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

from .store import Reject, Store
from .validate import Invalid, check

BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
TOPIC = os.getenv("VACCINATION_TOPIC", "vaccinations")
REJECTS_TOPIC = os.getenv("REJECTS_TOPIC", "rejects")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq")
GROUP = os.getenv("CONSUMER_GROUP", "vax-processor")
DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
MAX_RECORDS = int(os.getenv("PROCESSOR_MAX_RECORDS", "500"))
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
NS = uuid.UUID("6f1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")   # namespace for stable reject IDs

log = logging.getLogger("processor")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s processor %(message)s")

INSERTED = Counter("processor_records_inserted_total", "Records stored")
RETRIES = Counter("processor_retries_skipped_total", "Re-sent records skipped (already stored)")
REJECTED = Counter("processor_records_rejected_total", "Records sent to rejects", ["reason"])
SKEWED = Counter("processor_records_skew_flagged_total", "Records kept with clock_skew_flag")
DLQ = Counter("processor_dlq_total", "Messages sent to the DLQ")
BATCH_TIME = Histogram("processor_batch_seconds", "Time to process one batch",
                       buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30))


def stable_id(msg, reason: str, record_id: uuid.UUID | None = None) -> uuid.UUID:
    """Same record + same reason -> same reject ID, so neither a tablet retry nor a replay
    after a crash can log the same bad record twice. Without a usable record_id, fall back
    to the message's position in the topic (still stable on replay)."""
    if record_id is not None:
        return uuid.uuid5(NS, f"{record_id}:{reason}")
    return uuid.uuid5(NS, f"{msg.topic}:{msg.partition}:{msg.offset}:{reason}")


async def connect_kafka():
    for attempt in range(1, 61):
        consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=BROKERS, group_id=GROUP,
                                    enable_auto_commit=False, auto_offset_reset="earliest")
        producer = AIOKafkaProducer(bootstrap_servers=BROKERS, acks="all", enable_idempotence=True)
        try:
            await consumer.start()
            await producer.start()
            log.info("connected to Redpanda as group %s", GROUP)
            return consumer, producer
        except Exception as exc:
            log.warning("Redpanda not ready (%s); retry %d/60", exc, attempt)
            await consumer.stop()
            await producer.stop()
            await asyncio.sleep(2)
    raise RuntimeError("could not connect to Redpanda")


async def process(messages, store: Store, producer: AIOKafkaProducer) -> None:
    started = time.perf_counter()
    rows, rejects, dlq, offset_ids = [], [], [], {}
    repeats_in_fetch = 0

    for m in messages:
        try:
            body = json.loads(m.value)
        except (ValueError, UnicodeDecodeError):
            dlq.append({"topic": m.topic, "partition": m.partition, "offset": m.offset,
                        "error": "not valid JSON", "raw": m.value.decode("utf-8", "replace")[:2000]})
            continue
        try:
            row = check(body, store.facilities)
        except Invalid as exc:
            rid = body.get("record_id")
            try:
                rid = uuid.UUID(str(rid))
            except ValueError:
                rid = None
            rejects.append(Reject(stable_id(m, "invalid_format", rid), rid, "invalid_format", str(exc), body))
            continue
        if row.record_id in offset_ids:       # same record twice in one fetch: keep one
            repeats_in_fetch += 1
            continue
        offset_ids[row.record_id] = stable_id(m, "duplicate_dose", row.record_id)
        rows.append(row)

    try:
        stored, retries, dup_rejects = await store.write(rows, rejects, offset_ids)
    except Exception as exc:
        # A record the database refuses for a non-retryable reason: isolate it.
        log.error("batch failed (%s); retrying records one by one", exc)
        await store.conn.rollback()
        stored, retries, dup_rejects = set(), 0, []
        await store.write([], rejects, offset_ids)
        for row in rows:
            for attempt in range(3):
                try:
                    i, r, d = await store.write([row], [], offset_ids)
                    stored, retries, dup_rejects = stored | i, retries + r, dup_rejects + d
                    break
                except Exception as row_exc:
                    await store.conn.rollback()
                    if attempt == 2:
                        dlq.append({"record_id": str(row.record_id), "error": str(row_exc)})

    # publish rejects and DLQ entries BEFORE committing the offset (nothing is lost on a crash)
    sends = [await producer.send(REJECTS_TOPIC, json.dumps(j.as_message(), default=str).encode())
             for j in rejects + dup_rejects]
    sends += [await producer.send(DLQ_TOPIC, json.dumps(d, default=str).encode()) for d in dlq]
    if sends:
        await asyncio.gather(*sends)

    retries += repeats_in_fetch
    inserted = len(stored)
    skew_new = sum(1 for r in rows if r.clock_skew_flag and r.record_id in stored)
    INSERTED.inc(inserted)
    RETRIES.inc(retries)
    SKEWED.inc(skew_new)
    for j in rejects:
        REJECTED.labels(reason="invalid_format").inc()
    REJECTED.labels(reason="duplicate_dose").inc(len(dup_rejects))
    DLQ.inc(len(dlq))
    elapsed = time.perf_counter() - started
    BATCH_TIME.observe(elapsed)
    log.info("%d messages: %d inserted, %d already stored (skipped), %d stored with skew flag, "
             "%d invalid, %d duplicate doses, %d to DLQ (%.0f ms)",
             len(messages), inserted, retries, skew_new,
             len(rejects), len(dup_rejects), len(dlq), elapsed * 1000)


async def main() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    store = Store(DSN)
    await store.connect()
    consumer, producer = await connect_kafka()
    log.info("%s ready: reading %s", INSTANCE, TOPIC)
    try:
        while True:
            fetched = await consumer.getmany(timeout_ms=1000, max_records=MAX_RECORDS)
            messages = [m for batch in fetched.values() for m in batch]
            if not messages:
                continue
            await process(messages, store, producer)
            # The offset moves forward only now, after the database has committed.
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
