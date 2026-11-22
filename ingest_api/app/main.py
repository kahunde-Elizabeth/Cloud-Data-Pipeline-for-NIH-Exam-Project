"""NIDH ingestion API.

The front door for vaccination records. Tablets (and the SMS adapter) upload
batches of up to 500 records in one request. The API:

  1. checks the batch envelope and that every record has a valid record_id,
  2. applies a per-device rate limit and an in-flight limit (429 + Retry-After),
  3. sends every record to the Redpanda topic `vaccinations`, keyed by record_id,
  4. replies 200 ONLY after Redpanda confirms the whole batch with acks=all
     (a Raft majority, 2 of 3 brokers, has written it). This is the
     acknowledgement point from Milestone 1 (f).

Full validation (dates, dose numbers, clock skew, duplicates) happens later in
the processor, so a record with bad content is never lost here: it is
forwarded and the processor routes it to the `rejects` topic with a reason.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app
from pydantic import BaseModel, Field, field_validator

# --------------------------------------------------------------------------- settings
BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
TOPIC = os.getenv("VACCINATION_TOPIC", "vaccinations")
MAX_BATCH = int(os.getenv("MAX_BATCH_RECORDS", "500"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT_BATCHES", "32"))      # per API instance
DEVICE_RATE_PER_S = float(os.getenv("DEVICE_RATE_PER_S", "0.5"))  # refill rate per device
DEVICE_BURST = int(os.getenv("DEVICE_BURST", "5"))                # bucket size per device
RETRY_AFTER_S = int(os.getenv("RETRY_AFTER_S", "30"))
PRODUCE_TIMEOUT_S = float(os.getenv("PRODUCE_TIMEOUT_S", "10"))
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())

log = logging.getLogger("ingest_api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s ingest_api %(message)s")

# --------------------------------------------------------------------------- metrics
BATCHES = Counter("ingest_batches_total", "Batches received", ["outcome"])
RECORDS = Counter("ingest_records_acked_total", "Records acknowledged (acks=all)")
LATENCY = Histogram("ingest_batch_seconds", "Time from request to acknowledgement",
                    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10))
INFLIGHT = Gauge("ingest_inflight_batches", "Batches currently being acknowledged")


# --------------------------------------------------------------------------- request model
class Batch(BaseModel):
    """What a tablet sends. Records are kept as raw dicts so that nothing with
    odd content is dropped here; the processor does full validation."""
    device_id: str = Field(min_length=1, max_length=64)
    batch_id: str | None = Field(default=None, max_length=64)
    device_clock: str | None = Field(default=None, max_length=40)   # tablet's own "now" at upload
    records: list[dict[str, Any]] = Field(min_length=1)

    @field_validator("records")
    @classmethod
    def check_records(cls, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if len(records) > MAX_BATCH:
            raise ValueError(f"at most {MAX_BATCH} records per batch")
        for i, rec in enumerate(records):
            try:
                uuid.UUID(str(rec.get("record_id", "")))
            except ValueError:
                raise ValueError(f"record {i}: record_id must be a UUID (it is the idempotency key)")
        return records


# --------------------------------------------------------------------------- rate limiting
class TokenBucket:
    """Per-device token bucket: each device may send DEVICE_BURST batches at once,
    then DEVICE_RATE_PER_S more per second. Stops one tablet hammering the API."""

    def __init__(self, rate: float, burst: int) -> None:
        self.rate, self.burst = rate, burst
        self._state: dict[str, tuple[float, float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        tokens, last = self._state.get(key, (float(self.burst), now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens < 1:
            self._state[key] = (tokens, now)
            return False
        self._state[key] = (tokens - 1, now)
        return True


def too_busy(reason: str) -> JSONResponse:
    BATCHES.labels(outcome="rejected_429").inc()
    return JSONResponse(status_code=429, headers={"Retry-After": str(RETRY_AFTER_S)},
                        content={"status": "too_busy", "reason": reason,
                                 "retry_after_s": RETRY_AFTER_S})


# --------------------------------------------------------------------------- producer
async def start_producer(app: FastAPI) -> None:
    """Connect to Redpanda, retrying while the brokers start up."""
    from aiokafka import AIOKafkaProducer

    for attempt in range(1, 61):
        producer = AIOKafkaProducer(
            bootstrap_servers=BROKERS,
            acks="all",                 # wait for the Raft majority (2 of 3 brokers)
            enable_idempotence=True,    # broker drops duplicates caused by producer retries
            linger_ms=5,                # group records into fewer network requests
            request_timeout_ms=int(PRODUCE_TIMEOUT_S * 1000),
        )
        try:
            await producer.start()
            app.state.producer = producer
            log.info("connected to %s (attempt %d)", BROKERS, attempt)
            return
        except Exception as exc:  # brokers not ready yet
            log.warning("Redpanda not ready (%s); retry %d/60", exc, attempt)
            await producer.stop()
            await asyncio.sleep(2)
    raise RuntimeError("could not connect to Redpanda")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.buckets = TokenBucket(DEVICE_RATE_PER_S, DEVICE_BURST)
    app.state.inflight = 0
    if getattr(app.state, "producer", None) is None:  # tests inject a fake producer
        await start_producer(app)
    yield
    await app.state.producer.stop()


app = FastAPI(title="NIDH ingestion API", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


# --------------------------------------------------------------------------- endpoints
@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "instance": INSTANCE}


@app.post("/v1/batches")
async def upload_batch(batch: Batch, request: Request):
    started = time.perf_counter()
    state = request.app.state

    if not state.buckets.allow(batch.device_id):
        return too_busy("device rate limit")
    if state.inflight >= MAX_INFLIGHT:
        return too_busy("server busy")

    received_at = datetime.now(timezone.utc).isoformat()
    batch_id = batch.batch_id or str(uuid.uuid4())
    state.inflight += 1
    INFLIGHT.set(state.inflight)
    try:
        sends = []
        for rec in batch.records:
            message = dict(rec)
            message["received_at"] = received_at      # server clock: authoritative for ordering
            message["device_id"] = batch.device_id
            message["batch_id"] = batch_id
            if batch.device_clock:
                message["device_clock"] = batch.device_clock  # lets the processor measure clock skew
            message["ingest_instance"] = INSTANCE
            sends.append(await state.producer.send(
                TOPIC,
                key=str(rec["record_id"]).encode(),
                value=json.dumps(message, default=str).encode(),
            ))
        # The acknowledgement point: wait until Redpanda confirms EVERY record.
        await asyncio.wait_for(asyncio.gather(*sends), timeout=PRODUCE_TIMEOUT_S)
    except Exception as exc:
        # No 200 means the tablet keeps the batch and resends it later.
        # Resending is safe: duplicates are skipped by record_id in the database.
        BATCHES.labels(outcome="failed_503").inc()
        log.warning("batch %s from %s NOT acknowledged: %s", batch_id, batch.device_id, exc)
        return JSONResponse(status_code=503, headers={"Retry-After": "10"},
                            content={"status": "not_acknowledged", "batch_id": batch_id,
                                     "detail": "stream unavailable; resend the same batch"})
    finally:
        state.inflight -= 1
        INFLIGHT.set(state.inflight)

    elapsed = time.perf_counter() - started
    LATENCY.observe(elapsed)
    BATCHES.labels(outcome="acked").inc()
    RECORDS.inc(len(batch.records))
    log.info("batch %s from %s: %d records acknowledged in %.0f ms",
             batch_id, batch.device_id, len(batch.records), elapsed * 1000)
    return {"status": "acknowledged", "batch_id": batch_id, "records": len(batch.records),
            "received_at": received_at, "instance": INSTANCE}
