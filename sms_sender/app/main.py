"""NIDH SMS sender (two copies).

Both copies, as one consumer group on the `alerts` topic:
  1. Queue one SMS per fridge alert in `sms_messages` (ID fixed by the alert, so a repeated
     alert from the outbox is skipped), then commit the offset.

Only the copy holding the 'sms_sender' lease:
  2. Reminder job (every REMINDER_INTERVAL_S; once a day in production): find doses that are
     due, for children whose caregiver consented, and queue ONE reminder per dose, under the
     monthly cap (180,000 at national scale; 9,000 at k = 20). Fridge alerts are never capped.
  3. Send what is due, fridge alerts first, through the gateway. On failure, retry with
     backoff + jitter: alerts about every 30 s for 2 hours (then 'failed'; the alert is still
     on the dashboard and in GET /v1/alerts); reminders 1, 2, 4 ... minutes, at most hourly,
     for 48 hours (then 'expired'). Before each try a reminder is cancelled if the child has
     already had that dose.
  4. A crash between "gateway accepted" and "database updated" only means the message is
     tried again: the gateway recognises the message_id and does not send or charge twice.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time

import httpx
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Gauge, start_http_server

from . import rules
from .store import Store, utcnow

BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
GATEWAY = os.getenv("SMS_GATEWAY_URL", "http://sms-gateway:8092")
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
CAP = int(os.getenv("SMS_MONTHLY_CAP", "9000"))             # 180,000 / k 20
REMINDER_INTERVAL_S = float(os.getenv("REMINDER_INTERVAL_S", "60"))
SEND_INTERVAL_S = float(os.getenv("SEND_INTERVAL_S", "2"))
PARALLEL = int(os.getenv("SMS_PARALLEL", "5"))
LEASE_TTL_S = int(os.getenv("LEASE_TTL_S", "30"))

log = logging.getLogger("sms_sender")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s sms-sender %(message)s")

QUEUED = Counter("sms_queued_total", "Messages queued", ["kind"])
CAPPED = Counter("sms_capped_total", "Reminders not sent because the monthly cap was reached")
SENT = Counter("sms_sent_total", "Messages accepted by the gateway", ["kind"])
FAILED_TRIES = Counter("sms_send_failures_total", "Failed tries (will retry)", ["kind"])
ENDED = Counter("sms_ended_total", "Messages that ended without being sent", ["kind", "status"])
IS_LEADER = Gauge("sms_sender_is_leader", "1 if this copy sends")
MONTH = Gauge("sms_month_messages", "Messages this month", ["kind", "status"])
MONTH_COST = Gauge("sms_month_cost_ugx", "Money spent on SMS this month (UGX)")
CAP_GAUGE = Gauge("sms_monthly_cap", "Monthly reminder cap")
CAP_GAUGE.set(CAP)


async def consume_alerts(store: Store) -> None:
    while True:
        consumer = AIOKafkaConsumer(ALERTS_TOPIC, bootstrap_servers=BROKERS, group_id="sms-alerts",
                                    enable_auto_commit=False, auto_offset_reset="earliest")
        try:
            await consumer.start()
            break
        except Exception as exc:
            log.warning("Redpanda not ready (%s); retrying in 3 s", exc)
            await consumer.stop()
            await asyncio.sleep(3)
    log.info("%s reading %s", INSTANCE, ALERTS_TOPIC)
    try:
        while True:
            fetched = await consumer.getmany(timeout_ms=1000, max_records=200)
            alerts = []
            for messages in fetched.values():
                for m in messages:
                    try:
                        a = json.loads(m.value)
                        if a.get("alert_id") and a.get("facility_id") and a.get("kind"):
                            alerts.append(a)
                    except ValueError:
                        log.warning("skipping a message that is not JSON")
            if alerts:
                await store.queue_alerts(alerts)
                QUEUED.labels(kind="fridge_alert").inc(len(alerts))
            if fetched:
                await consumer.commit()           # only after the queue rows are committed
    finally:
        await consumer.stop()


async def post(client: httpx.AsyncClient, row, to: str):
    """One gateway call. Returns (provider_id, cost) on success, or an error string."""
    message_id, kind, _ref, _c, _r, _p, body, *_ = row
    try:
        resp = await client.post(f"{GATEWAY}/send", json={
            "message_id": str(message_id), "to": to, "body": body, "kind": kind})
        if resp.status_code == 200:
            data = resp.json()
            return data["provider_id"], int(data.get("cost_ugx", 0))
        return f"gateway {resp.status_code}: {resp.text[:100]}"
    except Exception as exc:
        return f"gateway unreachable: {exc!r}"


async def send_batch(store: Store, client: httpx.AsyncClient, rows) -> None:
    """Checks one at a time, gateway calls in parallel, database updates one at a time
    (the store has one connection, which must not run two transactions at once)."""
    ready = []
    for row in rows:
        message_id, kind, ref, _c, recipient, recipient_plain, _b, attempts, expires_at = row
        if expires_at < utcnow():
            status = rules.final_status(kind)
            await store.finish(message_id, status, "ran out of time")
            ENDED.labels(kind=kind, status=status).inc()
            log.info("%s %s %s after %d tries", kind, message_id, status, attempts)
            continue
        if kind == "dose_reminder" and await store.dose_given(ref):
            await store.finish(message_id, "cancelled", "dose already given")
            ENDED.labels(kind=kind, status="cancelled").inc()
            continue
        try:
            ready.append((row, recipient_plain or rules_decrypt(recipient)))
        except Exception as exc:
            FAILED_TRIES.labels(kind=kind).inc()
            await store.mark_retry(message_id, attempts, kind, f"cannot read recipient: {exc!r}")
    results = await asyncio.gather(*[post(client, row, to) for row, to in ready])
    for (row, _to), result in zip(ready, results):
        message_id, kind, *_rest = row
        if isinstance(result, tuple):
            await store.mark_sent(message_id, *result)
            SENT.labels(kind=kind).inc()
        else:
            FAILED_TRIES.labels(kind=kind).inc()
            await store.mark_retry(message_id, row[7], kind, result)


def rules_decrypt(recipient: bytes | None) -> str:
    """Only this service holds PHONE_KEY; the number exists in clear only while sending."""
    if not recipient:
        raise ValueError("no recipient")
    from cryptography.fernet import Fernet
    return Fernet(os.environ["PHONE_KEY"].encode()).decrypt(bytes(recipient)).decode()


async def lead(store: Store) -> None:
    leader, last_reminders, last_summary = False, 0.0, 0.0
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            started = time.monotonic()
            try:
                now_leader = await store.take_lease("sms_sender", INSTANCE, LEASE_TTL_S)
                if now_leader != leader:
                    log.info("%s %s the sms_sender lease", INSTANCE,
                             "took" if now_leader else "does not hold")
                leader = now_leader
                IS_LEADER.set(1 if leader else 0)
                if leader:
                    if started - last_reminders >= REMINDER_INTERVAL_S:
                        last_reminders = started
                        queued, capped = await store.queue_reminders(utcnow(), CAP)
                        if queued or capped:
                            log.info("reminder job: %d queued, %d over the monthly cap",
                                     queued, capped)
                        QUEUED.labels(kind="dose_reminder").inc(queued)
                        CAPPED.inc(capped)
                    rows = await store.due_messages(PARALLEL * 4)
                    for i in range(0, len(rows), PARALLEL):
                        await send_batch(store, client, rows[i:i + PARALLEL])
                    if started - last_summary >= 15:
                        last_summary = started
                        MONTH.clear()
                        cost = 0
                        for kind, status, n, ugx in await store.month_summary(utcnow()):
                            MONTH.labels(kind=kind, status=status).set(n)
                            cost += int(ugx)
                        MONTH_COST.set(cost)
            except Exception as exc:
                log.exception("sender run failed: %s", exc)
            await asyncio.sleep(max(0.0, SEND_INTERVAL_S - (time.monotonic() - started)))


async def main() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    if not os.getenv("PHONE_KEY"):
        log.warning("PHONE_KEY is not set: reminders cannot be decrypted and will retry")
    consumer_store, sender_store = Store(DSN), Store(DSN)
    await consumer_store.connect()
    await sender_store.connect()
    await asyncio.gather(consume_alerts(consumer_store), lead(sender_store))


if __name__ == "__main__":
    asyncio.run(main())
