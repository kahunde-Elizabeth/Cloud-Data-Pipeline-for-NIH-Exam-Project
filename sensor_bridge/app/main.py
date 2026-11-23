"""NIDH sensor bridge: MQTT (Mosquitto) -> Redpanda topic `sensor-readings`.

  * Subscribes with QoS 1 to readings and to status messages (including each sensor's
    Last Will, which the broker publishes if a sensor's connection drops).
  * Two copies share one subscription ($share/bridge/...), so the broker hands each
    message to only one of them, and to the other if one copy is down.
  * ACK point: a message is acknowledged to the broker (PUBACK) only AFTER Redpanda has
    confirmed it with acks=all. If Redpanda cannot confirm, nothing is acknowledged; the
    bridge reconnects and the broker, which kept the session (clean_session=False),
    sends the unacknowledged messages again.
  * The same reading delivered twice (same sensor_id + ts) is forwarded once.
    (A repeat that reaches the other copy is caught again by the sensor aggregator.)

The design names Redpanda Connect for this job; the prototype uses this small Python bridge,
which does the same steps and can be unit-tested.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections import OrderedDict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
TOPIC = os.getenv("SENSOR_TOPIC", "sensor-readings")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq")
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
SUBSCRIPTIONS = ["$share/bridge/nidh/sensors/+/reading", "$share/bridge/nidh/sensors/+/status"]
BATCH_MAX = int(os.getenv("BRIDGE_BATCH_MAX", "200"))
DEDUP_SIZE = int(os.getenv("BRIDGE_DEDUP_SIZE", "50000"))

log = logging.getLogger("sensor_bridge")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s sensor-bridge %(message)s")

FORWARDED = Counter("bridge_messages_forwarded_total", "Messages confirmed by Redpanda", ["kind"])
DUPLICATES = Counter("bridge_duplicates_skipped_total", "Repeated readings (same sensor_id + ts)")
TO_DLQ = Counter("bridge_dlq_total", "Messages that are not valid JSON")
KAFKA_FAIL = Counter("bridge_kafka_failures_total", "Batches Redpanda could not confirm")
LATENCY = Histogram("bridge_batch_seconds", "MQTT receive to Redpanda confirmation",
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))


class Dedup:
    """Remembers the last N (sensor_id, ts) pairs."""

    def __init__(self, size: int):
        self.size, self.seen = size, OrderedDict()

    def is_new(self, key) -> bool:
        if key in self.seen:
            self.seen.move_to_end(key)
            return False
        self.seen[key] = None
        if len(self.seen) > self.size:
            self.seen.popitem(last=False)
        return True


def convert(topic: str, payload: bytes, received_at: datetime):
    """MQTT message -> (kind, key, value) for Redpanda, or ('dlq', None, value)."""
    kind = "status" if topic.endswith("/status") else "reading"
    try:
        body = json.loads(payload)
        if not isinstance(body, dict) or not body.get("sensor_id"):
            raise ValueError("no sensor_id")
    except (ValueError, UnicodeDecodeError) as exc:
        return "dlq", None, {"source": "sensor_bridge", "topic": topic, "error": str(exc),
                             "raw": payload.decode("utf-8", "replace")[:500]}
    body.update(kind=kind, received_at=received_at.isoformat(), bridge=INSTANCE)
    return kind, body["sensor_id"], body


class Bridge:
    def __init__(self, producer, client):
        self.producer, self.client = producer, client
        self.queue: asyncio.Queue = asyncio.Queue()
        self.dedup = Dedup(DEDUP_SIZE)

    def on_message(self, loop, msg):          # paho thread -> asyncio thread
        loop.call_soon_threadsafe(self.queue.put_nowait,
                                  (msg.mid, msg.qos, msg.topic, msg.payload, time.perf_counter()))

    async def forward(self, batch) -> None:
        """Send a batch to Redpanda; acknowledge to MQTT only after every send is confirmed."""
        now = datetime.now(timezone.utc)
        sends, kinds = [], []
        for _mid, _qos, topic, payload, _t in batch:
            kind, key, value = convert(topic, payload, now)
            if kind == "reading" and not self.dedup.is_new((key, value.get("ts"))):
                DUPLICATES.inc()
                continue
            target = DLQ_TOPIC if kind == "dlq" else TOPIC
            sends.append(await self.producer.send(
                target, json.dumps(value).encode(), key=key.encode() if key else None))
            kinds.append(kind)
        await asyncio.gather(*sends)                  # raises if Redpanda cannot confirm
        for mid, qos, *_ in batch:                    # the ACK point
            if qos > 0:
                self.client.ack(mid, qos)
        for kind in kinds:
            (TO_DLQ.inc() if kind == "dlq" else FORWARDED.labels(kind=kind).inc())
        LATENCY.observe(time.perf_counter() - batch[0][4])

    async def run(self) -> None:
        while True:
            batch = [await self.queue.get()]
            while len(batch) < BATCH_MAX and not self.queue.empty():
                batch.append(self.queue.get_nowait())
            try:
                await self.forward(batch)
            except Exception as exc:
                # Nothing in this batch was acknowledged. Forget what we queued and reconnect:
                # the broker resends every unacknowledged message from the kept session.
                KAFKA_FAIL.inc()
                for _mid, _q, topic, payload, _t in batch:
                    kind, key, value = convert(topic, payload, datetime.now(timezone.utc))
                    self.dedup.seen.pop((key, value.get("ts")), None)
                while not self.queue.empty():
                    self.queue.get_nowait()
                log.warning("Redpanda did not confirm %d messages (%s); reconnecting to MQTT "
                            "so the broker resends them", len(batch), exc)
                await asyncio.sleep(2)
                self.client.reconnect()


def make_client(loop, bridge_ref) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"bridge-{INSTANCE}",
                         clean_session=False, manual_ack=True)
    client.max_inflight_messages_set(BATCH_MAX)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(c, _u, _flags, reason, _props):
        if reason.is_failure:
            log.warning("MQTT connect failed: %s", reason)
            return
        for sub in SUBSCRIPTIONS:
            c.subscribe(sub, qos=1)
        log.info("%s connected to MQTT and subscribed", INSTANCE)

    client.on_connect = on_connect
    client.on_message = lambda _c, _u, msg: bridge_ref[0].on_message(loop, msg)
    return client


async def start_producer() -> AIOKafkaProducer:
    while True:
        producer = AIOKafkaProducer(bootstrap_servers=BROKERS, acks="all",
                                    enable_idempotence=True, linger_ms=5)
        try:
            await producer.start()
            return producer
        except Exception as exc:
            log.warning("Redpanda not ready (%s); retrying in 3 s", exc)
            await producer.stop()
            await asyncio.sleep(3)


async def main() -> None:
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    loop = asyncio.get_running_loop()
    producer = await start_producer()
    ref: list = [None]
    client = make_client(loop, ref)
    ref[0] = bridge = Bridge(producer, client)
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as exc:
            log.warning("MQTT broker not ready (%s); retrying in 3 s", exc)
            await asyncio.sleep(3)
    client.loop_start()
    try:
        await bridge.run()
    finally:
        client.loop_stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
