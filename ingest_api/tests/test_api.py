"""Tests for the ingestion API. They replace Redpanda with a fake producer,
so they run anywhere: `pip install -r requirements-dev.txt && pytest`."""
import asyncio
import json
import uuid

import pytest
from fastapi.testclient import TestClient

from app import main


class FakeProducer:
    """Stands in for AIOKafkaProducer. send() returns a future, like the real one."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: list[tuple[str, bytes, dict]] = []

    async def send(self, topic, key, value):
        self.sent.append((topic, key, json.loads(value)))
        fut = asyncio.get_running_loop().create_future()
        if self.fail:
            fut.set_exception(RuntimeError("broker down"))
        else:
            fut.set_result("ok")
        return fut

    async def stop(self):
        pass


def record(**overrides):
    rec = {"record_id": str(uuid.uuid4()), "child_id": "SYN-0000000001", "facility_id": "F0001",
           "district_id": "D001", "vaccine_code": "PENTA", "dose_no": 1,
           "device_ts": "2026-11-22T10:00:00+03:00", "source": "tablet"}
    rec.update(overrides)
    return rec


def client_with(producer):
    main.app.state.producer = producer
    return TestClient(main.app)


def test_batch_is_acknowledged_and_forwarded():
    fake = FakeProducer()
    with client_with(fake) as c:
        recs = [record() for _ in range(3)]
        r = c.post("/v1/batches", json={"device_id": "TAB-0001", "records": recs})
    assert r.status_code == 200
    assert r.json()["records"] == 3
    topic, key, msg = fake.sent[0]
    assert topic == "vaccinations"
    assert key == recs[0]["record_id"].encode()          # keyed by the idempotency key
    assert msg["device_ts"] == recs[0]["device_ts"]      # tablet time kept as recorded
    assert "received_at" in msg                          # server time added


def test_no_ack_when_stream_fails():
    with client_with(FakeProducer(fail=True)) as c:
        r = c.post("/v1/batches", json={"device_id": "TAB-0002", "records": [record()]})
    assert r.status_code == 503                          # tablet must resend
    assert r.json()["status"] == "not_acknowledged"


def test_missing_or_bad_record_id_rejects_batch():
    with client_with(FakeProducer()) as c:
        r = c.post("/v1/batches", json={"device_id": "TAB-0003",
                                        "records": [record(record_id="not-a-uuid")]})
    assert r.status_code == 422


def test_batch_over_500_records_rejected():
    with client_with(FakeProducer()) as c:
        r = c.post("/v1/batches", json={"device_id": "TAB-0004",
                                        "records": [record() for _ in range(501)]})
    assert r.status_code == 422


def test_device_rate_limit_returns_429_with_retry_after():
    with client_with(FakeProducer()) as c:
        codes = [c.post("/v1/batches", json={"device_id": "TAB-0005", "records": [record()]}).status_code
                 for _ in range(main.DEVICE_BURST + 1)]
        last = c.post("/v1/batches", json={"device_id": "TAB-0005", "records": [record()]})
    assert codes[:main.DEVICE_BURST] == [200] * main.DEVICE_BURST
    assert last.status_code == 429
    assert "Retry-After" in last.headers


def test_bad_content_is_forwarded_not_dropped():
    """Impossible values are the processor's job (rejects topic), not the API's."""
    fake = FakeProducer()
    with client_with(fake) as c:
        r = c.post("/v1/batches", json={"device_id": "TAB-0006", "records": [record(dose_no=7)]})
    assert r.status_code == 200
    assert fake.sent[0][2]["dose_no"] == 7


def test_healthz():
    with client_with(FakeProducer()) as c:
        assert c.get("/healthz").json()["status"] == "ok"
