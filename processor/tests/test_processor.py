"""Processor tests: run with `pytest -q` inside processor/ (no Docker needed)."""
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app import main as proc
from app.validate import Invalid, check

NOW = datetime(2026, 11, 22, 18, 0, tzinfo=timezone.utc)


def msg(**overrides):
    m = {"record_id": str(uuid.uuid4()), "child_id": "SYN-0000000001", "facility_id": "F0001",
         "district_id": "D001", "vaccine_code": "PENTA", "dose_no": 1,
         "device_ts": (NOW - timedelta(hours=3)).isoformat(), "received_at": NOW.isoformat(),
         "source": "tablet", "child_birth_date": "2026-06-01"}
    m.update(overrides)
    return m


# ------------------------------------------------------------------ "Valid format?"
def test_valid_record_passes():
    row = check(msg(), {"F0001"})
    assert row.dose_no == 1 and not row.clock_skew_flag


@pytest.mark.parametrize("field,value", [
    ("child_id", "BOB-1"), ("facility_id", "X1"), ("district_id", "1"), ("vaccine_code", "ABC"),
    ("dose_no", 7), ("dose_no", "two"), ("device_ts", "yesterday"), ("source", "fax"),
])
def test_invalid_values_rejected(field, value):
    with pytest.raises(Invalid):
        check(msg(**{field: value}), {"F0001"})


def test_unknown_facility_rejected():
    with pytest.raises(Invalid, match="unknown facility"):
        check(msg(facility_id="F9999"), {"F0001"})


def test_vaccinated_before_birth_rejected():
    with pytest.raises(Invalid, match="before the child's birth"):
        check(msg(child_birth_date="2026-12-01"), {"F0001"})


# ------------------------------------------------------------------ "Clock skewed?"
def test_offline_record_hours_old_is_not_skew():
    """Records made offline hours ago are normal: device_ts << received_at is fine."""
    assert not check(msg(device_ts=(NOW - timedelta(hours=9)).isoformat()), {"F0001"}).clock_skew_flag


def test_tablet_clock_two_hours_slow_is_flagged_but_kept():
    row = check(msg(device_clock=(NOW - timedelta(hours=2)).isoformat()), {"F0001"})
    assert row.clock_skew_flag and row.skew_seconds == -7200


def test_future_dated_record_is_flagged_but_kept():
    row = check(msg(device_ts=(NOW + timedelta(hours=1)).isoformat()), {"F0001"})
    assert row.clock_skew_flag and row.skew_seconds == 3600


def test_small_clock_difference_not_flagged():
    row = check(msg(device_clock=(NOW + timedelta(seconds=90)).isoformat()), {"F0001"})
    assert not row.clock_skew_flag


# ------------------------------------------------------------------ batch handling
class FakeMsg:
    def __init__(self, value, offset):
        self.value, self.topic, self.partition, self.offset = value, "vaccinations", 0, offset


class FakeStore:
    def __init__(self):
        self.facilities = {"F0001"}
        self.stored = set()
        self.calls = []

    async def write(self, rows, rejects, offset_ids):
        self.calls.append((rows, rejects))
        new = [r for r in rows if r.record_id not in self.stored]
        self.stored.update(r.record_id for r in new)
        return {r.record_id for r in new}, len(rows) - len(new), []


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, value):
        self.sent.append((topic, json.loads(value)))
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut


def run(messages, store, producer):
    asyncio.run(proc.process(messages, store, producer))


def test_batch_splits_good_bad_and_poison():
    store, prod = FakeStore(), FakeProducer()
    good = msg()
    run([FakeMsg(json.dumps(good).encode(), 1),
         FakeMsg(json.dumps(msg(dose_no=9)).encode(), 2),
         FakeMsg(b"not json at all", 3)], store, prod)
    rows, rejects = store.calls[0]
    assert [str(r.record_id) for r in rows] == [good["record_id"]]
    assert rejects[0].reason == "invalid_format"
    assert {t for t, _ in prod.sent} == {"rejects", "dlq"}


def test_replay_of_same_messages_stores_nothing_twice():
    """A crash before the offset commit replays the batch; nothing is stored twice."""
    store, prod = FakeStore(), FakeProducer()
    batch = [FakeMsg(json.dumps(msg()).encode(), i) for i in range(5)]
    run(batch, store, prod)
    run(batch, store, prod)          # the replay
    assert len(store.stored) == 5


def test_same_record_twice_in_one_fetch_kept_once():
    store, prod = FakeStore(), FakeProducer()
    m = json.dumps(msg()).encode()
    run([FakeMsg(m, 1), FakeMsg(m, 2)], store, prod)
    assert len(store.calls[0][0]) == 1


def test_stable_reject_id_on_replay():
    a, b = FakeMsg(b"{}", 7), FakeMsg(b"{}", 7)
    assert proc.stable_id(a, "invalid_format") == proc.stable_id(b, "invalid_format")


def test_retried_bad_record_gets_same_reject_id():
    """A tablet retry puts the same bad record at a NEW offset; it must not be logged twice."""
    store, prod = FakeStore(), FakeProducer()
    bad = json.dumps(msg(dose_no=7)).encode()
    run([FakeMsg(bad, 1)], store, prod)
    run([FakeMsg(bad, 99)], store, prod)          # the retry, at a different offset
    first, second = store.calls[0][1][0], store.calls[1][1][0]
    assert first.reject_id == second.reject_id


# ---------------------------------------------------------------- caregiver contact (step 5)
def test_caregiver_phone_and_consent_kept():
    row = check(msg(caregiver_phone="SYN-0712345678", sms_consent=True), {"F0001"})
    assert row.caregiver_phone == "SYN-0712345678" and row.sms_consent is True


def test_consent_without_phone_is_no_consent():
    assert check(msg(sms_consent=True), {"F0001"}).sms_consent is False


@pytest.mark.parametrize("phone", ["0712345678", "+256712345678", "SYN-071234"])
def test_real_looking_phone_numbers_rejected(phone):
    with pytest.raises(Invalid):
        check(msg(caregiver_phone=phone), {"F0001"})


def test_consent_must_be_boolean():
    with pytest.raises(Invalid):
        check(msg(caregiver_phone="SYN-0712345678", sms_consent="yes"), {"F0001"})


def test_phone_is_encrypted_before_storing(monkeypatch):
    from cryptography.fernet import Fernet
    from app import store
    key = Fernet.generate_key()
    monkeypatch.setenv("PHONE_KEY", key.decode())
    monkeypatch.setattr(store, "_FERNET", None)
    secret = store.encrypt_phone("SYN-0712345678")
    assert b"0712345678" not in secret
    assert Fernet(key).decrypt(secret) == b"SYN-0712345678"
    assert store.encrypt_phone(None) is None


# ---------------------------------------------------------------- Annex B fields (step 6)
def test_mcv_is_a_known_vaccine():
    assert check(msg(vaccine_code="MCV"), {"F0001"}).vaccine_code == "MCV"


def test_administered_at_is_skew_corrected():
    ok = check(msg(), {"F0001"})
    assert ok.administered_at == ok.device_ts
    clock = (NOW + timedelta(hours=2)).isoformat()             # tablet clock 2 h fast
    skewed = check(msg(device_ts=(NOW + timedelta(hours=1)).isoformat(), device_clock=clock),
                   {"F0001"})
    assert skewed.clock_skew_flag and skewed.administered_at == NOW - timedelta(hours=1)
