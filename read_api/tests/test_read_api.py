"""Read API tests with an in-memory store and cache (no Docker needed)."""
import uuid

import pytest
from fastapi.testclient import TestClient

from app import main

ROWS = [
    ("D001", "BCG", 1, "2026-W47", 3),
    ("D001", "OPV", 1, "2026-W47", 2),
    ("D001", "OPV", 1, "2026-W46", 4),
    ("D002", "MR", 1, "2026-W47", 5),
]
USERS = {
    "officer_d001": {"user_id": uuid.uuid4(), "role": "district_officer", "district_id": "D001"},
    "analyst_national": {"user_id": uuid.uuid4(), "role": "national_analyst", "district_id": None},
}


class FakeStore:
    def __init__(self):
        self.audits, self.coverage_reads, self.alert_reads = [], 0, []
        self.audit_fails = self.db_down = False

    async def user(self, username):
        if self.db_down:
            raise RuntimeError("db down")
        return USERS.get(username)

    async def audit(self, user_id, action, resource, params):
        if self.audit_fails:
            raise RuntimeError("audit down")
        self.audits.append((user_id, action, resource, params))

    async def coverage(self, district_id):
        self.coverage_reads += 1
        return [r for r in ROWS if district_id is None or r[0] == district_id]

    async def alerts(self, district_id, hours):
        self.alert_reads.append((district_id, hours))
        rows = [{"sensor_id": "S0001", "district_id": "D001", "kind": "too_warm"},
                {"sensor_id": "S0002", "district_id": "D002", "kind": "offline"}]
        return [r for r in rows if district_id is None or r["district_id"] == district_id]

    async def close(self):
        pass


class FakeCache:
    def __init__(self):
        self.data, self.down = {}, False

    async def get(self, key):
        if self.down:
            raise ConnectionError("cache down")
        return self.data.get(key)

    async def set(self, key, value):
        if self.down:
            raise ConnectionError("cache down")
        self.data[key] = value

    async def close(self):
        pass


@pytest.fixture
def env():
    store, cache = FakeStore(), FakeCache()
    main.app.state.store, main.app.state.cache = store, cache
    with TestClient(main.app) as client:
        yield client, store, cache
    del main.app.state.store, main.app.state.cache


def get(client, path, user):
    return client.get(path, headers={"X-User": user} if user else {})


def test_officer_reads_own_district_and_it_is_audited(env):
    client, store, _ = env
    r = get(client, "/v1/coverage/districts/D001", "officer_d001")
    assert r.status_code == 200
    body = r.json()
    assert body["total_doses"] == 9 and body["source"] == "database"
    assert store.audits[-1][1:3] == ("read", "coverage/district/D001")
    assert store.audits[-1][0] == USERS["officer_d001"]["user_id"]


def test_second_read_comes_from_cache(env):
    client, store, _ = env
    get(client, "/v1/coverage/districts/D001", "officer_d001")
    r = get(client, "/v1/coverage/districts/D001", "officer_d001")
    assert r.json()["source"] == "cache"
    assert store.coverage_reads == 1          # the database was read once only
    assert len(store.audits) == 2             # but BOTH reads were audited


def test_officer_refused_other_district_and_refusal_is_audited(env):
    client, store, _ = env
    r = get(client, "/v1/coverage/districts/D002", "officer_d001")
    assert r.status_code == 403
    assert store.audits[-1][1] == "read_denied"
    assert store.coverage_reads == 0


def test_officer_refused_national(env):
    client, store, _ = env
    r = get(client, "/v1/coverage/national", "officer_d001")
    assert r.status_code == 403 and store.audits[-1][1] == "read_denied"


def test_analyst_reads_national_and_any_district(env):
    client, store, _ = env
    r = get(client, "/v1/coverage/national", "analyst_national")
    assert r.status_code == 200 and r.json()["total_doses"] == 14
    assert get(client, "/v1/coverage/districts/D002", "analyst_national").json()["total_doses"] == 5
    assert [a[1] for a in store.audits] == ["read", "read"]


def test_week_filter(env):
    client, _, _ = env
    r = get(client, "/v1/coverage/districts/D001?week=2026-W47", "officer_d001")
    assert r.json()["total_doses"] == 5
    assert {row["week"] for row in r.json()["rows"]} == {"2026-W47"}


def test_no_or_unknown_user_is_401(env):
    client, store, _ = env
    assert get(client, "/v1/coverage/national", None).status_code == 401
    assert get(client, "/v1/coverage/national", "mallory").status_code == 401
    assert store.audits == []


def test_bad_district_format_is_400(env):
    client, _, _ = env
    assert get(client, "/v1/coverage/districts/kampala", "analyst_national").status_code == 400


def test_no_audit_means_no_data(env):
    client, store, _ = env
    store.audit_fails = True
    r = get(client, "/v1/coverage/districts/D001", "officer_d001")
    assert r.status_code == 503 and "rows" not in r.json()
    assert store.coverage_reads == 0


def test_cache_down_falls_back_to_database(env):
    client, store, cache = env
    cache.down = True
    r = get(client, "/v1/coverage/districts/D001", "officer_d001")
    assert r.status_code == 200 and r.json()["source"] == "database"


def test_database_down_is_503_with_retry_after(env):
    client, store, _ = env
    store.db_down = True
    r = get(client, "/v1/coverage/national", "analyst_national")
    assert r.status_code == 503 and r.headers["Retry-After"] == "5"


def test_empty_district_returns_zero(env):
    client, _, _ = env
    r = get(client, "/v1/coverage/districts/D150", "analyst_national")
    assert r.status_code == 200 and r.json()["total_doses"] == 0 and r.json()["rows"] == []


def test_officer_alerts_are_limited_to_own_district(env):
    client, store, _ = env
    r = get(client, "/v1/alerts", "officer_d001")
    assert r.status_code == 200 and r.json()["count"] == 1
    assert store.alert_reads[-1] == ("D001", 24)
    assert store.audits[-1][1:3] == ("read", "alerts/district/D001")


def test_officer_refused_other_districts_alerts(env):
    client, store, _ = env
    r = get(client, "/v1/alerts?district=D002", "officer_d001")
    assert r.status_code == 403 and store.audits[-1][1] == "read_denied"
    assert store.alert_reads == []


def test_analyst_sees_all_alerts_or_one_district(env):
    client, store, _ = env
    assert get(client, "/v1/alerts", "analyst_national").json()["count"] == 2
    assert get(client, "/v1/alerts?district=D002&hours=6", "analyst_national").json()["count"] == 1
    assert store.alert_reads[-1] == ("D002", 6)
    assert store.audits[-2][2] == "alerts/national"
