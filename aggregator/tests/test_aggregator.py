"""Aggregator tests: payload shape, and the full run against fakes (no Docker needed)."""
import asyncio
import json
from datetime import date, datetime, timedelta, timezone

from app import main
from app.payloads import build_payloads, by_iso_week, iso_week

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
DAY_ROWS = [
    ("D001", "BCG", 1, date(2026, 11, 20), 3),
    ("D001", "OPV", 2, date(2026, 11, 20), 2),
    ("D002", "BCG", 1, date(2026, 11, 18), 5),
    ("D002", "BCG", 1, date(2026, 11, 10), 1),
]
ROWS = [
    ("D001", "BCG", 1, "2026-W47", 3),
    ("D001", "OPV", 2, "2026-W47", 2),
    ("D002", "BCG", 1, "2026-W47", 5),
    ("D002", "BCG", 1, "2026-W46", 1),
]


def test_payloads_per_district_and_national():
    districts, national, by_vaccine = build_payloads(ROWS, NOW)
    assert set(districts) == {"D001", "D002"}
    assert districts["D001"]["total_doses"] == 5
    assert districts["D002"]["rows"][0]["week"] == "2026-W46"          # sorted by week
    assert national["total_doses"] == 11
    assert {"district_id": "D002", "week": "2026-W47", "doses": 5} in national["rows"]
    assert by_vaccine == {("BCG", 1): 9, ("OPV", 2): 2}


def test_national_payload_has_no_child_level_data():
    _, national, _ = build_payloads(ROWS, NOW)
    assert set(national["rows"][0]) == {"district_id", "week", "doses"}


def test_empty_database():
    districts, national, by_vaccine = build_payloads([], NOW)
    assert districts == {} and national["total_doses"] == 0 and by_vaccine == {}


def test_week_sql_uses_skew_corrected_uganda_time():
    assert "skew_seconds" in main.EVENT_DAY and "3 hours" in main.EVENT_DAY


def test_iso_week_across_new_year():
    assert iso_week(date(2026, 11, 22)) == "2026-W47"
    assert iso_week(date(2026, 12, 31)) == "2026-W53"
    assert iso_week(date(2027, 1, 4)) == "2027-W01"


def test_days_merge_into_weeks():
    days = [("D001", "BCG", 1, date(2026, 11, 16), 2),     # Monday, W47
            ("D001", "BCG", 1, date(2026, 11, 22), 3),     # Sunday, W47
            ("D001", "BCG", 1, date(2026, 11, 23), 1)]     # Monday, W48
    assert by_iso_week(days) == [("D001", "BCG", 1, "2026-W47", 5), ("D001", "BCG", 1, "2026-W48", 1)]


# ---------------------------------------------------------------- run_once with fakes
class FakeCursor:
    def __init__(self, rows=None):
        self.rows, self.many = rows, []

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return self.rows[0]

    async def executemany(self, sql, rows):
        self.many.append((sql, list(rows)))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()

    async def execute(self, sql, *a):
        if "GROUP BY" in sql:
            return FakeCursor(DAY_ROWS)
        return FakeCursor([(NOW - timedelta(seconds=7),)])

    def transaction(self):
        return FakeTx()

    def cursor(self):
        return self.cur


class FakePipe:
    def __init__(self, store, fail):
        self.store, self.fail, self.pending = store, fail, {}

    def set(self, key, value, ex):
        self.pending[key] = (json.loads(value), ex)

    async def execute(self):
        if self.fail:
            raise ConnectionError("cache down")
        self.store.update(self.pending)


class FakeCache:
    def __init__(self, fail=False):
        self.data, self.fail = {}, fail

    def pipeline(self, transaction=False):
        return FakePipe(self.data, self.fail)


def test_run_once_writes_table_and_cache():
    conn, cache = FakeConn(), FakeCache()
    n = asyncio.run(main.run_once(conn, cache))
    assert n == 4
    sql, rows = conn.cur.many[0]
    assert sql.strip().startswith("UPSERT INTO district_coverage") and len(rows) == 4
    assert cache.data["cov:district:D001"][0]["total_doses"] == 5
    assert cache.data["cov:national"][1] == main.CACHE_TTL_S       # every key expires


def test_cache_down_does_not_fail_the_run():
    conn = FakeConn()
    assert asyncio.run(main.run_once(conn, FakeCache(fail=True))) == 4
    assert conn.cur.many                       # the table was still refreshed
