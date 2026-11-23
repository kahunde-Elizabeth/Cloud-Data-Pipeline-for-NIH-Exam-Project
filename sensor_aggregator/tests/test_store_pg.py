"""Run the aggregator's real SQL against PostgreSQL (CockroachDB speaks the same protocol).
Skipped unless NIDH_PG_DSN points at an empty PostgreSQL database, e.g.
    NIDH_PG_DSN=postgresql://postgres@localhost:5432/nidh pytest -q
The tables are taken from db/schema.sql, with CockroachDB-only syntax translated."""
import asyncio
import os
import re
from datetime import datetime, timedelta, timezone

import pytest

psycopg = pytest.importorskip("psycopg")
DSN = os.getenv("NIDH_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="NIDH_PG_DSN not set")

from app.rules import Reading, StatusEvent, Windows       # noqa: E402
from app.store import Store                               # noqa: E402

SCHEMA = os.path.join(os.path.dirname(__file__), "..", "..", "db", "schema.sql")
TABLES = ("facilities", "sensor_readings_1min", "sensor_status", "alerts", "aggregator_lease")
NOW = datetime.now(timezone.utc).replace(microsecond=0)


def pg_schema() -> str:
    sql = re.sub(r"--[^\n]*", "", open(SCHEMA).read())
    out = []
    for stmt in sql.split(";"):
        s = stmt.strip()
        if not any(re.search(rf"\b(TABLE|ON)\s+(IF NOT EXISTS\s+)?{t}\b", s) for t in TABLES):
            continue
        s = re.sub(r"\bSTRING\b", "TEXT", s)
        s = re.sub(r"\)\s*WITH \(ttl_expire_after[^)]*\)", ")", s)
        s = re.sub(r",\s*INDEX [^\n]*?\)(?=\s*(,|\)\s*$))", "", s)       # inline indexes
        s = re.sub(r"\s*USING HASH", "", s)
        out.append(s)
    return ";\n".join(out) + ";"


@pytest.fixture
def store():
    async def setup():
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
        await conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                           "WHERE datname = current_database() AND pid <> pg_backend_pid()")
        for t in reversed(TABLES):
            await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        await conn.execute(pg_schema())
        await conn.execute("INSERT INTO facilities (facility_id, district_id, name) "
                           "VALUES ('F0001','D001','x'), ('F0002','D002','y')")
        await conn.close()
        s = Store(DSN)
        await s.connect()
        return s
    return asyncio.run(setup())


def r(temp, minutes_ago, sensor="S0001", facility="F0001"):
    at = NOW - timedelta(minutes=minutes_ago)
    return Reading(sensor, facility, at, at, temp)


def run(coro):
    return asyncio.run(coro)


def test_schema_translates_and_loads(store):
    assert store.facilities == {"F0001", "F0002"}


def test_state_alerts_and_windows_saved(store):
    async def go():
        await store.connect()
        alerts = await store.process({"S0001": [r(4.0, 3), r(9.2, 2)]}, [])
        assert [a.kind for a in alerts] == ["too_warm"]
        w = Windows()
        for i, rd in enumerate([r(4.0, 3), r(4.4, 3)]):
            w.add(rd, 0, i, NOW)
        rows = [x.row() for x in w.open.values()]
        await store.process({}, rows)
        cur = await store.conn.execute(
            "SELECT status, temp_state, last_temp_c FROM sensor_status WHERE sensor_id='S0001'")
        row = await cur.fetchone()
        assert row[:2] == ("online", "too_warm") and float(row[2]) == 9.2
        cur = await store.conn.execute("SELECT count(*), bool_or(published) FROM alerts")
        assert await cur.fetchone() == (1, False)
        cur = await store.conn.execute("SELECT n_readings FROM sensor_readings_1min")
        assert (await cur.fetchone())[0] == 1                 # same ts -> one reading
        await store.conn.commit()
    run(go())


def test_replay_saves_nothing_twice(store):
    async def go():
        await store.connect()
        events = {"S0001": [r(4.0, 3), r(9.2, 2)]}
        first = await store.process(events, [])
        again = await store.process(events, [])
        cur = await store.conn.execute("SELECT count(*) FROM alerts")
        assert len(first) == 1 and again == [] and (await cur.fetchone())[0] == 1
        await store.conn.commit()
    run(go())


def test_partial_window_never_overwrites_complete_one(store):
    async def go():
        await store.connect()
        start = NOW.replace(second=0) - timedelta(minutes=5)
        full = ("S0001", start, "F0001", 4.0, 4.5, 5.0, 3, False)
        partial = ("S0001", start, "F0001", 4.0, 4.0, 4.0, 1, False)
        await store.process({}, [full])
        await store.process({}, [partial])
        cur = await store.conn.execute("SELECT n_readings, max_c FROM sensor_readings_1min")
        n, mx = await cur.fetchone()
        assert (n, float(mx)) == (3, 5.0)
        await store.conn.commit()
    run(go())


def test_silence_check_and_outbox(store):
    async def go():
        await store.connect()
        await store.process({"S0001": [r(4.0, 7)], "S0002": [r(4.0, 1, "S0002", "F0002")]}, [])
        silent = await store.mark_silent(300)
        assert [(a.sensor_id, a.kind) for a in silent] == [("S0001", "offline")]
        assert await store.mark_silent(300) == []              # already offline: no repeat
        pending = await store.unpublished()
        assert len(pending) == 1
        await store.mark_published([p[0] for p in pending])
        assert await store.unpublished() == []
        back = await store.process({"S0001": [r(4.1, 0)]}, [])
        assert [a.kind for a in back] == ["back_online"]
        counts, temps = await store.summary()
        assert sorted(counts) == [("online", "ok", 2)] and len(temps) == 2
    run(go())


def test_last_will_marks_offline(store):
    async def go():
        await store.connect()
        await store.process({"S0001": [r(4.0, 2)]}, [])
        lw = StatusEvent("S0001", "F0001", "offline", NOW)
        alerts = await store.process({"S0001": [lw]}, [])
        assert [a.kind for a in alerts] == ["offline"]
        cur = await store.conn.execute("SELECT status FROM sensor_status")
        assert (await cur.fetchone())[0] == "offline"
        await store.conn.commit()
    run(go())


def test_lease_only_one_holder(store):
    async def go():
        await store.connect()
        other = Store(DSN)
        await other.connect()
        assert await store.take_lease("sensor_watch", "agg-1", 30) is True
        assert await other.take_lease("sensor_watch", "agg-2", 30) is False
        assert await store.take_lease("sensor_watch", "agg-1", 30) is True    # renew
        await store.conn.execute("UPDATE aggregator_lease SET expires_at = now() - interval '1 s'")
        await store.conn.commit()
        assert await other.take_lease("sensor_watch", "agg-2", 30) is True    # takeover
    run(go())
