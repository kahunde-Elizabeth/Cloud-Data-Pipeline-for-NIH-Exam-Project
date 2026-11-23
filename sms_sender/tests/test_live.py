"""SMS sender against real PostgreSQL (same protocol as CockroachDB) and the real mock gateway.
Skipped unless NIDH_PG_DSN points at an empty PostgreSQL database."""
import asyncio
import importlib.util
import os
import re
import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer

import pytest

psycopg = pytest.importorskip("psycopg")
httpx = pytest.importorskip("httpx")
from cryptography.fernet import Fernet  # noqa: E402

DSN = os.getenv("NIDH_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="NIDH_PG_DSN not set")

from app import main as sender, rules        # noqa: E402
from app.store import Store                  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
TABLES = ("facilities", "children", "vaccination_records", "sms_messages", "aggregator_lease")
KEY = Fernet.generate_key()
NOW = datetime.now(timezone.utc)


def pg_schema() -> str:
    sql = re.sub(r"--[^\n]*", "", open(os.path.join(ROOT, "db", "schema.sql")).read())
    out = []
    for stmt in sql.split(";"):
        s = stmt.strip()
        if not any(re.search(rf"\b(TABLE|ON)\s+(IF NOT EXISTS\s+)?{t}\b", s) for t in TABLES):
            continue
        s = re.sub(r"\bSTRING\b", "TEXT", s)
        s = re.sub(r"\bBYTES\b", "BYTEA", s)
        s = re.sub(r",\s*INDEX [^\n]*?\)(\s*USING HASH)?(?=\s*(,|\)\s*$))", "", s)
        out.append(s)
    return ";\n".join(out) + ";"


def load_gateway():
    spec = importlib.util.spec_from_file_location(
        "sms_gateway_main", os.path.join(ROOT, "sms_gateway", "app", "main.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


GW = load_gateway()


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PHONE_KEY", KEY.decode())
    GW.FAIL_RATE, GW.SLOW_RATE = 0.0, 0.0
    GW.GATEWAY = GW.Gateway(str(tmp_path / "sent.log"))
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    server = ThreadingHTTPServer(("127.0.0.1", port), GW.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(sender, "GATEWAY", f"http://127.0.0.1:{port}")

    async def setup():
        conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
        await conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                           "WHERE datname = current_database() AND pid <> pg_backend_pid()")
        for t in reversed(TABLES):
            await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        await conn.execute(pg_schema())
        await conn.execute("INSERT INTO facilities (facility_id, district_id, name, contact_phone) "
                           "VALUES ('F0001','D001','x','SYN-0700000001')")
        f = Fernet(KEY)
        for child, consent in (("SYN-0000000001", True), ("SYN-0000000002", False)):
            await conn.execute(
                "INSERT INTO children (child_id, home_facility, caregiver_phone, sms_consent) "
                "VALUES (%s, 'F0001', %s, %s)", (child, f.encrypt(b"SYN-0712345678"), consent))
            await conn.execute(
                "INSERT INTO vaccination_records (record_id, child_id, facility_id, district_id, "
                "vaccine_code, dose_no, device_ts, source) VALUES (%s, %s, 'F0001', 'D001', "
                "'PENTA', 1, %s, 'tablet')", (uuid.uuid4(), child, NOW - timedelta(days=26)))
        await conn.close()
        st = Store(DSN)
        await st.connect()
        return st
    st = asyncio.run(setup())
    yield st, GW.GATEWAY
    server.shutdown()


ALERT = {"alert_id": str(uuid.uuid4()), "sensor_id": "S0001", "facility_id": "F0001",
         "kind": "too_warm", "detail": "9.1 C", "temp_c": 9.1, "at": NOW.isoformat()}


async def send_all(st):
    async with httpx.AsyncClient(timeout=5) as client:
        await sender.send_batch(st, client, await st.due_messages(50))


async def rows(st, sql="SELECT kind, status, attempts, cost_ugx FROM sms_messages ORDER BY kind"):
    cur = await st.conn.execute(sql)
    out = await cur.fetchall()
    await st.conn.commit()
    return out


def test_alert_queued_once_and_sent_once(env):
    st, gw = env
    async def go():
        await st.connect()
        await st.queue_alerts([ALERT]); await st.queue_alerts([ALERT])     # outbox repeat
        await send_all(st)
        assert await rows(st) == [("fridge_alert", "sent", 1, 30)]
        sent = gw.recent(10, None)
        assert len(sent) == 1 and sent[0]["to"] == "SYN-0700000001" and "9.1 C" in sent[0]["body"]
    asyncio.run(go())


def test_reminder_only_with_consent_once_and_decrypted(env):
    st, gw = env
    async def go():
        await st.connect()
        assert await st.queue_reminders(datetime.now(timezone.utc), 9000) == (1, 0)
        assert await st.queue_reminders(datetime.now(timezone.utc), 9000) == (0, 0)  # once
        await send_all(st)
        assert await rows(st) == [("dose_reminder", "sent", 1, 30)]
        sms = gw.recent(10, "SYN-0712345678")
        assert len(sms) == 1 and "PENTA dose 2" in sms[0]["body"] and "...0001" in sms[0]["body"]
        cur = await st.conn.execute("SELECT recipient FROM sms_messages")
        assert b"0712345678" not in bytes((await cur.fetchone())[0])     # stored encrypted
        await st.conn.commit()
    asyncio.run(go())


def test_monthly_cap(env):
    st, _ = env
    async def go():
        await st.connect()
        assert await st.queue_reminders(datetime.now(timezone.utc), 0) == (0, 1)
        assert await rows(st) == [("dose_reminder", "capped", 0, 0)]
        await send_all(st)                                  # capped is never sent
        assert await rows(st) == [("dose_reminder", "capped", 0, 0)]
    asyncio.run(go())


def test_outage_then_recovery_without_double_send(env):
    st, gw = env
    async def go():
        await st.connect()
        await st.queue_alerts([ALERT])
        gw.outage_until = 1e12                               # provider down
        await send_all(st)
        kind, status, attempts, cost = (await rows(st))[0]
        assert (status, attempts, cost) == ("queued", 1, 0)
        cur = await st.conn.execute("SELECT next_attempt_at > now(), last_error FROM sms_messages")
        later, err = await cur.fetchone(); await st.conn.commit()
        assert later and "503" in err                       # backoff: not retried at once
        gw.outage_until = 0
        await st.conn.execute("UPDATE sms_messages SET next_attempt_at = now()"); await st.conn.commit()
        await send_all(st)
        assert (await rows(st))[0][1:] == ("sent", 2, 30)
        # crash after the gateway accepted but before our update: the retry is not re-sent
        await st.conn.execute("UPDATE sms_messages SET status = 'queued'"); await st.conn.commit()
        await send_all(st)
        assert len(gw.recent(10, None)) == 1 and (await rows(st))[0][3] == 0   # no new charge
    asyncio.run(go())


def test_expiry_and_cancel(env):
    st, _ = env
    async def go():
        await st.connect()
        await st.queue_alerts([dict(ALERT, at=(NOW - timedelta(hours=3)).isoformat())])
        await st.queue_reminders(datetime.now(timezone.utc), 9000)
        await st.conn.execute(                              # the child came in anyway
            "INSERT INTO vaccination_records (record_id, child_id, facility_id, district_id, "
            "vaccine_code, dose_no, device_ts, source) VALUES (%s, 'SYN-0000000001', 'F0001', "
            "'D001', 'PENTA', 2, now(), 'tablet')", (uuid.uuid4(),))
        await st.conn.commit()
        await send_all(st)
        assert await rows(st) == [("dose_reminder", "cancelled", 0, 0),
                                  ("fridge_alert", "failed", 0, 0)]
    asyncio.run(go())


def test_gateway_restart_remembers_sent_ids(tmp_path):
    g = GW.Gateway(str(tmp_path / "log"))
    GW.FAIL_RATE = GW.SLOW_RATE = 0.0
    mid = str(uuid.uuid4())
    assert g.send(mid, "SYN-0700000001", "hi", "x")[1]["duplicate"] is False
    again = GW.Gateway(str(tmp_path / "log"))               # after a restart
    assert again.send(mid, "SYN-0700000001", "hi", "x")[1]["duplicate"] is True
