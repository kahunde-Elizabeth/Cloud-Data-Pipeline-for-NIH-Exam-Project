"""CockroachDB access for the SMS sender."""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone

import psycopg
from psycopg import errors

from . import rules

log = logging.getLogger("sms_sender")

EVENT_TS = ("CASE WHEN v.clock_skew_flag AND v.skew_seconds IS NOT NULL "
            "THEN v.device_ts - v.skew_seconds * INTERVAL '1 second' ELSE v.device_ts END")

# Each consenting child's latest dose of each scheduled vaccine.
LATEST_DOSES = f"""
SELECT v.child_id, v.vaccine_code, max(v.dose_no), max({EVENT_TS}), c.birth_date, c.home_facility
FROM vaccination_records AS v JOIN children AS c ON c.child_id = v.child_id
WHERE c.sms_consent AND c.caregiver_phone IS NOT NULL
  AND v.vaccine_code IN ('OPV', 'PENTA', 'PCV', 'ROTA', 'MR')
GROUP BY v.child_id, v.vaccine_code, c.birth_date, c.home_facility
"""

QUEUE_ALERT = """
INSERT INTO sms_messages (message_id, kind, ref, facility_id, recipient_plain, body, expires_at)
SELECT %s, 'fridge_alert', %s, f.facility_id, f.contact_phone, %s,
       %s::TIMESTAMPTZ + %s::INT * INTERVAL '1 second'
FROM facilities AS f WHERE f.facility_id = %s AND f.contact_phone IS NOT NULL
ON CONFLICT (message_id) DO NOTHING
"""

QUEUE_REMINDER = """
INSERT INTO sms_messages (message_id, kind, ref, facility_id, child_id, recipient, body, status,
                          expires_at)
SELECT %s, 'dose_reminder', %s, %s, c.child_id, c.caregiver_phone, %s, %s,
       now() + %s::INT * INTERVAL '1 second'
FROM children AS c WHERE c.child_id = %s
ON CONFLICT (message_id) DO NOTHING
RETURNING message_id
"""

DUE_NOW = """
SELECT message_id, kind, ref, child_id, recipient, recipient_plain, body, attempts, expires_at
FROM sms_messages
WHERE status = 'queued' AND next_attempt_at <= now()
ORDER BY kind = 'fridge_alert' DESC, next_attempt_at
LIMIT %s
"""


class Store:
    def __init__(self, dsn: str):
        self.dsn, self.conn = dsn, None

    async def connect(self):
        while True:
            try:
                self.conn = await psycopg.AsyncConnection.connect(self.dsn, connect_timeout=5)
                return
            except psycopg.Error as exc:
                log.warning("database not ready (%s); retrying in 3 s", exc)
                await asyncio.sleep(3)

    async def _retry(self, fn, *args):
        for attempt in range(10):
            try:
                result = await fn(*args)
                await self.conn.commit()
                return result
            except errors.SerializationFailure:
                await self.conn.rollback()
                await asyncio.sleep(min(2.0, 0.05 * 2 ** attempt) * random.random())
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                log.warning("database connection lost (%s); reconnecting", exc)
                try:
                    await self.conn.close()
                except Exception:
                    pass
                await self.connect()
        raise RuntimeError("database transaction kept failing")

    # ------------------------------------------------------------ alerts -> queue
    async def queue_alerts(self, alerts: list[dict]) -> None:
        async def go():
            async with self.conn.cursor() as cur:
                await cur.executemany(QUEUE_ALERT, [
                    (rules.alert_sms_id(a["alert_id"]), a["alert_id"], rules.alert_body(a),
                     a["at"], int(rules.ALERT_LIFETIME.total_seconds()), a["facility_id"])
                    for a in alerts])
        await self._retry(go)

    # ------------------------------------------------------------ reminder job
    async def queue_reminders(self, now: datetime, cap: int):
        """Find doses due now and queue one reminder each, under the monthly cap.
        Returns (queued, capped)."""
        async def go():
            cur = await self.conn.execute(LATEST_DOSES)
            today = now.astimezone(rules.EAT).date()
            due = []
            for child, vaccine, last_dose, last_at, birth, facility in await cur.fetchall():
                d = rules.next_due(child, vaccine, int(last_dose), last_at, birth, facility)
                if d is not None and rules.in_window(d, today):
                    due.append(d)
            if not due:
                return 0, 0
            ids = [rules.reminder_id(d) for d in due]
            cur = await self.conn.execute(
                "SELECT message_id FROM sms_messages WHERE message_id = ANY(%s)", (ids,))
            known = {r[0] for r in await cur.fetchall()}
            new = [d for d in due if rules.reminder_id(d) not in known]
            cur = await self.conn.execute(
                "SELECT count(*) FROM sms_messages WHERE kind = 'dose_reminder' "
                "AND status != 'capped' AND created_at >= %s", (rules.month_start(now),))
            used = (await cur.fetchone())[0]
            fits, over = rules.split_by_cap(new, used, cap)
            lifetime = int(rules.REMINDER_LIFETIME.total_seconds())
            async with self.conn.cursor() as c:
                for status, group in (("queued", fits), ("capped", over)):
                    for d in group:
                        await c.execute(QUEUE_REMINDER, (
                            rules.reminder_id(d), f"{d.child_id}:{d.vaccine}:{d.dose}",
                            d.facility_id, rules.reminder_body(d), status, lifetime, d.child_id))
            return len(fits), len(over)
        return await self._retry(go)

    # ------------------------------------------------------------ sending
    async def due_messages(self, limit: int):
        async def go():
            cur = await self.conn.execute(DUE_NOW, (limit,))
            return await cur.fetchall()
        return await self._retry(go)

    async def dose_given(self, ref: str) -> bool:
        child, vaccine, dose = ref.rsplit(":", 2)
        async def go():
            cur = await self.conn.execute(
                "SELECT 1 FROM vaccination_records WHERE child_id = %s AND vaccine_code = %s "
                "AND dose_no >= %s LIMIT 1", (child, vaccine, int(dose)))
            return (await cur.fetchone()) is not None
        return await self._retry(go)

    async def finish(self, message_id, status: str, error: str | None = None) -> None:
        async def go():
            await self.conn.execute(
                "UPDATE sms_messages SET status = %s, last_error = coalesce(%s, last_error) "
                "WHERE message_id = %s AND status = 'queued'", (status, error, message_id))
        await self._retry(go)

    async def mark_sent(self, message_id, provider_id: str, cost: int) -> None:
        async def go():
            await self.conn.execute(
                "UPDATE sms_messages SET status = 'sent', provider_id = %s, cost_ugx = %s, "
                "sent_at = now(), attempts = attempts + 1 WHERE message_id = %s",
                (provider_id, cost, message_id))
        await self._retry(go)

    async def mark_retry(self, message_id, attempts: int, kind: str, error: str) -> None:
        wait = rules.backoff(kind, attempts + 1)
        async def go():
            await self.conn.execute(
                "UPDATE sms_messages SET attempts = attempts + 1, last_error = %s, "
                "next_attempt_at = now() + %s::FLOAT * INTERVAL '1 second' "
                "WHERE message_id = %s AND status = 'queued'",
                (error[:200], wait.total_seconds(), message_id))
        await self._retry(go)

    # ------------------------------------------------------------ dashboard numbers
    async def month_summary(self, now: datetime):
        async def go():
            cur = await self.conn.execute(
                "SELECT kind, status, count(*), coalesce(sum(cost_ugx), 0) FROM sms_messages "
                "WHERE created_at >= %s GROUP BY 1, 2", (rules.month_start(now),))
            return await cur.fetchall()
        return await self._retry(go)

    async def take_lease(self, name: str, me: str, ttl: int) -> bool:
        async def go():
            cur = await self.conn.execute(
                "INSERT INTO aggregator_lease (lease_name, holder, expires_at) "
                "VALUES (%(n)s, %(me)s, now() + %(ttl)s::INT * INTERVAL '1 second') "
                "ON CONFLICT (lease_name) DO UPDATE SET holder = excluded.holder, "
                "expires_at = excluded.expires_at WHERE aggregator_lease.holder = excluded.holder "
                "OR aggregator_lease.expires_at < now() RETURNING holder",
                {"n": name, "me": me, "ttl": ttl})
            return (await cur.fetchone()) is not None
        return await self._retry(go)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
