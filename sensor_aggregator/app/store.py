"""CockroachDB access for the sensor aggregator."""
from __future__ import annotations

import asyncio
import logging
import random

import psycopg
from psycopg import errors

from .rules import Alert, State, alert_id, apply

log = logging.getLogger("sensor_aggregator")

LOAD_STATES = """
SELECT sensor_id, status, temp_state, last_seen_at, last_temp_c, facility_id
FROM sensor_status WHERE sensor_id = ANY(%s)
"""
SAVE_STATE = """
INSERT INTO sensor_status (sensor_id, facility_id, last_seen_at, status, temp_state, last_temp_c)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id) DO UPDATE SET
    facility_id = excluded.facility_id, last_seen_at = excluded.last_seen_at,
    status = excluded.status, temp_state = excluded.temp_state, last_temp_c = excluded.last_temp_c
"""
SAVE_ALERT = """
INSERT INTO alerts (alert_id, sensor_id, facility_id, kind, detail, temp_c, at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (alert_id) DO NOTHING
"""
# A window is only replaced by one with MORE readings, so a partial window rebuilt after a
# crash or a rebalance can never overwrite a complete one.
SAVE_WINDOW = """
INSERT INTO sensor_readings_1min
    (sensor_id, window_start, facility_id, min_c, avg_c, max_c, n_readings, out_of_range)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id, window_start) DO UPDATE SET
    min_c = excluded.min_c, avg_c = excluded.avg_c, max_c = excluded.max_c,
    n_readings = excluded.n_readings, out_of_range = excluded.out_of_range
WHERE excluded.n_readings > sensor_readings_1min.n_readings
"""
MARK_SILENT = """
UPDATE sensor_status SET status = 'offline'
WHERE status = 'online' AND last_seen_at < now() - %s * INTERVAL '1 second'
RETURNING sensor_id, facility_id, last_seen_at, last_temp_c
"""
LEASE_SQL = """
INSERT INTO aggregator_lease (lease_name, holder, expires_at)
VALUES (%(name)s, %(me)s, now() + %(ttl)s::INT * INTERVAL '1 second')
ON CONFLICT (lease_name) DO UPDATE
    SET holder = excluded.holder, expires_at = excluded.expires_at
    WHERE aggregator_lease.holder = excluded.holder OR aggregator_lease.expires_at < now()
RETURNING holder
"""


class Store:
    def __init__(self, dsn: str):
        self.dsn, self.conn, self.facilities = dsn, None, set()

    async def connect(self):
        while True:
            try:
                self.conn = await psycopg.AsyncConnection.connect(self.dsn, connect_timeout=5)
                cur = await self.conn.execute("SELECT facility_id FROM facilities")
                self.facilities = {r[0] for r in await cur.fetchall()}
                await self.conn.commit()
                log.info("connected to the database (%d facilities)", len(self.facilities))
                return
            except psycopg.Error as exc:
                log.warning("database not ready (%s); retrying in 3 s", exc)
                await asyncio.sleep(3)

    async def _retry(self, fn, *args):
        """Run fn in one serializable transaction; retry on conflicts (40001) or a lost node."""
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

    # ------------------------------------------------------------ consumer side
    async def process(self, events_by_sensor: dict, window_rows: list) -> list[Alert]:
        """Read the sensors' state, apply the rules, and save state + alerts + finished windows,
        all in ONE transaction. Reading and writing together means a concurrent silence check
        cannot be overwritten: one of the two transactions is retried."""
        return await self._retry(self._process, events_by_sensor, window_rows)

    async def _process(self, events_by_sensor, window_rows):
        alerts: list[Alert] = []
        async with self.conn.cursor() as cur:
            if events_by_sensor:
                await cur.execute(LOAD_STATES, (list(events_by_sensor),))
                states = {r[0]: State(r[1], r[2], r[3], float(r[4]) if r[4] is not None else None,
                                      r[5]) for r in await cur.fetchall()}
                rows = []
                for sensor, events in events_by_sensor.items():
                    state, new = apply(states.get(sensor, State()), events)
                    alerts += new
                    if state.last_seen_at is None:          # only status messages so far
                        state.last_seen_at = events[-1].received_at
                    rows.append((sensor, state.facility_id, state.last_seen_at, state.status,
                                 state.temp_state, state.last_temp_c))
                await cur.executemany(SAVE_STATE, rows)
                if alerts:
                    await cur.executemany(SAVE_ALERT, [(a.alert_id, a.sensor_id, a.facility_id,
                                                        a.kind, a.detail, a.temp_c, a.at)
                                                       for a in alerts])
            if window_rows:
                await cur.executemany(SAVE_WINDOW, window_rows)
        return alerts

    # ------------------------------------------------------------ watcher side (lease holder)
    async def take_lease(self, name: str, me: str, ttl: int) -> bool:
        async def go():
            cur = await self.conn.execute(LEASE_SQL, {"name": name, "me": me, "ttl": ttl})
            return (await cur.fetchone()) is not None
        return await self._retry(go)

    async def mark_silent(self, silence_s: int) -> list[Alert]:
        async def go():
            cur = await self.conn.execute(MARK_SILENT, (silence_s,))
            alerts = []
            for sensor, facility, last_seen, temp in await cur.fetchall():
                minutes = silence_s // 60
                alerts.append(Alert(alert_id(sensor, "offline", last_seen), sensor, facility,
                                    "offline", f"no reading for {minutes} minutes",
                                    float(temp) if temp is not None else None, last_seen))
            if alerts:
                async with self.conn.cursor() as c:
                    await c.executemany(SAVE_ALERT, [
                        (a.alert_id, a.sensor_id, a.facility_id, a.kind, a.detail, a.temp_c, a.at)
                        for a in alerts])
            return alerts
        return await self._retry(go)

    async def unpublished(self, limit: int = 500):
        async def go():
            cur = await self.conn.execute(
                "SELECT alert_id, sensor_id, facility_id, kind, detail, temp_c, at FROM alerts "
                "WHERE NOT published ORDER BY created_at LIMIT %s", (limit,))
            return await cur.fetchall()
        return await self._retry(go)

    async def mark_published(self, ids) -> None:
        async def go():
            await self.conn.execute("UPDATE alerts SET published = true WHERE alert_id = ANY(%s)",
                                    (list(ids),))
        await self._retry(go)

    async def summary(self):
        async def go():
            cur = await self.conn.execute(
                "SELECT status, temp_state, count(*) FROM sensor_status GROUP BY 1, 2")
            counts = await cur.fetchall()
            cur = await self.conn.execute(
                "SELECT sensor_id, last_temp_c FROM sensor_status WHERE last_temp_c IS NOT NULL")
            return counts, await cur.fetchall()
        return await self._retry(go)
