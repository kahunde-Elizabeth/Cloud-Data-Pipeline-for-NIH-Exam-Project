"""Database side of the processor: one transaction per batch, safe to repeat."""
from __future__ import annotations

import asyncio
import os
import json
import logging
import random
import uuid

import psycopg
from psycopg import errors
from psycopg.types.json import Jsonb

from .validate import Row

log = logging.getLogger("processor.store")

INSERT_COLS = ("record_id", "child_id", "facility_id", "district_id", "vaccine_code", "dose_no",
               "batch_no", "device_ts", "received_at", "clock_skew_flag", "skew_seconds", "source",
               "administered_at")


class Reject:
    def __init__(self, reject_id: uuid.UUID, record_id: uuid.UUID | None, reason: str,
                 detail: str, raw: dict | str):
        self.reject_id, self.record_id, self.reason = reject_id, record_id, reason
        self.detail, self.raw = detail, raw

    def as_message(self) -> dict:
        return {"reject_id": str(self.reject_id),
                "record_id": str(self.record_id) if self.record_id else None,
                "reason": self.reason, "detail": self.detail, "raw": self.raw}



def encrypt_phone(phone):
    """The caregiver's phone is stored encrypted (M1 data model); only the SMS sender holds the
    key to read it. Synthetic numbers only in this prototype."""
    if not phone:
        return None
    return _fernet().encrypt(phone.encode())


_FERNET = None


def _fernet():
    global _FERNET
    if _FERNET is None:
        from cryptography.fernet import Fernet
        key = os.getenv("PHONE_KEY")
        if not key:
            raise RuntimeError("PHONE_KEY is not set")
        _FERNET = Fernet(key.encode())
    return _FERNET


class Store:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn: psycopg.AsyncConnection | None = None
        self.facilities: set[str] = set()

    async def connect(self) -> None:
        """Connect to any live CockroachDB node (the DSN lists all three), retrying."""
        attempt = 0
        while True:
            attempt += 1
            try:
                self.conn = await psycopg.AsyncConnection.connect(self.dsn, connect_timeout=5)
                async with self.conn.cursor() as cur:
                    await cur.execute("SELECT facility_id FROM facilities")
                    self.facilities = {r[0] for r in await cur.fetchall()}
                await self.conn.commit()
                log.info("connected to CockroachDB; %d facilities loaded", len(self.facilities))
                return
            except Exception as exc:
                log.warning("database not ready (%s); retry %d", exc, attempt)
                await asyncio.sleep(min(2 * attempt, 10))

    async def write(self, rows: list[Row], rejects: list[Reject],
                    offset_ids: dict[uuid.UUID, uuid.UUID]) -> tuple[set, int, list[Reject]]:
        """Insert a batch in ONE transaction.

        Returns (record_ids_inserted, retries_skipped, duplicate_dose_rejects).
        Retries automatically on serialization conflicts (SQLSTATE 40001) and on lost
        connections (a node was killed: reconnect to another node and try again).
        Any other database error is raised so the caller can isolate the bad record.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                return await self._write_once(rows, rejects, offset_ids)
            except errors.SerializationFailure:
                await self.conn.rollback()
                wait = min(0.05 * 2 ** attempt, 2) * random.random()
                log.info("serialization conflict (40001); retry %d after %.2fs", attempt, wait)
                await asyncio.sleep(wait)
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                log.warning("lost database connection (%s); reconnecting", exc)
                try:
                    await self.conn.close()
                except Exception:
                    pass
                await self.connect()

    async def _write_once(self, rows, rejects, offset_ids):
        dup_rejects: list[Reject] = []
        stored: set = set()
        retries = 0
        async with self.conn.cursor() as cur:
            if rows:
                # children first (the foreign key needs them); existing children are left alone
                children = {r.child_id: r for r in rows}
                await cur.executemany(
                    "INSERT INTO children (child_id, birth_date, home_facility, caregiver_phone, "
                    "sms_consent) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (child_id) DO NOTHING",
                    [(c.child_id, c.child_birth_date, c.facility_id,
                      encrypt_phone(c.caregiver_phone), c.sms_consent) for c in children.values()])

                placeholders = ", ".join(["(" + ", ".join(["%s"] * len(INSERT_COLS)) + ")"] * len(rows))
                params = [getattr(r, col) for r in rows for col in INSERT_COLS]
                await cur.execute(
                    f"INSERT INTO vaccination_records ({', '.join(INSERT_COLS)}) VALUES {placeholders} "
                    "ON CONFLICT DO NOTHING RETURNING record_id", params)
                stored = {r[0] for r in await cur.fetchall()}

                # Rows not stored hit a unique rule. Which one?
                missing = [r for r in rows if r.record_id not in stored]
                if missing:
                    await cur.execute("SELECT record_id FROM vaccination_records WHERE record_id = ANY(%s)",
                                      [[r.record_id for r in missing]])
                    already = {r[0] for r in await cur.fetchall()}
                    for r in missing:
                        if r.record_id in already:
                            retries += 1                    # same record sent again: harmless
                        else:                               # new record_id, same child + vaccine + dose
                            dup_rejects.append(Reject(
                                offset_ids[r.record_id], r.record_id, "duplicate_dose",
                                f"{r.child_id} already has {r.vaccine_code} dose {r.dose_no}",
                                {"child_id": r.child_id, "vaccine_code": r.vaccine_code,
                                 "dose_no": r.dose_no, "facility_id": r.facility_id}))

            all_rejects = rejects + dup_rejects
            if all_rejects:
                await cur.executemany(
                    "INSERT INTO rejects (reject_id, record_id, reason, detail, raw_payload) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (reject_id) DO NOTHING",
                    [(j.reject_id, j.record_id, j.reason, j.detail, Jsonb(json.loads(json.dumps(j.raw, default=str))))
                     for j in all_rejects])
        await self.conn.commit()
        return stored, retries, dup_rejects
