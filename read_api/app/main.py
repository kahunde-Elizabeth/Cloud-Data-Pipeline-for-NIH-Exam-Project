"""NIDH read API: what dashboards call.

Two roles (from the `users` table):
  district_officer  -> may read only their own district
  national_analyst  -> may read any district and the national summary

Every request by a known user is written to `audit_log` BEFORE any data is returned,
including refused requests (DPPA 2019: accountability for who accessed personal-data systems).
If the audit row cannot be written, no data is returned (503).

Answers come from the cache first (filled every 15 s by the aggregator). On a cache miss,
or if the cache is down, the API reads `district_coverage` and refills the cache.

Prototype note: the user is identified by the X-User header. A real deployment would use the
government single sign-on (OIDC) and take the username from a verified token instead.
"""
from __future__ import annotations

import json
import logging
import os
import re
import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app

from .payloads import build_payloads

DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
CACHE_URL = os.getenv("CACHE_URL", "redis://cache:6379/0")
CACHE_TTL_S = int(os.getenv("CACHE_TTL_S", "45"))
INSTANCE = os.getenv("INSTANCE_NAME", socket.gethostname())
DISTRICT_RE = re.compile(r"^D\d{3}$")
WEEK_RE = re.compile(r"^\d{4}-W\d{2}$")

log = logging.getLogger("read_api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s read-api %(message)s")

REQUESTS = Counter("read_requests_total", "Read requests", ["endpoint", "outcome"])
CACHE = Counter("read_cache_total", "Cache lookups", ["result"])      # hit / miss / error
LATENCY = Histogram("read_request_seconds", "Time to answer a read", ["endpoint"],
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2))


# ---------------------------------------------------------------- data access
class Store:
    """CockroachDB access: users, audit_log and district_coverage."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def open(self):
        from psycopg_pool import AsyncConnectionPool
        self.pool = AsyncConnectionPool(self.dsn, min_size=1, max_size=4, open=False, timeout=5,
                                        kwargs={"autocommit": True, "connect_timeout": 5})
        await self.pool.open(wait=False)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def user(self, username: str):
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT user_id, role, district_id FROM users WHERE username = %s", (username,))
            row = await cur.fetchone()
        return None if row is None else {"user_id": row[0], "role": row[1], "district_id": row[2]}

    async def audit(self, user_id, action: str, resource: str, params: dict):
        from psycopg.types.json import Jsonb
        async with self.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO audit_log (user_id, action, resource, params) VALUES (%s, %s, %s, %s)",
                (user_id, action, resource, Jsonb(params)))

    async def coverage(self, district_id: str | None):
        sql = "SELECT district_id, vaccine_code, dose_no, iso_week, doses FROM district_coverage"
        args: tuple = ()
        if district_id:
            sql += " WHERE district_id = %s"
            args = (district_id,)
        async with self.pool.connection() as conn:
            return await (await conn.execute(sql, args)).fetchall()


class Cache:
    def __init__(self, url: str):
        import redis.asyncio as aioredis
        self.r = aioredis.from_url(url, socket_timeout=0.5, socket_connect_timeout=0.5)

    async def get(self, key):
        raw = await self.r.get(key)
        return None if raw is None else json.loads(raw)

    async def set(self, key, value):
        await self.r.set(key, json.dumps(value), ex=CACHE_TTL_S)

    async def close(self):
        await self.r.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not hasattr(app.state, "store"):           # tests inject fakes
        app.state.store = Store(DSN)
        await app.state.store.open()
        app.state.cache = Cache(CACHE_URL)
    log.info("%s ready", INSTANCE)
    yield
    await app.state.store.close()
    await app.state.cache.close()


app = FastAPI(title="NIDH read API", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    return JSONResponse({"status": "error", "detail": exc.detail, "instance": INSTANCE},
                        status_code=exc.status_code, headers=exc.headers)


# ---------------------------------------------------------------- helpers
async def who(request: Request, username: str | None):
    if not username:
        raise HTTPException(401, "missing X-User header")
    try:
        user = await request.app.state.store.user(username)
    except Exception as exc:
        log.warning("user lookup failed: %s", exc)
        raise HTTPException(503, "database unavailable", headers={"Retry-After": "5"})
    if user is None:
        raise HTTPException(401, "unknown user")
    return user


async def audit_or_fail(request: Request, user, action: str, resource: str, params: dict):
    try:
        await request.app.state.store.audit(user["user_id"], action, resource, params)
    except Exception as exc:
        log.error("audit write failed, refusing to return data: %s", exc)
        raise HTTPException(503, "audit log unavailable; no data returned",
                            headers={"Retry-After": "5"})


async def cached(request: Request, key: str, district_id: str | None):
    """Cache first; on a miss (or cache error) read the summary table and refill the cache."""
    cache = request.app.state.cache
    try:
        value = await cache.get(key)
        if value is not None:
            CACHE.labels(result="hit").inc()
            return value, "cache"
        CACHE.labels(result="miss").inc()
    except Exception as exc:
        CACHE.labels(result="error").inc()
        log.warning("cache unavailable (%s); reading the database", exc)
        cache = None

    try:
        rows = await request.app.state.store.coverage(district_id)
    except Exception as exc:
        log.warning("coverage read failed: %s", exc)
        raise HTTPException(503, "database unavailable", headers={"Retry-After": "5"})
    districts, national, _ = build_payloads(rows, datetime.now(timezone.utc))
    if district_id:
        value = districts.get(district_id) or {
            "district_id": district_id, "updated_at": national["updated_at"],
            "total_doses": 0, "rows": []}
    else:
        value = national
    if cache is not None:
        try:
            await cache.set(key, value)
        except Exception:
            CACHE.labels(result="error").inc()
    return value, "database"


def by_week(value: dict, week: str | None) -> dict:
    if not week:
        return value
    rows = [r for r in value["rows"] if r["week"] == week]
    return dict(value, rows=rows, total_doses=sum(r["doses"] for r in rows))


# ---------------------------------------------------------------- endpoints
@app.get("/v1/coverage/districts/{district_id}")
async def district(request: Request, district_id: str,
                   week: str | None = Query(None, description="ISO week, e.g. 2026-W47"),
                   x_user: str | None = Header(None)):
    started = time.perf_counter()
    if not DISTRICT_RE.match(district_id) or (week and not WEEK_RE.match(week)):
        REQUESTS.labels(endpoint="district", outcome="bad_request").inc()
        raise HTTPException(400, "district must look like D001 and week like 2026-W47")
    user = await who(request, x_user)
    resource = f"coverage/district/{district_id}"
    params = {"week": week, "instance": INSTANCE}

    if user["role"] == "district_officer" and user["district_id"] != district_id:
        await audit_or_fail(request, user, "read_denied", resource, params)
        REQUESTS.labels(endpoint="district", outcome="denied").inc()
        raise HTTPException(403, f"a district officer may read only {user['district_id']}")

    await audit_or_fail(request, user, "read", resource, params)
    value, source = await cached(request, f"cov:district:{district_id}", district_id)
    REQUESTS.labels(endpoint="district", outcome="ok").inc()
    LATENCY.labels(endpoint="district").observe(time.perf_counter() - started)
    return dict(by_week(value, week), source=source, instance=INSTANCE)


@app.get("/v1/coverage/national")
async def national(request: Request,
                   week: str | None = Query(None, description="ISO week, e.g. 2026-W47"),
                   x_user: str | None = Header(None)):
    started = time.perf_counter()
    if week and not WEEK_RE.match(week):
        REQUESTS.labels(endpoint="national", outcome="bad_request").inc()
        raise HTTPException(400, "week must look like 2026-W47")
    user = await who(request, x_user)
    params = {"week": week, "instance": INSTANCE}

    if user["role"] != "national_analyst":
        await audit_or_fail(request, user, "read_denied", "coverage/national", params)
        REQUESTS.labels(endpoint="national", outcome="denied").inc()
        raise HTTPException(403, "only a national analyst may read the national summary")

    await audit_or_fail(request, user, "read", "coverage/national", params)
    value, source = await cached(request, "cov:national", None)
    REQUESTS.labels(endpoint="national", outcome="ok").inc()
    LATENCY.labels(endpoint="national").observe(time.perf_counter() - started)
    return dict(by_week(value, week), source=source, instance=INSTANCE)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "instance": INSTANCE}
