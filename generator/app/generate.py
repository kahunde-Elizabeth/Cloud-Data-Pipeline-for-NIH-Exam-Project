"""NIDH synthetic load generator (Annex A3 profiles, Annex B data, Annex C logs).

Plays 2,240 / k tablets. Records are created at the profile's rate and handed to a random
tablet (20% of tablets belong to the hot district D001, so about 20% of records target one
district). Each tablet behaves like the real app:
  * keeps its records until the API answers 200 (the acknowledgement point), then deletes them;
  * syncs every 15-45 s, up to 500 records per batch; a backlog goes out in several batches;
  * some tablets go offline for 2-10 minutes and upload their backlog when they return;
  * a few tablets have a wrong clock (10 min to 3 h fast or slow);
  * on 429 waits Retry-After + 0-10 s; on 503 / errors waits 1, 2, 4 ... 60 s with jitter,
    at most 8 tries, then keeps the batch for the next sync (the same batch_id and records).
  * E6: a share of acknowledged batches is re-sent once (--resend-share), and one 500-record
    batch is replayed 3 times (--replay-big).

Writes to results/<run_id>/:
  sent.log      every record_id attempted: time, record_id, batch_id, attempt, device
  ack.log       every record_id whose batch got 200: time, record_id, batch_id, instance, ms
  timeline.csv  per second: target rate, records created, sent, acknowledged, errors
  summary.json  totals, latency p50 / p95 / p99, throughput, HTTP status counts
After the run it keeps retrying unacknowledged records for up to --drain-s seconds.

Usage (from the repo folder):
  docker compose run --rm generator e1 --run-id e1-run1
  docker compose run --rm generator e2-step --run-id e2
  docker compose run --rm generator steady --minutes 15 --run-id e4-f1
  docker compose run --rm generator e6 --run-id e6 --resend-share 0.03 --replay-big
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import statistics
import time
import uuid
from collections import Counter as Tally
from datetime import datetime, timedelta, timezone

import httpx

from . import data, profiles

EAT = timezone(timedelta(hours=3))


def now_iso() -> str:
    return datetime.now(EAT).isoformat(timespec="milliseconds")


class Logs:
    """Append-only logs, flushed every second (Annex C)."""

    def __init__(self, folder: str):
        os.makedirs(folder, exist_ok=True)
        self.sent = open(os.path.join(folder, "sent.log"), "a", buffering=1 << 16)
        self.ack = open(os.path.join(folder, "ack.log"), "a", buffering=1 << 16)
        if self.sent.tell() == 0:
            self.sent.write("time\trecord_id\tbatch_id\tattempt\tdevice_id\n")
        if self.ack.tell() == 0:
            self.ack.write("time\trecord_id\tbatch_id\tinstance\tlatency_ms\n")

    def flush(self):
        self.sent.flush()
        self.ack.flush()

    def close(self):
        self.sent.close()
        self.ack.close()


class Stats:
    def __init__(self):
        self.per_second: dict[int, Tally] = {}
        self.latency_ms: list[float] = []
        self.status = Tally()
        self.totals = Tally()

    def add(self, second: int, **counts):
        row = self.per_second.setdefault(second, Tally())
        row.update(counts)
        self.totals.update(counts)


class Tablet:
    def __init__(self, n: int, facility: str, district: str, skew: timedelta):
        self.device_id = f"SYN-TAB-{n:04d}"
        self.facility, self.district, self.skew = facility, district, skew
        self.buffer: list[dict] = []
        self.offline_until = 0.0
        self.pending: tuple[str, list[dict]] | None = None   # a batch not yet acknowledged


class Generator:
    def __init__(self, args):
        self.args = args
        self.k = args.k
        self.profile = profiles.get(args.profile, args.minutes, args.rate)
        self.folder = os.path.join(args.results, args.run_id)
        self.logs = Logs(self.folder)
        self.stats = Stats()
        self.counter = data.Counter(os.path.join(args.results, "generator_state.json"))
        self.tablets = self._make_tablets()
        self.t0 = 0.0
        self.generating = True
        self.client: httpx.AsyncClient | None = None

    def _make_tablets(self) -> list[Tablet]:
        n_tablets = max(2, round(2240 / self.k))
        n_facilities = max(2, round(3200 / self.k))
        hot = [n for n in range(1, n_facilities + 1)
               if data.facility_district(n)[1] == data.HOT_DISTRICT]
        others = [n for n in range(1, n_facilities + 1) if n not in hot]
        n_hot = max(1, round(self.args.hot_share * n_tablets))
        tablets = []
        for i in range(n_tablets):
            fac = hot[i % len(hot)] if i < n_hot else others[(i - n_hot) % len(others)]
            facility, district = data.facility_district(fac)
            skew = timedelta(0)
            if random.random() < self.args.skew_share:
                skew = timedelta(minutes=random.uniform(10, 180) * random.choice((-1, 1)))
            tablets.append(Tablet(i + 1, facility, district, skew))
        return tablets

    def second(self) -> int:
        return int(time.monotonic() - self.t0)

    # ------------------------------------------------------------ creating records
    async def produce(self) -> None:
        owed, last = 0.0, time.monotonic()
        while True:
            now = time.monotonic()
            t = now - self.t0
            if t >= self.profile.duration_s:
                break
            owed += self.profile.rate(t, self.k) * (now - last)
            last = now
            created = 0
            utc = datetime.now(timezone.utc)
            while owed >= 1:
                tab = random.choice(self.tablets)
                rec = data.make_record(self.counter.take(), tab.facility, tab.district, utc,
                                       tab.skew, self.args.run_id)
                tab.buffer.append(rec)
                owed -= 1
                created += 1
            if created:
                self.stats.add(self.second(), created=created)
            await asyncio.sleep(0.1)
        self.generating = False

    # ------------------------------------------------------------ one tablet
    async def tablet_loop(self, tab: Tablet) -> None:
        await asyncio.sleep(random.uniform(0, 30))
        last_check = time.monotonic()
        share = min(0.9, self.args.offline_share)
        go_offline_per_s = share / (1 - share) / 360          # outages last 6 min on average
        while self.generating or tab.buffer or tab.pending:
            now = time.monotonic()
            if self.generating and now >= tab.offline_until and \
                    random.random() < go_offline_per_s * (now - last_check):
                tab.offline_until = now + random.uniform(120, 600)
            last_check = now
            if now < tab.offline_until and self.generating:
                await asyncio.sleep(5)
                continue
            while (tab.buffer or tab.pending) and time.monotonic() < self.drain_deadline:
                if tab.pending is None:
                    records, tab.buffer = tab.buffer[:500], tab.buffer[500:]
                    tab.pending = (str(uuid.uuid4()), records)
                ok = await self.upload(tab, *tab.pending)
                if not ok:
                    break                         # keep the batch; try at the next sync
                batch_id, records = tab.pending
                tab.pending = None
                if random.random() < self.args.resend_share:      # E6: a duplicate upload
                    await self.upload(tab, batch_id, records, resend=True)
            if not self.generating and time.monotonic() >= self.drain_deadline:
                break
            await asyncio.sleep(random.uniform(15, 45) if self.generating else 5)

    async def upload(self, tab: Tablet, batch_id: str, records: list[dict],
                     resend: bool = False) -> bool:
        """Send one batch with up to 8 tries. True once the API answered 200."""
        body = {"device_id": tab.device_id, "batch_id": batch_id, "records": records}
        for attempt in range(1, 9):
            body["device_clock"] = (datetime.now(EAT) + tab.skew).isoformat()
            stamp = now_iso()
            tag = f"resend-{attempt}" if resend else str(attempt)
            for r in records:
                self.logs.sent.write(f"{stamp}\t{r['record_id']}\t{batch_id}\t{tag}\t{tab.device_id}\n")
            self.stats.add(self.second(), sent=len(records), requests=1)
            started = time.perf_counter()
            try:
                resp = await self.client.post("/v1/batches", json=body,
                                              headers={"X-Device-Id": tab.device_id})
                code = resp.status_code
            except httpx.HTTPError as exc:
                code, resp = f"error:{type(exc).__name__}", None
            ms = (time.perf_counter() - started) * 1000
            self.stats.status[str(code)] += 1
            if code == 200:
                instance = resp.json().get("instance", "?")
                stamp = now_iso()
                for r in records:
                    self.logs.ack.write(f"{stamp}\t{r['record_id']}\t{batch_id}\t{instance}\t{ms:.0f}\n")
                self.stats.latency_ms.append(ms)
                self.stats.add(self.second(), acked=0 if resend else len(records))
                return True
            if code == 429:
                self.stats.add(self.second(), rejected_429=1)
                wait = float(resp.headers.get("Retry-After", 30)) + random.uniform(0, 10)
            elif code == 422 or code == 400:
                self.stats.add(self.second(), bad_request=1)
                return False                       # our bug, not the server's: do not hammer
            else:
                self.stats.add(self.second(), errors=1)
                wait = min(60.0, 2 ** (attempt - 1)) * random.uniform(0.5, 1.0)
            if not self.generating and time.monotonic() + wait > self.drain_deadline:
                return False
            await asyncio.sleep(wait)
        return False

    async def replay_big(self) -> None:
        """E6: one 500-record batch sent, then replayed 3 times (same batch_id and records)."""
        await asyncio.sleep(20)
        tab = self.tablets[-1]
        utc = datetime.now(timezone.utc)
        records = [data.make_record(self.counter.take(), tab.facility, tab.district, utc,
                                    tab.skew, self.args.run_id) for _ in range(500)]
        self.stats.add(self.second(), created=500)
        batch_id = str(uuid.uuid4())
        if await self.upload(tab, batch_id, records):
            for _ in range(3):
                await asyncio.sleep(5)
                await self.upload(tab, batch_id, records, resend=True)
        else:
            tab.buffer.extend(records)

    async def flusher(self) -> None:
        while True:
            await asyncio.sleep(1)
            self.logs.flush()
            self.counter.save()

    async def reporter(self) -> None:
        last = Tally()
        while True:
            await asyncio.sleep(10)
            tot = self.stats.totals
            waiting = sum(len(t.buffer) + (len(t.pending[1]) if t.pending else 0)
                          for t in self.tablets)
            rate = (tot["acked"] - last["acked"]) / 10
            print(f"[{self.second():5d} s] target {self.profile.rate(self.second(), self.k):6.1f}/s  "
                  f"acked {rate:6.1f}/s  total acked {tot['acked']:7d}  waiting on tablets "
                  f"{waiting:6d}  429 {tot['rejected_429']}  errors {tot['errors']}", flush=True)
            last = Tally(tot)

    # ------------------------------------------------------------ run
    async def run(self) -> dict:
        limits = httpx.Limits(max_connections=400, max_keepalive_connections=100)
        self.client = httpx.AsyncClient(base_url=self.args.url, timeout=20.0, limits=limits)
        self.t0 = time.monotonic()
        self.drain_deadline = self.t0 + self.profile.duration_s + self.args.drain_s
        started = now_iso()
        print(f"run {self.args.run_id}: {self.profile.name} ({self.profile.description}), k = {self.k}, "
              f"{len(self.tablets)} tablets, about "
              f"{profiles.total_records(self.profile, self.k):,.0f} records over "
              f"{self.profile.duration_s / 60:.0f} min", flush=True)
        helpers = [asyncio.create_task(self.flusher()), asyncio.create_task(self.reporter())]
        work = [asyncio.create_task(self.produce())]
        work += [asyncio.create_task(self.tablet_loop(t)) for t in self.tablets]
        if self.args.replay_big:
            work.append(asyncio.create_task(self.replay_big()))
        await asyncio.gather(*work)
        for h in helpers:
            h.cancel()
        await self.client.aclose()
        self.logs.flush()
        self.logs.close()
        self.counter.save()
        return self.summary(started)

    def summary(self, started: str) -> dict:
        tot, lat = self.stats.totals, sorted(self.stats.latency_ms)
        active = [s for s, row in self.stats.per_second.items() if row["acked"]]
        span = (max(active) - min(active) + 1) if active else 0

        def pct(p):
            return round(lat[min(len(lat) - 1, int(p / 100 * len(lat)))], 1) if lat else None

        unacked = sum(len(t.buffer) + (len(t.pending[1]) if t.pending else 0) for t in self.tablets)
        out = {
            "run_id": self.args.run_id, "profile": self.profile.name, "k": self.k,
            "started": started, "finished": now_iso(), "url": self.args.url,
            "tablets": len(self.tablets), "hot_district": data.HOT_DISTRICT,
            "hot_share_of_tablets": self.args.hot_share, "offline_share": self.args.offline_share,
            "skewed_tablets": sum(1 for t in self.tablets if t.skew), "resend_share": self.args.resend_share,
            "records_created": tot["created"], "records_acknowledged": tot["acked"],
            "records_never_acknowledged": unacked, "record_sends_incl_retries": tot["sent"],
            "requests": tot["requests"], "http_status": dict(self.stats.status),
            "latency_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99),
                           "mean": round(statistics.fmean(lat), 1) if lat else None},
            "throughput_acked_per_s": round(tot["acked"] / span, 1) if span else 0,
            "target_mean_per_s": round(profiles.total_records(self.profile, self.k)
                                       / self.profile.duration_s, 1),
            "peak_acked_per_s": max((row["acked"] for row in self.stats.per_second.values()), default=0),
        }
        with open(os.path.join(self.folder, "summary.json"), "w") as f:
            json.dump(out, f, indent=1)
        with open(os.path.join(self.folder, "timeline.csv"), "w", newline="") as f:
            w = csv.writer(f)
            cols = ["created", "sent", "acked", "requests", "rejected_429", "errors"]
            w.writerow(["second", "target_rate"] + cols)
            for s in range(0, max(self.stats.per_second, default=0) + 1):
                row = self.stats.per_second.get(s, Tally())
                w.writerow([s, round(self.profile.rate(s, self.k), 2)] + [row[c] for c in cols])
        return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("profile", choices=["e1", "e2", "e2-step", "steady", "e6"])
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--k", type=int, default=int(os.getenv("SCALE_K", "20")))
    ap.add_argument("--minutes", type=float, help="length for steady / e1 / e6")
    ap.add_argument("--rate", type=float, help="NATIONAL records/s for steady (divided by k)")
    ap.add_argument("--url", default=os.getenv("GENERATOR_URL", "http://nginx:80"))
    ap.add_argument("--results", default=os.getenv("RESULTS_DIR", "/results"))
    ap.add_argument("--hot-share", type=float, default=0.20, help="share of tablets in D001")
    ap.add_argument("--offline-share", type=float, default=0.10,
                    help="share of tablets offline at any moment (outages of 2-10 min)")
    ap.add_argument("--skew-share", type=float, default=0.05, help="tablets with a wrong clock")
    ap.add_argument("--resend-share", type=float, default=None,
                    help="share of acknowledged batches re-sent once (default 0.03 for e6, else 0)")
    ap.add_argument("--replay-big", action="store_true", help="E6: one 500-record batch x 3 replays")
    ap.add_argument("--drain-s", type=float, default=300, help="keep retrying after the run")
    args = ap.parse_args()
    if args.resend_share is None:
        args.resend_share = 0.03 if args.profile == "e6" else 0.0
    if args.profile == "e6":
        args.replay_big = True
    args.run_id = args.run_id or f"{args.profile}-{datetime.now(EAT):%Y%m%d-%H%M%S}"
    if args.k not in (20, 10, 5, 2, 1):
        raise SystemExit("k must be one of 20, 10, 5, 2, 1 (Annex A3)")
    result = asyncio.run(Generator(args).run())
    print(json.dumps(result, indent=1))
    print(f"\nlogs in results/{args.run_id}/ ; now run: docker compose run --rm reconcile {args.run_id}")


if __name__ == "__main__":
    main()
