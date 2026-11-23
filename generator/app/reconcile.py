"""Reconcile a generator run against the database (Annex C).

  1. Drain: wait until the processors' backlog (queue lag) is zero.
  2. Read results/<run>/sent.log and ack.log.
  3. Compute, and write results/<run>/reconcile.json and reconcile.md (with the exact queries):
       Lost      = acknowledged record_ids not in the database              (must be 0)
       Phantom   = database rows received during the run that were never sent (must be 0)
       Logical duplicates = rows sharing a record_id, or sharing child_id + vaccine_code +
                            dose_no, in the whole table                       (must be 0)
       Stored but never acknowledged: acceptable (the client would retry).
  Phantom uses the run's time window: nothing else should write vaccination records during a run.

Usage:  docker compose run --rm reconcile e1-run1 [more run ids]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta

import psycopg

DSN = os.getenv("DATABASE_URL",
                "postgresql://root@roach-0:26257,roach-1:26257,roach-2:26257/nidh?sslmode=disable")
LAG_QUERY = "sum(processor_lag_messages)"

Q_FOUND = "SELECT record_id FROM vaccination_records WHERE record_id = ANY(%s)"
Q_REJECTED = "SELECT DISTINCT record_id FROM rejects WHERE record_id = ANY(%s)"
Q_WINDOW = ("SELECT record_id FROM vaccination_records "
            "WHERE received_at >= %s AND received_at <= %s")
Q_DUP_ID = ("SELECT count(*) FROM (SELECT record_id FROM vaccination_records "
            "GROUP BY record_id HAVING count(*) > 1) AS d")
Q_DUP_LOGICAL = ("SELECT count(*) FROM (SELECT child_id, vaccine_code, dose_no "
                 "FROM vaccination_records GROUP BY 1, 2, 3 HAVING count(*) > 1) AS d")


def read_log(path: str):
    """Returns (set of record_ids, first time, last time)."""
    ids, first, last = set(), None, None
    with open(path) as f:
        next(f, None)                                   # header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            ids.add(parts[1])
            first = first or parts[0]
            last = parts[0]
    return ids, first, last


def wait_for_drain(prometheus: str, timeout_s: float) -> dict:
    """Queue lag must read 0 three times in a row, 5 s apart."""
    url = f"{prometheus}/api/v1/query?query={urllib.parse.quote(LAG_QUERY)}"
    started, zeros, lag = time.monotonic(), 0, None
    while time.monotonic() - started < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                result = json.loads(resp.read())["data"]["result"]
            lag = float(result[0]["value"][1]) if result else 0.0
        except Exception as exc:
            print(f"  cannot read queue lag from Prometheus ({exc}); waiting 30 s instead")
            time.sleep(30)
            return {"method": "fixed 30 s wait (Prometheus unreachable)", "lag": None}
        zeros = zeros + 1 if lag == 0 else 0
        print(f"  queue lag: {lag:.0f} messages")
        if zeros >= 3:
            return {"method": f"Prometheus {LAG_QUERY} == 0 three times, 5 s apart",
                    "waited_s": round(time.monotonic() - started)}
        time.sleep(5)
    return {"method": "timed out", "lag": lag, "waited_s": round(time.monotonic() - started)}


def chunks(items, n=5000):
    items = list(items)
    for i in range(0, len(items), n):
        yield items[i:i + n]


def reconcile(conn, sent: set, acked: set, window: tuple[datetime, datetime]) -> dict:
    found, rejected = set(), set()
    for part in chunks(sent):
        ids = [uuid.UUID(x) for x in part]
        found |= {str(r[0]) for r in conn.execute(Q_FOUND, (ids,)).fetchall()}
        rejected |= {str(r[0]) for r in conn.execute(Q_REJECTED, (ids,)).fetchall()}
    in_window = {str(r[0]) for r in conn.execute(Q_WINDOW, window).fetchall()}
    lost = acked - found
    phantom = in_window - sent
    dup_id = conn.execute(Q_DUP_ID).fetchone()[0]
    dup_logical = conn.execute(Q_DUP_LOGICAL).fetchone()[0]
    stored_not_acked = (sent - acked) & found
    return {
        "sent_distinct": len(sent), "acknowledged_distinct": len(acked),
        "stored_of_sent": len(found & sent),
        "lost": len(lost), "lost_but_in_rejects": len(lost & rejected),
        "phantom": len(phantom), "duplicate_record_id_rows": dup_id,
        "duplicate_child_vaccine_dose": dup_logical,
        "stored_but_never_acknowledged": len(stored_not_acked),
        "sent_never_acknowledged_not_stored": len(sent - acked - found),
        "examples": {"lost": sorted(lost)[:10], "phantom": sorted(phantom)[:10]},
        "pass": len(lost) == 0 and len(phantom) == 0 and dup_id == 0 and dup_logical == 0,
    }


def report_md(run_ids, result, drain, window) -> str:
    ok = "PASS" if result["pass"] else "FAIL"
    lines = [f"# Reconciliation: {', '.join(run_ids)} ({ok})", "",
             f"Method: Annex C. Drain: {drain['method']}. "
             f"Phantom window: {window[0].isoformat()} to {window[1].isoformat()}.", "",
             "| Check | Count | Required |", "| --- | --- | --- |",
             f"| Records sent (distinct record_ids) | {result['sent_distinct']} | |",
             f"| Records acknowledged (distinct) | {result['acknowledged_distinct']} | |",
             f"| **Lost** (acknowledged, not in database) | {result['lost']} | 0 |",
             f"| **Phantom** (in database, never sent) | {result['phantom']} | 0 |",
             f"| **Duplicate record_id rows** | {result['duplicate_record_id_rows']} | 0 |",
             f"| **Duplicate child + vaccine + dose** | {result['duplicate_child_vaccine_dose']} | 0 |",
             f"| Stored but never acknowledged | {result['stored_but_never_acknowledged']} | acceptable |",
             f"| Sent, never acknowledged, not stored | {result['sent_never_acknowledged_not_stored']} | "
             "acceptable (client keeps them) |", "",
             "## Exact queries", "", "```sql",
             f"-- found: run for every sent record_id, 5,000 at a time\n{Q_FOUND};",
             f"-- lost ones that were rejected instead\n{Q_REJECTED};",
             f"-- phantom: rows received during the run window\n{Q_WINDOW};",
             f"{Q_DUP_ID};", f"{Q_DUP_LOGICAL};", "```", "",
             "Lost = acknowledged - found; Phantom = window rows - sent; computed in "
             "generator/app/reconcile.py from sent.log and ack.log."]
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_ids", nargs="+")
    ap.add_argument("--results", default=os.getenv("RESULTS_DIR", "/results"))
    ap.add_argument("--prometheus", default=os.getenv("PROMETHEUS_URL", "http://prometheus:9090"))
    ap.add_argument("--drain-timeout", type=float, default=600)
    ap.add_argument("--no-drain", action="store_true")
    ap.add_argument("--margin-s", type=float, default=60, help="added around the run window")
    args = ap.parse_args()

    sent, acked, starts, ends = set(), set(), [], []
    for run in args.run_ids:
        folder = os.path.join(args.results, run)
        s, first, last = read_log(os.path.join(folder, "sent.log"))
        a, _, alast = read_log(os.path.join(folder, "ack.log"))
        sent |= s
        acked |= a
        starts += [first] if first else []
        ends += [x for x in (last, alast) if x]
    if not sent:
        sys.exit("no records in sent.log")
    window = (datetime.fromisoformat(min(starts)) - timedelta(seconds=args.margin_s),
              datetime.fromisoformat(max(ends)) + timedelta(seconds=args.margin_s))

    print("1. Drain (wait for queue lag 0)")
    drain = {"method": "skipped (--no-drain)"} if args.no_drain else \
        wait_for_drain(args.prometheus, args.drain_timeout)
    print("2. Compare logs with the database")
    with psycopg.connect(DSN, autocommit=True) as conn:
        result = reconcile(conn, sent, acked, window)
    result.update(run_ids=args.run_ids, drain=drain,
                  window=[window[0].isoformat(), window[1].isoformat()],
                  checked_at=datetime.now().astimezone().isoformat())

    target = os.path.join(args.results, args.run_ids[0])
    with open(os.path.join(target, "reconcile.json"), "w") as f:
        json.dump(result, f, indent=1)
    with open(os.path.join(target, "reconcile.md"), "w") as f:
        f.write(report_md(args.run_ids, result, drain, window))
    print(report_md(args.run_ids, result, drain, window))
    sys.exit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
