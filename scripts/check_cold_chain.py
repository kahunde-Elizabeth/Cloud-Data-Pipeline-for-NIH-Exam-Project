"""Check the fridge sensor path end to end (step 4), through the read API as the D001 officer.

  1. Warm fridge:  S0001 is made warm. Waits for a 'too_warm' alert. Then back to normal and
                   waits for 'back_in_range'. Readings come once a minute, so each part takes
                   about 1-3 minutes.
  2. Power cut:    S0001's connection is dropped. The broker sends its Last Will, so an
                   'offline' alert should appear within seconds. Then power returns and a
                   'back_online' alert follows with the next reading (within about a minute).
  3. --silence     (optional, adds about 6 minutes) S0001 stays connected but stops sending.
                   The 5-minute silence check should raise 'offline'.
  4. Role check:   the officer cannot read D002's alerts (403).

Usage:  python scripts/check_cold_chain.py [--silence]
Uses only the Python standard library.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

SENSOR = "S0001"


def get(url, user=None):
    req = urllib.request.Request(url, headers={"X-User": user} if user else {})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def fault(sim, mode):
    status, body = get(f"{sim}/fault?sensor={SENSOR}&mode={mode}")
    if status != 200:
        sys.exit(f"simulator answered {status}: {body}")
    print(f"  -> {SENSOR} set to {mode} (temperature now {body['temp_c']} C)")


def seen(api):
    status, body = get(f"{api}/v1/alerts?hours=1", "officer_d001")
    if status != 200:
        sys.exit(f"read API answered {status}: {body}")
    return {a["alert_id"] for a in body["alerts"] if a["sensor_id"] == SENSOR}, body["alerts"]


def wait_for(api, kind, before, limit_s, started):
    while time.monotonic() - started < limit_s:
        ids, alerts = seen(api)
        for a in alerts:
            if a["alert_id"] not in before and a["sensor_id"] == SENSOR and a["kind"] == kind:
                waited = time.monotonic() - started
                print(f"  [ok ] {kind:13s} after {waited:4.0f} s: {a['detail']}")
                return ids
        time.sleep(3)
    print(f"  [BAD] no {kind} alert within {limit_s} s")
    return seen(api)[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--sim", default="http://localhost:8091")
    ap.add_argument("--silence", action="store_true", help="also test the 5-minute silence check")
    args = ap.parse_args()

    before, _ = seen(args.api)
    print("1. Warm fridge")
    fault(args.sim, "warm")
    before = wait_for(args.api, "too_warm", before, 240, time.monotonic())
    fault(args.sim, "normal")
    before = wait_for(args.api, "back_in_range", before, 150, time.monotonic())

    print("2. Power cut (Last Will)")
    fault(args.sim, "power_cut")
    before = wait_for(args.api, "offline", before, 60, time.monotonic())
    fault(args.sim, "normal")
    before = wait_for(args.api, "back_online", before, 150, time.monotonic())

    if args.silence:
        print("3. Silent probe (5-minute silence check)")
        fault(args.sim, "silent")
        before = wait_for(args.api, "offline", before, 420, time.monotonic())
        fault(args.sim, "normal")
        wait_for(args.api, "back_online", before, 150, time.monotonic())

    print("4. Role check")
    status, body = get(f"{args.api}/v1/alerts?district=D002", "officer_d001")
    mark = "ok " if status == 403 else "BAD"
    print(f"  [{mark}] officer_d001 reading D002 alerts -> {status} (expected 403)")


if __name__ == "__main__":
    main()
