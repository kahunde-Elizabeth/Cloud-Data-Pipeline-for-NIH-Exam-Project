"""Cause a fridge fault in the sensor simulator (for tests and the demo video).

Usage (from the repo folder, with the stack running):
    python scripts/fridge_fault.py S0001 warm        temperature climbs above 8 C
    python scripts/fridge_fault.py S0001 cold        temperature falls below 2 C
    python scripts/fridge_fault.py S0001 silent      probe stops sending (offline after 5 min)
    python scripts/fridge_fault.py S0001 power_cut   connection drops -> Last Will -> offline
    python scripts/fridge_fault.py S0001 normal      back to normal
    python scripts/fridge_fault.py --status          which sensors are not normal

Sensor S0001 is in facility F0001, district D001 (the district officer's district).
Uses only the Python standard library.
"""
import argparse
import json
import urllib.error
import urllib.request


def call(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sensor", nargs="?")
    ap.add_argument("mode", nargs="?", choices=["warm", "cold", "silent", "power_cut", "normal"])
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--url", default="http://localhost:8091")
    args = ap.parse_args()
    if args.status or not (args.sensor and args.mode):
        print(json.dumps(call(f"{args.url}/status")[1], indent=1))
        return
    status, body = call(f"{args.url}/fault?sensor={args.sensor}&mode={args.mode}")
    print(status, body)


if __name__ == "__main__":
    main()
