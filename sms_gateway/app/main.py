"""Mock SMS gateway: behaves like a paid SMS provider, but sends nothing.

  POST /send   {"message_id": "...", "to": "SYN-07...", "body": "..."}
               200 {"provider_id": "...", "duplicate": false, "cost_ugx": 30}
               A repeat of the same message_id is NOT sent or charged again: it returns the
               first provider_id with "duplicate": true (real providers offer this as a
               client reference / idempotency key).
               Like a real provider it sometimes fails (500, about 1 in 20) and is sometimes
               slow (1-2 s, about 1 in 10).
  GET /outage?seconds=120   every send fails with 503 for that long (for tests)
  GET /sent?limit=20&to=SYN-07...   what the "phones" received, newest first
  GET /healthz

Every delivered message is appended to /data/sms_sent.log, and the IDs are reloaded from it
on start, so a restart of the gateway cannot cause a double send.
Only synthetic numbers (SYN-...) are accepted.
"""
from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from prometheus_client import Counter, start_http_server

PORT = int(os.getenv("GATEWAY_PORT", "8092"))
LOG_PATH = os.getenv("SMS_LOG", "/data/sms_sent.log")
FAIL_RATE = float(os.getenv("GATEWAY_FAIL_RATE", "0.05"))
SLOW_RATE = float(os.getenv("GATEWAY_SLOW_RATE", "0.10"))
COST_UGX = int(os.getenv("SMS_COST_UGX", "30"))
EAT = timezone(timedelta(hours=3))

log = logging.getLogger("sms_gateway")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s sms-gateway %(message)s")

DELIVERED = Counter("gateway_sms_delivered_total", "Messages delivered (charged)", ["kind"])
DUPLICATES = Counter("gateway_duplicates_total", "Repeats of a message_id, not sent again")
FAILURES = Counter("gateway_failures_total", "Sends refused", ["reason"])
COST = Counter("gateway_cost_ugx_total", "Money charged, in UGX")


class Gateway:
    def __init__(self, path: str):
        self.path, self.lock = path, threading.Lock()
        self.sent: dict[str, dict] = {}
        self.outage_until = 0.0
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        self.sent[entry["message_id"]] = entry
                    except (ValueError, KeyError):
                        pass
            log.info("reloaded %d delivered message IDs", len(self.sent))

    def send(self, message_id: str, to: str, body: str, kind: str):
        """Returns (http_status, response_body)."""
        if time.time() < self.outage_until:
            FAILURES.labels(reason="outage").inc()
            return 503, {"error": "provider unavailable (simulated outage)"}
        with self.lock:
            if message_id in self.sent:
                DUPLICATES.inc()
                return 200, {"provider_id": self.sent[message_id]["provider_id"],
                             "duplicate": True, "cost_ugx": 0}
        if random.random() < SLOW_RATE:
            time.sleep(random.uniform(1.0, 2.0))
        if random.random() < FAIL_RATE:
            FAILURES.labels(reason="random").inc()
            return 500, {"error": "provider error (simulated)"}
        entry = {"message_id": message_id, "provider_id": f"MOCK-{uuid.uuid4().hex[:12]}",
                 "to": to, "kind": kind, "body": body,
                 "delivered_at": datetime.now(EAT).isoformat(timespec="seconds")}
        with self.lock:
            if message_id in self.sent:            # two copies raced: still only one SMS
                DUPLICATES.inc()
                return 200, {"provider_id": self.sent[message_id]["provider_id"],
                             "duplicate": True, "cost_ugx": 0}
            self.sent[message_id] = entry
            with open(self.path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        DELIVERED.labels(kind=kind).inc()
        COST.inc(COST_UGX)
        return 200, {"provider_id": entry["provider_id"], "duplicate": False, "cost_ugx": COST_UGX}

    def recent(self, limit: int, to: str | None):
        with self.lock:
            items = [e for e in self.sent.values() if to is None or e["to"] == to]
        return sorted(items, key=lambda e: e["delivered_at"], reverse=True)[:limit]


GATEWAY: Gateway | None = None


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code, body):
        data = json.dumps(body, indent=1).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if urlparse(self.path).path != "/send":
            return self._reply(404, {"error": "POST /send"})
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            message_id, to, text = str(body["message_id"]), str(body["to"]), str(body["body"])
            uuid.UUID(message_id)
        except (ValueError, KeyError, TypeError):
            return self._reply(400, {"error": "need message_id (UUID), to and body"})
        if not to.startswith("SYN-"):
            return self._reply(400, {"error": "only synthetic numbers (SYN-...) in this prototype"})
        self._reply(*GATEWAY.send(message_id, to, text, str(body.get("kind", "unknown"))))

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path == "/healthz":
            return self._reply(200, {"status": "ok", "delivered": len(GATEWAY.sent)})
        if url.path == "/outage":
            seconds = float(q.get("seconds", "60"))
            GATEWAY.outage_until = time.time() + seconds
            log.info("simulated outage for %.0f s", seconds)
            return self._reply(200, {"outage_for_s": seconds})
        if url.path == "/sent":
            return self._reply(200, GATEWAY.recent(int(q.get("limit", "20")), q.get("to")))
        self._reply(404, {"error": "use /send, /sent, /outage or /healthz"})

    def log_message(self, *_):
        pass


def main():
    global GATEWAY
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
    GATEWAY = Gateway(LOG_PATH)
    log.info("mock SMS gateway on port %d (fail %.0f%%, slow %.0f%%)", PORT,
             FAIL_RATE * 100, SLOW_RATE * 100)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
