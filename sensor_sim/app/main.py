"""Fridge sensor simulator (synthetic data, Annex B).

Plays SENSOR_COUNT fridge sensors (default 160 = 3,200 facilities / k 20), sensor S0001 in
facility F0001 and so on. Each sensor is its own MQTT connection, like a real device:
  * publishes one reading every READING_INTERVAL_S (default 60 s) with QoS 1 to
    nidh/sensors/<id>/reading, about 120 bytes;
  * registers a Last Will: if its connection drops without a goodbye, the broker itself
    publishes {"status": "offline"} to nidh/sensors/<id>/status.

Faults for tests and the demo, via http://localhost:8091 (see scripts/fridge_fault.py):
  /fault?sensor=S0001&mode=warm        temperature climbs above 8 C
  /fault?sensor=S0001&mode=cold        temperature falls below 2 C (vaccines can freeze)
  /fault?sensor=S0001&mode=silent      stays connected but stops sending (dead probe)
  /fault?sensor=S0001&mode=power_cut   connection dropped abruptly -> broker sends the Last Will
  /fault?sensor=S0001&mode=normal      back to normal (reconnects if needed)
  /status                              current modes
"""
from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import paho.mqtt.client as mqtt
from prometheus_client import Counter, start_http_server

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
COUNT = int(os.getenv("SENSOR_COUNT", "160"))
INTERVAL = float(os.getenv("READING_INTERVAL_S", "60"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "8091"))
EAT = timezone(timedelta(hours=3))
MODES = {"normal", "warm", "cold", "silent", "power_cut"}

log = logging.getLogger("sensor_sim")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s sensor-sim %(message)s")
PUBLISHED = Counter("sim_readings_published_total", "Readings handed to MQTT")


def next_temp(temp: float, mode: str) -> float:
    """A gentle random walk around 4.5 C, or a drift for the warm / cold faults."""
    if mode == "warm":
        return min(15.0, temp + random.uniform(1.5, 2.5))     # a door left open
    if mode == "cold":
        return max(-3.0, temp - random.uniform(1.5, 2.5))     # thermostat stuck
    pull = (4.5 - temp) * 0.3                       # back towards 4.5 C after a fault
    return round(min(7.6, max(2.4, temp + pull + random.uniform(-0.3, 0.3))), 1)


class Sensor:
    def __init__(self, n: int):
        self.sensor_id, self.facility_id = f"S{n:04d}", f"F{n:04d}"
        self.temp, self.seq, self.mode = round(random.uniform(3.5, 5.5), 1), 0, "normal"
        self.status_topic = f"nidh/sensors/{self.sensor_id}/status"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.sensor_id,
                                  clean_session=False)
        self.client.will_set(self.status_topic, self._status("offline"), qos=1)
        self.client.on_connect = lambda c, *_: c.publish(self.status_topic,
                                                         self._status("online"), qos=1)
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def _status(self, status: str) -> str:
        return json.dumps({"sensor_id": self.sensor_id, "facility_id": self.facility_id,
                           "status": status, "ts": datetime.now(EAT).isoformat()})

    def start(self):
        self.client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_start()

    def reading(self) -> str:
        self.seq += 1
        self.temp = next_temp(self.temp, self.mode)
        return json.dumps({"sensor_id": self.sensor_id, "facility_id": self.facility_id,
                           "ts": datetime.now(EAT).isoformat(timespec="seconds"),
                           "temp_c": round(self.temp, 1), "seq": self.seq})

    def publish(self):
        if self.mode in ("silent", "power_cut") or not self.client.is_connected():
            return
        self.client.publish(f"nidh/sensors/{self.sensor_id}/reading", self.reading(), qos=1)
        PUBLISHED.inc()

    def set_mode(self, mode: str):
        old, self.mode = self.mode, mode
        if mode == "power_cut" and old != "power_cut":
            self.client.loop_stop()
            sock = self.client.socket()
            if sock:
                sock.close()          # no DISCONNECT packet, so the broker sends the Last Will
        elif old == "power_cut" and mode != "power_cut":
            self.client.reconnect()
            self.client.loop_start()


SENSORS: dict[str, Sensor] = {}


class Control(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body, indent=1).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path == "/status":
            return self._send(200, {s.sensor_id: {"mode": s.mode, "temp_c": round(s.temp, 1),
                                                  "connected": s.mode != "power_cut" and s.client.is_connected()}
                                    for s in SENSORS.values() if s.mode != "normal"}
                              or {"all": "normal"})
        if url.path == "/fault":
            sensor, mode = SENSORS.get(q.get("sensor", "")), q.get("mode", "")
            if sensor is None or mode not in MODES:
                return self._send(400, {"error": "need sensor=S0001..S%04d and mode in %s"
                                        % (COUNT, sorted(MODES))})
            sensor.set_mode(mode)
            log.info("fault: %s -> %s", sensor.sensor_id, mode)
            return self._send(200, {"sensor": sensor.sensor_id, "mode": mode,
                                    "temp_c": round(sensor.temp, 1)})
        self._send(404, {"error": "use /fault or /status"})

    def log_message(self, *_):
        pass


def main():
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    for n in range(1, COUNT + 1):
        SENSORS[f"S{n:04d}"] = Sensor(n)
        SENSORS[f"S{n:04d}"].start()
        time.sleep(0.02)                     # do not open 160 connections in the same instant
    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", CONTROL_PORT), Control).serve_forever,
                     daemon=True).start()
    log.info("%d sensors started, one reading every %.0f s each", COUNT, INTERVAL)

    # Spread the sensors evenly over the interval, as real devices would be.
    offsets = {s: random.uniform(0, INTERVAL) for s in SENSORS.values()}
    start = time.monotonic()
    rounds: dict[Sensor, int] = {s: 0 for s in SENSORS.values()}
    while True:
        now = time.monotonic() - start
        for s, off in offsets.items():
            if now >= off + rounds[s] * INTERVAL:
                rounds[s] += 1
                s.publish()
        time.sleep(0.2)


if __name__ == "__main__":
    main()
