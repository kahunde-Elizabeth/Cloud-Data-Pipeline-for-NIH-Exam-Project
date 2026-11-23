"""Sensor bridge tests. The live tests start a real Mosquitto if it is installed
(skipped otherwise); Redpanda is replaced by a fake producer."""
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

import pytest

from app import main as bridge

NOW = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------- unit tests
def test_convert_reading_adds_server_time():
    kind, key, value = bridge.convert("nidh/sensors/S0001/reading",
                                      b'{"sensor_id":"S0001","ts":"x","temp_c":4.2}', NOW)
    assert (kind, key) == ("reading", "S0001")
    assert value["received_at"] == NOW.isoformat() and value["kind"] == "reading"


def test_convert_status():
    kind, _, value = bridge.convert("nidh/sensors/S0001/status",
                                    b'{"sensor_id":"S0001","status":"offline"}', NOW)
    assert kind == "status" and value["status"] == "offline"


@pytest.mark.parametrize("payload", [b"not json", b"[1,2]", b'{"temp_c":4}', b"\xff\xfe"])
def test_bad_payload_goes_to_dlq(payload):
    kind, key, value = bridge.convert("nidh/sensors/S0001/reading", payload, NOW)
    assert kind == "dlq" and key is None and "error" in value


def test_dedup_remembers_last_n():
    d = bridge.Dedup(2)
    assert d.is_new(("S1", "t1")) and not d.is_new(("S1", "t1"))
    d.is_new(("S1", "t2")); d.is_new(("S1", "t3"))
    assert d.is_new(("S1", "t1"))                  # forgotten after 2 newer entries


# ---------------------------------------------------------------- live tests
class FakeProducer:
    def __init__(self):
        self.sent, self.fail = [], False

    async def send(self, topic, value, key=None):
        fut = asyncio.get_running_loop().create_future()
        if self.fail:
            fut.set_exception(ConnectionError("no broker confirmed"))
        else:
            self.sent.append((topic, json.loads(value)))
            fut.set_result(None)
        return fut


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


@pytest.fixture
def broker(tmp_path):
    exe = shutil.which("mosquitto") or "/usr/sbin/mosquitto"
    if not os.path.exists(exe):
        pytest.skip("mosquitto not installed")
    port = free_port()
    conf = tmp_path / "m.conf"
    conf.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\n")
    log = open(os.environ.get("MQTT_TEST_LOG", os.devnull), "w")
    proc = subprocess.Popen([exe, "-c", str(conf), "-v"], stdout=log, stderr=subprocess.STDOUT)
    time.sleep(0.4)
    yield port
    proc.terminate(); proc.wait()


async def start_bridge(port, name, producer):
    bridge.INSTANCE = name
    loop = asyncio.get_running_loop()
    ref = [None]
    client = bridge.make_client(loop, ref)
    ref[0] = b = bridge.Bridge(producer, client)
    client.connect("127.0.0.1", port, keepalive=10)
    client.loop_start()
    task = asyncio.create_task(b.run())
    await asyncio.sleep(0.5)
    return client, task


def sensor(port, n, mode=None):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sensor_sim"))
    from app_sim_loader import load_sim
    sim = load_sim()
    sim.MQTT_HOST, sim.MQTT_PORT = "127.0.0.1", port
    s = sim.Sensor(n)
    s.start()
    for _ in range(50):
        if s.client.is_connected():
            break
        time.sleep(0.05)
    return s


async def wait_for(cond, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def test_live_forward_and_last_will(broker):
    async def go():
        prod = FakeProducer()
        client, task = await start_bridge(broker, "b1", prod)
        s = sensor(broker, 1)
        s.publish()
        await asyncio.sleep(1.1)                   # readings are identified by sensor + second
        s.publish()
        assert await wait_for(lambda: sum(1 for t, v in prod.sent if v["kind"] == "reading") == 2)
        assert all(t == "sensor-readings" for t, _ in prod.sent)
        s.set_mode("power_cut")                    # abrupt drop: broker publishes the Last Will
        assert await wait_for(lambda: any(v.get("status") == "offline" for _, v in prod.sent))
        task.cancel(); client.loop_stop(); client.disconnect()
    asyncio.run(go())


def test_live_nothing_acknowledged_until_redpanda_confirms(broker):
    async def go():
        prod = FakeProducer()
        prod.fail = True                            # Redpanda down
        client, task = await start_bridge(broker, "b2", prod)
        s = sensor(broker, 2)
        s.publish()
        await asyncio.sleep(1.0)
        assert not any(v["kind"] == "reading" for _, v in prod.sent)
        prod.fail = False                           # Redpanda back
        # the bridge reconnects; the broker resends the unacknowledged reading
        assert await wait_for(lambda: any(v["kind"] == "reading" and v["sensor_id"] == "S0002"
                                          for _, v in prod.sent), timeout=8)
        task.cancel(); client.loop_stop(); client.disconnect()
        s.client.loop_stop(); s.client.disconnect()
    asyncio.run(go())


def test_live_shared_subscription_each_message_once(broker):
    async def go():
        p1, p2 = FakeProducer(), FakeProducer()
        c1, t1 = await start_bridge(broker, "b3", p1)
        c2, t2 = await start_bridge(broker, "b4", p2)
        sensors = [sensor(broker, n) for n in range(10, 20)]
        for s in sensors:
            s.publish()
        readings = lambda p: [v for _, v in p.sent if v["kind"] == "reading"]
        assert await wait_for(lambda: len(readings(p1)) + len(readings(p2)) == 10)
        await asyncio.sleep(0.5)
        assert len(readings(p1)) + len(readings(p2)) == 10        # none twice
        assert readings(p1) and readings(p2)                       # both copies did work
        for t, c in ((t1, c1), (t2, c2)):
            t.cancel(); c.loop_stop(); c.disconnect()
    asyncio.run(go())
