"""Test helper: load sensor_sim/app/main.py under another name (both services use 'app')."""
import importlib.util
import os

_MOD = None


def load_sim():
    global _MOD
    if _MOD is None:
        path = os.path.join(os.path.dirname(__file__), "app", "main.py")
        spec = importlib.util.spec_from_file_location("sensor_sim_main", path)
        _MOD = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_MOD)
    return _MOD
