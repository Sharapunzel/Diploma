"""Cheap orchestration/deadline and DB readiness; never starts a JVM."""
import json
import os
import time
from pathlib import Path

import psycopg

from .adapters.wal_bootstrap import read_setting


def healthy(path="/tmp/detector-worker.json"):
    try:
        state = json.loads(Path(path).read_text(encoding="utf-8"))
        now = time.monotonic()
        os.kill(state["pid"], 0)
        if not 0 <= now - state["heartbeat"] < 10 or now > state["deadline"]:
            return False
        url = read_setting("DATABASE_URL").replace("postgresql+psycopg:", "postgresql:")
        with psycopg.connect(url, connect_timeout=2, options="-c statement_timeout=2000",
                keepalives=1, keepalives_idle=1, keepalives_interval=1, keepalives_count=1,
                tcp_user_timeout=2000) as connection:
            if connection.execute("SELECT 1 FROM logs.detector_coordinator WHERE id=1").fetchone() != (1,):
                return False
        return True
    except (OSError, ValueError, KeyError, TypeError, psycopg.Error):
        return False


if __name__ == "__main__":
    raise SystemExit(0 if healthy() else 1)
