"""Detector worker scenario on the same isolated Compose profiles as the app."""
import json
import time

SNAPSHOT = """
import json
from sqlalchemy import select
from app.adapters.wal_bootstrap import read_setting
from app.db import Database
from app.models import Detector,DetectorWarmup,DetectorOutcome,Finding
db=Database(read_setting('DATABASE_URL'))
with db.session_factory() as session:
 facts={}
 for model in (Detector,DetectorWarmup,DetectorOutcome,Finding):
  rows=session.scalars(select(model)).all()
  facts[model.__tablename__]={str(r.id):{c.key:getattr(r,c.key) for c in model.__table__.columns} for r in rows}
 print(json.dumps(facts,default=str,sort_keys=True))
db.dispose()
"""


def detector_runtime_smoke(compose, env, production, command, lifecycle=None):
    # The fixture is its own synthetic processor/probe. Keep the independent
    # API consumer from also claiming its synthetic Kafka identities. Actual
    # WAL, aggregate-worker and detector-worker remain live throughout.
    command([*compose, "stop", "api"], env=env)
    try:
        command([*compose, "exec", "-T", "detector-worker", "python", "tests/ml/detector_runtime.py"],
                env=env, timeout=180)
    finally:
        command([*compose, "up", "-d", "--no-deps", "--wait", "api"], env=env)
    output = command([*compose, "exec", "-T", "detector-worker", "cat", "/tmp/detector-runtime-result.json"],
                     env=env, capture=True)
    result = json.loads(output.strip().splitlines()[-1])
    print(f"Detector real worker: {result}", flush=True)
    command([*compose, "exec", "-T", "detector-worker", "python", "-m", "app.detector_health"], env=env)
    identity = result["detectors"][0]
    def cli(action):
        output = command([*compose, "exec", "-T", "detector-worker", "python", "-m", "app.detector_cli", action, identity],
                         env=env, capture=True)
        return json.loads(output.strip().splitlines()[-1])
    assert cli("start")["detector"]["execution_state"] in {"requested", "starting", "running"}
    deadline = time.monotonic() + 20
    while cli("state")["detector"]["execution_state"] != "running":
        assert time.monotonic() < deadline, "CLI resume was not accepted"
        time.sleep(.1)
    active = cli("state")["detector"]
    command([*compose, "restart", "detector-worker"], env=env)
    command([*compose, "up", "-d", "--wait", "detector-worker"], env=env)
    restored = cli("state")["detector"]
    assert restored["execution_state"] == "running"
    assert restored["start_request_id"] == active["start_request_id"]
    assert restored["active_attempt_id"] == active["active_attempt_id"]
    assert restored["checkpoint"] == active["checkpoint"]
    assert cli("stop")["detector"]["execution_state"] == "stopped"
    def snapshot():
        output = command([*compose, "exec", "-T", "detector-worker", "python", "-c", SNAPSHOT], env=env, capture=True)
        return json.loads(output.strip().splitlines()[-1])
    before = snapshot()
    if lifecycle is not None:
        lifecycle()
    else:
        command([*compose, "restart", "detector-worker"], env=env)
        command([*compose, "up", "-d", "--wait", "detector-worker"], env=env)
    assert snapshot() == before, "Detector request/checkpoint/history changed during a stopped restart"
    print("Detector runtime health/restart and durable manual stop passed", flush=True)
