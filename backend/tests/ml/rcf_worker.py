"""Cheap worker boundaries executed in the mandatory Linux ML profile."""
import json
import os
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app import detector_health


@pytest.mark.parametrize("command", ["prepare-warmup", "warmup", "advance-one"])
def test_cli_cannot_bypass_managed_execution(command):
    result = subprocess.run([sys.executable, "-m", "app.detector_cli", command, str(uuid4())],
        capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 1
    assert json.loads(result.stdout)["error"] == "managed_worker_required"
    assert "start" in json.loads(result.stdout)["message"]


def test_health_accepts_bounded_ml_phase_and_rejects_deadline_or_stale_loop(tmp_path, monkeypatch):
    path = tmp_path / "health.json"
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, *_): return SimpleNamespace(fetchone=lambda: (1,))
    monkeypatch.setattr(detector_health.psycopg, "connect", lambda *a, **k: Connection())
    monkeypatch.setattr(detector_health, "read_setting", lambda _: "postgresql://synthetic")
    now = time.monotonic()
    state = {"pid": os.getpid(), "heartbeat": now, "phase": "warming", "deadline": now + 120}
    path.write_text(json.dumps(state))
    assert detector_health.healthy(path)
    path.write_text(json.dumps({**state, "deadline": now - .1}))
    assert not detector_health.healthy(path)
    path.write_text(json.dumps({**state, "heartbeat": now - 11}))
    assert not detector_health.healthy(path)
    path.write_text(json.dumps({**state, "deadline": "invalid"}))
    assert not detector_health.healthy(path)


def test_resource_details_preserve_known_factor_and_drop_arbitrary_text():
    from app.detection.execution import reason_details
    assert reason_details("resource_budget:model_seconds") == {"factor": "model_seconds"}
    assert reason_details("resource_budget:arbitrary credential or driver text") == {}
    assert reason_details("database connection traceback") == {}


def test_cancelled_candidate_leaves_no_child_process():
    from threading import Event

    from app.detection.contracts import StaleCandidate
    from app.detection.model_process import invoke
    cancel = Event()
    cancel.set()
    with pytest.raises(StaleCandidate, match="ownership_lost"):
        invoke({"action": "warmup"}, cancel)


def test_acquisition_uses_service_contract_and_recovers_after_publication(tmp_path):
    from threading import Event

    from app.services.implementations.detector_worker import DetectorWorker

    trace, first, second, owner = [], uuid4(), uuid4(), object()
    class Session:
        def __enter__(self): return self
        def __exit__(self, *_): pass
    class Service:
        def acquire_owner(self, identity, pid):
            trace.append(("publish", identity, pid))
            return owner
        def candidates(self, after=None):
            trace.append(("candidates", after))
            return [first, second] if after is None else []
        def recover(self, identity):
            trace.append(("recover", identity))
    leadership = SimpleNamespace(acquire=lambda: 42, ping=lambda: True)
    worker = DetectorWorker(Session, lambda *_: Service(), None, Event(), leadership, tmp_path / "heartbeat")
    assert worker._acquire() and worker.owner is owner
    assert trace[0][0] == "publish" and trace[0][2] == 42
    assert trace[1:] == [("candidates", None), ("recover", first), ("recover", second), ("candidates", second)]
    worker.heartbeat.unlink()


def test_failed_owner_publication_rolls_back_without_recovery(tmp_path):
    from threading import Event

    from app.services.implementations.detector_worker import DetectorWorker
    from app.services.implementations.detectors import DetectorService

    trace = []
    class Repository:
        def acquire_owner(self, *_):
            trace.append("publish")
            return object()
    class UnitOfWork:
        def commit(self):
            trace.append("commit")
            raise RuntimeError("unknown commit")
        def rollback(self): trace.append("rollback")
    service = DetectorService(Repository(), None, None, None, UnitOfWork(), None)
    class Session:
        def __enter__(self): return self
        def __exit__(self, *_): pass
    leadership = SimpleNamespace(acquire=lambda: 42, ping=lambda: True)
    worker = DetectorWorker(Session, lambda *_: service, None, Event(), leadership, tmp_path / "heartbeat")
    with pytest.raises(RuntimeError, match="unknown commit"):
        worker._acquire()
    assert worker.owner is None and trace == ["publish", "commit", "rollback"]


def test_cancel_reaps_a_process_with_an_actually_loaded_jvm(monkeypatch):
    from dataclasses import asdict
    from pathlib import Path
    from threading import Event, Thread

    from app.detection import model_process
    from app.detection.contracts import ModelConfig, StaleCandidate

    cancel, loaded = Event(), Event()
    children, monitors = [], []
    original = model_process.subprocess.Popen
    def launch(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        def monitor():
            deadline = time.monotonic() + 5
            while child.poll() is None and time.monotonic() < deadline:
                try:
                    if "libjvm.so" in Path(f"/proc/{child.pid}/maps").read_text():
                        loaded.set()
                        break
                except OSError:
                    pass
                time.sleep(.01)
            cancel.set()
        thread = Thread(target=monitor)
        monitors.append(thread)
        thread.start()
        return child
    monkeypatch.setattr(model_process.subprocess, "Popen", launch)
    config = asdict(ModelConfig(dimensions=1, trees=1000))
    try:
        with pytest.raises(StaleCandidate, match="ownership_lost"):
            model_process.invoke({"action": "warmup", "config": config, "keys": ["n"],
                "rule_hash": "synthetic", "rows": [[i % 3] for i in range(4096)]}, cancel)
    finally:
        cancel.set()
        for thread in monitors:
            thread.join(timeout=6)
    assert loaded.is_set(), "The cancellation must exercise an actual JVM"
    assert children and all(child.poll() is not None for child in children)
    assert all(not thread.is_alive() for thread in monitors)


@pytest.mark.parametrize("failure", ["exception", "retry"])
def test_failed_future_is_discarded_before_reconnect(tmp_path, failure):
    from threading import Event, Timer

    from app.services.implementations.detector_worker import DetectorWorker
    stop = Event()
    class Session:
        def __enter__(self): return self
        def __exit__(self, *_): pass
    service = SimpleNamespace(candidates=lambda *_: [uuid4()])
    leadership = SimpleNamespace(ping=lambda: True, close=lambda: None)
    worker = DetectorWorker(Session, lambda *_: service, None, stop, leadership, tmp_path / "heartbeat.json")
    attempts = []
    acquisitions = []
    def acquire():
        acquisitions.append(1)
        worker.owner = object()
        return True
    def step(_):
        attempts.append(1)
        if len(attempts) == 1:
            if failure == "exception":
                raise RuntimeError("injected future failure")
            return {"state": "retry"}
        stop.set()
        return {"state": "waiting"}
    worker._acquire, worker._step = acquire, step
    timeout = Timer(5, stop.set)
    timeout.start()
    try:
        worker.run()
    finally:
        timeout.cancel()
    assert len(attempts) == 2
    assert len(acquisitions) == 2
    assert not (tmp_path / "heartbeat.json").exists()
