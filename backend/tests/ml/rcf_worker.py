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

    from app.adapters.model_runtime import ModelRuntime
    from app.detection.contracts import StaleCandidate
    cancel = Event()
    cancel.set()
    with pytest.raises(StaleCandidate, match="ownership_lost"):
        ModelRuntime().invoke({"action": "warmup"}, cancel)


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

    from app.adapters import model_runtime
    from app.detection.contracts import ModelConfig, StaleCandidate

    cancel, loaded = Event(), Event()
    children, monitors = [], []
    original = model_runtime.subprocess.Popen
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
    monkeypatch.setattr(model_runtime.subprocess, "Popen", launch)
    config = asdict(ModelConfig(dimensions=1, trees=1000))
    runtime = model_runtime.ModelRuntime()
    try:
        with pytest.raises(StaleCandidate, match="ownership_lost"):
            runtime.invoke({"action": "warmup", "config": config, "keys": ["n"],
                "rule_hash": "synthetic", "rows": [[i % 3] for i in range(4096)]}, cancel)
    finally:
        cancel.set()
        runtime.close()
        for thread in monitors:
            thread.join(timeout=6)
    assert loaded.is_set(), "The cancellation must exercise an actual JVM"
    assert children and all(child.poll() is not None for child in children)
    assert all(not thread.is_alive() for thread in monitors)


def test_reused_runtime_matches_cold_checkpoints_and_discards_uncommitted_candidates(capsys):
    from dataclasses import asdict
    from pathlib import Path

    from app.adapters.model_runtime import ModelRuntime
    from app.detection.contracts import ModelConfig, canonical, digest
    from app.detection.model_process import checked_response

    def cold(request):
        started = time.monotonic()
        result = subprocess.run([sys.executable, "-m", "app.detection.model_process"],
            input=canonical(request), capture_output=True, text=True, timeout=120, check=False)
        assert result.returncode == 0, "cold model reference failed"
        reply = json.loads(result.stdout)
        return checked_response(reply["response"]), {
            "rpc_seconds": time.monotonic() - started, **reply["timings"]}

    runtime, checkpoints, configs, keys = ModelRuntime(), {}, {}, {}
    profiles, grades = [], []
    try:
        assert runtime.pid is None
        for identity, dimensions in (("A", 1), ("B", 2)):
            configs[identity] = asdict(ModelConfig(dimensions=dimensions, trees=20,
                sample_size=8, output_after=8, seed=42 if identity == "A" else 17))
            keys[identity] = [f"x{i}" for i in range(dimensions)]
            request = {"action": "warmup", "config": configs[identity], "keys": keys[identity],
                "rule_hash": identity, "rows": [[(i % 3) + j for j in range(dimensions)] for i in range(16)]}
            expected, timing = cold(request)
            actual = checked_response(runtime.invoke(request))
            assert actual == expected
            checkpoints[identity] = actual["checkpoint"]
            profiles.append({"action": "warmup", "cold": timing, "managed": runtime.last_timings})
        pid = runtime.pid
        rss_before = int(next(line.split()[1] for line in Path(f"/proc/{pid}/status").read_text().splitlines()
            if line.startswith("VmRSS:")))
        for step, identity in enumerate(["A", "B"] * 5):
            committed = checkpoints[identity]
            request = {"action": "advance", "config": configs[identity], "keys": keys[identity],
                "rule_hash": identity, "checkpoint": committed,
                "values": [100 if step < 2 else step % 3] * len(keys[identity])}
            expected, timing = cold(request)
            actual = checked_response(runtime.invoke(request))
            assert actual == expected  # result, decision, explanation AND full next mapper/hash
            assert runtime.pid == pid
            grades.append(actual["result"]["grade"])
            profiles.append({"action": "advance", "cold": timing, "managed": runtime.last_timings})
            if step in {0, 1, 6}:
                # Rejected/unknown commit: the durable DB checkpoint is unchanged.
                # Even a mutated candidate in the child may not affect this replay.
                replay = checked_response(runtime.invoke(request))
                assert replay == expected and digest(committed["body"]) == committed["sha256"]
            else:
                checkpoints[identity] = actual["checkpoint"]
        assert any(grade > 0 for grade in grades)
        # No model/input cache or unbounded response retention over repeated restores.
        for _ in range(20):
            assert checked_response(runtime.invoke(request)) == expected
        rss_after = int(next(line.split()[1] for line in Path(f"/proc/{pid}/status").read_text().splitlines()
            if line.startswith("VmRSS:")))
        assert rss_after < 800 * 1024 and rss_after - rss_before < 256 * 1024
        with capsys.disabled():
            print("Model runtime cold/warm stages:", canonical({"requests": profiles,
                "rss_kib_before": rss_before, "rss_kib_after": rss_after}), flush=True)
    finally:
        runtime.close()
    assert runtime.pid is None


@pytest.mark.parametrize("reason", ["shutdown", "ownership_lost"])
def test_coordinator_releases_runtime_before_waiting_for_pending_job(tmp_path, reason):
    from threading import Event, Thread

    from app.services.implementations.detector_worker import DetectorWorker

    stop, entered, released, owner_alive = Event(), Event(), Event(), Event()
    owner_alive.set()
    class Session:
        def __enter__(self): return self
        def __exit__(self, *_): pass
    def acquire():
        if not owner_alive.is_set():
            stop.set()
            return False
        worker.owner = object()
        return True
    worker = DetectorWorker(Session, lambda *_: SimpleNamespace(candidates=lambda *_: [uuid4()]),
        None, stop, SimpleNamespace(ping=owner_alive.is_set, close=lambda: None),
        tmp_path / "heartbeat.json", release_runtime=released.set)
    worker._acquire = acquire
    def step(_):
        entered.set()
        assert released.wait(3), "runtime must be cancelled before executor shutdown waits"
        assert worker.cancel.is_set()
        return {"state": "stale"}
    worker._step = step
    thread = Thread(target=worker.run)
    thread.start()
    try:
        assert entered.wait(3)
        if reason == "shutdown":
            stop.set()
        else:
            owner_alive.clear()
        thread.join(timeout=3)
        assert not thread.is_alive() and released.is_set()
        assert not worker.heartbeat.exists()
    finally:
        stop.set()
        released.set()
        thread.join(timeout=3)


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


@pytest.mark.parametrize("outcome", ["evaluated", "waiting"])
def test_three_detectors_dispatch_on_completion_and_wrap_without_idle(tmp_path, monkeypatch, outcome):
    """Controlled completions expose scheduler delay without wall-clock flakes."""
    from concurrent.futures import Future

    from app.services.implementations import detector_worker as module

    identities = sorted([uuid4() for _ in range(3)])
    clock, calls, queued = [0.0], [], []

    class Executor:
        def __init__(self, **kwargs):
            assert kwargs["max_workers"] == 1
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def submit(self, fn, identity):
            assert not queued  # one candidate at a time, including wraparound
            future = Future()
            queued.append((future, fn, identity))
            return future

    def complete():
        if queued:
            future, fn, identity = queued.pop()
            future.set_result(fn(identity))

    class Stop:
        stopped = False
        def set(self): self.stopped = True
        def is_set(self): return self.stopped
        def wait(self, seconds):
            clock[0] += seconds
            complete()

    def completion_wait(futures, timeout):
        assert len(futures) == 1 and timeout <= .2
        clock[0] += .01  # the candidate completes between heartbeat ticks
        complete()

    class Session:
        def __enter__(self): return self
        def __exit__(self, *_): pass

    stop = Stop()
    service = SimpleNamespace(candidates=lambda after, limit: [i for i in identities if after is None or i > after][:limit])
    worker = module.DetectorWorker(Session, lambda *_: service, None, stop,
        SimpleNamespace(ping=lambda: True, close=lambda: None), tmp_path / "heartbeat.json")
    worker.owner = object()

    def step(identity):
        calls.append((identity, clock[0]))
        if len(calls) == 6:
            stop.set()
        return {"state": outcome}

    worker._step = step
    monkeypatch.setattr(module, "ThreadPoolExecutor", Executor)
    monkeypatch.setattr(module, "wait", completion_wait)
    worker.run()
    assert [i for i, _ in calls] == identities * 2
    if outcome == "evaluated":
        assert calls[-1][1] <= .061  # no poll/wrap delay while making progress
    else:
        assert .25 <= calls[-1][1] <= .4  # bounded idle backoff, never a busy loop
    assert not queued
