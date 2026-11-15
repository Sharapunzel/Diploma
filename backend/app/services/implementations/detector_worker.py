"""One fenced coordinator, fair bounded steps and independently live heartbeat."""
import json
import logging
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

from ...detection.contracts import DetectionError, StaleCandidate

LOGGER = logging.getLogger(__name__)


class DetectorWorker:
    def __init__(self, sessions, build, catalog, stop, leadership, heartbeat="/tmp/detector-worker.json",
                 release_runtime=lambda: None):
        self.sessions, self.build, self.catalog = sessions, build, catalog
        self.stop, self.leadership, self.heartbeat = stop, leadership, Path(heartbeat)
        self.owner = None
        self.after = None
        self.cancel = Event()
        self.release_runtime = release_runtime
        self.phase_lock = Lock()
        self.phase, self.deadline = "starting", time.monotonic() + 30
        self.steps = deque(maxlen=48)
        self.startup_steps = deque(maxlen=16)

    def _phase(self, phase, seconds=10):
        with self.phase_lock:
            self.phase, self.deadline = phase, time.monotonic() + seconds

    def _heartbeat(self):
        with self.phase_lock:
            data = {"pid": os.getpid(), "heartbeat": time.monotonic(), "phase": self.phase,
                    "deadline": self.deadline, "leader": self.owner is not None,
                    "steps": list(self.steps), "startup_steps": list(self.startup_steps)}
        temporary = self.heartbeat.with_suffix(".new")
        temporary.write_text(json.dumps(data), encoding="utf-8")
        temporary.replace(self.heartbeat)

    def _service(self, session):
        return self.build(session, self.catalog, self.owner, self.cancel)

    def _cancel(self):
        self.cancel.set()
        self.release_runtime()

    def _acquire(self):
        pid = self.leadership.acquire()
        if pid is None:
            self._phase("standby")
            return False
        with self.sessions() as session:
            self.owner = self._service(session).acquire_owner(uuid4(), pid)
        self.cancel.clear()
        # Recovery boundaries are committed before any computation. Bounded pages
        # make startup cancellable and never turn a standby poll into recovery.
        after = None
        while not self.stop.is_set():
            self._phase("recovery", 35)
            with self.sessions() as session:
                ids = self._service(session).candidates(after)
            if not ids:
                break
            for identity in ids:
                self._phase("recovery", 35)
                if self.stop.is_set() or not self.leadership.ping():
                    raise StaleCandidate("ownership_lost")
                with self.sessions() as session:
                    self._service(session).recover(identity)
                self._heartbeat()
            after = ids[-1]
        return True

    def _step(self, identity):
        started = time.monotonic()
        observation = {"detector_id": str(identity), "started_at": time.time()}
        try:
            result = self._execute_step(identity, observation)
            observation["result"] = result.get("state")
            return result
        finally:
            observation["seconds"] = time.monotonic() - started
            with self.phase_lock:
                self.steps.append(observation)
                if observation.get("execution_state") in {"requested", "starting"}:
                    self.startup_steps.append(observation)

    def _execute_step(self, identity, observation):
        if self.stop.is_set():
            return {"state": "cancelled"}
        with self.sessions() as session:
            service = self._service(session)
            expected = None
            try:
                reading = time.monotonic()
                context = service.state(identity)
                observation["state_seconds"] = time.monotonic() - reading
                expected = context
                detector = context["detector"]
                observation.update(execution_state=detector["execution_state"],
                    consumed_index=detector.get("consumed_index"), evaluated_index=detector.get("evaluated_index"),
                    backlog=context.get("diagnostics", {}).get("backlog"))
                if context["schedule"].get("paused") or context["schedule"].get("active_cleanup_id"):
                    return {"state": "waiting", "reason": "aggregate_paused"}
                if detector["execution_state"] == "requested":
                    self._phase("accepting_start", 35)
                    return service.begin_start(identity)
                if detector["execution_state"] == "starting":
                    if detector["checkpoint"] is not None:
                        self._phase("resuming", 35)
                        return service.resume(identity)
                    if context.get("warmup", {}).get("status") == "prepared":
                        self._phase("warming", 125)
                        return service.warmup(identity)
                    self._phase("preparing", 35)
                    return service.prepare_warmup(identity)
                if detector["execution_state"] == "running":
                    self._phase("evaluating", 125)
                    result = service.advance_one(identity)
                    timings = getattr(service, "transition_timings", None)
                    if timings is not None:
                        observation["transition"] = timings
                    return result
                return {"state": "waiting"}
            except StaleCandidate:
                session.rollback()
                return {"state": "stale"}
            except (DetectionError, ValueError) as error:
                session.rollback()
                try:
                    # An operation may already have committed its safe failure.
                    # Never reread a newer generation to diagnose an old candidate.
                    if expected is not None:
                        service.fail(identity, error, expected)
                except DetectionError:
                    session.rollback()
                except Exception:  # noqa: BLE001 - diagnosis itself was not confirmed by DB
                    session.rollback()
                    LOGGER.warning("detector_diagnosis_temporary_unavailable")
                    return {"state": "retry"}
                return {"state": "failed"}
            except Exception:  # noqa: BLE001 - infrastructure failure has no unconfirmed diagnosis
                session.rollback()
                LOGGER.warning("detector_step_temporary_unavailable")
                return {"state": "retry"}

    def run(self):
        pending = None
        cycle_progress = False
        try:
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="detector") as executor:
                while not self.stop.is_set():
                    try:
                        self._heartbeat()
                        if self.owner is not None and not self.leadership.ping():
                            self._cancel()
                            self.owner = None
                        if pending is not None:
                            if not pending.done():
                                # Completion wakes dispatch immediately; the
                                # timeout only bounds heartbeat/ownership polls.
                                wait([pending], timeout=.2)
                                continue
                            completed = pending
                            pending = None
                            result = completed.result()
                            cycle_progress |= result.get("state") in {
                                "starting", "prepared", "ready", "evaluated", "skipped"}
                            # A failed initial attempt stays failed. Infrastructure
                            # retry rereads durable state and never reuses a JVM candidate.
                            if result.get("state") == "retry":
                                # Initial accepted starts cannot silently resume
                                # after an interrupted transaction. Re-election
                                # resolves the commit outcome and durably fails
                                # remaining `starting` requests before more work.
                                self._cancel()
                                self.leadership.close()
                                self.owner = None
                                self._phase("backoff")
                                self.stop.wait(1)
                        if self.owner is None:
                            cycle_progress = False
                            self._phase("electing", 10)
                            if not self._acquire():
                                self.stop.wait(.5)
                                continue
                        if self.stop.is_set():
                            break
                        self._phase("discovery", 35)
                        with self.sessions() as session:
                            ids = self._service(session).candidates(self.after, 1)
                            if not ids and self.after is not None:
                                self.after = None
                                if not cycle_progress:
                                    # All detectors waited: back off once per
                                    # complete idle sweep, never between work.
                                    self._phase("waiting")
                                    self.stop.wait(.25)
                                cycle_progress = False
                                ids = self._service(session).candidates(None, 1)
                        if self.stop.is_set():
                            break
                        if not ids:
                            self.after = None
                            self._phase("waiting")
                            self.stop.wait(.25)
                            continue
                        self.after = ids[0]
                        self._phase("dispatching", 35)
                        pending = executor.submit(self._step, ids[0])
                    except Exception:  # noqa: BLE001 - reconnect only from confirmed DB facts
                        LOGGER.warning("detector_coordinator_temporary_unavailable")
                        self._cancel()
                        self.leadership.close()
                        self.owner = None
                        self._phase("backoff")
                        self.stop.wait(1)
                self._cancel()
                self.leadership.close()
        finally:
            self._cancel()
            self.leadership.close()
            self.heartbeat.unlink(missing_ok=True)
            self.heartbeat.with_suffix(".new").unlink(missing_ok=True)
