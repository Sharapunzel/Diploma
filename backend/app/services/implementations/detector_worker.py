"""One fenced coordinator, fair bounded steps and independently live heartbeat."""
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

from ...detection.contracts import DetectionError, StaleCandidate

LOGGER = logging.getLogger(__name__)


class DetectorWorker:
    def __init__(self, sessions, build, catalog, stop, leadership, heartbeat="/tmp/detector-worker.json"):
        self.sessions, self.build, self.catalog = sessions, build, catalog
        self.stop, self.leadership, self.heartbeat = stop, leadership, Path(heartbeat)
        self.owner = None
        self.after = None
        self.cancel = Event()
        self.phase_lock = Lock()
        self.phase, self.deadline = "starting", time.monotonic() + 30

    def _phase(self, phase, seconds=10):
        with self.phase_lock:
            self.phase, self.deadline = phase, time.monotonic() + seconds

    def _heartbeat(self):
        with self.phase_lock:
            data = {"pid": os.getpid(), "heartbeat": time.monotonic(), "phase": self.phase,
                    "deadline": self.deadline, "leader": self.owner is not None}
        temporary = self.heartbeat.with_suffix(".new")
        temporary.write_text(json.dumps(data), encoding="utf-8")
        temporary.replace(self.heartbeat)

    def _service(self, session):
        return self.build(session, self.catalog, self.owner, self.cancel)

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
        if self.stop.is_set():
            return {"state": "cancelled"}
        with self.sessions() as session:
            service = self._service(session)
            expected = None
            try:
                context = service.state(identity)
                expected = context
                detector = context["detector"]
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
                    return service.advance_one(identity)
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
        try:
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="detector") as executor:
                while not self.stop.is_set():
                    try:
                        self._heartbeat()
                        if self.owner is not None and not self.leadership.ping():
                            self.cancel.set()
                            self.owner = None
                        if pending is not None:
                            if not pending.done():
                                self.stop.wait(.2)
                                continue
                            completed = pending
                            pending = None
                            result = completed.result()
                            # A failed initial attempt stays failed. Infrastructure
                            # retry rereads durable state and never reuses a JVM candidate.
                            if result.get("state") == "retry":
                                # Initial accepted starts cannot silently resume
                                # after an interrupted transaction. Re-election
                                # resolves the commit outcome and durably fails
                                # remaining `starting` requests before more work.
                                self.cancel.set()
                                self.leadership.close()
                                self.owner = None
                                self._phase("backoff")
                                self.stop.wait(1)
                        if self.owner is None:
                            self._phase("electing", 10)
                            if not self._acquire():
                                self.stop.wait(.5)
                                continue
                        if self.stop.is_set():
                            break
                        self._phase("discovery", 35)
                        with self.sessions() as session:
                            ids = self._service(session).candidates(self.after, 1)
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
                        self.cancel.set()
                        self.leadership.close()
                        self.owner = None
                        self._phase("backoff")
                        self.stop.wait(1)
                self.cancel.set()
                self.leadership.close()
        finally:
            self.cancel.set()
            self.leadership.close()
            self.heartbeat.unlink(missing_ok=True)
            self.heartbeat.with_suffix(".new").unlink(missing_ok=True)
