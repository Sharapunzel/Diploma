"""Fair UTC scheduling with interruptible waits and bounded error backoff."""

import logging
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime

from ...aggregation.event_time import MAX_SCAN_PAGE
from ...aggregation.worker_admission import WorkerAdmission
from ...core.errors import DomainError
from ..protocols.aggregates import AggregateWorkerServiceProtocol
from .working_windows import WindowNotReady

LOGGER = logging.getLogger(__name__)


class AggregateWorker:
    def __init__(self, sessions, build_service: Callable[..., AggregateWorkerServiceProtocol], catalog, stop,
                 operation_memory_mb=256, observe=None):
        self.sessions, self.build, self.catalog, self.stop = sessions, build_service, catalog, stop
        self.failures = {}
        self.pending = {}
        self.admission = WorkerAdmission(operation_memory_mb)
        self.maintenance_failures = {}
        self.steps = deque(maxlen=64)
        self.observe = observe

    def _advance(self, aggregate_id, *, execution_started=None):
        with self.sessions() as session:
            service = self.build(session, self.catalog)
            try:
                result = service.advance(aggregate_id, execution_started=execution_started)
                state = result.get("state") if isinstance(result, dict) else result
                if state not in {"closed", "already_closed", "candidate_cancelled", "scan_cancelled", "attempt_running"}:
                    service.record_worker_error(aggregate_id, None)
                if isinstance(result, dict) and not result.get("work_available") and (
                    result.get("state") in {"waiting_close_deadline", "history_waiting", "paused", "retired",
                        "archived", "deleted", "failed", "candidate_cancelled", "scan_cancelled", "attempt_running"}
                    or result.get("state") == "scanned" and result.get("records", MAX_SCAN_PAGE) < MAX_SCAN_PAGE
                ):
                    return {"state": "waiting"}
                return {"state": "complete", "result": result}
            except WindowNotReady as error:
                service.record_worker_error(aggregate_id, str(error))
                return "waiting"
            except DomainError as error:
                if error.code not in {"cleanup_in_progress", "aggregate_time_semantics_retired", "aggregate_deleted",
                        "aggregate_inactive", "aggregate_recovery_required"}:
                    service.record_worker_error(aggregate_id, error.code)
                return "failed"
            except Exception:
                LOGGER.exception("aggregate_calculation_failed", extra={"aggregate_id": str(aggregate_id)})
                service.record_worker_error(aggregate_id, "aggregate_calculation_failed")
                return "failed"

    def _maintain(self, aggregate_id):
        with self.sessions() as session:
            return self.build(session, self.catalog).maintain(aggregate_id)

    def _finished(self, aggregate_id, state):
        if state == "failed":
            attempts = self.failures.get(aggregate_id, (0, 0))[0] + 1
            self.failures[aggregate_id] = (attempts, time.monotonic() + min(30, .25 * 2**min(attempts, 7)))
        elif state == "waiting":
            # Completed diagnostic cycles/no-work turns do not need continuous
            # source scans and mutex traffic. Durable cursors stay in services.
            self.failures[aggregate_id] = (0, time.monotonic() + .25)
        else:
            self.failures.pop(aggregate_id, None)

    def _execute(self, job):
        started = time.monotonic()
        started_at = datetime.now(UTC).isoformat()
        try:
            result = self._advance(job.aggregate_id, execution_started=started) if job.kind == "foreground" else self._maintain(job.aggregate_id)
            return {"result": result, "started": started, "completed": time.monotonic(),
                "started_at": started_at, "completed_at": datetime.now(UTC).isoformat()}
        except Exception:
            LOGGER.exception("aggregate_task_failed", extra={"aggregate_id": str(job.aggregate_id), "kind": job.kind})
            return {"result": "failed", "started": started, "completed": time.monotonic(),
                "started_at": started_at, "completed_at": datetime.now(UTC).isoformat()}

    def _completed(self, aggregate_id, outcome):
        job = self.admission.release(aggregate_id)
        result = outcome["result"]
        if job.kind == "foreground":
            self._finished(aggregate_id, result.get("state") if isinstance(result, dict) else result)
        elif result == "failed":
            # A failure before the service could persist its own retry (for
            # example opening a broken DB connection) must not busy-spin.
            attempts = self.maintenance_failures.get(aggregate_id, (0, 0))[0] + 1
            self.maintenance_failures[aggregate_id] = (attempts, time.monotonic() + min(60, 2**min(attempts - 1, 6)))
        else:
            self.maintenance_failures.pop(aggregate_id, None)
        details = result.get("result") if isinstance(result, dict) else None
        self.steps.append({"aggregate_id": str(aggregate_id), "kind": job.kind,
            "started_at": outcome["started_at"], "completed_at": outcome["completed_at"],
            "queue_seconds": max(0, outcome["started"] - job.queued_at),
            "total_seconds": max(0, outcome["completed"] - outcome["started"]),
            "state": result.get("state", result.get("status")) if isinstance(result, dict) else result,
            "point_id": str(details["point_id"]) if isinstance(details, dict) and details.get("point_id") else None,
            "origin": details.get("origin") if isinstance(details, dict) else None,
            "timings": details.get("timings") if isinstance(details, dict) else None})

    def _discover(self):
        now = time.monotonic()
        with self.sessions() as session:
            service = self.build(session, self.catalog)
            # One waiting maintenance identity reserves bounded queue space,
            # even under continuous foreground backlog. Both kinds may queue
            # for one target, but admission serializes their execution.
            if self.admission.available and not any(job.kind == "maintenance" for job in self.admission.ready):
                excluded = self.admission.excluded("maintenance") | {
                    identity for identity, (_, retry) in self.maintenance_failures.items() if now < retry}
                self.admission.offer("maintenance", service.maintenance_due(1, exclude_ids=excluded), now)
            if self.admission.available:
                excluded = self.admission.excluded("foreground") | {
                    identity for identity, (_, retry) in self.failures.items() if now < retry}
                self.admission.offer("foreground", service.due(self.admission.available, exclude_ids=excluded), now)

    def tick(self, executor=None):
        for aggregate_id, future in list(self.pending.items()):
            if future.done():
                del self.pending[aggregate_id]
                self._completed(aggregate_id, future.result())
        self._discover()
        while not self.stop.is_set():
            job = self.admission.acquire()
            if job is None:
                break
            if executor is None:
                self._completed(job.aggregate_id, self._execute(job))
            else:
                try:
                    self.pending[job.aggregate_id] = executor.submit(self._execute, job)
                except Exception:
                    self.admission.release(job.aggregate_id)
                    raise
        if self.observe:
            self.observe({"admission": self.admission.summary(), "steps": list(self.steps)})

    def run(self):
        # Independent transactions prevent one timed-out aggregate from holding
        # the discovery loop or unrelated aggregates until its timeout expires.
        with ThreadPoolExecutor(max_workers=self.admission.capacity, thread_name_prefix="aggregate") as executor:
            while not self.stop.is_set():
                try:
                    self.tick(executor)
                except Exception:
                    LOGGER.exception("aggregate_discovery_failed")
                    if self.stop.wait(.5):
                        break
                if self.pending:
                    # Wake as soon as a bounded task completes. A fixed sleep
                    # after every turn otherwise adds 3*50ms to each window,
                    # even with zero SQL cost and a ready durable backlog.
                    wait(tuple(self.pending.values()), timeout=.05, return_when=FIRST_COMPLETED)
                else:
                    self.stop.wait(.05)
