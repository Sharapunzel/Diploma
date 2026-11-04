"""Fair UTC scheduling with interruptible waits and bounded error backoff."""

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from ...core.errors import DomainError
from ..protocols.aggregates import AggregateWorkerServiceProtocol
from .working_windows import WindowNotReady

LOGGER = logging.getLogger(__name__)


class AggregateWorker:
    def __init__(self, sessions, build_service: Callable[..., AggregateWorkerServiceProtocol], catalog, stop):
        self.sessions, self.build, self.catalog, self.stop = sessions, build_service, catalog, stop
        self.failures = {}
        self.pending = {}

    def _advance(self, aggregate_id):
        with self.sessions() as session:
            service = self.build(session, self.catalog)
            try:
                service.advance(aggregate_id)
                service.record_worker_error(aggregate_id, None)
                return "complete"
            except WindowNotReady as error:
                service.record_worker_error(aggregate_id, str(error))
                return "waiting"
            except DomainError as error:
                service.record_worker_error(aggregate_id, error.code)
                return "failed"
            except Exception:
                LOGGER.exception("aggregate_calculation_failed", extra={"aggregate_id": str(aggregate_id)})
                service.record_worker_error(aggregate_id, "aggregate_calculation_failed")
                return "failed"

    def _finished(self, aggregate_id, state):
        if state == "failed":
            attempts = self.failures.get(aggregate_id, (0, 0))[0] + 1
            self.failures[aggregate_id] = (attempts, time.monotonic() + min(30, .25 * 2**min(attempts, 7)))
        else:
            self.failures.pop(aggregate_id, None)

    def tick(self, executor=None):
        for aggregate_id, future in list(self.pending.items()):
            if future.done():
                del self.pending[aggregate_id]
                self._finished(aggregate_id, future.result())
        with self.sessions() as session:
            service = self.build(session, self.catalog)
            due = service.due(1000)
        for aggregate_id in due:
            if self.stop.is_set():
                break
            if aggregate_id in self.pending:
                continue
            _, retry_at = self.failures.get(aggregate_id, (0, 0))
            if time.monotonic() < retry_at:
                continue
            if executor is None:
                self._finished(aggregate_id, self._advance(aggregate_id))
            elif len(self.pending) < 4:
                self.pending[aggregate_id] = executor.submit(self._advance, aggregate_id)

    def run(self):
        # Independent transactions prevent one timed-out aggregate from holding
        # the discovery loop or unrelated aggregates until its timeout expires.
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="aggregate") as executor:
            while not self.stop.is_set():
                try:
                    self.tick(executor)
                except Exception:
                    LOGGER.exception("aggregate_discovery_failed")
                    if self.stop.wait(.5):
                        break
                self.stop.wait(.05)
