"""Source-locked WAL barriers retained for capture verification and diagnostics.

Aggregate execution uses event-time snapshots; this service never calculates
features, creates points or advances an aggregate schedule.
"""

from uuid import uuid4


class WindowNotReady(RuntimeError):
    """A bounded operation is waiting for a durable prerequisite."""


class CaptureBarrierService:
    def __init__(self, repository, capture, uow):
        self.repository, self.capture, self.uow = repository, capture, uow

    def prepare(self, source_id, marker_id=None):
        try:
            state = self.capture.status()
            if not state.ready:
                raise WindowNotReady("availability_capture_" + state.reason)
            if not self.repository.lock_source(source_id):
                raise WindowNotReady("source_unavailable")
            identity = marker_id or uuid4()
            self.repository.ensure_barrier(identity, source_id, self.repository.db_now())
            self.uow.commit()
            return identity
        except Exception:
            self.uow.rollback()
            raise

    def materialized(self, marker_id):
        marker = self.repository.barrier(marker_id)
        if marker is None or marker.availability_at is None:
            raise WindowNotReady("barrier_not_materialized")
        return marker
