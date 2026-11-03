"""Ordered working-window closure around a durable WAL barrier."""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid5

from ...models import AggregateSchedule, ParsedLog, WorkingWindowPoint
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.evidence import CoverageEvidenceRepository
from ...repositories.protocols.windows import WorkingWindowRepository
from .saved_coverage import evaluate_saved_coverage
from .wal_guard import CaptureGuard
from .window_schedule import WindowSchedule


class WindowNotReady(RuntimeError):
    """A source, scheduled boundary or durable WAL barrier is not ready."""


@dataclass(frozen=True, slots=True)
class HistoricalEvent:
    event: ParsedLog
    time_basis: str = "backend_processed_at_historical"


def _barrier_id(schedule_id: UUID, index: int) -> UUID:
    return uuid5(schedule_id, f"working-window:{index}")


class WorkingWindowService:
    def __init__(self, repository: WorkingWindowRepository,
                 evidence: CoverageEvidenceRepository, capture: CaptureGuard,
                 uow: UnitOfWork) -> None:
        self.repository = repository
        self.evidence = evidence
        self.capture = capture
        self.uow = uow

    def _schedule(self, schedule_id: UUID) -> tuple[AggregateSchedule, WindowSchedule]:
        try:
            stored = self.repository.lock_schedule(schedule_id)
        except RuntimeError as error:
            raise WindowNotReady(str(error)) from error
        if stored is None:
            raise WindowNotReady("schedule_missing")
        return stored, WindowSchedule(stored.anchor_utc, stored.duration_seconds)

    def _check_capture(self) -> None:
        capture = self.capture.status()
        if not capture.ready:
            raise WindowNotReady(f"availability_capture_{capture.reason}")

    def prepare_barrier(self, schedule_id: UUID) -> UUID:
        """Commit the source-locked barrier before any WAL wait or point closure."""
        try:
            stored, schedule = self._schedule(schedule_id)
            window = schedule.window(stored.next_index)
            if self.repository.db_now() < window.scheduled_at:
                raise WindowNotReady("window_not_due")
            self._check_capture()
            if not self.repository.lock_source(stored.source_identity):
                raise WindowNotReady("source_unavailable")
            started_at = self.repository.db_now()
            barrier_id = _barrier_id(stored.id, stored.next_index)
            try:
                self.repository.ensure_barrier(barrier_id, stored.source_identity, started_at)
            except RuntimeError as error:
                raise WindowNotReady(str(error)) from error
            self.uow.commit()
            return barrier_id
        except Exception:
            self.uow.rollback()
            raise

    def close_next_window(self, schedule_id: UUID) -> WorkingWindowPoint:
        """Commit immutable point and next cursor only after barrier materialization."""
        try:
            stored, schedule = self._schedule(schedule_id)
            window = schedule.window(stored.next_index)
            self._check_capture()
            barrier = self.repository.barrier(_barrier_id(stored.id, stored.next_index))
            if (barrier is None or barrier.kind != "barrier"
                    or barrier.source_identity != stored.source_identity
                    or barrier.availability_at is None):
                raise WindowNotReady("barrier_not_materialized")
            if barrier.created_at < window.scheduled_at or barrier.availability_at < window.scheduled_at:
                raise WindowNotReady("barrier_before_schedule")
            count, event_time_min, event_time_max = self.repository.event_bounds(
                stored.source_identity, window.start, window.end,
            )
            if count > 0 and (event_time_min is None or event_time_max is None):
                raise WindowNotReady("event_timestamp_unavailable")
            completeness, reason, evidence_refs = evaluate_saved_coverage(
                self.evidence, stored.source_identity, window.start, window.end,
                barrier.availability_at,
            )
            completed_at = self.repository.db_now()
            if completed_at < barrier.created_at:
                raise WindowNotReady("database_clock_regressed")
            point = WorkingWindowPoint(
                schedule_id=stored.id, window_index=stored.next_index, start_utc=window.start,
                end_utc=window.end, scheduled_at=window.scheduled_at,
                actual_started_at=barrier.created_at, actual_completed_at=completed_at,
                event_count=count, completeness=completeness, completeness_reason=reason,
                event_time_min=event_time_min, event_time_max=event_time_max,
                evidence_refs=evidence_refs,
            )
            self.repository.save_point(point, stored)
            self.uow.commit()
            return point
        except Exception:
            self.uow.rollback()
            raise

    def historical_events(self, source_id: UUID, start: datetime, end: datetime) -> list[HistoricalEvent]:
        """Stage-1 history uses its explicitly labelled backend processing time."""
        if (start.tzinfo is None or start.utcoffset() is None or end.tzinfo is None
                or end.utcoffset() is None or start.astimezone(UTC) >= end.astimezone(UTC)):
            raise ValueError("invalid historical interval")
        return [HistoricalEvent(event) for event in
                self.repository.historical_events(source_id, start, end)]
