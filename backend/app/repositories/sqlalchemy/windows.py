"""SQLAlchemy operations for ordered, source-locked working windows."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ...models import AggregateSchedule, AvailabilityMarker, ParsedLog, Source, WorkingWindowPoint


class SqlAlchemyWorkingWindowRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def lock_schedule(self, schedule_id: UUID) -> AggregateSchedule | None:
        if self.session.scalar(select(func.current_setting("transaction_isolation"))) != "read committed":
            raise RuntimeError("read_committed_required")
        return self.session.scalar(select(AggregateSchedule).where(
            AggregateSchedule.id == schedule_id).with_for_update())

    def db_now(self) -> datetime:
        return self.session.scalar(select(func.clock_timestamp()))

    def lock_source(self, source_id: UUID) -> bool:
        source = self.session.scalar(select(Source).where(Source.id == source_id).with_for_update())
        return source is not None and source.source_type == "kafka"

    def ensure_barrier(self, marker_id: UUID, source_id: UUID, at: datetime) -> None:
        self.session.execute(insert(AvailabilityMarker).values(
            id=marker_id, source_identity=source_id, kind="barrier", event_id=None,
            created_at=at,
        ).on_conflict_do_nothing(index_elements=[AvailabilityMarker.id]))
        marker = self.session.get(AvailabilityMarker, marker_id)
        if marker is None or marker.source_identity != source_id or marker.kind != "barrier":
            raise RuntimeError("barrier_conflict")

    def barrier(self, marker_id: UUID) -> AvailabilityMarker | None:
        return self.session.get(AvailabilityMarker, marker_id)

    def event_bounds(self, source_id: UUID, start: datetime, end: datetime) -> tuple[int, datetime | None, datetime | None]:
        occurred = ParsedLog.ecs_data["@timestamp"].astext.cast(DateTime(timezone=True))
        return self.session.execute(select(func.count(), func.min(occurred), func.max(occurred))
            .select_from(AvailabilityMarker).join(
            ParsedLog, ParsedLog.id == AvailabilityMarker.event_id).where(
            AvailabilityMarker.source_identity == source_id,
            AvailabilityMarker.kind == "event",
            AvailabilityMarker.availability_at >= start,
            AvailabilityMarker.availability_at < end,
        )).one()

    def save_point(self, point: WorkingWindowPoint, schedule: AggregateSchedule) -> None:
        self.session.add(point)
        schedule.next_index += 1
        self.session.flush()

    def historical_events(self, source_id: UUID, start: datetime, end: datetime) -> list[ParsedLog]:
        return list(self.session.scalars(select(ParsedLog).where(
            ParsedLog.source_id == source_id,
            ParsedLog.backend_processed_at >= start,
            ParsedLog.backend_processed_at < end,
        ).order_by(ParsedLog.backend_processed_at, ParsedLog.id)))
