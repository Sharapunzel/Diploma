"""SQLAlchemy operations for ordered, source-locked working windows."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ...models import (
    AvailabilityMarker,
    Source,
)


class SqlAlchemyWorkingWindowRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def db_now(self) -> datetime:
        return self.session.scalar(select(func.clock_timestamp()))

    def lock_source(self, source_id: UUID) -> bool:
        source = self.session.scalar(select(Source).where(Source.id == source_id)
            .with_for_update().execution_options(populate_existing=True))
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
