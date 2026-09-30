from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...models import KafkaConnection, KafkaOperationalEvent, ProcessingError, Source


class SqlAlchemyDiagnosticsRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def sources(self, limit: int, offset: int):
        total = self.session.scalar(select(func.count()).select_from(Source)) or 0
        rows = self.session.scalars(
            select(Source).order_by(Source.id).limit(limit).offset(offset)
        ).all()
        return list(rows), total

    def source(self, source_id: UUID):
        return self.session.get(Source, source_id)

    def connection_names(self, connection_ids: list[UUID]) -> dict[UUID, str]:
        if not connection_ids:
            return {}
        rows = self.session.execute(
            select(KafkaConnection.id, KafkaConnection.name)
            .where(KafkaConnection.id.in_(connection_ids))
        ).all()
        return {connection_id: name for connection_id, name in rows}

    def errors(
        self,
        source_id: UUID | None,
        stage: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ):
        query = select(ProcessingError)
        count = select(func.count()).select_from(ProcessingError)
        conditions = []
        if source_id is not None:
            conditions.append(ProcessingError.source_identity == source_id)
        if stage is not None:
            conditions.append(ProcessingError.stage == stage)
        if start is not None:
            conditions.append(ProcessingError.backend_processed_at >= start)
        if end is not None:
            conditions.append(ProcessingError.backend_processed_at < end)
        if conditions:
            query, count = query.where(*conditions), count.where(*conditions)
        total = self.session.scalar(count) or 0
        rows = self.session.scalars(
            query.order_by(ProcessingError.backend_processed_at.desc(), ProcessingError.id.desc())
            .limit(limit)
            .offset(offset)
        ).all()
        return list(rows), total

    def error(self, error_id: UUID):
        return self.session.get(ProcessingError, error_id)

    def events(
        self,
        source_id: UUID | None,
        kind: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ):
        query = select(KafkaOperationalEvent)
        count = select(func.count()).select_from(KafkaOperationalEvent)
        conditions = []
        if source_id is not None:
            conditions.append(KafkaOperationalEvent.source_identity == source_id)
        if kind is not None:
            conditions.append(KafkaOperationalEvent.kind == kind)
        if start is not None:
            conditions.append(KafkaOperationalEvent.detected_at >= start)
        if end is not None:
            conditions.append(KafkaOperationalEvent.detected_at < end)
        if conditions:
            query, count = query.where(*conditions), count.where(*conditions)
        total = self.session.scalar(count) or 0
        rows = self.session.scalars(
            query.order_by(
                KafkaOperationalEvent.detected_at.desc(), KafkaOperationalEvent.id.desc()
            )
            .limit(limit)
            .offset(offset)
        ).all()
        return list(rows), total

    def overview(self, recent_limit: int, worker_source_ids: list[UUID]) -> dict:
        counts = self.session.execute(
            select(
                func.count(Source.id),
                func.count(Source.id).filter(Source.is_enabled.is_(True)),
                func.count(Source.id).filter(Source.is_archived.is_(True)),
            )
        ).one()
        enabled_worker_ids = (
            set(self.session.scalars(
                select(Source.id).where(
                    Source.is_enabled.is_(True), Source.id.in_(worker_source_ids)
                )
            ).all())
            if worker_source_ids
            else set()
        )
        errors = self.session.scalars(
            select(ProcessingError)
            .order_by(ProcessingError.backend_processed_at.desc(), ProcessingError.id.desc())
            .limit(recent_limit)
        ).all()
        events = self.session.scalars(
            select(KafkaOperationalEvent)
            .order_by(KafkaOperationalEvent.detected_at.desc(), KafkaOperationalEvent.id.desc())
            .limit(recent_limit)
        ).all()
        return {
            "registered_sources": counts[0],
            "enabled_sources": counts[1],
            "archived_sources": counts[2],
            "enabled_worker_source_ids": enabled_worker_ids,
            "errors": list(errors),
            "events": list(events),
        }
