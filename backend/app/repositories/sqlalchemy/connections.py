from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ...models import KafkaConnection, Source


def _like(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


class SqlAlchemyKafkaConnectionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list(self, query: str | None, limit: int, offset: int) -> tuple[list[KafkaConnection], int]:
        statement = select(KafkaConnection)
        if query:
            pattern = _like(query)
            statement = statement.where(
                or_(
                    KafkaConnection.name.ilike(pattern, escape="\\"),
                    func.array_to_string(KafkaConnection.bootstrap_servers, ",").ilike(
                        pattern, escape="\\"
                    ),
                )
            )
        total = self.session.scalar(select(func.count()).select_from(statement.subquery())) or 0
        statement = statement.order_by(KafkaConnection.name, KafkaConnection.id).limit(limit).offset(offset)
        return list(self.session.scalars(statement)), total

    def find_by_id(self, connection_id: UUID) -> KafkaConnection | None:
        return self.session.get(KafkaConnection, connection_id)

    def find_by_id_for_update(self, connection_id: UUID) -> KafkaConnection | None:
        statement = select(KafkaConnection).where(KafkaConnection.id == connection_id).with_for_update()
        return self.session.scalar(statement)

    def has_enabled_sources(self, connection_id: UUID) -> bool:
        return bool(
            self.session.scalar(
                select(Source.id).where(Source.connection_id == connection_id, Source.is_enabled.is_(True)).limit(1)
            )
        )

    def add(self, connection: KafkaConnection) -> KafkaConnection:
        self.session.add(connection)
        self.session.flush()
        return connection

    def update(self, connection: KafkaConnection, values: Mapping[str, object], at: datetime) -> None:
        for key, value in values.items():
            setattr(connection, key, value)
        connection.updated_at = at

    def delete(self, connection: KafkaConnection) -> None:
        self.session.delete(connection)


class SqlAlchemySourceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list(
        self,
        query: str | None,
        connection_id: UUID | None,
        normalizer_id: UUID | None,
        is_enabled: bool | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Source], int]:
        statement = select(Source)
        conditions = []
        if query:
            pattern = _like(query)
            conditions.append(
                or_(
                    Source.name.ilike(pattern, escape="\\"),
                    Source.topic_name.ilike(pattern, escape="\\"),
                )
            )
        if connection_id is not None:
            conditions.append(Source.connection_id == connection_id)
        if normalizer_id is not None:
            conditions.append(Source.normalizer_id == normalizer_id)
        if is_enabled is not None:
            conditions.append(Source.is_enabled == is_enabled)
        if conditions:
            statement = statement.where(*conditions)
        total = self.session.scalar(select(func.count()).select_from(statement.subquery())) or 0
        statement = statement.order_by(Source.name, Source.id).limit(limit).offset(offset)
        return list(self.session.scalars(statement)), total

    def find_by_id(self, source_id: UUID) -> Source | None:
        return self.session.get(Source, source_id)

    def find_by_id_for_update(self, source_id: UUID) -> Source | None:
        statement = select(Source).where(Source.id == source_id).with_for_update()
        return self.session.scalar(statement)

    def add(self, source: Source) -> Source:
        self.session.add(source)
        self.session.flush()
        return source

    def update(self, source: Source, values: Mapping[str, object], at: datetime) -> None:
        for key, value in values.items():
            setattr(source, key, value)
        source.updated_at = at

    def delete(self, source: Source) -> None:
        self.session.delete(source)
