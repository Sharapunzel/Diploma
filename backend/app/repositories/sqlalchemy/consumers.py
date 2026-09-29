from __future__ import annotations

from uuid import UUID

from sqlalchemy import exists, select, update
from sqlalchemy.orm import Session

from ...models import KafkaConnection, ProcessedKafkaRecord, Source
from ..protocols.consumers import ConsumerSourceConfig


class SqlAlchemyConsumerStateRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    @staticmethod
    def _config(source: Source, connection: KafkaConnection) -> ConsumerSourceConfig:
        return ConsumerSourceConfig(
            source_id=source.id,
            connection_id=connection.id,
            bootstrap_servers=tuple(connection.bootstrap_servers),
            security_protocol=connection.security_protocol,
            topic_name=source.topic_name,
            kafka_topic_identity=source.kafka_topic_identity,
        )

    def list_enabled_sources(self) -> list[ConsumerSourceConfig]:
        rows = self.session.execute(
            select(Source, KafkaConnection)
            .join(KafkaConnection, KafkaConnection.id == Source.connection_id)
            .where(Source.is_enabled.is_(True))
            .order_by(Source.id)
        )
        return [self._config(source, connection) for source, connection in rows]

    def get_enabled_source(self, source_id: UUID) -> ConsumerSourceConfig | None:
        row = self.session.execute(
            select(Source, KafkaConnection)
            .join(KafkaConnection, KafkaConnection.id == Source.connection_id)
            .where(Source.id == source_id, Source.is_enabled.is_(True))
        ).one_or_none()
        return None if row is None else self._config(*row)

    def has_durable_history(
        self, connection_id: UUID, topic_name: str, partition: int | None = None
    ) -> bool:
        conditions = [
            ProcessedKafkaRecord.connection_identity == connection_id,
            ProcessedKafkaRecord.kafka_topic == topic_name,
        ]
        if partition is not None:
            conditions.append(ProcessedKafkaRecord.kafka_partition == partition)
        return bool(self.session.scalar(select(exists().where(*conditions))))

    def durable_topic_identities(
        self, connection_id: UUID, topic_name: str
    ) -> set[str | None]:
        return set(
            self.session.scalars(
                select(ProcessedKafkaRecord.kafka_topic_identity).where(
                    ProcessedKafkaRecord.connection_identity == connection_id,
                    ProcessedKafkaRecord.kafka_topic == topic_name,
                )
            )
        )

    def set_topic_identity_if_current(
        self, source_id: UUID, expected: str | None, identity: str
    ) -> bool:
        statement = update(Source).where(
            Source.id == source_id,
            Source.is_enabled.is_(True),
        )
        if expected is None:
            statement = statement.where(Source.kafka_topic_identity.is_(None))
        else:
            statement = statement.where(Source.kafka_topic_identity == expected)
        result = self.session.execute(
            statement.values(kafka_topic_identity=identity)
        )
        return result.rowcount == 1
