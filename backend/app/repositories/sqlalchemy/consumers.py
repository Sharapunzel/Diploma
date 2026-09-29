from __future__ import annotations

from uuid import UUID

from sqlalchemy import exists, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ...models import KafkaConnection, KafkaOperationalEvent, ProcessedKafkaRecord, Source
from ..protocols.consumers import ConsumerSourceConfig


class RetentionGapConflict(RuntimeError):
    """Observed watermark contradicts a previously recorded gap."""


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
            .where(Source.is_enabled.is_(True), Source.is_archived.is_(False))
            .order_by(Source.id)
        )
        return [self._config(source, connection) for source, connection in rows]

    def get_enabled_source(self, source_id: UUID) -> ConsumerSourceConfig | None:
        row = self.session.execute(
            select(Source, KafkaConnection)
            .join(KafkaConnection, KafkaConnection.id == Source.connection_id)
            .where(Source.id == source_id, Source.is_enabled.is_(True), Source.is_archived.is_(False))
        ).one_or_none()
        return None if row is None else self._config(*row)

    def has_durable_history(
        self, connection_id: UUID, topic_name: str, partition: int | None = None,
        topic_id: str | None = None,
    ) -> bool:
        conditions = [
            ProcessedKafkaRecord.connection_identity == connection_id,
            ProcessedKafkaRecord.kafka_topic == topic_name,
        ]
        if partition is not None:
            conditions.append(ProcessedKafkaRecord.kafka_partition == partition)
        if topic_id is not None:
            conditions.append(ProcessedKafkaRecord.kafka_topic_identity == topic_id)
        if self.session.scalar(select(exists().where(*conditions))):
            return True
        gap_conditions = [
            KafkaOperationalEvent.kind == "retention_gap",
            KafkaOperationalEvent.connection_identity == connection_id,
            KafkaOperationalEvent.topic_name == topic_name,
        ]
        if partition is not None:
            gap_conditions.append(KafkaOperationalEvent.kafka_partition == partition)
        if topic_id is not None:
            gap_conditions.append(KafkaOperationalEvent.old_topic_identity == topic_id)
        return bool(self.session.scalar(select(exists().where(*gap_conditions))))

    def known_gap_end(self, connection_id: UUID, topic_id: str, partition: int) -> int | None:
        return self.session.scalar(select(func.max(KafkaOperationalEvent.offset_end)).where(
            KafkaOperationalEvent.kind == "retention_gap",
            KafkaOperationalEvent.connection_identity == connection_id,
            KafkaOperationalEvent.old_topic_identity == topic_id,
            KafkaOperationalEvent.kafka_partition == partition,
        ))

    def has_legacy_history(self, connection_id: UUID, topic_name: str) -> bool:
        return bool(self.session.scalar(select(exists().where(
            ProcessedKafkaRecord.connection_identity == connection_id,
            ProcessedKafkaRecord.kafka_topic == topic_name,
            ProcessedKafkaRecord.kafka_topic_identity.is_(None),
        ))))

    def bind_cluster(self, connection_id: UUID, cluster_id: str) -> bool:
        connection = self.session.scalar(
            select(KafkaConnection).where(KafkaConnection.id == connection_id).with_for_update()
        )
        if connection is None or (connection.cluster_identity is not None and connection.cluster_identity != cluster_id):
            return False
        if connection.cluster_identity is None:
            connection.cluster_identity = cluster_id
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                return False
        return True

    def record_retention_gap(
        self, source_id: UUID, topic_id: str, partition: int, start: int, end: int
    ) -> bool:
        source = self.session.scalar(select(Source).where(Source.id == source_id).with_for_update())
        if source is None or source.is_archived or not source.is_enabled or source.kafka_topic_identity != topic_id:
            return False
        connection = self.session.get(KafkaConnection, source.connection_id)
        if connection is None or not connection.cluster_identity:
            return False
        known_end = self.known_gap_end(connection.id, topic_id, partition)
        if known_end is not None and end < known_end:
            raise RetentionGapConflict
        covered = start
        intervals = self.session.execute(
            select(KafkaOperationalEvent.offset_start, KafkaOperationalEvent.offset_end)
            .where(
                KafkaOperationalEvent.kind == "retention_gap",
                KafkaOperationalEvent.source_identity == source_id,
                KafkaOperationalEvent.old_topic_identity == topic_id,
                KafkaOperationalEvent.kafka_partition == partition,
                KafkaOperationalEvent.offset_end > start,
                KafkaOperationalEvent.offset_start < end,
            ).order_by(KafkaOperationalEvent.offset_start)
        )
        for interval_start, interval_end in intervals:
            if interval_start > covered:
                self._add_gap(source, connection, topic_id, partition, covered, interval_start)
            covered = max(covered, interval_end)
        if covered < end:
            self._add_gap(source, connection, topic_id, partition, covered, end)
        self.session.commit()
        return True

    def _add_gap(self, source, connection, topic_id, partition, start, end) -> None:
        self.session.add(KafkaOperationalEvent(
            kind="retention_gap", reason_code="offset_below_low_watermark",
            source_id=source.id, source_identity=source.id, source_name=source.name,
            connection_id=connection.id, connection_identity=connection.id,
            connection_name=connection.name, cluster_identity=connection.cluster_identity,
            topic_name=source.topic_name, old_topic_identity=topic_id,
            kafka_partition=partition, offset_start=start, offset_end=end,
        ))

    def archive_recreated(self, source_id: UUID, old_id: str, new_id: str) -> bool:
        source = self.session.scalar(select(Source).where(Source.id == source_id).with_for_update())
        if source is None or source.is_archived or source.kafka_topic_identity != old_id:
            return False
        connection = self.session.get(KafkaConnection, source.connection_id)
        if connection is None or not connection.cluster_identity:
            return False
        self.session.add(KafkaOperationalEvent(
            kind="topic_recreated", reason_code="topic_identity_changed",
            source_id=source.id, source_identity=source.id, source_name=source.name,
            connection_id=connection.id, connection_identity=connection.id,
            connection_name=connection.name, cluster_identity=connection.cluster_identity,
            topic_name=source.topic_name, old_topic_identity=old_id,
            new_topic_identity=new_id,
        ))
        source.is_enabled = False
        source.is_archived = True
        self.session.flush()
        return True

    def durable_topic_identities(
        self, source_id: UUID
    ) -> set[str | None]:
        return set(
            self.session.scalars(
                select(ProcessedKafkaRecord.kafka_topic_identity).where(
                    ProcessedKafkaRecord.source_id == source_id,
                )
            )
        )

    def set_topic_identity_if_current(
        self, source_id: UUID, expected: str | None, identity: str
    ) -> bool:
        statement = update(Source).where(
            Source.id == source_id,
            Source.is_enabled.is_(True),
            Source.is_archived.is_(False),
        )
        if expected is None:
            statement = statement.where(Source.kafka_topic_identity.is_(None))
        else:
            statement = statement.where(Source.kafka_topic_identity == expected)
        result = self.session.execute(
            statement.values(kafka_topic_identity=identity)
        )
        return result.rowcount == 1
