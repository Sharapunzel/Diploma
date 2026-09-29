from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ...models import (
    KafkaConnection,
    Normalizer,
    ParsedLog,
    ProcessedKafkaRecord,
    ProcessingError,
    Source,
)
from ..protocols.processing import KafkaCoordinates, ProcessingContext


class SqlAlchemyProcessingRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def find_result(self, coordinates: KafkaCoordinates) -> str | None:
        return self.session.scalar(
            select(ProcessedKafkaRecord.result_status).where(
                ProcessedKafkaRecord.connection_identity == coordinates.connection_id,
                ProcessedKafkaRecord.kafka_topic_identity == coordinates.kafka_topic_identity,
                ProcessedKafkaRecord.kafka_partition == coordinates.partition,
                ProcessedKafkaRecord.kafka_offset == coordinates.offset,
            )
        )

    def resolve_context(self, source_id: UUID) -> ProcessingContext | None:
        connection_id = self.session.scalar(
            select(Source.connection_id).where(Source.id == source_id)
        )
        if connection_id is None:
            return None
        # Match source-management lock order: connection -> source -> normalizer.
        connection = self.session.scalar(
            select(KafkaConnection)
            .where(KafkaConnection.id == connection_id)
            .with_for_update(read=True)
        )
        if connection is None:
            return None
        source = self.session.scalar(
            select(Source).where(Source.id == source_id).with_for_update(read=True)
        )
        if (
            source is None
            or source.connection_id != connection_id
            or not source.is_enabled
            or source.is_archived
            or source.normalizer_id is None
        ):
            return None
        normalizer = self.session.scalar(
            select(Normalizer)
            .where(Normalizer.id == source.normalizer_id)
            .with_for_update(read=True)
        )
        if normalizer is None:
            return None
        return ProcessingContext(
            connection_id=connection.id,
            connection_name=connection.name,
            source_id=source.id,
            source_name=source.name,
            topic=source.topic_name,
            kafka_topic_identity=source.kafka_topic_identity,
            normalizer_id=normalizer.id,
            normalizer_name=normalizer.name,
            normalizer_version=normalizer.version,
            rule=normalizer.rule,
        )

    def _claim(self, record: ProcessedKafkaRecord) -> bool:
        result = self.session.execute(
            insert(ProcessedKafkaRecord)
            .values(
                id=record.id,
                connection_id=record.connection_id,
                connection_identity=record.connection_identity,
                kafka_topic_identity=record.kafka_topic_identity,
                source_id=record.source_id,
                normalizer_id=record.normalizer_id,
                kafka_topic=record.kafka_topic,
                kafka_partition=record.kafka_partition,
                kafka_offset=record.kafka_offset,
                result_status=record.result_status,
                backend_received_at=record.backend_received_at,
                backend_processed_at=record.backend_processed_at,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    ProcessedKafkaRecord.connection_identity,
                    ProcessedKafkaRecord.kafka_topic_identity,
                    ProcessedKafkaRecord.kafka_partition,
                    ProcessedKafkaRecord.kafka_offset,
                ],
                index_where=ProcessedKafkaRecord.kafka_topic_identity.is_not(None),
            )
            .returning(ProcessedKafkaRecord.id)
        )
        return result.scalar_one_or_none() is not None

    def store_event(self, record: ProcessedKafkaRecord, event: ParsedLog) -> bool:
        if not self._claim(record):
            return False
        event.processed_record_id = record.id
        self.session.add(event)
        self.session.flush()
        return True

    def store_error(
        self, record: ProcessedKafkaRecord, error: ProcessingError
    ) -> bool:
        if not self._claim(record):
            return False
        error.processed_record_id = record.id
        self.session.add(error)
        self.session.flush()
        return True
