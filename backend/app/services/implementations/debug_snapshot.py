import json
from uuid import UUID

from ...core.errors import DomainError
from ...kafka import KafkaConnectionConfig, KafkaMetadataClient, KafkaMetadataError
from ...kafka_debug import DebugReader, DebugReadError
from ...normalization.schema import MAX_LOG_BYTES
from ...repositories.protocols.connections import KafkaConnectionRepository, SourceRepository
from ...schemas.debug_snapshot import SnapshotRecord, SourceSnapshot
from .processing import _decode_envelope

MAX_RESPONSE_BYTES = 4_194_304


class SourceSnapshotServiceImpl:
    def __init__(self, sources: SourceRepository, connections: KafkaConnectionRepository,
                 metadata: KafkaMetadataClient, reader: DebugReader, timeout: float) -> None:
        self.sources, self.connections = sources, connections
        self.metadata, self.reader, self.timeout = metadata, reader, timeout

    def read(self, source_id: UUID) -> SourceSnapshot:
        source = self.sources.find_by_id(source_id)
        if source is None or source.source_type != "kafka":
            raise DomainError("source_not_found", "Kafka source not found", 404)
        if source.is_archived or not source.kafka_topic_identity:
            raise DomainError("source_generation_unavailable", "Source generation is unavailable", 409)
        connection = self.connections.find_by_id(source.connection_id)
        if connection is None:
            raise DomainError("connection_not_found", "Kafka connection not found", 404)
        config = KafkaConnectionConfig(tuple(connection.bootstrap_servers), connection.security_protocol)

        def check_identity() -> None:
            try:
                metadata = self.metadata.metadata_for_topic(config, source.topic_name, self.timeout)
            except KafkaMetadataError as error:
                status = 504 if error.kind == "timeout" else 503
                raise DomainError("kafka_unavailable", "Kafka metadata unavailable", status) from error
            topic = next((item for item in metadata.topics if item.name == source.topic_name), None)
            if topic is None or topic.identity != source.kafka_topic_identity:
                raise DomainError("source_generation_changed", "Kafka topic generation changed", 409)

        check_identity()
        try:
            raw = self.reader.read(config, source.topic_name, self.timeout)
        except DebugReadError as error:
            if error.kind == "incomplete":
                raise DomainError("kafka_snapshot_incomplete", "Kafka records changed while reading", 409) from error
            if error.kind == "byte_limit":
                raise DomainError("kafka_snapshot_too_large", "Kafka snapshot exceeds the safe read limit", 413) from error
            status = 504 if error.kind == "timeout" else 503
            raise DomainError("kafka_debug_unavailable", "Kafka snapshot unavailable", status) from error
        check_identity()
        current = self.sources.find_by_id_for_update(source_id)
        if (current is None or current.is_archived or
                current.kafka_topic_identity != source.kafka_topic_identity):
            raise DomainError("source_generation_changed", "Source generation changed", 409)
        records: list[SnapshotRecord] = []
        skipped = 0
        response_bytes = 0
        for position, item in enumerate(raw):
            if item.value is not None and len(item.value) > 131072:
                records.append(SnapshotRecord(position=position, issue="message_too_large"))
                skipped += 1
                continue
            envelope, _, _, _ = _decode_envelope(item.value)
            if envelope is None or len(envelope["log"].encode("utf-8")) > MAX_LOG_BYTES:
                records.append(SnapshotRecord(position=position, issue="invalid_envelope"))
                skipped += 1
            else:
                item_bytes = len(json.dumps(envelope, ensure_ascii=True).encode("utf-8"))
                if response_bytes + item_bytes > MAX_RESPONSE_BYTES:
                    records.append(SnapshotRecord(position=position, issue="response_limit"))
                    skipped += 1
                    continue
                response_bytes += item_bytes
                records.append(SnapshotRecord(position=position,
                                              timestamp=envelope["timestamp"], log=envelope["log"]))
        return SourceSnapshot(records=records, skipped=skipped, truncated=len(raw) == 50)
