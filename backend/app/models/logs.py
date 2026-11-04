from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class ParsedLog(Base):
    __tablename__ = "parsed_logs"
    __table_args__ = (
        CheckConstraint("normalizer_version > 0", name="normalizer_version_positive"),
        CheckConstraint("btrim(kafka_topic) <> ''", name="topic_not_blank"),
        CheckConstraint("kafka_partition >= 0", name="kafka_partition_nonnegative"),
        CheckConstraint("kafka_offset >= 0", name="kafka_offset_nonnegative"),
        CheckConstraint("jsonb_typeof(ecs_data) = 'object'", name="ecs_data_object"),
        CheckConstraint(
            "normalization_status IN ('complete', 'partial')",
            name="normalization_status_valid",
        ),
        CheckConstraint(
            "jsonb_typeof(normalization_diagnostics) = 'array'",
            name="normalization_diagnostics_array",
        ),
        CheckConstraint(
            "backend_processed_at >= backend_received_at",
            name="processing_after_receipt",
        ),
        Index("ix_parsed_logs_source_processed", "source_id", "backend_processed_at"),
        Index("ix_parsed_logs_connection_id", "connection_id"),
        Index("ix_parsed_logs_normalizer_id", "normalizer_id"),
        Index("ix_parsed_logs_fluent_collected", "fluent_bit_collected_at"),
        Index("ix_parsed_logs_ecs_data", "ecs_data", postgresql_using="gin"),
        Index(
            "ix_parsed_logs_raw_trgm",
            "raw",
            postgresql_using="gin",
            postgresql_ops={"raw": "gin_trgm_ops"},
        ),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    source_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.sources.id", ondelete="SET NULL")
    )
    connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.kafka_connections.id", ondelete="SET NULL")
    )
    normalizer_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.normalizers.id", ondelete="SET NULL")
    )
    normalizer_version: Mapped[int] = mapped_column(Integer, nullable=False)
    normalizer_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    connection_name: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_topic: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deduplication_key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    processed_record_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("logs.processed_kafka_records.id", ondelete="RESTRICT"),
        unique=True,
    )
    normalization_status: Mapped[str] = mapped_column(
        Text, server_default=text("'complete'"), nullable=False
    )
    normalization_diagnostics: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, server_default=text("'[]'::jsonb"), nullable=False
    )
    fluent_bit_collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    backend_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    backend_processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    raw: Mapped[str] = mapped_column(Text, nullable=False)
    ecs_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )


class ProcessedKafkaRecord(Base):
    """Durable identity and outcome for one Kafka coordinate tuple."""

    __tablename__ = "processed_kafka_records"
    __table_args__ = (
        Index(
            "uq_processed_kafka_record_generation",
            "connection_identity", "kafka_topic_identity", "kafka_partition", "kafka_offset",
            unique=True, postgresql_where=text("kafka_topic_identity IS NOT NULL"),
        ),
        Index(
            "uq_processed_kafka_record_legacy",
            "connection_identity", "kafka_topic", "kafka_partition", "kafka_offset",
            unique=True, postgresql_where=text("kafka_topic_identity IS NULL"),
        ),
        CheckConstraint("btrim(kafka_topic) <> ''", name="topic_not_blank"),
        CheckConstraint("kafka_partition >= 0", name="partition_nonnegative"),
        CheckConstraint("kafka_offset >= 0", name="offset_nonnegative"),
        CheckConstraint(
            "result_status IN ('complete', 'partial', 'failed')", name="result_status_valid"
        ),
        CheckConstraint("backend_processed_at >= backend_received_at", name="processing_after_receipt"),
        Index("ix_processed_kafka_records_source_received", "source_id", "backend_received_at"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.kafka_connections.id", ondelete="SET NULL")
    )
    # Immutable UUID scope survives deletion of the mutable connection row.
    connection_identity: Mapped[UUID] = mapped_column(nullable=False)
    kafka_topic_identity: Mapped[str | None] = mapped_column(Text)
    source_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.sources.id", ondelete="SET NULL")
    )
    normalizer_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.normalizers.id", ondelete="SET NULL")
    )
    kafka_topic: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    result_status: Mapped[str] = mapped_column(Text, nullable=False)
    backend_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    backend_processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class KafkaOperationalEvent(Base):
    __tablename__ = "kafka_operational_events"
    __table_args__ = (
        CheckConstraint("kind IN ('retention_gap', 'topic_recreated')", name="kind_valid"),
        CheckConstraint("btrim(topic_name) <> ''", name="topic_not_blank"),
        CheckConstraint("btrim(old_topic_identity) <> ''", name="old_identity_not_blank"),
        CheckConstraint(
            "(kind = 'retention_gap' AND kafka_partition >= 0 AND "
            "offset_start >= 0 AND offset_end > offset_start AND new_topic_identity IS NULL) "
            "OR (kind = 'topic_recreated' AND kafka_partition IS NULL AND "
            "offset_start IS NULL AND offset_end IS NULL AND "
            "new_topic_identity IS NOT NULL AND btrim(new_topic_identity) <> '' AND "
            "new_topic_identity <> old_topic_identity)",
            name="payload_valid",
        ),
        Index(
            "uq_kafka_retention_gap", "source_identity", "old_topic_identity",
            "kafka_partition", "offset_start", "offset_end", unique=True,
            postgresql_where=text("kind = 'retention_gap'"),
        ),
        Index(
            "uq_kafka_topic_recreated", "source_identity", "old_topic_identity",
            "new_topic_identity", unique=True,
            postgresql_where=text("kind = 'topic_recreated'"),
        ),
        Index("ix_kafka_operational_events_source_detected", "source_identity", "detected_at"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    reason_code: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[UUID | None] = mapped_column(ForeignKey("app.sources.id", ondelete="SET NULL"))
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.kafka_connections.id", ondelete="SET NULL")
    )
    connection_identity: Mapped[UUID] = mapped_column(nullable=False)
    connection_name: Mapped[str] = mapped_column(Text, nullable=False)
    cluster_identity: Mapped[str] = mapped_column(Text, nullable=False)
    topic_name: Mapped[str] = mapped_column(Text, nullable=False)
    old_topic_identity: Mapped[str] = mapped_column(Text, nullable=False)
    new_topic_identity: Mapped[str | None] = mapped_column(Text)
    kafka_partition: Mapped[int | None] = mapped_column(Integer)
    offset_start: Mapped[int | None] = mapped_column(BigInteger)
    offset_end: Mapped[int | None] = mapped_column(BigInteger)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"), nullable=False
    )


class ProcessingError(Base):
    __tablename__ = "processing_errors"
    __table_args__ = (
        CheckConstraint(
            "stage IN ('decode', 'envelope', 'normalization')", name="stage_valid"
        ),
        CheckConstraint("btrim(connection_name) <> ''", name="connection_name_not_blank"),
        CheckConstraint("btrim(source_name) <> ''", name="source_name_not_blank"),
        CheckConstraint("btrim(normalizer_name) <> ''", name="normalizer_name_not_blank"),
        CheckConstraint("normalizer_version > 0", name="normalizer_version_positive"),
        CheckConstraint("btrim(kafka_topic) <> ''", name="topic_not_blank"),
        CheckConstraint("kafka_partition >= 0", name="partition_nonnegative"),
        CheckConstraint("kafka_offset >= 0", name="offset_nonnegative"),
        CheckConstraint("jsonb_typeof(diagnostics) = 'array'", name="diagnostics_array"),
        CheckConstraint("backend_processed_at >= backend_received_at", name="processing_after_receipt"),
        Index("ix_processing_errors_source_received", "source_id", "backend_received_at"),
        Index("ix_processing_errors_source_identity_processed", "source_identity", "backend_processed_at"),
        Index("ix_processing_errors_connection_id", "connection_id"),
        Index("ix_processing_errors_normalizer_id", "normalizer_id"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    processed_record_id: Mapped[UUID] = mapped_column(
        ForeignKey("logs.processed_kafka_records.id", ondelete="RESTRICT"), unique=True,
        nullable=False,
    )
    connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.kafka_connections.id", ondelete="SET NULL")
    )
    source_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.sources.id", ondelete="SET NULL")
    )
    source_identity: Mapped[UUID | None] = mapped_column()
    normalizer_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.normalizers.id", ondelete="SET NULL")
    )
    connection_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalizer_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalizer_version: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_topic: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    raw_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    diagnostics: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    fluent_bit_collected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    backend_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    backend_processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class AvailabilityMarker(Base):
    __tablename__ = "availability_markers"
    __table_args__ = (
        CheckConstraint("kind IN ('event', 'barrier')", name="kind_valid"),
        CheckConstraint("(kind = 'event' AND event_id IS NOT NULL) OR "
                        "(kind = 'barrier' AND event_id IS NULL)", name="payload_valid"),
        Index("ix_availability_markers_source_time", "source_identity", "availability_at"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    # Do not cascade marker history if a user deletes an event.
    event_id: Mapped[UUID | None] = mapped_column(unique=True)
    availability_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )

class ReplicationProgress(Base):
    __tablename__ = "replication_progress"
    __table_args__ = (
        CheckConstraint("id = 1", name="singleton"),
        CheckConstraint("durable_lsn >= 0 AND durable_lsn <= 18446744073709551615", name="lsn_valid"),
        {"schema": "logs"},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    durable_lsn: Mapped[int] = mapped_column(Numeric(20, 0), nullable=False)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    legacy_cutover_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    availability_verified_from_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    boundary_origin: Mapped[str | None] = mapped_column(Text)
    boundary_barrier_id: Mapped[UUID | None] = mapped_column()

class InputProbe(Base):
    """Administrator-registered public key for one input configuration generation."""

    __tablename__ = "input_probes"
    __table_args__ = (
        CheckConstraint("octet_length(public_key) = 32", name="public_key_length"),
        Index("uq_input_probe_generation", "source_identity", "input_generation", unique=True),
        Index("uq_input_probe_active_source", "source_identity", unique=True,
              postgresql_where=text("is_active")),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    input_generation: Mapped[UUID] = mapped_column(nullable=False)
    public_key: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

class InputAttestation(Base):
    __tablename__ = "input_attestations"
    __table_args__ = (
        CheckConstraint("sequence >= 0", name="sequence_nonnegative"),
        CheckConstraint("end_utc > start_utc", name="bounds_valid"),
        CheckConstraint("state IN ('covered', 'interrupted')", name="state_valid"),
        CheckConstraint("octet_length(signature) = 64", name="signature_length"),
        Index("uq_input_attestation_sequence", "probe_id", "sequence", unique=True),
        Index("ix_input_attestation_source_interval", "source_identity", "start_utc", "end_utc"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    probe_id: Mapped[UUID] = mapped_column(ForeignKey("logs.input_probes.id", ondelete="RESTRICT"), nullable=False)
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    input_generation: Mapped[UUID] = mapped_column(nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    topic_identity: Mapped[str] = mapped_column(Text, nullable=False)
    start_positions: Mapped[dict] = mapped_column(JSONB, nullable=False)
    end_positions: Mapped[dict] = mapped_column(JSONB, nullable=False)
    signed_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    signature: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                  server_default=text("clock_timestamp()"))

class KafkaCheckpoint(Base):
    """Durable snapshot produced by the consumer from Kafka metadata/watermarks."""

    __tablename__ = "source_kafka_checkpoints"
    __table_args__ = (
        Index("ix_source_checkpoint_observed", "source_identity", "observed_at"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    topic_identity: Mapped[str] = mapped_column(Text, nullable=False)
    # One entry per metadata partition: low, high and durably committed next offset.
    partitions: Mapped[dict] = mapped_column(JSONB, nullable=False)
    worker_instance: Mapped[UUID] = mapped_column(nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                  server_default=text("clock_timestamp()"))

class SourceInterruption(Base):
    __tablename__ = "source_interruptions"
    __table_args__ = (
        CheckConstraint("kind IN ('source_disabled', 'processing_stopped')", name="kind_valid"),
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="bounds_valid"),
        Index("ix_source_interruption_interval", "source_identity", "started_at", "ended_at"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    source_identity: Mapped[UUID] = mapped_column(nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 server_default=text("clock_timestamp()"))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
