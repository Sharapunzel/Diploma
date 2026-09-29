from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
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
        UniqueConstraint(
            "connection_identity", "kafka_topic", "kafka_partition", "kafka_offset",
            name="uq_processed_kafka_record_coordinates",
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
