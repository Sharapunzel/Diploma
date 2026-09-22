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
        CheckConstraint("kafka_partition >= 0", name="kafka_partition_nonnegative"),
        CheckConstraint("kafka_offset >= 0", name="kafka_offset_nonnegative"),
        CheckConstraint("jsonb_typeof(ecs_data) = 'object'", name="ecs_data_object"),
        CheckConstraint(
            "backend_processed_at >= backend_received_at",
            name="processing_after_receipt",
        ),
        Index("ix_parsed_logs_source_processed", "source_id", "backend_processed_at"),
        Index("ix_parsed_logs_connection_id", "connection_id"),
        Index("ix_parsed_logs_normalizer_id", "normalizer_id"),
        Index("ix_parsed_logs_fluent_collected", "fluent_bit_collected_at"),
        Index("ix_parsed_logs_ecs_data", "ecs_data", postgresql_using="gin"),
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
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    connection_name: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_topic: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deduplication_key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
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
