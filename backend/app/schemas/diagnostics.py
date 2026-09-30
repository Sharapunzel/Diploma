from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class Page(BaseModel):
    total: int
    limit: int
    offset: int


class PartitionState(BaseModel):
    partition: int
    status: Literal["running", "retrying", "stopped", "unknown"]
    reason_code: str | None = None
    reason_description: str | None = None


class SourceState(BaseModel):
    source_id: UUID
    source_name: str
    connection_id: UUID | None
    connection_name: str | None
    topic: str
    topic_id: str | None
    is_enabled: bool
    is_archived: bool
    worker_status: Literal["running", "retrying", "stopped", "not_running", "unknown"]
    reason_code: str | None
    reason_description: str | None
    captured_at: datetime
    partitions: list[PartitionState]


class SourceStatePage(Page):
    items: list[SourceState]


class ProcessingErrorDTO(BaseModel):
    id: UUID
    source_id: UUID | None
    connection_id: UUID | None
    normalizer_id: UUID | None
    stage: Literal["decode", "envelope", "normalization"]
    diagnostics: list[dict]
    source_name: str
    connection_name: str
    normalizer_name: str
    normalizer_version: int
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    fluent_bit_collected_at: datetime | None
    backend_received_at: datetime
    backend_processed_at: datetime

    @classmethod
    def from_error(cls, error):
        return cls(
            id=error.id,
            source_id=error.source_identity or error.source_id,
            connection_id=error.connection_id,
            normalizer_id=error.normalizer_id,
            stage=error.stage,
            diagnostics=error.diagnostics,
            source_name=error.source_name,
            connection_name=error.connection_name,
            normalizer_name=error.normalizer_name,
            normalizer_version=error.normalizer_version,
            kafka_topic=error.kafka_topic,
            kafka_partition=error.kafka_partition,
            kafka_offset=error.kafka_offset,
            fluent_bit_collected_at=error.fluent_bit_collected_at,
            backend_received_at=error.backend_received_at,
            backend_processed_at=error.backend_processed_at,
        )


class ProcessingErrorPage(Page):
    items: list[ProcessingErrorDTO]


class RawPayloadDTO(BaseModel):
    error_id: UUID
    encoding: Literal["base64"] = "base64"
    data: str
    original_size: int
    returned_size: int
    truncated: bool


class OperationalEventDTO(BaseModel):
    id: UUID
    kind: Literal["retention_gap", "topic_recreated"]
    reason_code: str
    source_id: UUID
    source_name: str
    connection_id: UUID | None
    connection_identity: UUID
    connection_name: str
    cluster_identity: str
    topic: str
    old_topic_id: str
    new_topic_id: str | None
    partition: int | None
    offset_start: int | None
    offset_end: int | None
    offset_range_semantics: Literal["unavailable_kafka_positions_not_message_count"] = (
        "unavailable_kafka_positions_not_message_count"
    )
    detected_at: datetime

    @classmethod
    def from_event(cls, event):
        return cls(
            id=event.id,
            kind=event.kind,
            reason_code=event.reason_code,
            source_id=event.source_identity,
            source_name=event.source_name,
            connection_id=event.connection_id,
            connection_identity=event.connection_identity,
            connection_name=event.connection_name,
            cluster_identity=event.cluster_identity,
            topic=event.topic_name,
            old_topic_id=event.old_topic_identity,
            new_topic_id=event.new_topic_identity,
            partition=event.kafka_partition,
            offset_start=event.offset_start,
            offset_end=event.offset_end,
            detected_at=event.detected_at,
        )


class OperationalEventPage(Page):
    items: list[OperationalEventDTO]


class Overview(BaseModel):
    registered_sources: int
    enabled_sources: int
    archived_sources: int
    worker_counts: dict[str, int]
    worker_counts_scope: Literal["application_process"] = Field(
        default="application_process",
        description=(
            "Counts cover this FastAPI process only. A multi-process deployment needs "
            "an external aggregation service for global worker counts."
        ),
    )
    recent_errors: list[ProcessingErrorDTO]
    recent_events: list[OperationalEventDTO]
    captured_at: datetime


def validate_interval(start: datetime | None, end: datetime | None):
    if start and end and start >= end:
        raise ValueError("start must be earlier than end")
    return start, end
