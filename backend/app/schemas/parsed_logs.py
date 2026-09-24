from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

from ..ecs import EcsField


class StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EcsFieldDTO(StrictRequest):
    name: str
    type: str
    level: str
    description: str
    is_array: bool
    field_set: str
    filterable: bool
    operators: list[str]
    mappable: bool
    mappable_reason: str | None

    @classmethod
    def from_field(cls, field: EcsField) -> EcsFieldDTO:
        return cls(
            **{
                **field.__dict__,
                "operators": list(field.operators),
                "mappable": field.mappable,
                "mappable_reason": field.mappable_reason,
            }
        )


class EcsFieldPage(StrictRequest):
    items: list[EcsFieldDTO]
    total: int
    limit: int
    offset: int


class EcsSchemaDTO(StrictRequest):
    version: str
    field_count: int
    upstream: str
    tag: str
    source_file: str
    retrieved_on: str
    sha256: str


class EcsFilter(StrictRequest):
    field: str = Field(min_length=1, max_length=512)
    operator: str = Field(min_length=1, max_length=32)
    value: Any = None

    @model_validator(mode="after")
    def validate_value_presence(self):
        presence_operator = self.operator in {"exists", "not_exists"}
        if presence_operator and ("value" in self.model_fields_set):
            raise ValueError("exists operators do not accept value")
        if not presence_operator and ("value" not in self.model_fields_set or self.value is None):
            raise ValueError("this operator requires a non-null value")
        return self


class ParsedLogSearchRequest(StrictRequest):
    source_ids: list[UUID] | None = None
    connection_ids: list[UUID] | None = None
    normalizer_ids: list[UUID] | None = None
    kafka_topics: list[str] | None = None
    kafka_partitions: list[StrictInt] | None = None
    raw_query: str | None = Field(default=None, max_length=512)
    processed_from: datetime | None = None
    processed_to: datetime | None = None
    collected_from: datetime | None = None
    collected_to: datetime | None = None
    ecs_filters: list[EcsFilter] = Field(default_factory=list, max_length=20)
    limit: int = Field(default=50, ge=1, le=100)
    cursor: str | None = Field(default=None, min_length=1, max_length=4096)

    @field_validator("source_ids", "connection_ids", "normalizer_ids", "kafka_topics", "kafka_partitions")
    @classmethod
    def validate_filter_groups(cls, value):
        if value is not None and (not value or len(value) > 100):
            raise ValueError("filter groups must contain between 1 and 100 values")
        return value

    @field_validator("kafka_partitions")
    @classmethod
    def validate_partitions(cls, value):
        if value is not None and any(item < 0 for item in value):
            raise ValueError("Kafka partitions must be non-negative")
        return value

    @field_validator("raw_query")
    @classmethod
    def normalize_raw_query(cls, value):
        if value is None:
            return None
        normalized = value.strip()
        if not 1 <= len(normalized) <= 512:
            raise ValueError("raw_query must contain between 1 and 512 characters")
        return normalized

    @field_validator("processed_from", "processed_to", "collected_from", "collected_to")
    @classmethod
    def require_timezone(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime values must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_time_ranges(self):
        for lower, upper in (
            (self.processed_from, self.processed_to),
            (self.collected_from, self.collected_to),
        ):
            if lower is not None and upper is not None and lower >= upper:
                raise ValueError("time range lower bound must be earlier than upper bound")
        return self


class ParsedLogSummary(StrictRequest):
    id: UUID
    source_id: UUID | None
    connection_id: UUID | None
    normalizer_id: UUID | None
    source_name: str
    connection_name: str
    normalizer_name: str
    normalizer_version: int
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    fluent_bit_collected_at: datetime
    backend_received_at: datetime
    backend_processed_at: datetime
    created_at: datetime
    raw_preview: str = Field(max_length=500)
    ecs_version: str | None

    @classmethod
    def from_model(cls, log) -> ParsedLogSummary:
        ecs_version = None
        if isinstance(log.ecs_data, dict):
            ecs = log.ecs_data.get("ecs")
            if isinstance(ecs, dict) and isinstance(ecs.get("version"), (str, int, float)):
                ecs_version = str(ecs["version"])
        return cls(
            id=log.id,
            source_id=log.source_id,
            connection_id=log.connection_id,
            normalizer_id=log.normalizer_id,
            source_name=log.source_name,
            connection_name=log.connection_name,
            normalizer_name=log.normalizer_name,
            normalizer_version=log.normalizer_version,
            kafka_topic=log.kafka_topic,
            kafka_partition=log.kafka_partition,
            kafka_offset=log.kafka_offset,
            fluent_bit_collected_at=log.fluent_bit_collected_at,
            backend_received_at=log.backend_received_at,
            backend_processed_at=log.backend_processed_at,
            created_at=log.created_at,
            raw_preview=log.raw[:500],
            ecs_version=ecs_version,
        )

    @classmethod
    def from_search_row(cls, log, raw_preview: str, ecs_version: str | None):
        return cls(
            id=log.id,
            source_id=log.source_id,
            connection_id=log.connection_id,
            normalizer_id=log.normalizer_id,
            source_name=log.source_name,
            connection_name=log.connection_name,
            normalizer_name=log.normalizer_name,
            normalizer_version=log.normalizer_version,
            kafka_topic=log.kafka_topic,
            kafka_partition=log.kafka_partition,
            kafka_offset=log.kafka_offset,
            fluent_bit_collected_at=log.fluent_bit_collected_at,
            backend_received_at=log.backend_received_at,
            backend_processed_at=log.backend_processed_at,
            created_at=log.created_at,
            raw_preview=raw_preview,
            ecs_version=ecs_version,
        )


class ParsedLogDetail(ParsedLogSummary):
    deduplication_key: str
    raw: str
    ecs_data: dict[str, Any]

    @classmethod
    def from_model(cls, log) -> ParsedLogDetail:
        summary = ParsedLogSummary.from_model(log)
        return cls(**summary.model_dump(), deduplication_key=log.deduplication_key,
                   raw=log.raw, ecs_data=log.ecs_data if isinstance(log.ecs_data, dict) else {})


class ParsedLogSearchResponse(StrictRequest):
    items: list[ParsedLogSummary]
    has_more: bool
    next_cursor: str | None


class ParsedLogBulkDeleteRequest(StrictRequest):
    source_id: UUID | None = None
    processed_from: datetime | None = None
    processed_to: datetime | None = None

    @field_validator("processed_from", "processed_to")
    @classmethod
    def require_timezone(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime values must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_scope_shape(self):
        has_range = self.processed_from is not None or self.processed_to is not None
        if has_range and (self.processed_from is None or self.processed_to is None):
            raise ValueError("both processed time boundaries are required")
        if (
            self.processed_from is not None
            and self.processed_to is not None
            and self.processed_from >= self.processed_to
        ):
            raise ValueError("processed_from must be earlier than processed_to")
        return self


class ParsedLogBulkDeleteResponse(StrictRequest):
    deleted_count: int
