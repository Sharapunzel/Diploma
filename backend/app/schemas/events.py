from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator, model_validator

from .parsed_logs import StrictRequest


class EventFilter(StrictRequest):
    field: str = Field(min_length=1, max_length=512)
    operator: str = Field(min_length=1, max_length=32)
    value: Any = None

    @model_validator(mode="after")
    def validate_value(self):
        if self.operator in {"exists", "not_exists"}:
            if "value" in self.model_fields_set:
                raise ValueError("exists operators do not accept a value")
        elif "value" not in self.model_fields_set or self.value is None:
            raise ValueError("operator requires a non-null value")
        return self


class EventSearchRequest(StrictRequest):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "timestamp_from": "2026-01-01T00:00:00Z",
                "timestamp_to": "2026-02-01T00:00:00Z",
                "sort": "desc",
                "filters": [
                    {"field": "event.action", "operator": "in", "value": ["login", "logout"]}
                ],
                "limit": 50,
                "cursor": None,
            }
        },
    )
    filters: list[EventFilter] = Field(default_factory=list, max_length=20)
    timestamp_from: datetime | None = None
    timestamp_to: datetime | None = None
    sort: Literal["asc", "desc"] = "desc"
    limit: int = Field(default=50, ge=1, le=100)
    cursor: str | None = Field(default=None, min_length=1, max_length=4096)

    @field_validator("timestamp_from", "timestamp_to")
    @classmethod
    def timestamp_timezone(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def timestamp_range(self):
        if (
            self.timestamp_from is not None
            and self.timestamp_to is not None
            and self.timestamp_from >= self.timestamp_to
        ):
            raise ValueError("timestamp_from must be earlier than timestamp_to")
        return self


class EventFieldDTO(StrictRequest):
    name: str
    type: str
    operators: list[str]


class EventFieldPage(StrictRequest):
    items: list[EventFieldDTO]
    total: int
    limit: int
    offset: int


class EventSearchItem(StrictRequest):
    source_id: UUID
    source_type: Literal["kafka", "external"]
    id: str
    event_timestamp: datetime
    fields: dict[str, Any]


class EventSearchResponse(StrictRequest):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "items": [
                    {
                        "source_id": "90f9bb38-8a64-4f78-a552-213ff5a6c20d",
                        "source_type": "kafka",
                        "id": "gAAAAAB...opaque-token",
                        "event_timestamp": "2026-01-12T09:42:00Z",
                        "fields": {
                            "@timestamp": "2026-01-12T09:42:00Z",
                            "event": {"action": "login"},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        },
    )
    items: list[EventSearchItem]
    has_more: bool
    next_cursor: str | None


class LocalEventMetadata(StrictRequest):
    connection_id: UUID | None
    normalizer_id: UUID | None
    source_name: str
    connection_name: str
    normalizer_name: str
    normalizer_version: int
    normalization_status: Literal["complete", "partial"]
    normalization_diagnostics: list[dict[str, Any]]
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    fluent_bit_collected_at: datetime
    backend_received_at: datetime
    backend_processed_at: datetime
    created_at: datetime


class EventCardBase(StrictRequest):
    source_id: UUID
    source_type: Literal["kafka", "external"]
    id: str
    event_timestamp: datetime
    fields: dict[str, Any]


class LocalEventDetails(StrictRequest):
    raw: str
    metadata: LocalEventMetadata


class KafkaEventCard(EventCardBase):
    source_type: Literal["kafka"]
    details: LocalEventDetails


class ExternalEventCard(EventCardBase):
    source_type: Literal["external"]
    details: dict[str, Any] | None = None


EventCard = Annotated[KafkaEventCard | ExternalEventCard, Field(discriminator="source_type")]
