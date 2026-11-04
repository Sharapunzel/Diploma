"""Aggregate API contracts. There is deliberately no update-rule request."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AwareDatetime, Field, StrictInt, field_validator, model_validator

from ..aggregation.rules import AggregateRule, FeatureResult, Node


class ScheduleInput(Node):
    boundary: AwareDatetime
    timezone: str = Field(min_length=1, max_length=100)
    duration_seconds: StrictInt = Field(ge=1, le=604800)

    @model_validator(mode="after")
    def valid_zone(self):
        try:
            zone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError("unknown_timezone") from error
        local = self.boundary.astimezone(zone)
        if (local.replace(tzinfo=None) != self.boundary.replace(tzinfo=None)
                or local.utcoffset() != self.boundary.utcoffset()):
            raise ValueError("boundary_offset_does_not_match_timezone")
        return self

    @property
    def anchor_utc(self):
        return self.boundary.astimezone(UTC)


class ValidateRequest(Node):
    source_id: UUID
    aggregate_rule: AggregateRule
    schedule: ScheduleInput


class AggregatePreviewRequest(ValidateRequest):
    window_start_utc: AwareDatetime
    time_basis: Literal["availability_at", "backend_processed_at_historical"] = "availability_at"


class CreateAggregateRequest(AggregatePreviewRequest):
    name: str = Field(min_length=1, max_length=200)
    rule_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class HistoricalRequest(Node):
    window_start_utc: AwareDatetime


class UTCResponse(Node):
    @field_validator("*", mode="after")
    @classmethod
    def utc_datetimes(cls, value):
        return value.astimezone(UTC) if isinstance(value, datetime) else value


class BudgetEstimate(Node):
    estimated_rows: int
    source_rows: int
    operator_weight: int
    cost: int
    vector_width: int
    estimated_memory_bytes: int
    estimated_row_width_bytes: int
    work_mem_mb_per_operation: int
    rcf_preliminary_bytes: int
    window_seconds: int
    time_guarantee: bool


class ValidationResult(UTCResponse):
    rule_hash: str
    anchor_utc: datetime
    duration_seconds: int
    feature_keys: list[str]
    budget: BudgetEstimate


class NamedFeatureResult(FeatureResult):
    key: str


class PreviewResult(UTCResponse):
    rule_hash: str
    start_utc: datetime
    end_utc: datetime
    scheduled_at: datetime
    observed_rows: int
    event_count: int
    event_time_min: datetime | None
    event_time_max: datetime | None
    features: list[NamedFeatureResult]
    completeness: Literal["complete", "unknown", "interrupted", "unverified"]
    completeness_reason: str
    time_basis: str
    budget: BudgetEstimate


class AggregateCard(UTCResponse):
    id: UUID
    name: str
    source_id: UUID
    source_name: str
    source_type: str
    aggregate_rule: AggregateRule
    rule_hash: str
    schedule_id: UUID
    anchor_utc: datetime
    duration_seconds: int
    original_boundary: str
    timezone: str
    created_at: datetime
    created_by: UUID
    state: str
    archived_at: datetime | None
    deleted_at: datetime | None
    deleted_by: UUID | None
    skipped_ranges: list[dict]
    next_index: int
    next_scheduled_at: datetime
    lag_seconds: float
    last_error: str | None
    last_error_at: datetime | None


class AggregatePage(Node):
    items: list[AggregateCard]
    total: int


class PointCard(UTCResponse):
    id: UUID
    aggregate_id: UUID
    kind: Literal["working", "historical"]
    start_utc: datetime
    end_utc: datetime
    scheduled_at: datetime
    actual_started_at: datetime
    actual_completed_at: datetime
    event_count: int
    event_time_min: datetime | None
    event_time_max: datetime | None
    features: list[NamedFeatureResult] | None
    completeness: str
    completeness_reason: str
    evidence_refs: dict
    time_basis: str


class PointPage(Node):
    items: list[PointCard]
    total: int


class OperatorCard(Node):
    name: str
    kind: str
    inputs: list[str]
    parameters: list[str]
    weight: int
    result: str
    parameter_types: dict[str, str]
    result_nullable: bool


class AggregateField(Node):
    name: str
    type: str
    is_array: bool
    description: str
    supported: bool
    reason: str | None


class AggregateFieldPage(Node):
    items: list[AggregateField]
    total: int
