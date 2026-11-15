"""Aggregate API contracts. There is deliberately no update-rule request."""

from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AwareDatetime,
    Field,
    StrictInt,
    field_validator,
    model_serializer,
    model_validator,
)

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
        try:
            local = self.boundary.astimezone(zone)
            self.anchor_utc + timedelta(seconds=self.duration_seconds)
        except (ValueError, OverflowError) as error:
            raise ValueError("schedule_datetime_range") from error
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
    # Legacy forms are accepted by transport only for exact create replay.
    time_basis: Literal["event_time", "availability_at", "backend_processed_at_historical"] | None = None


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
    exact_materialized_features: int = 0
    array_elements_limit: int = 4096
    statement_columns: int = 0
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
    snapshot_reference_at: datetime | None = None
    close_not_before_at: datetime | None = None
    origin: str = "preview"
    in_process: bool = True
    snapshot_metadata: dict = Field(default_factory=dict)


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
    semantic_basis: str
    execution_mode: str
    execution_reason: str | None = None
    bootstrap_at: datetime | None
    online_start_index: int | None
    paused: bool
    execution_generation: int
    active_cleanup_id: UUID | None
    next_close_not_before_at: datetime | None
    diagnostics: dict = Field(default_factory=dict)


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
    origin: str = "legacy"
    window_index: int | None = None
    snapshot_reference_at: datetime | None = None
    close_not_before_at: datetime | None = None
    payload_state: str = "present"
    snapshot_metadata: dict = Field(default_factory=dict)


class CleanupRequest(Node):
    keep_last: StrictInt = Field(ge=0, le=9223372036854775807)


class AggregateCommandReceipt(UTCResponse):
    operation_id: UUID
    aggregate_id: UUID
    action: str
    recorded_at: datetime
    execution_generation: int
    cleanup_operation_id: UUID | None = None


class CleanupBlocker(Node):
    detector_id: UUID
    name: str
    reason: str
    minimum_keep: int
    protected_start_index: int | None = None
    protected_end_index: int | None = None


class CleanupProtection(UTCResponse):
    aggregate_id: UUID
    reference_at: datetime
    requested_keep: int
    total_points: int
    minimum_keep: int
    maximum_delete: int
    planned_delete: int
    tail_identity: dict | None
    can_cleanup: bool
    reason: str | None
    blockers: list[CleanupBlocker]


class CleanupOperation(UTCResponse):
    id: UUID
    aggregate_id: UUID
    created_at: datetime
    requested_keep: int
    reference_tail: dict | None
    planned_delete: int
    deleted_points: int
    deleted_bytes_if_measured: int | None
    completed_at: datetime | None
    status: Literal["pending", "running", "succeeded", "failed"]
    safe_error: str | None
    progress: dict


class PointPage(Node):
    items: list[PointCard]
    total: int


class OperatorCard(Node):
    aggregate_operators: list[str] | None = None
    allowed_in: list[str] | None = None
    weight_formula: str = "weight"
    arity: dict[str, int | None] | None = None
    compatible_fields: list[dict[str, str | bool]] = Field(default_factory=list)
    name: str
    kind: str
    inputs: list[str]
    parameters: list[str]
    weight: int
    result: str
    parameter_types: dict[str, str]
    result_nullable: bool

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        document = handler(self)
        for key in ("aggregate_operators", "allowed_in"):
            if getattr(self, key) is None:
                document.pop(key, None)
        return document


class AggregateField(Node):
    operators: list[str] = Field(default_factory=list)
    name: str
    type: str
    is_array: bool
    description: str
    supported: bool
    reason: str | None


class AggregateFieldPage(Node):
    items: list[AggregateField]
    total: int


class SourceCatalogField(AggregateField):
    provenance: list[Literal["observed", "normalizer"]]
    normalizer_origin: list[Literal["mapping", "system"]]


class CatalogObservation(Node):
    scope: Literal["all_retained_events"]
    complete: Literal[True]
    rows: int


class CatalogNormalizer(Node):
    id: UUID
    version: int


class SourceFieldCatalogPage(UTCResponse):
    items: list[SourceCatalogField]
    total: int
    limit: int
    offset: int
    snapshot_at: datetime
    catalog_hash: str
    observation: CatalogObservation
    normalizer: CatalogNormalizer | None
