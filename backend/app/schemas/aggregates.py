"""Aggregate API contracts. There is deliberately no update-rule request."""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
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
    execution_policy: "ExecutionPolicy"
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


class ExecutionPolicy(Node):
    policy_version: Literal["window_execution_v1"]
    soft_latency_seconds: Literal[5]
    hard_budget_seconds: float = Field(gt=0, le=30, allow_inf_nan=False, strict=True)


class ExecutionTimings(Node):
    selection: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    manifest: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    statistics: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    interpret: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    cohorts: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    snapshot: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    save: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    commit: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_serializer(mode="wrap")
    def known_parts(self, handler):
        return {key: value for key, value in handler(self).items() if value is not None}


class ExecutionDiagnostics(ExecutionPolicy, UTCResponse):
    calculation_status: Literal["computed", "uncomputed"]
    calculation_reason: Literal["execution_budget_exceeded"] | None
    attempt_id: UUID
    origin: Literal["preview", "online", "initial_history"]
    window_index: StrictInt | None
    start_utc: AwareDatetime
    end_utc: AwareDatetime
    started_at: AwareDatetime
    execution_deadline_at: AwareDatetime
    measured_at: AwareDatetime
    measured_through: Literal["before_commit", "preview_complete", "finalization"]
    elapsed_seconds: float = Field(ge=0, allow_inf_nan=False)
    queue_seconds: float | None = Field(ge=0, allow_inf_nan=False)
    online_latency_seconds: float | None = Field(ge=0, allow_inf_nan=False)
    soft_latency_exceeded: bool | None
    timings: ExecutionTimings

    @model_validator(mode="after")
    def valid_outcome(self):
        if (self.calculation_status == "uncomputed") != (self.calculation_reason == "execution_budget_exceeded"):
            raise ValueError("calculation_reason_mismatch")
        return self


class ValidationResult(UTCResponse):
    rule_hash: str
    anchor_utc: datetime
    duration_seconds: int
    feature_keys: list[str]
    budget: BudgetEstimate


class NamedFeatureResult(FeatureResult):
    key: str


class PreviewResult(UTCResponse):
    calculation_status: Literal["computed"] = "computed"
    calculation_reason: None = None
    execution: ExecutionDiagnostics
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


class AutoCleanupProtector(Node):
    detector_id: UUID
    name: str
    reason: Literal["unconsumed_points", "frozen_warmup", "prepared_warmup_continuation", "planned_warmup", "insufficient_warmup_history"]
    protected_from_index: StrictInt
    protected_to_index: StrictInt | None


class AutoCleanupSummary(UTCResponse):
    policy_version: Literal["model_history_v1"] | None
    status: Literal["scheduled", "idle", "compacting", "protected", "deferred", "error", "inactive"]
    reason: str | None
    checked_at: datetime | None
    next_check_at: datetime | None
    reserve_windows: StrictInt | None
    keep_from_index: StrictInt | None
    protected_from_index: StrictInt | None
    auto_cleared_total: StrictInt | None
    history_revision: StrictInt | None
    last_cleared_at: datetime | None
    retained_from_index: StrictInt | None
    retained_to_index: StrictInt | None
    protected_total: StrictInt | None
    protected_by: list[AutoCleanupProtector] = Field(max_length=10)


class AggregateCard(UTCResponse):
    execution_policy: ExecutionPolicy
    last_execution: ExecutionDiagnostics | None
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
    auto_cleanup: AutoCleanupSummary


class AggregatePage(Node):
    items: list[AggregateCard]
    total: int


class PointIdentity(UTCResponse):
    id: UUID
    aggregate_id: UUID
    kind: Literal["working", "historical"]
    start_utc: datetime
    end_utc: datetime
    scheduled_at: datetime
    actual_started_at: datetime
    actual_completed_at: datetime

    time_basis: str
    origin: str = "legacy"
    window_index: int | None = None
    close_not_before_at: datetime | None = None
    payload_state: Literal["present"] = "present"

    @model_validator(mode="after")
    def matching_execution_identity(self):
        execution = getattr(self, "execution", None)
        if execution is not None:
            duration = (self.end_utc - self.start_utc).total_seconds()
            if (execution.origin != self.origin or execution.window_index != self.window_index
                    or execution.start_utc != self.start_utc or execution.end_utc != self.end_utc
                    or execution.hard_budget_seconds != min(30.0, (4 * duration) / 5)):
                raise ValueError("execution_window_mismatch")
        return self


class ComputedPointCard(PointIdentity):
    calculation_status: Literal["computed"] = "computed"
    calculation_reason: None = None
    execution: ExecutionDiagnostics | None
    event_count: int
    event_time_min: datetime | None
    event_time_max: datetime | None
    features: list[NamedFeatureResult] | None
    completeness: str
    completeness_reason: str
    evidence_refs: dict
    snapshot_reference_at: datetime | None = None
    snapshot_metadata: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid_execution(self):
        if self.execution is not None and self.execution.calculation_status != "computed":
            raise ValueError("calculation_execution_mismatch")
        return self


class UncomputedPointCard(PointIdentity):
    calculation_status: Literal["uncomputed"]
    calculation_reason: Literal["execution_budget_exceeded"]
    execution: ExecutionDiagnostics
    event_count: None
    event_time_min: None
    event_time_max: None
    features: None
    completeness: None
    completeness_reason: None
    evidence_refs: None
    snapshot_reference_at: None
    snapshot_metadata: None

    @model_validator(mode="after")
    def valid_execution(self):
        if self.execution.calculation_status != "uncomputed":
            raise ValueError("calculation_execution_mismatch")
        return self


PointCard = Annotated[ComputedPointCard | UncomputedPointCard, Field(discriminator="calculation_status")]


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
