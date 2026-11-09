"""Explicit public detector transport; no storage/model envelopes."""
from datetime import UTC
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

UtcDateTime = Annotated[AwareDatetime, AfterValidator(lambda value: value.astimezone(UTC))]

ExecutionState = Literal["created", "requested", "starting", "running", "stopped", "failed", "terminated"]
ModelState = Literal["waiting", "warming", "ready", "error", "stopped"]


class ModelOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trees: Annotated[StrictInt, Field(ge=1)] = 100
    sample_size: Annotated[StrictInt, Field(ge=2)] = 256
    output_after: Annotated[StrictInt, Field(ge=2)] = 256
    seed: Annotated[StrictInt, Field(ge=-(2**63), le=2**63 - 1)] = 42

    @model_validator(mode="after")
    def output_covers_sample(self):
        if self.output_after < self.sample_size:
            raise ValueError("output_after должен быть не меньше sample_size")
        return self


class CreateDetectorRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    aggregate_id: UUID
    name: Annotated[str, Field(min_length=1, max_length=200)]
    accelerated: StrictBool = False
    step_seconds: StrictInt | None = None
    config: ModelOptions = Field(default_factory=ModelOptions)

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, value):
        return value.strip() if isinstance(value, str) else value

    @model_validator(mode="after")
    def mode_step(self):
        if self.accelerated and (self.step_seconds is None or self.step_seconds < 1):
            raise ValueError("Для ускоренного прогрева требуется положительный целый шаг")
        if not self.accelerated and self.step_seconds is not None:
            raise ValueError("В обычном режиме шаг не задаётся")
        return self


class EmptyCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DetectorCommandReceipt(BaseModel):
    operation_id: UUID
    detector_id: UUID
    action: Literal["create", "start", "stop"]
    recorded_at: UtcDateTime
    execution_generation: int
    start_request_id: UUID | None


class DetectorReason(BaseModel):
    code: str
    message: str
    factor: str | None = None


class DetectorFailure(BaseModel):
    reason: DetectorReason
    at: UtcDateTime
    start_request_id: UUID | None


class DetectorWindow(BaseModel):
    index: int
    kind: Literal["evaluated", "skipped"]
    reason: DetectorReason | None
    start_utc: UtcDateTime
    end_utc: UtcDateTime
    scheduled_at: UtcDateTime
    aggregation_started_at: UtcDateTime
    aggregation_completed_at: UtcDateTime
    evaluation_started_at: UtcDateTime | None
    evaluation_completed_at: UtcDateTime | None
    late: bool


class DetectorSummary(BaseModel):
    id: UUID
    name: str
    aggregate_id: UUID
    aggregate_name: str
    aggregate_state: Literal["active", "archived", "deleted"]
    source_id: UUID
    source_name: str
    created_at: UtcDateTime
    execution_state: ExecutionState
    model_state: ModelState
    accelerated: bool
    accelerated_used: bool
    reason: DetectorReason | None
    last_consumed: DetectorWindow | None
    last_evaluated: DetectorWindow | None
    backlog: int | None
    lag_seconds: float | None


class DetectorWarmupSummary(BaseModel):
    cutoff: UtcDateTime
    step_seconds: int
    required_windows: int
    planned_windows: int
    materialized_windows: int
    feature_valid_windows: int
    eligible_windows: int
    ineligible_windows: int
    materialized_coverage_seconds: float
    eligible_coverage_seconds: float
    readiness: ModelState
    reason: DetectorReason | None
    status: Literal["preparing", "prepared", "waiting", "ready"]


class DetectorRecoverySummary(BaseModel):
    reason: Literal["crash_recovery", "manual_resume"]
    first_index: int
    last_index: int
    started_at: UtcDateTime


class DetectorConfig(ModelOptions):
    dimensions: int


class DetectorCard(DetectorSummary):
    config: DetectorConfig
    feature_keys: list[str]
    feature_names: list[str]
    anchor_utc: UtcDateTime
    duration_seconds: int
    step_seconds: int
    revision: int
    execution_generation: int
    start_request_id: UUID | None
    start_requested_at: UtcDateTime | None
    execution_started_at: UtcDateTime | None
    stopped_at: UtcDateTime | None
    updated_at: UtcDateTime
    last_failure: DetectorFailure | None
    warmup: DetectorWarmupSummary | None
    recovery: DetectorRecoverySummary | None
    next_scheduled_at: UtcDateTime | None
    waiting_for_point: bool | None


class DetectorPage(BaseModel):
    items: list[DetectorSummary]
    total: int
    limit: int
    offset: int
