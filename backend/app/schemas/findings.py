from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, field_validator, model_validator

UnitValue = Annotated[float, Field(ge=0, le=1)]
TimeBasis = Literal["event_time", "availability_at", "backend_processed_at_historical"]


class FindingFilters(BaseModel):
    timestamp_from: AwareDatetime
    timestamp_to: AwareDatetime
    detector_id: UUID | None = None
    grade_min: UnitValue | None = None
    grade_max: UnitValue | None = None
    confidence_min: UnitValue | None = None
    confidence_max: UnitValue | None = None

    @field_validator("timestamp_from", "timestamp_to")
    @classmethod
    def normalize_boundary(cls, value):
        try:
            return value.astimezone(UTC)
        except OverflowError:
            raise ValueError("timestamp outside the UTC range") from None

    @model_validator(mode="after")
    def validate_bounds(self):
        if self.timestamp_from >= self.timestamp_to:
            raise ValueError("timestamp_from must be earlier than timestamp_to")
        for lower, upper in (
            (self.grade_min, self.grade_max),
            (self.confidence_min, self.confidence_max),
        ):
            if lower is not None and upper is not None and lower > upper:
                raise ValueError("numeric lower bound must not exceed upper bound")
        return self


class FindingListQuery(FindingFilters):
    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    anchor_id: UUID | None = None


class FindingRow(BaseModel):
    id: UUID
    detector_id: UUID
    detector_name: str | None
    source_name: str | None
    window_start: datetime | None
    window_end: datetime | None
    time_basis: TimeBasis
    grade: UnitValue | None
    confidence: UnitValue | None
    late: bool
    accelerated_used: bool

    @field_validator("window_start", "window_end")
    @classmethod
    def window_utc(cls, value):
        return value.astimezone(UTC) if value is not None else None


class FindingPage(BaseModel):
    items: list[FindingRow]
    total: int
    limit: int
    offset: int


class FindingDetector(BaseModel):
    detector_id: UUID
    detector_name: str | None


class FindingFeature(BaseModel):
    key: str
    name: str
    actual: str | None
    expected: str | None
    attribution: float | None


class FindingEventRange(BaseModel):
    timestamp_from: datetime
    timestamp_to: datetime


class FindingCard(FindingRow):
    aggregate_name: str | None
    source_id: UUID | None
    event_range: FindingEventRange | None
    event_range_reason: Literal["empty_window", "unavailable", "unrepresentable"] | None
    aggregation_started_at: datetime | None
    aggregation_completed_at: datetime | None
    evaluation_started_at: datetime | None
    evaluation_completed_at: datetime | None
    expected_origin: Literal["aws_rcf", "preceding_median"] | None
    features: list[FindingFeature]
