from typing import Literal

from pydantic import BaseModel, Field


class SnapshotRecord(BaseModel):
    position: int = Field(ge=0, le=49, description="Position in this snapshot; not a Kafka offset")
    timestamp: str | None = None
    log: str | None = None
    issue: Literal["invalid_envelope", "message_too_large", "response_limit"] | None = None


class SourceSnapshot(BaseModel):
    records: list[SnapshotRecord] = Field(max_length=50)
    skipped: int = Field(ge=0, le=50)
    truncated: bool
