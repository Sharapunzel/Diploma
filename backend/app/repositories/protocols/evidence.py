"""Plain saved evidence read by window completeness rules."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ProbeEvidence:
    id: UUID
    source_identity: UUID
    input_generation: UUID
    public_key: bytes
    is_active: bool


@dataclass(frozen=True, slots=True)
class AttestationEvidence:
    id: UUID
    probe_id: UUID
    source_identity: UUID
    input_generation: UUID
    sequence: int
    start_utc: datetime
    end_utc: datetime
    state: str
    topic_identity: str
    start_positions: dict
    end_positions: dict
    signed_payload: dict
    signature: bytes


@dataclass(frozen=True, slots=True)
class CheckpointEvidence:
    id: UUID
    partitions: dict


@dataclass(frozen=True, slots=True)
class ConsumerHeartbeat:
    id: UUID
    worker_instance: UUID
    observed_at: datetime


class CoverageEvidenceRepository(Protocol):
    def interruptions(self, source_id: UUID, start: datetime, end: datetime,
                      barrier_at: datetime) -> list[UUID]: ...
    def attestations(self, source_id: UUID, start: datetime, end: datetime,
                     barrier_at: datetime) -> list[tuple[AttestationEvidence, ProbeEvidence]]: ...
    def checkpoint(self, source_id: UUID, topic: str | None, end: datetime,
                   barrier_at: datetime, reference_at: datetime | None = None) -> CheckpointEvidence | None: ...
    def consumer_heartbeats(self, source_id: UUID, topic: str, start: datetime,
                            end: datetime) -> list[ConsumerHeartbeat]: ...
    def has_gap(self, source_id: UUID, topic: str, partition: int,
                beginning: int, final: int) -> bool: ...
    def processing_counts(self, source_id: UUID, topic: str, partition: int,
                          beginning: int, final: int) -> tuple[int, int]: ...
