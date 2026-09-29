from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from ...repositories.protocols.processing import KafkaCoordinates

ProcessingOutcome = Literal[
    "stored_complete", "stored_partial", "stored_failed", "already_processed"
]


@dataclass(frozen=True)
class ProcessMessageResult:
    outcome: ProcessingOutcome
    processed_record_id: UUID | None = None


class DurableProcessingService(Protocol):
    def process(
        self,
        source_id: UUID,
        coordinates: KafkaCoordinates,
        payload: bytes | None,
        received_at: datetime,
    ) -> ProcessMessageResult: ...
