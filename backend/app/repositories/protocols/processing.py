from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from ...models import ParsedLog, ProcessedKafkaRecord, ProcessingError

StoredResultStatus = Literal["complete", "partial", "failed"]


@dataclass(frozen=True)
class KafkaCoordinates:
    connection_id: UUID
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ProcessingContext:
    connection_id: UUID
    connection_name: str
    source_id: UUID
    source_name: str
    topic: str
    normalizer_id: UUID
    normalizer_name: str
    normalizer_version: int
    rule: dict[str, Any]


class ProcessingRepository(Protocol):
    def find_result(self, coordinates: KafkaCoordinates) -> StoredResultStatus | None: ...
    def resolve_context(self, source_id: UUID) -> ProcessingContext | None: ...
    def store_event(self, record: ProcessedKafkaRecord, event: ParsedLog) -> bool: ...
    def store_error(
        self, record: ProcessedKafkaRecord, error: ProcessingError
    ) -> bool: ...


class ProcessingClock(Protocol):
    def __call__(self) -> datetime: ...
