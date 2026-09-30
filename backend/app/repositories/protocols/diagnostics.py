from datetime import datetime
from typing import Protocol
from uuid import UUID

from ...models import KafkaOperationalEvent, ProcessingError, Source


class DiagnosticsRepository(Protocol):
    def sources(self, limit: int, offset: int) -> tuple[list[Source], int]: ...
    def source(self, source_id: UUID) -> Source | None: ...
    def connection_names(self, connection_ids: list[UUID]) -> dict[UUID, str]: ...
    def errors(
        self,
        source_id: UUID | None,
        stage: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ) -> tuple[list[ProcessingError], int]: ...
    def error(self, error_id: UUID) -> ProcessingError | None: ...
    def events(
        self,
        source_id: UUID | None,
        kind: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ) -> tuple[list[KafkaOperationalEvent], int]: ...
    def overview(self, recent_limit: int, worker_source_ids: list[UUID]) -> dict: ...
