from datetime import datetime
from typing import Protocol
from uuid import UUID

from ...schemas.diagnostics import (
    OperationalEventPage,
    Overview,
    ProcessingErrorPage,
    RawPayloadDTO,
    SourceStatePage,
)


class DiagnosticsService(Protocol):
    def source_states(self, limit: int, offset: int) -> SourceStatePage: ...
    def source_state(self, source_id: UUID): ...
    def errors(
        self,
        source_id: UUID | None,
        stage: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ) -> ProcessingErrorPage: ...
    def error(self, error_id: UUID): ...
    def raw_payload(self, error_id: UUID) -> RawPayloadDTO: ...
    def events(
        self,
        source_id: UUID | None,
        kind: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ) -> OperationalEventPage: ...
    def overview(self, limit: int) -> Overview: ...
