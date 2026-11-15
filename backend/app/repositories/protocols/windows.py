"""Persistence needed to close a working window."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from ...models import AvailabilityMarker


class WorkingWindowRepository(Protocol):
    def db_now(self) -> datetime: ...
    def lock_source(self, source_id: UUID) -> bool: ...
    def ensure_barrier(self, marker_id: UUID, source_id: UUID, at: datetime) -> None: ...
    def barrier(self, marker_id: UUID) -> AvailabilityMarker | None: ...
