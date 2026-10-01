from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


class EventQueryRepository(Protocol):
    def snapshot_boundary(self) -> datetime: ...
    def search(
        self,
        source_id: UUID,
        filters: list[tuple[Any, str, Any]],
        timestamp_from: datetime | None,
        timestamp_to: datetime | None,
        sort: str,
        after: tuple[datetime, UUID] | None,
        snapshot_boundary: datetime,
        limit: int,
    ) -> list[tuple[UUID, datetime, dict]]: ...
    def get(self, source_id: UUID, event_id: UUID) -> Any | None: ...


class EventSourceRepository(Protocol):
    def source_type(self, source_id: UUID) -> tuple[str, bool] | None: ...
