from datetime import datetime
from typing import Protocol
from uuid import UUID

from ...ecs import ValidatedEcsFilter
from ...models import ParsedLog


class ParsedLogRepository(Protocol):
    def snapshot_boundary(self) -> datetime: ...
    def search(
        self,
        *,
        source_ids: list[UUID] | None,
        connection_ids: list[UUID] | None,
        normalizer_ids: list[UUID] | None,
        kafka_topics: list[str] | None,
        kafka_partitions: list[int] | None,
        raw_query: str | None,
        processed_from: datetime | None,
        processed_to: datetime | None,
        collected_from: datetime | None,
        collected_to: datetime | None,
        ecs_filters: list[ValidatedEcsFilter],
        after: tuple[datetime, UUID] | None,
        snapshot_boundary: datetime,
        limit: int,
    ) -> list[tuple[ParsedLog, str, str | None]]: ...
    def find_by_id(self, log_id: UUID) -> ParsedLog | None: ...
    def delete(self, log: ParsedLog) -> None: ...
    def delete_many(
        self, source_id: UUID | None, processed_from: datetime | None,
        processed_to: datetime | None,
    ) -> int: ...
