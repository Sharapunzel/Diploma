from typing import Protocol
from uuid import UUID

from ...schemas.events import (
    EventCard,
    EventFieldPage,
    EventSearchRequest,
    EventSearchResponse,
)


class EventQueryProvider(Protocol):
    """A source-type-specific provider for the common read-only event contract.

    Providers must use opaque cursors bound to source, normalized filters, time bounds,
    sort direction, and their stable snapshot boundary. Cursors may carry provider-specific
    continuation state, such as a PostgreSQL keyset or OpenSearch search_after token.
    """

    source_type: str

    def fields(self, source_id: UUID, q: str | None, limit: int, offset: int) -> EventFieldPage: ...
    def search(self, source_id: UUID, request: EventSearchRequest) -> EventSearchResponse: ...
    def get(self, source_id: UUID, event_id: str) -> EventCard: ...


class EventQueryService(Protocol):
    def fields(self, source_id: UUID, q: str | None, limit: int, offset: int) -> EventFieldPage: ...
    def search(self, source_id: UUID, request: EventSearchRequest) -> EventSearchResponse: ...
    def get(self, source_id: UUID, event_id: str) -> EventCard: ...
