from typing import Protocol
from uuid import UUID


class EventDeletionService(Protocol):
    def delete(self, source_id: UUID, event_id: str) -> None: ...


class EventIdDecoder(Protocol):
    def decode(self, value: str) -> UUID: ...
