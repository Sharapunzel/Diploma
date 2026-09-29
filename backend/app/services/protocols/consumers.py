from typing import Protocol
from uuid import UUID


class SourceConsumerLifecycle(Protocol):
    def wake(self) -> None: ...
    def stop_source(self, source_id: UUID) -> None: ...
