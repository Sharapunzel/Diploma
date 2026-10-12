from typing import Protocol
from uuid import UUID

from ...schemas.debug_snapshot import SourceSnapshot


class SourceSnapshotService(Protocol):
    def read(self, source_id: UUID) -> SourceSnapshot: ...
