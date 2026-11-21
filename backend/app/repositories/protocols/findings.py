from typing import Protocol
from uuid import UUID

from ...schemas.findings import (
    FindingDetector,
    FindingListQuery,
    FindingPage,
)


class FindingRepository(Protocol):
    def page(self, query: FindingListQuery) -> FindingPage | None: ...
    def detectors(self) -> list[FindingDetector]: ...
    def snapshot(self, finding_id: UUID) -> tuple[UUID, bool, dict] | None: ...
