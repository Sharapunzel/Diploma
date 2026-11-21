from typing import Protocol
from uuid import UUID

from ...schemas.findings import (
    FindingCard,
    FindingDetector,
    FindingListQuery,
    FindingPage,
)


class FindingService(Protocol):
    def page(self, query: FindingListQuery) -> FindingPage: ...
    def detectors(self) -> list[FindingDetector]: ...
    def card(self, finding_id: UUID) -> FindingCard: ...
