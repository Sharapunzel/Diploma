from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from ....dependencies import get_finding_service, require_permission
from ....schemas.findings import (
    FindingCard,
    FindingDetector,
    FindingListQuery,
    FindingPage,
)
from ....services.protocols.findings import FindingService

router = APIRouter(
    prefix="/findings",
    tags=["Findings"],
    dependencies=[Depends(require_permission("findings.read"))],
)
Service = Annotated[FindingService, Depends(get_finding_service)]


@router.get("", response_model=FindingPage)
def list_findings(query: Annotated[FindingListQuery, Query()], service: Service):
    return service.page(query)


@router.get("/timeline", response_model=FindingPage)
def finding_timeline(query: Annotated[FindingListQuery, Query()], service: Service):
    return service.page(query)


@router.get("/detectors", response_model=list[FindingDetector])
def finding_detectors(service: Service):
    return service.detectors()


@router.get("/{finding_id}", response_model=FindingCard)
def finding_card(finding_id: UUID, service: Service):
    return service.card(finding_id)
