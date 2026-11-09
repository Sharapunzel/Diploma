"""Authenticated detector HTTP operations; calculation belongs to the worker."""
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Query

from ....dependencies import get_detector_service, require_permission
from ....schemas.detectors import (
    CreateDetectorRequest,
    DetectorCard,
    DetectorCommandReceipt,
    DetectorPage,
    EmptyCommand,
    ExecutionState,
)
from ....services.protocols.detectors import DetectorService

router = APIRouter(prefix="/detectors", tags=["Detectors"])
Service = Annotated[DetectorService, Depends(get_detector_service)]
Read = Annotated[tuple, Depends(require_permission("detectors.read"))]
Write = Annotated[tuple, Depends(require_permission("detectors.write"))]
Key = Annotated[str, Header(min_length=1, max_length=200, pattern=r"\S")]


@router.post("", response_model=DetectorCommandReceipt, status_code=201)
def create(body: CreateDetectorRequest, service: Service, principal: Write, idempotency_key: Key):
    return service.command("create", principal[1].id, idempotency_key, payload=body.model_dump(mode="json"))


@router.get("", response_model=DetectorPage)
def list_detectors(service: Service, _: Read, limit: int = Query(50, ge=1, le=100),
                   offset: int = Query(0, ge=0, le=100000), aggregate_id: UUID | None = None,
                   execution_state: ExecutionState | None = None):
    return service.public_list(limit, offset, aggregate_id, execution_state)


@router.get("/{detector_id}", response_model=DetectorCard)
def card(detector_id: UUID, service: Service, _: Read):
    return service.public_card(detector_id)


@router.post("/{detector_id}/start", response_model=DetectorCommandReceipt, status_code=202)
def start(detector_id: UUID, service: Service, principal: Write, idempotency_key: Key,
          body: EmptyCommand = Body(default_factory=EmptyCommand)):
    return service.command("start", principal[1].id, idempotency_key, detector_id, body.model_dump())


@router.post("/{detector_id}/stop", response_model=DetectorCommandReceipt)
def stop(detector_id: UUID, service: Service, principal: Write, idempotency_key: Key,
         body: EmptyCommand = Body(default_factory=EmptyCommand)):
    return service.command("stop", principal[1].id, idempotency_key, detector_id, body.model_dump())
