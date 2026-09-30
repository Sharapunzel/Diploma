from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from ....core.errors import DomainError
from ....dependencies import get_diagnostics_service, require_permission
from ....schemas.auth import ErrorResponse
from ....schemas.diagnostics import (
    OperationalEventPage,
    Overview,
    ProcessingErrorDTO,
    ProcessingErrorPage,
    RawPayloadDTO,
    SourceState,
    SourceStatePage,
)
from ....services.protocols.diagnostics import DiagnosticsService

router = APIRouter(prefix="/diagnostics", tags=["Diagnostics"])
READ = {
    "401": {"model": ErrorResponse},
    "403": {"model": ErrorResponse},
    "404": {"model": ErrorResponse},
    "422": {"model": ErrorResponse},
}
ReadPermission = Depends(require_permission("events.read"))
Service = Depends(get_diagnostics_service)


def check_interval(start: datetime | None, end: datetime | None):
    if any(value is not None and value.utcoffset() is None for value in (start, end)):
        raise DomainError("invalid_time_range", "timestamps must include a timezone", 422)
    if start is not None and end is not None and start >= end:
        raise DomainError("invalid_time_range", "start must be earlier than end", 422)


@router.get("/sources", response_model=SourceStatePage, responses=READ)
def source_states(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _: object = ReadPermission,
    service: DiagnosticsService = Service,
):
    return service.source_states(limit, offset)


@router.get("/sources/{source_id}", response_model=SourceState, responses=READ)
def source_state(
    source_id: UUID, _: object = ReadPermission, service: DiagnosticsService = Service
):
    return service.source_state(source_id)


@router.get("/processing-errors", response_model=ProcessingErrorPage, responses=READ)
def processing_errors(
    source_id: UUID | None = None,
    stage: Literal["decode", "envelope", "normalization"] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _: object = ReadPermission,
    service: DiagnosticsService = Service,
):
    check_interval(start, end)
    return service.errors(source_id, stage, start, end, limit, offset)


@router.get("/processing-errors/{error_id}/raw", response_model=RawPayloadDTO, responses=READ)
def raw_error_payload(
    error_id: UUID,
    _: object = Depends(require_permission("processing_errors.raw.read")),
    service: DiagnosticsService = Service,
):
    return service.raw_payload(error_id)


@router.get("/processing-errors/{error_id}", response_model=ProcessingErrorDTO, responses=READ)
def processing_error(
    error_id: UUID, _: object = ReadPermission, service: DiagnosticsService = Service
):
    return service.error(error_id)


@router.get("/kafka-events", response_model=OperationalEventPage, responses=READ)
def kafka_events(
    source_id: UUID | None = None,
    kind: Literal["retention_gap", "topic_recreated"] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _: object = ReadPermission,
    service: DiagnosticsService = Service,
):
    check_interval(start, end)
    return service.events(source_id, kind, start, end, limit, offset)


@router.get("/overview", response_model=Overview, responses=READ)
def overview(
    limit: int = Query(10, ge=1, le=50),
    _: object = ReadPermission,
    service: DiagnosticsService = Service,
):
    return service.overview(limit)
