"""Aggregate HTTP boundary: typed requests, permissions and service calls."""

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query

from ....dependencies import get_aggregate_service, require_permission
from ....schemas.aggregates import (
    AggregateCard,
    AggregateFieldPage,
    AggregatePage,
    AggregatePreviewRequest,
    CreateAggregateRequest,
    HistoricalRequest,
    OperatorCard,
    PointCard,
    PointPage,
    PreviewResult,
    ValidateRequest,
    ValidationResult,
)
from ....services.protocols.aggregates import AggregateServiceProtocol

router = APIRouter(prefix="/aggregates", tags=["Aggregates"])
Service = Annotated[AggregateServiceProtocol, Depends(get_aggregate_service)]
Read = Annotated[tuple, Depends(require_permission("aggregates.read"))]
Write = Annotated[tuple, Depends(require_permission("aggregates.write"))]


@router.get("/operators", response_model=list[OperatorCard])
def operators(service: Service, _: Read):
    return service.operators()


@router.get("/sources/{source_id}/fields", response_model=AggregateFieldPage)
def fields(source_id: UUID, service: Service, _: Read,
           q: str | None = Query(None, max_length=200),
           limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    return service.fields(source_id, q, limit, offset)


@router.post("/validate", response_model=ValidationResult)
def validate(body: ValidateRequest, service: Service, _: Write):
    return service.validate(body)


@router.post("/preview", response_model=PreviewResult)
def preview(body: AggregatePreviewRequest, service: Service, _: Write):
    return service.preview(body)


@router.post("", response_model=AggregateCard, status_code=201)
def create(body: CreateAggregateRequest, service: Service, principal: Write,
           idempotency_key: str = Header(..., min_length=1, max_length=200)):
    return service.create(body, principal[1].id, idempotency_key)


@router.get("", response_model=AggregatePage)
def list_aggregates(service: Service, _: Read, limit: int = Query(50, ge=1, le=100),
                    offset: int = Query(0, ge=0, le=100000),
                    q: str | None = Query(None, max_length=200),
                    state: Literal["active", "archived"] | None = None):
    return service.list(limit, offset, q, state)


@router.get("/{aggregate_id}", response_model=AggregateCard)
def card(aggregate_id: UUID, service: Service, _: Read):
    return service.card(aggregate_id)


@router.get("/{aggregate_id}/points", response_model=PointPage)
def points(aggregate_id: UUID, service: Service, _: Read, limit: int = Query(50, ge=1, le=100),
           offset: int = Query(0, ge=0, le=100000)):
    return service.points(aggregate_id, limit, offset)


@router.get("/{aggregate_id}/copy", response_model=ValidateRequest)
def copy(aggregate_id: UUID, service: Service, _: Write):
    return service.copy(aggregate_id)


@router.post("/{aggregate_id}/historical-points", response_model=PointCard)
def historical(aggregate_id: UUID, body: HistoricalRequest, service: Service, _: Write):
    return service.historical(aggregate_id, body)


@router.post("/{aggregate_id}/archive", response_model=AggregateCard)
def archive(aggregate_id: UUID, service: Service, principal: Write):
    return service.lifecycle(aggregate_id, "archive", principal[1].id)


@router.post("/{aggregate_id}/resume", response_model=AggregateCard)
def resume(aggregate_id: UUID, service: Service, principal: Write):
    return service.lifecycle(aggregate_id, "resume", principal[1].id)


@router.delete("/{aggregate_id}", response_model=AggregateCard)
def delete(aggregate_id: UUID, service: Service, principal: Write):
    return service.lifecycle(aggregate_id, "delete", principal[1].id)
