from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from ....dependencies import get_event_query_service, require_permission
from ....schemas.errors import ErrorResponse
from ....schemas.events import EventCard, EventFieldPage, EventSearchRequest, EventSearchResponse
from ....services.protocols.events import EventQueryService

router = APIRouter(prefix="/events/sources/{source_id}", tags=["Events"])
Service = Annotated[EventQueryService, Depends(get_event_query_service)]
ReadPermission = Annotated[tuple, Depends(require_permission("events.read"))]
AUTH_ERRORS = {
    401: {"model": ErrorResponse, "description": "Authentication required"},
    403: {"model": ErrorResponse, "description": "Permission denied"},
    404: {"model": ErrorResponse, "description": "Source or event not found"},
}
READ_ERRORS = AUTH_ERRORS | {
    409: {"model": ErrorResponse, "description": "Source is disabled"},
    422: {"model": ErrorResponse, "description": "Invalid field, filter, or cursor"},
    501: {"model": ErrorResponse, "description": "Reading this source type is not implemented"},
}


@router.get("/fields", response_model=EventFieldPage, responses=READ_ERRORS)
def get_event_fields(
    source_id: UUID,
    _: ReadPermission,
    service: Service,
    q: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=100_000),
):
    return service.fields(source_id, q, limit, offset)


@router.post("/search", response_model=EventSearchResponse, responses=READ_ERRORS)
def search_events(source_id: UUID, body: EventSearchRequest, _: ReadPermission, service: Service):
    return service.search(source_id, body)


@router.get("/events/{event_id}", response_model=EventCard, responses=READ_ERRORS)
def get_event(source_id: UUID, event_id: str, _: ReadPermission, service: Service):
    return service.get(source_id, event_id)
