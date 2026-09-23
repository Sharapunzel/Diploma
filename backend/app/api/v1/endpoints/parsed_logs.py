from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response

from ....dependencies import get_parsed_log_service, require_permission
from ....schemas.errors import ErrorResponse
from ....schemas.parsed_logs import (
    ParsedLogBulkDeleteRequest,
    ParsedLogBulkDeleteResponse,
    ParsedLogDetail,
    ParsedLogSearchRequest,
    ParsedLogSearchResponse,
)
from ....services.protocols.parsed_logs import ParsedLogService

router = APIRouter(prefix="/parsed-logs", tags=["Parsed logs"])
ReadPermission = Annotated[tuple, Depends(require_permission("events.read"))]
DeletePermission = Annotated[tuple, Depends(require_permission("events.delete"))]
Service = Annotated[ParsedLogService, Depends(get_parsed_log_service)]
READ_ERRORS = {
    401: {"model": ErrorResponse, "description": "Authentication required"},
    403: {"model": ErrorResponse, "description": "Permission denied"},
}
DELETE_ERRORS = READ_ERRORS | {
    404: {"model": ErrorResponse, "description": "Parsed log not found"},
}


@router.post("/search", response_model=ParsedLogSearchResponse, responses=READ_ERRORS | {
    422: {"model": ErrorResponse, "description": "Invalid search or ECS filter"},
})
def search_parsed_logs(_: ReadPermission, body: ParsedLogSearchRequest, service: Service):
    return service.search(body)


@router.get("/{log_id}", response_model=ParsedLogDetail, responses=READ_ERRORS | {
    404: {"model": ErrorResponse, "description": "Parsed log not found"},
})
def get_parsed_log(_: ReadPermission, log_id: UUID, service: Service):
    return service.get(log_id)


@router.delete("/{log_id}", status_code=204, responses=DELETE_ERRORS)
def delete_parsed_log(_: DeletePermission, log_id: UUID, service: Service):
    service.delete(log_id)
    return Response(status_code=204)


@router.post("/actions/delete", response_model=ParsedLogBulkDeleteResponse, responses=READ_ERRORS | {
    422: {"model": ErrorResponse, "description": "Invalid or missing deletion scope"},
})
def delete_parsed_logs(
    _: DeletePermission, body: ParsedLogBulkDeleteRequest, service: Service
):
    return service.delete_many(body)
