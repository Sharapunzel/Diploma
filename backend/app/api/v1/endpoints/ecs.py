from fastapi import APIRouter, Depends, Query

from ....dependencies import get_ecs_catalog_service, require_permission
from ....schemas.errors import ErrorResponse
from ....schemas.parsed_logs import EcsFieldDTO, EcsFieldPage, EcsSchemaDTO
from ....services.protocols.parsed_logs import EcsCatalogService

router = APIRouter(prefix="/ecs", tags=["ECS"])
AUTH_ERRORS = {
    401: {"model": ErrorResponse, "description": "Authentication required"},
    403: {"model": ErrorResponse, "description": "Permission denied"},
}


@router.get("/schema", response_model=EcsSchemaDTO, responses=AUTH_ERRORS)
def get_schema(
    _: tuple = Depends(require_permission("events.read")),
    service: EcsCatalogService = Depends(get_ecs_catalog_service),
):
    return service.schema()


@router.get("/fields", response_model=EcsFieldPage, responses=AUTH_ERRORS | {
    422: {"model": ErrorResponse, "description": "Invalid catalog query"},
})
def get_fields(
    q: str | None = Query(default=None, max_length=200),
    field_type: str | None = Query(default=None, alias="type", max_length=64),
    level: str | None = Query(default=None, max_length=32),
    filterable: bool | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=100_000),
    _: tuple = Depends(require_permission("events.read")),
    service: EcsCatalogService = Depends(get_ecs_catalog_service),
):
    return service.fields(q, field_type, level, filterable, limit, offset)


@router.get("/fields/{field_name:path}", response_model=EcsFieldDTO, responses=AUTH_ERRORS | {
    404: {"model": ErrorResponse, "description": "ECS field not found"},
})
def get_field(
    field_name: str,
    _: tuple = Depends(require_permission("events.read")),
    service: EcsCatalogService = Depends(get_ecs_catalog_service),
):
    return service.field(field_name)
