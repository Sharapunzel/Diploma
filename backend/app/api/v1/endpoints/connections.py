from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response

from ....dependencies import (
    get_connection_service,
    get_source_service,
    require_permission,
)
from ....schemas.auth import ErrorResponse
from ....schemas.connections import (
    KafkaConnectionCreate,
    KafkaConnectionDTO,
    KafkaConnectionPage,
    KafkaConnectionPatch,
    KafkaTestResponse,
    SourceCreate,
    SourceDTO,
    SourceNormalizerUpdate,
    SourcePage,
    SourcePatch,
    TopicPage,
)
from ....services.protocols.connections import KafkaConnectionService, SourceService

router = APIRouter(tags=["connections", "sources"])
ERRORS = {
    "401": {"model": ErrorResponse},
    "403": {"model": ErrorResponse},
    "404": {"model": ErrorResponse},
    "409": {"model": ErrorResponse},
    "422": {"model": ErrorResponse},
    "503": {"model": ErrorResponse},
    "504": {"model": ErrorResponse},
}


@router.get("/kafka-connections", response_model=KafkaConnectionPage, responses=ERRORS)
def list_connections(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = None,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.read")),
):
    items, total = service.list(q, limit, offset)
    return KafkaConnectionPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/kafka-connections/{connection_id}", response_model=KafkaConnectionDTO, responses=ERRORS)
def get_connection(
    connection_id: UUID,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.read")),
):
    return service.get(connection_id)


@router.post("/kafka-connections", response_model=KafkaConnectionDTO, status_code=201, responses=ERRORS)
def create_connection(
    data: KafkaConnectionCreate,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.write")),
):
    return service.create(data)


@router.patch("/kafka-connections/{connection_id}", response_model=KafkaConnectionDTO, responses=ERRORS)
def update_connection(
    connection_id: UUID,
    data: KafkaConnectionPatch,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.write")),
):
    return service.update(connection_id, data)


@router.delete("/kafka-connections/{connection_id}", status_code=204, responses=ERRORS)
def delete_connection(
    connection_id: UUID,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.write")),
):
    service.delete(connection_id)
    return Response(status_code=204)


@router.post("/kafka-connections/{connection_id}/test", response_model=KafkaTestResponse, responses=ERRORS)
def test_connection(
    connection_id: UUID,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.read")),
):
    return service.test(connection_id)


@router.get("/kafka-connections/{connection_id}/topics", response_model=TopicPage, responses=ERRORS)
def list_topics(
    connection_id: UUID,
    include_internal: bool = False,
    service: KafkaConnectionService = Depends(get_connection_service),
    _: object = Depends(require_permission("connections.read")),
):
    items, total = service.topics(connection_id, include_internal)
    return TopicPage(items=items, total=total)


@router.get("/sources", response_model=SourcePage, responses=ERRORS)
def list_sources(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = None,
    connection_id: UUID | None = None,
    normalizer_id: UUID | None = None,
    is_enabled: bool | None = None,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.read")),
):
    items, total = service.list(q, connection_id, normalizer_id, is_enabled, limit, offset)
    return SourcePage(items=items, total=total, limit=limit, offset=offset)


@router.get("/sources/{source_id}", response_model=SourceDTO, responses=ERRORS)
def get_source(
    source_id: UUID,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.read")),
):
    return service.get(source_id)


@router.post("/sources", response_model=SourceDTO, status_code=201, responses=ERRORS)
def create_source(
    data: SourceCreate,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    return service.create(data)


@router.patch("/sources/{source_id}", response_model=SourceDTO, responses=ERRORS)
def update_source(
    source_id: UUID,
    data: SourcePatch,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    return service.update(source_id, data)


@router.delete("/sources/{source_id}", status_code=204, responses=ERRORS)
def delete_source(
    source_id: UUID,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    service.delete(source_id)
    return Response(status_code=204)


@router.put("/sources/{source_id}/normalizer", response_model=SourceDTO, responses=ERRORS)
def set_source_normalizer(
    source_id: UUID,
    data: SourceNormalizerUpdate,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    return service.set_normalizer(source_id, data)


@router.post("/sources/{source_id}/enable", response_model=SourceDTO, responses=ERRORS)
def enable_source(
    source_id: UUID,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    return service.enable(source_id)


@router.post("/sources/{source_id}/disable", response_model=SourceDTO, responses=ERRORS)
def disable_source(
    source_id: UUID,
    service: SourceService = Depends(get_source_service),
    _: object = Depends(require_permission("sources.write")),
):
    return service.disable(source_id)
