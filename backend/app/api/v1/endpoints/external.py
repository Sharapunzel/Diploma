from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response

from ....core.errors import DomainError
from ....dependencies import get_external_service, require_permission
from ....schemas.auth import ErrorResponse
from ....schemas.external import (
    ExternalConnectionCreate,
    ExternalConnectionDTO,
    ExternalConnectionPage,
    ExternalConnectionPatch,
    ExternalSourceCreate,
    ExternalSourceDTO,
    ExternalSourcePage,
    ExternalSourcePatch,
    ExternalTestResponse,
    IndexPage,
)
from ....services.protocols.external import ExternalService

router = APIRouter(tags=["external connections", "external sources"])
ERRORS = {str(code): {"model": ErrorResponse} for code in (401, 403, 404, 409, 422, 502, 503, 504)}
READ_ERRORS = {key: ERRORS[key] for key in ("401", "403", "422")}
DETAIL_ERRORS = {**READ_ERRORS, "404": ERRORS["404"]}
WRITE_ERRORS = {**DETAIL_ERRORS, "409": ERRORS["409"]}
INDEXER_ERRORS = {**DETAIL_ERRORS, **{key: ERRORS[key] for key in ("502", "503", "504")}}
ENABLE_ERRORS = {**INDEXER_ERRORS, "409": ERRORS["409"]}
CA_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/x-pem-file": {
                "schema": {
                    "type": "string",
                    "format": "binary",
                    "description": "PEM X.509 CA certificate or CA chain, 1–65536 bytes.",
                }
            }
        },
    }
}


def connection_dto(item):
    return ExternalConnectionDTO(id=item.id, name=item.name, base_url=item.base_url,
                                 username=item.username, has_password=bool(item.encrypted_password),
                                 has_ca=item.ca_pem is not None,
                                 created_at=item.created_at, updated_at=item.updated_at)


def source_dto(item):
    return ExternalSourceDTO(id=item.id, source_type="external", name=item.name,
                             external_connection_id=item.external_connection_id,
                             target_type=item.target_type,
                             index_name=item.index_name, index_pattern=item.index_pattern,
                             data_stream_name=item.data_stream_name,
                             data_stream_pattern=item.data_stream_pattern,
                             is_enabled=item.is_enabled, created_at=item.created_at,
                             updated_at=item.updated_at)


@router.get("/external-connections", response_model=ExternalConnectionPage,
            responses=READ_ERRORS)
def connections(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
                service: ExternalService = Depends(get_external_service),
                _: object = Depends(require_permission("connections.read"))):
    items, total = service.connections(limit, offset)
    return ExternalConnectionPage(items=[connection_dto(item) for item in items],
                                  total=total, limit=limit, offset=offset)


@router.get("/external-connections/{identity}", response_model=ExternalConnectionDTO,
            responses=DETAIL_ERRORS)
def connection(identity: UUID, service: ExternalService = Depends(get_external_service),
               _: object = Depends(require_permission("connections.read"))):
    return connection_dto(service.connection(identity))


@router.post("/external-connections", response_model=ExternalConnectionDTO, status_code=201,
             responses=WRITE_ERRORS)
def create_connection(data: ExternalConnectionCreate,
                      service: ExternalService = Depends(get_external_service),
                      _: object = Depends(require_permission("connections.write"))):
    return connection_dto(service.create_connection(data))


@router.patch("/external-connections/{identity}", response_model=ExternalConnectionDTO,
              responses=WRITE_ERRORS)
def update_connection(identity: UUID, data: ExternalConnectionPatch,
                      service: ExternalService = Depends(get_external_service),
                      _: object = Depends(require_permission("connections.write"))):
    return connection_dto(service.update_connection(identity, data))


@router.delete("/external-connections/{identity}", status_code=204,
               responses=DETAIL_ERRORS)
def delete_connection(identity: UUID, service: ExternalService = Depends(get_external_service),
                      _: object = Depends(require_permission("connections.write"))):
    service.delete_connection(identity)
    return Response(status_code=204)


@router.put("/external-connections/{identity}/ca", status_code=204,
            responses=DETAIL_ERRORS, openapi_extra=CA_BODY)
async def upload_ca(identity: UUID, request: Request,
                    service: ExternalService = Depends(get_external_service),
                    _: object = Depends(require_permission("connections.write"))):
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/x-pem-file":
        raise DomainError("invalid_ca_media_type", "Use application/x-pem-file", 422)
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > 65536:
            raise DomainError("invalid_ca", "CA exceeds 65536 bytes", 422)
    service.set_ca(identity, bytes(content))
    return Response(status_code=204)


@router.delete("/external-connections/{identity}/ca", status_code=204,
               responses=DETAIL_ERRORS)
def delete_ca(identity: UUID, service: ExternalService = Depends(get_external_service),
              _: object = Depends(require_permission("connections.write"))):
    service.delete_ca(identity)
    return Response(status_code=204)


@router.post("/external-connections/{identity}/test", response_model=ExternalTestResponse,
             responses=INDEXER_ERRORS)
def test_connection(identity: UUID, service: ExternalService = Depends(get_external_service),
                    _: object = Depends(require_permission("connections.write"))):
    return service.test(identity)


@router.get("/external-connections/{identity}/indices", response_model=IndexPage,
            responses=INDEXER_ERRORS)
def indices(identity: UUID, limit: int = Query(50, ge=1, le=100),
            offset: int = Query(0, ge=0), service: ExternalService = Depends(get_external_service),
            _: object = Depends(require_permission("connections.write"))):
    items, total = service.indices(identity, limit, offset)
    return IndexPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/external-connections/{identity}/data-streams", response_model=IndexPage,
            responses=INDEXER_ERRORS)
def data_streams(identity: UUID, limit: int = Query(50, ge=1, le=100),
                 offset: int = Query(0, ge=0), service: ExternalService = Depends(get_external_service),
                 _: object = Depends(require_permission("connections.write"))):
    items, total = service.data_streams(identity, limit, offset)
    return IndexPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/external-sources", response_model=ExternalSourcePage,
            responses=READ_ERRORS)
def sources(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
            service: ExternalService = Depends(get_external_service),
            _: object = Depends(require_permission("sources.read"))):
    items, total = service.sources(limit, offset)
    return ExternalSourcePage(items=[source_dto(item) for item in items], total=total,
                              limit=limit, offset=offset)


@router.get("/external-sources/{identity}", response_model=ExternalSourceDTO,
            responses=DETAIL_ERRORS)
def source(identity: UUID, service: ExternalService = Depends(get_external_service),
           _: object = Depends(require_permission("sources.read"))):
    return source_dto(service.source(identity))


@router.post("/external-sources", response_model=ExternalSourceDTO, status_code=201,
             responses=WRITE_ERRORS)
def create_source(data: ExternalSourceCreate, service: ExternalService = Depends(get_external_service),
                  _: object = Depends(require_permission("sources.write"))):
    return source_dto(service.create_source(data))


@router.patch("/external-sources/{identity}", response_model=ExternalSourceDTO,
              responses=WRITE_ERRORS)
def update_source(identity: UUID, data: ExternalSourcePatch,
                  service: ExternalService = Depends(get_external_service),
                  _: object = Depends(require_permission("sources.write"))):
    return source_dto(service.update_source(identity, data))


@router.delete("/external-sources/{identity}", status_code=204,
               responses=DETAIL_ERRORS)
def delete_source(identity: UUID, service: ExternalService = Depends(get_external_service),
                  _: object = Depends(require_permission("sources.write"))):
    service.delete_source(identity)
    return Response(status_code=204)


@router.post("/external-sources/{identity}/enable", response_model=ExternalSourceDTO,
             responses=ENABLE_ERRORS)
def enable_source(identity: UUID, service: ExternalService = Depends(get_external_service),
                  _: object = Depends(require_permission("sources.write"))):
    return source_dto(service.enable(identity))


@router.post("/external-sources/{identity}/disable", response_model=ExternalSourceDTO,
             responses=DETAIL_ERRORS)
def disable_source(identity: UUID, service: ExternalService = Depends(get_external_service),
                   _: object = Depends(require_permission("sources.write"))):
    return source_dto(service.disable(identity))
