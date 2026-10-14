from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse

from ....dependencies import get_agent_lifecycle_service, require_permission
from ....schemas.agents import (
    AgentCreate,
    AgentDTO,
    AgentPage,
    AgentPatch,
    AgentTopicDTO,
    AgentTopicRequest,
    OneTimeCredentials,
    ProvisioningOperationDTO,
)
from ....schemas.auth import ErrorResponse
from ....services.protocols.agents import AgentLifecycleService

router = APIRouter(prefix="/agents", tags=["agents"])
ERRORS = {
    "401": {"model": ErrorResponse},
    "403": {"model": ErrorResponse},
    "404": {"model": ErrorResponse},
    "409": {"model": ErrorResponse},
    "422": {"model": ErrorResponse},
    "503": {"model": ErrorResponse},
}


def agent_dto(value):
    agent, topics = value["agent"], value["topics"]
    return AgentDTO.model_validate(
        {
            "id": agent.id,
            "name": agent.name,
            "description": agent.description,
            "state": agent.state,
            "access_revision": agent.access_revision,
            "safe_error": agent.safe_error,
            "operation_id": value.get("operation_id"),
            "topics": [AgentTopicDTO.model_validate(item) for item in topics],
        }
    )


@router.get("", response_model=AgentPage, responses=ERRORS)
def list_agents(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.read")),
):
    items, total = service.list(limit, offset)
    return AgentPage(
        items=[agent_dto(item) for item in items], total=total, limit=limit, offset=offset
    )


@router.get("/{agent_id}", response_model=AgentDTO, responses=ERRORS)
def get_agent(
    agent_id: UUID,
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.read")),
):
    return agent_dto(service.get(agent_id))


@router.post("", response_model=AgentDTO, status_code=202, responses=ERRORS)
def create_agent(
    data: AgentCreate,
    idempotency_key: UUID = Header(..., alias="Idempotency-Key"),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    agent, _ = service.create(data, str(idempotency_key))
    return agent_dto(service.get(agent.id))


@router.patch("/{agent_id}", response_model=AgentDTO, responses=ERRORS)
def update_agent(
    agent_id: UUID,
    data: AgentPatch,
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    return agent_dto(service.update(agent_id, data))


@router.post(
    "/{agent_id}/topics", response_model=ProvisioningOperationDTO, status_code=202, responses=ERRORS
)
def add_agent_topic(
    agent_id: UUID,
    data: AgentTopicRequest,
    idempotency_key: UUID = Header(..., alias="Idempotency-Key"),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    return service.add_topic(agent_id, data, str(idempotency_key))


@router.delete(
    "/{agent_id}/topics/{topic_id}",
    response_model=ProvisioningOperationDTO,
    status_code=202,
    responses=ERRORS,
)
def remove_agent_topic(
    agent_id: UUID,
    topic_id: UUID,
    idempotency_key: UUID = Header(..., alias="Idempotency-Key"),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    return service.remove_topic(agent_id, topic_id, str(idempotency_key))


@router.get(
    "/{agent_id}/operations/{operation_id}",
    response_model=ProvisioningOperationDTO,
    responses=ERRORS,
)
def get_agent_operation(
    agent_id: UUID,
    operation_id: UUID,
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.read")),
):
    operation = service.operation(operation_id)
    if operation.agent_id != agent_id:
        from ....core.errors import DomainError

        raise DomainError("agent_operation_not_found", "Provisioning operation not found", 404)
    return operation


@router.post("/{agent_id}/credentials/claim", response_model=OneTimeCredentials, responses=ERRORS)
def claim_credentials(
    agent_id: UUID,
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    credentials = service.claim_credentials(agent_id)
    return JSONResponse(
        content=credentials.model_dump(mode="json"),
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


@router.post(
    "/{agent_id}/revoke", response_model=ProvisioningOperationDTO, status_code=202, responses=ERRORS
)
def revoke_agent(
    agent_id: UUID,
    idempotency_key: UUID = Header(..., alias="Idempotency-Key"),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    operation = service.revoke(agent_id, str(idempotency_key))
    return operation


@router.post(
    "/{agent_id}/credentials/rotate",
    response_model=ProvisioningOperationDTO,
    status_code=202,
    responses=ERRORS,
)
def rotate_credentials(
    agent_id: UUID,
    idempotency_key: UUID = Header(..., alias="Idempotency-Key"),
    service: AgentLifecycleService = Depends(get_agent_lifecycle_service),
    _: object = Depends(require_permission("agents.write")),
):
    return service.rotate(agent_id, str(idempotency_key))
