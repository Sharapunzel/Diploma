import re
from uuid import UUID

from cryptography import x509
from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse

from ....config import Settings
from ....core.errors import DomainError
from ....dependencies import get_agent_lifecycle_service, get_settings, require_permission
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

_CERTIFICATE_BLOCK = re.compile(
    rb"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----"
)


def _public_ca_bundle(path):
    try:
        with path.open("rb") as source:
            raw = source.read(65537)
            if source.read(1) or len(raw) > 65536:
                raise ValueError("CA bundle is too large")
        matches = list(_CERTIFICATE_BLOCK.finditer(raw))
        if not matches or _CERTIFICATE_BLOCK.sub(b"", raw).strip():
            raise ValueError("CA bundle contains non-certificate data")
        for match in matches:
            certificate = x509.load_pem_x509_certificate(match.group())
            constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
            if not constraints.value.ca:
                raise ValueError("CA bundle contains an end-entity certificate")
        return raw.decode("ascii")
    except (OSError, UnicodeError, ValueError, x509.ExtensionNotFound) as error:
        raise DomainError("agent_transport_unavailable", "Agent CA is unavailable", 503) from error


@router.get("/transport", responses=ERRORS)
def agent_transport(
    settings: Settings = Depends(get_settings),
    _: object = Depends(require_permission("agents.write")),
):
    """Public Kafka listener and optional trust anchor for a one-time agent config."""
    if not settings.kafka_agent_public_host or not settings.kafka_agent_public_port:
        raise DomainError("agent_transport_unavailable", "Agent transport is not configured", 503)
    protocol = settings.kafka_security_protocol
    if protocol not in {"SASL_SSL", "SASL_PLAINTEXT"}:
        raise DomainError("agent_transport_unavailable", "Agent transport is not configured", 503)
    ca_pem = None
    if protocol == "SASL_SSL":
        if not settings.kafka_ca_file:
            raise DomainError("agent_transport_unavailable", "Agent CA is unavailable", 503)
        ca_pem = _public_ca_bundle(settings.kafka_ca_file)
    return JSONResponse(
        content={
            "bootstrap_server": f"{settings.kafka_agent_public_host}:{settings.kafka_agent_public_port}",
            "security_protocol": protocol,
            "ca_pem": ca_pem,
        },
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


def agent_dto(value):
    agent, topics = value["agent"], value["topics"]
    return AgentDTO.model_validate(
        {
            "id": agent.id,
            "name": agent.name,
            "description": agent.description,
            "state": agent.state,
            "access_revision": agent.access_revision,
            "credentials_claimable": agent.state == "ready" and agent.pending_credentials_ciphertext is not None,
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
