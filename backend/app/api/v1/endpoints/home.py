from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ....db import get_session
from ....dependencies import get_principal
from ....models.app import ExternalConnection, FluentBitAgent, KafkaConnection, Normalizer, Source
from ....schemas.auth import ErrorResponse
from ....schemas.home import AgentCounts, ConnectionCounts, HomeOverview, SourceCounts

router = APIRouter(prefix="/home", tags=["Home"])


@router.get(
    "/overview",
    response_model=HomeOverview,
    response_model_exclude_none=True,
    responses={401: {"model": ErrorResponse}},
)
def overview(principal=Depends(get_principal), session: Session = Depends(get_session)):
    permissions = principal[2].permissions
    result = HomeOverview(captured_at=datetime.now(UTC))
    if "connections.read" in permissions:
        result.connections = ConnectionCounts(
            own=session.scalar(select(func.count()).select_from(KafkaConnection)) or 0,
            external=session.scalar(select(func.count()).select_from(ExternalConnection)) or 0,
        )
    if "sources.read" in permissions:
        counts = session.execute(
            select(
                func.count().filter(
                    Source.source_type == "kafka",
                    Source.is_archived.is_(False),
                    Source.is_enabled.is_(True),
                ),
                func.count().filter(
                    Source.source_type == "kafka",
                    Source.is_archived.is_(False),
                    Source.is_enabled.is_(False),
                ),
                func.count().filter(Source.source_type == "kafka", Source.is_archived.is_(True)),
                func.count().filter(Source.source_type == "external", Source.is_enabled.is_(True)),
                func.count().filter(Source.source_type == "external", Source.is_enabled.is_(False)),
            )
        ).one()
        result.sources = SourceCounts(
            own_enabled=counts[0],
            own_disabled=counts[1],
            own_archived=counts[2],
            external_enabled=counts[3],
            external_disabled=counts[4],
        )
    if "agents.read" in permissions:
        rows = session.execute(
            select(FluentBitAgent.state, func.count()).group_by(FluentBitAgent.state)
        ).all()
        states = dict(rows)
        result.agents = AgentCounts(
            **{
                state: states.get(state, 0)
                for state in ("provisioning", "ready", "error", "revocation_pending", "revoked")
            }
        )
    if "normalizers.read" in permissions:
        result.normalizers = session.scalar(select(func.count()).select_from(Normalizer)) or 0
    return result
