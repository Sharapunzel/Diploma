from fastapi import APIRouter, Depends
from sqlalchemy.exc import SQLAlchemyError

from ....core.errors import DomainError
from ....dependencies import get_readiness_repository
from ....repositories.protocols import ReadinessRepository
from ....schemas.auth import ErrorResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", responses={500: {"model": ErrorResponse}})
def live():
    return {"status": "ok"}


@router.get(
    "/ready",
    responses={503: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def ready(repository: ReadinessRepository = Depends(get_readiness_repository)):
    try:
        repository.check()
    except SQLAlchemyError as exc:
        raise DomainError("database_unavailable", "Database is unavailable", 503) from exc
    return {"status": "ok"}
