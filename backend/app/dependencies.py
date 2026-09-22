from fastapi import Depends, Request
from sqlalchemy.orm import Session

from .config import Settings, settings
from .db import get_session
from .repositories.protocols import OidcClient, ReadinessRepository
from .repositories.sqlalchemy import (
    SqlAlchemyOidcMappingRepository,
    SqlAlchemyReadinessRepository,
    SqlAlchemyRoleRepository,
    SqlAlchemySessionRepository,
    SqlAlchemyUnitOfWork,
    SqlAlchemyUserRepository,
)
from .services.implementations.auth import AuthService
from .services.protocols import AuthenticationService


def get_settings() -> Settings:
    return settings


def build_auth_service(session: Session, app_settings: Settings) -> AuthService:
    return AuthService(
        users=SqlAlchemyUserRepository(session),
        roles=SqlAlchemyRoleRepository(session),
        mappings=SqlAlchemyOidcMappingRepository(session),
        sessions=SqlAlchemySessionRepository(session),
        unit_of_work=SqlAlchemyUnitOfWork(session),
        settings=app_settings,
    )


def get_auth_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
) -> AuthenticationService:
    return build_auth_service(session, app_settings)


def get_readiness_repository(
    session: Session = Depends(get_session),
) -> ReadinessRepository:
    return SqlAlchemyReadinessRepository(session)


def get_oidc_client(request: Request) -> OidcClient:
    return request.app.state.oidc_client


def session_token(request: Request) -> str | None:
    configured: Settings = request.app.state.settings
    return request.cookies.get(configured.session_cookie_name)


def get_principal(
    request: Request,
    service: AuthenticationService = Depends(get_auth_service),
):
    from .core.errors import DomainError

    current = service.inspect_session(session_token(request))
    if current is None:
        raise DomainError("authentication_required", "Authentication is required", 401)
    return current


def require_permission(permission: str):
    def dependency(principal=Depends(get_principal)):
        from .core.errors import DomainError

        if permission not in principal[2].permissions:
            raise DomainError("permission_denied", "Permission denied", 403)
        return principal

    return dependency
