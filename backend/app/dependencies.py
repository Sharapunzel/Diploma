from fastapi import Depends, Request
from sqlalchemy.orm import Session

from .config import Settings, settings
from .db import get_session
from .ecs import EcsCatalog
from .kafka import KafkaMetadataClient
from .repositories.protocols import OidcClient, ReadinessRepository
from .repositories.sqlalchemy import (
    SqlAlchemyOidcMappingRepository,
    SqlAlchemyReadinessRepository,
    SqlAlchemyRoleRepository,
    SqlAlchemySessionRepository,
    SqlAlchemyUnitOfWork,
    SqlAlchemyUserRepository,
)
from .repositories.sqlalchemy.administration import (
    SqlAlchemyAdministrationUserRepository,
    SqlAlchemyNormalizerRepository,
    SqlAlchemySettingRepository,
)
from .repositories.sqlalchemy.connections import (
    SqlAlchemyKafkaConnectionRepository,
    SqlAlchemySourceRepository,
)
from .services.implementations.administration import (
    MappingAdministration,
    NormalizerAdministration,
    RoleAdministration,
    SettingAdministration,
    UserAdministration,
)
from .services.implementations.auth import AuthService
from .services.implementations.connections import KafkaConnectionServiceImpl, SourceServiceImpl
from .services.implementations.cursor import SignedParsedLogCursorCodec
from .services.implementations.parsed_logs import EcsCatalogServiceImpl, ParsedLogServiceImpl
from .services.protocols import AuthenticationService
from .services.protocols.administration import (
    MappingAdministrationService,
    NormalizerAdministrationService,
    RoleAdministrationService,
    SettingAdministrationService,
    UserAdministrationService,
)
from .services.protocols.connections import KafkaConnectionService, SourceService
from .services.protocols.parsed_logs import CursorCodec, EcsCatalogService, ParsedLogService


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


def get_kafka_client(request: Request) -> KafkaMetadataClient:
    return request.app.state.kafka_client


def get_ecs_catalog(request: Request) -> EcsCatalog:
    return request.app.state.ecs_catalog


def get_cursor_codec(
    app_settings: Settings = Depends(get_settings),
) -> CursorCodec:
    return SignedParsedLogCursorCodec(app_settings.oidc_state_secret.get_secret_value())


def get_ecs_catalog_service(
    catalog: EcsCatalog = Depends(get_ecs_catalog),
) -> EcsCatalogService:
    return EcsCatalogServiceImpl(catalog)


def get_parsed_log_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    catalog: EcsCatalog = Depends(get_ecs_catalog),
    cursor_codec: CursorCodec = Depends(get_cursor_codec),
) -> ParsedLogService:
    from .repositories.sqlalchemy.parsed_logs import SqlAlchemyParsedLogRepository

    return ParsedLogServiceImpl(
        SqlAlchemyParsedLogRepository(session),
        SqlAlchemyUnitOfWork(session),
        catalog,
        cursor_codec,
    )


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


def build_user_admin(session: Session) -> UserAdministration:
    return UserAdministration(
        SqlAlchemyAdministrationUserRepository(session),
        SqlAlchemyRoleRepository(session),
        SqlAlchemySessionRepository(session),
        SqlAlchemyUnitOfWork(session),
    )


def build_role_admin(session: Session) -> RoleAdministration:
    return RoleAdministration(SqlAlchemyRoleRepository(session), SqlAlchemyUnitOfWork(session))


def build_mapping_admin(session: Session) -> MappingAdministration:
    return MappingAdministration(
        SqlAlchemyOidcMappingRepository(session),
        SqlAlchemyRoleRepository(session),
        SqlAlchemyUnitOfWork(session),
    )


def build_setting_admin(session: Session) -> SettingAdministration:
    return SettingAdministration(SqlAlchemySettingRepository(session), SqlAlchemyUnitOfWork(session))


def build_normalizer_admin(session: Session) -> NormalizerAdministration:
    return NormalizerAdministration(SqlAlchemyNormalizerRepository(session), SqlAlchemyUnitOfWork(session))


def get_user_admin(
    session: Session = Depends(get_session),
) -> UserAdministrationService:
    return build_user_admin(session)


def get_role_admin(session: Session = Depends(get_session)) -> RoleAdministrationService:
    return build_role_admin(session)


def get_mapping_admin(session: Session = Depends(get_session)) -> MappingAdministrationService:
    return build_mapping_admin(session)


def get_setting_admin(session: Session = Depends(get_session)) -> SettingAdministrationService:
    return build_setting_admin(session)


def get_normalizer_admin(session: Session = Depends(get_session)) -> NormalizerAdministrationService:
    return build_normalizer_admin(session)


def build_connection_service(
    session: Session, app_settings: Settings, client: KafkaMetadataClient
) -> KafkaConnectionService:
    return KafkaConnectionServiceImpl(
        SqlAlchemyKafkaConnectionRepository(session),
        SqlAlchemyUnitOfWork(session),
        client,
        app_settings.kafka_metadata_timeout_seconds,
    )


def build_source_service(
    session: Session, app_settings: Settings, client: KafkaMetadataClient
) -> SourceService:
    return SourceServiceImpl(
        SqlAlchemySourceRepository(session),
        SqlAlchemyKafkaConnectionRepository(session),
        SqlAlchemyNormalizerRepository(session),
        SqlAlchemyUnitOfWork(session),
        client,
        app_settings.kafka_metadata_timeout_seconds,
    )


def get_connection_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    client: KafkaMetadataClient = Depends(get_kafka_client),
) -> KafkaConnectionService:
    return build_connection_service(session, app_settings, client)


def get_source_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    client: KafkaMetadataClient = Depends(get_kafka_client),
) -> SourceService:
    return build_source_service(session, app_settings, client)
