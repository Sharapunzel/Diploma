from fastapi import Depends, Request
from sqlalchemy.orm import Session

from .config import Settings, settings
from .db import get_session
from .ecs import EcsCatalog
from .indexer import IndexerMetadataAdapter
from .kafka import KafkaMetadataClient
from .normalization.engine import NormalizationEngine
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
from .repositories.sqlalchemy.diagnostics import SqlAlchemyDiagnosticsRepository
from .repositories.sqlalchemy.external import SqlAlchemyExternalRepository
from .repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
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
from .services.implementations.diagnostics import DiagnosticsServiceImpl
from .services.implementations.events import EventQueryServiceImpl, PostgreSqlEventQueryProvider
from .services.implementations.external import ExternalServiceImpl
from .services.implementations.normalization import NormalizerPreviewServiceImpl
from .services.implementations.parsed_logs import EcsCatalogServiceImpl, ParsedLogServiceImpl
from .services.implementations.processing import DurableProcessingServiceImpl
from .services.protocols import AuthenticationService
from .services.protocols.administration import (
    MappingAdministrationService,
    NormalizerAdministrationService,
    RoleAdministrationService,
    SettingAdministrationService,
    UserAdministrationService,
)
from .services.protocols.connections import KafkaConnectionService, SourceService
from .services.protocols.consumers import SourceConsumerLifecycle
from .services.protocols.diagnostics import DiagnosticsService
from .services.protocols.events import EventQueryService
from .services.protocols.external import ExternalService
from .services.protocols.normalization import NormalizerPreviewService
from .services.protocols.parsed_logs import CursorCodec, EcsCatalogService, ParsedLogService
from .services.protocols.processing import DurableProcessingService


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


def get_indexer_adapter(request: Request) -> IndexerMetadataAdapter:
    return request.app.state.indexer_adapter


def get_external_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    adapter: IndexerMetadataAdapter = Depends(get_indexer_adapter),
) -> ExternalService:
    key = app_settings.external_secret_key
    return ExternalServiceImpl(SqlAlchemyExternalRepository(session),
                               SqlAlchemyUnitOfWork(session), adapter,
                               key.get_secret_value() if key else None)


def get_consumer_lifecycle(request: Request) -> SourceConsumerLifecycle:
    return request.app.state.kafka_consumer_supervisor


def get_ecs_catalog(request: Request) -> EcsCatalog:
    return request.app.state.ecs_catalog


def get_normalization_engine(request: Request) -> NormalizationEngine:
    return request.app.state.normalization_engine


def get_normalizer_preview_service(
    engine: NormalizationEngine = Depends(get_normalization_engine),
) -> NormalizerPreviewService:
    return NormalizerPreviewServiceImpl(engine)


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


def get_event_query_service(
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    catalog: EcsCatalog = Depends(get_ecs_catalog),
) -> EventQueryService:
    from .repositories.sqlalchemy.events import SqlAlchemyEventQueryRepository

    repository = SqlAlchemyEventQueryRepository(session)
    provider = PostgreSqlEventQueryProvider(
        repository,
        catalog,
        app_settings.oidc_state_secret.get_secret_value(),
    )
    return EventQueryServiceImpl(repository, {provider.source_type: provider})


def get_diagnostics_service(
    request: Request,
    session: Session = Depends(get_session),
) -> DiagnosticsService:
    return DiagnosticsServiceImpl(
        SqlAlchemyDiagnosticsRepository(session), request.app.state.kafka_consumer_supervisor
    )


def build_durable_processing_service(
    session: Session, engine: NormalizationEngine
) -> DurableProcessingService:
    return DurableProcessingServiceImpl(
        SqlAlchemyProcessingRepository(session), SqlAlchemyUnitOfWork(session), engine
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


def build_normalizer_admin(
    session: Session, engine: NormalizationEngine | None = None,
) -> NormalizerAdministration:
    return NormalizerAdministration(
        SqlAlchemyNormalizerRepository(session), SqlAlchemyUnitOfWork(session), engine,
    )


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


def get_normalizer_admin(
    session: Session = Depends(get_session),
    engine: NormalizationEngine = Depends(get_normalization_engine),
) -> NormalizerAdministrationService:
    return build_normalizer_admin(session, engine)


def build_connection_service(
    session: Session,
    app_settings: Settings,
    client: KafkaMetadataClient,
    lifecycle: SourceConsumerLifecycle | None = None,
) -> KafkaConnectionService:
    return KafkaConnectionServiceImpl(
        SqlAlchemyKafkaConnectionRepository(session),
        SqlAlchemyUnitOfWork(session),
        client,
        app_settings.kafka_metadata_timeout_seconds,
        lifecycle,
    )


def build_source_service(
    session: Session,
    app_settings: Settings,
    client: KafkaMetadataClient,
    lifecycle: SourceConsumerLifecycle | None = None,
) -> SourceService:
    return SourceServiceImpl(
        SqlAlchemySourceRepository(session),
        SqlAlchemyKafkaConnectionRepository(session),
        SqlAlchemyNormalizerRepository(session),
        SqlAlchemyUnitOfWork(session),
        client,
        app_settings.kafka_metadata_timeout_seconds,
        lifecycle,
    )


def get_connection_service(
    request: Request,
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    client: KafkaMetadataClient = Depends(get_kafka_client),
) -> KafkaConnectionService:
    return build_connection_service(
        session, app_settings, client, get_consumer_lifecycle(request)
    )


def get_source_service(
    request: Request,
    session: Session = Depends(get_session),
    app_settings: Settings = Depends(get_settings),
    client: KafkaMetadataClient = Depends(get_kafka_client),
) -> SourceService:
    return build_source_service(
        session, app_settings, client, get_consumer_lifecycle(request)
    )
