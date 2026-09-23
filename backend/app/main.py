import logging
import time
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .api.v1.router import router
from .config import Settings, settings
from .core.errors import DomainError
from .db import Database
from .dependencies import build_auth_service, get_settings
from .kafka import ConfluentKafkaMetadataClient
from .oidc import AuthlibOidcClient

REQUEST_LOG = logging.getLogger("app.request")
ERROR_LOG = logging.getLogger("app.error")


def _request_id(request: Request) -> str:
    candidate = request.headers.get("X-Request-ID", "")
    try:
        return str(UUID(candidate))
    except (ValueError, AttributeError):
        return str(uuid4())


def _error(request_id: str, code: str, message: str, status: int, details=None):
    return JSONResponse(
        status_code=status,
        content={
            "code": code,
            "message": message,
            "request_id": request_id,
            "details": details,
        },
        headers={"X-Request-ID": request_id},
    )


def _csrf_state(
    app: FastAPI,
    session_token: str | None,
    header: str | None,
    cookie: str | None,
) -> tuple[bool, bool]:
    with app.state.database.session_factory() as session:
        service = build_auth_service(session, app.state.settings)
        current_exists = service.inspect_session(session_token) is not None
        if not current_exists:
            return False, False
        return True, service.csrf_matches(session_token, header, cookie)


def create_app(
    config: Settings | None = None,
    database: Database | None = None,
    oidc_client=None,
    kafka_client=None,
) -> FastAPI:
    current = config or settings
    owned_database = database is None
    configured_database = database or Database(current.database_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if owned_database:
            configured_database.dispose()

    app = FastAPI(title="Diploma API", version="1.0.0", lifespan=lifespan)
    app.state.settings = current
    app.state.database = configured_database
    app.state.oidc_client = oidc_client or AuthlibOidcClient(current)
    app.state.kafka_client = kafka_client or ConfluentKafkaMetadataClient()
    app.dependency_overrides[get_settings] = lambda: current
    logging.getLogger("app").setLevel(current.log_level.upper())

    app.add_middleware(
        SessionMiddleware,
        secret_key=current.oidc_state_secret.get_secret_value(),
        session_cookie="diploma_oidc_handshake",
        max_age=current.oidc_handshake_ttl_seconds,
        same_site="lax",
        https_only=current.session_cookie_secure,
    )
    if current.trusted_hosts:
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=current.trusted_hosts)
    if current.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=current.cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Content-Type", "X-CSRF-Token", "X-Request-ID"],
        )

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = _request_id(request)
        request.state.request_id = request_id
        started = time.perf_counter()
        unsafe = request.method in {"POST", "PUT", "PATCH", "DELETE"}
        api_request = request.url.path.startswith("/api/v1")
        callback = request.url.path == "/api/v1/auth/oidc/callback"
        session_token = request.cookies.get(current.session_cookie_name)
        csrf_candidate = unsafe and api_request and not callback
        if csrf_candidate and session_token:
            active_session, valid = await run_in_threadpool(
                _csrf_state,
                app,
                session_token,
                request.headers.get("X-CSRF-Token"),
                request.cookies.get(f"{current.session_cookie_name}_csrf"),
            )
            if active_session and not valid:
                response = _error(
                    request_id,
                    "csrf_invalid",
                    "CSRF validation failed",
                    403,
                )
                REQUEST_LOG.info(
                    "request_completed",
                    extra={
                        "request_id": request_id,
                        "method": request.method,
                        "path": request.url.path,
                        "status_code": 403,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                    },
                )
                return response
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        REQUEST_LOG.info(
            "request_completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return response

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        return _error(
            request.state.request_id,
            exc.code,
            exc.message,
            exc.status_code,
            exc.details,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        details = [
            {key: value for key, value in error.items() if key not in {"input", "ctx"}}
            for error in exc.errors()
        ]
        return _error(
            request.state.request_id,
            "validation_error",
            "Request validation failed",
            422,
            {"errors": details},
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        detail = (
            exc.detail
            if isinstance(exc.detail, dict)
            else {"code": "http_error", "message": str(exc.detail)}
        )
        return _error(
            request.state.request_id,
            detail.get("code", "http_error"),
            detail.get("message", "Request failed"),
            exc.status_code,
            detail.get("details"),
        )

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception):
        ERROR_LOG.exception(
            "unhandled_request_error",
            extra={"request_id": request.state.request_id},
        )
        return _error(
            request.state.request_id,
            "internal_error",
            "Internal server error",
            500,
        )

    app.include_router(router)
    return app


app = create_app()
