import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from starlette.concurrency import run_in_threadpool
from starlette.responses import RedirectResponse

from ....config import Settings
from ....core.errors import DomainError
from ....dependencies import get_auth_service, get_oidc_client, get_settings
from ....repositories.protocols import OidcClient
from ....schemas.auth import ErrorResponse, LocalLoginRequest, PrincipalDTO, SessionResponse
from ....services.protocols import AuthenticationService

router = APIRouter(prefix="/auth", tags=["auth"])
LOGGER = logging.getLogger("app.oidc")
ERRORS = {400: {"model": ErrorResponse}, 401: {"model": ErrorResponse},
          403: {"model": ErrorResponse}, 404: {"model": ErrorResponse},
          422: {"model": ErrorResponse}, 500: {"model": ErrorResponse}}


def error_response(exc: DomainError):
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    )


def session_payload(user, role, method, csrf):
    return SessionResponse(
        authenticated=True,
        user=PrincipalDTO.model_validate(user),
        role=role.name,
        permissions=list(role.permissions),
        authentication_method=method,
        csrf_token=csrf,
    )


def set_session_cookie(response: Response, token: str, settings: Settings):
    response.set_cookie(
        settings.session_cookie_name,
        token,
        max_age=settings.session_absolute_ttl_seconds,
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite=settings.session_cookie_samesite,
        path="/",
    )


def set_csrf_cookie(response: Response, token: str, settings: Settings):
    response.set_cookie(
        f"{settings.session_cookie_name}_csrf",
        token,
        max_age=settings.session_absolute_ttl_seconds,
        httponly=False,
        secure=settings.session_cookie_secure,
        samesite=settings.session_cookie_samesite,
        path="/",
    )


def clear_auth_cookies(response: Response, settings: Settings):
    response.delete_cookie(
        settings.session_cookie_name,
        path="/",
        secure=settings.session_cookie_secure,
        samesite=settings.session_cookie_samesite,
    )
    response.delete_cookie(
        f"{settings.session_cookie_name}_csrf",
        path="/",
        secure=settings.session_cookie_secure,
        samesite=settings.session_cookie_samesite,
    )


@router.post("/local/login", response_model=SessionResponse, responses=ERRORS)
def local_login(
    body: LocalLoginRequest,
    request: Request,
    response: Response,
    service: AuthenticationService = Depends(get_auth_service),
    settings: Settings = Depends(get_settings),
):
    try:
        result = service.local_login(
            body.username,
            body.password,
            request.cookies.get(settings.session_cookie_name),
        )
    except DomainError as exc:
        error_response(exc)
    token, csrf, user, role = result
    set_session_cookie(response, token, settings)
    set_csrf_cookie(response, csrf, settings)
    return session_payload(user, role, "local", csrf)


@router.get("/session", response_model=SessionResponse, responses=ERRORS)
def session_status(
    request: Request,
    response: Response,
    service: AuthenticationService = Depends(get_auth_service),
    settings: Settings = Depends(get_settings),
):
    token = request.cookies.get(settings.session_cookie_name)
    csrf = request.cookies.get(f"{settings.session_cookie_name}_csrf")
    current = service.inspect_session(token)
    if current is None:
        if token or csrf:
            clear_auth_cookies(response, settings)
        return SessionResponse(authenticated=False)
    auth_session, user, role = current
    verified_csrf = csrf if service.csrf_matches(token, csrf, csrf) else None
    return session_payload(
        user,
        role,
        auth_session.authentication_method,
        verified_csrf,
    )


@router.post("/logout", responses=ERRORS)
def logout(
    request: Request,
    response: Response,
    service: AuthenticationService = Depends(get_auth_service),
    settings: Settings = Depends(get_settings),
):
    service.revoke(request.cookies.get(settings.session_cookie_name))
    clear_auth_cookies(response, settings)
    return {"status": "ok"}


@router.get("/oidc/login", responses=ERRORS)
async def oidc_login(
    request: Request,
    settings: Settings = Depends(get_settings),
    client: OidcClient = Depends(get_oidc_client),
):
    if not settings.oidc_enabled:
        error_response(
            DomainError("auth_method_disabled", "Authentication method is disabled", 404)
        )
    nonce = secrets.token_urlsafe(32)
    request.session["oidc_nonce"] = nonce
    try:
        return await client.authorization_redirect(request, nonce)
    except Exception:
        LOGGER.exception(
            "oidc_authorization_start_failed",
            extra={"request_id": request.state.request_id},
        )
        client.clear_handshake(request)
        return RedirectResponse(settings.oidc_error_redirect_url)


@router.get("/oidc/callback", responses=ERRORS)
async def oidc_callback(
    request: Request,
    service: AuthenticationService = Depends(get_auth_service),
    settings: Settings = Depends(get_settings),
    client: OidcClient = Depends(get_oidc_client),
):
    if not settings.oidc_enabled:
        error_response(
            DomainError("auth_method_disabled", "Authentication method is disabled", 404)
        )
    nonce = request.session.get("oidc_nonce")
    if not isinstance(nonce, str) or not nonce:
        client.clear_handshake(request)
        return RedirectResponse(settings.oidc_error_redirect_url)
    try:
        claims = await client.verified_claims(request, nonce)
        result = await run_in_threadpool(
            service.oidc_login,
            claims,
            request.cookies.get(settings.session_cookie_name),
        )
        response = RedirectResponse(settings.oidc_success_redirect_url)
        set_session_cookie(response, result[0], settings)
        set_csrf_cookie(response, result[1], settings)
        return response
    except Exception:
        LOGGER.exception(
            "oidc_callback_failed",
            extra={"request_id": request.state.request_id},
        )
        return RedirectResponse(settings.oidc_error_redirect_url)
    finally:
        client.clear_handshake(request)
