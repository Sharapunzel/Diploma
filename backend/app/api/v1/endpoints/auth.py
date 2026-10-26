import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool
from starlette.responses import RedirectResponse

from ....config import Settings
from ....core.errors import DomainError
from ....db import get_session
from ....dependencies import get_auth_service, get_oidc_client, get_settings, require_permission
from ....oidc_provider import _row, _safe_url, mark_trial
from ....oidc_provider import active as active_provider
from ....oidc_provider import client as managed_client
from ....repositories.protocols import OidcClient
from ....schemas.auth import (
    AuthMethodsResponse,
    ErrorResponse,
    LocalLoginRequest,
    PrincipalDTO,
    SessionResponse,
)
from ....services.protocols import AuthenticationService

router = APIRouter(prefix="/auth", tags=["auth"])
LOGGER = logging.getLogger("app.oidc")
ERRORS = {
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    403: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    422: {"model": ErrorResponse},
    500: {"model": ErrorResponse},
}


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


@router.get("/methods", response_model=AuthMethodsResponse)
def auth_methods(
    settings: Settings = Depends(get_settings), session: Session = Depends(get_session)
):
    enabled, _, _ = active_provider(session, settings, build_client=False)
    return AuthMethodsResponse(
        local_enabled=settings.local_auth_enabled,
        oidc_enabled=enabled,
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
    session: Session = Depends(get_session),
):
    enabled, managed, version = active_provider(session, settings)
    if not enabled:
        error_response(
            DomainError("auth_method_disabled", "Authentication method is disabled", 404)
        )
    client = managed or client
    nonce = secrets.token_urlsafe(32)
    request.session["oidc_nonce"] = nonce
    request.session["oidc_version"] = version
    if managed is not None:
        request.session["oidc_public_origin"] = settings.public_origin
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
    session: Session = Depends(get_session),
):
    enabled, managed, version = active_provider(session, settings)
    is_trial = request.session.get("oidc_trial_version") is not None
    if is_trial:
        row = _row(session)
        trial_version = request.session.get("oidc_trial_version")
        if row["version"] != trial_version or row["checked_version"] != trial_version:
            request.session.clear()
            return RedirectResponse("/administration?oidc_trial=invalid")
        if request.session.get("oidc_public_origin") != settings.public_origin:
            request.session.clear()
            return RedirectResponse("/administration?oidc_trial=invalid")
        try:
            client = managed_client(row["draft"], settings)
        except DomainError:
            request.session.clear()
            return RedirectResponse("/administration?oidc_trial=failed")
    elif (
        not enabled
        or request.session.get("oidc_version") != version
        or (managed is not None and request.session.get("oidc_public_origin") != settings.public_origin)
    ):
        request.session.clear()
        return RedirectResponse(settings.oidc_error_redirect_url)
    else:
        client = managed or client
    nonce = request.session.get("oidc_nonce")
    if not isinstance(nonce, str) or not nonce:
        client.clear_handshake(request)
        return RedirectResponse(
            "/administration?oidc_trial=failed" if is_trial else settings.oidc_error_redirect_url
        )
    try:
        claims = await client.verified_claims(request, nonce)
        if is_trial:
            current = service.inspect_session(request.cookies.get(settings.session_cookie_name))
            if (
                current is None
                or "settings.write" not in current[2].permissions
                or str(current[0].id) != request.session.get("oidc_trial_session")
            ):
                return RedirectResponse("/administration?oidc_trial=failed")
            mark_trial(session, settings, trial_version, current[0].id)
            return RedirectResponse("/administration?oidc_trial=success")
        result = await run_in_threadpool(
            service.oidc_login,
            claims,
            request.cookies.get(settings.session_cookie_name),
            allow_managed=managed is not None,
        )
        response = RedirectResponse(settings.oidc_success_redirect_url)
        set_session_cookie(response, result[0], settings)
        set_csrf_cookie(response, result[1], settings)
        return response
    except Exception:  # noqa: BLE001 - IdP errors can contain sensitive response data.
        LOGGER.error(
            "oidc_callback_failed",
            extra={"request_id": request.state.request_id},
        )
        return RedirectResponse(
            "/administration?oidc_trial=failed" if is_trial else settings.oidc_error_redirect_url
        )
    finally:
        client.clear_handshake(request)


@router.get("/oidc/trial/start", responses=ERRORS)
async def oidc_trial_start(
    request: Request,
    version: int,
    settings: Settings = Depends(get_settings),
    session: Session = Depends(get_session),
    principal=Depends(require_permission("settings.write")),
):
    row = _row(session)
    if row["version"] != version or row["checked_version"] != version:
        error_response(DomainError("oidc_version_conflict", "Draft changed", 409))
    try:
        _safe_url(row["draft"]["redirect_uri"], settings, callback=True)
    except DomainError as exc:
        error_response(exc)
    try:
        trial_client = managed_client(row["draft"], settings)
        request.session["oidc_trial_version"] = version
        request.session["oidc_trial_session"] = str(principal[0].id)
        request.session["oidc_public_origin"] = settings.public_origin
        nonce = secrets.token_urlsafe(32)
        request.session["oidc_nonce"] = nonce
        return await trial_client.authorization_redirect(request, nonce)
    except Exception:
        LOGGER.exception(
            "oidc_trial_start_failed",
            extra={"request_id": request.state.request_id},
        )
        request.session.clear()
        return RedirectResponse("/administration?oidc_trial=failed")
