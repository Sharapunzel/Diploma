"""Versioned configuration for the single administratively managed OIDC provider."""

from __future__ import annotations

import hmac
import re
import ssl
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Literal
from urllib.parse import urlparse

import httpx
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from pydantic import BaseModel, Field, SecretStr, field_validator
from sqlalchemy import JSON, bindparam, text
from sqlalchemy.orm import Session

from .config import Settings
from .core.errors import DomainError
from .indexer import validate_ca
from .oidc import AuthlibOidcClient


class ProviderInput(BaseModel):
    issuer_url: str = Field(min_length=1, max_length=2048)
    client_id: str = Field(min_length=1, max_length=255)
    redirect_uri: str = Field(min_length=1, max_length=2048)
    scopes: list[str] = Field(default_factory=lambda: ["openid", "profile", "email"])
    client_secret: SecretStr | None = Field(default=None, max_length=4096, exclude=True)

    @field_validator("scopes")
    @classmethod
    def validate_scopes(cls, scopes: list[str]) -> list[str]:
        if "openid" not in scopes or not scopes or len(scopes) > 20:
            raise ValueError("openid scope is required")
        if any(
            not value.isascii()
            or not value
            or len(value) > 255
            or any(char.isspace() for char in value)
            for value in scopes
        ):
            raise ValueError("invalid scope")
        return scopes


class ProviderPublic(BaseModel):
    issuer_url: str
    client_id: str
    redirect_uri: str
    scopes: list[str]
    has_secret: bool
    has_ca: bool


class ProviderStatus(BaseModel):
    mode: Literal["env", "managed", "disabled"]
    env_enabled: bool
    version: int
    active_version: int | None
    active: ProviderPublic | None
    draft: ProviderPublic | None
    diagnostic: str | None
    checked: bool
    trial_succeeded: bool


def _error(code: str, status: int = 400) -> DomainError:
    return DomainError(code, code.replace("_", " "), status)


def _cipher(settings: Settings) -> Fernet:
    try:
        if settings.external_secret_key is None:
            raise ValueError
        return Fernet(settings.external_secret_key.get_secret_value().encode("ascii"))
    except (ValueError, UnicodeError):
        raise _error("oidc_key_unavailable", 503) from None


def _secret(config: dict, settings: Settings) -> str:
    try:
        return _cipher(settings).decrypt(config["secret_ciphertext"].encode("ascii")).decode()
    except (InvalidToken, KeyError, UnicodeError, ValueError):
        raise _error("oidc_key_unavailable", 503) from None


def _safe_url(value: str, settings: Settings, callback: bool = False) -> None:
    field = "redirect_uri" if callback else "issuer_url"
    try:
        parsed = urlparse(value)
        parsed_port = parsed.port
    except ValueError:
        raise DomainError("oidc_url_invalid", "Invalid URL", 422, {"field": field}) from None
    if (
        parsed.username
        or parsed.password
        or parsed.fragment
        or not parsed.hostname
        or parsed.scheme not in {"http", "https"}
    ):
        raise DomainError("oidc_url_invalid", "Invalid URL", 422, {"field": field})
    if parsed.scheme == "http" and not (
        settings.environment != "production"
        and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        raise DomainError("oidc_https_required", "HTTPS is required", 422, {"field": field})
    if callback and (parsed.path != "/api/v1/auth/oidc/callback" or parsed.query):
        raise DomainError("oidc_callback_invalid", "Invalid callback path", 422, {"field": field})
    if callback:
        try:
            origin = urlparse(settings.public_origin)
            origin_port = origin.port
        except ValueError:
            raise _error("oidc_public_origin_invalid", 503) from None
        if (
            origin.scheme not in {"http", "https"}
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
            or (settings.environment == "production" and origin.scheme != "https")
            or (
                origin.scheme == "http"
                and origin.hostname not in {"localhost", "127.0.0.1", "::1"}
            )
        ):
            raise _error("oidc_public_origin_invalid", 503)
        if (
            parsed.scheme.lower(), parsed.hostname.lower(), parsed_port or _default_port(parsed.scheme)
        ) != (
            origin.scheme.lower(), origin.hostname.lower(), origin_port or _default_port(origin.scheme)
        ):
            raise DomainError(
                "oidc_callback_origin_invalid",
                "Callback must use the public application origin",
                422,
                {"field": field},
            )


def _default_port(scheme: str) -> int:
    return 443 if scheme.lower() == "https" else 80


def _public(config: dict | None) -> dict | None:
    if config is None:
        return None
    return {
        "issuer_url": config["issuer_url"],
        "client_id": config["client_id"],
        "redirect_uri": config["redirect_uri"],
        "scopes": config["scopes"],
        "has_secret": bool(config.get("secret_ciphertext")),
        "has_ca": bool(config.get("ca_pem")),
    }


def _row(session: Session, lock: bool = False) -> dict:
    suffix = " FOR UPDATE" if lock else ""
    result = (
        session.execute(text("SELECT * FROM app.oidc_provider WHERE id = 1" + suffix))
        .mappings()
        .one()
    )
    return dict(result)


def status(session: Session, settings: Settings, principal_id=None) -> dict:
    row = _row(session)
    active = row["active"] if row["mode"] == "managed" else None
    diagnostic = (
        "oidc_configuration_unavailable" if row["mode"] == "managed" and active is None else None
    )
    if active is not None:
        try:
            _safe_url(active["redirect_uri"], settings, callback=True)
            _secret(active, settings)
            _context(active)
        except DomainError as exc:
            diagnostic = exc.code
    trial_ok = bool(
        row["trial_version"] == row["version"]
        and row["trial_session_id"] == principal_id
        and row["trial_expires_at"]
        and row["trial_expires_at"] > datetime.now(UTC)
    )
    draft_invalid = False
    if row["draft"] is not None:
        try:
            _safe_url(row["draft"]["redirect_uri"], settings, callback=True)
        except DomainError as exc:
            diagnostic = diagnostic or exc.code
            trial_ok = False
            draft_invalid = True
    env_active = None
    if row["mode"] == "env" and settings.oidc_enabled:
        env_active = {
            "issuer_url": settings.oidc_issuer_url,
            "client_id": settings.oidc_client_id,
            "redirect_uri": settings.oidc_redirect_uri,
            "scopes": settings.oidc_scopes,
            "has_secret": settings.oidc_client_secret is not None,
            "has_ca": False,
        }
    return {
        "mode": row["mode"],
        "env_enabled": settings.oidc_enabled,
        "version": row["version"],
        "active_version": row["active_version"],
        "active": _public(active) if active is not None else env_active,
        "diagnostic": diagnostic,
        "draft": _public(row["draft"]),
        "checked": row["checked_version"] == row["version"] and not draft_invalid,
        "trial_succeeded": trial_ok,
    }


def save(session: Session, settings: Settings, data: ProviderInput, version: int) -> dict:
    _safe_url(data.issuer_url, settings)
    _safe_url(data.redirect_uri, settings, callback=True)
    row = _row(session, lock=True)
    if row["version"] != version:
        raise _error("oidc_version_conflict", 409)
    old = row["draft"] or row["active"] or {}
    encrypted = old.get("secret_ciphertext")
    if data.client_secret and data.client_secret.get_secret_value():
        encrypted = (
            _cipher(settings).encrypt(data.client_secret.get_secret_value().encode()).decode()
        )
    draft = data.model_dump(exclude={"client_secret"})
    draft["secret_ciphertext"] = encrypted
    draft["ca_pem"] = old.get("ca_pem")
    session.execute(
        text("""
        UPDATE app.oidc_provider SET version = version + 1, draft = :draft,
            checked_version = NULL, trial_version = NULL, trial_session_id = NULL,
            trial_expires_at = NULL WHERE id = 1
    """).bindparams(bindparam("draft", type_=JSON)),
        {"draft": draft},
    )
    session.commit()
    return status(session, settings)


def remove_secret(session: Session, settings: Settings, version: int) -> dict:
    row = _row(session, lock=True)
    if row["version"] != version or not row["draft"]:
        raise _error("oidc_version_conflict", 409)
    draft = dict(row["draft"])
    draft["secret_ciphertext"] = None
    session.execute(
        text("""
        UPDATE app.oidc_provider SET version = version + 1, draft = :draft,
            checked_version = NULL, trial_version = NULL, trial_session_id = NULL,
            trial_expires_at = NULL WHERE id = 1
    """).bindparams(bindparam("draft", type_=JSON)),
        {"draft": draft},
    )
    session.commit()
    return status(session, settings)


def set_ca(session: Session, settings: Settings, version: int, pem: bytes | None) -> dict:
    ca = validate_provider_ca(pem) if pem is not None else None
    row = _row(session, lock=True)
    if row["version"] != version or not row["draft"]:
        raise _error("oidc_version_conflict", 409)
    draft = dict(row["draft"])
    draft["ca_pem"] = ca
    session.execute(
        text("""
        UPDATE app.oidc_provider SET version = version + 1, draft = :draft,
            checked_version = NULL, trial_version = NULL, trial_session_id = NULL,
            trial_expires_at = NULL WHERE id = 1
    """).bindparams(bindparam("draft", type_=JSON)),
        {"draft": draft},
    )
    session.commit()
    return status(session, settings)


def validate_provider_ca(content: bytes) -> str:
    pem = validate_ca(content)
    blocks = re.findall(
        rb"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+?-----END CERTIFICATE-----",
        pem.encode("ascii"),
    )
    certificates = [x509.load_pem_x509_certificate(block) for block in blocks]
    now = datetime.now(UTC)
    if len({certificate.fingerprint(hashes.SHA256()) for certificate in certificates}) != len(
        certificates
    ):
        raise ValueError("duplicate CA certificate")
    for certificate in certificates:
        if not certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc:
            raise ValueError("expired CA certificate")
        try:
            usage = certificate.extensions.get_extension_for_class(x509.KeyUsage).value
            if not usage.key_cert_sign:
                raise ValueError("CA cannot sign certificates")
        except x509.ExtensionNotFound:
            pass
        issuers = [item for item in certificates if item.subject == certificate.issuer]
        if not issuers:
            raise ValueError("incomplete CA chain")
        if not any(_issued_by(certificate, issuer) for issuer in issuers):
            raise ValueError("invalid CA chain signature")
    return pem


def _issued_by(certificate: x509.Certificate, issuer: x509.Certificate) -> bool:
    try:
        certificate.verify_directly_issued_by(issuer)
        return True
    except (ValueError, InvalidSignature):
        return False


def _context(config: dict):
    context = ssl.create_default_context()
    if config.get("ca_pem"):
        try:
            context.load_verify_locations(cadata=config["ca_pem"])
        except ssl.SSLError:
            raise _error("oidc_ca_invalid", 422) from None
    return context


def client(config: dict, settings: Settings, redirect_uri: str | None = None) -> AuthlibOidcClient:
    _safe_url(config["redirect_uri"], settings, callback=True)
    configured = settings.model_copy(
        update={
            "oidc_enabled": True,
            "oidc_issuer_url": config["issuer_url"],
            "oidc_client_id": config["client_id"],
            "oidc_client_secret": SecretStr(_secret(config, settings)),
            "oidc_redirect_uri": redirect_uri or config["redirect_uri"],
            "oidc_scopes": config["scopes"],
        }
    )
    return AuthlibOidcClient(configured, metadata=config.get("metadata"), verify=_context(config))


async def check(session: Session, settings: Settings, version: int) -> dict:
    row = _row(session)
    if row["version"] != version or not row["draft"]:
        raise _error("oidc_version_conflict", 409)
    config = row["draft"]
    _safe_url(config["redirect_uri"], settings, callback=True)
    if not config.get("secret_ciphertext"):
        raise _error("oidc_secret_required")
    _secret(config, settings)
    issuer = config["issuer_url"].rstrip("/")
    _safe_url(issuer, settings)
    discovery = issuer + "/.well-known/openid-configuration"
    try:
        async with httpx.AsyncClient(
            verify=_context(config), trust_env=False, timeout=8, follow_redirects=False
        ) as http:
            response = await http.get(discovery)
            response.raise_for_status()
            if len(response.content) > 65536:
                raise _error("oidc_metadata_invalid")
            metadata = response.json()
            if not isinstance(metadata, dict):
                raise _error("oidc_metadata_invalid")
            if metadata.get("issuer") != issuer:
                raise _error("oidc_metadata_invalid")
            origin = urlparse(issuer).netloc
            for key in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
                endpoint = metadata.get(key)
                if not isinstance(endpoint, str):
                    raise _error("oidc_metadata_invalid")
                _safe_url(endpoint, settings)
                if urlparse(endpoint).netloc != origin:
                    raise _error("oidc_metadata_origin_invalid")
            if "code" not in metadata.get("response_types_supported", []):
                raise _error("oidc_metadata_invalid")
            offered_scopes = metadata.get("scopes_supported")
            if offered_scopes is not None and (
                not isinstance(offered_scopes, list)
                or any(scope not in offered_scopes for scope in config["scopes"])
            ):
                raise _error("oidc_metadata_invalid")
            algorithms = metadata.get("id_token_signing_alg_values_supported", [])
            if not isinstance(algorithms, list) or not algorithms or "none" in algorithms:
                raise _error("oidc_metadata_invalid")
            auth_methods = metadata.get(
                "token_endpoint_auth_methods_supported", ["client_secret_basic"]
            )
            if "client_secret_basic" not in auth_methods:
                raise _error("oidc_metadata_invalid")
            jwks = await http.get(metadata["jwks_uri"])
            jwks.raise_for_status()
            keys = jwks.json()
            if len(jwks.content) > 65536 or not isinstance(keys, dict) or not keys.get("keys"):
                raise _error("oidc_jwks_invalid")
    except httpx.ConnectError as exc:
        message = str(exc).lower()
        if "certificate verify failed" in message:
            if "hostname" in message or "ip address mismatch" in message:
                raise _error("oidc_hostname_mismatch", 422) from None
            raise _error("oidc_tls_invalid", 422) from None
        cause = exc.__cause__
        while cause is not None:
            if isinstance(cause, ssl.SSLCertVerificationError):
                detail = str(getattr(cause, "verify_message", "")).lower()
                if "hostname" in detail or "ip address mismatch" in detail:
                    raise _error("oidc_hostname_mismatch", 422) from None
                raise _error("oidc_tls_invalid", 422) from None
            cause = cause.__cause__
        raise _error("oidc_unreachable", 422) from None
    except httpx.TimeoutException:
        raise _error("oidc_unreachable", 422) from None
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        raise _error("oidc_metadata_invalid", 422) from None
    row = _row(session, lock=True)
    if row["version"] != version:
        raise _error("oidc_version_conflict", 409)
    updated = dict(row["draft"])
    updated["metadata"] = {
        "issuer": issuer,
        "authorization_endpoint": metadata["authorization_endpoint"],
        "token_endpoint": metadata["token_endpoint"],
        "jwks_uri": metadata["jwks_uri"],
        "response_types_supported": metadata["response_types_supported"],
        "id_token_signing_alg_values_supported": algorithms,
        "token_endpoint_auth_methods_supported": auth_methods,
    }
    session.execute(
        text("""
        UPDATE app.oidc_provider SET checked_version = :version, draft = :draft WHERE id = 1
    """).bindparams(bindparam("draft", type_=JSON)),
        {"version": version, "draft": updated},
    )
    session.commit()
    return status(session, settings)


def mark_trial(session: Session, settings: Settings, version: int, session_id) -> dict:
    row = _row(session, lock=True)
    if row["version"] != version or row["checked_version"] != version:
        raise _error("oidc_version_conflict", 409)
    _safe_url(row["draft"]["redirect_uri"], settings, callback=True)
    session.execute(
        text("""
        UPDATE app.oidc_provider SET trial_version = :version, trial_session_id = :session_id,
            trial_expires_at = :expires WHERE id = 1
    """),
        {
            "version": version,
            "session_id": session_id,
            "expires": datetime.now(UTC) + timedelta(minutes=10),
        },
    )
    session.commit()
    return status(session, settings, session_id)


def activate(session: Session, settings: Settings, version: int, session_id) -> dict:
    row = _row(session, lock=True)
    if row["version"] != version or row["checked_version"] != version or not row["draft"]:
        raise _error("oidc_version_conflict", 409)
    _safe_url(row["draft"]["redirect_uri"], settings, callback=True)
    if (
        row["trial_version"] != version
        or row["trial_session_id"] != session_id
        or not row["trial_expires_at"]
        or row["trial_expires_at"] <= datetime.now(UTC)
    ):
        raise _error("oidc_trial_required", 409)
    _secret(row["draft"], settings)
    session.execute(
        text("""
        UPDATE app.oidc_provider SET mode = 'managed', active = draft,
            active_version = version, trial_version = NULL, trial_session_id = NULL,
            trial_expires_at = NULL WHERE id = 1
    """)
    )
    session.commit()
    return status(session, settings, session_id)


def disable(session: Session, settings: Settings, version: int) -> dict:
    row = _row(session, lock=True)
    if row["version"] != version:
        raise _error("oidc_version_conflict", 409)
    session.execute(
        text("""
        UPDATE app.oidc_provider SET mode = 'disabled', version = version + 1,
            active_version = NULL, trial_version = NULL, trial_session_id = NULL,
            trial_expires_at = NULL WHERE id = 1
    """)
    )
    session.commit()
    return status(session, settings)


def active(
    session: Session, settings: Settings, build_client: bool = True
) -> tuple[bool, AuthlibOidcClient | None, str]:
    row = _row(session)
    if row["mode"] == "disabled":
        return False, None, "disabled"
    if row["mode"] == "env":
        fingerprint = hmac.new(
            settings.oidc_state_secret.get_secret_value().encode(),
            "\0".join(
                (
                    settings.oidc_issuer_url or "",
                    settings.oidc_client_id or "",
                    settings.oidc_redirect_uri or "",
                    settings.oidc_client_secret.get_secret_value()
                    if settings.oidc_client_secret
                    else "",
                    *settings.oidc_scopes,
                )
            ).encode(),
            sha256,
        ).hexdigest()
        return settings.oidc_enabled, None, "env:" + fingerprint
    if not isinstance(row["active"], dict):
        return False, None, "unavailable"
    try:
        _safe_url(row["active"]["redirect_uri"], settings, callback=True)
        if not build_client:
            _secret(row["active"], settings)
            _context(row["active"])
        return (
            True,
            client(row["active"], settings) if build_client else None,
            str(row["active_version"]),
        )
    except DomainError:
        return False, None, "unavailable"
