from typing import Literal
from urllib.parse import urlparse

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://localhost/diploma_db"
    cors_origins: list[str] = []
    local_auth_enabled: bool = True
    oidc_enabled: bool = False
    oidc_issuer_url: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: SecretStr | None = None
    oidc_redirect_uri: str | None = None
    oidc_scopes: list[str] = ["openid", "profile", "email"]
    oidc_success_redirect_url: str = "/"
    oidc_error_redirect_url: str = "/login?error=oidc"
    oidc_state_secret: SecretStr = SecretStr("development-only-change-me-please-32-bytes")
    oidc_handshake_ttl_seconds: int = 600
    session_cookie_name: str = "diploma_session"
    session_cookie_secure: bool = False
    session_cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    session_absolute_ttl_seconds: int = 28800
    session_idle_ttl_seconds: int = 1800
    session_touch_interval_seconds: int = 60
    trusted_hosts: list[str] = []
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    @field_validator("cors_origins", "oidc_scopes", "trusted_hosts", mode="before")
    @classmethod
    def split_csv(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @model_validator(mode="after")
    def validate_security(self):
        if self.cors_origins and "*" in self.cors_origins:
            raise ValueError("cors_origins cannot contain wildcard")
        if self.session_idle_ttl_seconds > self.session_absolute_ttl_seconds:
            raise ValueError("idle session TTL cannot exceed absolute TTL")
        if any(value <= 0 for value in (
            self.oidc_handshake_ttl_seconds,
            self.session_absolute_ttl_seconds,
            self.session_idle_ttl_seconds,
            self.session_touch_interval_seconds,
        )):
            raise ValueError("TTL values must be positive")
        if self.oidc_enabled:
            required = (self.oidc_issuer_url, self.oidc_client_id,
                        self.oidc_client_secret, self.oidc_redirect_uri)
            if not all(required) or "openid" not in self.oidc_scopes:
                raise ValueError("OIDC requires issuer, client, redirect, secret and openid scope")
            for field_name, value in (
                ("oidc_issuer_url", self.oidc_issuer_url),
                ("oidc_redirect_uri", self.oidc_redirect_uri),
            ):
                parsed = urlparse(value or "")
                if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                    raise ValueError(f"{field_name} must be an absolute HTTP(S) URL")
        for field_name, value in (
            ("oidc_success_redirect_url", self.oidc_success_redirect_url),
            ("oidc_error_redirect_url", self.oidc_error_redirect_url),
        ):
            parsed = urlparse(value)
            if not (value.startswith("/") or (
                parsed.scheme in {"http", "https"} and parsed.netloc
            )):
                raise ValueError(f"{field_name} must be relative or an absolute HTTP(S) URL")
        if self.session_cookie_samesite == "none" and not self.session_cookie_secure:
            raise ValueError("SameSite=None requires Secure cookies")
        if self.environment == "production" and (
            not self.session_cookie_secure
            or len(self.oidc_state_secret.get_secret_value().encode()) < 32
            or self.oidc_state_secret.get_secret_value()
            == "development-only-change-me-please-32-bytes"
        ):
            raise ValueError(
                "production requires secure cookies and a unique 32-byte OIDC secret"
            )
        return self


settings = Settings()
