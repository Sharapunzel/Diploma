import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: Literal["development", "test", "production"] = "development"
    process_role: Literal["api", "provisioner"] = "api"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://localhost/diploma_db"
    cors_origins: list[str] = []
    local_auth_enabled: bool = True
    oidc_enabled: bool = False
    oidc_issuer_url: str | None = None
    oidc_dev_http_issuer: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: SecretStr | None = None
    oidc_redirect_uri: str | None = None
    public_origin: str = "http://localhost:8080"
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
    forwarded_allow_ips: list[str] = ["127.0.0.1"]
    kafka_metadata_timeout_seconds: int = 5
    kafka_security_protocol: Literal["SASL_PLAINTEXT", "SASL_SSL"] | None = None
    kafka_runtime_username: str | None = None
    kafka_runtime_password_file: Path | None = None
    kafka_admin_username: str | None = None
    kafka_admin_password_file: Path | None = None
    agent_credentials_key_file: Path | None = None
    kafka_ca_file: Path | None = None
    kafka_agent_public_host: str | None = None
    kafka_agent_public_port: int | None = None
    kafka_dev_allowed_bootstrap_servers: list[str] = [
        "127.0.0.1:9092",
        "localhost:9092",
        "[::1]:9092",
        "kafka:9092",
    ]
    kafka_consumers_enabled: bool = True
    kafka_consumer_poll_timeout_seconds: float = 1.0
    kafka_consumer_retry_delay_seconds: float = 2.0
    kafka_supervisor_sync_seconds: float = 1.0
    external_secret_key: SecretStr | None = None
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="", extra="ignore", enable_decoding=False
    )

    @field_validator(
        "cors_origins", "oidc_scopes", "trusted_hosts", "forwarded_allow_ips",
        "kafka_dev_allowed_bootstrap_servers", mode="before"
    )
    @classmethod
    def split_csv(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @model_validator(mode="before")
    @classmethod
    def read_file_secrets(cls, values):
        if not isinstance(values, dict):
            return values
        for field_name in (
            "database_url",
            "oidc_state_secret",
            "oidc_client_secret",
            "external_secret_key",
        ):
            file_value = os.getenv(f"{field_name.upper()}_FILE")
            if not file_value:
                continue
            try:
                values[field_name] = Path(file_value).read_text(encoding="utf-8").strip()
            except OSError as error:
                raise ValueError(f"cannot read {field_name} secret file") from error
        return values

    @model_validator(mode="after")
    def validate_security(self):
        if self.oidc_dev_http_issuer:
            try:
                dev_issuer = urlparse(self.oidc_dev_http_issuer)
                dev_port = dev_issuer.port
            except ValueError:
                raise ValueError("OIDC_DEV_HTTP_ISSUER is invalid") from None
            if (
                self.environment != "development"
                or dev_issuer.scheme != "http"
                or dev_issuer.hostname != "localhost"
                or dev_issuer.netloc != f"localhost{f':{dev_port}' if dev_port else ''}"
                or not dev_issuer.path.startswith("/")
                or dev_issuer.path == "/"
                or dev_issuer.query
                or dev_issuer.fragment
            ):
                raise ValueError("OIDC_DEV_HTTP_ISSUER must be a development localhost realm URL")
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
        if not 1 <= self.kafka_metadata_timeout_seconds <= 30:
            raise ValueError("kafka metadata timeout must be between 1 and 30 seconds")
        if self.kafka_security_protocol is not None:
            if not self.kafka_runtime_username or not self.kafka_runtime_password_file:
                raise ValueError("Kafka runtime credentials are required")
            if self.kafka_security_protocol == "SASL_SSL" and not self.kafka_ca_file:
                raise ValueError("SASL_SSL requires a Kafka CA file")
        if self.kafka_agent_public_port is not None and not 1 <= self.kafka_agent_public_port <= 65535:
            raise ValueError("Kafka agent public port is invalid")
        if self.kafka_agent_public_host and (not self.kafka_agent_public_port or (
            self.kafka_agent_public_host != "[::1]" and any(
                char in self.kafka_agent_public_host for char in "/:@ \t\r\n"
            )
        )):
            raise ValueError("Kafka agent public endpoint is invalid")
        if (self.kafka_agent_public_host and self.kafka_security_protocol == "SASL_PLAINTEXT"
                and self.kafka_agent_public_host not in {"localhost", "127.0.0.1", "[::1]"}):
            raise ValueError("Plaintext agent endpoint must use loopback")
        if self.process_role == "provisioner" and self.environment != "test" and self.kafka_admin_password_file is not None:
            if not self.kafka_admin_username:
                raise ValueError("Kafka provisioning admin username is required")
            if self.agent_credentials_key_file is None:
                raise ValueError("agent credentials encryption key file is required")
        if self.process_role == "provisioner" and self.environment == "production" and self.kafka_admin_password_file is None:
            raise ValueError("production agent provisioning requires Kafka admin credentials")
        if not 0.1 <= self.kafka_consumer_poll_timeout_seconds <= 30:
            raise ValueError("kafka consumer poll timeout must be between 0.1 and 30 seconds")
        if not 0.1 <= self.kafka_consumer_retry_delay_seconds <= 60:
            raise ValueError("kafka consumer retry delay must be between 0.1 and 60 seconds")
        if not 0.1 <= self.kafka_supervisor_sync_seconds <= 60:
            raise ValueError("kafka supervisor sync interval must be between 0.1 and 60 seconds")
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
            if self.environment == "production" and parsed.netloc and parsed.scheme != "https":
                raise ValueError(f"{field_name} absolute production URL must use HTTPS")
        if self.session_cookie_samesite == "none" and not self.session_cookie_secure:
            raise ValueError("SameSite=None requires Secure cookies")
        if self.environment == "production" and (
            not self.session_cookie_secure
            or not self.trusted_hosts
            or "*" in self.trusted_hosts
            or not self.forwarded_allow_ips
            or self.external_secret_key is None
            or len(self.oidc_state_secret.get_secret_value().encode()) < 32
            or self.oidc_state_secret.get_secret_value()
            == "development-only-change-me-please-32-bytes"
        ):
            raise ValueError(
                "production requires secure cookies, trusted proxy/host settings and a unique 32-byte OIDC secret"
            )
        if self.environment == "production" and self.oidc_enabled and self.oidc_client_secret is None:
            raise ValueError("production OIDC requires its client secret")
        if self.environment == "production" and self.oidc_enabled and (
            not self.oidc_redirect_uri or not self.oidc_redirect_uri.startswith("https://")
        ):
            raise ValueError("production OIDC_REDIRECT_URI must use public HTTPS")
        if self.environment == "production" and self.kafka_security_protocol != "SASL_SSL":
            raise ValueError("production Kafka requires SASL_SSL")
        return self


settings = Settings()
