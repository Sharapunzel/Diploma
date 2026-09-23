import getpass
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import httpx2
import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends
from fastapi.testclient import TestClient
from joserfc import jwt
from joserfc.jwk import RSAKey
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.api.v1.endpoints.auth import ERRORS
from app.cli import create_admin
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password, token_digest
from app.db import Database
from app.dependencies import (
    get_mapping_admin,
    get_normalizer_admin,
    get_readiness_repository,
    get_role_admin,
    get_setting_admin,
    get_user_admin,
    require_permission,
)
from app.main import create_app
from app.models import (
    AppSetting,
    AuthSession,
    KafkaConnection,
    Normalizer,
    OidcRoleMapping,
    ParsedLog,
    Role,
    Source,
    User,
)
from app.oidc import AuthlibOidcClient
from app.repositories.sqlalchemy import (
    SqlAlchemyOidcMappingRepository,
    SqlAlchemyRoleRepository,
    SqlAlchemySessionRepository,
    SqlAlchemyUnitOfWork,
    SqlAlchemyUserRepository,
)
from app.repositories.sqlalchemy.administration import (
    SqlAlchemyNormalizerRepository,
    SqlAlchemySettingRepository,
)
from app.schemas.administration import NormalizerPatch
from app.services.implementations.administration import (
    NormalizerAdministration,
    SettingAdministration,
)
from app.services.implementations.auth import AuthService

BACKEND_DIR = Path(__file__).resolve().parents[1]
ADMIN_ID = UUID("00000000-0000-4000-8000-000000000001")
GUEST_ID = UUID("00000000-0000-4000-8000-000000000002")


def run_alembic(database_url: str, *arguments: str):
    environment = {**os.environ, "DATABASE_URL": database_url}
    return subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=BACKEND_DIR,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def auth_database_url():
    raw_url = os.getenv("TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail("TEST_DATABASE_URL must point to a separate PostgreSQL test database")
    parsed = make_url(raw_url)
    if parsed.get_backend_name() != "postgresql" or parsed.database == "diploma_db":
        pytest.fail("Auth tests require a separate PostgreSQL database")
    return parsed.set(drivername="postgresql+psycopg").render_as_string(
        hide_password=False
    )


@pytest.fixture(scope="session")
def auth_database(auth_database_url):
    run_alembic(auth_database_url, "upgrade", "head")
    database = Database(auth_database_url)
    yield database
    database.dispose()


def clear_auth_state(auth_database):
    with auth_database.engine.begin() as connection:
        connection.execute(text("DELETE FROM logs.parsed_logs"))
        connection.execute(text("DELETE FROM app.auth_sessions"))
        connection.execute(text("DELETE FROM app.oidc_role_mappings"))
        connection.execute(text("DELETE FROM app.sources"))
        connection.execute(text("DELETE FROM app.kafka_connections"))
        connection.execute(text("DELETE FROM app.normalizers"))
        connection.execute(text("DELETE FROM app.app_settings"))
        connection.execute(text("DELETE FROM app.users"))
        connection.execute(
            text("UPDATE app.roles SET name = 'Administrator' WHERE id = :id"),
            {"id": ADMIN_ID},
        )
        connection.execute(
            text("UPDATE app.roles SET name = 'Guest' WHERE id = :id"),
            {"id": GUEST_ID},
        )


@pytest.fixture(autouse=True)
def clean_auth_state(auth_database):
    clear_auth_state(auth_database)
    yield
    clear_auth_state(auth_database)


@pytest.fixture
def auth_settings(auth_database_url):
    return Settings(
        environment="test",
        database_url=auth_database_url,
        session_absolute_ttl_seconds=3600,
        session_idle_ttl_seconds=600,
        session_touch_interval_seconds=60,
    )


@pytest.fixture
def client(auth_settings, auth_database):
    with TestClient(create_app(auth_settings, database=auth_database)) as test_client:
        yield test_client


def add_local_user(database: Database, username="alice", password="correct-password"):
    with database.session_factory() as session:
        user = User(
            username=username,
            password_hash=hash_password(password),
            display_name="Alice",
            role_id=GUEST_ID,
        )
        session.add(user)
        session.commit()
        return user.id


def login(client: TestClient, username="alice", password="correct-password"):
    return client.post(
        "/api/v1/auth/local/login",
        json={"username": username, "password": password},
    )


def service_for(database, settings, now_provider=None):
    session = database.session_factory()
    service = AuthService(
        SqlAlchemyUserRepository(session),
        SqlAlchemyRoleRepository(session),
        SqlAlchemyOidcMappingRepository(session),
        SqlAlchemySessionRepository(session),
        SqlAlchemyUnitOfWork(session),
        settings,
        now_provider=now_provider or (lambda: datetime.now(UTC)),
    )
    return session, service


def test_0002_schema_permission_and_downgrade(auth_database_url):
    run_alembic(auth_database_url, "upgrade", "head")
    psycopg_url = make_url(auth_database_url).set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    try:
        with psycopg.connect(psycopg_url) as connection:
            constraints = {
                row[0]
                for row in connection.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = 'app.auth_sessions'::regclass"
                )
            }
            indexes = {
                row[0]
                for row in connection.execute(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = 'app' AND tablename = 'auth_sessions'"
                )
            }
            permissions = connection.execute(
                "SELECT permissions FROM app.roles WHERE id = %s", (ADMIN_ID,)
            ).fetchone()[0]
        assert {
            "ck_auth_sessions_token_hash_sha256",
            "ck_auth_sessions_csrf_token_hash_sha256",
            "ck_auth_sessions_last_seen_after_created",
            "ck_auth_sessions_last_seen_before_expiry",
            "ck_auth_sessions_expires_after_created",
            "ck_auth_sessions_revoked_after_created",
            "ck_auth_sessions_auth_method_valid",
        } <= constraints
        assert {"ix_auth_sessions_user_id", "ix_auth_sessions_expiry"} <= indexes
        assert "events.delete" in permissions
        run_alembic(auth_database_url, "downgrade", "-1")
        with psycopg.connect(psycopg_url) as connection:
            assert connection.execute("SELECT to_regclass('app.auth_sessions')").fetchone()[0] is None
            permissions = connection.execute(
                "SELECT permissions FROM app.roles WHERE id = %s", (ADMIN_ID,)
            ).fetchone()[0]
            assert "events.delete" not in permissions
    finally:
        run_alembic(auth_database_url, "upgrade", "head")


def test_auth_session_defaults_constraints_and_user_delete(auth_database):
    user_id = add_local_user(auth_database)
    explicit_id = uuid4()
    now = datetime.now(UTC)
    with auth_database.session_factory() as session:
        generated = AuthSession(
            user_id=user_id,
            token_hash=b"a" * 32,
            csrf_token_hash=b"b" * 32,
            authentication_method="local",
            expires_at=now + timedelta(hours=1),
        )
        explicit = AuthSession(
            id=explicit_id,
            user_id=user_id,
            token_hash=b"c" * 32,
            csrf_token_hash=b"d" * 32,
            authentication_method="oidc",
            expires_at=now + timedelta(hours=1),
        )
        session.add_all([generated, explicit])
        session.commit()
        assert generated.id and explicit.id == explicit_id
        session.delete(session.get(User, user_id))
        session.commit()
        session.refresh(generated)
        assert generated.user_id is None


@pytest.mark.parametrize(
    "override",
    [
        {"token_hash": b"short"},
        {"csrf_token_hash": b"short"},
        {"authentication_method": "password"},
        {"last_seen_at": datetime(2029, 12, 31, tzinfo=UTC)},
        {"last_seen_at": datetime(2030, 1, 3, tzinfo=UTC)},
        {"expires_at": datetime(2030, 1, 1, tzinfo=UTC)},
        {"revoked_at": datetime(2029, 12, 31, tzinfo=UTC)},
    ],
)
def test_auth_session_constraints_reject_invalid_values(auth_database, override):
    values = {
        "token_hash": b"a" * 32,
        "csrf_token_hash": b"b" * 32,
        "authentication_method": "local",
        "created_at": datetime(2030, 1, 1, tzinfo=UTC),
        "last_seen_at": datetime(2030, 1, 1, tzinfo=UTC),
        "expires_at": datetime(2030, 1, 2, tzinfo=UTC),
        "revoked_at": None,
        **override,
    }
    with auth_database.session_factory() as session:
        session.add(AuthSession(**values))
        with pytest.raises(IntegrityError):
            session.commit()


def test_local_login_hashes_tokens_and_session_endpoint(client, auth_database):
    add_local_user(auth_database)
    response = login(client)
    assert response.status_code == 200
    body = response.json()
    raw_session = client.cookies["diploma_session"]
    raw_csrf = body["csrf_token"]
    with auth_database.session_factory() as session:
        stored = session.scalar(select(AuthSession))
        assert stored.token_hash == token_digest(raw_session)
        assert stored.csrf_token_hash == token_digest(raw_csrf)
        assert raw_session.encode() not in stored.token_hash
        assert raw_csrf.encode() not in stored.csrf_token_hash
    current = client.get("/api/v1/auth/session")
    assert current.status_code == 200
    assert current.json()["authenticated"] is True
    assert current.json()["role"] == "Guest"
    assert current.json()["csrf_token"] == raw_csrf
    cookies = response.headers.get_list("set-cookie")
    session_cookie = next(value for value in cookies if value.startswith("diploma_session="))
    csrf_cookie = next(
        value for value in cookies if value.startswith("diploma_session_csrf=")
    )
    assert "HttpOnly" in session_cookie and "SameSite=lax" in session_cookie
    assert "HttpOnly" not in csrf_cookie and "SameSite=lax" in csrf_cookie


@pytest.mark.parametrize(
    ("username", "password", "active", "role_id"),
    [
        ("missing", "correct-password", True, GUEST_ID),
        ("alice", "wrong-password", True, GUEST_ID),
        ("alice", "correct-password", False, GUEST_ID),
        ("alice", "correct-password", True, None),
    ],
)
def test_local_login_failures_share_error(
    client, auth_database, username, password, active, role_id
):
    if username == "alice":
        user_id = add_local_user(auth_database)
        with auth_database.session_factory() as session:
            user = session.get(User, user_id)
            user.is_active = active
            user.role_id = role_id
            session.commit()
    response = login(client, username, password)
    assert response.status_code == 401
    assert response.json()["code"] == "invalid_credentials"


def test_local_login_password_limit_and_sanitized_validation(client):
    secret = "x" * 129
    response = login(client, password=secret)
    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"
    assert secret not in response.text


def test_absolute_idle_boundary_touch_and_revoked_sessions(auth_database, auth_settings):
    user_id = add_local_user(auth_database)
    created = datetime(2030, 1, 1, tzinfo=UTC)
    current = [created]
    session, service = service_for(auth_database, auth_settings, lambda: current[0])
    try:
        user = session.get(User, user_id)
        role = session.get(Role, GUEST_ID)
        token, _, _, _ = service._replace_session(user, role, "local", None)
        stored = session.scalar(select(AuthSession))

        current[0] = created + timedelta(seconds=1200)
        stored.last_seen_at = current[0] - timedelta(seconds=599)
        session.commit()
        assert service.inspect_session(token) is not None
        assert stored.last_seen_at == current[0]

        stored.last_seen_at = current[0] - timedelta(seconds=600)
        session.commit()
        assert service.inspect_session(token) is None
        assert stored.last_seen_at == current[0] - timedelta(seconds=600)

        stored.last_seen_at = current[0]
        stored.expires_at = current[0]
        session.commit()
        assert service.inspect_session(token) is None

        stored.expires_at = current[0] + timedelta(hours=1)
        stored.revoked_at = current[0]
        session.commit()
        assert service.inspect_session(token) is None
        assert service.inspect_session("unknown") is None
    finally:
        session.close()


def test_invalid_session_clears_both_cookies(client):
    client.cookies.set("diploma_session", "unknown")
    client.cookies.set("diploma_session_csrf", "unknown-csrf")
    response = client.get("/api/v1/auth/session")
    assert response.status_code == 200
    assert response.json()["authenticated"] is False
    cookies = response.headers.get_list("set-cookie")
    assert any("diploma_session=" in value and "Max-Age=0" in value for value in cookies)
    assert any("diploma_session_csrf=" in value and "Max-Age=0" in value for value in cookies)


def test_csrf_is_bound_to_server_session_and_login_replacement(client, auth_database):
    add_local_user(auth_database)
    first = login(client)
    token_one = client.cookies["diploma_session"]
    csrf_one = first.json()["csrf_token"]

    second_client = TestClient(create_app(client.app.state.settings, database=auth_database))
    second = login(second_client)
    token_two = second_client.cookies["diploma_session"]
    csrf_two = second.json()["csrf_token"]

    client.cookies.set("diploma_session_csrf", csrf_two)
    forged = client.post(
        "/api/v1/auth/logout",
        headers={"X-CSRF-Token": csrf_two},
    )
    assert forged.status_code == 403

    client.cookies.set("diploma_session_csrf", csrf_one)
    replaced_without_csrf = login(client)
    assert replaced_without_csrf.status_code == 403
    replaced = client.post(
        "/api/v1/auth/local/login",
        json={"username": "alice", "password": "correct-password"},
        headers={"X-CSRF-Token": csrf_one},
    )
    assert replaced.status_code == 200
    with auth_database.session_factory() as session:
        old = session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_digest(token_one))
        )
        other = session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_digest(token_two))
        )
        assert old.revoked_at is not None
        assert other.revoked_at is None


def test_session_replacement_rolls_back_as_one_transaction(auth_database, auth_settings):
    user_id = add_local_user(auth_database)
    session, service = service_for(auth_database, auth_settings)
    user = session.get(User, user_id)
    role = session.get(Role, GUEST_ID)
    old_token, _, _, _ = service._replace_session(user, role, "local", None)
    previous_login = user.last_login_at

    original_commit = service.unit_of_work.commit

    def fail_commit():
        raise RuntimeError("forced failure")

    service.unit_of_work.commit = fail_commit
    with pytest.raises(RuntimeError, match="forced failure"):
        service.local_login("alice", "correct-password", old_token)
    service.unit_of_work.commit = original_commit
    session.expire_all()
    old = session.scalar(
        select(AuthSession).where(AuthSession.token_hash == token_digest(old_token))
    )
    assert old.revoked_at is None
    assert session.get(User, user_id).last_login_at == previous_login
    assert session.scalars(select(AuthSession)).all() == [old]
    session.close()


def test_active_user_and_current_role_are_rechecked(client, auth_database):
    user_id = add_local_user(auth_database)
    assert login(client).status_code == 200
    with auth_database.session_factory() as session:
        user = session.get(User, user_id)
        user.is_active = False
        session.commit()
    assert client.get("/api/v1/auth/session").json()["authenticated"] is False

    with auth_database.session_factory() as session:
        user = session.get(User, user_id)
        user.is_active = True
        user.role_id = ADMIN_ID
        session.commit()
    assert login(client).status_code == 200
    assert client.get("/api/v1/auth/session").json()["role"] == "Administrator"


def test_cli_success_conflict_exact_username_and_password_policy(
    auth_database, monkeypatch, capsys
):
    passwords = iter(["long-enough-password", "long-enough-password"])
    monkeypatch.setattr(getpass, "getpass", lambda _: next(passwords))
    assert create_admin("admin_%", "Admin", auth_database) == 0
    assert "long-enough-password" not in capsys.readouterr().out

    passwords = iter(["long-enough-password", "long-enough-password"])
    monkeypatch.setattr(getpass, "getpass", lambda _: next(passwords))
    assert create_admin("ADMIN_%", "Other", auth_database) == 3

    passwords = iter(["short", "short"])
    monkeypatch.setattr(getpass, "getpass", lambda _: next(passwords))
    assert create_admin("other", "Other", auth_database) == 2


def add_mapping(database, role_id, claim_value, issuer="https://idp.test"):
    with database.session_factory() as session:
        session.add(
            OidcRoleMapping(
                issuer=issuer,
                claim_name="groups",
                claim_value=claim_value,
                role_id=role_id,
            )
        )
        session.commit()


def test_oidc_provision_mapping_list_guest_and_no_email_link(auth_database, auth_settings):
    oidc_settings = auth_settings.model_copy(update={"oidc_enabled": True})
    add_mapping(auth_database, None, "ignored")
    add_mapping(auth_database, GUEST_ID, "users")
    add_mapping(auth_database, ADMIN_ID, "security")
    local_id = add_local_user(auth_database, username="same-email")
    with auth_database.session_factory() as session:
        session.get(User, local_id).email = "same@example.test"
        session.commit()
    session, service = service_for(auth_database, oidc_settings)
    try:
        _, _, user, role = service.oidc_login(
            {
                "iss": "https://idp.test",
                "sub": "subject-1",
                "email": "same@example.test",
                "groups": ["users", "security", "ignored"],
            }
        )
        assert role.id == ADMIN_ID
        assert user.id != local_id
        assert user.role_managed_by_oidc is True

        _, _, guest_user, guest_role = service.oidc_login(
            {"iss": "https://idp.test", "sub": "subject-2"}
        )
        assert guest_role.id == GUEST_ID
        assert guest_user.role_id == GUEST_ID
    finally:
        session.close()


def test_oidc_manual_role_is_preserved_and_missing_role_is_rejected(
    auth_database, auth_settings
):
    oidc_settings = auth_settings.model_copy(update={"oidc_enabled": True})
    add_mapping(auth_database, ADMIN_ID, "security")
    with auth_database.session_factory() as session:
        manual = User(
            oidc_issuer="https://idp.test",
            oidc_subject="manual",
            display_name="Manual",
            role_id=GUEST_ID,
            role_managed_by_oidc=False,
        )
        no_role = User(
            oidc_issuer="https://idp.test",
            oidc_subject="no-role",
            display_name="No role",
            role_id=None,
            role_managed_by_oidc=False,
        )
        inactive = User(
            oidc_issuer="https://idp.test",
            oidc_subject="inactive",
            display_name="Inactive",
            role_id=GUEST_ID,
            role_managed_by_oidc=True,
            is_active=False,
        )
        session.add_all([manual, no_role, inactive])
        session.commit()
    session, service = service_for(auth_database, oidc_settings)
    try:
        _, _, _, role = service.oidc_login(
            {
                "iss": "https://idp.test",
                "sub": "manual",
                "groups": "security",
            }
        )
        assert role.id == GUEST_ID
        with pytest.raises(Exception) as caught:
            service.oidc_login({"iss": "https://idp.test", "sub": "no-role"})
        assert getattr(caught.value, "code", None) == "invalid_credentials"
        with pytest.raises(Exception) as caught:
            service.oidc_login({"iss": "https://idp.test", "sub": "inactive"})
        assert getattr(caught.value, "code", None) == "invalid_credentials"
        with pytest.raises(Exception) as caught:
            service.oidc_login({"sub": "missing-issuer"})
        assert getattr(caught.value, "code", None) == "invalid_credentials"
    finally:
        session.close()


class FakeOidcClient:
    def __init__(self, claims=None, failure=False):
        self.claims = claims or {}
        self.failure = failure
        self.cleared = False

    async def authorization_redirect(self, request, nonce):
        from starlette.responses import RedirectResponse

        return RedirectResponse(f"https://idp.test/authorize?nonce={nonce}")

    async def verified_claims(self, request, nonce):
        if self.failure:
            raise ValueError("invalid signature/state/nonce")
        return self.claims

    def clear_handshake(self, request):
        request.session.clear()
        self.cleared = True


def oidc_settings(auth_settings):
    return Settings(
        **{
            **auth_settings.model_dump(),
            "oidc_enabled": True,
            "oidc_issuer_url": "https://idp.test",
            "oidc_client_id": "client",
            "oidc_client_secret": "secret",
            "oidc_redirect_uri": "http://testserver/api/v1/auth/oidc/callback",
            "oidc_success_redirect_url": "/success",
            "oidc_error_redirect_url": "/error",
        },
    )


def test_oidc_router_uses_adapter_and_clears_handshake(auth_database, auth_settings):
    fake = FakeOidcClient({"iss": "https://idp.test", "sub": "subject"})
    with TestClient(
        create_app(oidc_settings(auth_settings), auth_database, fake)
    ) as oidc_client:
        start = oidc_client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        assert start.status_code == 307
        callback = oidc_client.get("/api/v1/auth/oidc/callback", follow_redirects=False)
        assert callback.status_code == 307
        assert callback.headers["location"] == "/success"
        assert fake.cleared is True
        assert oidc_client.get("/api/v1/auth/session").json()["authenticated"] is True


def test_authlib_oidc_flow_uses_local_discovery_jwks_and_signed_id_token(
    auth_database, auth_settings
):
    configured = oidc_settings(auth_settings)
    private_key = RSAKey.import_key(rsa.generate_private_key(65537, 2048))
    public_jwk = private_key.as_dict(private=False, kid="test-key", use="sig", alg="RS256")
    handshake = {"nonce": None}

    def provider(request: httpx2.Request):
        if request.url.path == "/.well-known/openid-configuration":
            return httpx2.Response(
                200,
                request=request,
                json={
                    "issuer": "https://idp.test",
                    "authorization_endpoint": "https://idp.test/authorize",
                    "token_endpoint": "https://idp.test/token",
                    "jwks_uri": "https://idp.test/jwks",
                    "id_token_signing_alg_values_supported": ["RS256"],
                },
            )
        if request.url.path == "/token":
            now = int(time.time())
            id_token = jwt.encode(
                {"alg": "RS256", "kid": "test-key"},
                {
                    "iss": "https://idp.test",
                    "sub": "signed-subject",
                    "aud": "client",
                    "iat": now,
                    "exp": now + 300,
                    "nonce": handshake["nonce"],
                },
                private_key,
                algorithms=["RS256"],
            )
            return httpx2.Response(
                200,
                request=request,
                json={
                    "access_token": "provider-token-not-persisted",
                    "token_type": "Bearer",
                    "id_token": id_token,
                },
            )
        if request.url.path == "/jwks":
            return httpx2.Response(
                200,
                request=request,
                json={"keys": [public_jwk]},
            )
        return httpx2.Response(404, request=request)

    adapter = AuthlibOidcClient(configured, httpx2.MockTransport(provider))
    with TestClient(create_app(configured, auth_database, adapter)) as oidc_client:
        start = oidc_client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        query = parse_qs(urlparse(start.headers["location"]).query)
        handshake["nonce"] = query["nonce"][0]
        callback = oidc_client.get(
            "/api/v1/auth/oidc/callback",
            params={"code": "local-code", "state": query["state"][0]},
            follow_redirects=False,
        )
        assert callback.status_code == 307
        assert callback.headers["location"] == "/success"
        assert oidc_client.get("/api/v1/auth/session").json()["authenticated"] is True
    with auth_database.session_factory() as session:
        stored = session.scalar(select(User).where(User.oidc_subject == "signed-subject"))
        assert stored is not None
        serialized_sessions = " ".join(
            value.token_hash.hex() + value.csrf_token_hash.hex()
            for value in session.scalars(select(AuthSession))
        )
        assert "provider-token-not-persisted" not in serialized_sessions


def failing_oidc_provider(mode: str):
    signing_key = RSAKey.import_key(rsa.generate_private_key(65537, 2048))
    advertised_key = signing_key
    if mode == "signature":
        advertised_key = RSAKey.import_key(rsa.generate_private_key(65537, 2048))
    public_jwk = advertised_key.as_dict(
        private=False,
        kid="test-key",
        use="sig",
        alg="RS256",
    )
    handshake = {"nonce": None}

    def provider(request: httpx2.Request):
        if request.url.path == "/.well-known/openid-configuration":
            return httpx2.Response(
                200,
                request=request,
                json={
                    "issuer": "https://idp.test",
                    "authorization_endpoint": "https://idp.test/authorize",
                    "token_endpoint": "https://idp.test/token",
                    "jwks_uri": "https://idp.test/jwks",
                    "id_token_signing_alg_values_supported": ["RS256"],
                },
            )
        if request.url.path == "/jwks":
            return httpx2.Response(200, request=request, json={"keys": [public_jwk]})
        if request.url.path == "/token":
            if mode == "missing_token":
                return httpx2.Response(
                    200,
                    request=request,
                    json={"access_token": "opaque", "token_type": "Bearer"},
                )
            now = int(time.time())
            claims = {
                "iss": "https://other-idp.test" if mode == "issuer" else "https://idp.test",
                "sub": "failed-subject",
                "aud": "other-client" if mode == "audience" else "client",
                "iat": now - 600 if mode == "expired" else now,
                "exp": now - 300 if mode == "expired" else now + 300,
                "nonce": "wrong" if mode == "nonce" else handshake["nonce"],
            }
            return httpx2.Response(
                200,
                request=request,
                json={
                    "access_token": "opaque",
                    "token_type": "Bearer",
                    "id_token": jwt.encode(
                        {"alg": "RS256", "kid": "test-key"},
                        claims,
                        signing_key,
                        algorithms=["RS256"],
                    ),
                },
            )
        return httpx2.Response(404, request=request)

    return httpx2.MockTransport(provider), handshake


@pytest.mark.parametrize(
    "mode",
    ["signature", "issuer", "audience", "expired", "nonce", "missing_token", "state"],
)
def test_authlib_oidc_protocol_failures_redirect_safely(
    auth_database, auth_settings, mode
):
    configured = oidc_settings(auth_settings)
    transport, handshake = failing_oidc_provider(mode)
    adapter = AuthlibOidcClient(configured, transport)
    with TestClient(create_app(configured, auth_database, adapter)) as oidc_client:
        start = oidc_client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        query = parse_qs(urlparse(start.headers["location"]).query)
        handshake["nonce"] = query["nonce"][0]
        state = "forged-state" if mode == "state" else query["state"][0]
        callback = oidc_client.get(
            "/api/v1/auth/oidc/callback",
            params={"code": "local-code", "state": state},
            follow_redirects=False,
        )
        assert callback.status_code == 307
        assert callback.headers["location"] == "/error"
        assert "opaque" not in callback.text
    with auth_database.session_factory() as session:
        assert session.scalar(select(User).where(User.oidc_subject == "failed-subject")) is None


def test_oidc_protocol_failure_uses_safe_redirect(auth_database, auth_settings):
    fake = FakeOidcClient(failure=True)
    with TestClient(
        create_app(oidc_settings(auth_settings), auth_database, fake)
    ) as oidc_client:
        oidc_client.get("/api/v1/auth/oidc/login", follow_redirects=False)
        callback = oidc_client.get("/api/v1/auth/oidc/callback", follow_redirects=False)
        assert callback.headers["location"] == "/error"
        assert "invalid signature" not in callback.text
        assert fake.cleared is True


def test_permission_matrix_uses_permissions_not_role_name(
    auth_database, auth_settings
):
    user_id = add_local_user(auth_database)
    app = create_app(auth_settings, auth_database)

    @app.post(
        "/api/v1/protected",
        dependencies=[Depends(require_permission("events.delete"))],
    )
    def protected():
        return {"ok": True}

    with TestClient(app) as permission_client:
        request_id = str(uuid4())
        anonymous = permission_client.post(
            "/api/v1/protected",
            headers={"X-Request-ID": request_id},
        )
        assert anonymous.status_code == 401
        assert anonymous.json()["code"] == "authentication_required"
        assert anonymous.json()["request_id"] == request_id

        logged_in = login(permission_client)
        csrf = logged_in.json()["csrf_token"]
        missing_csrf = permission_client.post(
            "/api/v1/protected",
            headers={"X-Request-ID": request_id},
        )
        assert missing_csrf.status_code == 403
        assert missing_csrf.json()["code"] == "csrf_invalid"
        assert missing_csrf.json()["request_id"] == request_id

        guest = permission_client.post(
            "/api/v1/protected",
            headers={"X-CSRF-Token": csrf, "X-Request-ID": request_id},
        )
        assert guest.status_code == 403
        assert guest.json()["code"] == "permission_denied"
        assert guest.json()["request_id"] == request_id
        with auth_database.session_factory() as session:
            session.get(User, user_id).role_id = ADMIN_ID
            session.get(Role, ADMIN_ID).name = "Renamed"
            session.commit()
        administrator = permission_client.post(
            "/api/v1/protected",
            headers={"X-CSRF-Token": csrf, "X-Request-ID": request_id},
        )
        assert administrator.status_code == 200
        assert administrator.headers["X-Request-ID"] == request_id


class FailingReadiness:
    def check(self):
        raise SQLAlchemyError("dsn and sql must stay private")


def test_health_request_id_cors_readiness_and_internal_error(
    auth_database, auth_settings
):
    configured = auth_settings.model_copy(update={"cors_origins": ["https://ui.test"]})
    app = create_app(configured, auth_database)
    app.dependency_overrides[get_readiness_repository] = lambda: FailingReadiness()

    @app.get("/explode")
    def explode():
        raise RuntimeError("private secret")

    request_id = str(uuid4())
    with TestClient(app, raise_server_exceptions=False) as error_client:
        live = error_client.get(
            "/api/v1/health/live",
            headers={"X-Request-ID": request_id, "Origin": "https://ui.test"},
        )
        assert live.status_code == 200
        assert live.headers["X-Request-ID"] == request_id
        assert live.headers["access-control-allow-origin"] == "https://ui.test"
        ready = error_client.get("/api/v1/health/ready")
        assert ready.status_code == 503
        assert ready.json()["code"] == "database_unavailable"
        failed = error_client.get("/explode", headers={"X-Request-ID": request_id})
        assert failed.status_code == 500
        assert failed.headers["X-Request-ID"] == request_id
        assert failed.json()["request_id"] == request_id
        assert failed.json()["code"] == "internal_error"
        assert "private secret" not in failed.text


def test_logout_without_cookies_is_idempotent(client):
    request_id = str(uuid4())
    response = client.post(
        "/api/v1/auth/logout",
        headers={"X-Request-ID": request_id},
    )
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["X-Request-ID"] == request_id
    assert client.get("/api/v1/auth/session").json()["authenticated"] is False


@pytest.mark.parametrize("state", ["unknown", "expired", "revoked", "orphaned"])
def test_logout_with_invalid_session_is_idempotent_and_clears_cookies(
    client, auth_database, state
):
    if state == "unknown":
        client.cookies.set("diploma_session", "unknown-session")
        client.cookies.set("diploma_session_csrf", "unknown-csrf")
    else:
        user_id = add_local_user(auth_database)
        result = login(client)
        token = client.cookies["diploma_session"]
        with auth_database.session_factory() as session:
            stored = session.scalar(
                select(AuthSession).where(AuthSession.token_hash == token_digest(token))
            )
            now = datetime.now(UTC)
            if state == "expired":
                stored.created_at = now - timedelta(hours=2)
                stored.last_seen_at = now - timedelta(hours=1, minutes=30)
                stored.expires_at = now - timedelta(hours=1)
            elif state == "revoked":
                stored.revoked_at = now
            else:
                session.delete(session.get(User, user_id))
            session.commit()
        assert result.status_code == 200

    request_id = str(uuid4())
    response = client.post(
        "/api/v1/auth/logout",
        headers={"X-Request-ID": request_id},
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == request_id
    cookies = response.headers.get_list("set-cookie")
    assert any("diploma_session=" in value and "Max-Age=0" in value for value in cookies)
    assert any("diploma_session_csrf=" in value and "Max-Age=0" in value for value in cookies)


def test_logout_with_active_session_requires_csrf_without_revoking(
    client, auth_database
):
    add_local_user(auth_database)
    login(client)
    token = client.cookies["diploma_session"]
    request_id = str(uuid4())
    response = client.post(
        "/api/v1/auth/logout",
        headers={"X-Request-ID": request_id},
    )
    assert response.status_code == 403
    assert response.json()["code"] == "csrf_invalid"
    assert response.json()["request_id"] == request_id
    with auth_database.session_factory() as session:
        stored = session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_digest(token))
        )
        assert stored.revoked_at is None


def test_logout_rejects_cross_session_csrf_without_revoking(client, auth_database):
    add_local_user(auth_database)
    first = login(client)
    token = client.cookies["diploma_session"]
    other = TestClient(create_app(client.app.state.settings, database=auth_database))
    other_csrf = login(other).json()["csrf_token"]
    client.cookies.set("diploma_session_csrf", other_csrf)
    request_id = str(uuid4())
    response = client.post(
        "/api/v1/auth/logout",
        headers={"X-CSRF-Token": other_csrf, "X-Request-ID": request_id},
    )
    assert first.status_code == 200
    assert response.status_code == 403
    assert response.json()["request_id"] == request_id
    with auth_database.session_factory() as session:
        stored = session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_digest(token))
        )
        assert stored.revoked_at is None


def test_logout_with_valid_csrf_revokes_session_and_clears_cookies(
    client, auth_database
):
    add_local_user(auth_database)
    logged_in = login(client)
    token = client.cookies["diploma_session"]
    request_id = str(uuid4())
    response = client.post(
        "/api/v1/auth/logout",
        headers={
            "X-CSRF-Token": logged_in.json()["csrf_token"],
            "X-Request-ID": request_id,
        },
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == request_id
    cookies = response.headers.get_list("set-cookie")
    assert any("diploma_session=" in value and "Max-Age=0" in value for value in cookies)
    assert any("diploma_session_csrf=" in value and "Max-Age=0" in value for value in cookies)
    with auth_database.session_factory() as session:
        stored = session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_digest(token))
        )
        assert stored.revoked_at is not None


def test_app_instances_use_their_own_database_configuration(tmp_path):
    first = create_app(Settings(database_url=f"sqlite:///{tmp_path / 'one.db'}"))
    second = create_app(Settings(database_url=f"sqlite:///{tmp_path / 'two.db'}"))
    try:
        assert first.state.database.engine.url.database.endswith("one.db")
        assert second.state.database.engine.url.database.endswith("two.db")
        assert first.state.database.engine is not second.state.database.engine
    finally:
        first.state.database.dispose()
        second.state.database.dispose()


def test_settings_security_validation():
    with pytest.raises(ValidationError, match="wildcard"):
        Settings(cors_origins=["*"])
    with pytest.raises(ValidationError, match="OIDC requires"):
        Settings(oidc_enabled=True)
    with pytest.raises(ValidationError, match="SameSite=None"):
        Settings(session_cookie_samesite="none", session_cookie_secure=False)
    with pytest.raises(ValidationError, match="unique"):
        Settings(environment="production", session_cookie_secure=True)


def test_openapi_declares_auth_paths_success_and_error_dtos(client):
    schema = client.get("/openapi.json").json()
    for path in (
        "/api/v1/health/live",
        "/api/v1/health/ready",
        "/api/v1/auth/local/login",
        "/api/v1/auth/oidc/login",
        "/api/v1/auth/oidc/callback",
        "/api/v1/auth/session",
        "/api/v1/auth/logout",
    ):
        assert path in schema["paths"]
    login_schema = schema["paths"]["/api/v1/auth/local/login"]["post"]["responses"]
    assert "200" in login_schema and "401" in login_schema and "422" in login_schema
    assert ERRORS


def add_admin_user(database, username="admin", password="correct-password"):
    with database.session_factory() as session:
        user = User(
            username=username,
            password_hash=hash_password(password),
            display_name="Administrator",
            role_id=ADMIN_ID,
        )
        session.add(user)
        session.commit()
        return user.id


def test_guest_reads_administration_but_cannot_mutate(client, auth_database):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    assert client.get("/api/v1/users").status_code == 200
    assert client.get("/api/v1/roles").status_code == 200
    assert client.get("/api/v1/oidc-role-mappings").status_code == 200
    assert client.get("/api/v1/app-settings").status_code == 200
    assert client.get("/api/v1/normalizers").status_code == 200
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    headers = {"X-CSRF-Token": csrf}
    requests = (
        ("post", "/api/v1/users", {"username": "x", "password": "correct-password", "display_name": "X", "role_id": str(GUEST_ID)}),
        ("patch", f"/api/v1/roles/{ADMIN_ID}", {"name": "Nope"}),
        ("post", "/api/v1/oidc-role-mappings", {"issuer": "https://idp.test", "claim_name": "g", "claim_value": "x", "role_id": str(GUEST_ID)}),
        ("put", "/api/v1/app-settings/missing", {"value": True, "version": 1}),
        ("post", "/api/v1/normalizers", {"name": "x", "rule": "opaque"}),
    )
    for method, path, body in requests:
        response = getattr(client, method)(path, json=body, headers=headers)
        assert response.status_code == 403
        assert response.json()["code"] == "permission_denied"


def test_administrator_can_manage_users_roles_mappings_and_normalizers(
    client, auth_database
):
    add_admin_user(auth_database)
    assert login(client, "admin").status_code == 200
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    headers = {"X-CSRF-Token": csrf}
    created = client.post(
        "/api/v1/users",
        json={
            "username": "analyst",
            "password": "long-enough-password",
            "display_name": "Analyst",
            "role_id": str(GUEST_ID),
        },
        headers=headers,
    )
    assert created.status_code == 201
    user_id = created.json()["id"]
    assert "password_hash" not in created.text
    assert client.patch(
        f"/api/v1/users/{user_id}",
        json={"display_name": "Renamed Analyst"},
        headers=headers,
    ).status_code == 200
    role = client.patch(
        f"/api/v1/roles/{GUEST_ID}",
        json={"name": "Read Only"},
        headers=headers,
    )
    assert role.status_code == 200
    mapping = client.post(
        "/api/v1/oidc-role-mappings",
        json={"issuer": "https://idp.test/path", "claim_name": "groups", "claim_value": "read", "role_id": str(GUEST_ID)},
        headers=headers,
    )
    assert mapping.status_code == 201
    normalizer = client.post(
        "/api/v1/normalizers",
        json={"name": "syslog", "description": "opaque", "rule": "not executed"},
        headers=headers,
    )
    assert normalizer.status_code == 201
    normalizer_id = normalizer.json()["id"]
    assert normalizer.json()["version"] == 1
    updated = client.patch(
        f"/api/v1/normalizers/{normalizer_id}",
        json={"description": "changed", "version": 1},
        headers=headers,
    )
    assert updated.status_code == 200
    assert updated.json()["version"] == 2
    stale = client.patch(
        f"/api/v1/normalizers/{normalizer_id}",
        json={"rule": "stale", "version": 1},
        headers=headers,
    )
    assert stale.status_code == 409
    assert stale.json()["code"] == "version_conflict"


def test_settings_registry_and_normalizer_delete_are_version_aware(
    client, auth_database
):
    add_admin_user(auth_database)
    with auth_database.session_factory() as session:
        setting = AppSetting(
            key="test.setting",
            value={"enabled": False},
            category="test",
            is_public=True,
        )
        session.add(setting)
        session.commit()
    assert login(client, "admin").status_code == 200
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    headers = {"X-CSRF-Token": csrf}
    setting = client.get("/api/v1/app-settings/test.setting")
    assert setting.status_code == 200 and setting.json()["version"] == 1
    changed = client.put(
        "/api/v1/app-settings/test.setting",
        json={"value": {"enabled": True}, "version": 1},
        headers=headers,
    )
    assert changed.status_code == 200 and changed.json()["version"] == 2
    stale = client.put(
        "/api/v1/app-settings/test.setting",
        json={"value": {"enabled": False}, "version": 1},
        headers=headers,
    )
    assert stale.status_code == 409 and stale.json()["code"] == "version_conflict"
    assert client.put(
        "/api/v1/app-settings/unknown",
        json={"value": 1, "version": 1},
        headers=headers,
    ).status_code == 404
    normalizer = client.post(
        "/api/v1/normalizers",
        json={"name": "delete-me", "rule": "opaque"},
        headers=headers,
    ).json()
    assert client.patch(
        f"/api/v1/normalizers/{normalizer['id']}",
        json={"description": "changed", "version": 1},
        headers=headers,
    ).status_code == 200
    assert client.delete(
        f"/api/v1/normalizers/{normalizer['id']}?version=1", headers=headers
    ).status_code == 409
    assert client.delete(
        f"/api/v1/normalizers/{normalizer['id']}?version=2", headers=headers
    ).status_code == 204


def test_administration_openapi_excludes_forbidden_future_routes(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/v1/users" in paths
    assert "/api/v1/roles" in paths
    assert "/api/v1/app-settings/{key}" in paths
    assert "/api/v1/normalizers" in paths
    assert "/api/v1/kafka-connections" not in paths
    assert "/api/v1/sources" not in paths
    assert "/api/v1/parsed-logs" not in paths
    assert "post" not in paths["/api/v1/roles"]
    assert "delete" not in paths["/api/v1/roles/{role_id}"]


def admin_headers(client, database):
    add_admin_user(database)
    assert login(client, "admin").status_code == 200
    return {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]}


def create_normalizer(client, headers, name="normalizer"):
    response = client.post(
        "/api/v1/normalizers",
        json={"name": name, "description": "test", "rule": "opaque rule"},
        headers=headers,
    )
    assert response.status_code == 201
    return response.json()


def test_administration_dto_validation_and_safe_errors(client, auth_database):
    headers = admin_headers(client, auth_database)
    base = {
        "username": "email-user",
        "password": "long-enough-password",
        "display_name": "Email User",
        "role_id": str(GUEST_ID),
    }
    for invalid_email in ("bad@", "plain-address", "name@example"):
        response = client.post(
            "/api/v1/users", json={**base, "email": invalid_email}, headers=headers
        )
        assert response.status_code == 422
        assert response.json()["code"] == "validation_error"
        assert response.headers["X-Request-ID"] == response.json()["request_id"]
    created = client.post(
        "/api/v1/users", json={**base, "email": None}, headers=headers
    )
    assert created.status_code == 201
    user_id = created.json()["id"]
    normalizer = create_normalizer(client, headers)
    mapping = client.post(
        "/api/v1/oidc-role-mappings",
        json={
            "issuer": "https://idp.test/path/",
            "claim_name": "groups",
            "claim_value": "guests",
            "role_id": str(GUEST_ID),
        },
        headers=headers,
    ).json()
    invalid_requests = (
        ("patch", f"/api/v1/users/{user_id}", {"username": None}),
        ("patch", f"/api/v1/users/{user_id}", {"display_name": None}),
        ("patch", f"/api/v1/normalizers/{normalizer['id']}", {"name": None, "version": 1}),
        ("patch", f"/api/v1/normalizers/{normalizer['id']}", {"rule": None, "version": 1}),
        ("patch", f"/api/v1/oidc-role-mappings/{mapping['id']}", {"issuer": None}),
        ("patch", f"/api/v1/oidc-role-mappings/{mapping['id']}", {"role_id": None}),
        ("patch", f"/api/v1/normalizers/{normalizer['id']}", {"version": 1}),
    )
    for method, path, body in invalid_requests:
        response = getattr(client, method)(path, json=body, headers=headers)
        assert response.status_code == 422
        assert response.json()["code"] == "validation_error"
    assert mapping["issuer"] == "https://idp.test/path/"


def test_administration_permissions_csrf_and_dependency_override(client, auth_database):
    assert client.get("/api/v1/users").status_code == 401
    add_local_user(auth_database)
    assert login(client).status_code == 200
    invalid_csrf = client.post(
        "/api/v1/normalizers", json={"name": "x", "rule": "opaque"}
    )
    assert invalid_csrf.status_code == 403
    assert invalid_csrf.json()["code"] == "csrf_invalid"

    class FakeUsers:
        def list(self, *_):
            return [], 0

    class FakeRoles:
        def list(self):
            return []

    class FakeMappings:
        def list(self, *_):
            return [], 0

    class FakeSettings:
        def list(self, _):
            return []

    class FakeNormalizers:
        def list(self, *_):
            return [], 0

    overrides = {
        get_user_admin: FakeUsers(),
        get_role_admin: FakeRoles(),
        get_mapping_admin: FakeMappings(),
        get_setting_admin: FakeSettings(),
        get_normalizer_admin: FakeNormalizers(),
    }
    for dependency, service in overrides.items():
        client.app.dependency_overrides[dependency] = lambda service=service: service
    try:
        assert client.get("/api/v1/users").json()["items"] == []
        assert client.get("/api/v1/roles").json() == []
        assert client.get("/api/v1/oidc-role-mappings").json()["items"] == []
        assert client.get("/api/v1/app-settings").json() == []
        assert client.get("/api/v1/normalizers").json()["items"] == []
    finally:
        client.app.dependency_overrides.clear()


def test_user_lifecycle_self_password_and_session_revocation(client, auth_database):
    headers = admin_headers(client, auth_database)
    admin_session = client.get("/api/v1/auth/session").json()
    admin_id = admin_session["user"]["id"]
    changed = client.put(
        f"/api/v1/users/{admin_id}/password",
        json={"password": "new-long-enough-password"},
        headers=headers,
    )
    assert changed.status_code == 200
    assert client.get("/api/v1/auth/session").json()["authenticated"] is False
    assert login(client, "admin", "new-long-enough-password").status_code == 200
    refreshed_headers = {
        "X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]
    }
    created = client.post(
        "/api/v1/users",
        json={
            "id": "11111111-1111-4111-8111-111111111111",
            "username": "case-user",
            "password": "long-enough-password",
            "display_name": "Case User",
            "role_id": str(GUEST_ID),
        },
        headers=refreshed_headers,
    )
    assert created.status_code == 201
    user_id = created.json()["id"]
    duplicate = client.post(
        "/api/v1/users",
        json={
            "username": "CASE-USER",
            "password": "long-enough-password",
            "display_name": "Duplicate",
            "role_id": str(GUEST_ID),
        },
        headers=refreshed_headers,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "username_conflict"
    assert client.post(
        f"/api/v1/users/{user_id}/deactivate", headers=refreshed_headers
    ).status_code == 200
    assert client.post(
        f"/api/v1/users/{user_id}/activate", headers=refreshed_headers
    ).status_code == 200
    assert client.put(
        f"/api/v1/users/{admin_id}/role",
        json={"role_id": str(GUEST_ID), "role_managed_by_oidc": False},
        headers=refreshed_headers,
    ).status_code == 409


def test_deactivate_and_password_reset_revoke_other_user_sessions(
    client, auth_database, auth_settings
):
    headers = admin_headers(client, auth_database)
    created = client.post(
        "/api/v1/users",
        json={
            "username": "session-target",
            "password": "long-enough-password",
            "display_name": "Session Target",
            "role_id": str(GUEST_ID),
        },
        headers=headers,
    ).json()
    target_id = created["id"]
    with TestClient(create_app(auth_settings, database=auth_database)) as target_client:
        assert login(target_client, "session-target", "long-enough-password").status_code == 200
        assert client.post(
            f"/api/v1/users/{target_id}/deactivate", headers=headers
        ).status_code == 200
        assert target_client.get("/api/v1/auth/session").json()["authenticated"] is False
    assert client.post(
        f"/api/v1/users/{target_id}/activate", headers=headers
    ).status_code == 200
    with TestClient(create_app(auth_settings, database=auth_database)) as target_client:
        assert login(target_client, "session-target", "long-enough-password").status_code == 200
        assert client.put(
            f"/api/v1/users/{target_id}/password",
            json={"password": "changed-long-password"},
            headers=headers,
        ).status_code == 200
        assert target_client.get("/api/v1/auth/session").json()["authenticated"] is False


def test_mapping_pagination_and_normalizer_delete_graph(client, auth_database):
    headers = admin_headers(client, auth_database)
    for number in range(3):
        response = client.post(
            "/api/v1/oidc-role-mappings",
            json={
                "issuer": "https://idp.test",
                "claim_name": "groups",
                "claim_value": f"group-{number}",
                "role_id": str(GUEST_ID),
            },
            headers=headers,
        )
        assert response.status_code == 201
    page = client.get("/api/v1/oidc-role-mappings?limit=1&offset=1").json()
    assert page["total"] == 3 and len(page["items"]) == 1
    assert client.get(
        f"/api/v1/oidc-role-mappings?role_id={GUEST_ID}&limit=10"
    ).json()["total"] == 3

    normalizer = create_normalizer(client, headers, "delete-graph")
    with auth_database.session_factory() as session:
        connection = KafkaConnection(name="delete-graph-connection", bootstrap_servers=["broker:9092"])
        session.add(connection)
        session.flush()
        source = Source(
            name="delete-graph-source",
            connection_id=connection.id,
            normalizer_id=UUID(normalizer["id"]),
            topic_name="delete-graph-topic",
            is_enabled=True,
        )
        session.add(source)
        session.flush()
        timestamp = datetime.now(UTC)
        parsed = ParsedLog(
            source_id=source.id,
            connection_id=connection.id,
            normalizer_id=UUID(normalizer["id"]),
            normalizer_version=1,
            source_name=source.name,
            connection_name=connection.name,
            kafka_topic=source.topic_name,
            kafka_partition=0,
            kafka_offset=1,
            deduplication_key="delete-graph-log",
            fluent_bit_collected_at=timestamp,
            backend_received_at=timestamp,
            backend_processed_at=timestamp,
            raw="raw",
            ecs_data={},
        )
        session.add(parsed)
        session.commit()
        source_id, parsed_id = source.id, parsed.id
        source_updated_at = source.updated_at
    assert client.delete(
        f"/api/v1/normalizers/{normalizer['id']}?version=1", headers=headers
    ).status_code == 204
    with auth_database.session_factory() as session:
        source = session.get(Source, source_id)
        parsed = session.get(ParsedLog, parsed_id)
        assert source.is_enabled is False and source.normalizer_id is None
        assert source.updated_at > source_updated_at
        assert parsed is not None and parsed.normalizer_id is None


def test_concurrent_version_conflicts_roll_back_side_effects(auth_database):
    actor_id = add_admin_user(auth_database)
    with auth_database.session_factory() as session:
        setting = AppSetting(key="concurrent.setting", value={"value": 0}, category="test")
        normalizer = Normalizer(name="concurrent-normalizer", rule="opaque")
        session.add_all([setting, normalizer])
        session.flush()
        connection = KafkaConnection(
            name="concurrent-connection", bootstrap_servers=["broker:9092"]
        )
        session.add(connection)
        session.flush()
        source = Source(
            name="concurrent-source",
            connection_id=connection.id,
            normalizer_id=normalizer.id,
            topic_name="concurrent-topic",
            is_enabled=True,
        )
        session.add(source)
        session.commit()
        normalizer_id, source_id = normalizer.id, source.id
    first = auth_database.session_factory()
    second = auth_database.session_factory()
    try:
        settings_one = SettingAdministration(
            SqlAlchemySettingRepository(first), SqlAlchemyUnitOfWork(first)
        )
        settings_two = SettingAdministration(
            SqlAlchemySettingRepository(second), SqlAlchemyUnitOfWork(second)
        )
        assert settings_two.get("concurrent.setting").version == 1
        assert settings_one.update("concurrent.setting", {"value": 1}, 1, actor_id).version == 2
        with pytest.raises(DomainError) as stale_setting:
            settings_two.update("concurrent.setting", {"value": 2}, 1, actor_id)
        assert stale_setting.value.code == "version_conflict"
        assert stale_setting.value.details["current_version"] == 2
    finally:
        first.close()
        second.close()

    first = auth_database.session_factory()
    second = auth_database.session_factory()
    try:
        normalizers_one = NormalizerAdministration(
            SqlAlchemyNormalizerRepository(first), SqlAlchemyUnitOfWork(first)
        )
        normalizers_two = NormalizerAdministration(
            SqlAlchemyNormalizerRepository(second), SqlAlchemyUnitOfWork(second)
        )
        assert normalizers_two.get(normalizer_id).version == 1
        assert normalizers_one.update(
            normalizer_id,
            NormalizerPatch(description="first", version=1),
            actor_id,
        ).version == 2
        with pytest.raises(DomainError) as stale_normalizer:
            normalizers_two.delete(normalizer_id, 1)
        assert stale_normalizer.value.code == "version_conflict"
        assert stale_normalizer.value.details["current_version"] == 2
        with auth_database.session_factory() as session:
            assert session.get(Normalizer, normalizer_id).description == "first"
            source = session.get(Source, source_id)
            assert source.is_enabled is True and source.normalizer_id == normalizer_id
    finally:
        first.close()
        second.close()


def test_administration_conflicts_use_real_constraint_names(client, auth_database):
    headers = admin_headers(client, auth_database)
    duplicate_role = client.patch(
        f"/api/v1/roles/{GUEST_ID}", json={"name": "Administrator"}, headers=headers
    )
    assert duplicate_role.status_code == 409
    assert duplicate_role.json()["code"] == "role_name_conflict"

    first_mapping = {
        "issuer": "https://conflicts.test",
        "claim_name": "groups",
        "claim_value": "one",
        "role_id": str(GUEST_ID),
    }
    assert client.post("/api/v1/oidc-role-mappings", json=first_mapping, headers=headers).status_code == 201
    duplicate_mapping = client.post(
        "/api/v1/oidc-role-mappings", json=first_mapping, headers=headers
    )
    assert duplicate_mapping.status_code == 409
    assert duplicate_mapping.json()["code"] == "oidc_mapping_conflict"
    second_mapping = client.post(
        "/api/v1/oidc-role-mappings",
        json={**first_mapping, "claim_value": "two"},
        headers=headers,
    ).json()
    mapping_update = client.patch(
        f"/api/v1/oidc-role-mappings/{second_mapping['id']}",
        json={"claim_value": "one"},
        headers=headers,
    )
    assert mapping_update.status_code == 409
    assert mapping_update.json()["code"] == "oidc_mapping_conflict"

    first_normalizer = create_normalizer(client, headers, "conflict-first")
    duplicate_normalizer = client.post(
        "/api/v1/normalizers",
        json={"name": "conflict-first", "rule": "opaque"},
        headers=headers,
    )
    assert duplicate_normalizer.status_code == 409
    assert duplicate_normalizer.json()["code"] == "normalizer_name_conflict"
    second_normalizer = create_normalizer(client, headers, "conflict-second")
    normalizer_update = client.patch(
        f"/api/v1/normalizers/{second_normalizer['id']}",
        json={"name": first_normalizer["name"], "version": 1},
        headers=headers,
    )
    assert normalizer_update.status_code == 409
    assert normalizer_update.json()["code"] == "normalizer_name_conflict"

    explicit_id = "22222222-2222-4222-8222-222222222222"
    user = {
        "id": explicit_id,
        "username": "explicit-conflict",
        "password": "long-enough-password",
        "display_name": "Explicit Conflict",
        "role_id": str(GUEST_ID),
    }
    assert client.post("/api/v1/users", json=user, headers=headers).status_code == 201
    duplicate_uuid = client.post(
        "/api/v1/users",
        json={**user, "username": "another-user"},
        headers=headers,
    )
    assert duplicate_uuid.status_code == 409
    assert duplicate_uuid.json()["code"] == "integrity_conflict"


def test_user_filters_identity_rules_and_self_actions(client, auth_database):
    headers = admin_headers(client, auth_database)
    first = client.post(
        "/api/v1/users",
        json={
            "username": "filter-local",
            "password": "long-enough-password",
            "display_name": "Local Filter",
            "email": "local@example.com",
            "role_id": str(GUEST_ID),
        },
        headers=headers,
    ).json()
    explicit = client.post(
        "/api/v1/users",
        json={
            "id": "33333333-3333-4333-8333-333333333333",
            "username": "filter-admin",
            "password": "long-enough-password",
            "display_name": "Admin Filter",
            "role_id": str(ADMIN_ID),
            "is_active": False,
        },
        headers=headers,
    ).json()
    with auth_database.session_factory() as session:
        oidc_user = User(
            oidc_issuer="https://idp.filter.test",
            oidc_subject="filter-subject",
            email="oidc@example.com",
            display_name="OIDC Filter",
            role_id=GUEST_ID,
            role_managed_by_oidc=True,
        )
        session.add(oidc_user)
        session.commit()
        oidc_id = oidc_user.id
    users = client.get("/api/v1/users?limit=2&offset=0").json()
    assert users["total"] == 4 and len(users["items"]) == 2
    assert [item["id"] for item in users["items"]] == sorted(item["id"] for item in users["items"])
    assert client.get("/api/v1/users?q=Local%20Filter").json()["total"] == 1
    assert client.get(f"/api/v1/users?role_id={ADMIN_ID}").json()["total"] == 2
    assert client.get("/api/v1/users?is_active=false").json()["total"] == 1
    oidc_page = client.get("/api/v1/users?authentication_method=oidc").json()
    assert oidc_page["total"] == 1 and oidc_page["items"][0]["id"] == str(oidc_id)
    oidc_response = client.get(f"/api/v1/users/{oidc_id}")
    assert oidc_response.status_code == 200 and "password_hash" not in oidc_response.text
    assert client.patch(
        f"/api/v1/users/{first['id']}",
        json={"username": "filter-local-renamed", "display_name": "Renamed", "email": None},
        headers=headers,
    ).status_code == 200
    assert client.patch(
        f"/api/v1/users/{oidc_id}", json={"username": "not-allowed"}, headers=headers
    ).status_code == 409
    assert client.put(
        f"/api/v1/users/{oidc_id}/password",
        json={"password": "long-enough-password"},
        headers=headers,
    ).status_code == 409
    assert client.put(
        f"/api/v1/users/{first['id']}/role",
        json={"role_id": str(GUEST_ID), "role_managed_by_oidc": True},
        headers=headers,
    ).status_code == 409
    assert client.put(
        f"/api/v1/users/{oidc_id}/role",
        json={"role_id": str(ADMIN_ID), "role_managed_by_oidc": False},
        headers=headers,
    ).status_code == 200
    assert client.post(f"/api/v1/users/{explicit['id']}/deactivate", headers=headers).status_code == 200
    assert client.post(f"/api/v1/users/{explicit['id']}/deactivate", headers=headers).status_code == 200
    assert client.post(f"/api/v1/users/{explicit['id']}/activate", headers=headers).status_code == 200
    assert client.post(f"/api/v1/users/{explicit['id']}/activate", headers=headers).status_code == 200
    current_id = client.get("/api/v1/auth/session").json()["user"]["id"]
    assert client.post(f"/api/v1/users/{current_id}/deactivate", headers=headers).status_code == 409
    assert client.delete(f"/api/v1/users/{current_id}", headers=headers).status_code == 409
    missing = client.get("/api/v1/users/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert missing.status_code == 404 and missing.headers["X-Request-ID"] == missing.json()["request_id"]


def test_settings_registry_roles_and_openapi_contracts(client, auth_database):
    headers = admin_headers(client, auth_database)
    assert client.get("/api/v1/app-settings").json() == []
    with auth_database.session_factory() as session:
        session.add_all(
            [
                AppSetting(key="settings.one", value={"old": 1}, category="alpha", is_public=True),
                AppSetting(key="settings.two", value=False, category="beta"),
            ]
        )
        session.commit()
    assert len(client.get("/api/v1/app-settings?category=alpha").json()) == 1
    changed = client.put(
        "/api/v1/app-settings/settings.one",
        json={"value": {"new": 2}, "version": 1},
        headers=headers,
    ).json()
    assert changed["value"] == {"new": 2} and changed["version"] == 2
    assert changed["updated_by_user_id"] == client.get("/api/v1/auth/session").json()["user"]["id"]
    assert client.post("/api/v1/app-settings", headers=headers).status_code == 405
    assert client.delete("/api/v1/app-settings/settings.one", headers=headers).status_code == 405
    roles = client.get("/api/v1/roles").json()
    assert {item["id"] for item in roles} == {str(ADMIN_ID), str(GUEST_ID)}
    role_before = next(item for item in roles if item["id"] == str(GUEST_ID))
    renamed = client.patch(
        f"/api/v1/roles/{GUEST_ID}", json={"name": "Read-only"}, headers=headers
    ).json()
    assert renamed["id"] == role_before["id"]
    assert renamed["permissions"] == role_before["permissions"]
    assert renamed["priority"] == role_before["priority"]
    assert renamed["updated_at"] >= role_before["updated_at"]
    assert client.patch(
        f"/api/v1/roles/{GUEST_ID}", json={"name": "  "}, headers=headers
    ).status_code == 422
    schema = client.get("/openapi.json").json()
    password_schema = schema["components"]["schemas"]["PasswordUpdate"]["properties"]["password"]
    assert password_schema["format"] == "password" and password_schema["writeOnly"] is True
    assert "password_hash" not in schema["components"]["schemas"]["UserDTO"]["properties"]


def test_mapping_legacy_crud_and_normalizer_listing_contracts(client, auth_database):
    headers = admin_headers(client, auth_database)
    actor_id = client.get("/api/v1/auth/session").json()["user"]["id"]
    explicit_mapping = client.post(
        "/api/v1/oidc-role-mappings",
        json={
            "id": "44444444-4444-4444-8444-444444444444",
            "issuer": "https://mapping.test/issuer",
            "claim_name": "groups",
            "claim_value": "initial",
            "role_id": str(GUEST_ID),
        },
        headers=headers,
    ).json()
    assert client.get(f"/api/v1/oidc-role-mappings/{explicit_mapping['id']}").status_code == 200
    updated = client.patch(
        f"/api/v1/oidc-role-mappings/{explicit_mapping['id']}",
        json={"claim_value": "updated"},
        headers=headers,
    )
    assert updated.status_code == 200 and updated.json()["claim_value"] == "updated"
    with auth_database.session_factory() as session:
        legacy = OidcRoleMapping(
            issuer="https://legacy.test",
            claim_name="groups",
            claim_value="legacy",
            role_id=None,
        )
        session.add(legacy)
        session.commit()
        legacy_id = legacy.id
    legacy_response = client.get(f"/api/v1/oidc-role-mappings/{legacy_id}")
    assert legacy_response.status_code == 200 and legacy_response.json()["role_id"] is None
    assert client.delete(f"/api/v1/oidc-role-mappings/{legacy_id}", headers=headers).status_code == 204
    assert client.get(f"/api/v1/oidc-role-mappings/{legacy_id}").status_code == 404

    literal_percent = create_normalizer(client, headers, "literal%normalizer")
    literal_underscore = create_normalizer(client, headers, "literal_normalizer")
    literal_backslash = create_normalizer(client, headers, "literal\\normalizer")
    explicit_normalizer = client.post(
        "/api/v1/normalizers",
        json={
            "id": "55555555-5555-4555-8555-555555555555",
            "name": "opaque-normalizer",
            "rule": "this is deliberately not an ECS parser rule",
        },
        headers=headers,
    ).json()
    assert literal_percent["created_by_user_id"] == actor_id
    assert explicit_normalizer["updated_by_user_id"] == actor_id
    page = client.get("/api/v1/normalizers?limit=2&offset=0").json()
    assert page["total"] == 4 and len(page["items"]) == 2
    assert [item["id"] for item in page["items"]] == sorted(item["id"] for item in page["items"])
    assert client.get("/api/v1/normalizers", params={"q": "%"}).json()["total"] == 1
    assert client.get("/api/v1/normalizers", params={"q": "_"}).json()["total"] == 1
    assert client.get("/api/v1/normalizers", params={"q": "\\"}).json()["total"] == 1
    assert literal_underscore["version"] == 1
    assert literal_backslash["version"] == 1
    for body in ({"name": " ", "rule": "opaque"}, {"name": "blank-rule", "rule": "  "}):
        assert client.post("/api/v1/normalizers", json=body, headers=headers).status_code == 422
