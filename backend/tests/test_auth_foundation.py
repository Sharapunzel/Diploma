import base64
import getpass
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import httpx2
import jsonschema
import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends
from fastapi.testclient import TestClient
from joserfc import jwt
from joserfc.jwk import RSAKey
from pydantic import ValidationError
from sqlalchemy import event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.api.v1.endpoints.auth import ERRORS
from app.cli import create_admin
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password, token_digest
from app.db import Database
from app.dependencies import (
    get_cursor_codec,
    get_ecs_catalog,
    get_kafka_client,
    get_mapping_admin,
    get_normalizer_admin,
    get_readiness_repository,
    get_role_admin,
    get_setting_admin,
    get_user_admin,
    require_permission,
)
from app.ecs import PackagedEcsCatalog, ValidatedEcsFilter
from app.kafka import KafkaMetadata, KafkaMetadataError, KafkaTopic
from app.main import create_app
from app.models import (
    AppSetting,
    AuthSession,
    KafkaConnection,
    KafkaOperationalEvent,
    Normalizer,
    OidcRoleMapping,
    ParsedLog,
    ProcessedKafkaRecord,
    ProcessingError,
    Role,
    Source,
    User,
)
from app.normalization.schema import ValueSource
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
from app.repositories.sqlalchemy.consumers import SqlAlchemyConsumerStateRepository
from app.repositories.sqlalchemy.diagnostics import SqlAlchemyDiagnosticsRepository
from app.repositories.sqlalchemy.parsed_logs import _filter_expression
from app.schemas.administration import NormalizerPatch
from app.schemas.parsed_logs import ParsedLogBulkDeleteRequest, ParsedLogSearchRequest
from app.services.implementations.administration import (
    NormalizerAdministration,
    SettingAdministration,
)
from app.services.implementations.auth import AuthService
from app.services.implementations.consumers import ConsumerWorkerState
from app.services.implementations.cursor import SignedParsedLogCursorCodec
from app.services.implementations.diagnostics import DiagnosticsServiceImpl
from app.services.implementations.parsed_logs import ParsedLogServiceImpl

BACKEND_DIR = Path(__file__).resolve().parents[1]
ADMIN_ID = UUID("00000000-0000-4000-8000-000000000001")
GUEST_ID = UUID("00000000-0000-4000-8000-000000000002")


def simple_normalizer_rule():
    return {
        "format_version": 1,
        "variants": [{
            "key": "plain", "priority": 1,
            "when": {"kind": "contains", "value": " "},
            "blocks": [{
                "key": "message", "kind": "map_ecs", "target": "message",
                "source": {"ref": "log"}, "required": True,
            }],
        }],
    }


def test_normalizer_preview_catalog_and_permissions(client, auth_database):
    assert client.get("/api/v1/normalizers/block-types").status_code == 401
    assert client.post("/api/v1/normalizers/preview", json={
        "rule": simple_normalizer_rule(),
        "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"},
    }).status_code == 401
    add_local_user(auth_database)
    assert login(client).status_code == 200
    kinds = {item["kind"] for item in client.get("/api/v1/normalizers/block-types").json()}
    assert kinds == {"regex", "template", "columns", "json", "map_ecs"}
    mappable_page = client.get("/api/v1/ecs/fields", params={"mappable": True, "limit": 5})
    assert mappable_page.status_code == 200
    assert all(item["mappable"] for item in mappable_page.json()["items"])
    field = client.get("/api/v1/ecs/fields/event.original").json()
    assert field["mappable"] is False and field["mappable_reason"] == "system_managed"
    assert client.get("/api/v1/ecs/fields", params={"mappable": False, "q": "event.original"}).json()["total"] >= 1
    schema = client.get("/openapi.json").json()["components"]["schemas"]
    rule_schema = schema["RuleV1"]
    assert rule_schema["properties"]["format_version"]["const"] == 1
    block_items = schema["Variant"]["properties"]["blocks"]["items"]
    assert {"$ref": "#/components/schemas/RegexBlock"} in block_items["oneOf"]
    assert {"$ref": "#/components/schemas/MapBlock"} in block_items["oneOf"]
    assert {"RegexBlock", "TemplateBlock", "ColumnsBlock", "JsonBlock", "MapBlock"} <= set(schema)
    assert schema["Condition"]["properties"]["kind"]["enum"] == ["prefix", "contains", "regex"]
    assert schema["ColumnsCandidate"]["properties"]["delimiter"]["enum"] == [" ", ",", ";", "|", "\t"]
    source_schema = schema["ValueSource"]
    assert len(source_schema["oneOf"]) == 4
    assert {tuple(schema[item["$ref"].rsplit("/", 1)[-1]]["required"])
            for item in source_schema["oneOf"]} == {
        ("ref",), ("literal",), ("refs", "join"), ("refs", "as_array"),
    }
    guest_preview = client.post("/api/v1/normalizers/preview", json={
        "rule": simple_normalizer_rule(),
        "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"},
    })
    assert guest_preview.status_code == 403
    assert guest_preview.json()["code"] == "csrf_invalid"
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    assert client.post("/api/v1/normalizers/preview", json={
        "rule": simple_normalizer_rule(), "sample": {},
    }, headers={"X-CSRF-Token": csrf}).status_code == 403


def test_normalizer_preview_crud_and_rule_errors(client, auth_database):
    headers = admin_headers(client, auth_database)
    payload = {"rule": simple_normalizer_rule(),
               "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"}}
    preview = client.post("/api/v1/normalizers/preview", json=payload, headers=headers)
    assert preview.status_code == 200
    assert preview.json()["status"] == "partial"
    assert preview.json()["ecs_data"]["message"] == "user created"
    assert client.get("/api/v1/normalizers").json()["total"] == 0
    bad_rule = simple_normalizer_rule()
    bad_rule["variants"][0]["blocks"][0]["target"] = "event.original"
    invalid = client.post("/api/v1/normalizers/preview", json={**payload, "rule": bad_rule}, headers=headers)
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "normalizer_rule_invalid"
    assert invalid.json()["details"] == {"variant": "plain", "block": "message", "parameter": "target"}
    assert invalid.headers["X-Request-ID"] == invalid.json()["request_id"]
    assert client.post("/api/v1/normalizers", json={"name": "bad", "rule": bad_rule}, headers=headers).status_code == 422
    created = client.post("/api/v1/normalizers", json={"name": "v1", "rule": payload["rule"]}, headers=headers)
    assert created.status_code == 201
    normalizer = created.json()
    assert normalizer["rule_status"] == "ready" and normalizer["version"] == 1
    revised = simple_normalizer_rule()
    revised["variants"][0]["when"]["value"] = "created"
    changed = client.patch(f"/api/v1/normalizers/{normalizer['id']}",
                           json={"rule": revised, "version": 1}, headers=headers)
    assert changed.status_code == 200 and changed.json()["version"] == 2
    stale = client.patch(f"/api/v1/normalizers/{normalizer['id']}",
                         json={"rule": payload["rule"], "version": 1}, headers=headers)
    assert stale.status_code == 409 and stale.json()["code"] == "version_conflict"
    assert client.get(f"/api/v1/normalizers/{normalizer['id']}").json()["rule"] == revised


def test_normalizer_value_source_variants_are_accepted_and_exclusive(client, auth_database):
    headers = admin_headers(client, auth_database)

    def make_rule(source):
        configured = simple_normalizer_rule()
        if source.get("as_array") and "join" not in source:
            configured["variants"][0]["blocks"][0]["target"] = "related.ip"
            configured["variants"][0]["blocks"].insert(0, {
                "key": "parse", "kind": "regex",
                "candidates": [{"pattern": r"(?P<ip>\S+)"}],
            })
            source = {"refs": ["parse.ip"], "as_array": True}
        configured["variants"][0]["blocks"][-1]["source"] = source
        return configured

    for source in (
        {"ref": "log"}, {"literal": "constant"},
        {"refs": ["log"], "join": " "},
        {"refs": ["log"], "as_array": True},
    ):
        response = client.post("/api/v1/normalizers/preview", json={
            "rule": make_rule(source),
            "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"},
        }, headers=headers)
        assert response.status_code == 200, response.json()
    for source in (
        {"ref": "log", "literal": "two"},
        {"refs": ["log"], "join": " ", "as_array": True},
        {"refs": ["log"]},
    ):
        response = client.post("/api/v1/normalizers/preview", json={
            "rule": make_rule(source),
            "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"},
        }, headers=headers)
        assert response.status_code == 422, (source, response.json())
        assert response.json()["code"] in {"validation_error", "normalizer_rule_invalid"}


def test_value_source_openapi_matches_validation_and_api(client, auth_database):
    headers = admin_headers(client, auth_database)
    components = client.get("/openapi.json").json()["components"]
    published = {
        "$ref": "#/components/schemas/ValueSource",
        "components": components,
    }
    validator = jsonschema.Draft202012Validator(published)
    valid = (
        {"ref": "log"}, {"ref": "x" * 2048},
        {"literal": "value"}, {"literal": 0}, {"literal": False},
        {"literal": ["a", None]}, {"literal": {"key": None}},
        {"refs": ["log"], "join": ""},
        {"refs": ["log"], "join": "x" * 64},
        {"refs": ["log"] * 32, "as_array": True},
    )
    invalid = (
        {}, {"ref": "log", "as_array": False}, {"ref": "log", "join": None},
        {"refs": ["log"], "join": " ", "as_array": False},
        {"ref": "log", "literal": "other"},
        {"literal": None}, {"ref": ""}, {"refs": [""]},
        {"refs": ["log"]}, {"refs": ["log"], "as_array": False},
        {"refs": ["log"], "join": " ", "as_array": True},
        {"ref": "x" * 2049}, {"refs": ["log"] * 33, "as_array": True},
        {"refs": ["log"], "join": "x" * 65},
        {"literal": "v", "unexpected": 1},
    )
    for candidate in valid:
        assert validator.is_valid(candidate), candidate
        assert ValueSource.model_validate(candidate).model_dump() == candidate
    for candidate in invalid:
        assert not validator.is_valid(candidate), candidate
        with pytest.raises(ValidationError):
            ValueSource.model_validate(candidate)
        rule = simple_normalizer_rule()
        rule["variants"][0]["blocks"][0]["source"] = candidate
        response = client.post("/api/v1/normalizers/preview", json={
            "rule": rule,
            "sample": {"timestamp": "2025-06-01T12:00:00Z", "log": "user created"},
        }, headers=headers)
        assert response.status_code == 422, candidate


def test_normalizer_legacy_activation_rejected(client, auth_database):
    headers = admin_headers(client, auth_database)
    with auth_database.session_factory() as session:
        legacy = Normalizer(name="legacy", rule={"format_version": 0, "legacy_rule_text": "opaque"})
        connection = KafkaConnection(name="legacy-connection", bootstrap_servers=["broker:9092"])
        session.add_all([legacy, connection])
        session.flush()
        source = Source(name="legacy-source", connection_id=connection.id,
                        normalizer_id=legacy.id, topic_name="events", is_enabled=False)
        session.add(source)
        session.commit()
        source_id, legacy_id = source.id, legacy.id
    assert client.get(f"/api/v1/normalizers/{legacy_id}").json()["rule_status"] == "legacy_incompatible"
    response = client.post(f"/api/v1/sources/{source_id}/enable", headers=headers)
    assert response.status_code == 409
    assert response.json()["code"] == "normalizer_legacy_incompatible"
    with auth_database.session_factory() as session:
        assert session.get(Source, source_id).is_enabled is False


def test_0004_legacy_roundtrip_and_enabled_source(auth_database_url, auth_database):
    run_alembic(auth_database_url, "downgrade", "0003_parsed_logs_search")
    try:
        legacy_text = "legacy %d / raw ✓"
        with auth_database.engine.begin() as connection:
            legacy_id = connection.execute(text("""
                INSERT INTO app.normalizers (name, rule) VALUES ('legacy-roundtrip', :rule)
                RETURNING id
            """), {"rule": legacy_text}).scalar_one()
            connection_id = connection.execute(text("""
                INSERT INTO app.kafka_connections (name, bootstrap_servers)
                VALUES ('roundtrip-connection', ARRAY['broker:9092']) RETURNING id
            """)).scalar_one()
            source_id = connection.execute(text("""
                INSERT INTO app.sources (name, connection_id, normalizer_id, topic_name, is_enabled)
                VALUES ('roundtrip-source', :connection_id, :normalizer_id, 'events', true)
                RETURNING id
            """), {"connection_id": connection_id, "normalizer_id": legacy_id}).scalar_one()
        run_alembic(auth_database_url, "upgrade", "head")
        with auth_database.engine.connect() as connection:
            row = connection.execute(text("SELECT rule, version FROM app.normalizers WHERE id=:id"),
                                     {"id": legacy_id}).one()
            assert row.rule == {"format_version": 0, "legacy_rule_text": legacy_text}
            assert row.version == 1
            assert connection.execute(text("SELECT is_enabled FROM app.sources WHERE id=:id"),
                                      {"id": source_id}).scalar_one() is True
        with auth_database.session_factory() as session:
            session.add(Normalizer(name="v1-roundtrip", rule=simple_normalizer_rule()))
            session.commit()
        run_alembic(auth_database_url, "downgrade", "0003_parsed_logs_search")
        with auth_database.engine.connect() as connection:
            assert connection.execute(text("SELECT rule FROM app.normalizers WHERE id=:id"),
                                      {"id": legacy_id}).scalar_one() == legacy_text
            v1_text = connection.execute(text(
                "SELECT rule FROM app.normalizers WHERE name='v1-roundtrip'"
            )).scalar_one()
            import json
            assert json.loads(v1_text) == simple_normalizer_rule()
    finally:
        run_alembic(auth_database_url, "upgrade", "head")


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
        connection.execute(text("DELETE FROM logs.kafka_operational_events"))
        connection.execute(text("DELETE FROM logs.processing_errors"))
        connection.execute(text("DELETE FROM logs.parsed_logs"))
        connection.execute(text("DELETE FROM logs.processed_kafka_records"))
        connection.execute(text("DELETE FROM app.auth_sessions"))
        connection.execute(text("DELETE FROM app.oidc_role_mappings"))
        connection.execute(text("DELETE FROM app.sources"))
        connection.execute(text("DELETE FROM app.external_connections"))
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
        run_alembic(auth_database_url, "downgrade", "0001_database_foundation")
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
        json={"name": "syslog", "description": "opaque", "rule": simple_normalizer_rule()},
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
        json={"rule": simple_normalizer_rule(), "version": 1},
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
        json={"name": "delete-me", "rule": simple_normalizer_rule()},
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
    assert "/api/v1/kafka-connections" in paths
    assert "/api/v1/sources" in paths
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
        json={"name": name, "description": "test", "rule": simple_normalizer_rule()},
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
            normalizer_name="Legacy test normalizer",
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
        normalizer = Normalizer(name="concurrent-normalizer", rule=simple_normalizer_rule())
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
        json={"name": "conflict-first", "rule": simple_normalizer_rule()},
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
            "rule": simple_normalizer_rule(),
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


class FakeKafkaMetadataClient:
    def __init__(self):
        self.calls = 0
        self.mode = "ok"
        self.topic_names = ("events", "metrics", "__consumer_offsets")
        self.topic_ids = {name: f"id-{name}" for name in self.topic_names}

    def metadata(self, config, timeout):
        self.calls += 1
        if self.mode == "unexpected":
            raise RuntimeError("adapter implementation defect")
        if self.mode in {"unavailable", "timeout"}:
            raise KafkaMetadataError(self.mode)
        return KafkaMetadata(
            broker_count=1,
            topics=tuple(
                KafkaTopic(name=name, partition_count=index + 1, identity=self.topic_ids[name])
                for index, name in enumerate(self.topic_names)
            ),
            latency_ms=1.5,
            cluster_identity=config.bootstrap_servers[0],
        )


class BlockingKafkaMetadataClient(FakeKafkaMetadataClient):
    def __init__(self):
        super().__init__()
        self.block = False
        self.entered = threading.Event()
        self.release = threading.Event()

    def metadata(self, config, timeout):
        if self.block:
            self.entered.set()
            if not self.release.wait(5):
                raise TimeoutError("test barrier timed out")
        return super().metadata(config, timeout)


class ContentionKafkaMetadataClient(FakeKafkaMetadataClient):
    def __init__(self):
        super().__init__()
        self.block = False
        self.barrier = threading.Barrier(3, timeout=5)

    def metadata(self, config, timeout):
        if self.block:
            self.barrier.wait()
        return super().metadata(config, timeout)


def use_fake_kafka(client, fake):
    client.app.dependency_overrides[get_kafka_client] = lambda: fake


@pytest.mark.parametrize("mutation", ["topic", "connection", "normalizer", "bootstrap"])
def test_source_enable_rejects_concurrent_configuration_change(
    client, auth_database, mutation
):
    headers = admin_headers(client, auth_database)
    fake = BlockingKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": f"race-{mutation}", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    alternate = None
    if mutation == "connection":
        alternate = client.post(
            "/api/v1/kafka-connections",
            json={"name": "race-alternate", "bootstrap_servers": ["localhost:9092"]},
            headers=headers,
        ).json()
    normalizer = create_normalizer(client, headers, f"race-{mutation}")
    source = client.post(
        "/api/v1/sources",
        json={
            "name": f"race-{mutation}",
            "connection_id": connection["id"],
            "topic_name": "events",
        },
        headers=headers,
    ).json()
    client.put(
        f"/api/v1/sources/{source['id']}/normalizer",
        json={"normalizer_id": normalizer["id"]},
        headers=headers,
    )
    fake.block = True
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            client.post, f"/api/v1/sources/{source['id']}/enable", headers=headers
        )
        assert fake.entered.wait(5)
        with auth_database.engine.begin() as connection_handle:
            if mutation == "topic":
                connection_handle.execute(
                    text("UPDATE app.sources SET topic_name='metrics' WHERE id=:id"),
                    {"id": source["id"]},
                )
            elif mutation == "connection":
                connection_handle.execute(
                    text("UPDATE app.sources SET connection_id=:connection_id WHERE id=:id"),
                    {"connection_id": alternate["id"], "id": source["id"]},
                )
            elif mutation == "normalizer":
                connection_handle.execute(
                    text("UPDATE app.sources SET normalizer_id=NULL WHERE id=:id"),
                    {"id": source["id"]},
                )
            else:
                connection_handle.execute(
                    text("UPDATE app.kafka_connections SET bootstrap_servers=ARRAY['other:9092'] WHERE id=:id"),
                    {"id": connection["id"]},
                )
        fake.release.set()
        response = future.result(timeout=5)
    assert response.status_code == 409
    assert response.json()["code"] == "source_configuration_changed"
    with auth_database.session_factory() as session:
        persisted = session.get(Source, UUID(source["id"]))
        assert persisted is not None and persisted.is_enabled is False


def test_parallel_enable_and_source_update_contend_without_deadlock(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = ContentionKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "parallel-enable", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    normalizer = create_normalizer(client, headers, "parallel-enable")
    source = client.post(
        "/api/v1/sources",
        json={"name": "parallel-enable", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    client.put(
        f"/api/v1/sources/{source['id']}/normalizer",
        json={"normalizer_id": normalizer["id"]},
        headers=headers,
    )
    fake.block = True
    with ThreadPoolExecutor(max_workers=2) as pool:
        enable_future = pool.submit(
            client.post, f"/api/v1/sources/{source['id']}/enable", headers=headers
        )
        update_future = pool.submit(
            client.patch,
            f"/api/v1/sources/{source['id']}",
            json={"topic_name": "metrics"},
            headers=headers,
        )
        fake.barrier.wait()
        enabled = enable_future.result(timeout=5)
        updated = update_future.result(timeout=5)
    assert sorted([enabled.status_code, updated.status_code]) == [200, 409]
    conflict = enabled if enabled.status_code == 409 else updated
    assert conflict.json()["code"] in {"source_configuration_changed", "topic_identity_changed"}
    persisted = client.get(f"/api/v1/sources/{source['id']}").json()
    assert persisted["topic_name"] == "events"
    with auth_database.session_factory() as session:
        assert session.get(Source, UUID(source["id"])).kafka_topic_identity == "id-events"


def test_source_update_returns_conflict_when_source_deleted_during_kafka_check(
    client, auth_database
):
    headers = admin_headers(client, auth_database)
    fake = BlockingKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "deleted-update", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    source = client.post(
        "/api/v1/sources",
        json={"name": "deleted-update", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    fake.block = True
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            client.patch,
            f"/api/v1/sources/{source['id']}",
            json={"topic_name": "metrics"},
            headers=headers,
        )
        assert fake.entered.wait(5)
        with auth_database.engine.begin() as connection_handle:
            connection_handle.execute(
                text("DELETE FROM app.sources WHERE id=:id"), {"id": source["id"]}
            )
        fake.release.set()
        response = future.result(timeout=5)
    assert response.status_code == 409
    assert response.json()["code"] == "source_configuration_changed"
    assert client.get(f"/api/v1/sources/{source['id']}").status_code == 404


def test_source_create_rejects_concurrent_connection_change(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = BlockingKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "create-race", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    fake.block = True
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            client.post,
            "/api/v1/sources",
            json={"name": "create-race", "connection_id": connection["id"], "topic_name": "events"},
            headers=headers,
        )
        assert fake.entered.wait(5)
        with auth_database.engine.begin() as connection_handle:
            connection_handle.execute(
                text("UPDATE app.kafka_connections SET bootstrap_servers=ARRAY['other:9092'] WHERE id=:id"),
                {"id": connection["id"]},
            )
        fake.release.set()
        response = future.result(timeout=5)
    assert response.status_code == 409
    assert response.json()["code"] == "source_configuration_changed"
    assert client.get("/api/v1/sources", params={"connection_id": connection["id"]}).json()["total"] == 0


def test_source_update_rejects_concurrent_target_connection_change(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = BlockingKafkaMetadataClient()
    use_fake_kafka(client, fake)
    original = client.post(
        "/api/v1/kafka-connections",
        json={"name": "update-race-old", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    target = client.post(
        "/api/v1/kafka-connections",
        json={"name": "update-race-new", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    source = client.post(
        "/api/v1/sources",
        json={"name": "update-race", "connection_id": original["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    fake.block = True
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            client.patch,
            f"/api/v1/sources/{source['id']}",
            json={"connection_id": target["id"], "topic_name": "metrics"},
            headers=headers,
        )
        assert fake.entered.wait(5)
        with auth_database.engine.begin() as connection_handle:
            connection_handle.execute(
                text("UPDATE app.kafka_connections SET bootstrap_servers=ARRAY['other:9092'] WHERE id=:id"),
                {"id": target["id"]},
            )
        fake.release.set()
        response = future.result(timeout=5)
    assert response.status_code == 409
    persisted = client.get(f"/api/v1/sources/{source['id']}").json()
    assert persisted["connection_id"] == original["id"]
    assert persisted["topic_name"] == "events"


def test_kafka_connections_topics_and_source_lifecycle(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    created = client.post(
        "/api/v1/kafka-connections",
        json={
            "id": "66666666-6666-4666-8666-666666666666",
            "name": "local-kafka",
            "bootstrap_servers": ["localhost:9092"],
        },
        headers=headers,
    )
    assert created.status_code == 201
    connection = created.json()
    assert connection["id"] == "66666666-6666-4666-8666-666666666666"
    assert connection["security_protocol"] == "PLAINTEXT" and connection["extra_config"] == {}
    connection_id = connection["id"]
    assert client.get(f"/api/v1/kafka-connections/{connection_id}").status_code == 200
    assert client.get("/api/v1/kafka-connections", params={"q": "local"}).json()["total"] == 1
    patched_connection = client.patch(
        f"/api/v1/kafka-connections/{connection_id}",
        json={"name": "local-kafka-renamed"},
        headers=headers,
    )
    assert patched_connection.status_code == 200
    checked = client.post(f"/api/v1/kafka-connections/{connection_id}/test", headers=headers)
    assert checked.status_code == 200
    assert checked.json() == {
        "status": "ok", "broker_count": 1, "topic_count": 3, "latency_ms": 1.5
    }
    topics = client.get(f"/api/v1/kafka-connections/{connection_id}/topics")
    assert topics.json()["items"] == [
        {"name": "events", "partition_count": 1, "is_registered": False},
        {"name": "metrics", "partition_count": 2, "is_registered": False},
    ]
    assert topics.json()["total"] == 2
    assert client.get(
        f"/api/v1/kafka-connections/{connection_id}/topics?include_internal=true"
    ).json()["total"] == 3
    assert client.get("/api/v1/kafka-connections?limit=1").json()["total"] == 1
    assert fake.calls == 3

    normalizer = create_normalizer(client, headers, "source-normalizer")
    source = client.post(
        "/api/v1/sources",
        json={
            "id": "77777777-7777-4777-8777-777777777777",
            "name": "events-source",
            "connection_id": connection_id,
            "topic_name": "events",
        },
        headers=headers,
    )
    assert source.status_code == 201
    source_data = source.json()
    assert source_data["is_enabled"] is False and source_data["normalizer_id"] is None
    source_id = source_data["id"]
    assert client.get(f"/api/v1/sources/{source_id}").status_code == 200
    assert client.get("/api/v1/sources", params={"connection_id": connection_id}).json()["total"] == 1
    duplicate = client.post(
        "/api/v1/sources",
        json={"name": "duplicate", "connection_id": connection_id, "topic_name": "events"},
        headers=headers,
    )
    assert duplicate.status_code == 409 and duplicate.json()["code"] == "source_topic_conflict"
    assert client.post(f"/api/v1/sources/{source_id}/enable", headers=headers).status_code == 409
    assigned = client.put(
        f"/api/v1/sources/{source_id}/normalizer",
        json={"normalizer_id": normalizer["id"]},
        headers=headers,
    )
    assert assigned.status_code == 200
    enabled = client.post(f"/api/v1/sources/{source_id}/enable", headers=headers)
    assert enabled.status_code == 200 and enabled.json()["is_enabled"] is True
    blocked_connection = client.patch(
        f"/api/v1/kafka-connections/{connection_id}",
        json={"bootstrap_servers": ["other:9092"]},
        headers=headers,
    )
    assert blocked_connection.status_code == 409
    assert blocked_connection.json()["code"] == "source_enabled"
    renamed = client.patch(
        f"/api/v1/sources/{source_id}", json={"name": "renamed-source"}, headers=headers
    )
    assert renamed.status_code == 200
    assert client.patch(
        f"/api/v1/sources/{source_id}",
        json={"topic_name": "metrics"},
        headers=headers,
    ).status_code == 409
    calls_before_disable = fake.calls
    disabled = client.post(f"/api/v1/sources/{source_id}/disable", headers=headers)
    assert disabled.status_code == 200 and disabled.json()["is_enabled"] is False
    assert fake.calls == calls_before_disable
    assert client.put(
        f"/api/v1/sources/{source_id}/normalizer", json={"normalizer_id": None}, headers=headers
    ).status_code == 200
    assert client.delete(f"/api/v1/sources/{source_id}", headers=headers).status_code == 204


def test_diagnostics_read_access_raw_payload_boundary_and_openapi(client, auth_database):
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/v1/diagnostics/sources" in paths
    assert "/api/v1/diagnostics/processing-errors/{error_id}/raw" in paths
    assert client.get("/api/v1/diagnostics/overview").status_code == 401

    add_local_user(auth_database)
    assert login(client).status_code == 200
    assert client.get("/api/v1/diagnostics/overview").status_code == 200
    assert client.get("/api/v1/diagnostics/processing-errors/not-a-uuid").status_code == 422
    guest_raw = client.get(f"/api/v1/diagnostics/processing-errors/{uuid4()}/raw")
    assert guest_raw.status_code == 403
    assert guest_raw.json()["code"] == "permission_denied"

    add_admin_user(auth_database)
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    admin_login = client.post(
        "/api/v1/auth/local/login",
        json={"username": "admin", "password": "correct-password"},
        headers={"X-CSRF-Token": csrf},
    )
    assert admin_login.status_code == 200
    headers = {"X-CSRF-Token": admin_login.json()["csrf_token"]}
    missing = client.get(f"/api/v1/diagnostics/processing-errors/{uuid4()}/raw", headers=headers)
    assert missing.status_code == 404
    assert missing.json()["code"] == "processing_error_not_found"


def test_diagnostics_error_and_event_snapshots_survive_source_deletion(client, auth_database):
    now = datetime.now(UTC)
    source_id = uuid4()
    connection_id = uuid4()
    record_id = uuid4()
    error_id = uuid4()
    payload = b"\xff\x00diagnostic-payload"
    with auth_database.session_factory() as session:
        session.add(KafkaConnection(
            id=connection_id, name="diagnostic-connection", bootstrap_servers=["localhost:9092"],
            security_protocol="PLAINTEXT", extra_config={},
        ))
        session.flush()
        session.add(Source(
            id=source_id, name="diagnostic-source", connection_id=connection_id,
            topic_name="diagnostic-topic", kafka_topic_identity="topic-id",
            is_enabled=False, is_archived=True,
        ))
        session.add(ProcessedKafkaRecord(
            id=record_id, connection_id=connection_id, connection_identity=connection_id,
            kafka_topic_identity="topic-id", source_id=source_id, normalizer_id=None,
            kafka_topic="diagnostic-topic", kafka_partition=2, kafka_offset=17,
            result_status="failed", backend_received_at=now, backend_processed_at=now,
        ))
        session.flush()
        session.add(ProcessingError(
            id=error_id, processed_record_id=record_id, connection_id=connection_id,
            source_id=source_id, source_identity=source_id, normalizer_id=None,
            connection_name="diagnostic-connection", source_name="diagnostic-source",
            normalizer_name="deleted-normalizer", normalizer_version=3,
            kafka_topic="diagnostic-topic", kafka_partition=2, kafka_offset=17,
            raw_payload=payload, stage="decode", diagnostics=[{"code": "invalid_utf8"}],
            fluent_bit_collected_at=None, backend_received_at=now, backend_processed_at=now,
        ))
        session.add(KafkaOperationalEvent(
            kind="retention_gap", reason_code="retention_gap_detected", source_id=source_id,
            source_identity=source_id, source_name="diagnostic-source",
            connection_id=connection_id, connection_identity=connection_id,
            connection_name="diagnostic-connection", cluster_identity="cluster-id",
            topic_name="diagnostic-topic", old_topic_identity="topic-id",
            new_topic_identity=None, kafka_partition=2, offset_start=18, offset_end=23,
        ))
        session.commit()

    assert client.get(f"/api/v1/diagnostics/processing-errors?source_id={source_id}").status_code == 401
    add_local_user(auth_database)
    assert login(client).status_code == 200
    source_list = client.get("/api/v1/diagnostics/sources", params={"limit": 100})
    assert source_list.status_code == 200
    listed_source = next(
        item for item in source_list.json()["items"] if item["source_id"] == str(source_id)
    )
    assert listed_source["connection_id"] == str(connection_id)
    assert listed_source["connection_name"] == "diagnostic-connection"
    source_detail = client.get(f"/api/v1/diagnostics/sources/{source_id}")
    assert source_detail.status_code == 200
    assert source_detail.json()["connection_name"] == "diagnostic-connection"
    errors = client.get("/api/v1/diagnostics/processing-errors", params={"source_id": str(source_id)})
    assert errors.status_code == 200 and errors.json()["total"] == 1
    assert "raw_payload" not in errors.json()["items"][0]
    assert "diagnostic-payload" not in errors.text
    guest_raw = client.get(f"/api/v1/diagnostics/processing-errors/{error_id}/raw")
    assert guest_raw.status_code == 403

    overview = client.get("/api/v1/diagnostics/overview")
    assert overview.status_code == 200
    assert overview.json()["worker_counts_scope"] == "application_process"
    assert overview.json()["recent_errors"][0]["connection_name"] == "diagnostic-connection"
    assert overview.json()["recent_events"][0]["connection_name"] == "diagnostic-connection"

    add_admin_user(auth_database)
    csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
    admin_login = client.post(
        "/api/v1/auth/local/login",
        json={"username": "admin", "password": "correct-password"},
        headers={"X-CSRF-Token": csrf},
    )
    assert admin_login.status_code == 200
    headers = {"X-CSRF-Token": admin_login.json()["csrf_token"]}
    detail = client.get(f"/api/v1/diagnostics/processing-errors/{error_id}", headers=headers)
    assert detail.status_code == 200 and "raw_payload" not in detail.json()
    raw = client.get(f"/api/v1/diagnostics/processing-errors/{error_id}/raw", headers=headers)
    assert raw.status_code == 200 and base64.b64decode(raw.json()["data"]) == payload
    events = client.get("/api/v1/diagnostics/kafka-events", params={"source_id": str(source_id)}, headers=headers)
    event = events.json()["items"][0]
    assert events.json()["total"] == 1
    assert event["offset_start"] == 18 and event["offset_end"] == 23
    assert event["offset_range_semantics"] == "unavailable_kafka_positions_not_message_count"

    with auth_database.session_factory() as session:
        session.delete(session.get(Source, source_id))
        session.commit()
    detached_error = client.get(f"/api/v1/diagnostics/processing-errors/{error_id}", headers=headers)
    detached_event = client.get("/api/v1/diagnostics/kafka-events",
        params={"source_id": str(source_id)}, headers=headers)
    assert detached_error.json()["source_id"] == str(source_id)
    assert detached_event.json()["total"] == 1


def test_diagnostics_overview_aggregates_large_source_set_without_loading_source_rows(
    auth_database,
):
    connection_id = uuid4()
    with auth_database.session_factory() as session:
        session.add(KafkaConnection(
            id=connection_id, name="overview-connection", bootstrap_servers=["localhost:9092"],
            security_protocol="PLAINTEXT", extra_config={},
        ))
        session.flush()
        sources = [
            Source(
                id=uuid4(), name=f"overview-source-{index}", connection_id=connection_id,
                topic_name=f"overview-topic-{index}",
                is_enabled=index in (0, 1), is_archived=False,
            )
            for index in range(1000)
        ]
        session.add_all(sources)
        session.commit()

    running_id, stopped_id = sources[0].id, sources[1].id

    class OverviewSupervisor:
        def diagnostic_overview_snapshots(self):
            return {
                running_id: (ConsumerWorkerState("waiting"), False, True),
                stopped_id: (ConsumerWorkerState("waiting"), True, True),
            }

    statements = []

    def record_query(_conn, _cursor, statement, _parameters, _context, _many):
        if "app.sources" in statement:
            statements.append(" ".join(statement.lower().split()))

    event.listen(auth_database.engine, "before_cursor_execute", record_query)
    try:
        with auth_database.session_factory() as session:
            result = DiagnosticsServiceImpl(
                SqlAlchemyDiagnosticsRepository(session), OverviewSupervisor()
            ).overview(10)
    finally:
        event.remove(auth_database.engine, "before_cursor_execute", record_query)

    assert result.registered_sources == 1000
    assert result.enabled_sources == 2
    assert result.worker_counts == {
        "running": 1, "retrying": 0, "stopped": 1,
        "not_running": 998, "unknown": 0,
    }
    assert result.worker_counts_scope == "application_process"
    assert any("count(app.sources.id)" in statement for statement in statements)
    assert any("select app.sources.id from app.sources" in statement for statement in statements)
    assert not any("select app.sources.id, app.sources.name" in statement for statement in statements)


def test_0008_permission_migration_round_trip_preserves_role_configuration(auth_database_url):
    run_alembic(auth_database_url, "upgrade", "head")
    url = make_url(auth_database_url).set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    with psycopg.connect(url) as connection:
        before = {
            row[0]: (row[1], set(row[2]))
            for row in connection.execute(
                "SELECT id, name, permissions FROM app.roles WHERE id IN (%s, %s)",
                (ADMIN_ID, GUEST_ID),
            )
        }
    assert "processing_errors.raw.read" in before[ADMIN_ID][1]
    assert "processing_errors.raw.read" not in before[GUEST_ID][1]
    run_alembic(auth_database_url, "downgrade", "0007_kafka_generations_and_gaps")
    with psycopg.connect(url) as connection:
        downgraded = {
            row[0]: (row[1], set(row[2]))
            for row in connection.execute(
                "SELECT id, name, permissions FROM app.roles WHERE id IN (%s, %s)",
                (ADMIN_ID, GUEST_ID),
            )
        }
    for role_id in (ADMIN_ID, GUEST_ID):
        assert downgraded[role_id][0] == before[role_id][0]
        assert downgraded[role_id][1] == before[role_id][1] - {"processing_errors.raw.read"}
    run_alembic(auth_database_url, "upgrade", "head")
    with psycopg.connect(url) as connection:
        upgraded = connection.execute(
            "SELECT permissions FROM app.roles WHERE id = %s", (ADMIN_ID,)
        ).fetchone()[0]
    assert "processing_errors.raw.read" in upgraded


def test_archived_topic_generation_requires_explicit_new_source(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "generation", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    normalizer = create_normalizer(client, headers, "generation")
    old = client.post(
        "/api/v1/sources",
        json={"name": "old", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    assert client.put(
        f"/api/v1/sources/{old['id']}/normalizer",
        json={"normalizer_id": normalizer["id"]}, headers=headers,
    ).status_code == 200
    assert client.post(f"/api/v1/sources/{old['id']}/enable", headers=headers).status_code == 200
    fake.topic_ids["events"] = "new-events-id"
    with auth_database.session_factory() as session:
        assert SqlAlchemyConsumerStateRepository(session).archive_recreated(
            UUID(old["id"]), "id-events", "new-events-id"
        )
        session.commit()
    topics = client.get(f"/api/v1/kafka-connections/{connection['id']}/topics").json()
    assert next(item for item in topics["items"] if item["name"] == "events")["is_registered"] is False
    old_state = client.get(f"/api/v1/sources/{old['id']}").json()
    assert old_state["is_archived"] is True and old_state["is_enabled"] is False
    for path, method, payload in (
        (f"/api/v1/sources/{old['id']}", client.patch, {"topic_name": "metrics"}),
        (f"/api/v1/sources/{old['id']}/normalizer", client.put, {"normalizer_id": None}),
    ):
        response = method(path, json=payload, headers=headers)
        assert response.status_code == 409 and response.json()["code"] == "source_archived"
    blocked = client.post(f"/api/v1/sources/{old['id']}/enable", headers=headers)
    assert blocked.status_code == 409 and blocked.json()["code"] == "source_archived"
    new_response = client.post(
        "/api/v1/sources",
        json={"name": "new", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    )
    assert new_response.status_code == 201
    new = new_response.json()
    assert new["id"] != old["id"] and not new["is_enabled"] and new["normalizer_id"] is None
    assert client.post(f"/api/v1/sources/{new['id']}/enable", headers=headers).status_code == 409
    assert client.put(
        f"/api/v1/sources/{new['id']}/normalizer",
        json={"normalizer_id": normalizer["id"]}, headers=headers,
    ).status_code == 200
    assert client.post(f"/api/v1/sources/{new['id']}/enable", headers=headers).status_code == 200
    topics = client.get(f"/api/v1/kafka-connections/{connection['id']}/topics").json()
    assert next(item for item in topics["items"] if item["name"] == "events")["is_registered"] is True


def test_disabled_recreated_source_is_archived_on_explicit_registration(client, auth_database):
    from app.models import KafkaOperationalEvent, Source

    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "disabled-generation", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    old = client.post(
        "/api/v1/sources",
        json={"name": "old-disabled", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    assert old["is_enabled"] is False
    fake.topic_ids["events"] = "replacement-id"
    topics = client.get(f"/api/v1/kafka-connections/{connection['id']}/topics").json()
    assert next(item for item in topics["items"] if item["name"] == "events")["is_registered"] is False
    new_response = client.post(
        "/api/v1/sources",
        json={"name": "new-disabled", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    )
    assert new_response.status_code == 201, new_response.text
    new = new_response.json()
    assert new["id"] != old["id"] and not new["is_enabled"] and new["normalizer_id"] is None
    with auth_database.session_factory() as session:
        previous = session.get(Source, UUID(old["id"]))
        assert previous.is_archived and not previous.is_enabled
        event = session.scalar(select(KafkaOperationalEvent).where(
            KafkaOperationalEvent.source_identity == UUID(old["id"]),
            KafkaOperationalEvent.kind == "topic_recreated",
        ))
        assert event is not None and event.new_topic_identity == "replacement-id"
    blocked = client.post(f"/api/v1/sources/{old['id']}/enable", headers=headers)
    assert blocked.status_code == 409 and blocked.json()["code"] == "source_archived"


def test_patch_cannot_adopt_recreated_topic_without_history(client, auth_database):
    from app.models import KafkaOperationalEvent, Source

    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "patch-generation", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    old = client.post(
        "/api/v1/sources",
        json={"name": "old-patch", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    fake.topic_ids["events"] = "replacement-id"
    patch = client.patch(
        f"/api/v1/sources/{old['id']}", json={"topic_name": "events"}, headers=headers
    )
    assert patch.status_code == 409 and patch.json()["code"] == "topic_identity_changed"
    renamed = client.patch(
        f"/api/v1/sources/{old['id']}", json={"name": "old-renamed"}, headers=headers
    )
    assert renamed.status_code == 200
    with auth_database.session_factory() as session:
        previous = session.get(Source, UUID(old["id"]))
        assert previous.kafka_topic_identity == "id-events"
        assert previous.name == "old-renamed" and not previous.is_archived
        assert session.scalar(select(KafkaOperationalEvent).where(
            KafkaOperationalEvent.source_identity == UUID(old["id"])
        )) is None
    replacement = client.post(
        "/api/v1/sources",
        json={"name": "new-patch", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    )
    assert replacement.status_code == 201, replacement.text
    assert replacement.json()["id"] != old["id"]
    with auth_database.session_factory() as session:
        previous = session.get(Source, UUID(old["id"]))
        assert previous.is_archived and previous.kafka_topic_identity == "id-events"
        event = session.scalar(select(KafkaOperationalEvent).where(
            KafkaOperationalEvent.source_identity == UUID(old["id"]),
            KafkaOperationalEvent.kind == "topic_recreated",
        ))
        assert event is not None and event.new_topic_identity == "replacement-id"


def test_replacement_registration_rolls_back_archive_on_storage_failure(
    client, auth_database, monkeypatch
):
    from app.models import KafkaOperationalEvent, Source
    from app.repositories.sqlalchemy.connections import SqlAlchemySourceRepository

    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "rollback-generation", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    old = client.post(
        "/api/v1/sources",
        json={"name": "rollback-old", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    fake.topic_ids["events"] = "replacement-id"
    original = SqlAlchemySourceRepository.archive_recreated

    def fail_after_archive(self, source_id, old_id, new_id):
        assert original(self, source_id, old_id, new_id)
        raise SQLAlchemyError("simulated transaction failure")

    monkeypatch.setattr(SqlAlchemySourceRepository, "archive_recreated", fail_after_archive)
    with pytest.raises(SQLAlchemyError, match="simulated transaction failure"):
        client.post(
            "/api/v1/sources",
            json={"name": "rollback-new", "connection_id": connection["id"], "topic_name": "events"},
            headers=headers,
        )
    with auth_database.session_factory() as session:
        old_source = session.get(Source, UUID(old["id"]))
        assert not old_source.is_archived
        assert session.scalar(select(Source).where(Source.name == "rollback-new")) is None
        assert session.scalar(select(KafkaOperationalEvent).where(
            KafkaOperationalEvent.source_identity == UUID(old["id"]),
        )) is None


def test_same_cluster_second_connection_cannot_register_source(client, auth_database):
    from dataclasses import replace

    headers = admin_headers(client, auth_database)

    class SameClusterClient(FakeKafkaMetadataClient):
        def metadata(self, config, timeout):
            return replace(super().metadata(config, timeout), cluster_identity="shared-cluster")

    use_fake_kafka(client, SameClusterClient())
    first = client.post(
        "/api/v1/kafka-connections",
        json={"name": "first-cluster-address", "bootstrap_servers": ["first:9092"]},
        headers=headers,
    ).json()
    second = client.post(
        "/api/v1/kafka-connections",
        json={"name": "second-cluster-address", "bootstrap_servers": ["second:9092"]},
        headers=headers,
    ).json()
    first_source = client.post(
        "/api/v1/sources",
        json={"name": "first-source", "connection_id": first["id"], "topic_name": "events"},
        headers=headers,
    )
    assert first_source.status_code == 201
    duplicate = client.post(
        "/api/v1/sources",
        json={"name": "duplicate", "connection_id": second["id"], "topic_name": "events"},
        headers=headers,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "cluster_connection_conflict"
    blocked = client.patch(
        f"/api/v1/kafka-connections/{first['id']}",
        json={"bootstrap_servers": ["third:9092"]}, headers=headers,
    )
    assert blocked.status_code == 409 and blocked.json()["code"] == "source_exists"
    assert client.delete(f"/api/v1/sources/{first_source.json()['id']}", headers=headers).status_code == 204
    assert client.patch(
        f"/api/v1/kafka-connections/{first['id']}",
        json={"bootstrap_servers": ["third:9092"]}, headers=headers,
    ).status_code == 200
    assert client.post(
        "/api/v1/sources",
        json={"name": "second-source", "connection_id": second["id"], "topic_name": "events"},
        headers=headers,
    ).status_code == 201


def test_kafka_source_validation_permissions_and_safe_failures(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    invalid_connection = {
        "name": "invalid-kafka",
        "bootstrap_servers": ["https://localhost:9092"],
    }
    for body in (
        invalid_connection,
        {"name": "invalid-port", "bootstrap_servers": ["localhost:0"]},
        {"name": "duplicate-servers", "bootstrap_servers": ["localhost:9092", "localhost:9092"]},
        {"name": "secure", "bootstrap_servers": ["localhost:9092"], "security_protocol": "SASL_SSL"},
        {"name": "unknown", "bootstrap_servers": ["localhost:9092"], "extra_config": {}},
    ):
        assert client.post("/api/v1/kafka-connections", json=body, headers=headers).status_code == 422
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "error-kafka", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    assert client.patch(
        f"/api/v1/kafka-connections/{connection['id']}", json={"name": "no-csrf"}
    ).status_code == 403
    assert client.post(
        "/api/v1/sources",
        json={"name": "no-csrf", "connection_id": connection["id"], "topic_name": "events"},
    ).status_code == 403
    fake.mode = "unavailable"
    unavailable = client.post(
        f"/api/v1/kafka-connections/{connection['id']}/test", headers=headers
    )
    assert unavailable.status_code == 503
    assert unavailable.json()["code"] == "kafka_unavailable"
    fake.mode = "timeout"
    timeout = client.get(f"/api/v1/kafka-connections/{connection['id']}/topics")
    assert timeout.status_code == 504 and timeout.json()["code"] == "kafka_timeout"
    fake.mode = "ok"
    unknown_topic = client.post(
        "/api/v1/sources",
        json={
            "name": "unknown-topic-source",
            "connection_id": connection["id"],
            "topic_name": "missing",
        },
        headers=headers,
    )
    assert unknown_topic.status_code == 404 and unknown_topic.json()["code"] == "topic_not_found"
    fake.mode = "unexpected"
    safe_client = TestClient(
        create_app(
            Settings(
                environment="test",
                database_url=auth_database.engine.url.render_as_string(hide_password=False),
            ),
            database=auth_database,
            kafka_client=fake,
        ),
        raise_server_exceptions=False,
    )
    try:
        assert login(safe_client, "admin").status_code == 200
        safe_headers = {
            "X-CSRF-Token": safe_client.get("/api/v1/auth/session").json()["csrf_token"]
        }
        internal = safe_client.post(
            f"/api/v1/kafka-connections/{connection['id']}/test", headers=safe_headers
        )
        assert internal.status_code == 500
        assert internal.json()["code"] == "internal_error"
        assert "request_id" in internal.json()
    finally:
        safe_client.close()
    fake.mode = "ok"
    add_local_user(auth_database)
    guest = TestClient(create_app(Settings(
        environment="test", database_url=auth_database.engine.url.render_as_string(hide_password=False)
    ), database=auth_database, kafka_client=fake))
    try:
        assert login(guest).status_code == 200
        guest_csrf = guest.get("/api/v1/auth/session").json()["csrf_token"]
        guest_headers = {"X-CSRF-Token": guest_csrf}
        assert guest.get("/api/v1/kafka-connections").status_code == 200
        assert guest.post(
            f"/api/v1/kafka-connections/{connection['id']}/test", headers=guest_headers
        ).status_code == 200
        assert guest.get(f"/api/v1/kafka-connections/{connection['id']}/topics").status_code == 200
        assert guest.get("/api/v1/sources").status_code == 200
        forbidden = guest.delete(
            f"/api/v1/kafka-connections/{connection['id']}", headers=guest_headers
        )
        assert forbidden.status_code == 403 and forbidden.json()["code"] == "permission_denied"
    finally:
        guest.close()


def test_task4_openapi_has_no_future_ingestion_routes(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/v1/kafka-connections" in paths and "/api/v1/sources" in paths
    assert not any("consumer" in path for path in paths)
    assert "/api/v1/parsed-logs/search" in paths
    assert "/api/v1/parsed-logs/{log_id}" in paths
    assert "post" not in paths["/api/v1/parsed-logs/{log_id}"]


def test_connection_delete_cascades_source_and_preserves_parsed_log(client, auth_database):
    headers = admin_headers(client, auth_database)
    fake = FakeKafkaMetadataClient()
    use_fake_kafka(client, fake)
    connection = client.post(
        "/api/v1/kafka-connections",
        json={"name": "cascade-api", "bootstrap_servers": ["localhost:9092"]},
        headers=headers,
    ).json()
    source = client.post(
        "/api/v1/sources",
        json={"name": "cascade-api", "connection_id": connection["id"], "topic_name": "events"},
        headers=headers,
    ).json()
    with auth_database.session_factory() as session:
        timestamp = datetime.now(UTC)
        parsed = ParsedLog(
            source_id=UUID(source["id"]),
            connection_id=UUID(connection["id"]),
            normalizer_version=1,
            normalizer_name="Legacy test normalizer",
            source_name=source["name"],
            connection_name=connection["name"],
            kafka_topic=source["topic_name"],
            kafka_partition=0,
            kafka_offset=1,
            deduplication_key=f"cascade-{uuid4()}",
            fluent_bit_collected_at=timestamp,
            backend_received_at=timestamp,
            backend_processed_at=timestamp,
            raw="raw",
            ecs_data={},
        )
        session.add(parsed)
        session.commit()
        parsed_id = parsed.id
    assert client.delete(
        f"/api/v1/kafka-connections/{connection['id']}", headers=headers
    ).status_code == 204
    assert client.get(f"/api/v1/sources/{source['id']}").status_code == 404
    with auth_database.session_factory() as session:
        persisted = session.get(ParsedLog, parsed_id)
        assert persisted is not None
        assert persisted.source_id is None and persisted.connection_id is None


def add_parsed_log(
    database, *, timestamp=None, collected_at=None, created_at=None, source_id=None, connection_id=None,
    topic="events", partition=0, raw="raw event", ecs_data=None,
):
    timestamp = timestamp or datetime.now(UTC)
    collected_at = collected_at or timestamp
    with database.session_factory() as session:
        log = ParsedLog(
            source_id=source_id,
            connection_id=connection_id,
            normalizer_version=1,
            normalizer_name="Normalizer snapshot",
            source_name="Source snapshot",
            connection_name="Connection snapshot",
            kafka_topic=topic,
            kafka_partition=partition,
            kafka_offset=1,
            deduplication_key=f"test-{uuid4()}",
            fluent_bit_collected_at=collected_at,
            backend_received_at=timestamp,
            backend_processed_at=timestamp,
            created_at=created_at,
            raw=raw,
            ecs_data=ecs_data or {"ecs": {"version": "9.4.0"}, "event": {"name": "login"}},
        )
        session.add(log)
        session.commit()
        return log.id


def events_headers(client):
    session = client.get("/api/v1/auth/session").json()
    return {"X-CSRF-Token": session["csrf_token"]}


def add_source_pair(database):
    with database.session_factory() as session:
        connection = KafkaConnection(
            name=f"search-{uuid4()}", bootstrap_servers=["localhost:9092"]
        )
        session.add(connection)
        session.flush()
        sources = [
            Source(
                name=f"source-{uuid4()}",
                connection_id=connection.id,
                topic_name=f"topic-{index}-{uuid4()}",
            )
            for index in range(2)
        ]
        session.add_all(sources)
        session.commit()
        return connection.id, [(source.id, source.topic_name) for source in sources]


def test_ecs_catalog_is_vendored_complete_and_offline():
    catalog = PackagedEcsCatalog.load()
    assert catalog.version == "9.4.0"
    assert len(catalog.fields) > 2_000
    assert {
        "@timestamp", "message", "ecs.version", "event.action",
        "host.name", "user.name", "source.ip",
    } <= set(catalog.fields)
    assert "event.name" not in catalog.fields
    assert catalog.fields["@timestamp"].type == "date"
    assert catalog.fields["source.ip"].type == "ip"
    assert catalog.fields["message"].type == "match_only_text"
    assert catalog.fields["@timestamp"].field_set == "base"
    assert catalog.fields["message"].field_set == "base"
    assert catalog.fields["client.as.number"].field_set == "as"
    assert catalog.fields["source.ip"].field_set == "source"
    assert catalog.fields["event.action"].type == "keyword"
    assert catalog.provenance.sha256
    assert not any("path" in key.lower() for key in catalog.provenance.__dict__)


def test_create_app_accepts_ecs_catalog_without_loading_packaged_artifact(
    auth_settings, auth_database, monkeypatch
):
    fake_catalog = PackagedEcsCatalog.load()
    loader = Mock(return_value=fake_catalog)
    monkeypatch.setattr(PackagedEcsCatalog, "load", loader)
    default_app = create_app(auth_settings, database=auth_database)
    assert default_app.state.ecs_catalog is fake_catalog
    loader.assert_called_once_with()
    loader.reset_mock()
    app = create_app(auth_settings, database=auth_database, ecs_catalog=fake_catalog)
    assert app.state.ecs_catalog is fake_catalog
    loader.assert_not_called()


def test_ecs_catalog_api_auth_search_pagination_and_not_found(client, auth_database):
    assert client.get("/api/v1/ecs/schema").status_code == 401
    add_local_user(auth_database)
    response = login(client)
    assert response.status_code == 200
    schema = client.get("/api/v1/ecs/schema")
    assert schema.status_code == 200 and schema.json()["version"] == "9.4.0"
    fields = client.get("/api/v1/ecs/fields", params={"q": "event.action", "limit": 1})
    assert fields.status_code == 200
    assert fields.json()["items"][0]["name"] == "event.action"
    assert fields.json()["total"] >= 1
    assert client.get("/api/v1/ecs/fields/@timestamp").json()["type"] == "date"
    assert client.get("/api/v1/ecs/fields/@timestamp").json()["field_set"] == "base"
    assert client.get("/api/v1/ecs/fields/message").json()["field_set"] == "base"
    assert client.get("/api/v1/ecs/fields/client.as.number").json()["field_set"] == "as"
    assert client.get("/api/v1/ecs/fields/source.ip").json()["field_set"] == "source"
    missing = client.get("/api/v1/ecs/fields/not.a.real.ecs.field")
    assert missing.status_code == 404
    assert missing.json()["code"] == "ecs_field_not_found"
    packaged = client.app.state.ecs_catalog
    replacement = PackagedEcsCatalog(packaged.fields, packaged.provenance)
    client.app.dependency_overrides[get_ecs_catalog] = lambda: replacement
    assert client.get("/api/v1/ecs/fields/event.action").status_code == 200
    openapi = client.app.openapi()
    assert "/api/v1/parsed-logs/actions/delete" in openapi["paths"]
    assert "post" not in openapi["paths"]["/api/v1/parsed-logs/{log_id}"]
    assert "422" in openapi["paths"]["/api/v1/parsed-logs/search"]["post"]["responses"]


def test_parsed_log_search_filters_signed_cursor_and_detail(client, auth_database):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    class CountingCursorCodec:
        def __init__(self):
            self.delegate = SignedParsedLogCursorCodec(
                client.app.state.settings.oidc_state_secret.get_secret_value()
            )
            self.decoded = 0
            self.encoded = 0

        def decode(self, token, body):
            self.decoded += 1
            return self.delegate.decode(token, body)

        def encode(self, processed_at, log_id, snapshot_boundary, body):
            self.encoded += 1
            return self.delegate.encode(processed_at, log_id, snapshot_boundary, body)

    cursor_codec = CountingCursorCodec()
    client.app.dependency_overrides[get_cursor_codec] = lambda: cursor_codec
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    ids = [
        add_parsed_log(
            auth_database,
            timestamp=timestamp,
            raw=f"authentication failure {index}",
            ecs_data={"ecs": {"version": "9.4.0"}, "event": {"action": "login"}},
        )
        for index in range(3)
    ]
    request = {
        "limit": 2,
        "raw_query": "  failure  ",
        "ecs_filters": [{"field": "event.action", "operator": "eq", "value": "login"}],
    }
    response = client.post(
        "/api/v1/parsed-logs/search", json=request, headers=events_headers(client)
    )
    assert response.status_code == 200, response.text
    first_page = response.json()
    assert first_page["has_more"] is True
    assert cursor_codec.decoded == 1 and cursor_codec.encoded == 1
    assert all(len(item["raw_preview"]) <= 500 for item in first_page["items"])
    time.sleep(0.01)
    inserted_after_page_one = add_parsed_log(
        auth_database,
        timestamp=timestamp,
        created_at=datetime.now(UTC),
        raw="authentication failure inserted later",
        ecs_data={"ecs": {"version": "9.4.0"}, "event": {"action": "login"}},
    )
    backfill_after_page_one = add_parsed_log(
        auth_database,
        timestamp=timestamp - timedelta(days=1),
        created_at=datetime.now(UTC),
        raw="authentication failure backfill",
        ecs_data={"ecs": {"version": "9.4.0"}, "event": {"action": "login"}},
    )
    next_request = {**request, "cursor": first_page["next_cursor"]}
    second = client.post(
        "/api/v1/parsed-logs/search", json=next_request, headers=events_headers(client)
    )
    assert second.status_code == 200, second.text
    assert cursor_codec.decoded == 2
    all_ids = [item["id"] for item in first_page["items"] + second.json()["items"]]
    assert len(all_ids) == len(set(all_ids)) == 3
    assert str(inserted_after_page_one) not in all_ids
    assert str(backfill_after_page_one) not in all_ids
    time.sleep(0.01)
    fresh = client.post(
        "/api/v1/parsed-logs/search", json={**request, "limit": 100},
        headers=events_headers(client),
    )
    assert fresh.status_code == 200
    fresh_ids = {item["id"] for item in fresh.json()["items"]}
    assert {str(inserted_after_page_one), str(backfill_after_page_one)} <= fresh_ids
    changed_filters = {**next_request, "raw_query": "different"}
    invalid = client.post(
        "/api/v1/parsed-logs/search", json=changed_filters, headers=events_headers(client)
    )
    assert invalid.status_code == 422 and invalid.json()["code"] == "invalid_cursor"
    signed = cursor_codec.delegate.serializer
    payload = signed.loads(first_page["next_cursor"])
    payload["snapshot_boundary"] = None
    malformed = next_request | {"cursor": signed.dumps(payload)}
    invalid = client.post(
        "/api/v1/parsed-logs/search", json=malformed, headers=events_headers(client)
    )
    assert invalid.status_code == 422 and invalid.json()["code"] == "invalid_cursor"
    legacy_cursor = signed.dumps({
        "processed_at": timestamp.isoformat(), "id": str(ids[0]),
        "filters": cursor_codec.delegate._fingerprint(
            ParsedLogSearchRequest.model_validate(request)
        ),
    })
    invalid = client.post(
        "/api/v1/parsed-logs/search",
        json={**request, "cursor": legacy_cursor}, headers=events_headers(client),
    )
    assert invalid.status_code == 422 and invalid.json()["code"] == "invalid_cursor"
    tampered = next_request | {"cursor": first_page["next_cursor"] + "x"}
    invalid = client.post(
        "/api/v1/parsed-logs/search", json=tampered, headers=events_headers(client)
    )
    assert invalid.status_code == 422 and invalid.json()["code"] == "invalid_cursor"
    detail = client.get(f"/api/v1/parsed-logs/{ids[0]}")
    assert detail.status_code == 200
    assert detail.json()["raw"] == "authentication failure 0"
    assert detail.json()["ecs_data"]["event"]["action"] == "login"


def test_parsed_log_typed_ecs_filters_and_bad_values(client, auth_database):
    add_local_user(auth_database)
    login(client)
    add_parsed_log(
        auth_database,
        ecs_data={
            "ecs": {"version": "9.4.0"},
            "event": {
                "duration": 42,
                "action": "login",
                "category": ["authentication", "network"],
                "created": "2026-01-01T00:00:00Z",
            },
            "source": {"ip": "192.0.2.10"},
            "host": {"name": "sensor-a"},
            "cloud": {"entity": {"attributes": {"mfa_enabled": True}}},
        },
    )
    add_parsed_log(
        auth_database,
        ecs_data={
            "ecs": {"version": "legacy"},
            "event": {"duration": "bad-number", "category": "not-an-array"},
        },
    )
    headers = events_headers(client)
    for filter_value in (
        {"field": "event.duration", "operator": "gte", "value": 40},
        {"field": "source.ip", "operator": "eq", "value": "192.0.2.10"},
        {"field": "host.name", "operator": "contains", "value": "sensor"},
        {"field": "event.category", "operator": "contains", "value": "auth"},
        {"field": "event.category", "operator": "eq", "value": "network"},
        {"field": "event.created", "operator": "gte", "value": "2026-01-01T00:00:00Z"},
        {"field": "cloud.entity.attributes.mfa_enabled", "operator": "eq", "value": True},
        {"field": "source.ip", "operator": "exists"},
        {"field": "host.ip", "operator": "not_exists"},
    ):
        response = client.post(
            "/api/v1/parsed-logs/search",
            json={"ecs_filters": [filter_value]},
            headers=headers,
        )
        assert response.status_code == 200, response.text
        expected_count = 2 if filter_value["operator"] == "not_exists" else 1
        assert len(response.json()["items"]) == expected_count
    combined = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [
            {"field": "event.duration", "operator": "gt", "value": 40},
            {"field": "source.ip", "operator": "eq", "value": "192.0.2.10"},
        ]},
        headers=headers,
    )
    assert combined.status_code == 200 and len(combined.json()["items"]) == 1
    malformed_numeric = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.duration", "operator": "gte", "value": 1}]},
        headers=headers,
    )
    assert malformed_numeric.status_code == 200
    assert len(malformed_numeric.json()["items"]) == 1
    invalid_type = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.duration", "operator": "eq", "value": "42"}]},
        headers=headers,
    )
    assert invalid_type.status_code == 422
    assert invalid_type.json()["code"] == "ecs_filter_invalid"
    assert invalid_type.json()["details"] == {
        "index": 0, "field": "event.duration", "reason": "invalid_value_type"
    }
    fractional_integer = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.duration", "operator": "eq", "value": 1.5}]},
        headers=headers,
    )
    assert fractional_integer.status_code == 422
    assert fractional_integer.json()["code"] == "ecs_filter_invalid"
    positive_integer = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.duration", "operator": "eq", "value": 42}]},
        headers=headers,
    )
    assert positive_integer.status_code == 200
    assert len(positive_integer.json()["items"]) == 1
    invalid_ip = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "source.ip", "operator": "eq", "value": "not-an-ip"}]},
        headers=headers,
    )
    assert invalid_ip.status_code == 422 and invalid_ip.json()["code"] == "ecs_filter_invalid"
    invalid_array_ip = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "host.ip", "operator": "contains", "value": "not-an-ip"}]},
        headers=headers,
    )
    assert invalid_array_ip.status_code == 422
    invalid_date = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.created", "operator": "eq", "value": "2026-01-01T00:00:00"}]},
        headers=headers,
    )
    assert invalid_date.status_code == 422
    invalid_operator = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.action", "operator": "sql", "value": "x"}]},
        headers=headers,
    )
    assert invalid_operator.status_code == 422
    invalid_field = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "unknown.field", "operator": "eq", "value": 1}]},
        headers=headers,
    )
    assert invalid_field.status_code == 422
    non_filterable = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "agent", "operator": "exists"}]},
        headers=headers,
    )
    assert non_filterable.status_code == 422


def test_parsed_log_typed_array_filters_and_json_null_presence(client, auth_database):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    original = client.app.state.ecs_catalog
    array_fields = dict(original.fields)
    for name in (
        "event.duration", "event.created", "cloud.entity.attributes.mfa_enabled"
    ):
        field = array_fields[name]
        array_fields[name] = replace(
            field,
            is_array=True,
            operators=("eq", "neq", "contains", "exists", "not_exists"),
        )
    client.app.state.ecs_catalog = PackagedEcsCatalog(array_fields, original.provenance)

    for field_name, value in (
        ("event.duration", 7), ("event.created", "2026-01-01T00:00:00Z")
    ):
        catalog_field = client.get(f"/api/v1/ecs/fields/{field_name}").json()
        assert set(catalog_field["operators"]) == {
            "eq", "neq", "contains", "exists", "not_exists"
        }
        for operator in ("gt", "gte", "lt", "lte"):
            response = client.post(
                "/api/v1/parsed-logs/search",
                json={"ecs_filters": [{"field": field_name, "operator": operator, "value": value}]},
                headers=events_headers(client),
            )
            assert response.status_code == 422
            assert response.json()["code"] == "ecs_filter_invalid"
        with pytest.raises(ValueError, match="Unsupported ECS array operator"):
            _filter_expression(
                ValidatedEcsFilter(array_fields[field_name], "gt", value)
            )

    add_parsed_log(
        auth_database, raw="array-semantics valid",
        ecs_data={
            "ecs": {"version": "9.4.0"},
            "host": {"ip": ["192.0.2.1", "192.0.2.2"]},
            "event": {
                "duration": [42, 7],
                "created": ["2026-01-01T00:00:00+00:00"],
            },
            "cloud": {"entity": {"attributes": {"mfa_enabled": [True]}}},
        },
    )
    add_parsed_log(
        auth_database, raw="array-semantics malformed",
        ecs_data={
            "ecs": {"version": "9.4.0"},
            "host": {"ip": "not-an-array"},
            "event": {"duration": ["bad-number", None], "created": None},
            "cloud": {"entity": {"attributes": {"mfa_enabled": "true"}}},
        },
    )
    add_parsed_log(
        auth_database, raw="array-semantics missing",
        ecs_data={"ecs": {"version": "9.4.0"}},
    )
    headers = events_headers(client)

    def search(field, operator, value_marker=...):
        item = {"field": field, "operator": operator}
        if value_marker is not ...:
            item["value"] = value_marker
        response = client.post(
            "/api/v1/parsed-logs/search",
            json={"raw_query": "array-semantics", "ecs_filters": [item]},
            headers=headers,
        )
        assert response.status_code == 200, response.text
        return response.json()["items"]

    assert len(search("host.ip", "eq", "192.0.2.1")) == 1
    assert len(search("host.ip", "neq", "192.0.2.10")) == 1
    assert len(search("host.ip", "contains", "192.0.2.1")) == 1
    assert len(search("event.duration", "eq", 42)) == 1
    assert len(search("event.duration", "neq", 99)) == 1
    assert len(search("event.duration", "contains", 7)) == 1
    assert len(search("event.created", "eq", "2026-01-01T00:00:00+00:00")) == 1
    assert len(search("event.created", "eq", "2026-01-01T00:00:00Z")) == 1
    assert len(search(
        "cloud.entity.attributes.mfa_enabled", "contains", True
    )) == 1
    assert len(search("host.ip", "exists")) == 2
    assert len(search("host.ip", "not_exists")) == 1
    assert len(search("event.created", "exists")) == 2
    assert len(search("event.created", "not_exists")) == 1
    assert len(search("cloud.entity.attributes.mfa_enabled", "neq", False)) == 1

    invalid_numeric_array = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.duration", "operator": "contains", "value": 1.5}]},
        headers=headers,
    )
    assert invalid_numeric_array.status_code == 422

    invalid_ip = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "host.ip", "operator": "contains", "value": "bad-ip"}]},
        headers=headers,
    )
    assert invalid_ip.status_code == 422
    invalid_naive_date = client.post(
        "/api/v1/parsed-logs/search",
        json={"ecs_filters": [{"field": "event.created", "operator": "contains", "value": "2026-01-01T00:00:00"}]},
        headers=headers,
    )
    assert invalid_naive_date.status_code == 422


def test_parsed_log_raw_escaping_and_newest_first_projection(client, auth_database):
    add_local_user(auth_database)
    login(client)
    now = datetime.now(UTC)
    matching = add_parsed_log(
        auth_database, timestamp=now, raw='100%_\\ "quoted" café'
    )
    add_parsed_log(
        auth_database, timestamp=now + timedelta(seconds=1), raw="100XXZZ quoted cafe"
    )
    response = client.post(
        "/api/v1/parsed-logs/search",
        json={"raw_query": "100%_\\"},
        headers=events_headers(client),
    )
    assert response.status_code == 200, response.text
    assert [row["id"] for row in response.json()["items"]] == [str(matching)]
    assert set(response.json()["items"][0]).isdisjoint(
        {"raw", "ecs_data", "deduplication_key"}
    )
    empty_search = client.post(
        "/api/v1/parsed-logs/search", json={}, headers=events_headers(client)
    )
    assert empty_search.status_code == 200
    assert empty_search.json()["items"][0]["raw_preview"] == "100XXZZ quoted cafe"
    assert empty_search.json()["has_more"] is False


def test_parsed_log_string_contains_ignores_malformed_json_and_escapes_literals(
    client, auth_database
):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    matching_id = add_parsed_log(
        auth_database,
        raw="scalar-contains valid",
        ecs_data={"ecs": {"version": "9.4.0"}, "host": {"name": "Sensor-A"}},
    )
    literal_id = add_parsed_log(
        auth_database,
        raw="scalar-contains literal",
        ecs_data={
            "ecs": {"version": "9.4.0"},
            "host": {"name": r"sensor%_\node"},
        },
    )
    add_parsed_log(
        auth_database,
        raw="scalar-contains malformed",
        ecs_data={
            "ecs": {"version": "9.4.0"},
            "host": {"name": {"details": "sensor"}},
        },
    )
    headers = events_headers(client)

    def search(term):
        response = client.post(
            "/api/v1/parsed-logs/search",
            json={
                "raw_query": "scalar-contains",
                "ecs_filters": [{"field": "host.name", "operator": "contains", "value": term}],
            },
            headers=headers,
        )
        assert response.status_code == 200, response.text
        return {item["id"] for item in response.json()["items"]}

    assert search("SENSOR") == {str(matching_id), str(literal_id)}
    assert search("%_" + chr(92)) == {str(literal_id)}


def test_parsed_log_origin_and_half_open_time_filters(client, auth_database):
    add_local_user(auth_database)
    login(client)
    connection_id, sources = add_source_pair(auth_database)
    source_ids = [source_id for source_id, _ in sources]
    topics = [topic for _, topic in sources]
    start = datetime(2026, 2, 1, tzinfo=UTC)
    collected_start = datetime(2026, 1, 1, tzinfo=UTC)
    included = add_parsed_log(
        auth_database, timestamp=start, collected_at=collected_start,
        source_id=source_ids[0], connection_id=connection_id,
        topic=topics[0], partition=3,
    )
    add_parsed_log(
        auth_database, timestamp=start + timedelta(seconds=1),
        collected_at=collected_start + timedelta(seconds=1), source_id=source_ids[1],
        connection_id=connection_id, topic=topics[1], partition=4,
    )
    response = client.post(
        "/api/v1/parsed-logs/search",
        json={
            "source_ids": [str(source_ids[0]), str(source_ids[1])],
            "connection_ids": [str(connection_id)],
            "kafka_topics": topics,
            "kafka_partitions": [3, 4],
            "processed_from": start.isoformat(),
            "processed_to": (start + timedelta(seconds=1)).isoformat(),
            "collected_from": collected_start.isoformat(),
            "collected_to": (collected_start + timedelta(seconds=1)).isoformat(),
        },
        headers=events_headers(client),
    )
    assert response.status_code == 200, response.text
    assert [row["id"] for row in response.json()["items"]] == [str(included)]


def test_parsed_log_permissions_csrf_and_deletion_are_scoped(client, auth_database):
    guest_id = add_local_user(auth_database)
    first = add_parsed_log(auth_database)
    later = add_parsed_log(auth_database, timestamp=datetime.now(UTC) + timedelta(seconds=1))
    assert client.delete(f"/api/v1/parsed-logs/{first}").status_code == 401
    assert client.get(f"/api/v1/parsed-logs/{first}").status_code == 401
    assert login(client).status_code == 200
    guest_csrf = events_headers(client)
    assert client.post(
        "/api/v1/parsed-logs/search", json={}, headers=guest_csrf
    ).status_code == 200
    assert client.delete(
        f"/api/v1/parsed-logs/{first}", headers=guest_csrf
    ).status_code == 403
    with auth_database.session_factory() as session:
        user = session.get(User, guest_id)
        user.role_id = ADMIN_ID
        session.commit()
    # The current session re-reads its role from the database on each request.
    no_csrf = client.post(
        "/api/v1/parsed-logs/actions/delete", json={"source_id": str(uuid4())}
    )
    assert no_csrf.status_code == 403 and no_csrf.json()["code"] == "csrf_invalid"
    headers = events_headers(client)
    unsafe = client.post("/api/v1/parsed-logs/actions/delete", json={}, headers=headers)
    assert unsafe.status_code == 422 and unsafe.json()["code"] == "deletion_scope_required"
    partial_range = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={"processed_from": datetime.now(UTC).isoformat()},
        headers=headers,
    )
    assert partial_range.status_code == 422
    reverse_range = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={"processed_from": "2000-01-02T00:00:00Z", "processed_to": "2000-01-01T00:00:00Z"},
        headers=headers,
    )
    assert reverse_range.status_code == 422
    zero_range = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={"processed_from": "2000-01-01T00:00:00Z", "processed_to": "2000-01-01T00:00:00Z"},
        headers=headers,
    )
    assert zero_range.status_code == 422

    connection_id, sources = add_source_pair(auth_database)
    source_a, topic_a = sources[0]
    source_b, topic_b = sources[1]
    source_only_id = add_parsed_log(
        auth_database, timestamp=datetime(2020, 1, 1, tzinfo=UTC),
        source_id=source_a, connection_id=connection_id, topic=topic_a,
    )
    other_source_id = add_parsed_log(
        auth_database, timestamp=datetime(2020, 1, 1, tzinfo=UTC),
        source_id=source_b, connection_id=connection_id, topic=topic_b,
    )
    source_delete = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={"source_id": str(source_a)}, headers=headers,
    )
    assert source_delete.status_code == 200 and source_delete.json()["deleted_count"] == 1
    nonexistent_source_delete = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={"source_id": str(uuid4())}, headers=headers,
    )
    assert nonexistent_source_delete.status_code == 200
    assert nonexistent_source_delete.json()["deleted_count"] == 0
    range_start = datetime(2021, 1, 1, tzinfo=UTC)
    range_id = add_parsed_log(auth_database, timestamp=range_start)
    boundary_id = add_parsed_log(auth_database, timestamp=range_start + timedelta(days=1))
    range_delete = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={
            "processed_from": range_start.isoformat(),
            "processed_to": (range_start + timedelta(days=1)).isoformat(),
        },
        headers=headers,
    )
    assert range_delete.status_code == 200 and range_delete.json()["deleted_count"] == 1

    source_period_start = datetime(2022, 1, 1, tzinfo=UTC)
    intersection_id = add_parsed_log(
        auth_database, timestamp=source_period_start, source_id=source_a,
        connection_id=connection_id, topic=topic_a,
    )
    outside_intersection_id = add_parsed_log(
        auth_database, timestamp=source_period_start, source_id=source_b,
        connection_id=connection_id, topic=topic_b,
    )
    intersection = client.post(
        "/api/v1/parsed-logs/actions/delete",
        json={
            "source_id": str(source_a),
            "processed_from": source_period_start.isoformat(),
            "processed_to": (source_period_start + timedelta(days=1)).isoformat(),
        },
        headers=headers,
    )
    assert intersection.status_code == 200 and intersection.json()["deleted_count"] == 1
    deleted = client.delete(f"/api/v1/parsed-logs/{first}", headers=headers)
    assert deleted.status_code == 204
    assert client.delete(f"/api/v1/parsed-logs/{first}", headers=headers).status_code == 404
    with auth_database.session_factory() as session:
        assert session.get(ParsedLog, later) is not None
        assert session.get(ParsedLog, source_only_id) is None
        assert session.get(ParsedLog, other_source_id) is not None
        assert session.get(ParsedLog, range_id) is None
        assert session.get(ParsedLog, boundary_id) is not None
        assert session.get(ParsedLog, intersection_id) is None
        assert session.get(ParsedLog, outside_intersection_id) is not None


def test_parsed_log_database_failures_rollback_without_logging_values(caplog):
    repository = Mock()
    unit_of_work = Mock()
    codec = Mock()
    service = ParsedLogServiceImpl(repository, unit_of_work, object(), codec)
    repository.delete_many.side_effect = RuntimeError("sensitive filter value")
    request = ParsedLogBulkDeleteRequest(source_id=uuid4())
    with pytest.raises(DomainError) as bulk_error:
        service.delete_many(request)
    assert bulk_error.value.code == "internal_error"
    unit_of_work.rollback.assert_called_once()
    unit_of_work.commit.assert_not_called()
    assert "sensitive filter value" not in caplog.text

    unit_of_work.reset_mock()
    repository.reset_mock()
    repository.find_by_id.return_value = object()
    repository.delete.side_effect = RuntimeError("sensitive delete value")
    with pytest.raises(DomainError) as delete_error:
        service.delete(uuid4())
    assert delete_error.value.code == "internal_error"
    unit_of_work.rollback.assert_called_once()
    unit_of_work.commit.assert_not_called()
    assert "sensitive delete value" not in caplog.text

    unit_of_work.reset_mock()
    repository.reset_mock()
    repository.search.side_effect = RuntimeError("sensitive search value")
    with pytest.raises(DomainError) as search_error:
        service.search(ParsedLogSearchRequest())
    assert search_error.value.code == "internal_error"
    unit_of_work.rollback.assert_called_once()
    assert "sensitive search value" not in caplog.text


def test_parsed_log_migration_backfills_normalizer_snapshots(auth_database_url, auth_database):
    run_alembic(auth_database_url, "downgrade", "0002_auth_sessions")
    try:
        now = datetime.now(UTC)
        with auth_database.engine.begin() as connection:
            normalizer_id = connection.execute(
                text("INSERT INTO app.normalizers (name, rule) VALUES ('historical-normalizer', '{}') RETURNING id")
            ).scalar_one()
            for index, linked_normalizer in enumerate((normalizer_id, None)):
                connection.execute(
                    text("""
                        INSERT INTO logs.parsed_logs (
                            normalizer_id, normalizer_version, source_name, connection_name,
                            kafka_topic, kafka_partition, kafka_offset, deduplication_key,
                            fluent_bit_collected_at, backend_received_at, backend_processed_at,
                            raw, ecs_data
                        ) VALUES (
                            :normalizer_id, 1, 'source-old', 'connection-old', 'events', 0,
                            :offset, :dedup, :timestamp, :timestamp, :timestamp, 'raw', '{}'
                        )
                    """), {
                        "normalizer_id": linked_normalizer,
                        "offset": index,
                        "dedup": f"migration-backfill-{uuid4()}",
                        "timestamp": now,
                    },
                )
        run_alembic(auth_database_url, "upgrade", "head")
        with auth_database.engine.connect() as connection:
            names = set(connection.execute(text(
                "SELECT normalizer_name FROM logs.parsed_logs"
            )).scalars())
            opclass = connection.execute(text("""
                SELECT opc.opcname
                FROM pg_index AS idx
                JOIN pg_class AS index_class ON index_class.oid = idx.indexrelid
                JOIN pg_opclass AS opc ON opc.oid = idx.indclass[0]
                WHERE index_class.relname = 'ix_parsed_logs_raw_trgm'
            """)).scalar_one()
            column = connection.execute(text("""
                SELECT is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'logs' AND table_name = 'parsed_logs'
                  AND column_name = 'normalizer_name'
            """)).one()
        assert names == {"historical-normalizer", "legacy-unknown"}
        assert opclass == "gin_trgm_ops"
        assert column == ("NO", None)
        run_alembic(auth_database_url, "downgrade", "0002_auth_sessions")
        with auth_database.engine.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM logs.parsed_logs")).scalar_one() == 2
            assert connection.execute(text("""
                SELECT to_regclass('logs.ix_parsed_logs_raw_trgm')
            """)).scalar_one() is None
            assert connection.execute(text("""
                SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'
            """)).scalar_one() == "pg_trgm"
        run_alembic(auth_database_url, "upgrade", "head")
        with auth_database.engine.connect() as connection:
            assert set(connection.execute(text(
                "SELECT normalizer_name FROM logs.parsed_logs"
            )).scalars()) == {"historical-normalizer", "legacy-unknown"}
    finally:
        run_alembic(auth_database_url, "upgrade", "head")


def test_external_api_permissions_and_secret_dto(client, auth_database, auth_settings):
    from cryptography.fernet import Fernet
    from pydantic import SecretStr

    class Metadata:
        def indices(self, base_url, username, password, ca_pem):
            assert password == "indexer-password"
            return ["logs-2026", "logs-2027"]

    auth_settings.external_secret_key = SecretStr(Fernet.generate_key().decode())
    client.app.state.indexer_adapter = Metadata()
    add_local_user(auth_database, "reader")
    assert login(client, "reader").status_code == 200
    assert client.get("/api/v1/external-connections").status_code == 200
    assert client.get("/api/v1/external-sources").status_code == 200
    assert client.get("/api/v1/external-connections/00000000-0000-0000-0000-000000000000/indices").status_code == 403
    client.cookies.clear()
    headers = admin_headers(client, auth_database)
    created = client.post("/api/v1/external-connections", json={
        "name": "indexer", "base_url": "https://indexer.local:9200",
        "username": "reader", "password": "indexer-password",
    }, headers=headers)
    assert created.status_code == 201
    payload = created.json()
    assert payload["has_password"] and not payload["has_ca"]
    assert "password" not in payload and "ca_pem" not in payload
    connection_id = payload["id"]
    assert client.post(f"/api/v1/external-connections/{connection_id}/test",
                       headers=headers).json() == {"status": "ok"}
    assert client.get(f"/api/v1/external-connections/{connection_id}/indices?limit=1",
                      headers=headers).json()["items"] == ["logs-2026"]
    assert client.get("/api/v1/external-sources").json()["total"] == 0
    source = client.post("/api/v1/external-sources", json={
        "name": "logs", "external_connection_id": connection_id,
        "target_type": "index_pattern", "index_pattern": "logs-*",
    }, headers=headers)
    assert source.status_code == 201 and not source.json()["is_enabled"]
    source_id = source.json()["id"]
    assert client.post(f"/api/v1/external-sources/{source_id}/enable",
                       headers=headers).json()["is_enabled"]
    assert client.get("/api/v1/sources").json()["total"] == 0
    changed = client.patch(f"/api/v1/external-connections/{connection_id}",
                           json={"name": "renamed"}, headers=headers)
    assert changed.status_code == 200 and "password" not in changed.json()
    assert not client.get(f"/api/v1/external-sources/{source_id}").json()["is_enabled"]
    missing = client.post("/api/v1/external-sources", json={
        "name": "missing", "external_connection_id": connection_id,
        "target_type": "index", "index_name": "absent-2026",
    }, headers=headers)
    assert missing.status_code == 201
    unavailable = client.post(f"/api/v1/external-sources/{missing.json()['id']}/enable",
                              headers=headers)
    assert unavailable.status_code == 409
    assert unavailable.json()["code"] == "index_not_found"


def external_admin(client, auth_database, auth_settings, adapter=None):
    from cryptography.fernet import Fernet
    from pydantic import SecretStr

    auth_settings.external_secret_key = SecretStr(Fernet.generate_key().decode())
    if adapter is not None:
        client.app.state.indexer_adapter = adapter
    return admin_headers(client, auth_database)


def external_connection(client, headers, name="indexer", password="private-password"):
    response = client.post("/api/v1/external-connections", json={
        "name": name,
        "base_url": "https://indexer.local:9200",
        "username": "reader",
        "password": password,
    }, headers=headers)
    assert response.status_code == 201
    return response.json()["id"]


def ca_material(common_name):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    leaf = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    private_key = key.private_bytes(serialization.Encoding.PEM,
                                    serialization.PrivateFormat.TraditionalOpenSSL,
                                    serialization.NoEncryption())
    return (certificate.public_bytes(serialization.Encoding.PEM),
            leaf.public_bytes(serialization.Encoding.PEM), private_key)


def test_external_ca_http_upload_replace_delete_and_safe_errors(client, auth_database,
                                                                auth_settings):
    ca_one, leaf, private_key = ca_material("Test One")
    ca_two, _, _ = ca_material("Test Two")
    add_local_user(auth_database, "ca-guest")
    assert login(client, "ca-guest").status_code == 200
    guest_headers = {"Content-Type": "application/x-pem-file",
                     "X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]}
    nonexistent = "00000000-0000-0000-0000-000000000000"
    denied = client.put(f"/api/v1/external-connections/{nonexistent}/ca",
                        content=ca_one, headers=guest_headers)
    assert denied.status_code == 403 and denied.json()["code"] == "permission_denied"
    assert client.delete(f"/api/v1/external-connections/{nonexistent}/ca",
                         headers=guest_headers).status_code == 403
    client.cookies.clear()
    headers = external_admin(client, auth_database, auth_settings)
    identity = external_connection(client, headers)
    url = f"/api/v1/external-connections/{identity}/ca"
    upload_headers = {**headers, "Content-Type": "application/x-pem-file"}
    for invalid in (b"", b"ordinary file", leaf, private_key, b"x" * 65537):
        result = client.put(url, content=invalid, headers=upload_headers)
        assert result.status_code == 422
        assert result.json()["code"] == "invalid_ca"
        assert result.json()["request_id"]
        assert ca_one.decode() not in result.text
        assert "private-password" not in result.text
    wrong_media = client.put(url, content=ca_one,
                             headers={**headers, "Content-Type": "text/plain"})
    assert wrong_media.status_code == 422
    assert wrong_media.json()["code"] == "invalid_ca_media_type"
    assert client.put(url, content=ca_one, headers=upload_headers).status_code == 204
    dto = client.get(f"/api/v1/external-connections/{identity}").json()
    assert dto["has_ca"] and dto["has_password"]
    assert ca_one.decode() not in str(dto) and "private-password" not in str(dto)
    with auth_database.session_factory() as session:
        from app.models import ExternalConnection
        assert session.get(ExternalConnection, UUID(identity)).ca_pem == ca_one.decode()
    assert client.put(url, content=ca_two, headers=upload_headers).status_code == 204
    with auth_database.session_factory() as session:
        assert session.get(ExternalConnection, UUID(identity)).ca_pem == ca_two.decode()
    assert client.delete(url, headers=headers).status_code == 204
    assert not client.get(f"/api/v1/external-connections/{identity}").json()["has_ca"]


def test_external_http_target_password_connection_and_cascade(client, auth_database,
                                                              auth_settings):
    class Metadata:
        def indices(self, base_url, username, password, ca_pem):
            assert password in {"private-password", "rotated-password"}
            return ["logs-2026", "logs-2027"]

        def data_streams(self, base_url, username, password, ca_pem):
            assert password in {"private-password", "rotated-password"}
            return ["wazuh-findings-v5-security", "wazuh-findings-v5-vulnerability"]

    headers = external_admin(client, auth_database, auth_settings, Metadata())
    first = external_connection(client, headers)
    second = external_connection(client, headers, name="second")
    streams = client.get(f"/api/v1/external-connections/{first}/data-streams", headers=headers)
    assert streams.status_code == 200 and streams.json()["total"] == 2
    wrong_kind = client.post("/api/v1/external-sources", json={
        "name": "stream-as-index", "external_connection_id": first,
        "target_type": "index", "index_name": "wazuh-findings-v5-security",
    }, headers=headers)
    assert wrong_kind.status_code == 201
    assert client.post(f"/api/v1/external-sources/{wrong_kind.json()['id']}/enable",
                       headers=headers).status_code == 409
    for target_type, field, value in (
        ("data_stream", "data_stream_name", "wazuh-findings-v5-security"),
        ("data_stream_pattern", "data_stream_pattern", "wazuh-findings-v5-*"),
    ):
        stream = client.post("/api/v1/external-sources", json={
            "name": target_type, "external_connection_id": first,
            "target_type": target_type, field: value,
        }, headers=headers)
        assert stream.status_code == 201 and not stream.json()["is_enabled"]
        assert client.post(f"/api/v1/external-sources/{stream.json()['id']}/enable",
                           headers=headers).json()["is_enabled"]
    created = client.post("/api/v1/external-sources", json={
        "name": "exact", "external_connection_id": first, "target_type": "index", "index_name": "logs-2026",
    }, headers=headers)
    assert created.status_code == 201
    identity = created.json()["id"]
    duplicate = client.post("/api/v1/external-sources", json={
        "name": "duplicate", "external_connection_id": first, "target_type": "index", "index_name": "logs-2026",
    }, headers=headers)
    assert duplicate.status_code == 409
    assert client.post(f"/api/v1/external-sources/{identity}/enable",
                       headers=headers).json()["is_enabled"]
    changed = client.patch(f"/api/v1/external-sources/{identity}",
                           json={"target_type": "index_pattern", "index_pattern": "logs-*"}, headers=headers)
    assert changed.status_code == 200
    assert changed.json()["index_name"] is None and not changed.json()["is_enabled"]
    duplicate_pattern = client.post("/api/v1/external-sources", json={
        "name": "duplicate mask", "external_connection_id": first,
        "target_type": "index_pattern", "index_pattern": "logs-*",
    }, headers=headers)
    assert duplicate_pattern.status_code == 409
    assert client.post(f"/api/v1/external-sources/{identity}/enable",
                       headers=headers).json()["is_enabled"]
    changed = client.patch(f"/api/v1/external-sources/{identity}",
                           json={"target_type": "index", "index_name": "logs-2027"}, headers=headers)
    assert changed.status_code == 200
    assert changed.json()["index_pattern"] is None and not changed.json()["is_enabled"]
    assert client.post(f"/api/v1/external-sources/{identity}/enable",
                       headers=headers).json()["is_enabled"]
    switched = client.patch(f"/api/v1/external-sources/{identity}",
                            json={"external_connection_id": second}, headers=headers)
    assert switched.status_code == 200 and not switched.json()["is_enabled"]
    assert switched.json()["external_connection_id"] == second
    assert client.post(f"/api/v1/external-sources/{identity}/enable",
                       headers=headers).json()["is_enabled"]
    rotated = client.patch(f"/api/v1/external-connections/{second}",
                           json={"password": "rotated-password"}, headers=headers)
    assert rotated.status_code == 200
    assert "password" not in rotated.json() and "rotated-password" not in rotated.text
    assert not client.get(f"/api/v1/external-sources/{identity}").json()["is_enabled"]
    assert client.post(f"/api/v1/external-connections/{second}/test",
                       headers=headers).json() == {"status": "ok"}
    assert client.delete(f"/api/v1/external-connections/{first}",
                         headers=headers).status_code == 204
    assert client.get(f"/api/v1/external-sources/{identity}").status_code == 200
    assert client.delete(f"/api/v1/external-connections/{second}",
                         headers=headers).status_code == 204
    assert client.get(f"/api/v1/external-sources/{identity}").status_code == 404


def test_external_http_indexer_error_mapping(client, auth_database, auth_settings):
    from app.indexer import IndexerError

    class FailingMetadata:
        kind = "access_denied"

        def indices(self, base_url, username, password, ca_pem):
            raise IndexerError(self.kind)

    adapter = FailingMetadata()
    headers = external_admin(client, auth_database, auth_settings, adapter)
    identity = external_connection(client, headers)
    expected = {
        "access_denied": (403, "indexer_access_denied"),
        "untrusted_certificate": (502, "indexer_untrusted_certificate"),
        "hostname_mismatch": (502, "indexer_hostname_mismatch"),
        "timeout": (504, "indexer_timeout"),
        "unavailable": (503, "indexer_unavailable"),
        "invalid_response": (502, "indexer_invalid_response"),
    }
    for kind, (status, code) in expected.items():
        adapter.kind = kind
        result = client.post(f"/api/v1/external-connections/{identity}/test",
                             headers=headers)
        assert result.status_code == status
        assert result.json()["code"] == code
        assert result.json()["request_id"] == result.headers["X-Request-ID"]
        assert "private-password" not in result.text
        assert "indexer.local" not in result.text


def test_external_openapi_contract(client):
    paths = client.get("/openapi.json").json()["paths"]
    root = "/api/v1/external-connections/{identity}"
    checked = paths[root + "/test"]["post"]
    assert checked["responses"]["200"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ExternalTestResponse")
    assert set(checked["responses"]) >= {"200", "401", "403", "404",
                                         "422", "502", "503", "504"}
    ca = paths[root + "/ca"]["put"]
    body = ca["requestBody"]
    assert body["required"]
    schema = body["content"]["application/x-pem-file"]["schema"]
    assert schema["type"] == "string" and schema["format"] == "binary"
    assert "65536" in schema["description"]
    assert root + "/data-streams" in paths
    source_schema = client.get("/openapi.json").json()["components"]["schemas"]["ExternalSourceCreate"]
    assert "target_type" in source_schema["required"]
    for operation in (checked, ca, paths["/api/v1/external-sources/{identity}/enable"]["post"]):
        for status, response in operation["responses"].items():
            if status not in {"200", "204"}:
                assert response["content"]["application/json"]["schema"]["$ref"].endswith(
                    "/ErrorResponse")


@pytest.mark.skipif(not os.getenv("WAZUH_SMOKE_URL"), reason="isolated live Wazuh smoke only")
def test_external_live_wazuh_smoke(client, auth_database, auth_settings):
    from cryptography.fernet import Fernet
    from pydantic import SecretStr

    auth_settings.external_secret_key = SecretStr(Fernet.generate_key().decode())
    headers = admin_headers(client, auth_database)
    created = client.post("/api/v1/external-connections", json={
        "name": "wazuh-smoke", "base_url": os.environ["WAZUH_SMOKE_URL"],
        "username": os.environ["WAZUH_SMOKE_USERNAME"],
        "password": os.environ["WAZUH_SMOKE_PASSWORD"],
    }, headers=headers)
    assert created.status_code == 201, created.text
    connection_id = created.json()["id"]
    ca = Path(os.environ["WAZUH_SMOKE_CA"]).read_bytes()
    uploaded = client.put(f"/api/v1/external-connections/{connection_id}/ca",
                          content=ca, headers={**headers, "Content-Type": "application/x-pem-file"})
    assert uploaded.status_code == 204, uploaded.text
    streams = client.get(f"/api/v1/external-connections/{connection_id}/data-streams?limit=100",
                         headers=headers)
    assert streams.status_code == 200, streams.text
    assert "wazuh-findings-v5-security" in streams.json()["items"]
    indices = client.get(f"/api/v1/external-connections/{connection_id}/indices?limit=100",
                         headers=headers)
    assert indices.status_code == 200, indices.text
    assert "wazuh-findings-v5-security" not in indices.json()["items"]
    with auth_database.engine.connect() as connection:
        before = connection.scalar(text("SELECT count(*) FROM logs.parsed_logs"))
    targets = (
        ("index", "index_name", "task12-concrete"),
        ("index_pattern", "index_pattern", "task12-logs-*"),
        ("data_stream", "data_stream_name", "wazuh-findings-v5-security"),
        ("data_stream_pattern", "data_stream_pattern", "wazuh-findings-v5-*"),
    )
    registered = []
    for target_type, field, value in targets:
        source = client.post("/api/v1/external-sources", json={
            "name": target_type, "external_connection_id": connection_id,
            "target_type": target_type, field: value,
        }, headers=headers)
        assert source.status_code == 201 and not source.json()["is_enabled"], source.text
        enabled = client.post(f"/api/v1/external-sources/{source.json()['id']}/enable",
                              headers=headers)
        assert enabled.status_code == 200 and enabled.json()["is_enabled"], enabled.text
        registered.append(source.json()["id"])

    search_body = {
        "timestamp_from": "2026-06-15T12:00:00Z",
        "timestamp_to": "2026-06-15T12:04:00Z",
        "sort": "asc",
        "filters": [{"field": "wazuh.agent.name", "operator": "eq", "value": "agent-0"}],
        "limit": 1,
    }
    card_event_ids = {}
    for (target_type, _, _), source_id in zip(targets, registered, strict=True):
        fields = client.get(
            f"/api/v1/events/sources/{source_id}/fields?q=wazuh.agent.name",
            headers=headers,
        )
        assert fields.status_code == 200, f"{target_type}: {fields.text}"
        assert any(item["name"] == "wazuh.agent.name" for item in fields.json()["items"])
        first = client.post(
            f"/api/v1/events/sources/{source_id}/search", json=search_body, headers=headers
        )
        assert first.status_code == 200, first.text
        result = first.json()
        assert result["items"] and result["items"][0]["source_type"] == "external"
        assert result["has_more"] and result["next_cursor"]
        assert result["items"][0]["fields"]["wazuh"]["agent"]["name"] == "agent-0"
        card_event_ids[source_id] = result["items"][0]["id"]
        card = client.get(
            f"/api/v1/events/sources/{source_id}/events/{result['items'][0]['id']}",
            headers=headers,
        )
        assert card.status_code == 200, card.text
        assert card.json()["fields"]["wazuh"]["agent"]["name"] == "agent-0"
        second = client.post(
            f"/api/v1/events/sources/{source_id}/search",
            json={**search_body, "cursor": result["next_cursor"]},
            headers=headers,
        )
        assert second.status_code == 200, second.text
        assert second.json()["items"]
        assert second.json()["items"][0]["id"] != result["items"][0]["id"]

    numeric_time = client.post(
        f"/api/v1/events/sources/{registered[2]}/search",
        json={"timestamp_from": "2026-06-15T11:59:00Z",
              "timestamp_to": "2026-06-15T12:01:00Z", "limit": 10,
              "filters": [{"field": "event.sequence", "operator": "eq", "value": 20}]},
        headers=headers,
    )
    assert numeric_time.status_code == 200, numeric_time.text
    assert len(numeric_time.json()["items"]) == 1
    assert numeric_time.json()["items"][0]["fields"]["@timestamp"] == 1781524800000
    assert numeric_time.json()["items"][0]["event_timestamp"].startswith("2026-06-15T12:00:00")

    assert client.post("/api/v1/auth/logout", headers=headers).status_code == 200
    add_local_user(auth_database)
    assert login(client).status_code == 200
    guest_headers = events_headers(client)
    guest_search = client.post(
        f"/api/v1/events/sources/{registered[2]}/search",
        json={"limit": 1}, headers=guest_headers,
    )
    assert guest_search.status_code == 200, guest_search.text
    guest_card = client.get(
        f"/api/v1/events/sources/{registered[2]}/events/{card_event_ids[registered[2]]}",
        headers=guest_headers,
    )
    assert guest_card.status_code == 200, guest_card.text

    assert client.post("/api/v1/auth/logout", headers=guest_headers).status_code == 200
    assert login(client, "admin").status_code == 200
    admin_session = client.get("/api/v1/auth/session").json()
    disabled = client.post(
        f"/api/v1/external-sources/{registered[0]}/disable",
        headers={"X-CSRF-Token": admin_session["csrf_token"]},
    )
    assert disabled.status_code == 200 and not disabled.json()["is_enabled"]
    disabled_read = client.post(
        f"/api/v1/events/sources/{registered[0]}/search", json={},
        headers={"X-CSRF-Token": admin_session["csrf_token"]},
    )
    assert disabled_read.status_code == 409 and disabled_read.json()["code"] == "source_disabled"
    with auth_database.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM logs.parsed_logs")) == before
        assert connection.scalar(text("SELECT count(*) FROM logs.processed_kafka_records")) == 0
        assert connection.scalar(text("SELECT count(*) FROM logs.kafka_operational_events")) == 0

def test_source_scoped_event_query_api(client, auth_database):
    # Local event query dependencies must not require the optional external credential key.
    client.app.state.settings.external_secret_key = None
    add_local_user(auth_database)
    assert login(client).status_code == 200
    headers = events_headers(client)
    _, sources = add_source_pair(auth_database)
    source_id, _ = sources[0]
    other_source_id, _ = sources[1]
    add_parsed_log(
        auth_database,
        timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        source_id=source_id,
        raw="login event",
        ecs_data={
            "@timestamp": "2026-01-02T12:00:00Z",
            "event": {
                "action": "login",
                "duration": 500,
                "created": "2026-01-02T11:00:00Z",
            },
            "source": {"ip": "192.0.2.10"},
            "file": {"code_signature": {"valid": True}},
            "tags": ["prod", "secure"],
        },
    )
    add_parsed_log(
        auth_database,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        source_id=source_id,
        raw="logout event",
        ecs_data={
            "@timestamp": "2026-01-01T12:00:00Z",
            "event": {
                "action": "logout",
                "duration": 700,
                "created": "2026-01-01T11:00:00Z",
            },
            "source": {"ip": "192.0.2.11"},
            "file": {"code_signature": {"valid": False}},
            "tags": ["prod", "secure"],
        },
    )
    add_parsed_log(
        auth_database,
        source_id=other_source_id,
        ecs_data={"@timestamp": "2026-01-03T12:00:00Z", "event": {"action": "login"}},
    )
    add_parsed_log(
        auth_database,
        source_id=source_id,
        ecs_data={"@timestamp": "historically-invalid", "event": {"action": "login"}},
    )

    fields = client.get(
        f"/api/v1/events/sources/{source_id}/fields?q=event.action", headers=headers
    )
    assert fields.status_code == 200
    assert any(item["name"] == "event.action" for item in fields.json()["items"])

    first = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"limit": 1},
        headers=headers,
    )
    assert first.status_code == 200, first.text
    result = first.json()
    assert result["has_more"] is True
    assert result["items"][0]["event_timestamp"] == "2026-01-02T12:00:00Z"
    assert result["items"][0]["id"] != str(uuid4())
    assert not result["items"][0]["id"].startswith(str(source_id))

    card = client.get(
        f"/api/v1/events/sources/{source_id}/events/{result['items'][0]['id']}",
        headers=headers,
    )
    assert card.status_code == 200, card.text
    assert card.json()["source_type"] == "kafka"
    assert card.json()["details"]["raw"] == "login event"
    assert "normalization_diagnostics" in card.json()["details"]["metadata"]
    assert (
        client.get(
            f"/api/v1/events/sources/{other_source_id}/events/{result['items'][0]['id']}",
            headers=headers,
        ).status_code
        == 404
    )

    second = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"limit": 1, "cursor": result["next_cursor"]},
        headers=headers,
    )
    assert second.status_code == 200, second.text
    assert second.json()["items"][0]["event_timestamp"] == "2026-01-01T12:00:00Z"

    for condition in (
        {"field": "event.action", "operator": "eq", "value": "login"},
        {"field": "event.action", "operator": "in", "value": ["login", "logout"]},
        {"field": "event.action", "operator": "exists"},
        {"field": "source.ip", "operator": "eq", "value": "192.0.2.10"},
        {"field": "source.ip", "operator": "in", "value": ["192.0.2.10"]},
        {"field": "event.duration", "operator": "gt", "value": 600},
        {"field": "event.duration", "operator": "gte", "value": 700},
        {"field": "event.duration", "operator": "lt", "value": 600},
        {"field": "event.duration", "operator": "lte", "value": 500},
        {"field": "event.duration", "operator": "neq", "value": 500},
        {"field": "event.action", "operator": "contains", "value": "out"},
        {"field": "event.action", "operator": "contains", "value": "%"},
        {"field": "event.action", "operator": "contains", "value": "LOG"},
        {"field": "event.action", "operator": "starts_with", "value": "log"},
        {"field": "event.action", "operator": "ends_with", "value": "out"},
        {"field": "event.created", "operator": "eq", "value": "2026-01-02T11:00:00Z"},
        {"field": "event.created", "operator": "gt", "value": "2025-12-31T11:00:00Z"},
        {"field": "file.code_signature.valid", "operator": "in", "value": [True]},
        {"field": "tags", "operator": "in", "value": ["prod"]},
        {"field": "tags", "operator": "contains", "value": "cur"},
        {"field": "host.name", "operator": "not_exists"},
        {"field": "event.action", "operator": "neq", "value": "login"},
    ):
        response = client.post(
            f"/api/v1/events/sources/{source_id}/search",
            json={"filters": [condition]},
            headers=headers,
        )
        assert response.status_code == 200, (condition, response.text)
        if condition["operator"] == "contains" and condition.get("value") in {"%", "LOG"}:
            expected_count = 0
        else:
            expected_count = (
                2
                if condition["operator"] in {"starts_with", "not_exists", "exists"}
                or condition["field"] == "tags"
                or (condition["field"] == "event.action" and condition["operator"] == "in")
                or (condition["field"] == "event.created" and condition["operator"] == "gt")
                else 1
            )
        assert len(response.json()["items"]) == expected_count, condition

    bounded = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={
            "timestamp_from": "2026-01-02T12:00:00+00:00",
            "timestamp_to": "2026-01-03T12:00:00+00:00",
        },
        headers=headers,
    )
    assert bounded.status_code == 200 and len(bounded.json()["items"]) == 1

    filtered = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"filters": [{"field": "event.action", "operator": "in", "value": ["login"]}]},
        headers=headers,
    )
    assert filtered.status_code == 200, filtered.text
    assert len(filtered.json()["items"]) == 1
    tampered = result["next_cursor"][:8] + "A" + result["next_cursor"][9:]
    rejected = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"limit": 1, "cursor": tampered},
        headers=headers,
    )
    assert rejected.status_code == 422 and rejected.json()["code"] == "invalid_cursor"
    changed_query = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"limit": 1, "sort": "asc", "cursor": result["next_cursor"]},
        headers=headers,
    )
    assert changed_query.status_code == 422 and changed_query.json()["code"] == "invalid_cursor"
    assert (
        client.get(f"/api/v1/events/sources/{uuid4()}/fields", headers=headers).status_code == 404
    )
    legacy_catalog = client.get("/api/v1/ecs/fields/event.action", headers=headers).json()
    assert not ({"in", "starts_with", "ends_with"} & set(legacy_catalog["operators"]))
    for operator, value in (("in", ["login"]), ("starts_with", "log"), ("ends_with", "out")):
        legacy = client.post(
            "/api/v1/parsed-logs/search",
            json={"ecs_filters": [{"field": "event.action", "operator": operator, "value": value}]},
            headers=headers,
        )
        assert legacy.status_code == 422
        assert legacy.json()["code"] == "ecs_filter_invalid"
    assert client.post("/api/v1/auth/logout", headers=headers).status_code == 200
    assert client.get(f"/api/v1/events/sources/{source_id}/fields").status_code == 401


def test_event_query_null_presence_neq_ties_sort_and_guest_permissions(client, auth_database):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    headers = events_headers(client)
    _, sources = add_source_pair(auth_database)
    source_id = sources[0][0]
    records = [
        {"@timestamp": "2026-01-01T00:00:00Z", "tags": ["safe"], "event": {"duration": 100, "created": "invalid-date"}},
        {"@timestamp": "2026-01-01T05:00:00+05:00", "tags": [], "event": {"action": None, "duration": 200, "created": "2026-01-01T00:00:00Z"}},
        {"@timestamp": "2026-01-02T00:00:00Z", "tags": ["other"], "event": {"action": "", "duration": 300, "created": "2026-01-02T00:00:00Z"}},
        {"@timestamp": "2026-01-03T00:00:00Z", "event": {"action": "ok", "duration": 400, "created": "2026-01-03T00:00:00Z"}},
    ]
    for document in records:
        add_parsed_log(auth_database, source_id=source_id, ecs_data=document)

    def query(filters):
        response = client.post(
            f"/api/v1/events/sources/{source_id}/search",
            json={"filters": filters},
            headers=headers,
        )
        assert response.status_code == 200, response.text
        return response.json()["items"]

    assert len(query([{"field": "event.action", "operator": "exists"}])) == 3
    assert len(query([{"field": "event.action", "operator": "not_exists"}])) == 1
    assert len(query([{"field": "event.action", "operator": "eq", "value": ""}])) == 1
    assert len(query([{"field": "event.action", "operator": "neq", "value": "ok"}])) == 1
    assert len(query([{"field": "tags", "operator": "neq", "value": "safe"}])) == 2
    assert len(query([{"field": "event.duration", "operator": "in", "value": [100, 400]}])) == 2
    assert len(query([{"field": "event.created", "operator": "gt", "value": "2025-12-31T00:00:00Z"}])) == 3
    assert len(query([{"field": "event.action", "operator": "eq", "value": "ok"},
                      {"field": "event.duration", "operator": "eq", "value": 400}])) == 1
    assert len(query([{"field": "event.action", "operator": "eq", "value": "ok"},
                      {"field": "event.duration", "operator": "eq", "value": 300}])) == 0

    tied = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"timestamp_from": "2026-01-01T00:00:00Z",
              "timestamp_to": "2026-01-02T00:00:00Z", "limit": 1, "sort": "asc"},
        headers=headers,
    ).json()
    assert tied["has_more"] is True
    tied_next = client.post(
        f"/api/v1/events/sources/{source_id}/search",
        json={"timestamp_from": "2026-01-01T00:00:00Z",
              "timestamp_to": "2026-01-02T00:00:00Z", "limit": 1,
              "sort": "asc", "cursor": tied["next_cursor"]},
        headers=headers,
    ).json()
    assert tied_next["items"][0]["event_timestamp"] == tied["items"][0]["event_timestamp"]
    assert tied_next["items"][0]["id"] != tied["items"][0]["id"]

    descending = client.post(
        f"/api/v1/events/sources/{source_id}/search", json={"sort": "desc"}, headers=headers
    ).json()["items"]
    ascending = client.post(
        f"/api/v1/events/sources/{source_id}/search", json={"sort": "asc"}, headers=headers
    ).json()["items"]
    assert descending[0]["event_timestamp"] > descending[-1]["event_timestamp"]
    assert ascending[0]["event_timestamp"] < ascending[-1]["event_timestamp"]

    for condition in (
        {"field": "source.ip", "operator": "eq", "value": "not-an-ip"},
        {"field": "event.duration", "operator": "contains", "value": "100"},
        {"field": "wazuh.fake.field", "operator": "eq", "value": "bad"},
        {"field": "event.duration", "operator": "in", "value": []},
    ):
        invalid = client.post(
            f"/api/v1/events/sources/{source_id}/search",
            json={"filters": [condition]},
            headers=headers,
        )
        assert invalid.status_code == 422, (condition, invalid.text)

    with auth_database.session_factory() as session:
        guest = session.get(Role, GUEST_ID)
        original_permissions = list(guest.permissions)
        guest.permissions = [permission for permission in original_permissions if permission != "events.read"]
        session.commit()
    try:
        denied = client.post(
            f"/api/v1/events/sources/{source_id}/search", json={}, headers=headers
        )
        assert denied.status_code == 403
    finally:
        with auth_database.session_factory() as session:
            guest = session.get(Role, GUEST_ID)
            guest.permissions = original_permissions
            session.commit()


def test_event_cursor_excludes_later_inserts_and_skips_deleted_rows(client, auth_database):
    add_local_user(auth_database)
    assert login(client).status_code == 200
    headers = events_headers(client)
    _, sources = add_source_pair(auth_database)
    source_id = sources[0][0]
    identities = [
        add_parsed_log(
            auth_database,
            source_id=source_id,
            timestamp=datetime(2026, 1, day, tzinfo=UTC),
            ecs_data={"@timestamp": f"2026-01-{day:02d}T00:00:00Z", "event": {"action": str(day)}},
        )
        for day in (1, 2, 3)
    ]
    route = f"/api/v1/events/sources/{source_id}/search"
    first = client.post(route, json={"limit": 1}, headers=headers).json()
    assert first["has_more"] is True
    inserted = add_parsed_log(
        auth_database,
        source_id=source_id,
        timestamp=datetime(2026, 1, 4, tzinfo=UTC),
        ecs_data={"@timestamp": "2026-01-04T00:00:00Z", "event": {"action": "late"}},
    )
    with auth_database.session_factory() as session:
        session.delete(session.get(ParsedLog, identities[1]))
        session.commit()
    second = client.post(
        route, json={"limit": 1, "cursor": first["next_cursor"]}, headers=headers
    ).json()
    assert [item["fields"]["@timestamp"] for item in second["items"]] == ["2026-01-01T00:00:00Z"]
    assert second["has_more"] is False
    assert all(item["id"] != str(inserted) for item in second["items"])
