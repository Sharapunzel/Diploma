import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb
from sqlalchemy.engine import make_url

BACKEND_DIR = Path(__file__).resolve().parents[1]
ADMIN_ID = UUID("00000000-0000-4000-8000-000000000001")
GUEST_ID = UUID("00000000-0000-4000-8000-000000000002")
ADMIN_PERMISSIONS = [
    "users.read",
    "users.write",
    "settings.read",
    "settings.write",
    "connections.read",
    "connections.write",
    "sources.read",
    "sources.write",
    "normalizers.read",
    "normalizers.write",
    "events.read",
]
GUEST_PERMISSIONS = [
    "users.read",
    "settings.read",
    "connections.read",
    "sources.read",
    "normalizers.read",
    "events.read",
]


@pytest.fixture(scope="session")
def database_urls():
    raw_url = os.getenv("TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail(
            "TEST_DATABASE_URL must point to a separate PostgreSQL test database"
        )

    parsed = make_url(raw_url)
    if parsed.database == "diploma_db":
        pytest.fail("Tests must never run against the development database diploma_db")
    if parsed.get_backend_name() != "postgresql":
        pytest.fail("TEST_DATABASE_URL must use PostgreSQL")

    sqlalchemy_url = parsed.render_as_string(hide_password=False)
    psycopg_url = parsed.set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    return sqlalchemy_url, psycopg_url


def run_alembic(sqlalchemy_url, *arguments):
    environment = {**os.environ, "DATABASE_URL": sqlalchemy_url}
    return subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=BACKEND_DIR,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def db(database_urls):
    sqlalchemy_url, psycopg_url = database_urls
    run_alembic(sqlalchemy_url, "downgrade", "base")
    run_alembic(sqlalchemy_url, "upgrade", "head")
    connection = psycopg.connect(psycopg_url)
    try:
        yield connection
    finally:
        connection.close()
        run_alembic(sqlalchemy_url, "downgrade", "base")


def expect_constraint(db, statement, parameters=()):
    with pytest.raises(
        (psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation)
    ):
        db.execute(statement, parameters)
    db.rollback()


def insert_user(db, **overrides):
    values = {"username": "user", "password_hash": "hash", "display_name": "User"}
    values.update(overrides)
    columns = ", ".join(values)
    placeholders = ", ".join(["%s"] * len(values))
    query = f"INSERT INTO app.users ({columns}) VALUES ({placeholders}) RETURNING id"
    return db.execute(query, list(values.values())).fetchone()[0]


def insert_normalizer(db, user_id=None):
    return db.execute(
        """
        INSERT INTO app.normalizers
            (name, rule, created_by_user_id, updated_by_user_id)
        VALUES (%s, '{}', %s, %s)
        RETURNING id
        """,
        (f"normalizer-{uuid4()}", user_id, user_id),
    ).fetchone()[0]


def insert_connection(db):
    return db.execute(
        """
        INSERT INTO app.kafka_connections (name, bootstrap_servers)
        VALUES (%s, %s)
        RETURNING id
        """,
        (f"connection-{uuid4()}", ["broker:9092"]),
    ).fetchone()[0]


def insert_source(db, connection_id, normalizer_id=None):
    return db.execute(
        """
        INSERT INTO app.sources (name, connection_id, normalizer_id, topic_name)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        ("Source snapshot", connection_id, normalizer_id, f"topic-{uuid4()}"),
    ).fetchone()[0]


def insert_parsed_log(
    db,
    *,
    source_id=None,
    connection_id=None,
    normalizer_id=None,
    deduplication_key=None,
    **overrides,
):
    now = datetime.now(timezone.utc)
    values = {
        "source_id": source_id,
        "connection_id": connection_id,
        "normalizer_id": normalizer_id,
        "normalizer_version": 1,
        "source_name": "Source snapshot",
        "connection_name": "Connection snapshot",
        "kafka_topic": "events",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "deduplication_key": deduplication_key or str(uuid4()),
        "fluent_bit_collected_at": now,
        "backend_received_at": now,
        "backend_processed_at": now + timedelta(seconds=1),
        "raw": "raw event",
        "ecs_data": {"event": {"kind": "event"}},
    }
    values.update(overrides)
    values["ecs_data"] = Jsonb(values["ecs_data"])
    columns = ", ".join(values)
    placeholders = ", ".join(["%s"] * len(values))
    query = (
        f"INSERT INTO logs.parsed_logs ({columns}) VALUES ({placeholders}) RETURNING id"
    )
    return db.execute(query, list(values.values())).fetchone()[0]


def create_graph(db):
    user_id = insert_user(db)
    normalizer_id = insert_normalizer(db, user_id)
    connection_id = insert_connection(db)
    source_id = insert_source(db, connection_id, normalizer_id)
    log_id = insert_parsed_log(
        db,
        source_id=source_id,
        connection_id=connection_id,
        normalizer_id=normalizer_id,
    )
    db.commit()
    return user_id, normalizer_id, connection_id, source_id, log_id


def test_upgrade_downgrade_upgrade_and_alembic_check(database_urls):
    sqlalchemy_url, _ = database_urls
    try:
        run_alembic(sqlalchemy_url, "downgrade", "base")
        run_alembic(sqlalchemy_url, "upgrade", "head")
        run_alembic(sqlalchemy_url, "downgrade", "base")
        run_alembic(sqlalchemy_url, "upgrade", "head")
        result = run_alembic(sqlalchemy_url, "check")
        assert "No new upgrade operations detected" in result.stdout
    finally:
        run_alembic(sqlalchemy_url, "downgrade", "base")


def test_server_uuid_for_every_table_and_explicit_uuid(db):
    role_id = db.execute(
        "INSERT INTO app.roles (name, permissions, priority) VALUES ('Analyst', '[]', 1) RETURNING id"
    ).fetchone()[0]
    user_id = insert_user(db)
    mapping_id = db.execute("""
        INSERT INTO app.oidc_role_mappings (issuer, claim_name, claim_value)
        VALUES ('issuer', 'groups', 'analysts') RETURNING id
        """).fetchone()[0]
    setting_id = db.execute(
        "INSERT INTO app.app_settings (key, value, category) VALUES ('key', 'true', 'test') RETURNING id"
    ).fetchone()[0]
    normalizer_id = insert_normalizer(db, user_id)
    connection_id = insert_connection(db)
    source_id = insert_source(db, connection_id, normalizer_id)
    log_id = insert_parsed_log(
        db,
        source_id=source_id,
        connection_id=connection_id,
        normalizer_id=normalizer_id,
    )
    assert all(
        isinstance(value, UUID)
        for value in [
            role_id,
            user_id,
            mapping_id,
            setting_id,
            normalizer_id,
            connection_id,
            source_id,
            log_id,
        ]
    )

    explicit_id = uuid4()
    saved_id = db.execute(
        """
        INSERT INTO app.users (id, username, password_hash, display_name)
        VALUES (%s, 'explicit', 'hash', 'Explicit') RETURNING id
        """,
        (explicit_id,),
    ).fetchone()[0]
    assert saved_id == explicit_id


def test_seed_roles_are_exact_and_restored_for_every_test(db):
    roles = db.execute(
        "SELECT id, name, priority, permissions FROM app.roles ORDER BY priority DESC"
    ).fetchall()
    assert roles == [
        (ADMIN_ID, "Administrator", 100, ADMIN_PERMISSIONS),
        (GUEST_ID, "Guest", 10, GUEST_PERMISSIONS),
    ]


def test_valid_user_login_combinations(db):
    local_id = insert_user(db, username="local", password_hash="hash")
    oidc_id = insert_user(
        db,
        username=None,
        password_hash=None,
        oidc_issuer="issuer",
        oidc_subject="oidc-only",
    )
    both_id = insert_user(
        db,
        username="both",
        password_hash="hash",
        oidc_issuer="issuer",
        oidc_subject="both",
    )
    assert {local_id, oidc_id, both_id} == {
        row[0] for row in db.execute("SELECT id FROM app.users").fetchall()
    }


@pytest.mark.parametrize(
    "columns, values",
    [
        ("display_name", ("No login",)),
        ("username, display_name", ("user", "Incomplete local")),
        ("password_hash, display_name", ("hash", "Incomplete local")),
        ("oidc_issuer, display_name", ("issuer", "Incomplete OIDC")),
        ("oidc_subject, display_name", ("subject", "Incomplete OIDC")),
        ("username, password_hash, display_name", ("", "hash", "Blank username")),
        ("username, password_hash, display_name", ("user", "  ", "Blank hash")),
        ("oidc_issuer, oidc_subject, display_name", ("", "subject", "Blank issuer")),
        ("oidc_issuer, oidc_subject, display_name", ("issuer", " ", "Blank subject")),
        ("username, password_hash, display_name", ("user", "hash", "  ")),
    ],
)
def test_invalid_user_login_combinations(db, columns, values):
    placeholders = ", ".join(["%s"] * len(values))
    expect_constraint(
        db,
        f"INSERT INTO app.users ({columns}) VALUES ({placeholders})",
        values,
    )


def test_user_and_oidc_mapping_uniqueness(db):
    insert_user(db, username="Admin")
    db.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        insert_user(db, username="admin")
    db.rollback()

    insert_user(
        db,
        username=None,
        password_hash=None,
        oidc_issuer="issuer",
        oidc_subject="subject",
    )
    db.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        insert_user(
            db,
            username=None,
            password_hash=None,
            oidc_issuer="issuer",
            oidc_subject="subject",
        )
    db.rollback()

    statement = """
        INSERT INTO app.oidc_role_mappings (issuer, claim_name, claim_value)
        VALUES ('issuer', 'groups', 'admin')
    """
    db.execute(statement)
    db.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(statement)
    db.rollback()


def test_defaults_and_version_triggers(db):
    user_id = insert_user(db)
    user_defaults = db.execute(
        "SELECT is_active, role_managed_by_oidc FROM app.users WHERE id = %s",
        (user_id,),
    ).fetchone()
    assert user_defaults == (True, False)

    normalizer_id = insert_normalizer(db, user_id)
    setting_id = db.execute("""
        INSERT INTO app.app_settings (key, value, category)
        VALUES ('setting', 'false', 'test') RETURNING id
        """).fetchone()[0]
    connection_id = insert_connection(db)
    source_id = insert_source(db, connection_id, normalizer_id)
    assert (
        db.execute(
            "SELECT is_enabled FROM app.sources WHERE id = %s", (source_id,)
        ).fetchone()[0]
        is False
    )
    assert (
        db.execute(
            "SELECT version FROM app.normalizers WHERE id = %s", (normalizer_id,)
        ).fetchone()[0]
        == 1
    )
    assert (
        db.execute(
            "SELECT version FROM app.app_settings WHERE id = %s", (setting_id,)
        ).fetchone()[0]
        == 1
    )
    assert (
        db.execute(
            "UPDATE app.normalizers SET version = 99 WHERE id = %s RETURNING version",
            (normalizer_id,),
        ).fetchone()[0]
        == 2
    )
    assert (
        db.execute(
            "UPDATE app.app_settings SET version = 99 WHERE id = %s RETURNING version",
            (setting_id,),
        ).fetchone()[0]
        == 2
    )


@pytest.mark.parametrize("bootstrap_servers", [[], [""], ["  "], [None]])
def test_invalid_bootstrap_servers(db, bootstrap_servers):
    expect_constraint(
        db,
        "INSERT INTO app.kafka_connections (name, bootstrap_servers) VALUES (%s, %s)",
        (f"invalid-{uuid4()}", bootstrap_servers),
    )


def test_source_uniqueness_and_positive_bootstrap(db):
    connection_id = insert_connection(db)
    first_source = insert_source(db, connection_id)
    topic = db.execute(
        "SELECT topic_name FROM app.sources WHERE id = %s", (first_source,)
    ).fetchone()[0]
    db.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(
            """
            INSERT INTO app.sources (name, connection_id, topic_name)
            VALUES ('Duplicate', %s, %s)
            """,
            (connection_id, topic),
        )
    db.rollback()


def test_valid_parsed_log_and_required_indexes(db):
    log_id = insert_parsed_log(db)
    assert isinstance(log_id, UUID)
    indexes = {
        row[0]
        for row in db.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'logs'"
        ).fetchall()
    }
    assert "ix_parsed_logs_connection_id" in indexes
    assert "ix_parsed_logs_normalizer_id" in indexes


@pytest.mark.parametrize(
    "overrides",
    [
        {"normalizer_version": 0},
        {"kafka_partition": -1},
        {"kafka_offset": -1},
        {"ecs_data": []},
        {
            "backend_received_at": datetime.now(timezone.utc),
            "backend_processed_at": datetime.now(timezone.utc) - timedelta(seconds=1),
        },
    ],
)
def test_parsed_log_check_constraints(db, overrides):
    with pytest.raises(psycopg.errors.CheckViolation):
        insert_parsed_log(db, **overrides)
    db.rollback()


def test_parsed_log_deduplication_key_is_unique(db):
    insert_parsed_log(db, deduplication_key="same-key")
    db.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        insert_parsed_log(db, deduplication_key="same-key")
    db.rollback()


def test_delete_connection_cascades_source_and_preserves_log(db):
    _, _, connection_id, source_id, log_id = create_graph(db)
    db.execute("DELETE FROM app.kafka_connections WHERE id = %s", (connection_id,))
    assert (
        db.execute(
            "SELECT count(*) FROM app.sources WHERE id = %s", (source_id,)
        ).fetchone()[0]
        == 0
    )
    log = db.execute(
        """
        SELECT source_id, connection_id, source_name, connection_name
        FROM logs.parsed_logs WHERE id = %s
        """,
        (log_id,),
    ).fetchone()
    assert log == (None, None, "Source snapshot", "Connection snapshot")


def test_delete_source_preserves_log(db):
    _, _, _, source_id, log_id = create_graph(db)
    db.execute("DELETE FROM app.sources WHERE id = %s", (source_id,))
    assert (
        db.execute(
            "SELECT source_id FROM logs.parsed_logs WHERE id = %s", (log_id,)
        ).fetchone()[0]
        is None
    )


def test_delete_normalizer_preserves_source_log_and_version(db):
    _, normalizer_id, _, source_id, log_id = create_graph(db)
    db.execute("DELETE FROM app.normalizers WHERE id = %s", (normalizer_id,))
    assert (
        db.execute(
            "SELECT normalizer_id FROM app.sources WHERE id = %s", (source_id,)
        ).fetchone()[0]
        is None
    )
    assert db.execute(
        "SELECT normalizer_id, normalizer_version FROM logs.parsed_logs WHERE id = %s",
        (log_id,),
    ).fetchone() == (None, 1)


def test_delete_role_preserves_users_and_mappings(db):
    user_id = insert_user(db, role_id=ADMIN_ID)
    mapping_id = db.execute(
        """
        INSERT INTO app.oidc_role_mappings (issuer, claim_name, claim_value, role_id)
        VALUES ('issuer', 'groups', 'admins', %s) RETURNING id
        """,
        (ADMIN_ID,),
    ).fetchone()[0]
    db.commit()
    db.execute("DELETE FROM app.roles WHERE id = %s", (ADMIN_ID,))
    assert (
        db.execute(
            "SELECT role_id FROM app.users WHERE id = %s", (user_id,)
        ).fetchone()[0]
        is None
    )
    assert (
        db.execute(
            "SELECT role_id FROM app.oidc_role_mappings WHERE id = %s", (mapping_id,)
        ).fetchone()[0]
        is None
    )


def test_delete_user_preserves_normalizers_and_settings(db):
    user_id = insert_user(db)
    normalizer_id = insert_normalizer(db, user_id)
    setting_id = db.execute(
        """
        INSERT INTO app.app_settings (key, value, category, updated_by_user_id)
        VALUES ('owned-setting', 'true', 'test', %s) RETURNING id
        """,
        (user_id,),
    ).fetchone()[0]
    db.commit()
    db.execute("DELETE FROM app.users WHERE id = %s", (user_id,))
    assert (
        db.execute(
            """
        SELECT created_by_user_id, updated_by_user_id
        FROM app.normalizers WHERE id = %s
        """,
            (normalizer_id,),
        ).fetchone()
        == (None, None)
    )
    assert (
        db.execute(
            "SELECT updated_by_user_id FROM app.app_settings WHERE id = %s",
            (setting_id,),
        ).fetchone()[0]
        is None
    )
