import ssl
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from psycopg.errors import CheckViolation, UniqueViolation
from pydantic import ValidationError
from test_database_foundation import (
    database_urls as foundation_database_urls,
)
from test_database_foundation import (
    db as foundation_db,
)
from test_database_foundation import (
    insert_connection,
    insert_parsed_log,
    insert_source,
    run_alembic,
)

from app.core.errors import DomainError
from app.indexer import HttpIndexerMetadataAdapter, IndexerError, matching_indices, validate_ca
from app.schemas.external import (
    ExternalConnectionCreate,
    ExternalConnectionPatch,
    ExternalSourceCreate,
    valid_target,
    valid_url,
)
from app.services.implementations.external import ExternalServiceImpl

database_urls = foundation_database_urls
db = foundation_db


class MemoryRepository:
    def __init__(self):
        self.items = {}

    def connection(self, identity, lock=False):
        return self.items.get(identity)

    def add(self, item):
        item.id = uuid4()
        item.created_at = item.updated_at = datetime.now(UTC)
        self.items[item.id] = item

    def disable_sources(self, identity):
        pass


class UnitOfWork:
    def commit(self):
        pass

    def rollback(self):
        pass


class Adapter:
    def indices(self, base_url, username, password, ca_pem):
        assert password == "correct-secret"
        return ["logs-2026", "logs-2027"]


def test_external_secret_survives_service_restart_and_patch_without_password():
    key = Fernet.generate_key().decode()
    repo = MemoryRepository()
    service = ExternalServiceImpl(repo, UnitOfWork(), Adapter(), key)
    item = service.create_connection(ExternalConnectionCreate(
        name="cluster", base_url="https://indexer.local:9200/prefix",
        username="analyst", password="correct-secret"))
    assert item.encrypted_password != "correct-secret"
    assert "correct-secret" not in item.encrypted_password
    saved = item.encrypted_password
    restarted = ExternalServiceImpl(repo, UnitOfWork(), Adapter(), key)
    assert restarted.test(item.id) == {"status": "ok"}
    restarted.update_connection(item.id, ExternalConnectionPatch(name="renamed"))
    assert item.encrypted_password == saved
    assert restarted.indices(item.id, 1, 1) == (["logs-2027"], 2)
    wrong = ExternalServiceImpl(repo, UnitOfWork(), Adapter(), Fernet.generate_key().decode())
    with pytest.raises(DomainError) as error:
        wrong.test(item.id)
    assert error.value.code == "external_key_mismatch"


def test_missing_key_and_invalid_inputs_are_rejected():
    service = ExternalServiceImpl(MemoryRepository(), UnitOfWork(), Adapter(), None)
    with pytest.raises(DomainError) as error:
        service.create_connection(ExternalConnectionCreate(
            name="cluster", base_url="https://indexer.local", username="analyst",
            password="correct-secret"))
    assert error.value.code == "external_key_unavailable"
    for url in ("http://localhost", "https://user:pass@host", "https://host?q=1",
                "https://host/#x", "https://host/%2e%2e"):
        with pytest.raises(ValueError):
            valid_url(url)
    for target in ("logs,*", "_cat/indices", "Logs-*", "logs-2026"):
        with pytest.raises(ValueError):
            valid_target(target, True)
    with pytest.raises(ValidationError):
        ExternalSourceCreate(name="a", external_connection_id=uuid4(), target_type="index",
                             index_name="logs-a", index_pattern="logs-*")
    assert matching_indices(["logs-1", "other"], None, "logs-*") == ["logs-1"]
    for payload in (b"", b"not a certificate", b"-----BEGIN PRIVATE KEY-----abc"):
        with pytest.raises(ValueError):
            validate_ca(payload)


def test_source_constraints_cascade_and_kafka_history(db):
    kafka_id = insert_connection(db)
    kafka_source_id = insert_source(db, kafka_id)
    log_id = insert_parsed_log(db, source_id=kafka_source_id, connection_id=kafka_id)
    external_id = db.execute("""
        INSERT INTO app.external_connections (name, base_url, username, encrypted_password)
        VALUES (%s, 'https://indexer.local', 'reader', 'ciphertext') RETURNING id
    """, (f"external-{uuid4()}",)).fetchone()[0]
    external_source_id = db.execute("""
        INSERT INTO app.sources (name, source_type, external_connection_id, target_type, index_pattern)
        VALUES ('external', 'external', %s, 'index_pattern', 'logs-*') RETURNING id
    """, (external_id,)).fetchone()[0]
    stream_id = db.execute("""
        INSERT INTO app.sources (name, source_type, external_connection_id,
                                 target_type, data_stream_name)
        VALUES ('findings', 'external', %s, 'data_stream', 'wazuh-findings-v5-security')
        RETURNING id
    """, (external_id,)).fetchone()[0]
    db.commit()
    with pytest.raises(UniqueViolation), db.transaction():
        db.execute("""
            INSERT INTO app.sources (name, source_type, external_connection_id,
                                     target_type, data_stream_name)
            VALUES ('duplicate-stream', 'external', %s, 'data_stream', 'wazuh-findings-v5-security')
        """, (external_id,))
    with pytest.raises(CheckViolation), db.transaction():
        db.execute("""
            INSERT INTO app.sources (name, source_type, external_connection_id,
                                     target_type, data_stream_name, index_name)
            VALUES ('mixed', 'external', %s, 'data_stream', 'wazuh-findings-v5-security', 'logs-1')
        """, (external_id,))
    with pytest.raises(CheckViolation), db.transaction():
        db.execute("""
            INSERT INTO app.sources (name, source_type, external_connection_id, index_name)
            VALUES ('missing-type', 'external', %s, 'logs-1')
        """, (external_id,))
    with pytest.raises(CheckViolation), db.transaction():
        db.execute("""
                INSERT INTO app.sources (name, source_type, external_connection_id,
                                         connection_id, index_name)
                VALUES ('invalid', 'external', %s, %s, 'logs-1')
            """, (external_id, kafka_id))
    with pytest.raises(UniqueViolation), db.transaction():
        db.execute("""
                INSERT INTO app.sources (name, source_type, external_connection_id, target_type, index_pattern)
                VALUES ('duplicate', 'external', %s, 'index_pattern', 'logs-*')
            """, (external_id,))
    db.execute("DELETE FROM app.external_connections WHERE id = %s", (external_id,))
    assert db.execute("SELECT count(*) FROM app.sources WHERE id = %s",
                      (external_source_id,)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM app.sources WHERE id = %s",
                      (stream_id,)).fetchone()[0] == 0
    assert db.execute("SELECT source_type FROM app.sources WHERE id = %s",
                      (kafka_source_id,)).fetchone()[0] == "kafka"
    assert db.execute("SELECT source_id FROM logs.parsed_logs WHERE id = %s",
                      (log_id,)).fetchone()[0] == kafka_source_id
    db.commit()
    db.execute("DELETE FROM logs.parsed_logs WHERE id = %s", (log_id,))
    db.execute("DELETE FROM app.sources WHERE id = %s", (kafka_source_id,))
    db.execute("DELETE FROM app.kafka_connections WHERE id = %s", (kafka_id,))
    db.commit()


def test_rotation_reencrypts_existing_password(db, database_urls, monkeypatch):
    from scripts.rotate_external_key import main

    old_key = Fernet.generate_key()
    new_key = Fernet.generate_key()
    encrypted = Fernet(old_key).encrypt(b"correct-secret").decode()
    identity = db.execute("""
        INSERT INTO app.external_connections (name, base_url, username, encrypted_password)
        VALUES (%s, 'https://indexer.local', 'reader', %s) RETURNING id
    """, (f"rotation-{uuid4()}", encrypted)).fetchone()[0]
    db.commit()
    monkeypatch.setenv("DATABASE_URL", database_urls[0])
    monkeypatch.setenv("EXTERNAL_OLD_KEY", old_key.decode())
    monkeypatch.setenv("EXTERNAL_NEW_KEY", new_key.decode())
    main()
    rotated = db.execute("SELECT encrypted_password FROM app.external_connections WHERE id = %s",
                         (identity,)).fetchone()[0]
    assert rotated != encrypted
    assert Fernet(new_key).decrypt(rotated.encode()) == b"correct-secret"
    db.execute("DELETE FROM app.external_connections WHERE id = %s", (identity,))
    db.commit()


def test_migration_preserves_existing_kafka_source_and_log(database_urls):
    import psycopg

    sqlalchemy_url, psycopg_url = database_urls
    try:
        run_alembic(sqlalchemy_url, "downgrade", "base")
        run_alembic(sqlalchemy_url, "upgrade", "0008_processing_error_identity")
        with psycopg.connect(psycopg_url) as connection:
            kafka_id = insert_connection(connection)
            source_id = insert_source(connection, kafka_id)
            log_id = insert_parsed_log(connection, source_id=source_id,
                                       connection_id=kafka_id)
            now = datetime.now(UTC)
            record_id = connection.execute("""
                INSERT INTO logs.processed_kafka_records
                    (connection_id, connection_identity, source_id, kafka_topic,
                     kafka_partition, kafka_offset, result_status,
                     backend_received_at, backend_processed_at)
                VALUES (%s, %s, %s, 'events', 0, 42, 'complete', %s, %s)
                RETURNING id
            """, (kafka_id, kafka_id, source_id, now, now)).fetchone()[0]
            connection.commit()
        run_alembic(sqlalchemy_url, "upgrade", "head")
        with psycopg.connect(psycopg_url) as connection:
            row = connection.execute("""
                SELECT source_type, connection_id, topic_name, external_connection_id
                FROM app.sources WHERE id = %s
            """, (source_id,)).fetchone()
            assert row[0] == "kafka" and row[1] == kafka_id
            assert row[2] and row[3] is None
            assert connection.execute("SELECT source_id FROM logs.parsed_logs WHERE id = %s",
                                      (log_id,)).fetchone()[0] == source_id
            assert connection.execute("SELECT source_id FROM logs.processed_kafka_records WHERE id = %s",
                                      (record_id,)).fetchone()[0] == source_id
        run_alembic(sqlalchemy_url, "downgrade", "0008_processing_error_identity")
        with psycopg.connect(psycopg_url) as connection:
            assert connection.execute("SELECT id FROM app.sources WHERE id = %s",
                                      (source_id,)).fetchone()[0] == source_id
            assert connection.execute("SELECT source_id FROM logs.parsed_logs WHERE id = %s",
                                      (log_id,)).fetchone()[0] == source_id
            assert connection.execute("SELECT source_id FROM logs.processed_kafka_records WHERE id = %s",
                                      (record_id,)).fetchone()[0] == source_id
            connection.execute("DELETE FROM logs.parsed_logs WHERE id = %s", (log_id,))
            connection.execute("DELETE FROM logs.processed_kafka_records WHERE id = %s",
                               (record_id,))
            connection.execute("DELETE FROM app.sources WHERE id = %s", (source_id,))
            connection.execute("DELETE FROM app.kafka_connections WHERE id = %s",
                               (kafka_id,))
    finally:
        run_alembic(sqlalchemy_url, "downgrade", "base")


def test_data_stream_migration_backfills_indexes_and_guards_downgrade(database_urls):
    import subprocess

    import psycopg

    sqlalchemy_url, psycopg_url = database_urls
    try:
        run_alembic(sqlalchemy_url, "downgrade", "base")
        run_alembic(sqlalchemy_url, "upgrade", "0009_external_sources")
        with psycopg.connect(psycopg_url) as connection:
            external_id = connection.execute("""
                INSERT INTO app.external_connections (name, base_url, username, encrypted_password)
                VALUES ('old', 'https://indexer.local', 'reader', 'ciphertext') RETURNING id
            """).fetchone()[0]
            exact_id = connection.execute("""
                INSERT INTO app.sources (name, source_type, external_connection_id, index_name)
                VALUES ('old-index', 'external', %s, 'logs-2026') RETURNING id
            """, (external_id,)).fetchone()[0]
            pattern_id = connection.execute("""
                INSERT INTO app.sources (name, source_type, external_connection_id, index_pattern)
                VALUES ('old-pattern', 'external', %s, 'logs-*') RETURNING id
            """, (external_id,)).fetchone()[0]
        run_alembic(sqlalchemy_url, "upgrade", "head")
        with psycopg.connect(psycopg_url) as connection:
            rows = connection.execute("""
                SELECT id, target_type FROM app.sources WHERE id IN (%s, %s)
            """, (exact_id, pattern_id)).fetchall()
            assert dict(rows) == {exact_id: "index", pattern_id: "index_pattern"}
            stream_id = connection.execute("""
                INSERT INTO app.sources (name, source_type, external_connection_id,
                                         target_type, data_stream_pattern)
                VALUES ('findings', 'external', %s, 'data_stream_pattern', 'wazuh-findings-v5-*')
                RETURNING id
            """, (external_id,)).fetchone()[0]
        with pytest.raises(subprocess.CalledProcessError):
            run_alembic(sqlalchemy_url, "downgrade", "0009_external_sources")
        with psycopg.connect(psycopg_url) as connection:
            connection.execute("DELETE FROM app.sources WHERE id = %s", (stream_id,))
        run_alembic(sqlalchemy_url, "downgrade", "0009_external_sources")
        with psycopg.connect(psycopg_url) as connection:
            assert connection.execute("SELECT count(*) FROM app.sources WHERE id IN (%s, %s)",
                                      (exact_id, pattern_id)).fetchone()[0] == 2
            connection.execute("DELETE FROM app.sources WHERE external_connection_id = %s", (external_id,))
            connection.execute("DELETE FROM app.external_connections WHERE id = %s", (external_id,))
    finally:
        run_alembic(sqlalchemy_url, "downgrade", "base")


def test_https_metadata_verifies_ca_hostname_auth_and_redirect(tmp_path):
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    leaf = (x509.CertificateBuilder().subject_name(leaf_name).issuer_name(ca_name)
            .public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .sign(ca_key, hashes.SHA256()))
    with pytest.raises(ValueError):
        validate_ca(leaf.public_bytes(serialization.Encoding.PEM))
    ca_pem = ca.public_bytes(serialization.Encoding.PEM)
    cert_file = tmp_path / "cert.pem"
    key_file = tmp_path / "key.pem"
    cert_file.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(leaf_key.private_bytes(serialization.Encoding.PEM,
                         serialization.PrivateFormat.TraditionalOpenSSL,
                         serialization.NoEncryption()))

    class Handler(BaseHTTPRequestHandler):
        redirect = False
        oversized = False
        slow = False

        def do_GET(self):
            if self.headers.get("Authorization") != "Basic dXNlcjpwYXNz":
                self.send_response(401)
                self.end_headers()
                return
            if self.redirect:
                self.send_response(302)
                self.send_header("Location", "https://example.com/")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if self.oversized:
                self.wfile.write(b" " * (HttpIndexerMetadataAdapter.MAX_RESPONSE + 1))
            elif self.slow:
                time.sleep(6)
                self.wfile.write(b'[{"index":"logs-2026"}]')
            elif self.path.startswith("/_resolve/index/"):
                self.wfile.write(b'{"indices":[],"data_streams":['
                                 b'{"name":"wazuh-findings-v5-security",'
                                 b'"backing_indices":[".ds-wazuh-findings-v5-security-000001"]}]}')
            else:
                self.wfile.write(b'[{"index":"logs-2026"},'
                                 b'{"index":".ds-wazuh-findings-v5-security-000001"}]')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file, key_file)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    adapter = HttpIndexerMetadataAdapter()
    try:
        url = f"https://localhost:{server.server_port}"
        assert validate_ca(ca_pem) == ca_pem.decode()
        assert adapter.indices(url, "user", "pass", ca_pem.decode()) == ["logs-2026"]
        assert adapter.data_streams(url, "user", "pass", ca_pem.decode()) == [
            "wazuh-findings-v5-security"]
        for target, username, password, trust, expected in (
            (url, "user", "pass", None, "untrusted_certificate"),
            (f"https://127.0.0.1:{server.server_port}", "user", "pass",
             ca_pem.decode(), "hostname_mismatch"),
            (url, "user", "wrong", ca_pem.decode(), "access_denied"),
        ):
            with pytest.raises(IndexerError) as error:
                adapter.indices(target, username, password, trust)
            assert error.value.kind == expected
        Handler.redirect = True
        with pytest.raises(IndexerError) as error:
            adapter.indices(url, "user", "pass", ca_pem.decode())
        assert error.value.kind == "invalid_response"
        Handler.redirect = False
        Handler.oversized = True
        with pytest.raises(IndexerError) as error:
            adapter.indices(url, "user", "pass", ca_pem.decode())
        assert error.value.kind == "response_too_large"
        Handler.oversized = False
        Handler.slow = True
        with pytest.raises(IndexerError) as error:
            adapter.indices(url, "user", "pass", ca_pem.decode())
        assert error.value.kind == "timeout"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    with pytest.raises(IndexerError) as error:
        adapter.indices(url, "user", "pass", ca_pem.decode())
    assert error.value.kind == "unavailable"
