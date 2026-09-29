from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.models import (
    KafkaConnection,
    Normalizer,
    ParsedLog,
    ProcessedKafkaRecord,
    ProcessingError,
    Source,
)
from app.normalization.engine import NormalizationEngine
from app.repositories.protocols.processing import KafkaCoordinates
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.parsed_logs import SqlAlchemyParsedLogRepository
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.schemas.parsed_logs import ParsedLogBulkDeleteRequest, ParsedLogSummary
from app.services.implementations.cursor import SignedParsedLogCursorCodec
from app.services.implementations.parsed_logs import ParsedLogServiceImpl
from app.services.implementations.processing import (
    DurableProcessingServiceImpl,
    ProcessingConfigurationError,
    ProcessingStorageError,
)

BACKEND_DIR = Path(__file__).resolve().parents[1]
RECEIVED_AT = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
COLLECTED_AT = datetime(2026, 3, 1, 11, 59, tzinfo=UTC)


def _run_alembic(url: str, *args: str) -> None:
    environment = {**os.environ, "DATABASE_URL": url}
    subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_DIR,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def processing_database_url() -> str:
    raw_url = os.getenv("TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail("TEST_DATABASE_URL must point to a separate PostgreSQL test database")
    parsed = make_url(raw_url)
    if parsed.get_backend_name() != "postgresql" or parsed.database == "diploma_db":
        pytest.fail("Durable processing tests require a separate PostgreSQL database")
    return parsed.set(drivername="postgresql+psycopg").render_as_string(
        hide_password=False
    )


@pytest.fixture(scope="session")
def processing_database(processing_database_url):
    _run_alembic(processing_database_url, "upgrade", "head")
    database = Database(processing_database_url)
    yield database
    database.dispose()


def _clear_processing_data(database: Database) -> None:
    with database.engine.begin() as connection:
        connection.execute(text("DELETE FROM logs.processing_errors"))
        connection.execute(text("DELETE FROM logs.parsed_logs"))
        connection.execute(text("DELETE FROM logs.processed_kafka_records"))
        connection.execute(text("DELETE FROM logs.kafka_operational_events"))
        connection.execute(text("DELETE FROM app.sources"))
        connection.execute(text("DELETE FROM app.kafka_connections"))
        connection.execute(text("DELETE FROM app.normalizers"))


@pytest.fixture
def clean_processing_data(processing_database):
    _clear_processing_data(processing_database)
    yield
    _clear_processing_data(processing_database)


def _rule(*, complete: bool = False) -> dict:
    blocks = [{
        "key": "message",
        "kind": "map_ecs",
        "target": "message",
        "source": {"ref": "log"},
        "required": True,
    }]
    if complete:
        blocks.append({
            "key": "event_time",
            "kind": "map_ecs",
            "target": "@timestamp",
            "source": {"literal": COLLECTED_AT.isoformat()},
            "required": True,
        })
    return {
        "format_version": 1,
        "variants": [{
            "key": "plain",
            "priority": 1,
            "when": {"kind": "prefix", "value": "ok"},
            "blocks": blocks,
        }],
    }


def _nested_json_rule() -> dict:
    return {
        "format_version": 1,
        "variants": [{
            "key": "nested_json",
            "priority": 1,
            "when": {"kind": "prefix", "value": "{"},
            "blocks": [
                {
                    "key": "json",
                    "kind": "json",
                    "candidates": [{
                        "requires": ["message"],
                        "extract": {"message": "message"},
                    }],
                },
                {
                    "key": "message",
                    "kind": "map_ecs",
                    "target": "message",
                    "source": {"ref": "json.message"},
                    "required": True,
                },
            ],
        }],
    }


def _context(database: Database, *, rule: dict | None = None, enabled: bool = True):
    with database.session_factory() as session:
        connection = KafkaConnection(
            name=f"connection-{uuid4()}", bootstrap_servers=["localhost:9092"]
        )
        normalizer = Normalizer(
            name=f"normalizer-{uuid4()}", rule=rule or _rule(complete=True)
        )
        session.add_all([connection, normalizer])
        session.flush()
        source = Source(
            name=f"source-{uuid4()}",
            connection_id=connection.id,
            normalizer_id=normalizer.id,
            topic_name="events",
            kafka_topic_identity="test-topic-id",
            is_enabled=enabled,
        )
        session.add(source)
        session.commit()
        return connection.id, source.id, normalizer.id


def _payload(log: str = "ok accepted", timestamp: str | None = None) -> bytes:
    return json.dumps({
        "timestamp": timestamp or COLLECTED_AT.isoformat(),
        "log": log,
    }, separators=(",", ":")).encode("utf-8")


def _service(session, *, engine=None, repository=None, uow=None, clock=None):
    catalog = PackagedEcsCatalog.load()
    return DurableProcessingServiceImpl(
        repository or SqlAlchemyProcessingRepository(session),
        uow or SqlAlchemyUnitOfWork(session),
        engine or NormalizationEngine(catalog),
        clock=clock or (lambda: RECEIVED_AT - timedelta(seconds=1)),
    )


def _coordinates(connection_id, partition=0, offset=0, topic="events"):
    return KafkaCoordinates(connection_id, topic, partition, offset, "test-topic-id")


def test_processing_rejects_record_without_verified_topic_identity(
    processing_database, clean_processing_data
):
    connection_id, source_id, _ = _context(processing_database)
    with processing_database.session_factory() as session:
        service = _service(session)
        with pytest.raises(ProcessingConfigurationError, match="kafka_coordinates_invalid"):
            service.process(
                source_id, KafkaCoordinates(connection_id, "events", 0, 0),
                _payload(), RECEIVED_AT,
            )


def test_complete_partial_failed_and_status_dto(processing_database, clean_processing_data):
    connection_id, source_id, _ = _context(processing_database)
    partial_connection_id, partial_source_id, _ = _context(
        processing_database, rule=_rule(complete=False)
    )
    with processing_database.session_factory() as session:
        service = _service(session)
        complete = service.process(source_id, _coordinates(connection_id, 0, 10), _payload(), RECEIVED_AT)
        partial = service.process(
            partial_source_id,
            _coordinates(partial_connection_id, 0, 11),
            _payload("ok partial", timestamp="2026-03-01T11:59:00Z"),
            RECEIVED_AT,
        )
        failed_raw = _payload("does-not-match")
        failed = service.process(
            source_id, _coordinates(connection_id, 0, 12), failed_raw, RECEIVED_AT
        )
        assert [complete.outcome, partial.outcome, failed.outcome] == [
            "stored_complete", "stored_partial", "stored_failed"
        ]

    with processing_database.session_factory() as session:
        events = list(session.scalars(select(ParsedLog).order_by(ParsedLog.kafka_offset)))
        error = session.scalar(select(ProcessingError))
        records = list(session.scalars(select(ProcessedKafkaRecord).order_by(ProcessedKafkaRecord.kafka_offset)))
        assert [event.normalization_status for event in events] == ["complete", "partial"]
        assert events[0].normalization_diagnostics == []
        assert events[1].normalization_diagnostics[0]["code"] == "event_time_fallback"
        assert ParsedLogSummary.from_model(events[1]).normalization_status == "partial"
        assert ParsedLogSummary.from_model(events[1]).normalization_diagnostics[0].code == "event_time_fallback"
        assert error is not None and error.raw_payload == failed_raw
        assert error.stage == "normalization"
        assert error.diagnostics[0]["code"] == "no_variant_matched"
        assert error.fluent_bit_collected_at == COLLECTED_AT
        assert error.backend_received_at == RECEIVED_AT
        assert error.backend_processed_at >= error.backend_received_at
        assert error.connection_name.startswith("connection-")
        assert error.source_name.startswith("source-")
        assert error.normalizer_name.startswith("normalizer-")
        assert error.normalizer_version == 1
        assert len(records) == 3
        assert all(event.backend_received_at == RECEIVED_AT for event in events)
        assert all(event.backend_processed_at >= event.backend_received_at for event in events)
        assert events[0].fluent_bit_collected_at == COLLECTED_AT
        assert events[0].connection_name.startswith("connection-")
        assert events[0].source_name.startswith("source-")
        assert events[0].normalizer_name.startswith("normalizer-")
        assert events[0].normalizer_version == 1


@pytest.mark.parametrize(
    ("raw", "stage", "code"),
    [
        (b"\xff\xfe", "decode", "invalid_utf8"),
        (b"{broken", "decode", "invalid_json"),
        (
            b'{"timestamp":"2026-03-01T11:59:00Z","log":"ok\\u0000accepted"}',
            "envelope",
            "invalid_envelope",
        ),
        (b'{"timestamp":"2026-03-01T11:59:00Z","log":"ok","extra":1}', "envelope", "invalid_envelope"),
        (b'{"timestamp":"2026-03-01T11:59:00","log":"ok"}', "envelope", "invalid_envelope"),
        (_payload("does-not-match"), "normalization", "no_variant_matched"),
    ],
)
def test_failed_payload_is_preserved_and_does_not_poison_next_record(
    processing_database, clean_processing_data, raw, stage, code
):
    connection_id, source_id, _ = _context(processing_database)
    with processing_database.session_factory() as session:
        service = _service(session)
        coordinates = _coordinates(connection_id, 2, 30)
        result = service.process(source_id, coordinates, raw, RECEIVED_AT)
        assert result.outcome == "stored_failed"
        assert service.process(source_id, coordinates, raw, RECEIVED_AT).outcome == "already_processed"
        assert service.process(
            source_id, _coordinates(connection_id, 2, 31), _payload(), RECEIVED_AT
        ).outcome == "stored_complete"
    with processing_database.session_factory() as session:
        error = session.scalar(select(ProcessingError))
        assert error is not None
        assert error.raw_payload == raw
        assert error.stage == stage
        assert error.diagnostics[0]["code"] == code
        assert set(error.diagnostics[0]) <= {"code", "variant", "block", "field"}
        assert session.scalar(select(ParsedLog.kafka_offset)) == 31


@pytest.mark.parametrize(
    "nested_log",
    [
        '{"message":"ok\\u0000accepted"}',
        '{"message":"ok\\ud800accepted"}',
    ],
)
def test_unrepresentable_nested_json_is_a_durable_failure(
    processing_database, clean_processing_data, nested_log
):
    connection_id, source_id, _ = _context(
        processing_database, rule=_nested_json_rule()
    )
    raw = _payload(nested_log)
    coordinates = _coordinates(connection_id, 2, 32)
    with processing_database.session_factory() as session:
        service = _service(session)
        assert service.process(source_id, coordinates, raw, RECEIVED_AT).outcome == "stored_failed"
        assert service.process(
            source_id, coordinates, raw, RECEIVED_AT
        ).outcome == "already_processed"
        assert service.process(
            source_id,
            _coordinates(connection_id, 2, 33),
            _payload('{"message":"ok accepted"}'),
            RECEIVED_AT,
        ).outcome == "stored_partial"
    with processing_database.session_factory() as session:
        error = session.scalar(
            select(ProcessingError).where(ProcessingError.kafka_offset == 32)
        )
        assert error is not None
        assert error.raw_payload == raw
        assert error.stage == "normalization"
        assert error.diagnostics == [{"code": "unrepresentable_ecs_text"}]
        assert session.scalar(
            select(ParsedLog.id).where(ParsedLog.kafka_offset == 32)
        ) is None
        assert session.scalar(
            select(ParsedLog.id).where(ParsedLog.kafka_offset == 33)
        ) is not None


def test_identity_uses_coordinates_and_deduplicates_success_and_failure(
    processing_database, clean_processing_data
):
    connection_id, source_id, normalizer_id = _context(processing_database)
    second_connection_id, second_source_id, _ = _context(processing_database)
    payload = _payload()
    failed_payload = _payload("does-not-match")
    with processing_database.session_factory() as session:
        service = _service(session)
        assert service.process(source_id, _coordinates(connection_id, 0, 1), payload, RECEIVED_AT).outcome == "stored_complete"
        assert service.process(source_id, _coordinates(connection_id, 0, 2), payload, RECEIVED_AT).outcome == "stored_complete"
        assert service.process(source_id, _coordinates(connection_id, 1, 1), payload, RECEIVED_AT).outcome == "stored_complete"
        assert service.process(second_source_id, _coordinates(second_connection_id, 0, 1), payload, RECEIVED_AT).outcome == "stored_complete"
        failure_coordinates = _coordinates(connection_id, 0, 3)
        assert service.process(source_id, failure_coordinates, failed_payload, RECEIVED_AT).outcome == "stored_failed"
        assert service.process(source_id, failure_coordinates, payload, RECEIVED_AT).outcome == "already_processed"
        assert service.process(source_id, _coordinates(connection_id, 0, 1), failed_payload, RECEIVED_AT).outcome == "already_processed"
    with processing_database.session_factory() as session:
        offset_one = list(
            session.scalars(select(ParsedLog).where(ParsedLog.kafka_offset == 1))
        )
        assert len(offset_one) == 3
        assert {event.connection_id for event in offset_one} == {
            connection_id,
            second_connection_id,
        }
        assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 2)) is not None
        assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_partition == 1)) is not None
        assert session.scalar(select(ProcessingError.normalizer_id)) == normalizer_id
        assert session.scalar(select(ParsedLog).where(ParsedLog.kafka_offset == 1)).raw == "ok accepted"


@pytest.mark.parametrize("failed", [False, True])
def test_concurrent_duplicate_has_one_durable_outcome(
    processing_database, clean_processing_data, failed
):
    connection_id, source_id, _ = _context(processing_database)
    barrier = threading.Barrier(2)
    catalog = PackagedEcsCatalog.load()

    class SynchronizedEngine:
        def __init__(self):
            self.delegate = NormalizationEngine(catalog)

        def compile(self, *args, **kwargs):
            return self.delegate.compile(*args, **kwargs)

        def normalize(self, *args, **kwargs):
            barrier.wait(timeout=10)
            return self.delegate.normalize(*args, **kwargs)

    def process_once():
        with processing_database.session_factory() as session:
            service = _service(session, engine=SynchronizedEngine())
            return service.process(
                source_id,
                _coordinates(connection_id, 3, 99),
                _payload("not-matching" if failed else "ok accepted"),
                RECEIVED_AT,
            ).outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: process_once(), range(2)))
    expected_stored = "stored_failed" if failed else "stored_complete"
    assert sorted(outcomes) == ["already_processed", expected_stored]
    with processing_database.session_factory() as session:
        assert session.scalar(select(ProcessedKafkaRecord.id).where(ProcessedKafkaRecord.kafka_offset == 99)) is not None
        if failed:
            assert session.scalar(select(ProcessingError.id).where(ProcessingError.kafka_offset == 99)) is not None
            assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 99)) is None
        else:
            assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 99)) is not None
            assert session.scalar(select(ProcessingError.id).where(ProcessingError.kafka_offset == 99)) is None


def test_configuration_errors_are_not_marked_and_existing_repeat_skips_new_rule(
    processing_database, clean_processing_data
):
    connection_id, source_id, normalizer_id = _context(processing_database)
    coordinates = _coordinates(connection_id, 0, 4)
    with processing_database.session_factory() as session:
        service = _service(session)
        assert service.process(source_id, coordinates, _payload(), RECEIVED_AT).outcome == "stored_complete"
    with processing_database.session_factory() as session:
        normalizer = session.get(Normalizer, normalizer_id)
        normalizer.rule = {"format_version": 0, "legacy_rule_text": "legacy"}
        session.commit()
    with processing_database.session_factory() as session:
        service = _service(session)
        assert service.process(source_id, coordinates, _payload("changed"), RECEIVED_AT).outcome == "already_processed"
        with pytest.raises(ProcessingConfigurationError, match="normalizer_rule_invalid"):
            service.process(
                source_id,
                _coordinates(connection_id, 0, 5),
                _payload(),
                RECEIVED_AT,
            )
    with processing_database.session_factory() as session:
        assert session.scalar(select(ProcessedKafkaRecord.kafka_offset).where(ProcessedKafkaRecord.kafka_offset == 5)) is None


@pytest.mark.parametrize(
    ("failure", "result_kind"),
    [("write", "event"), ("commit", "event"), ("write", "error"), ("commit", "error")],
)
def test_write_or_commit_failure_rolls_back_and_retry_succeeds(
    processing_database, clean_processing_data, failure, result_kind
):
    connection_id, source_id, _ = _context(processing_database)
    coordinates = _coordinates(connection_id, 0, 44)
    with processing_database.session_factory() as session:
        base_repository = SqlAlchemyProcessingRepository(session)
        base_uow = SqlAlchemyUnitOfWork(session)

        class FailingRepository:
            failed = False

            def __getattr__(self, name):
                return getattr(base_repository, name)

            def store_event(self, record, event):
                inserted = base_repository.store_event(record, event)
                if failure == "write" and result_kind == "event" and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated post-flush storage error")
                return inserted

            def store_error(self, record, error):
                inserted = base_repository.store_error(record, error)
                if failure == "write" and result_kind == "error" and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated post-flush storage error")
                return inserted

        class FailingUnitOfWork:
            failed = False

            def commit(self):
                if failure == "commit" and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated commit failure")
                base_uow.commit()

            def rollback(self):
                base_uow.rollback()

        service = _service(
            session,
            repository=FailingRepository(),
            uow=FailingUnitOfWork(),
        )
        payload = _payload("ok accepted" if result_kind == "event" else "not-matching")
        with pytest.raises(ProcessingStorageError):
            service.process(source_id, coordinates, payload, RECEIVED_AT)
        assert session.scalar(select(ProcessedKafkaRecord.id).where(ProcessedKafkaRecord.kafka_offset == 44)) is None
        assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 44)) is None
        assert session.scalar(select(ProcessingError.id).where(ProcessingError.kafka_offset == 44)) is None
        expected = "stored_complete" if result_kind == "event" else "stored_failed"
        assert service.process(source_id, coordinates, payload, RECEIVED_AT).outcome == expected
    with processing_database.session_factory() as session:
        assert session.scalar(select(ProcessedKafkaRecord.id).where(ProcessedKafkaRecord.kafka_offset == 44)) is not None
        assert (session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 44)) is not None) == (result_kind == "event")
        assert (session.scalar(select(ProcessingError.id).where(ProcessingError.kafka_offset == 44)) is not None) == (result_kind == "error")


def test_delete_event_keeps_identity_and_config_deletion_keeps_error_snapshots(
    processing_database, clean_processing_data
):
    connection_id, source_id, normalizer_id = _context(processing_database)
    catalog = PackagedEcsCatalog.load()
    coordinates = _coordinates(connection_id, 0, 70)
    with processing_database.session_factory() as session:
        processing = _service(session)
        assert processing.process(source_id, coordinates, _payload(), RECEIVED_AT).outcome == "stored_complete"
        log = session.scalar(select(ParsedLog).where(ParsedLog.kafka_offset == 70))
        logs = ParsedLogServiceImpl(
            SqlAlchemyParsedLogRepository(session),
            SqlAlchemyUnitOfWork(session),
            catalog,
            SignedParsedLogCursorCodec("test-secret"),
        )
        logs.delete(log.id)
    with processing_database.session_factory() as session:
        assert _service(session).process(source_id, coordinates, _payload(), RECEIVED_AT).outcome == "already_processed"
        assert session.scalar(select(ParsedLog.id).where(ParsedLog.kafka_offset == 70)) is None
        assert session.scalar(select(ProcessedKafkaRecord.id).where(ProcessedKafkaRecord.kafka_offset == 70)) is not None

    error_coordinates = _coordinates(connection_id, 0, 71)
    with processing_database.session_factory() as session:
        assert _service(session).process(
            source_id, error_coordinates, _payload("no match"), RECEIVED_AT
        ).outcome == "stored_failed"
    with processing_database.session_factory() as session:
        session.delete(session.get(KafkaConnection, connection_id))
        session.delete(session.get(Normalizer, normalizer_id))
        session.commit()
    with processing_database.session_factory() as session:
        error = session.scalar(select(ProcessingError).where(ProcessingError.kafka_offset == 71))
        record = session.scalar(select(ProcessedKafkaRecord).where(ProcessedKafkaRecord.kafka_offset == 71))
        assert error is not None
        assert error.connection_id is None and error.source_id is None and error.normalizer_id is None
        assert error.connection_name.startswith("connection-")
        assert error.source_name.startswith("source-")
        assert error.normalizer_name.startswith("normalizer-")
        assert record is not None and record.connection_id is None
        assert record.connection_identity == connection_id
        assert _service(session).process(
            source_id, error_coordinates, _payload(), RECEIVED_AT
        ).outcome == "already_processed"


def test_bulk_delete_preserves_processing_identity(processing_database, clean_processing_data):
    connection_id, source_id, _ = _context(processing_database)
    with processing_database.session_factory() as session:
        service = _service(session)
        service.process(source_id, _coordinates(connection_id, 0, 80), _payload(), RECEIVED_AT)
        service.process(source_id, _coordinates(connection_id, 0, 81), _payload(), RECEIVED_AT)
        logs = ParsedLogServiceImpl(
            SqlAlchemyParsedLogRepository(session),
            SqlAlchemyUnitOfWork(session),
            PackagedEcsCatalog.load(),
            SignedParsedLogCursorCodec("test-secret"),
        )
        response = logs.delete_many(ParsedLogBulkDeleteRequest(source_id=source_id))
        assert response.deleted_count == 2
    with processing_database.session_factory() as session:
        service = _service(session)
        assert service.process(
            source_id, _coordinates(connection_id, 0, 80), _payload(), RECEIVED_AT
        ).outcome == "already_processed"
        assert session.scalar(select(ParsedLog.id)) is None
        assert len(list(session.scalars(select(ProcessedKafkaRecord.id)))) == 2


def test_migration_roundtrip_backfills_status_and_preserves_legacy_rows(
    processing_database, processing_database_url, clean_processing_data
):
    _run_alembic(processing_database_url, "downgrade", "0004_normalizer_json_rules")
    event_id = uuid4()
    collected = COLLECTED_AT.isoformat()
    received = RECEIVED_AT.isoformat()
    with processing_database.engine.begin() as connection:
        connection_id = connection.execute(text(
            "INSERT INTO app.kafka_connections (name, bootstrap_servers) "
            "VALUES (:name, ARRAY['localhost:9092']) RETURNING id"
        ), {"name": f"migration-connection-{uuid4()}"}).scalar_one()
        normalizer_id = connection.execute(text(
            "INSERT INTO app.normalizers (name, rule) VALUES (:name, '{}') RETURNING id"
        ), {"name": f"migration-normalizer-{uuid4()}"}).scalar_one()
        source_id = connection.execute(text(
            "INSERT INTO app.sources (name, connection_id, normalizer_id, topic_name) "
            "VALUES (:name, :connection_id, :normalizer_id, 'events') RETURNING id"
        ), {
            "name": f"migration-source-{uuid4()}",
            "connection_id": connection_id,
            "normalizer_id": normalizer_id,
        }).scalar_one()
        connection.execute(text(
            "INSERT INTO logs.parsed_logs (id, source_id, connection_id, normalizer_id, "
            "normalizer_version, normalizer_name, source_name, connection_name, kafka_topic, "
            "kafka_partition, kafka_offset, deduplication_key, fluent_bit_collected_at, "
            "backend_received_at, backend_processed_at, raw, ecs_data) VALUES "
            "(:id, :source_id, :connection_id, :normalizer_id, 1, 'normalizer', 'source', "
            "'connection', 'events', 0, 1, :dedup, :collected, :received, :received, "
            "'legacy raw', '{\"ecs\":{\"version\":\"9.4.0\"}}'::jsonb)"
        ), {
            "id": event_id,
            "source_id": source_id,
            "connection_id": connection_id,
            "normalizer_id": normalizer_id,
            "dedup": str(uuid4()),
            "collected": collected,
            "received": received,
        })
    try:
        _run_alembic(processing_database_url, "upgrade", "head")
        with processing_database.engine.connect() as connection:
            row = connection.execute(text(
                "SELECT normalization_status, normalization_diagnostics, raw "
                "FROM logs.parsed_logs WHERE id=:id"
            ), {"id": event_id}).one()
            assert row.normalization_status == "complete"
            assert row.normalization_diagnostics == []
            assert row.raw == "legacy raw"
        _run_alembic(processing_database_url, "downgrade", "0004_normalizer_json_rules")
        with processing_database.engine.connect() as connection:
            assert connection.execute(text(
                "SELECT count(*) FROM logs.parsed_logs WHERE id=:id"
            ), {"id": event_id}).scalar_one() == 1
        _run_alembic(processing_database_url, "upgrade", "head")
        with processing_database.engine.connect() as connection:
            assert connection.execute(text(
                "SELECT normalization_status FROM logs.parsed_logs WHERE id=:id"
            ), {"id": event_id}).scalar_one() == "complete"
    finally:
        _run_alembic(processing_database_url, "upgrade", "head")
