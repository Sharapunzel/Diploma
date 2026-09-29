from __future__ import annotations

import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.kafka import (
    ConfluentKafkaConsumer,
    KafkaAssignment,
    KafkaConnectionConfig,
    KafkaConsumerError,
    KafkaMessage,
    KafkaMetadata,
    KafkaPartition,
    KafkaRevocation,
    KafkaTopic,
)
from app.models import (
    KafkaConnection,
    KafkaOperationalEvent,
    Normalizer,
    ParsedLog,
    ProcessedKafkaRecord,
    ProcessingError,
    Source,
)
from app.normalization.engine import NormalizationEngine
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.administration import SqlAlchemyNormalizerRepository
from app.repositories.sqlalchemy.connections import (
    SqlAlchemyKafkaConnectionRepository,
    SqlAlchemySourceRepository,
)
from app.schemas.connections import SourcePatch
from app.services.implementations.connections import SourceServiceImpl
from app.services.implementations.consumers import KafkaSourceSupervisor
from app.services.implementations.processing import (
    DurableProcessingServiceImpl,
    ProcessingStorageError,
)

BACKEND_DIR = Path(__file__).resolve().parents[1]
RECEIVED_AT = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)


def _run_alembic(url: str, *args: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_DIR,
        env={**os.environ, "DATABASE_URL": url},
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def consumer_database_url() -> str:
    raw_url = os.getenv("TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail("TEST_DATABASE_URL must point to a separate PostgreSQL test database")
    parsed = make_url(raw_url)
    if parsed.get_backend_name() != "postgresql" or parsed.database == "diploma_db":
        pytest.fail("Kafka consumer tests require a separate PostgreSQL database")
    return parsed.set(drivername="postgresql+psycopg").render_as_string(
        hide_password=False
    )


@pytest.fixture(scope="session")
def consumer_database(consumer_database_url):
    _run_alembic(consumer_database_url, "upgrade", "head")
    database = Database(consumer_database_url)
    yield database
    database.dispose()


def _clear(database: Database) -> None:
    with database.engine.begin() as connection:
        connection.execute(text("DELETE FROM logs.kafka_operational_events"))
        connection.execute(text("DELETE FROM logs.processing_errors"))
        connection.execute(text("DELETE FROM logs.parsed_logs"))
        connection.execute(text("DELETE FROM logs.processed_kafka_records"))
        connection.execute(text("DELETE FROM app.sources"))
        connection.execute(text("DELETE FROM app.kafka_connections"))
        connection.execute(text("DELETE FROM app.normalizers"))


@pytest.fixture
def clean_consumer_data(consumer_database):
    _clear(consumer_database)
    yield
    _clear(consumer_database)


def _rule() -> dict:
    return {
        "format_version": 1,
        "variants": [{
            "key": "plain",
            "priority": 1,
            "when": {"kind": "prefix", "value": "ok"},
            "blocks": [
                {
                    "key": "message",
                    "kind": "map_ecs",
                    "target": "message",
                    "source": {"ref": "log"},
                    "required": True,
                },
                {
                    "key": "event_time",
                    "kind": "map_ecs",
                    "target": "@timestamp",
                    "source": {"literal": "2026-03-02T11:59:00Z"},
                    "required": True,
                },
            ],
        }],
    }


def _source(database: Database, *, enabled: bool = True, topic: str = "events"):
    with database.session_factory() as session:
        connection = KafkaConnection(
            name=f"consumer-connection-{uuid4()}",
            bootstrap_servers=[f"broker-{uuid4()}:9092"],
        )
        normalizer = Normalizer(name=f"consumer-normalizer-{uuid4()}", rule=_rule())
        session.add_all([connection, normalizer])
        session.flush()
        source = Source(
            name=f"consumer-source-{uuid4()}",
            connection_id=connection.id,
            normalizer_id=normalizer.id,
            topic_name=topic,
            kafka_topic_identity="topic-id" if topic == "events" else "topic-disabled",
            is_enabled=enabled,
        )
        session.add(source)
        session.commit()
        return connection.id, source.id


class FakeMetadataClient:
    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata:
        return KafkaMetadata(
            broker_count=1,
            topics=(KafkaTopic("events", 1, "topic-id"), KafkaTopic("disabled", 1, "topic-disabled")),
            latency_ms=1,
            cluster_identity=config.bootstrap_servers[0],
        )


class FixedMetadataClient:
    def __init__(self, identity: str | None) -> None:
        self.identity = identity

    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata:
        return KafkaMetadata(
            broker_count=1,
            topics=(KafkaTopic("events", 1, self.identity),),
            latency_ms=1,
            cluster_identity=config.bootstrap_servers[0],
        )


class FakeConsumer:
    def __init__(
        self,
        events,
        *,
        committed=None,
        low=0,
        high=100,
        failed_commits=0,
        resume_events=(),
    ):
        self.events = list(events)
        self.committed_offsets = committed or {}
        self.low = low
        self.high = high
        self.failed_commits = failed_commits
        self.resume_events = list(resume_events)
        self.assigned: list[tuple[KafkaPartition, ...]] = []
        self.unassigned = 0
        self.commits: list[tuple[KafkaPartition, ...]] = []
        self.paused: list[tuple[KafkaPartition, ...]] = []
        self.resumed: list[tuple[KafkaPartition, ...]] = []
        self.seeks: list[KafkaPartition] = []
        self.closed = False
        self.subscribed = None
        self.lock = threading.Lock()

    def subscribe(self, topic: str) -> None:
        self.subscribed = topic

    def poll(self, timeout: float):
        with self.lock:
            if self.events:
                return self.events.pop(0)
        sleep(min(timeout, 0.01))
        return None

    def assign(self, partitions: tuple[KafkaPartition, ...]) -> None:
        self.assigned.append(partitions)

    def unassign(self) -> None:
        self.unassigned += 1

    def seek(self, partition: KafkaPartition) -> None:
        self.seeks.append(partition)

    def pause(self, partitions: tuple[KafkaPartition, ...]) -> None:
        self.paused.append(partitions)

    def resume(self, partitions: tuple[KafkaPartition, ...]) -> None:
        self.resumed.append(partitions)
        with self.lock:
            self.events.extend(self.resume_events)
            self.resume_events.clear()

    def committed(self, partitions: tuple[KafkaPartition, ...], timeout: float):
        return tuple(self.committed_offsets.get(item.partition) for item in partitions)

    def watermarks(self, topic: str, partition: int, timeout: float):
        return self.low, self.high

    def commit(self, offsets: tuple[KafkaPartition, ...], timeout: float) -> None:
        self.commits.append(offsets)
        if self.failed_commits:
            self.failed_commits -= 1
            from app.kafka import KafkaConsumerError

            raise KafkaConsumerError("unavailable")
        for item in offsets:
            self.committed_offsets[item.partition] = item.offset

    def close(self) -> None:
        self.closed = True


class FakeConsumerFactory:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer
        self.groups: list[str] = []

    def create(self, config: KafkaConnectionConfig, group_id: str) -> FakeConsumer:
        self.groups.append(group_id)
        return self.consumer


def _wait(predicate, timeout: float = 3) -> None:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.02)
    raise AssertionError("condition was not reached")


def _supervisor(
    database: Database,
    consumer: FakeConsumer,
    metadata_client=None,
) -> KafkaSourceSupervisor:
    return KafkaSourceSupervisor(
        database.session_factory,
        NormalizationEngine(PackagedEcsCatalog.load()),
        metadata_client or FakeMetadataClient(),
        FakeConsumerFactory(consumer),
        metadata_timeout=1,
        poll_timeout=0.02,
        retry_delay=0.02,
        sync_interval=0.02,
    )


def test_consumer_commits_only_after_durable_outcomes_and_keeps_stream_moving(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    _source(consumer_database, enabled=False, topic="disabled")
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage("events", 0, 0, b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT),
        KafkaMessage("events", 0, 1, b"\xff", RECEIVED_AT),
        KafkaMessage("events", 0, 2, None, RECEIVED_AT),
        KafkaMessage("events", 0, 3, b"", RECEIVED_AT),
    ])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) == 4)
    finally:
        supervisor.stop()
    assert consumer.subscribed == "events"
    assert consumer.assigned == [(KafkaPartition("events", 0, 0),)]
    assert [offsets[0].offset for offsets in consumer.commits] == [1, 2, 3, 4]
    with consumer_database.session_factory() as session:
        assert session.scalar(select(ParsedLog).where(ParsedLog.source_id == source_id)) is not None
        errors = list(session.scalars(select(ProcessingError).order_by(ProcessingError.kafka_offset)))
        assert [error.kafka_offset for error in errors] == [1, 2, 3]
        assert errors[1].diagnostics == [{"code": "null_payload"}]
        assert errors[1].raw_payload == b""
        assert session.scalar(
            select(ProcessedKafkaRecord).where(
                ProcessedKafkaRecord.connection_identity == connection_id,
                ProcessedKafkaRecord.kafka_offset == 3,
            )
        ) is not None


def test_commit_retry_uses_durable_already_processed_without_duplicate_rows(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    message = KafkaMessage(
        "events",
        0,
        0,
        b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}',
        RECEIVED_AT,
    )
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        message,
    ], failed_commits=1, resume_events=[message])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) >= 2)
    finally:
        supervisor.stop()
    with consumer_database.session_factory() as session:
        assert len(list(session.scalars(select(ProcessedKafkaRecord)))) == 1
        assert len(list(session.scalars(select(ParsedLog).where(ParsedLog.source_id == source_id)))) == 1
    assert consumer.paused and consumer.resumed
    assert [offsets[0].offset for offsets in consumer.commits] == [1, 1]


def test_topic_recreated_during_processing_prevents_first_commit(
    consumer_database, clean_consumer_data, monkeypatch
):
    from app.services.implementations.processing import DurableProcessingServiceImpl

    _, source_id = _source(consumer_database)
    metadata = FixedMetadataClient("topic-id")
    original = DurableProcessingServiceImpl.process

    def switch_after_durable_write(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        metadata.identity = "replacement-topic-id"
        return result

    monkeypatch.setattr(DurableProcessingServiceImpl, "process", switch_after_durable_write)
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(consumer_database, consumer, metadata)
    supervisor.start()
    try:
        _wait(lambda: consumer.closed)
    finally:
        assert supervisor.stop()
    assert consumer.commits == []
    with consumer_database.session_factory() as session:
        assert session.get(Source, source_id).is_archived
        assert session.scalar(select(func.count()).select_from(ProcessedKafkaRecord)) == 1
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


def test_topic_recreated_before_failed_commit_retry_prevents_second_commit(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    metadata = FixedMetadataClient("topic-id")

    class SwitchingConsumer(FakeConsumer):
        def commit(self, offsets, timeout):
            try:
                super().commit(offsets, timeout)
            finally:
                metadata.identity = "replacement-topic-id"

    consumer = SwitchingConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT,
        ),
    ], failed_commits=1)
    supervisor = _supervisor(consumer_database, consumer, metadata)
    supervisor.start()
    try:
        _wait(lambda: consumer.closed)
    finally:
        assert supervisor.stop()
    assert [item[0].offset for item in consumer.commits] == [1]
    assert consumer.committed_offsets.get(0) is None
    with consumer_database.session_factory() as session:
        assert session.get(Source, source_id).is_archived
        assert session.scalar(select(func.count()).select_from(ProcessedKafkaRecord)) == 1
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


def test_metadata_outage_after_durable_write_delays_commit_without_duplicate(
    consumer_database, clean_consumer_data, monkeypatch
):
    from app.kafka import KafkaMetadataError
    from app.services.implementations.processing import DurableProcessingServiceImpl

    _, source_id = _source(consumer_database)

    class FlakyMetadata(FixedMetadataClient):
        fail_next = False
        failed = False

        def metadata(self, config, timeout):
            if self.fail_next:
                self.fail_next = False
                self.failed = True
                raise KafkaMetadataError("unavailable")
            return super().metadata(config, timeout)

    metadata = FlakyMetadata("topic-id")
    original = DurableProcessingServiceImpl.process

    def fail_metadata_after_write(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        metadata.fail_next = True
        return result

    monkeypatch.setattr(DurableProcessingServiceImpl, "process", fail_metadata_after_write)
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(consumer_database, consumer, metadata)
    supervisor.start()
    try:
        _wait(lambda: consumer.commits == [(KafkaPartition("events", 0, 1),)])
    finally:
        assert supervisor.stop()
    assert metadata.failed and consumer.paused and consumer.resumed
    with consumer_database.session_factory() as session:
        assert session.get(Source, source_id).is_enabled
        assert session.scalar(select(func.count()).select_from(ProcessedKafkaRecord)) == 1


def test_consumer_never_skips_unknown_position_with_history(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory() as session:
        session.add(ProcessedKafkaRecord(
            connection_identity=connection_id,
            connection_id=connection_id,
            source_id=source_id,
            normalizer_id=None,
            kafka_topic="events",
            kafka_topic_identity="topic-id",
            kafka_partition=0,
            kafka_offset=0,
            result_status="failed",
            backend_received_at=RECEIVED_AT,
            backend_processed_at=RECEIVED_AT + timedelta(seconds=1),
        ))
        session.commit()
    consumer = FakeConsumer([KafkaAssignment("events", (0,))], committed={0: None})
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: consumer.closed)
    finally:
        supervisor.stop()
    assert consumer.assigned == []
    assert consumer.commits == []


def test_retention_gap_is_durable_before_assignment_and_other_partition_continues(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    payload = b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}'

    class CheckedConsumer(FakeConsumer):
        def watermarks(self, topic, partition, timeout):
            return (4 if partition == 0 else 0), 20

        def assign(self, partitions):
            with consumer_database.session_factory() as session:
                gaps = list(session.scalars(select(KafkaOperationalEvent)))
            assert [(gap.offset_start, gap.offset_end) for gap in gaps] == [(2, 4)]
            super().assign(partitions)

    consumer = CheckedConsumer(
        [
            KafkaAssignment("events", (0, 1)),
            KafkaMessage("events", 0, 4, payload, RECEIVED_AT),
            KafkaMessage("events", 1, 0, payload, RECEIVED_AT),
        ],
        committed={0: 2, 1: 0},
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) == 2)
    finally:
        assert supervisor.stop()
    assert consumer.assigned == [
        (KafkaPartition("events", 0, 4), KafkaPartition("events", 1, 0))
    ]
    assert {offsets[0].offset for offsets in consumer.commits} == {1, 5}
    with consumer_database.session_factory() as session:
        gap = session.scalar(select(KafkaOperationalEvent))
        assert gap is not None
        assert (gap.source_identity, gap.connection_identity, gap.kafka_partition) == (
            source_id, connection_id, 0
        )
        assert gap.reason_code == "offset_below_low_watermark"
        assert gap.old_topic_identity == "topic-id"
        assert session.scalar(select(ProcessedKafkaRecord).where(
            ProcessedKafkaRecord.kafka_offset == 2
        )) is None


def test_sparse_offset_stops_only_affected_partition_after_one_seek(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    payload = b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}'
    consumer = FakeConsumer(
        [
            KafkaAssignment("events", (0, 1)),
            KafkaMessage("events", 0, 2, payload, RECEIVED_AT),
            KafkaMessage("events", 1, 0, payload, RECEIVED_AT),
        ],
        committed={0: 1, 1: 0},
        low=0,
        high=3,
        resume_events=(KafkaMessage("events", 0, 2, payload, RECEIVED_AT),),
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) == 1 and len(consumer.seeks) == 1)
        _wait(lambda: supervisor._workers[source_id].stopped_partitions.get(0)
              == "offset_missing_within_watermarks")
        sleep(0.1)
    finally:
        assert supervisor.stop()
    assert consumer.seeks == [KafkaPartition("events", 0, 1)]
    assert [item[0].offset for item in consumer.commits] == [1]
    with consumer_database.session_factory() as session:
        assert session.scalar(select(ParsedLog).where(
            ParsedLog.source_id == source_id,
            ParsedLog.kafka_partition == 1,
        )) is not None
        assert session.scalar(select(KafkaOperationalEvent)) is None


def test_targeted_topic_identity_change_precedes_new_generation_processing(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    payload = b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}'

    class SwitchingMetadata(FixedMetadataClient):
        targeted_calls = 0

        def metadata_for_topic(self, config, topic_name, timeout):
            self.targeted_calls += 1
            return self.metadata(config, timeout)

    metadata = SwitchingMetadata("topic-id")

    class SwitchingConsumer(FakeConsumer):
        def commit(self, offsets, timeout):
            super().commit(offsets, timeout)
            metadata.identity = "replacement-topic-id"

    consumer = SwitchingConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage("events", 0, 0, payload, RECEIVED_AT),
        KafkaMessage("events", 0, 1, payload, RECEIVED_AT),
    ])
    supervisor = _supervisor(consumer_database, consumer, metadata)
    supervisor.start()
    try:
        _wait(lambda: consumer.closed)
    finally:
        assert supervisor.stop()
    assert metadata.targeted_calls >= 2
    assert [item[0].offset for item in consumer.commits] == [1]
    with consumer_database.session_factory() as session:
        source = session.get(Source, source_id)
        assert source.is_archived
        assert session.scalar(select(ProcessedKafkaRecord).where(
            ProcessedKafkaRecord.source_id == source_id,
            ProcessedKafkaRecord.kafka_offset == 1,
        )) is None


def test_retention_gap_restart_and_growing_low_do_not_duplicate_or_hide_ranges(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    for low, expected in ((4, [(2, 4)]), (4, [(2, 4)]), (7, [(2, 4), (4, 7)])):
        consumer = FakeConsumer([KafkaAssignment("events", (0,))], committed={0: 2}, low=low)
        supervisor = _supervisor(consumer_database, consumer)
        supervisor.start()
        try:
            _wait(lambda current=consumer: bool(current.assigned))
        finally:
            assert supervisor.stop()
        assert consumer.assigned[0] == (KafkaPartition("events", 0, low),)
        with consumer_database.session_factory() as session:
            intervals = list(session.execute(
                select(KafkaOperationalEvent.offset_start, KafkaOperationalEvent.offset_end)
                .order_by(KafkaOperationalEvent.offset_start)
            ))
        assert intervals == expected


def test_restart_after_gap_commit_but_before_assignment_reuses_event(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    assignment_failed = threading.Event()

    class FailAssignment(FakeConsumer):
        def assign(self, partitions):
            assignment_failed.set()
            raise KafkaConsumerError("unavailable")

    first = FailAssignment([KafkaAssignment("events", (0,))], committed={0: 2}, low=4)
    supervisor = _supervisor(consumer_database, first)
    supervisor.start()
    try:
        assert assignment_failed.wait(3)
    finally:
        assert supervisor.stop()
    assert not first.assigned and not first.commits
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1
    restarted = FakeConsumer([KafkaAssignment("events", (0,))], committed={0: 2}, low=4)
    supervisor = _supervisor(consumer_database, restarted)
    supervisor.start()
    try:
        _wait(lambda: bool(restarted.assigned))
    finally:
        assert supervisor.stop()
    assert restarted.assigned == [(KafkaPartition("events", 0, 4),)]
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


@pytest.mark.parametrize(
    ("committed", "low", "reason"),
    [({0: None}, 4, "position_unknown_after_history"),
     ({0: 2}, 3, "retention_watermark_regressed")],
)
def test_gap_history_rejects_unknown_position_or_regressed_watermark(
    consumer_database, clean_consumer_data, committed, low, reason
):
    _, source_id = _source(consumer_database)
    first = FakeConsumer([KafkaAssignment("events", (0,))], committed={0: 2}, low=4)
    supervisor = _supervisor(consumer_database, first)
    supervisor.start()
    try:
        _wait(lambda: bool(first.assigned))
    finally:
        assert supervisor.stop()
    restarted = FakeConsumer([KafkaAssignment("events", (0,))], committed=committed, low=low)
    supervisor = _supervisor(consumer_database, restarted)
    supervisor.start()
    try:
        _wait(lambda: source_id in supervisor._workers and
              supervisor._workers[source_id].stopped_partitions.get(0) == reason)
    finally:
        assert supervisor.stop()
    assert not restarted.assigned and not restarted.commits
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


def test_watermark_advancing_after_assignment_is_recorded_before_later_message(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)

    class AdvancingConsumer(FakeConsumer):
        def __init__(self, events):
            super().__init__(events, committed={0: 2})
            self.watermark_calls = 0

        def watermarks(self, topic, partition, timeout):
            self.watermark_calls += 1
            return (2 if self.watermark_calls == 1 else 4), 10

    consumer = AdvancingConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 4,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: consumer.commits == [(KafkaPartition("events", 0, 5),)])
    finally:
        assert supervisor.stop()
    assert consumer.paused and consumer.resumed
    with consumer_database.session_factory() as session:
        gap = session.scalar(select(KafkaOperationalEvent))
        assert (gap.offset_start, gap.offset_end) == (2, 4)


def test_retention_gap_storage_failure_never_advances_before_retry(
    consumer_database, clean_consumer_data, monkeypatch
):
    from sqlalchemy.exc import SQLAlchemyError

    from app.repositories.sqlalchemy.consumers import SqlAlchemyConsumerStateRepository

    _source(consumer_database)
    original = SqlAlchemyConsumerStateRepository.record_retention_gap
    attempts = 0

    def fail_once(repository, *args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SQLAlchemyError("simulated storage outage")
        return original(repository, *args)

    monkeypatch.setattr(SqlAlchemyConsumerStateRepository, "record_retention_gap", fail_once)
    consumer = FakeConsumer([KafkaAssignment("events", (0,))], committed={0: 2}, low=4)
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: attempts >= 2 and bool(consumer.assigned))
    finally:
        assert supervisor.stop()
    assert consumer.assigned == [(KafkaPartition("events", 0, 4),)]
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


def test_retention_gap_storage_retry_does_not_block_other_partition(
    consumer_database, clean_consumer_data, monkeypatch
):
    from sqlalchemy.exc import SQLAlchemyError

    from app.repositories.sqlalchemy.consumers import SqlAlchemyConsumerStateRepository

    _source(consumer_database)
    allow_gap = threading.Event()
    original = SqlAlchemyConsumerStateRepository.record_retention_gap

    def delayed_gap(repository, *args):
        if not allow_gap.is_set():
            raise SQLAlchemyError("simulated storage outage")
        return original(repository, *args)

    monkeypatch.setattr(SqlAlchemyConsumerStateRepository, "record_retention_gap", delayed_gap)

    class TwoPartitionConsumer(FakeConsumer):
        def watermarks(self, topic, partition, timeout):
            return (4 if partition == 0 else 0), 10

    payload = b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}'
    consumer = TwoPartitionConsumer([
        KafkaAssignment("events", (0, 1)),
        KafkaMessage("events", 0, 4, payload, RECEIVED_AT),
        KafkaMessage("events", 1, 0, payload, RECEIVED_AT),
    ], committed={0: 2, 1: 0})
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: consumer.commits == [(KafkaPartition("events", 1, 1),)])
        assert consumer.assigned[0] == (KafkaPartition("events", 1, 0),)
        with consumer_database.session_factory() as session:
            assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 0
        allow_gap.set()
        _wait(lambda: any(offsets[0].partition == 0 for offsets in consumer.commits))
    finally:
        allow_gap.set()
        assert supervisor.stop()
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 1


@pytest.mark.parametrize(
    ("position", "low", "high", "expected_assignment"),
    [(4, 4, 8, True), (8, 4, 8, True), (9, 4, 8, False)],
)
def test_assignment_watermark_boundaries_do_not_create_gap(
    consumer_database, clean_consumer_data, position, low, high, expected_assignment
):
    _source(consumer_database)
    consumer = FakeConsumer(
        [KafkaAssignment("events", (0,))], committed={0: position}, low=low, high=high
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        if expected_assignment:
            _wait(lambda: bool(consumer.assigned))
        else:
            _wait(lambda: consumer.closed)
    finally:
        assert supervisor.stop()
    assert bool(consumer.assigned) is expected_assignment
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 0


def test_invalid_position_stops_only_affected_partition(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    payload = b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok accepted"}'
    consumer = FakeConsumer([
        KafkaAssignment("events", (0, 1)),
        KafkaMessage("events", 1, 0, payload, RECEIVED_AT),
    ], committed={0: 11, 1: 0}, high=10)
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: consumer.commits == [(KafkaPartition("events", 1, 1),)])
        worker = next(iter(supervisor._workers.values()))
        assert worker.stopped_partitions == {0: "position_outside_available_range"}
    finally:
        assert supervisor.stop()
    assert consumer.assigned == [(KafkaPartition("events", 1, 0),)]
    with consumer_database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 0


def test_restart_uses_confirmed_position_and_assigns_new_partition(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    consumer = FakeConsumer(
        [KafkaAssignment("events", (0, 1))],
        committed={0: 6, 1: 2},
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: bool(consumer.assigned))
    finally:
        assert supervisor.stop()
    assert consumer.assigned == [
        (KafkaPartition("events", 0, 6), KafkaPartition("events", 1, 2))
    ]
    assert consumer.commits == []


def test_rebalance_revokes_then_resumes_from_confirmed_position(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"before rebalance"}',
            RECEIVED_AT,
        ),
        KafkaRevocation("events", (0,)),
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 1,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"after rebalance"}',
            RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) == 2)
    finally:
        assert supervisor.stop()
    assert [entry[0].offset for entry in consumer.assigned] == [0, 1]
    assert consumer.unassigned == 1
    assert [entry[0].offset for entry in consumer.commits] == [1, 2]
    with consumer_database.session_factory() as session:
        assert len(
            list(
                session.scalars(
                    select(ProcessedKafkaRecord).where(
                        ProcessedKafkaRecord.source_id == source_id
                    )
                )
            )
        ) == 2


def test_postgres_failure_does_not_commit_or_advance_same_partition(
    consumer_database, clean_consumer_data, monkeypatch
):
    _source(consumer_database)
    messages = [
        KafkaMessage("events", 0, offset, b"payload", RECEIVED_AT)
        for offset in (0, 1)
    ]
    consumer = FakeConsumer([KafkaAssignment("events", (0,)), *messages])

    def fail_storage(*args, **kwargs):
        raise ProcessingStorageError("storage_unavailable")

    monkeypatch.setattr(
        "app.services.implementations.consumers.DurableProcessingServiceImpl.process",
        fail_storage,
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: bool(consumer.paused))
        sleep(0.1)
    finally:
        assert supervisor.stop()
    assert consumer.commits == []
    assert consumer.seeks
    assert {partition.offset for partition in consumer.seeks} == {0}


def test_topic_identity_migration_roundtrip(
    consumer_database, consumer_database_url, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory() as session:
        session.add(
            ProcessedKafkaRecord(
                connection_identity=connection_id,
                connection_id=connection_id,
                source_id=source_id,
                normalizer_id=None,
                kafka_topic="events",
                kafka_topic_identity="topic-id",
                kafka_partition=0,
                kafka_offset=4,
                result_status="failed",
                backend_received_at=RECEIVED_AT,
                backend_processed_at=RECEIVED_AT,
            )
        )
        session.commit()
    _run_alembic(consumer_database_url, "downgrade", "0005_durable_processing")
    try:
        with consumer_database.engine.connect() as connection:
            assert connection.execute(
                text("SELECT count(*) FROM app.sources WHERE id=:id"), {"id": source_id}
            ).scalar_one() == 1
            assert connection.execute(
                text(
                    "SELECT count(*) FROM logs.processed_kafka_records "
                    "WHERE connection_identity=:connection_id AND kafka_offset=4"
                ),
                {"connection_id": connection_id},
            ).scalar_one() == 1
            assert connection.execute(
                text(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_schema='logs' AND table_name='processed_kafka_records' "
                    "AND column_name='kafka_topic_identity'"
                )
            ).scalar_one() == 0
        _run_alembic(consumer_database_url, "upgrade", "head")
        with consumer_database.engine.connect() as connection:
            assert connection.execute(
                text("SELECT kafka_topic_identity FROM app.sources WHERE id=:id"),
                {"id": source_id},
            ).scalar_one() is None
            assert connection.execute(
                text(
                    "SELECT kafka_topic_identity FROM logs.processed_kafka_records "
                    "WHERE connection_identity=:connection_id AND kafka_offset=4"
                ),
                {"connection_id": connection_id},
            ).scalar_one() is None
    finally:
        _run_alembic(consumer_database_url, "upgrade", "head")


def test_generation_migration_roundtrip_preserves_existing_ledger(
    consumer_database, consumer_database_url, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database, enabled=False)
    with consumer_database.session_factory.begin() as session:
        session.add(ProcessedKafkaRecord(
            connection_identity=connection_id, connection_id=connection_id,
            source_id=source_id, kafka_topic="events", kafka_topic_identity="topic-id",
            kafka_partition=0, kafka_offset=11, result_status="failed",
            backend_received_at=RECEIVED_AT, backend_processed_at=RECEIVED_AT,
        ))
    _run_alembic(consumer_database_url, "downgrade", "0006_kafka_topic_identity")
    try:
        with consumer_database.engine.connect() as connection:
            assert connection.execute(text(
                "SELECT count(*) FROM logs.processed_kafka_records WHERE kafka_offset=11"
            )).scalar_one() == 1
        _run_alembic(consumer_database_url, "upgrade", "head")
        with consumer_database.engine.connect() as connection:
            assert connection.execute(text(
                "SELECT is_archived FROM app.sources WHERE id=:id"
            ), {"id": source_id}).scalar_one() is False
    finally:
        _run_alembic(consumer_database_url, "upgrade", "head")


@pytest.mark.parametrize("identity", [None, ""])
def test_history_with_missing_or_invalid_topic_identity_stops_safely(
    consumer_database, clean_consumer_data, identity
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory() as session:
        session.add(
            ProcessedKafkaRecord(
                connection_identity=connection_id,
                connection_id=connection_id,
                source_id=source_id,
                normalizer_id=None,
                kafka_topic="events",
                kafka_topic_identity="topic-id",
                kafka_partition=0,
                kafka_offset=0,
                result_status="failed",
                backend_received_at=RECEIVED_AT,
                backend_processed_at=RECEIVED_AT,
            )
        )
        session.commit()
    consumer = FakeConsumer([])
    supervisor = _supervisor(
        consumer_database, consumer, FixedMetadataClient(identity)
    )
    supervisor.start()
    try:
        _wait(lambda: not consumer.subscribed)
        sleep(0.05)
    finally:
        supervisor.stop()
    assert consumer.commits == []
    assert consumer.assigned == []


def test_legacy_null_topic_identity_history_is_not_assigned_to_new_generation(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory.begin() as session:
        session.add(ProcessedKafkaRecord(
            connection_identity=connection_id, connection_id=connection_id,
            source_id=source_id, kafka_topic="events", kafka_topic_identity=None,
            kafka_partition=0, kafka_offset=0, result_status="failed",
            backend_received_at=RECEIVED_AT, backend_processed_at=RECEIVED_AT,
        ))
    consumer = FakeConsumer([KafkaAssignment("events", (0,))])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: source_id in supervisor._workers and
              supervisor._workers[source_id].state.reason == "legacy_topic_identity_unknown")
    finally:
        assert supervisor.stop()
    assert not consumer.assigned and not consumer.commits


def test_recreated_topic_cannot_reuse_durable_coordinates(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory() as session:
        session.add(
            ProcessedKafkaRecord(
                connection_identity=connection_id,
                connection_id=connection_id,
                source_id=source_id,
                normalizer_id=None,
                kafka_topic="events",
                kafka_topic_identity="old-topic-id",
                kafka_partition=0,
                kafka_offset=0,
                result_status="failed",
                backend_received_at=RECEIVED_AT,
                backend_processed_at=RECEIVED_AT,
            )
        )
        session.commit()
    consumer = FakeConsumer([KafkaAssignment("events", (0,))])
    supervisor = _supervisor(
        consumer_database, consumer, FixedMetadataClient("new-topic-id")
    )
    supervisor.start()
    try:
        _wait(lambda: not consumer.subscribed)
        sleep(0.05)
    finally:
        supervisor.stop()
    assert consumer.assigned == []
    assert consumer.commits == []
    with consumer_database.session_factory() as session:
        archived = session.get(Source, source_id)
        assert archived.is_archived and not archived.is_enabled
        events = list(session.scalars(select(KafkaOperationalEvent)))
        assert len(events) == 1
        assert (events[0].kind, events[0].old_topic_identity,
                events[0].new_topic_identity) == (
            "topic_recreated", "topic-id", "new-topic-id"
        )


def test_recreation_storage_outage_neither_archives_nor_reads_new_topic(
    consumer_database, clean_consumer_data, monkeypatch
):
    from sqlalchemy.exc import SQLAlchemyError

    from app.repositories.sqlalchemy.consumers import SqlAlchemyConsumerStateRepository

    _, source_id = _source(consumer_database)

    def unavailable(*_args):
        raise SQLAlchemyError("simulated storage outage")

    monkeypatch.setattr(SqlAlchemyConsumerStateRepository, "archive_recreated", unavailable)
    consumer = FakeConsumer([KafkaAssignment("events", (0,))])
    supervisor = _supervisor(consumer_database, consumer, FixedMetadataClient("new-topic-id"))
    supervisor.start()
    try:
        _wait(lambda: source_id in supervisor._workers and
              supervisor._workers[source_id].state.reason == "storage_unavailable")
    finally:
        assert supervisor.stop()
    assert not consumer.assigned and not consumer.commits
    with consumer_database.session_factory() as session:
        source = session.get(Source, source_id)
        assert not source.is_archived and source.is_enabled
        assert session.scalar(select(func.count()).select_from(KafkaOperationalEvent)) == 0


def test_preexisting_duplicate_cluster_sources_cannot_start_two_consumers(
    consumer_database, clean_consumer_data
):
    from dataclasses import replace

    _source(consumer_database)
    _source(consumer_database)

    class SameClusterMetadata(FakeMetadataClient):
        def metadata(self, config, timeout):
            return replace(super().metadata(config, timeout), cluster_identity="one-cluster")

    factory = FakeConsumerFactory(FakeConsumer([KafkaAssignment("events", (0,))]))
    supervisor = KafkaSourceSupervisor(
        consumer_database.session_factory,
        NormalizationEngine(PackagedEcsCatalog.load()),
        SameClusterMetadata(), factory,
        metadata_timeout=1, poll_timeout=0.02, retry_delay=0.02, sync_interval=0.02,
    )
    supervisor.start()
    try:
        _wait(lambda: len(supervisor._workers) == 2 and
              any(worker.state.reason == "cluster_identity_conflict"
                  for worker in supervisor._workers.values()))
    finally:
        assert supervisor.stop()
    assert len(factory.groups) == 1


def test_patch_same_topic_after_recreation_preserves_identity_with_history(
    consumer_database, clean_consumer_data
):
    from app.core.errors import DomainError

    connection_id, source_id = _source(consumer_database, enabled=False)
    with consumer_database.session_factory() as session:
        source = session.get(Source, source_id)
        source.kafka_topic_identity = "old-topic-id"
        session.add(
            ProcessedKafkaRecord(
                connection_identity=connection_id,
                connection_id=connection_id,
                source_id=source_id,
                normalizer_id=None,
                kafka_topic="events",
                kafka_topic_identity="old-topic-id",
                kafka_partition=0,
                kafka_offset=0,
                result_status="failed",
                backend_received_at=RECEIVED_AT,
                backend_processed_at=RECEIVED_AT,
            )
        )
        session.commit()
    with consumer_database.session_factory() as session:
        service = SourceServiceImpl(
            SqlAlchemySourceRepository(session),
            SqlAlchemyKafkaConnectionRepository(session),
            SqlAlchemyNormalizerRepository(session),
            SqlAlchemyUnitOfWork(session),
            FixedMetadataClient("new-topic-id"),
            timeout=1,
        )
        with pytest.raises(DomainError) as error:
            service.update(source_id, SourcePatch(topic_name="events"))
        assert error.value.code == "topic_identity_changed"
    with consumer_database.session_factory() as session:
        source = session.get(Source, source_id)
        assert source.kafka_topic_identity == "old-topic-id"


def test_recreated_source_with_history_processes_new_generation_separately(
    consumer_database, clean_consumer_data
):
    connection_id, source_id = _source(consumer_database)
    with consumer_database.session_factory() as session:
        normalizer_id = session.get(Source, source_id).normalizer_id
        session.add(
            ProcessedKafkaRecord(
                connection_identity=connection_id,
                connection_id=connection_id,
                source_id=source_id,
                normalizer_id=None,
                kafka_topic="events",
                kafka_topic_identity="old-topic-id",
                kafka_partition=0,
                kafka_offset=0,
                result_status="failed",
                backend_received_at=RECEIVED_AT,
                backend_processed_at=RECEIVED_AT,
            )
        )
        session.delete(session.get(Source, source_id))
        session.flush()
        recreated = Source(
            name=f"recreated-{uuid4()}",
            connection_id=connection_id,
            normalizer_id=normalizer_id,
            topic_name="events",
            kafka_topic_identity="new-topic-id",
            is_enabled=True,
        )
        session.add(recreated)
        session.commit()
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(
        consumer_database, consumer, FixedMetadataClient("new-topic-id")
    )
    with consumer_database.session_factory() as session:
        recreated_id = session.scalar(
            select(Source.id).where(Source.name.like("recreated-%"))
        )
    supervisor.start()
    try:
        _wait(lambda: len(consumer.commits) == 1)
    finally:
        assert supervisor.stop()
    assert consumer.assigned == [(KafkaPartition("events", 0, 0),)]
    assert consumer.commits == [(KafkaPartition("events", 0, 1),)]
    with consumer_database.session_factory() as session:
        rows = list(session.scalars(select(ProcessedKafkaRecord).order_by(
            ProcessedKafkaRecord.kafka_topic_identity
        )))
        assert {row.kafka_topic_identity for row in rows} == {"old-topic-id", "new-topic-id"}
        assert any(row.source_id == recreated_id and row.result_status == "complete" for row in rows)


def test_commit_failure_buffers_later_offsets_in_partition_order(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    messages = [
        KafkaMessage("events", 0, offset, b'{"timestamp":"2026-03-02T11:59:00Z","log":"ok"}', RECEIVED_AT)
        for offset in range(3)
    ]
    consumer = FakeConsumer(
        [KafkaAssignment("events", (0,)), *messages], failed_commits=1
    )
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    try:
        _wait(lambda: [item[0].offset for item in consumer.commits] == [1, 1, 2, 3])
    finally:
        supervisor.stop()
    with consumer_database.session_factory() as session:
        records = list(
            session.scalars(
                select(ProcessedKafkaRecord)
                .where(ProcessedKafkaRecord.source_id == source_id)
                .order_by(ProcessedKafkaRecord.kafka_offset)
            )
        )
    assert [record.kafka_offset for record in records] == [0, 1, 2]


def test_adapter_preserves_message_when_assignment_callback_shares_poll() -> None:
    class Partition:
        topic = "events"
        partition = 0

    class Message:
        def error(self):
            return None

        def topic(self):
            return "events"

        def partition(self):
            return 0

        def offset(self):
            return 7

        def value(self):
            return b"payload"

    class Client:
        def subscribe(self, topics, on_assign, on_revoke):
            self.on_assign = on_assign

        def poll(self, timeout):
            self.on_assign(self, [Partition()])
            return Message()

    consumer = ConfluentKafkaConsumer(Client())
    consumer.subscribe("events")
    assert consumer.poll(0.01) == KafkaAssignment("events", (0,))
    consumer.assigned = True
    event = consumer.poll(0.01)
    assert isinstance(event, KafkaMessage)
    assert event.offset == 7
    assert event.value == b"payload"


def test_adapter_commit_wait_obeys_deadline_and_checks_callback() -> None:
    class Client:
        def commit(self, *, offsets, asynchronous):
            assert asynchronous is True
            self.offsets = offsets

        def poll(self, timeout):
            sleep(timeout)
            self.consumer._on_commit(None, self.offsets)

    client = Client()
    consumer = ConfluentKafkaConsumer(client)
    client.consumer = consumer
    consumer.commit((KafkaPartition("events", 0, 9),), 0.2)

    class StalledClient:
        def commit(self, **kwargs):
            self.offsets = kwargs["offsets"]

        def poll(self, timeout):
            sleep(timeout)

    stalled = ConfluentKafkaConsumer(StalledClient())
    started = monotonic()
    with pytest.raises(KafkaConsumerError) as error:
        stalled.commit((KafkaPartition("events", 0, 10),), 0.05)
    assert getattr(error.value, "kind", None) == "timeout"
    assert monotonic() - started < 0.3

    class RejectedClient:
        def commit(self, **kwargs):
            self.offsets = kwargs["offsets"]

        def poll(self, timeout):
            from confluent_kafka import KafkaError

            self.consumer._on_commit(
                KafkaError(KafkaError._ALL_BROKERS_DOWN), []
            )

    rejected_client = RejectedClient()
    rejected = ConfluentKafkaConsumer(rejected_client)
    rejected_client.consumer = rejected
    with pytest.raises(KafkaConsumerError) as error:
        rejected.commit((KafkaPartition("events", 0, 11),), 0.2)
    assert error.value.kind == "unavailable"


def test_supervisor_shutdown_waits_for_current_worker_and_closes_it(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    entered = threading.Event()
    release = threading.Event()

    class SlowConsumer(FakeConsumer):
        def poll(self, timeout):
            entered.set()
            release.wait(2)

    consumer = SlowConsumer([])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    assert entered.wait(2)
    stopped = threading.Event()
    stopper = threading.Thread(target=lambda: (supervisor.stop(), stopped.set()))
    stopper.start()
    try:
        sleep(0.1)
        assert not stopped.is_set()
    finally:
        release.set()
    stopper.join(2)
    assert stopped.is_set()
    assert consumer.closed
    assert all(not worker.is_alive for worker in supervisor._workers.values())
    assert supervisor._thread is not None and not supervisor._thread.daemon


def test_shutdown_during_enabled_lookup_cannot_start_untracked_worker(
    consumer_database, clean_consumer_data, monkeypatch
):
    _source(consumer_database)
    supervisor = _supervisor(consumer_database, FakeConsumer([]))
    lookup_entered = threading.Event()
    release_lookup = threading.Event()
    worker_snapshot_taken = threading.Event()
    original_enabled = supervisor._enabled

    def delayed_enabled():
        lookup_entered.set()
        assert release_lookup.wait(3)
        return original_enabled()

    class TrackedWorkers(dict):
        def values(self):
            result = super().values()
            worker_snapshot_taken.set()
            return result

    monkeypatch.setattr(supervisor, "_enabled", delayed_enabled)
    supervisor._workers = TrackedWorkers()
    supervisor.start()
    stopper = None
    results = []
    try:
        assert lookup_entered.wait(2)
        stopper = threading.Thread(target=lambda: results.append(supervisor.stop()))
        stopper.start()
        assert worker_snapshot_taken.wait(2)
    finally:
        release_lookup.set()
        if stopper is not None:
            stopper.join(4)
        supervisor.stop()
    assert results == [True]
    assert not supervisor._workers
    assert supervisor._thread is not None and not supervisor._thread.is_alive()


def test_supervisor_refuses_shutdown_while_worker_is_still_alive(
    consumer_database, clean_consumer_data
):
    _source(consumer_database)
    entered = threading.Event()
    release = threading.Event()

    class StuckConsumer(FakeConsumer):
        def poll(self, timeout):
            entered.set()
            release.wait()

    consumer = StuckConsumer([])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    assert entered.wait(2)
    try:
        assert supervisor.stop() is False
        worker = next(iter(supervisor._workers.values()))
        assert worker.is_alive
        assert not worker.thread.daemon
        assert not consumer.closed
    finally:
        release.set()
    assert supervisor.stop() is True
    assert consumer.closed
    assert all(not worker.is_alive for worker in supervisor._workers.values())


def test_fast_disable_enable_does_not_overlap_source_workers(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    poll_entered = threading.Event()
    release_poll = threading.Event()
    factory_state = {"created": 0, "active": 0, "max_active": 0}
    state_lock = threading.Lock()

    class TrackedConsumer(FakeConsumer):
        def poll(self, timeout):
            poll_entered.set()
            release_poll.wait()
            sleep(0.005)

        def close(self):
            with state_lock:
                if not self.closed:
                    self.closed = True
                    factory_state["active"] -= 1

    class TrackedFactory:
        def create(self, config, group_id):
            with state_lock:
                factory_state["created"] += 1
                factory_state["active"] += 1
                factory_state["max_active"] = max(
                    factory_state["max_active"], factory_state["active"]
                )
            return TrackedConsumer([])

    supervisor = KafkaSourceSupervisor(
        consumer_database.session_factory,
        NormalizationEngine(PackagedEcsCatalog.load()),
        FakeMetadataClient(),
        TrackedFactory(),
        metadata_timeout=1,
        poll_timeout=0.02,
        retry_delay=0.02,
        sync_interval=0.02,
    )
    supervisor.start()
    assert poll_entered.wait(2)
    try:
        with consumer_database.session_factory.begin() as session:
            session.get(Source, source_id).is_enabled = False
        supervisor.stop_source(source_id)
        with consumer_database.session_factory.begin() as session:
            session.get(Source, source_id).is_enabled = True
        supervisor.wake()
        sleep(0.1)
        with state_lock:
            assert factory_state["created"] == 1
            assert factory_state["active"] == 1
    finally:
        release_poll.set()
    _wait(lambda: factory_state["created"] == 2)
    assert supervisor.stop()
    assert factory_state["active"] == 0
    assert factory_state["max_active"] == 1


def test_shutdown_finishes_inflight_processing_before_closing_worker(
    consumer_database, clean_consumer_data, monkeypatch
):
    _, source_id = _source(consumer_database)
    processing_entered = threading.Event()
    release_processing = threading.Event()
    original_process = DurableProcessingServiceImpl.process

    def delayed_process(service, *args, **kwargs):
        processing_entered.set()
        release_processing.wait()
        return original_process(service, *args, **kwargs)

    monkeypatch.setattr(
        "app.services.implementations.consumers.DurableProcessingServiceImpl.process",
        delayed_process,
    )
    consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage(
            "events", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"shutdown"}',
            RECEIVED_AT,
        ),
    ])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    assert processing_entered.wait(3)
    stopped = threading.Event()
    stopper = threading.Thread(target=lambda: (supervisor.stop(), stopped.set()))
    stopper.start()
    try:
        sleep(0.1)
        assert not stopped.is_set()
        assert not consumer.closed
    finally:
        release_processing.set()
    stopper.join(5)
    assert stopped.is_set()
    assert consumer.closed
    assert [offset[0].offset for offset in consumer.commits] == [1]
    with consumer_database.session_factory() as session:
        assert session.scalar(
            select(ProcessedKafkaRecord).where(
                ProcessedKafkaRecord.source_id == source_id
            )
        ) is not None


def test_deleted_enabled_source_is_stopped_without_another_commit(
    consumer_database, clean_consumer_data
):
    _, source_id = _source(consumer_database)
    consumer = FakeConsumer([KafkaAssignment("events", (0,))])
    supervisor = _supervisor(consumer_database, consumer)
    supervisor.start()
    _wait(lambda: consumer.subscribed == "events")
    with consumer_database.session_factory.begin() as session:
        session.delete(session.get(Source, source_id))
    supervisor.wake()
    try:
        _wait(lambda: consumer.closed)
    finally:
        assert supervisor.stop()
    assert consumer.commits == []


def test_failure_in_one_source_does_not_block_another_source(
    consumer_database, clean_consumer_data, monkeypatch
):
    _, failed_source_id = _source(consumer_database)
    _, healthy_source_id = _source(consumer_database, topic="disabled")
    failed_consumer = FakeConsumer([
        KafkaAssignment("events", (0,)),
        KafkaMessage("events", 0, 0, b"failed", RECEIVED_AT),
    ])
    healthy_consumer = FakeConsumer([
        KafkaAssignment("disabled", (0,)),
        KafkaMessage(
            "disabled", 0, 0,
            b'{"timestamp":"2026-03-02T11:59:00Z","log":"healthy"}',
            RECEIVED_AT,
        ),
    ])

    class PerSourceFactory:
        def create(self, config, group_id):
            source_id = UUID(group_id.removeprefix("diploma-source-"))
            return failed_consumer if source_id == failed_source_id else healthy_consumer

    original_process = DurableProcessingServiceImpl.process

    def fail_one_source(service, source_id, *args, **kwargs):
        if source_id == failed_source_id:
            raise ProcessingStorageError("storage_unavailable")
        return original_process(service, source_id, *args, **kwargs)

    monkeypatch.setattr(
        "app.services.implementations.consumers.DurableProcessingServiceImpl.process",
        fail_one_source,
    )
    supervisor = KafkaSourceSupervisor(
        consumer_database.session_factory,
        NormalizationEngine(PackagedEcsCatalog.load()),
        FakeMetadataClient(),
        PerSourceFactory(),
        metadata_timeout=1,
        poll_timeout=0.02,
        retry_delay=0.02,
        sync_interval=0.02,
    )
    supervisor.start()
    try:
        _wait(lambda: len(healthy_consumer.commits) == 1)
        sleep(0.05)
    finally:
        assert supervisor.stop()
    assert failed_consumer.commits == []
    assert healthy_consumer.commits == [(KafkaPartition("disabled", 0, 1),)]
    with consumer_database.session_factory() as session:
        assert session.scalar(
            select(ProcessedKafkaRecord).where(
                ProcessedKafkaRecord.source_id == healthy_source_id
            )
        ) is not None
