import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app.adapters.pgoutput import DecodedCommit, InvalidReplicationMessage
from app.models import (
    AggregateSchedule,
    AvailabilityMarker,
    InputProbe,
    KafkaCheckpoint,
    ReplicationProgress,
    SourceInterruption,
)
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.availability import SqlAlchemyAvailabilityProgressRepository
from app.repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository
from app.repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
from app.repositories.sqlalchemy.windows import SqlAlchemyWorkingWindowRepository
from app.services.implementations.availability_materializer import AvailabilityMaterializer
from app.services.implementations.input_attestations import canonical_payload, ingest_attestation
from app.services.implementations.saved_coverage import evaluate_saved_coverage
from app.services.implementations.working_windows import (
    WindowNotReady,
    WorkingWindowService,
)


@pytest.fixture
def sessions():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("TEST_DATABASE_URL must name an isolated test PostgreSQL")
    assert "/diploma_db" not in url
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    engine = create_engine(url)
    try:
        yield sessionmaker(bind=engine, expire_on_commit=False)
    finally:
        engine.dispose()


class Feedback:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def acknowledge(self, lsn):
        self.sent.append(lsn)
        if self.fail:
            raise ConnectionError("simulated lost feedback")


def persist_then_ack(sessions, feedback, commit):
    with sessions() as session:
        AvailabilityMaterializer(
            SqlAlchemyAvailabilityProgressRepository(session),
            SqlAlchemyUnitOfWork(session), feedback,
        ).apply(commit)


def window_service(session):
    from types import SimpleNamespace
    return WorkingWindowService(
        SqlAlchemyWorkingWindowRepository(session), SqlAlchemyCoverageEvidenceRepository(session),
        SimpleNamespace(status=lambda: SimpleNamespace(ready=True)), SqlAlchemyUnitOfWork(session),
    )


def test_materialization_is_committed_before_feedback_and_replay_is_idempotent(sessions):
    marker_id, source_id, event_id = uuid4(), uuid4(), uuid4()
    with sessions.begin() as session:
        session.add(AvailabilityMarker(id=marker_id, source_identity=source_id,
                                       kind="event", event_id=event_id))
    commit = DecodedCommit(1, 110, 120, datetime(2026, 1, 1, tzinfo=UTC),
                           ((str(marker_id), str(source_id), "event", str(event_id)),))
    first = Feedback(fail=True)
    with pytest.raises(ConnectionError):
        persist_then_ack(sessions, first, commit)
    with sessions() as session:
        assert session.get(AvailabilityMarker, marker_id).availability_at == commit.committed_at
        assert session.get(ReplicationProgress, 1).durable_lsn >= commit.end_lsn
    second = Feedback()
    persist_then_ack(sessions, second, commit)
    assert second.sent == [120]


def test_marker_mismatch_rolls_back_and_does_not_acknowledge(sessions):
    marker_id, source_id, event_id = uuid4(), uuid4(), uuid4()
    with sessions.begin() as session:
        session.add(AvailabilityMarker(id=marker_id, source_identity=source_id,
                                       kind="event", event_id=event_id))
    commit = DecodedCommit(2, 130, 140, datetime(2026, 1, 1, tzinfo=UTC),
                           ((str(marker_id), str(uuid4()), "event", str(event_id)),))
    feedback = Feedback()
    with pytest.raises(InvalidReplicationMessage):
        persist_then_ack(sessions, feedback, commit)
    assert not feedback.sent
    with sessions() as session:
        assert session.scalar(select(AvailabilityMarker.availability_at).where(
            AvailabilityMarker.id == marker_id)) is None


def test_missing_marker_without_an_active_writer_is_not_acknowledged(sessions):
    marker_id, source_id = uuid4(), uuid4()
    commit = DecodedCommit(1, 150, 160, datetime(2026, 1, 1, tzinfo=UTC),
                           ((str(marker_id), str(source_id), "barrier", None),))
    feedback = Feedback()
    with pytest.raises(InvalidReplicationMessage, match="fields=missing") as caught:
        persist_then_ack(sessions, feedback, commit)
    assert not feedback.sent
    assert str(marker_id) in str(caught.value) and "xid=1" in str(caught.value)
    with sessions() as session:
        assert session.get(AvailabilityMarker, marker_id) is None


def test_working_window_requires_materialized_barrier_and_advances_once(sessions):
    source, connection, schedule = uuid4(), uuid4(), uuid4()
    with sessions.begin() as session:
        session.execute(text("""INSERT INTO app.kafka_connections
            (id, name, cluster_identity, bootstrap_servers)
            VALUES (:id, :name, 'test', ARRAY['localhost:9092'])"""),
            {"id": connection, "name": str(connection)})
        session.execute(text("""INSERT INTO app.sources
            (id, name, connection_id, topic_name, is_enabled) VALUES (:id, :name, :connection, :topic, true)"""),
            {"id": source, "name": str(source), "connection": connection, "topic": str(source)})
        session.add(AggregateSchedule(id=schedule, source_identity=source,
                    anchor_utc=datetime.now(UTC) - timedelta(seconds=2), duration_seconds=1))
    with sessions() as session:
        marker_id = window_service(session).prepare_barrier(schedule)
    with pytest.raises(WindowNotReady, match="barrier_not_materialized"), sessions() as session:
        window_service(session).close_next_window(schedule)
    with sessions.begin() as session:
        marker = session.get(AvailabilityMarker, marker_id)
        marker.availability_at = datetime.now(UTC)
    with sessions() as session:
        point = window_service(session).close_next_window(schedule)
        assert point.window_index == 0 and point.event_count == 0
        assert point.event_time_min is None and point.event_time_max is None
        assert point.completeness == "unknown"
    with sessions() as session:
        assert session.get(AggregateSchedule, schedule).next_index == 1
    with pytest.raises(WindowNotReady, match="barrier_not_materialized"), sessions() as session:
        window_service(session).close_next_window(schedule)


def test_caller_cannot_inject_completeness_evidence():
    with pytest.raises(TypeError):
        WorkingWindowService(None, None, None, None).close_next_window(uuid4(), evidence=[])


def test_saved_signed_coverage_requires_matching_kafka_checkpoint(sessions):
    source, connection, probe_id, generation = uuid4(), uuid4(), uuid4(), uuid4()
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    end = datetime.now(UTC) - timedelta(seconds=5)
    start = end - timedelta(seconds=10)
    report = {"id": str(uuid4()), "probe_id": str(probe_id), "source_id": str(source),
              "input_generation": str(generation), "sequence": 42,
              "start": start.isoformat(), "end": end.isoformat(), "state": "covered",
              "topic_identity": "test-generation", "start_positions": {"0": 0},
              "end_positions": {"0": 0}}
    signature = private.sign(canonical_payload(report))
    with sessions.begin() as session:
        session.execute(text("""INSERT INTO app.kafka_connections (id,name,cluster_identity,bootstrap_servers)
            VALUES (:id,:name,:cluster,ARRAY['localhost:9092'])"""),
            {"id": connection, "name": str(connection), "cluster": str(connection)})
        session.execute(text("""INSERT INTO app.sources (id,name,connection_id,topic_name,kafka_topic_identity,is_enabled)
            VALUES (:id,:name,:conn,:topic,'test-generation',true)"""),
            {"id": source, "name": str(source), "conn": connection, "topic": str(source)})
        session.add(InputProbe(id=probe_id, source_identity=source, input_generation=generation,
                               public_key=public, is_active=True))
    with sessions.begin() as session:
        with pytest.raises(ValueError, match="signature"):
            ingest_attestation(SqlAlchemyInputAttestationRepository(session), report, bytes(64))
        ingest_attestation(SqlAlchemyInputAttestationRepository(session), report, signature)
    barrier_at = datetime.now(UTC)
    with sessions() as session:
        assert evaluate_saved_coverage(
            SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at,
        )[0] == "unknown"
    worker_id = uuid4()
    with sessions.begin() as session:
        for at in (start - timedelta(seconds=1), start + timedelta(seconds=5), end + timedelta(seconds=1)):
            session.add(KafkaCheckpoint(source_identity=source, topic_identity="test-generation",
                                        worker_instance=worker_id, observed_at=at,
                                        partitions={"0": {"low": 0, "high": 0, "committed": 0}}))
        session.add(KafkaCheckpoint(source_identity=source, topic_identity="test-generation",
                                    worker_instance=worker_id,
                                    partitions={"0": {"low": 0, "high": 0, "committed": 0}}))
    with sessions() as session:
        state, reason, refs = evaluate_saved_coverage(SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at)
        assert (state, reason) == ("complete", "verified_input_and_kafka_coverage")
        assert refs["attestations"] == [report["id"]] and refs["kafka_checkpoint"]
        assert len(refs["consumer_heartbeats"]) >= 3
    with sessions.begin() as session:
        middle = session.scalar(select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source,
            KafkaCheckpoint.observed_at == start + timedelta(seconds=5)))
        middle.worker_instance = uuid4()
    with sessions() as session:
        assert evaluate_saved_coverage(
            SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at,
        )[:2] == ("unknown", "consumer_coverage_gap")
    with sessions.begin() as session:
        middle = session.scalar(select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source,
            KafkaCheckpoint.observed_at == start + timedelta(seconds=5)))
        middle.worker_instance = worker_id
    with sessions.begin() as session:
        middle = session.scalar(select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source,
            KafkaCheckpoint.observed_at == start + timedelta(seconds=5)))
        session.delete(middle)
    with sessions() as session:
        assert evaluate_saved_coverage(
            SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at,
        )[:2] == ("unknown", "consumer_coverage_gap")
    with sessions.begin() as session:
        session.add(KafkaCheckpoint(source_identity=source, topic_identity="test-generation",
                                    worker_instance=worker_id, observed_at=start + timedelta(seconds=5),
                                    partitions={"0": {"low": 0, "high": 0, "committed": 0}}))
    with sessions.begin() as session:
        session.add(KafkaCheckpoint(source_identity=source, topic_identity="test-generation",
                                    worker_instance=worker_id,
                                    partitions={"0": {"low": 0, "high": 1, "committed": 0}}))
    with sessions() as session:
        assert evaluate_saved_coverage(SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at)[0] == "unknown"
    with sessions.begin() as session:
        session.add(SourceInterruption(source_identity=source, kind="processing_stopped",
                                       started_at=start, ended_at=end))
    with sessions() as session:
        assert evaluate_saved_coverage(SqlAlchemyCoverageEvidenceRepository(session), source, start, end, barrier_at)[0] == "interrupted"


@pytest.mark.parametrize("age,barrier_age,expected", [
    (0, 1, True), (10, 11, True), (10.001, 11, False),
    (-.001, 1, False), (2, 1, False),
])
def test_checkpoint_freshness_uses_one_explicit_batch_reference(sessions, age, barrier_age, expected):
    source, worker = uuid4(), uuid4()
    # This old, fixed reference proves the query doesn't silently substitute
    # wall clock for the batch's reference or accept future/pre-barrier proof.
    reference = datetime.now(UTC) - timedelta(seconds=100)
    with sessions.begin() as session:
        session.add(KafkaCheckpoint(source_identity=source, topic_identity="reference-topic",
            worker_instance=worker, observed_at=reference - timedelta(seconds=age),
            partitions={"0": {"low": 0, "high": 0, "committed": 0}}))
    with sessions() as session:
        evidence = SqlAlchemyCoverageEvidenceRepository(session)
        end = reference - timedelta(seconds=20)
        barrier = reference - timedelta(seconds=barrier_age)
        assert (evidence.checkpoint(source, "reference-topic", end, barrier, reference) is not None) is expected
        assert evidence.checkpoint(source, "another-topic", end, barrier, reference) is None
        assert evidence.checkpoint(source, "reference-topic", end, barrier) is None
