"""Real writer/pgoutput/probe fixture for formula-to-RCF regressions.

Uses the backend-db PostgreSQL and existing adapters; owns one disposable DB,
capture slot/role and two bounded threads. No assigned availability/completeness.
"""

import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg import sql
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.adapters.detector_lock import DetectorSessionLock
from app.adapters.pgoutput import PgOutputDecoder
from app.adapters.pgoutput_stream import ReplicationStream
from app.adapters.wal_bootstrap import PUBLICATION, ROLE, SLOT, bootstrap
from app.aggregate_composition import build_aggregate_service
from app.db import Database
from app.detector_composition import build_detector_service
from app.models import (
    InputProbe,
    KafkaCheckpoint,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
)
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.availability import SqlAlchemyAvailabilityProgressRepository
from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from app.repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
from app.services.implementations.availability_materializer import AvailabilityMaterializer
from app.services.implementations.input_attestations import canonical_payload, ingest_attestation
from app.services.implementations.working_windows import WorkingWindowService


def wait_until(moment):
    while (remaining := (moment - datetime.now(UTC)).total_seconds()) > 0:
        time.sleep(min(.1, remaining))


@contextmanager
def real_formula_history(catalog):
    base = make_url(os.environ["TEST_DATABASE_URL"])
    assert (base.database or "").startswith("diploma_test_review_")
    name = "diploma_test_formula_" + uuid4().hex[:12]
    url = base.set(database=name).render_as_string(hide_password=False)
    admin_url = base.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False)
    dsn = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    db, lock = None, None
    threads = []
    stop = threading.Event()
    capture_read = threading.Lock()
    errors = []
    with psycopg.connect(admin_url, autocommit=True) as admin:
        assert not admin.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (SLOT,)).fetchone()
        assert not admin.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (ROLE,)).fetchone()
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            env = {**os.environ, "DATABASE_URL": url}
            env.pop("DATABASE_URL_FILE", None)
            subprocess.run([sys.executable, "-B", "-m", "alembic", "upgrade", "head"], env=env, check=True)
            bootstrap(dsn, "synthetic_formula_capture_password_123")
            db = Database(url)
            source_id, connection_id, probe_id, generation, worker = [uuid4() for _ in range(5)]
            private = Ed25519PrivateKey.generate()
            with db.session_factory.begin() as session:
                session.add(KafkaConnection(id=connection_id, name=str(connection_id),
                    cluster_identity=str(connection_id), bootstrap_servers=["synthetic:9092"]))
                session.flush()
                session.add(Source(id=source_id, name=str(source_id), connection_id=connection_id,
                    topic_name=str(source_id), kafka_topic_identity="formula-synthetic", is_enabled=True))
                session.add(InputProbe(id=probe_id, source_identity=source_id, input_generation=generation,
                    public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
            fixture = SimpleNamespace(database=db, catalog=catalog, source_id=source_id, count=0,
                sequence=0, position=0, coverage_start=None)

            def decoder():
                try:
                    parser = PgOutputDecoder("logs", "availability_markers", ("id", "source_identity", "kind", "event_id"))
                    with ReplicationStream(dsn, SLOT, PUBLICATION, parser) as stream:
                        while not stop.is_set():
                            commit = stream.read_commit(.1)
                            with db.session_factory() as session:
                                materializer = AvailabilityMaterializer(SqlAlchemyAvailabilityProgressRepository(session),
                                    SqlAlchemyUnitOfWork(session), stream)
                                with capture_read:
                                    materializer.apply(commit) if commit is not None else materializer.heartbeat()
                except Exception as error:  # noqa: BLE001 - fail the parent test on every worker error
                    errors.append(error)

            def checkpoints():
                try:
                    while not stop.is_set():
                        with db.session_factory.begin() as session:
                            position = fixture.count
                            session.add(KafkaCheckpoint(source_identity=source_id, topic_identity="formula-synthetic",
                                worker_instance=worker, partitions={"0": {"low": 0, "high": position, "committed": position}}))
                        stop.wait(.1)
                except Exception as error:  # noqa: BLE001 - fail the parent test on every worker error
                    errors.append(error)

            for target in (decoder, checkpoints):
                thread = threading.Thread(target=target)
                threads.append(thread)
                thread.start()

            def write(count):
                with db.session_factory.begin() as session:
                    writer = SqlAlchemyProcessingRepository(session)
                    for index in range(fixture.count, fixture.count + count):
                        at = session.scalar(select(func.clock_timestamp()))
                        record = ProcessedKafkaRecord(id=uuid4(), connection_id=connection_id,
                            connection_identity=connection_id, source_id=source_id,
                            kafka_topic_identity="formula-synthetic", kafka_topic=str(source_id),
                            kafka_partition=0, kafka_offset=index, result_status="complete",
                            backend_received_at=at, backend_processed_at=at)
                        event = ParsedLog(id=uuid4(), source_id=source_id, connection_id=connection_id,
                            normalizer_version=1, normalizer_name="Synthetic", source_name="Synthetic", connection_name="Synthetic",
                            kafka_topic=str(source_id), kafka_partition=0, kafka_offset=index,
                            deduplication_key=str(uuid4()), raw="synthetic", ecs_data={"@timestamp": at.isoformat(),
                                "network": {"bytes": 7}, "event": {"category": ["network"]}},
                            fluent_bit_collected_at=at, backend_received_at=at, backend_processed_at=at)
                        assert writer.store_event(record, event)
                fixture.count += count

            def attest(end):
                assert end <= datetime.now(UTC)
                fixture.sequence += 1
                payload = {"id": str(uuid4()), "probe_id": str(probe_id), "source_id": str(source_id),
                    "input_generation": str(generation), "sequence": fixture.sequence,
                    "start": fixture.coverage_start.isoformat(), "end": end.isoformat(), "state": "covered",
                    "topic_identity": "formula-synthetic", "start_positions": {"0": fixture.position},
                    "end_positions": {"0": fixture.count}}
                with db.session_factory.begin() as session:
                    ingest_attestation(SqlAlchemyInputAttestationRepository(session), payload, private.sign(canonical_payload(payload)))
                fixture.coverage_start, fixture.position = end, fixture.count

            def aggregates(session):
                api = build_aggregate_service(session, catalog)
                real_status = api.capture.status
                def stable_status():
                    # The real guard performs separate progress/slot reads.
                    # Keep this controlled snapshot comparison independent of
                    # a decoder commit/feedback occurring BETWEEN those reads.
                    # Every health/budget check still runs against real state.
                    with capture_read:
                        return real_status()
                api.capture.status = stable_status
                return api

            def close_one(card):
                deadline = time.monotonic() + 5
                with db.session_factory() as session:
                    api = aggregates(session)
                    barrier_id = WorkingWindowService(api.windows, api.evidence, api.capture, api.uow).prepare_barrier(card.schedule_id)
                # A closed point is immutable. Observe the accepted barrier and
                # independently published post-barrier checkpoint BEFORE asking
                # the real service to close it; do not repair an unknown point.
                while True:
                    assert not errors, errors
                    with db.session_factory() as session:
                        api = aggregates(session)
                        barrier = api.windows.barrier(barrier_id)
                        if (barrier is not None and barrier.availability_at is not None
                                and api.evidence.checkpoint(source_id, None, barrier.created_at, barrier.availability_at) is not None):
                            break
                    assert time.monotonic() < deadline
                    time.sleep(.02)
                with db.session_factory() as session:
                    return aggregates(session).advance(card.id)

            fixture.write, fixture.attest, fixture.close_one = write, attest, close_one
            fixture.aggregates = aggregates
            lock = DetectorSessionLock(url)
            pid = lock.acquire()
            assert pid is not None
            with db.session_factory.begin() as session:
                owner = SqlAlchemyDetectorRepository(session).acquire_owner(uuid4(), pid)
            fixture.detectors = lambda session: build_detector_service(session, catalog, owner)
            yield fixture
            assert not errors, errors
        finally:
            stop.set()
            for thread in threads:
                thread.join(5)
                assert not thread.is_alive()
            if lock is not None:
                lock.close()
            if db is not None:
                db.dispose()
            owned_slot = admin.execute("SELECT database FROM pg_replication_slots WHERE slot_name=%s", (SLOT,)).fetchone()
            if owned_slot is not None:
                assert owned_slot[0] == name
                admin.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
            if admin.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (ROLE,)).fetchone():
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(ROLE)))


def create_formula(fixture):
    now = datetime.now(UTC)
    anchor = now.replace(microsecond=0) - timedelta(seconds=100)
    rule = {"auxiliary": [
        {"key": "events", "name": "Events", "op": "count"},
        {"key": "twice", "name": "Twice", "op": "derive", "expr": {
            "op": "multiply", "args": [{"ref": "events"}, {"value": 2}]}}],
        "features": [{"key": "n", "name": "Final", "op": "formula", "expr": {
            "op": "add", "args": [{"ref": "twice"}, {"aggregate": {"op": "count"}}]}}]}
    body = AggregatePreviewRequest(source_id=fixture.source_id, aggregate_rule=rule,
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 2},
        window_start_utc=anchor, time_basis="backend_processed_at_historical")
    with fixture.database.session_factory() as session:
        api = fixture.aggregates(session)
        checked = api.validate(body)
        card = api.create(CreateAggregateRequest(**body.model_dump(), name="Formula RCF",
            rule_hash=checked.rule_hash), UUID(int=1), str(uuid4()))
    start = card.anchor_utc + timedelta(seconds=card.next_index * 2)
    fixture.coverage_start = start
    return card, body, start
