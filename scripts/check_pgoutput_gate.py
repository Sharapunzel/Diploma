"""TASK-30 pgoutput experiment on the isolated backend-db Compose profile.

Run from the repository root: python -B scripts/check_pgoutput_gate.py
Checks both the protocol experiment and the application decoder.
"""
from __future__ import annotations

import json
import os
import select
import shutil
import struct
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import pq
from psycopg.conninfo import make_conninfo
from sqlalchemy import create_engine, select as sa_select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

import agent_test_profiles as profiles

sys.path.insert(0, str(profiles.ROOT / "backend"))
from app.adapters.pgoutput import PgOutputDecoder  # noqa: E402
from app.adapters.pgoutput_stream import ReplicationStream  # noqa: E402
from app.services.implementations.wal_guard import CaptureGuard  # noqa: E402
from app.adapters.replication_slot import PostgreSQLReplicationSlotAdapter  # noqa: E402
from app.repositories.sqlalchemy.capture import SqlAlchemyCaptureStateRepository  # noqa: E402
from app.repositories.sqlalchemy.availability import SqlAlchemyAvailabilityProgressRepository  # noqa: E402
from app.repositories.sqlalchemy.windows import SqlAlchemyWorkingWindowRepository  # noqa: E402
from app.repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository  # noqa: E402
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork  # noqa: E402
from app.models import ParsedLog, ProcessedKafkaRecord, Source  # noqa: E402
from app.models import AggregateSchedule, AvailabilityMarker  # noqa: E402
from app.repositories.sqlalchemy import processing as processing_repository  # noqa: E402
from app.services.implementations import working_windows  # noqa: E402

SLOT = "task30_gate"
PUBLICATION = "task30_gate_publication"
EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


def capture_status(session, *, min_safe_bytes):
    return CaptureGuard(SqlAlchemyCaptureStateRepository(session),
                        PostgreSQLReplicationSlotAdapter(session),
                        min_safe_bytes=min_safe_bytes).status()


def decode(rows):
    """Decode the restricted proto_version=1 INSERT-only publication used here."""
    result = []
    markers = []
    for _lsn, _xid, raw in rows:
        data = bytes(raw)
        tag = chr(data[0])
        if tag == "B":
            markers = []
        elif tag == "R":
            continue
        elif tag == "I":
            assert data[5:6] == b"N"
            count = struct.unpack_from("!H", data, 6)[0]
            assert count == 2
            offset = 8
            fields = []
            for _ in range(count):
                assert data[offset:offset + 1] == b"t"
                length = struct.unpack_from("!I", data, offset + 1)[0]
                offset += 5
                fields.append(data[offset:offset + length].decode())
                offset += length
            assert offset == len(data)
            markers.append(tuple(fields))
        elif tag == "C":
            assert len(data) == 26 and data[1] == 0
            _, end_lsn, micros = struct.unpack_from("!QQq", data, 2)
            result.append((markers, EPOCH + timedelta(microseconds=micros),
                           f"{end_lsn >> 32:X}/{end_lsn & 0xffffffff:X}"))
        else:
            raise AssertionError(f"Unexpected pgoutput message: {tag}")
    return result


def peek(conn):
    rows = conn.execute("""SELECT lsn, xid, data
        FROM pg_logical_slot_peek_binary_changes(%s, NULL, NULL,
             'proto_version', '1', 'publication_names', %s)""",
                        (SLOT, PUBLICATION)).fetchall()
    return decode(rows)


def stream_without_ack(url):
    """Exercise the application's parser with real replication protocol frames."""
    conninfo = make_conninfo(url, replication="database")
    connection = pq.PGconn.connect(conninfo.encode())
    try:
        if connection.status != pq.ConnStatus.OK:
            raise RuntimeError("Replication connection failed")
        command = (f"START_REPLICATION SLOT {SLOT} LOGICAL 0/0 "
                   f"(proto_version '1', publication_names '{PUBLICATION}')")
        result = connection.exec_(command.encode())
        if result.status != pq.ExecStatus.COPY_BOTH:
            raise RuntimeError(f"START_REPLICATION failed: {result.error_message!r}")
        decoder = PgOutputDecoder("public", "gate_markers", ("id", "kind"))
        commits = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            size, payload = connection.get_copy_data(1)
            if size == 0:
                select.select([connection.socket], [], [], max(0, deadline - time.monotonic()))
                connection.consume_input()
                continue
            if size < 0:
                raise RuntimeError("Replication stream ended unexpectedly")
            frame = bytes(payload)
            if frame[:1] == b"k":
                continue
            if frame[:1] != b"w" or len(frame) < 25:
                raise RuntimeError("Invalid replication frame")
            commit = decoder.feed(frame[25:])
            if commit:
                commits.append(commit)
                if any(row[1] == "barrier" for row in commit.rows):
                    return commits
        raise TimeoutError("Replication stream did not reach the barrier")
    finally:
        connection.finish()


def main():
    run = profiles.ROOT / ".test_runs" / "runs" / f"pgoutput-gate-{uuid4().hex}"
    run.mkdir(parents=True, exist_ok=False)
    env = {**os.environ, "TEMP": str(run), "TMP": str(run), "TMPDIR": str(run),
           "PYTHONDONTWRITEBYTECODE": "1"}
    before = None
    compose = None
    project = None
    worker = None
    try:
        before = profiles.inventory(env)
        (run / "docker-before.json").write_text(json.dumps(before), encoding="utf-8")
        env, compose, project = profiles.prepare(run, "backend-db")
        overlay = run / "logical.json"
        overlay.write_text(json.dumps({"services": {"postgres": {"command": [
            "postgres", "-c", "wal_level=logical", "-c", "track_commit_timestamp=on",
            "-c", "max_slot_wal_keep_size=64MB"]}}}), encoding="utf-8")
        compose += ["-f", str(overlay)]
        profiles.check_config(compose, project, env, run)
        print(f"Profile=backend-db project={project} db={env['POSTGRES_BIND']} run={run}", flush=True)
        profiles.command([*compose, "up", "-d", "--wait", "postgres"], env=env)
        url = env["TEST_DATABASE_URL"].replace("postgresql+psycopg:", "postgresql:")
        with psycopg.connect(url, autocommit=True) as db:
            print(db.execute("SELECT version()").fetchone()[0], flush=True)
            assert db.execute("SHOW wal_level").fetchone()[0] == "logical"
            db.execute("CREATE TABLE gate_markers (id text PRIMARY KEY, kind text NOT NULL)")
            db.execute("CREATE TABLE gate_results (id text PRIMARY KEY, kind text NOT NULL, committed_at timestamptz NOT NULL, end_lsn pg_lsn NOT NULL)")
            db.execute(f"CREATE PUBLICATION {PUBLICATION} FOR TABLE gate_markers WITH (publish='insert')")
            db.execute("SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))
            db.execute("INSERT INTO gate_markers VALUES ('event-before', 'event')")
            db.execute("INSERT INTO gate_markers VALUES ('vacuum-event', 'event')")
            slot = db.execute("""SELECT active, wal_status, restart_lsn,
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                FROM pg_replication_slots WHERE slot_name = %s""", (SLOT,)).fetchone()
            assert not slot[0] and slot[1] in ("reserved", "extended") and slot[3] > 0
            print(f"Stopped slot: active={slot[0]} status={slot[1]} retained_bytes={slot[3]}", flush=True)
            db.execute("VACUUM FREEZE gate_markers")

            # The closure must wait for the transaction holding the source lock.
            boundary = db.execute("SELECT clock_timestamp() + interval '1 second'").fetchone()[0]
            writer = psycopg.connect(url)
            writer.execute("SELECT pg_advisory_xact_lock(30)")
            writer.execute("INSERT INTO gate_markers VALUES ('event-after', 'event')")
            db.execute("SELECT pg_sleep(greatest(0, extract(epoch from %s - clock_timestamp())))",
                       (boundary,))
            started = threading.Event()
            errors = []
            def barrier():
                try:
                    with psycopg.connect(url) as closure:
                        started.set()
                        closure.execute("SELECT pg_advisory_xact_lock(30)")
                        closure.execute("INSERT INTO gate_markers VALUES ('barrier', 'barrier')")
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=barrier)
            thread.start()
            assert started.wait(5)
            # Check actual blocked PostgreSQL session rather than elapsed time.
            for _ in range(200):
                blocked = db.execute("""SELECT count(*) FROM pg_stat_activity
                    WHERE wait_event_type='Lock' AND query LIKE 'SELECT pg_advisory_xact_lock(30)%'""").fetchone()[0]
                if blocked:
                    break
                threading.Event().wait(0.01)
            assert blocked == 1
            writer.commit()
            writer.close()
            thread.join(10)
            assert not thread.is_alive() and not errors
            streamed = stream_without_ack(url)
            txns = peek(db)
            events = [(i, k, timestamp, lsn) for markers, timestamp, lsn in txns for i, k in markers]
            assert [x[0] for x in events] == ["event-before", "vacuum-event", "event-after", "barrier"]
            assert [(row, commit.committed_at) for commit in streamed for row in commit.rows] == [
                ((i, kind), timestamp) for i, kind, timestamp, _ in events
            ]
            timestamps = {i: ts for i, _, ts, _ in events}
            assert timestamps["event-before"] < boundary <= timestamps["event-after"]
            # A decoder crash before materialization leaves the slot untouched.
            assert peek(db) == txns
            # Compare genuine WAL timestamp to PostgreSQL's independent commit metadata.
            real = db.execute("""SELECT id, pg_xact_commit_timestamp(xmin)
                FROM gate_markers ORDER BY id""").fetchall()
            assert {i: ts for i, ts in real} == {i: ts for i, _, ts, _ in events}
            print("Real replication stream and SQL agree; barrier follows blocked writer", flush=True)

            # Commit results, simulate a crash before acknowledging the slot.
            with db.transaction():
                for i, kind, timestamp, lsn in events:
                    db.execute("INSERT INTO gate_results VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                               (i, kind, timestamp, lsn))
            db.execute("CHECKPOINT")
        # Simulate a decoder outage and a database restart after durable results,
        # but before acknowledging the slot.
        profiles.command([*compose, "restart", "postgres"], env=env)
        profiles.command([*compose, "up", "-d", "--wait", "postgres"], env=env)
        with psycopg.connect(url, autocommit=True) as db:
            replay = [(i, k, timestamp, lsn) for markers, timestamp, lsn in peek(db) for i, k in markers]
            assert replay == events
            decoder = PgOutputDecoder("public", "gate_markers", ("id", "kind"))
            with ReplicationStream(url, SLOT, PUBLICATION, decoder) as stream:
                delivered = []
                while len(delivered) < len(events):
                    commit = stream.read_commit(10)
                    assert commit is not None
                    lsn = f"{commit.end_lsn >> 32:X}/{commit.end_lsn & 0xffffffff:X}"
                    with db.transaction():
                        for i, kind in commit.rows:
                            db.execute("INSERT INTO gate_results VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                       (i, kind, commit.committed_at, lsn))
                            delivered.append((i, kind, commit.committed_at, lsn))
                    stream.acknowledge(commit.end_lsn)
                assert delivered == events
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    acknowledged = db.execute("""SELECT confirmed_flush_lsn >= %s::pg_lsn
                        FROM pg_replication_slots WHERE slot_name=%s""", (events[-1][3], SLOT)).fetchone()[0]
                    if acknowledged:
                        break
                    time.sleep(0.01)
                assert acknowledged
            assert db.execute("SELECT count(*) FROM gate_results").fetchone()[0] == len(events)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                active = db.execute("SELECT active FROM pg_replication_slots WHERE slot_name=%s", (SLOT,)).fetchone()[0]
                if not active:
                    break
                time.sleep(0.05)
            assert not active, "reader did not release the replication slot"
            assert not peek(db)
            print(f"Crash before acknowledgement replayed without duplicates; confirmed={events[-1][3]}", flush=True)
            db.execute("CHECKPOINT")
        profiles.command([*compose, "restart", "postgres"], env=env)
        profiles.command([*compose, "up", "-d", "--wait", "postgres"], env=env)
        with psycopg.connect(url, autocommit=True) as db:
            assert not peek(db)
            assert db.execute("SELECT count(*) FROM gate_results").fetchone()[0] == 4
            print("Acknowledged position and four results survive restart", flush=True)
            db.execute("INSERT INTO gate_markers VALUES ('unread-before-loss', 'event')")
            # Exercise the configured finite WAL budget in this disposable cluster.
            # Switch real WAL segments while the reader is offline, then checkpoint.
            for _ in range(8):
                db.execute("SELECT pg_switch_wal()")
                db.execute("CREATE TABLE IF NOT EXISTS gate_wal_pressure (payload text)")
                db.execute("INSERT INTO gate_wal_pressure VALUES (repeat('x', 4000))")
            db.execute("CHECKPOINT")
            status = db.execute("""SELECT wal_status, safe_wal_size
                FROM pg_replication_slots WHERE slot_name = %s""", (SLOT,)).fetchone()
            print(f"Slot after WAL pressure: status={status[0]} safe_wal_size={status[1]}", flush=True)
            assert status[0] == "lost", "Expected a bounded slot to be invalidated"
            try:
                peek(db)
            except psycopg.Error as exc:
                print(f"Missing WAL detected: SQLSTATE={exc.sqlstate}; unread marker remains unclassified", flush=True)
            else:
                raise AssertionError("A lost slot must not allow classification")
            # Exercise the actual bootstrap, restricted role and decoder on the
            # same isolated PostgreSQL, after migrating the project's schema.
            app_env = {**env, "DATABASE_URL": env["TEST_DATABASE_URL"],
                       "WAL_READER_PASSWORD": Path(env["WAL_READER_PASSWORD_FILE"]).read_text(encoding="utf-8").strip()}
            app_env["WAL_DATABASE_URL"] = make_url(env["TEST_DATABASE_URL"]).set(
                username="diploma_wal_reader", password=app_env["WAL_READER_PASSWORD"]
            ).render_as_string(hide_password=False)
            profiles.command([sys.executable, "-B", "-m", "alembic", "upgrade", "head"],
                             env=app_env, cwd=profiles.ROOT / "backend")
            profiles.command([sys.executable, "-B", "-m", "app.wal_bootstrap"],
                             env=app_env, cwd=profiles.ROOT / "backend")
            first_boundary = db.execute("SELECT legacy_cutover_at,availability_verified_from_at,boundary_origin FROM logs.replication_progress WHERE id=1").fetchone()
            assert first_boundary[0] is not None and first_boundary[0] == first_boundary[1]
            assert first_boundary[2] == "first_bootstrap"
            with psycopg.connect(url) as old_writer:
                old_writer.execute("LOCK TABLE logs.parsed_logs IN ROW EXCLUSIVE MODE")
                refused_writer = subprocess.run([sys.executable, "-B", "-m", "app.wal_bootstrap"],
                    env=app_env, cwd=profiles.ROOT / "backend", capture_output=True, text=True, timeout=20)
                assert refused_writer.returncode != 0 and "writers_not_stopped" in refused_writer.stderr
                old_writer.rollback()
            profiles.command([sys.executable, "-B", "-m", "app.wal_bootstrap"], env=app_env, cwd=profiles.ROOT / "backend")
            assert db.execute("SELECT legacy_cutover_at,availability_verified_from_at,boundary_origin FROM logs.replication_progress WHERE id=1").fetchone() == first_boundary
            # Reproduce an existing TASK-30 database: keep its slot/progress,
            # remove only the newly introduced schema, then apply it again.
            profiles.command([sys.executable, "-B", "-m", "alembic", "downgrade", "0015_aggregation_foundation"], env=app_env, cwd=profiles.ROOT / "backend")
            profiles.command([sys.executable, "-B", "-m", "alembic", "upgrade", "head"], env=app_env, cwd=profiles.ROOT / "backend")
            profiles.command([sys.executable, "-B", "-m", "app.wal_bootstrap"], env=app_env, cwd=profiles.ROOT / "backend")
            upgrade_boundary = db.execute("SELECT legacy_cutover_at,availability_verified_from_at,boundary_barrier_id FROM logs.replication_progress WHERE id=1").fetchone()
            assert upgrade_boundary[:2] == (None, None) and upgrade_boundary[2] is not None
            worker_log = run / "decoder.log"
            with worker_log.open("w", encoding="utf-8") as log:
                worker = subprocess.Popen([sys.executable, "-B", "-m", "app.wal_decoder"],
                                          env=app_env, cwd=profiles.ROOT / "backend",
                                          stdout=log, stderr=log)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if db.execute("SELECT active FROM pg_replication_slots WHERE slot_name='diploma_availability'").fetchone()[0]:
                    break
                if worker.poll() is not None:
                    raise RuntimeError("application decoder failed to start")
                time.sleep(0.05)
            else:
                raise AssertionError("application decoder did not acquire the slot")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                upgraded = db.execute("SELECT legacy_cutover_at,availability_verified_from_at FROM logs.replication_progress WHERE id=1").fetchone()
                if upgraded[1] is not None:
                    break
                time.sleep(.05)
            marker_boundary = db.execute("SELECT created_at,availability_at FROM logs.availability_markers WHERE id=%s", (upgrade_boundary[2],)).fetchone()
            assert upgraded[0] is None and upgraded[1] == marker_boundary[1] >= marker_boundary[0]
            print("Capture boundaries: first bootstrap immutable; legacy upgrade proves only a future barrier boundary", flush=True)
            worker.terminate()
            worker.wait(timeout=10)
            worker = None
            marker, source, event = uuid4(), uuid4(), uuid4()
            db.execute("""INSERT INTO logs.availability_markers (id, source_identity, kind, event_id)
                VALUES (%s,%s,'event',%s)""", (marker, source, event))
            expected = db.execute("SELECT pg_xact_commit_timestamp(xmin) FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone()[0]
            db.execute("VACUUM FREEZE logs.availability_markers")
            decoder = PgOutputDecoder("logs", "availability_markers", ("id", "source_identity", "kind", "event_id"))
            reader_url = make_url(app_env["WAL_DATABASE_URL"]).set(drivername="postgresql").render_as_string(hide_password=False)
            with ReplicationStream(reader_url, "diploma_availability", "diploma_availability", decoder) as stream:
                deadline = time.monotonic() + 15
                decoded = None
                while time.monotonic() < deadline:
                    commit = stream.read_commit(1)
                    if commit and any(row[0] == str(marker) for row in commit.rows):
                        decoded = commit
                        break
                assert decoded is not None and decoded.committed_at == expected
            with worker_log.open("a", encoding="utf-8") as log:
                worker = subprocess.Popen([sys.executable, "-B", "-m", "app.wal_decoder"],
                                          env=app_env, cwd=profiles.ROOT / "backend",
                                          stdout=log, stderr=log)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if worker.poll() is not None:
                    log_text = worker_log.read_text(encoding="utf-8").replace(app_env["WAL_READER_PASSWORD"], "[REDACTED]")
                    raise RuntimeError(f"WAL decoder terminated: {worker.returncode}; {log_text}")
                actual = db.execute("SELECT availability_at FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone()[0]
                if actual is not None:
                    break
                time.sleep(0.05)
            assert actual == expected and actual is not None
            progress = db.execute("SELECT durable_lsn FROM logs.replication_progress WHERE id=1").fetchone()[0]
            assert progress >= decoded.end_lsn
            replay_engine = create_engine(app_env["WAL_DATABASE_URL"])
            try:
                with Session(replay_engine) as session:
                    SqlAlchemyAvailabilityProgressRepository(session).apply_decoded_commit(decoded)
                    session.commit()
            finally:
                replay_engine.dispose()
            assert db.execute("SELECT count(*) FROM logs.availability_markers WHERE id=%s AND availability_at=%s", (marker, expected)).fetchone()[0] == 1
            assert db.execute("SELECT durable_lsn FROM logs.replication_progress WHERE id=1").fetchone()[0] >= progress
            print("Application decoder recovered actual commit after VACUUM; replay was idempotent", flush=True)
            guard_engine = create_engine(app_env["WAL_DATABASE_URL"])
            try:
                with Session(guard_engine) as session:
                    status = capture_status(session, min_safe_bytes=8 * 1024 * 1024)
                    assert status.ready, status.reason
                    assert not capture_status(session, min_safe_bytes=128 * 1024 * 1024).ready
                print("Capture guard recognizes ready and exhausted-budget states", flush=True)
            finally:
                guard_engine.dispose()
            reader_dsn = make_url(app_env["WAL_DATABASE_URL"]).set(drivername="postgresql").render_as_string(hide_password=False)
            with psycopg.connect(reader_dsn, autocommit=True) as reader:
                try:
                    reader.execute("SELECT raw FROM logs.parsed_logs LIMIT 1")
                except psycopg.errors.InsufficientPrivilege:
                    pass
                else:
                    raise AssertionError("WAL reader must not read raw events")
            print("WAL reader cannot read raw events", flush=True)
            # Use the real processing repository with a threshold scaled to this
            # test's 64 MiB WAL budget; production uses the fixed 256 MiB margin.
            repository_engine = create_engine(app_env["DATABASE_URL"])
            sessions = sessionmaker(bind=repository_engine, expire_on_commit=False)
            connection_id, source_id = uuid4(), uuid4()
            db.execute("""INSERT INTO app.kafka_connections (id,name,bootstrap_servers)
                VALUES (%s,%s,ARRAY['localhost:9092'])""", (connection_id, str(connection_id)))
            db.execute("""INSERT INTO app.sources (id,name,connection_id,topic_name,is_enabled)
                VALUES (%s,%s,%s,%s,true)""", (source_id, str(source_id), connection_id, str(source_id)))
            def event_pair(offset):
                now = datetime.now(UTC)
                record_id = uuid4()
                record = ProcessedKafkaRecord(
                    id=record_id, connection_id=connection_id, connection_identity=connection_id,
                    kafka_topic_identity="synthetic", source_id=source_id, kafka_topic=str(source_id),
                    kafka_partition=0, kafka_offset=offset, result_status="complete",
                    backend_received_at=now, backend_processed_at=now)
                entry = ParsedLog(
                    source_id=source_id, connection_id=connection_id, normalizer_version=1,
                    normalizer_name="synthetic", source_name="synthetic", connection_name="synthetic",
                    kafka_topic=str(source_id), kafka_partition=0, kafka_offset=offset,
                    deduplication_key=str(record_id), fluent_bit_collected_at=now,
                    backend_received_at=now, backend_processed_at=now, raw="synthetic",
                    ecs_data={"@timestamp": "2000-01-01T00:00:00Z"})
                return record, entry
            record, entry = event_pair(1)
            _, old_entry = event_pair(0)
            try:
                with sessions.begin() as session:
                    session.add(old_entry)
            except IntegrityError as error:
                assert error.orig.sqlstate == "23514"
            else:
                raise AssertionError("markerless old writer was accepted after capture bootstrap")
            print("Active writer blocks bootstrap; markerless old writer is rejected at commit", flush=True)
            with sessions.begin() as session:
                assert capture_status(session, min_safe_bytes=8 * 1024 * 1024).ready
                assert processing_repository.SqlAlchemyProcessingRepository(session).store_event(record, entry)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                actual, expected = db.execute("""SELECT m.availability_at, pg_xact_commit_timestamp(p.xmin)
                    FROM logs.availability_markers m JOIN logs.parsed_logs p ON p.id=m.event_id
                    WHERE p.id=%s""", (entry.id,)).fetchone()
                if actual is not None:
                    break
                time.sleep(0.05)
            assert actual is not None and actual == expected
            print("Processing repository saved event and its actual commit marker atomically", flush=True)
            # Exercise the application barrier with a writer that starts before
            # the scheduled end and commits after it while holding the source lock.
            schedule_id = uuid4()
            anchor = actual - timedelta(seconds=1)
            boundary = anchor + timedelta(seconds=5)
            with sessions.begin() as session:
                session.add(AggregateSchedule(id=schedule_id, source_identity=source_id,
                                              anchor_utc=anchor, duration_seconds=5))
            def window_service(session):
                from types import SimpleNamespace
                from app.dependencies import build_aggregate_service
                from app.ecs import PackagedEcsCatalog
                aggregate_service = build_aggregate_service(session, PackagedEcsCatalog.load())
                rule = {"features": [{"key": "n", "name": "Count", "op": "count"}]}
                return working_windows.WorkingWindowService(
                    SqlAlchemyWorkingWindowRepository(session),
                    SqlAlchemyCoverageEvidenceRepository(session),
                    CaptureGuard(SqlAlchemyCaptureStateRepository(session),
                                 PostgreSQLReplicationSlotAdapter(session),
                                 min_safe_bytes=8 * 1024 * 1024),
                    SqlAlchemyUnitOfWork(session),
                    calculator=lambda stored, window: aggregate_service._working_calculation(
                        SimpleNamespace(rule=rule), stored, window),
                )
            writer = sessions()
            writer.begin()
            barrier_result = []
            barrier_error = []
            waiter = None
            try:
                writer.scalar(sa_select(Source).where(Source.id == source_id).with_for_update(read=True))
                late_record, late_entry = event_pair(2)
                assert capture_status(writer, min_safe_bytes=8 * 1024 * 1024).ready
                assert processing_repository.SqlAlchemyProcessingRepository(writer).store_event(late_record, late_entry)
                assert db.execute("SELECT clock_timestamp() < %s", (boundary,)).fetchone()[0]
                db.execute("SELECT pg_sleep(greatest(0, extract(epoch from %s - clock_timestamp())))", (boundary,))
                def close_barrier():
                    try:
                        with sessions() as session:
                            barrier_result.append(window_service(session).prepare_barrier(schedule_id))
                    except Exception as exc:
                        barrier_error.append(exc)
                waiter = threading.Thread(target=close_barrier)
                waiter.start()
                blocked = False
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    blocked = db.execute("""SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                        WHERE wait_event_type='Lock' AND query LIKE 'SELECT app.sources.%FOR UPDATE%')""").fetchone()[0]
                    if blocked:
                        break
                    time.sleep(0.02)
                assert blocked, "Application barrier did not wait for source writer"
                writer.commit()
            finally:
                writer.close()
                if waiter is not None:
                    waiter.join(10)
            assert not waiter.is_alive() and not barrier_error and barrier_result
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                barrier_time = db.execute("SELECT availability_at FROM logs.availability_markers WHERE id=%s", (barrier_result[0],)).fetchone()[0]
                if barrier_time is not None:
                    break
                time.sleep(0.05)
            assert barrier_time is not None
            with sessions() as session:
                point = window_service(session).close_next_window(schedule_id)
                assert point.event_count == 1 and point.completeness == "unknown"
                assert point.features == [{"key": "n", "value": 1, "reason": None, "valid_count": 1, "invalid_count": 0}]
                assert point.event_time_min == point.event_time_max == datetime(2000, 1, 1, tzinfo=UTC)
            db.execute("SELECT pg_sleep(greatest(0, extract(epoch from %s - clock_timestamp())))", (boundary + timedelta(seconds=5),))
            with sessions() as session:
                second_id = window_service(session).prepare_barrier(schedule_id)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                second_time = db.execute("SELECT availability_at FROM logs.availability_markers WHERE id=%s", (second_id,)).fetchone()[0]
                if second_time is not None:
                    break
                time.sleep(0.05)
            assert second_time is not None
            with sessions() as session:
                point = window_service(session).close_next_window(schedule_id)
                assert point.event_count == 1 and point.completeness == "unknown"
                assert point.features[0]["value"] == 1
                assert point.event_time_min == point.event_time_max == datetime(2000, 1, 1, tzinfo=UTC)
            print("Application barrier waited for writer; before/after commits landed in successive windows", flush=True)
            worker.terminate()
            worker.wait(timeout=10)
            worker = None
            db.execute("SELECT pg_drop_replication_slot('diploma_availability')")
            record, entry = event_pair(3)
            with sessions.begin() as session:
                assert not capture_status(session, min_safe_bytes=8 * 1024 * 1024).ready
            assert db.execute("SELECT count(*) FROM logs.processed_kafka_records WHERE id=%s", (record.id,)).fetchone()[0] == 0
            repository_engine.dispose()
            print("Missing slot is rejected by the capture guard", flush=True)
            lost_bootstrap = subprocess.run([sys.executable, "-B", "-m", "app.wal_bootstrap"],
                                            env=app_env, cwd=profiles.ROOT / "backend",
                                            capture_output=True, text=True, timeout=20)
            assert lost_bootstrap.returncode != 0 and "slot missing after capture began" in lost_bootstrap.stderr
            assert db.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name='diploma_availability'").fetchone()[0] == 0
            print("Application bootstrap refuses to recreate a lost slot", flush=True)
    finally:
        if worker is not None:
            worker.terminate()
            worker.wait(timeout=10)
        if before is not None:
            if compose and project:
                profiles.command([*compose, "down", "--volumes", "--remove-orphans"], env=env)
            profiles.remove_new_images(before, env, project or "", "backend-db")
            after = profiles.inventory(env)
            (run / "docker-after.json").write_text(json.dumps(after), encoding="utf-8")
            profiles.compare_inventory(before, after, project or "unassigned")
            print("Docker inventory unchanged", flush=True)
        target = run.resolve()
        root = (profiles.ROOT / ".test_runs" / "runs").resolve()
        if target == root or not target.is_relative_to(root):
            raise RuntimeError(f"Unexpected cleanup path: {target}")
        shutil.rmtree(target)
        print("Temporary run directory removed", flush=True)


if __name__ == "__main__":
    main()
