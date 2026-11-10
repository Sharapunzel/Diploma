"""Real pgoutput commit visibility before the writer leaves SyncRep.

Run via backend-db --pytest tests/test_availability_visibility.py. The profile
owns PostgreSQL: no production settings, slots or user data are involved.
"""
import os
import threading
import time
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
import test_availability_materializer as shared_materializer
from psycopg import sql
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from test_availability_materializer import (
    Feedback,
    persist_then_ack,
)

from app.adapters.pgoutput import InvalidReplicationMessage, PgOutputDecoder
from app.adapters.pgoutput_stream import ReplicationStream
from app.repositories.sqlalchemy import availability

sessions = shared_materializer.sessions


@pytest.fixture
def materialization_sessions(sessions):
    # Delay only the source writer. The materializer's commit has no remote
    # standby in this isolated test and must not join the synthetic SyncRep wait.
    engine = sessions.kw["bind"]
    def local_commit(connection):
        connection.exec_driver_sql("SET LOCAL synchronous_commit=local")
    event.listen(engine, "begin", local_commit)
    try:
        yield sessions
    finally:
        event.remove(engine, "begin", local_commit)


@contextmanager
def committing_writer():
    assert os.environ.get("COMPOSE_PROJECT_NAME", "").startswith("diploma_t21_"), (
        "SyncRep regression requires the isolated backend-db profile")
    url = make_url(os.environ["TEST_DATABASE_URL"])
    assert (url.database == "diploma_test" or url.database.startswith("diploma_test_"))
    assert url.host in {"127.0.0.1", "localhost"}
    dsn = url.set(drivername="postgresql").render_as_string(hide_password=False)
    slot = "visibility_" + uuid4().hex[:16]
    marker, source = uuid4(), uuid4()
    writer = None
    thread = None
    errors = []
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("SET synchronous_commit=local")
        assert not admin.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname='diploma_db')").fetchone()[0]
        admin.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE logs.availability_markers (id,source_identity,kind,event_id) WITH (publish='insert')").format(sql.Identifier(slot)))
        admin.execute("SELECT * FROM pg_create_logical_replication_slot(%s,'pgoutput')", (slot,))
        try:
            admin.execute("ALTER SYSTEM SET synchronous_standby_names='visibility_never_connects'")
            admin.execute("SELECT pg_reload_conf()")
            deadline = time.monotonic() + 5
            while admin.execute("SHOW synchronous_standby_names").fetchone()[0] != "visibility_never_connects":
                assert time.monotonic() < deadline
                time.sleep(.01)
            writer = psycopg.connect(dsn)
            writer.execute("SET LOCAL synchronous_commit=on")
            pid, xid = writer.execute("SELECT pg_backend_pid(),pg_current_xact_id()::text").fetchone()
            writer.execute("INSERT INTO logs.availability_markers(id,source_identity,kind) VALUES (%s,%s,'barrier')", (marker, source))

            def commit():
                try:
                    writer.commit()
                except psycopg.Error as error:
                    errors.append(error)

            thread = threading.Thread(target=commit)
            thread.start()
            deadline = time.monotonic() + 5
            while admin.execute("SELECT wait_event FROM pg_stat_activity WHERE pid=%s", (pid,)).fetchone()[0] != "SyncRep":
                assert time.monotonic() < deadline
                time.sleep(.01)
            decoder = PgOutputDecoder("logs", "availability_markers", ("id", "source_identity", "kind", "event_id"))
            with ReplicationStream(dsn, slot, slot, decoder) as stream:
                decoded = stream.read_commit(5)
                assert decoded is not None and decoded.xid == int(xid)
                assert decoded.rows == ((str(marker), str(source), "barrier", None),)
                assert admin.execute("SELECT id FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone() is None

                def finish():
                    admin.execute("SELECT pg_cancel_backend(%s)", (pid,))
                    thread.join(5)
                    assert not thread.is_alive() and not errors

                yield decoded, marker, admin, finish
        finally:
            admin.execute("ALTER SYSTEM RESET synchronous_standby_names")
            admin.execute("SELECT pg_reload_conf()")
            if thread is not None and thread.is_alive():
                admin.execute("SELECT pg_cancel_backend(%s)", (pid,))
                thread.join(5)
            if writer is not None:
                writer.close()
            admin.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            admin.execute(sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(slot)))


def test_pgoutput_commit_waits_for_sql_visibility_before_durable_feedback(materialization_sessions, caplog):
    sessions = materialization_sessions
    caplog.set_level("INFO", logger=availability.__name__)
    waiting = threading.Event()
    engine = sessions.kw["bind"]

    def observed(_conn, _cursor, statement, _parameters, _context, _many):
        if "FROM pg_locks" in statement:
            waiting.set()

    feedback, errors = Feedback(), []
    event.listen(engine, "after_cursor_execute", observed)
    try:
        with committing_writer() as (decoded, marker, admin, finish):
            def apply():
                try:
                    persist_then_ack(sessions, feedback, decoded)
                except (SQLAlchemyError, InvalidReplicationMessage, RuntimeError) as error:
                    errors.append(error)

            worker = threading.Thread(target=apply)
            worker.start()
            try:
                assert waiting.wait(3), "materializer never observed the still-active writer"
                assert not feedback.sent
                assert admin.execute("SELECT id FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone() is None
                finish()
            finally:
                worker.join(5)
            assert not worker.is_alive() and not errors
            assert feedback.sent == [decoded.end_lsn]
            assert admin.execute("SELECT availability_at FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone()[0] == decoded.committed_at
            assert "availability_commit_visibility_wait" in caplog.text
            assert f"xid={decoded.xid}" in caplog.text and str(marker) in caplog.text
    finally:
        event.remove(engine, "after_cursor_execute", observed)


def test_still_invisible_commit_fails_within_budget_without_feedback(materialization_sessions, monkeypatch):
    sessions = materialization_sessions
    monkeypatch.setattr(availability, "COMMIT_VISIBILITY_SECONDS", .05)
    feedback = Feedback()
    with committing_writer() as (decoded, marker, admin, finish):
        with pytest.raises(InvalidReplicationMessage, match="visibility deadline") as caught:
            persist_then_ack(sessions, feedback, decoded)
        assert not feedback.sent and f"xid={decoded.xid}" in str(caught.value)
        assert admin.execute("SELECT id FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone() is None
        finish()
        assert admin.execute("SELECT availability_at FROM logs.availability_markers WHERE id=%s", (marker,)).fetchone()[0] is None
        persist_then_ack(sessions, feedback, decoded)
        assert feedback.sent == [decoded.end_lsn]
