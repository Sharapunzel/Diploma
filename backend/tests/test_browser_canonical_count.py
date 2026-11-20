"""Addressed PostgreSQL regressions for the browser canonical readiness RPC."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from app.models import AggregateClosure
from ml import browser_canonical_count as probe
from test_event_time_pipeline import context, create, migrated

__all__ = ["context", "migrated"]


def db_endpoint(context):
    url = context[0].engine.url
    assert url.database == "diploma_test" or url.database.startswith("diploma_test_review_")
    return {"database": url.database, "host": url.host, "port": str(url.port or 5432)}


def read_counts(context, identity, seconds=5):
    return probe.count_closures(os.environ["TEST_DATABASE_URL"], identity,
        time.monotonic() + seconds, **db_endpoint(context))


def closure(card, source, index, *, origin="initial_history", time_basis="event_time",
        uncomputed=False, cleared=False):
    start = card.anchor_utc + timedelta(seconds=index * card.duration_seconds)
    end = start + timedelta(seconds=card.duration_seconds)
    return AggregateClosure(origin_point_id=uuid4(), aggregate_id=card.id,
        schedule_id=card.schedule_id, source_identity=source, window_index=index,
        start_utc=start, end_utc=end, time_basis=time_basis, origin=origin,
        snapshot_reference_at=None if uncomputed else end,
        close_not_before_at=end, closed_at=end, point_kind="working",
        payload_state="cleared" if cleared else "present", cleared_at=end if cleared else None,
        clear_reason="manual" if cleared else None,
        calculation_status="uncomputed" if uncomputed else "computed",
        calculation_reason="execution_budget_exceeded" if uncomputed else None,
        execution={"fixture": True} if uncomputed else None)


def test_count_is_committed_canonical_headers_for_one_aggregate_including_cleared(context):
    db, _, source, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    target, _ = create(context, anchor, 10)
    other, _ = create(context, anchor, 10)
    with db.session_factory.begin() as tx:
        tx.add_all([
            closure(target, source, 0),
            closure(target, source, 1, cleared=True),
            closure(target, source, 519, uncomputed=True),
            closure(target, source, -1),
            closure(target, source, 520),
            closure(target, source, 2, origin="online"),
            closure(target, source, 3, time_basis="availability_at"),
            closure(other, source, 4),
        ])
    with db.session_factory() as tx:
        tx.add(closure(target, source, 4))
        tx.flush()
        result = read_counts(context, target.id)
    assert result["canonical_history_count"] == 3
    assert result["computed_count"] == 2
    assert result["uncomputed_count"] == 1
    assert result["connect_seconds"] >= 0 and result["sql_seconds"] >= 0


@pytest.mark.parametrize("url", [
    "postgresql://fixture:synthetic@postgres:5432/diploma_db",
    "postgresql://fixture:synthetic@other:5432/diploma_test",
    "postgresql://fixture:synthetic@postgres:6543/diploma_test",
    "postgresql://fixture:synthetic@postgres:5432/diploma_test?hostaddr=127.0.0.1",
])
def test_wrong_database_endpoint_is_rejected_before_connect(monkeypatch, url):
    monkeypatch.setattr(psycopg, "connect", lambda **_: pytest.fail("unexpected connect"))
    with pytest.raises(ValueError, match="isolated profile database"):
        probe.count_closures(url, uuid4(), time.monotonic() + 5,
            database="diploma_test", host="postgres", port="5432")


def test_url_file_takes_precedence_and_is_stripped(monkeypatch, tmp_path):
    url = "postgresql://fixture:synthetic@postgres:5432/diploma_test"
    file = tmp_path / "profile-url"
    file.write_text(url + "\n", encoding="utf-8")
    monkeypatch.setenv("DATABASE_URL_FILE", str(file))
    monkeypatch.setenv("DATABASE_URL", "wrong")
    assert probe.database_url() == url


def test_count_transaction_readonly(context, monkeypatch):
    # A mutating statement with no candidate rows is still refused by PostgreSQL.
    monkeypatch.setattr(probe, "COUNT_SQL", """
        WITH written AS (
            DELETE FROM logs.aggregate_closures WHERE aggregate_id = %s AND false RETURNING 1
        ) SELECT count(*), 0, 0 FROM written
    """)
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        read_counts(context, uuid4())


def test_locked_count_cancels_within_its_statement_budget(context):
    db = context[0]
    with db.engine.begin() as connection:
        connection.exec_driver_sql("LOCK TABLE logs.aggregate_closures IN ACCESS EXCLUSIVE MODE")
        with pytest.raises(psycopg.errors.QueryCanceled):
            read_counts(context, uuid4(), seconds=.5)


def cold_count_script(endpoint):
    return (
        "import sys\n"
        "sys.path.insert(0, 'tests')\n"
        "from ml.browser_canonical_count import main\n"
        f"main(**{endpoint!r})\n"
        "assert not any(m.split('.')[0] in {'app', 'sqlalchemy', 'pydantic', 'jpype'} "
        "for m in sys.modules)\n"
    )


def cold_count(source, identity, budget, env):
    started = time.monotonic()
    process = subprocess.run([sys.executable, "-B", "-c", source, str(identity), str(budget)],
        env=env, check=False, capture_output=True, text=True, timeout=15)
    return process, time.monotonic() - started


@pytest.mark.skipif(sys.platform != "linux", reason="Readiness RPC runs in the Linux API container")
def test_cold_lightweight_process_against_postgresql(context, capsys):
    target, _ = create(context, datetime.now(UTC) - timedelta(days=2), 10)
    with context[0].session_factory.begin() as tx:
        tx.add(closure(target, context[2], 0))
    env = {**os.environ, "DATABASE_URL": os.environ["TEST_DATABASE_URL"]}
    env.pop("DATABASE_URL_FILE", None)
    process, elapsed = cold_count(cold_count_script(db_endpoint(context)), target.id, 5, env)
    assert process.returncode == 0, process.returncode
    facts = json.loads(process.stdout)
    assert (facts["canonical_history_count"], facts["computed_count"], facts["uncomputed_count"]) == (1, 1, 0)
    assert 0 <= facts["sql_seconds"] <= elapsed and 0 <= facts["connect_seconds"] <= elapsed
    with capsys.disabled():
        print("Canonical cold Linux process: " + json.dumps({"elapsed_seconds": elapsed,
            "connect_seconds": facts["connect_seconds"], "sql_seconds": facts["sql_seconds"],
            "python": sys.version.split()[0], "psycopg": psycopg.__version__,
            "orm_app_ml_imports": False}), flush=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Readiness RPC runs in the Linux API container")
def test_remote_process_timer_closes_a_blocked_libpq_socket():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(.5)
        port = str(listener.getsockname()[1])
        endpoint = {"database": "diploma_test", "host": "127.0.0.1", "port": port}
        env = {**os.environ, "DATABASE_URL": f"postgresql://fixture@127.0.0.1:{port}/diploma_test"}
        env.pop("DATABASE_URL_FILE", None)
        process, _ = cold_count(cold_count_script(endpoint), uuid4(), 1, env)
        assert process.returncode == -signal.SIGALRM and not process.stdout
        connected, _ = listener.accept()
        with connected:
            connected.settimeout(.5)
            assert connected.recv(1024), "libpq never reached the blocked handshake"
            assert connected.recv(1024) == b""
