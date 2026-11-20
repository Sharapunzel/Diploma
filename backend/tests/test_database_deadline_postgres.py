"""Actual driver/pool release, pre_ping cancellation and bounded reconnect."""

from time import monotonic, sleep

import pytest
from app.aggregation.event_time import OperationBudget
from app.db_deadline import DatabaseDeadlineExceeded, database_deadline
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from test_event_time_pipeline import context, migrated

__all__ = ["context", "migrated"]


@pytest.fixture
def finalization():
    budgets = []

    def provider():
        if not budgets:
            budgets.append(OperationBudget(5))
        return budgets[0]

    return budgets, provider


def test_actual_busy_pool_releases_waiter_without_changing_capacity(context, record_property, finalization):
    database, _, _, _ = context
    pool = database.engine.pool
    held = [database.engine.connect() for _ in range(15)]
    started = monotonic()
    budget = OperationBudget(.05)
    try:
        with database_deadline(lambda: budget, on_deadline=finalization[1]), pytest.raises(DBAPIError) as failure:
            database.engine.connect()
        assert isinstance(failure.value.orig, DatabaseDeadlineExceeded)
        elapsed = monotonic() - started
        assert .04 <= elapsed < .8
        assert pool.size() == 5 and pool.timeout() == 30
        assert pool.checkedout() == 15 and pool.overflow() == 10
        record_property("busy_pool_release_seconds", elapsed)
    finally:
        for connection in held:
            connection.close()
    assert pool.checkedout() == 0 and pool.overflow() == 0
    with database.engine.connect() as connection:
        assert connection.scalar(text("SELECT 1")) == 1


def test_actual_pre_ping_obeys_original_deadline_and_discards_connection(context, record_property, finalization):
    database, _, _, _ = context
    engine, pool = database.engine, database.engine.pool
    with engine.connect() as connection:
        old_pid = connection.scalar(text("SELECT pg_backend_pid()"))
    original_ping = engine.dialect._dialect_specific_select_one
    engine.dialect._dialect_specific_select_one = "SELECT pg_sleep(10)"
    budget = OperationBudget(.08)
    started = monotonic()
    connections = []

    def connected(_connection, _record):
        connections.append(True)

    event.listen(pool, "connect", connected)
    try:
        with database_deadline(lambda: budget, on_deadline=finalization[1]), pytest.raises(DBAPIError) as failure:
            engine.connect()
        assert isinstance(failure.value.orig, DatabaseDeadlineExceeded)
        elapsed = monotonic() - started
        assert .06 <= elapsed < .8
        assert pool.checkedout() == 0
        assert connections == [], "expired pre_ping must not reconnect using finalization time"
        record_property("pre_ping_release_seconds", elapsed)
    finally:
        event.remove(pool, "connect", connected)
        engine.dialect._dialect_specific_select_one = original_ping
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT pg_backend_pid()")) != old_pid
        assert connection.scalar(text("SELECT 1")) == 1
        assert len(finalization[0]) == 1
        service_budget = finalization[0][0]
        service_deadline = service_budget.deadline
        while True:
            connection.exec_driver_sql("SET LOCAL statement_timeout = '100ms'")
            present = connection.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE pid=:pid"), {"pid": old_pid})
            connection.rollback()
            if not present or monotonic() >= service_deadline:
                break
            sleep(min(.05, max(0, service_deadline - monotonic())))
        record_property("pre_ping_server_observed_check_seconds", monotonic() - started)
        assert not present, "pre_ping backend still exists after the shared finalization bound"
        assert finalization[1]() is service_budget and service_budget.deadline == service_deadline
    assert pool.checkedout() == 0


def test_actual_stale_connection_reconnect_keeps_one_deadline(context, record_property, finalization):
    database, _, _, _ = context
    engine, pool = database.engine, database.engine.pool
    with engine.connect() as control:
        with engine.connect() as stale:
            stale_pid = stale.scalar(text("SELECT pg_backend_pid()"))
        # Keep the control checked out: the next checkout really encounters
        # the returned, terminated connection before reconnecting.
        assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": stale_pid})
        budget = OperationBudget(.8)
        original_deadline = budget.deadline
        started = monotonic()
        with database_deadline(lambda: budget, on_deadline=finalization[1]), engine.connect() as connection:
            assert connection.scalar(text("SELECT pg_backend_pid()")) != stale_pid
            assert connection.scalar(text("SELECT 1")) == 1
    elapsed = monotonic() - started
    assert budget.deadline == original_deadline and budget.remaining() > 0
    assert elapsed < .8 and pool.checkedout() == 0
    record_property("reconnect_release_seconds", elapsed)
