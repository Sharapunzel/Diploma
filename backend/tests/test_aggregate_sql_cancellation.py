"""Real PostgreSQL cancellation returns the connection before service completion.

Run with the standard backend-db profile selecting this module. No production
database or manufactured aggregate point is used.
"""

import time

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from test_event_time_pipeline import context, migrated

from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import OperationBudget

__all__ = ["context", "migrated"]


def test_remaining_budget_cancels_real_sql_and_returns_a_reusable_connection(context, record_property):
    database, catalog, _, _ = context
    checked_out = database.engine.pool.checkedout()
    budget = OperationBudget(0.8)
    began = time.monotonic()
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        api.repository.read_budget(budget.milliseconds())
        with pytest.raises(DBAPIError) as failure:
            session.execute(select(func.pg_sleep(10)))
        assert getattr(failure.value.orig, "sqlstate", None) == "57014"
        cancelled = time.monotonic()
        session.rollback()
        assert session.scalar(select(1)) == 1
        session.rollback()
    released = time.monotonic()
    assert released - began < budget.seconds + 5
    assert database.engine.pool.checkedout() == checked_out
    record_property("sql_cancel_seconds", cancelled - began)
    record_property("connection_release_seconds", released - began)
