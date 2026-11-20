"""Pure policy and strict terminal transport contracts, without a database."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
from app.aggregation.rules import Limits
from app.aggregation.time_policy import (
    RecoveryTimeError,
    execution_diagnostics,
    execution_policy,
    recovery_remaining,
)
from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
from app.schemas.aggregates import PointCard
from app.services.implementations.aggregates import AggregateService


@pytest.mark.parametrize("duration,seconds", [(1,.8),(3,2.4),(5,4),(6,4.8),(7,5.6),(14,11.2),(19,15.2),(20,16),(37,29.6),(38,30),(100,30),(300,30),(604800,30)])
def test_window_policy_preserves_fractional_duration_budget(duration, seconds):
    assert execution_policy(duration) == {"policy_version": "window_execution_v1",
        "soft_latency_seconds": 5, "hard_budget_seconds": seconds}


def test_budget_uses_original_start_before_read_and_never_grants_new_duration():
    clock = [101.25]
    budget = OperationBudget(4, clock=lambda: clock[0], started=100)
    assert budget.milliseconds() == 2750
    clock[0] = 104
    with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
        budget.check()
    assert budget.remaining() == 0


def point_document(status="uncomputed"):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    attempt = SimpleNamespace(attempt_id=UUID(int=5), origin="online", window_index=0,
        start_utc=start, end_utc=start+timedelta(seconds=5), started_at=start+timedelta(seconds=10),
        deadline_at=start+timedelta(seconds=14), hard_budget_seconds=4)
    execution = execution_diagnostics(attempt, start+timedelta(seconds=15), 5, status,
        "finalization" if status == "uncomputed" else "before_commit", {"compile": .2, "facts": .1, "total": 3})
    return {"id": str(UUID(int=1)), "aggregate_id": str(UUID(int=2)), "kind": "working",
        "start_utc": attempt.start_utc, "end_utc": attempt.end_utc, "scheduled_at": attempt.end_utc,
        "actual_started_at": attempt.started_at, "actual_completed_at": start+timedelta(seconds=15),
        "time_basis": "event_time", "origin": "online", "window_index": 0,
        "close_not_before_at": attempt.started_at, "payload_state": "present",
        "calculation_status": status, "calculation_reason": execution["calculation_reason"],
        "execution": execution, **{name: None for name in ("event_count", "event_time_min", "event_time_max",
            "features", "completeness", "completeness_reason", "evidence_refs", "snapshot_reference_at", "snapshot_metadata")}}


def test_terminal_point_is_diagnostic_and_never_empty_success_or_fabricated_snapshot():
    point = TypeAdapter(PointCard).validate_python(point_document())
    assert point.calculation_status == "uncomputed"
    assert point.model_dump(mode="json")["execution"]["timings"] == {"snapshot": 3}
    for name, wrong in [("event_count",0),("features",[]),("completeness","complete"),
            ("evidence_refs",{}),("snapshot_metadata",{}),("snapshot_reference_at",point.start_utc),
            ("execution",None),("payload_state","cleared")]:
        with pytest.raises(ValidationError):
            TypeAdapter(PointCard).validate_python({**point_document(), name: wrong})


def test_computed_point_keeps_required_old_fields_and_old_measurements_unknown():
    document = {**point_document("computed"), "event_count": 0, "features": [], "completeness": "complete",
        "completeness_reason": "received_event_snapshot", "evidence_refs": {}, "snapshot_metadata": {},
        "snapshot_reference_at": datetime(2026, 1, 2, tzinfo=UTC), "execution": None}
    assert TypeAdapter(PointCard).validate_python(document).execution is None
    for name in ("event_count", "features", "completeness", "evidence_refs"):
        missing = {key: value for key, value in document.items() if key != name}
        with pytest.raises(ValidationError):
            TypeAdapter(PointCard).validate_python(missing)


def test_read_budget_rechecks_checkout_before_setting_or_starting_subject_sql(monkeypatch):
    entered, release, clock, statements = Event(), Event(), [100.0], []
    def connection():
        entered.set()
        assert release.wait(1)
    session = SimpleNamespace(no_autoflush=nullcontext(), connection=connection, execute=statements.append)
    repository = SqlAlchemyAggregateRepository(session)
    monkeypatch.setattr("app.repositories.sqlalchemy.aggregates.monotonic", lambda: clock[0])
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(repository.read_budget, 800)
        try:
            assert entered.wait(1)
            assert not future.done()  # budget is cooperative; it cannot cancel checkout.
            clock[0] = 100.81
        finally:
            release.set()
        with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
            future.result(timeout=1)
    assert statements == []


def test_read_budget_sets_both_local_timeouts_to_checkout_remainder(monkeypatch):
    clock, statements = [100.0], []
    def connection():
        clock[0] += .25
    repository = SqlAlchemyAggregateRepository(SimpleNamespace(
        no_autoflush=nullcontext(), connection=connection, execute=statements.append))
    monkeypatch.setattr("app.repositories.sqlalchemy.aggregates.monotonic", lambda: clock[0])
    repository.read_budget(1000)
    assert len(statements) == 1
    parameters = list(statements[0].compile().params.values())
    assert parameters.count("750") == 2 and parameters.count(True) == 2


def test_recovery_never_extends_original_budget_for_a_backward_utc_anchor():
    start = datetime(2026,1,1,tzinfo=UTC)
    assert recovery_remaining(start,start+timedelta(seconds=30),30,start+timedelta(seconds=10)) == 20
    assert recovery_remaining(start,start+timedelta(seconds=30),30,start+timedelta(seconds=31)) == 0
    with pytest.raises(RecoveryTimeError):
        recovery_remaining(start,start+timedelta(seconds=30),30,start-timedelta(seconds=.001))


def test_worker_error_record_does_not_allocate_new_time_or_begin_sql_when_remainder_is_zero():
    rollbacks = []
    service = object.__new__(AggregateService)
    service.current_operation_budget = OperationBudget(1, clock=lambda: 2, started=0)
    service.current_execution_generation = 1
    service.budget_limits = Limits()
    service.uow = SimpleNamespace(rollback=lambda: rollbacks.append(1))
    service._execution_service = lambda: pytest.fail("metadata must not begin another SQL operation")
    service.record_worker_error(UUID(int=1), "synthetic")
    assert rollbacks
