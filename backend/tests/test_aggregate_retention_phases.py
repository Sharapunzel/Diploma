"""Host-only causal proofs for the membership/commit phase boundary."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy.exc import DBAPIError

from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
from app.aggregation.retention import POLICY_VERSION, MembershipCompaction
from app.services.implementations import aggregate_retention as retention_service


class PendingRepository:
    def __init__(self, rows, clock, failure=None):
        self.rows, self.clock, self.failure = rows, clock, failure
        self.calls = []
        self.aggregate = SimpleNamespace(id=UUID(int=1), schedule_id=UUID(int=2),
            state="active", execution_mode="current", semantic_basis="event_time")
        self.plan = SimpleNamespace(paused=False, active_cleanup_id=None, execution_generation=1)
        self.state = SimpleNamespace(policy_version=POLICY_VERSION, pending_closure_id=UUID(int=3),
            pending_phase="memberships", pending_cursor=None, authorization_txid=None,
            status="compacting", reason=None, next_check_at=datetime.now(UTC), retry_count=0)
        self.durable = deepcopy(self.state), rows
        self.commits, self.rollbacks, self.fail_commit = 0, 0, None

    def maintenance_budget(self, budget):
        budget.check()

    def get(self, _identity):
        return self.aggregate

    def schedule(self, _identity, _lock):
        return self.plan

    def refresh(self, _aggregate):
        pass

    def retention_state(self, _identity, budget, _lock=False):
        budget.check()
        return self.state

    def now(self):
        return datetime(2026, 11, 18, tzinfo=UTC)

    def retention_pending_closure(self, _aggregate, point, budget):
        budget.check()
        return SimpleNamespace(origin_point_id=point, payload_state="cleared")

    def retention_authorize(self, state, phase, budget):
        budget.check()
        self.calls.append(("authorize", phase))
        state.authorization_txid = "current-transaction"

    def compact_memberships(self, _point, budget, automatic=False):
        assert automatic
        budget.check()
        self.calls.append(("memberships", self.rows))
        if self.failure == "mandatory":
            raise DBAPIError(None, None, RuntimeError("membership SQL failed"))
        changed = min(5000, self.rows)
        self.rows -= changed
        self.clock[0] += .84
        return MembershipCompaction(UUID(int=5000) if changed == 5000 else None, changed)

    def compact_outcomes(self, _point, budget, automatic=False):
        assert automatic
        self.calls.append(("outcomes", self.state.pending_phase))
        if self.failure == "next-phase":
            self.clock[0] += .02
        budget.check()

    @contextmanager
    def read_snapshot(self):
        try:
            yield
        finally:
            self.rollback()

    def rollback(self):
        self.rollbacks += 1
        self.state, self.rows = deepcopy(self.durable)

    def commit(self):
        if self.fail_commit == "before":
            self.fail_commit = None
            raise DBAPIError(None, None, RuntimeError("unconfirmed commit"))
        self.commits += 1
        self.durable = deepcopy(self.state), self.rows
        if self.fail_commit == "after":
            self.fail_commit = None
            raise DBAPIError(None, None, RuntimeError("lost commit ACK"))


def pending_service(monkeypatch, rows, failure=None):
    clock = [0.0]
    monkeypatch.setattr(retention_service, "OperationBudget",
        lambda seconds: OperationBudget(seconds, clock=lambda: clock[0]))
    repo = PendingRepository(rows, clock, failure)
    return repo, retention_service.AggregateRetentionService(repo, repo)


def test_completed_short_membership_commits_before_optional_phase_spends_remainder(monkeypatch):
    repo, service = pending_service(monkeypatch, 1, "next-phase")
    result = service.maintain(repo.aggregate.id)
    assert result["status"] == "compacting" and result["pending"]
    assert repo.rows == repo.durable[1] == 0
    assert repo.state.pending_phase == "outcomes" and repo.state.pending_cursor is None
    assert repo.state.authorization_txid is None and repo.commits == 1
    assert not any(call[0] == "outcomes" for call in repo.calls)
    # The same legal remaining time cannot fund the next optional phase. The
    # call would exhaust .85s and must be isolated from the committed portion.
    with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
        repo.compact_outcomes(UUID(int=3), OperationBudget(.85, clock=lambda: repo.clock[0], started=0), True)
    assert repo.durable[0].pending_phase == "outcomes" and repo.durable[1] == 0


def test_empty_membership_keeps_immediate_outcomes_and_one_commit(monkeypatch):
    repo, service = pending_service(monkeypatch, 0)
    result = service.maintain(repo.aggregate.id)
    assert result["status"] == "scheduled" and not result["pending"]
    assert repo.state.pending_closure_id is repo.state.pending_phase is None
    assert repo.commits == 1
    assert [call[0] for call in repo.calls].count("outcomes") == 1


def test_full_portion_keeps_membership_cursor_and_never_starts_outcomes(monkeypatch):
    repo, service = pending_service(monkeypatch, 5001)
    assert service.maintain(repo.aggregate.id)["pending"]
    assert repo.rows == 1 and repo.state.pending_phase == "memberships"
    assert repo.state.pending_cursor == UUID(int=5000)
    assert repo.commits == 1 and not any(call[0] == "outcomes" for call in repo.calls)


@pytest.mark.parametrize("failure,remaining,phase", [
    ("before", 1, "memberships"), ("after", 0, "outcomes")])
def test_uncertain_commit_reloads_durable_membership_phase(monkeypatch, failure, remaining, phase):
    repo, service = pending_service(monkeypatch, 1)
    repo.fail_commit = failure
    result = service.maintain(repo.aggregate.id)
    assert result["reason"] == "retention_database_error"
    assert repo.rows == repo.durable[1] == remaining
    assert repo.state.pending_phase == phase and repo.state.authorization_txid is None
    assert repo.state.retry_count == 1


def test_mandatory_membership_failure_rolls_back_without_false_progress(monkeypatch):
    repo, service = pending_service(monkeypatch, 1, "mandatory")
    result = service.maintain(repo.aggregate.id)
    assert result["reason"] == "retention_database_error"
    assert repo.rows == 1 and repo.state.pending_phase == "memberships"
    assert repo.state.pending_cursor is None and repo.state.authorization_txid is None


def test_next_turn_outcome_failure_keeps_previously_committed_membership(monkeypatch):
    repo, service = pending_service(monkeypatch, 1)
    assert service.maintain(repo.aggregate.id)["pending"]
    repo.failure = "next-phase"
    repo.clock[0] = .84
    # This next call has its own unchanged .85s budget, so force exhaustion
    # only after the actual outcomes operation, not during an earlier lock.
    original = repo.compact_outcomes
    def exhausted(*args, **kwargs):
        repo.clock[0] += .84
        return original(*args, **kwargs)
    monkeypatch.setattr(repo, "compact_outcomes", exhausted)
    result = service.maintain(repo.aggregate.id)
    assert result["reason"] == "retention_budget"
    assert repo.rows == repo.durable[1] == 0 and repo.state.pending_phase == "outcomes"


def test_manual_caller_unwraps_only_cursor_and_preserves_outcomes_contract():
    from app.repositories.sqlalchemy.aggregate_cleanup import SqlAlchemyAggregateCleanupQueries
    calls = []
    closure = SimpleNamespace(origin_point_id=UUID(int=4))
    repo = SimpleNamespace(compact_memberships=lambda *_args: MembershipCompaction(None, 1),
        compact_outcomes=lambda *_args: calls.append("outcomes") or None,
        clear_payload=lambda *_args: calls.append("clear") or True)
    assert SqlAlchemyAggregateCleanupQueries.compact_point(repo, closure, None)
    assert calls == ["outcomes", "clear"]


def test_manual_positive_cursor_commits_details_before_any_outcome_work():
    from app.repositories.sqlalchemy.aggregate_cleanup import SqlAlchemyAggregateCleanupQueries
    calls = []
    closure = SimpleNamespace(origin_point_id=UUID(int=4))
    repo = SimpleNamespace(compact_memberships=lambda *_args: MembershipCompaction(UUID(int=5), 1),
        compact_outcomes=lambda *_args: calls.append("outcomes") or None,
        clear_payload=lambda *_args: calls.append("clear") or True)
    assert not SqlAlchemyAggregateCleanupQueries.compact_point(repo, closure, None)
    assert calls == []
