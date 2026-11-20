"""Additional real DB publication/GC barriers, separate from the initial gate."""

import os
import re
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from retention_assertions import (
    assert_maintenance_result,
    assert_remaining_index,
    maintenance_trace,
)
from sqlalchemy import event, func, select, update
from sqlalchemy.exc import DBAPIError
from test_aggregate_retention import history, maintain, read_state, settle
from test_event_time_pipeline import context, create, migrated, turns, write_event

from app.adapters.detector_lock import DetectorSessionLock
from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import OperationBudget
from app.core.errors import DomainError
from app.detection.contracts import ModelConfig, StaleCandidate, digest
from app.detector_composition import build_detector_service
from app.models import (
    Aggregate,
    AggregateClosure,
    AggregateLateEvent,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSchedule,
    AggregateSnapshot,
    Detector,
    DetectorOutcome,
    DetectorWarmup,
    Finding,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository

__all__ = ["context", "migrated"]


def add_closure(context, card, index):
    db, _, source, _ = context
    start, identity = card.anchor_utc + timedelta(seconds=index * card.duration_seconds), uuid4()
    end = start + timedelta(seconds=card.duration_seconds)
    with db.session_factory.begin() as session:
        session.add(AggregateClosure(origin_point_id=identity, aggregate_id=card.id,
            schedule_id=card.schedule_id, source_identity=source, window_index=index,
            start_utc=start, end_utc=end, time_basis="event_time", origin="initial_history",
            snapshot_reference_at=end, closed_at=end, close_not_before_at=end,
            payload_state="present", point_kind="working"))
        session.flush()
        session.add(WorkingWindowPoint(id=identity, schedule_id=card.schedule_id, window_index=index,
            start_utc=start, end_utc=end, scheduled_at=end, actual_started_at=end, actual_completed_at=end,
            event_count=0, features=[], completeness="unknown", completeness_reason="storage_fixture",
            evidence_refs={}, time_basis="event_time"))
        session.add(AggregateSnapshot(closure_id=identity, metadata_json={"storage_fixture": True}))
    return identity


@contextmanager
def owner(context):
    db, catalog, _, _ = context
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    pid = lock.acquire()
    assert pid is not None
    try:
        with db.session_factory() as session:
            token = build_detector_service(session, catalog).acquire_owner(uuid4(), pid)
        yield token
    finally:
        lock.close()


def storage_attempt(context, status="prepared"):
    """Guard-only metadata fixture; no training/model validity is claimed."""
    db, catalog, _, _ = context
    card, _ = history(context, count=16)
    with db.session_factory() as session:
        identity = build_detector_service(session, catalog).create(card.id, UUID(int=1), "Immutable metadata",
            config={"sample_size": 8, "output_after": 8})
        attempt_id = uuid4()
        cutoff = card.anchor_utc + timedelta(seconds=160)
        plan = {"first_start": card.anchor_utc.isoformat(), "cutoff": cutoff.isoformat(),
            "count": 16, "step": 10, "duration": 10, "evaluation_start_index": card.online_start_index}
        session.add(DetectorWarmup(id=attempt_id, detector_id=identity, cutoff=cutoff, plan=plan,
            inputs=[], inputs_hash=digest([]), status=status, reason=None, created_at=card.created_at))
        session.flush()
        model = session.get(Detector, identity)
        model.active_attempt_id, model.revision = attempt_id, model.revision + 1
        session.commit()
    return card, identity, attempt_id


@pytest.mark.parametrize("mutation", ["plan", "cutoff", "id", "inputs", "prepared", "waiting", "ready"])
def test_attempt_identity_plan_and_final_inputs_cannot_be_reopened_with_revision_bump(context, mutation):
    db, _, _, _ = context
    original_status = mutation if mutation in {"waiting", "ready"} else "prepared"
    _card, identity, attempt_id = storage_attempt(context, original_status)
    with db.session_factory() as session:
        before = session.get(Detector, identity).revision
        saved = session.get(DetectorWarmup, attempt_id)
        plan, cutoff = dict(saved.plan), saved.cutoff
        session.execute(update(Detector).where(Detector.id == identity).values(revision=Detector.revision + 1))
        change = ({"plan": {**plan, "count": 17}} if mutation == "plan" else
            {"cutoff": cutoff + timedelta(seconds=10)} if mutation == "cutoff" else
            {"id": uuid4()} if mutation == "id" else
            {"inputs": [{"changed": True}], "inputs_hash": digest([{"changed": True}])} if mutation == "inputs" else
            {"status": "preparing"})
        with pytest.raises(DBAPIError) as error:
            session.execute(update(DetectorWarmup).where(DetectorWarmup.id == attempt_id).values(**change))
        assert error.value.orig.sqlstate == "23514"
        session.rollback()
        assert session.get(Detector, identity).revision == before
        saved = session.get(DetectorWarmup, attempt_id)
        assert saved.plan == plan and saved.cutoff == cutoff and saved.status == original_status
        assert saved.inputs == [] and saved.inputs_hash == digest([])


def test_locked_dependency_headers_do_not_fetch_or_hash_plan_checkpoint_inputs(context):
    db, _, _, _ = context
    card, identity, attempt_id = storage_attempt(context)
    statements = []
    def inspect(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        repo.schedule(card.schedule_id, True)
        event.listen(db.engine, "before_cursor_execute", inspect)
        try:
            detectors, attempts = repo.retention_headers(card.id, OperationBudget(1), lock=True)
        finally:
            event.remove(db.engine, "before_cursor_execute", inspect)
        assert detectors[0]["id"] == identity and attempts[0]["id"] == attempt_id
        assert statements
        assert all(re.search(r"\b(plan|checkpoint|inputs|md5|sha256|to_jsonb)\b", sql, re.IGNORECASE) is None for sql in statements)


def test_preparation_progress_is_atomic_with_revision_and_failed_restart_uses_new_attempt(context, monkeypatch):
    db, catalog, _, _ = context
    card, _ = history(context, count=16)
    with owner(context) as token, db.session_factory() as session:
        service = build_detector_service(session, catalog, token)
        identity = service.create(card.id, UUID(int=1), "Revision publication", config={"sample_size": 8, "output_after": 8})
        service.start(identity)
        service.begin_start(identity)
        before = service.state(identity)["detector"]["revision"]
        observations, commit = [], service.uow.commit
        def observe_commit():
            commit()
            with db.session_factory() as observer:
                model = observer.get(Detector, identity)
                attempt = observer.get(DetectorWarmup, model.active_attempt_id)
                observations.append((model.revision, attempt.id, attempt.status, attempt.inputs_hash))
        monkeypatch.setattr(service.uow, "commit", observe_commit)
        first = service.prepare_warmup(identity)
        assert first["state"] == "waiting"
        assert observations[0] == (before + 1, first["attempt_id"], "preparing", digest([]))
        assert observations[1][0:3] == (before + 2, first["attempt_id"], "waiting")
        assert observations[1][3] != observations[0][3]
        saved = service.repository.attempt(first["attempt_id"])
        # Every writer of an old waiting attempt preserves it; explicit retry
        # creates a new identity rather than reopening its immutable inputs.
        monkeypatch.setattr(service.uow, "commit", commit)
        service.start(identity)
        service.begin_start(identity)
        second = service.prepare_warmup(identity)
        assert second["state"] == "waiting" and second["attempt_id"] != first["attempt_id"]
        assert service.repository.attempt(first["attempt_id"]) == saved


def test_present_pending_claim_is_replanned_without_deleting_or_counting(context):
    db, _, _, _ = context
    card, ids = history(context)
    with db.session_factory.begin() as session:
        state = session.get(AggregateRetentionState, card.id)
        state.pending_closure_id, state.pending_phase = ids[0], "claim"
        state.status = "compacting"
    result = maintain(context, card.id)
    assert result["status"] == "deferred" and result["reason"] == "dependencies_changed"
    state = read_state(context, card.id)
    assert state["pending_closure_id"] is state["pending_phase"] is state["authorization_txid"] is None
    assert state["auto_cleared_total"] == state["history_revision"] == 0
    with db.session_factory() as session:
        assert session.get(WorkingWindowPoint, ids[0]) is not None
    maintain(context, card.id)
    assert read_state(context, card.id)["auto_cleared_total"] == 1


def test_plan_to_preparing_race_replans_without_publishing_cleared_base(context, monkeypatch):
    db, catalog, _, _ = context
    card, ids = history(context, count=16, first_index=504)
    with owner(context) as token, db.session_factory() as session:
        service = build_detector_service(session, catalog, token)
        identity = service.create(card.id, UUID(int=1), "Plan publication", config={"sample_size": 8, "output_after": 8})
        service.start(identity)
        service.begin_start(identity)
        before = service.state(identity)["detector"]
        original = service.history.plan
        def race(*args):
            result = original(*args)
            add_closure(context, card, 1020)
            outcome = maintain(context, card.id)
            assert outcome["status"] != "error", outcome
            assert read_state(context, card.id)["auto_cleared_total"] == 1
            return result
        monkeypatch.setattr(service.history, "plan", race)
        with pytest.raises(StaleCandidate, match="warmup_plan_payload_changed"):
            service.prepare_warmup(identity)
        current = service.state(identity)["detector"]
        assert current["execution_state"] == "starting" and current["failure_code"] is None
        assert current["revision"] == before["revision"] and current["start_request_id"] == before["start_request_id"]
        assert current["active_attempt_id"] is current["checkpoint"] is None
        assert session.scalar(select(func.count()).select_from(DetectorWarmup).where(DetectorWarmup.detector_id == identity)) == 0
        assert session.get(AggregateClosure, ids[0]).payload_state == "cleared"


def test_published_preparing_plan_pins_old_base_after_new_tail(context, monkeypatch):
    db, catalog, _, _ = context
    card, ids = history(context, count=16, first_index=504)
    with owner(context) as token, db.session_factory() as session:
        service = build_detector_service(session, catalog, token)
        identity = service.create(card.id, UUID(int=1), "Frozen publication", config={"sample_size": 8, "output_after": 8})
        service.start(identity)
        service.begin_start(identity)
        original = service.history.materialize
        observed = []
        def race(*args):
            with db.session_factory() as observer:
                model = observer.get(Detector, identity)
                frozen = observer.get(DetectorWarmup, model.active_attempt_id)
                assert frozen.status == "preparing" and frozen.plan["base_closures"][0]["window_index"] == 504
                observed.append(frozen.id)
            add_closure(context, card, 1020)
            result = maintain(context, card.id)
            assert result["status"] == "protected", result
            state = read_state(context, card.id)
            assert state["keep_from_index"] == 504 and state["auto_cleared_total"] == 0
            assert state["protected_by"][0]["reason"] == "frozen_warmup"
            return original(*args)
        monkeypatch.setattr(service.history, "materialize", race)
        result = service.prepare_warmup(identity)
        # Unknown storage-fixture quality causes its original explicit start to
        # fail; GC must not be the cause or remove any pinned canonical payload.
        assert observed and result["state"] == "waiting"
        assert all(session.get(AggregateClosure, identity).payload_state == "present" for identity in ids)
        assert service.state(identity)["warmup"]["status"] == "waiting"


def test_later_initial_history_behind_old_progress_is_found_without_regeneration(context):
    db, catalog, _, _ = context
    card, ids = history(context, omit_indices={3})
    settle(context, card.id, 7)
    late_history = add_closure(context, card, 3)
    settle(context, card.id, 8)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.points(card.id, 1, 0)["total"] == 512
        assert session.get(AggregateClosure, late_history).payload_state == "cleared"
        assert all(session.get(AggregateClosure, identity).payload_state == "cleared" for identity in ids[:3])
        assert session.get(AggregateClosure, ids[4]).payload_state == "cleared"
        assert session.get(AggregateRetentionState, card.id).history_revision == 8


@pytest.mark.parametrize("wrong", ["missing", "foreign", "phase", "transaction"])
def test_membership_authority_is_specific_even_after_logical_clear(context, monkeypatch, wrong):
    db, catalog, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]
        def crash_after_claim():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))
        monkeypatch.setattr(api.uow, "commit", crash_after_claim)
        assert api.maintain(card.id)["pending"]
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        state = repo.retention_state(card.id, lock=True)
        assert session.get(AggregateClosure, ids[0]).payload_state == "cleared"
        state.pending_closure_id = ids[1] if wrong == "foreign" else ids[0]
        if wrong == "missing":
            state.authorization_txid = None
            session.flush()
        else:
            repo.retention_authorize(state, "outcomes" if wrong == "phase" else "memberships", OperationBudget(1))
            if wrong == "transaction":
                session.commit()
        with pytest.raises(DBAPIError):
            session.execute(update(AggregateMembership).where(AggregateMembership.closure_id == ids[0])
                .values(fingerprint=None, event_time=None))
        session.rollback()
        assert session.scalar(select(AggregateMembership.fingerprint).where(AggregateMembership.closure_id == ids[0])) == "f" * 64
        assert session.get(AggregateRetentionState, card.id).auto_cleared_total == 1


@pytest.mark.parametrize("wrong", ["foreign", "phase", "transaction"])
def test_first_clear_authority_rejects_foreign_phase_or_previous_transaction(context, wrong):
    db, _, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        state = repo.retention_state(card.id, lock=True)
        state.pending_closure_id = ids[1] if wrong == "foreign" else ids[0]
        repo.retention_authorize(state, "outcomes" if wrong == "phase" else "claim", OperationBudget(1))
        if wrong == "transaction":
            session.commit()
        closure = session.get(AggregateClosure, ids[0])
        with pytest.raises(DBAPIError):
            repo.clear_payload(closure, OperationBudget(1), automatic=True, state=state)
        session.rollback()
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.get(AggregateSnapshot, ids[0]) is not None
        assert session.get(AggregateClosure, ids[0]).payload_state == "present"
        assert session.get(AggregateRetentionState, card.id).auto_cleared_total == 0


@pytest.mark.parametrize("fence", ["paused", "archived", "manual"])
def test_current_transaction_claim_authority_cannot_override_lifecycle_fence(context, fence):
    db, _, _, _ = context
    card, ids = history(context)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        state = repo.retention_state(card.id, lock=True)
        state.pending_closure_id = ids[0]
        repo.retention_authorize(state, "claim", OperationBudget(1))
        if fence == "archived":
            session.get(Aggregate, card.id).state = "archived"
        else:
            schedule = session.get(AggregateSchedule, card.schedule_id)
            if fence == "paused":
                schedule.paused = True
            else:
                schedule.active_cleanup_id = uuid4()
        session.flush()
        with pytest.raises(DBAPIError):
            repo.clear_payload(session.get(AggregateClosure, ids[0]), OperationBudget(1), automatic=True, state=state)
        session.rollback()
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.get(AggregateClosure, ids[0]).payload_state == "present"
        assert session.get(AggregateRetentionState, card.id).history_revision == 0


@pytest.mark.parametrize("accepted", [False, True])
def test_missing_state_blocks_manual_delete_and_preserves_existing_fence(context, accepted):
    db, catalog, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        receipt = api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 519) if accepted else None
        state = session.get(AggregateRetentionState, card.id)
        assert state.history_revision == 0
        session.delete(state)
        session.commit()
        if accepted:
            with pytest.raises(DomainError) as error:
                api.advance(card.id)
            assert error.value.code == "retention_state_missing"
            operation = api.cleanup_operation(card.id, receipt.cleanup_operation_id)
            assert operation.status == "failed" and operation.deleted_points == 0
            assert session.get(AggregateSchedule, card.schedule_id).active_cleanup_id == receipt.cleanup_operation_id
        else:
            plan = api.cleanup_preview(card.id, 519)
            assert not plan.can_cleanup and plan.reason == "retention_state_missing"
            with pytest.raises(DomainError) as error:
                api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 519)
            assert error.value.code == "retention_state_missing" and error.value.status_code == 409
            assert session.get(AggregateSchedule, card.schedule_id).active_cleanup_id is None
            # A no-op keeps the existing stopped gate and creates no substitute state.
            no_op = api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 520)
            api.advance(card.id)
            assert api.cleanup_operation(card.id, no_op.cleanup_operation_id).status == "succeeded"
        assert session.get(AggregateRetentionState, card.id) is None
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.get(AggregateSnapshot, ids[0]) is not None
        assert session.scalar(select(AggregateMembership.fingerprint).where(AggregateMembership.closure_id == ids[0])) == "f" * 64


def test_new_processes_resume_physical_work_without_counter_or_claim_replay(context, monkeypatch):
    db, catalog, _, _ = context
    card, ids = history(context, members=5001)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]
        def crash_after_claim():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))
        monkeypatch.setattr(api.uow, "commit", crash_after_claim)
        assert api.maintain(card.id)["pending"]
    initial = read_state(context, card.id)
    program = """import os
from uuid import UUID
from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.aggregate_composition import build_aggregate_service
db = Database(os.environ['TEST_DATABASE_URL'])
try:
    with db.session_factory() as session:
        result = build_aggregate_service(session, PackagedEcsCatalog.load()).maintain(UUID(os.environ['RETENTION_TEST_AGGREGATE']))
        assert result['status'] != 'error', result['reason']
finally:
    db.dispose()
"""
    environment = {**os.environ, "RETENTION_TEST_AGGREGATE": str(card.id), "PYTHONDONTWRITEBYTECODE": "1"}
    for _ in range(3):
        result = subprocess.run([sys.executable, "-B", "-c", program], env=environment,
            timeout=30, check=False, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    final = read_state(context, card.id)
    assert final["pending_closure_id"] is final["pending_phase"] is final["authorization_txid"] is None
    assert final["auto_cleared_total"] == final["history_revision"] == 1
    assert final["last_cleared_at"] == initial["last_cleared_at"]
    with db.session_factory() as session:
        assert session.get(AggregateClosure, ids[1]).payload_state == "present"
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(AggregateMembership.closure_id == ids[0])) == 5001
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == ids[0], AggregateMembership.fingerprint.is_not(None))) == 0


def test_actual_prepared_inputs_survive_source_release_and_are_reused(context, monkeypatch):
    db, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=10)).replace(microsecond=0)
    offset = 0
    for index in range(504, 520):
        for second in range(1 + index % 3):
            write_event(context, (anchor + timedelta(seconds=index * 300 + second)).isoformat(), offset=offset, value=index - 500)
            offset += 1
    card, _ = create(context, anchor, 300)
    turns(context, card.id, 60)
    with owner(context) as token, db.session_factory() as session:
        service = build_detector_service(session, catalog, token)
        identity = service.create(card.id, UUID(int=1), "Saved batch", config={"sample_size": 8, "output_after": 8})
        service.start(identity)
        service.begin_start(identity)
        prepared = service.prepare_warmup(identity)
        assert prepared["state"] == "prepared", prepared
        saved = service.repository.attempt(prepared["attempt_id"])
        assert all(item["completeness"] == "complete" for item in saved["inputs"])
        add_closure(context, card, 1020)
        settle(context, card.id, 5)
        after = service.repository.attempt(prepared["attempt_id"])
        assert (after["inputs"], after["inputs_hash"], after["plan"]) == (saved["inputs"], saved["inputs_hash"], saved["plan"])
        assert session.get(AggregateClosure, UUID(saved["inputs"][0]["point_id"])).payload_state == "cleared"
        def forbidden(*args):
            pytest.fail("Prepared batch was recomputed from removable source")
        monkeypatch.setattr(service.history, "materialize", forbidden)
        reused = service.prepare_warmup(identity)
        assert {key: reused[key] for key in ("attempt_id", "state", "inputs", "plan")} == {
            key: prepared[key] for key in ("attempt_id", "state", "inputs", "plan")}
    cleared_id = UUID(saved["inputs"][0]["point_id"])
    late = write_event(context, (datetime.fromisoformat(saved["inputs"][0]["start"]) + timedelta(seconds=5)).isoformat(),
        offset=offset, value=77)
    turns(context, card.id, 18)
    with db.session_factory() as session:
        ledger = list(session.scalars(select(AggregateLateEvent).where(AggregateLateEvent.closure_id == cleared_id)))
        assert [item.event_id for item in ledger] == [late]
        assert session.get(AggregateClosure, cleared_id).payload_state == "cleared"
        assert session.get(WorkingWindowPoint, cleared_id) is None
        assert session.get(AggregateRetentionState, card.id).history_revision == 5
        assert build_detector_service(session, catalog).repository.attempt(prepared["attempt_id"])["inputs"] == saved["inputs"]


def test_outcome_gc_over_5000_preserves_projection_finding_and_membership_identity(context, monkeypatch):
    db, catalog, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]
        def crash_after_claim():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))
        monkeypatch.setattr(api.uow, "commit", crash_after_claim)
        api.maintain(card.id)
    model_ids = [uuid4() for _ in range(5001)]
    config = asdict(ModelConfig(dimensions=2, trees=20, sample_size=8, output_after=8))
    now, finding_id = datetime.now(UTC), uuid4()
    projection = {"exact": [2**60 + 1], "storage_fixture": True}
    with db.session_factory.begin() as session:
        for identity in model_ids:
            session.add(Detector(id=identity, aggregate_id=card.id, name="Outcome storage", created_by=UUID(int=1),
                created_at=now, updated_at=now, config=config, config_hash=digest(config),
                feature_keys=["n", "bytes"], feature_names=["Count", "Bytes"], rule_hash=card.rule_hash,
                state="waiting", reason="warmup_required", revision=0, accelerated=False,
                step_seconds=10, accelerated_used=False, execution_state="stopped", execution_generation=0,
                last_consumed_projection=projection, last_evaluated_projection=projection))
        session.flush()
        for identity in model_ids:
            session.add(DetectorOutcome(id=uuid4(), detector_id=identity, working_point_id=ids[0], window_index=0,
                revision=0, kind="skipped", reason="storage_fixture", data={"features": [2**60 + 1],
                    "start": now.isoformat(), "end": now.isoformat(), "late": False, "recovery": []}))
        session.add(Finding(id=finding_id, detector_id=model_ids[0], working_point_id=ids[0],
            created_at=now, late=False, data=projection))
    with db.session_factory() as session:
        # Archive prevents new claims but cannot strand already-cleared physical work.
        build_aggregate_service(session, catalog).lifecycle(card.id, "archive", UUID(int=1))
        member_ids = list(session.scalars(select(AggregateMembership.event_id).where(AggregateMembership.closure_id == ids[0])))
    previous, inserted, chunks = 5001, False, []
    for _ in range(8):
        with maintenance_trace(db) as trace:
            result = maintain(context, card.id)
        assert_maintenance_result(result, trace)
        with db.session_factory() as session:
            remaining = session.scalar(select(func.count()).select_from(DetectorOutcome).where(
                DetectorOutcome.working_point_id == ids[0], DetectorOutcome.data["compacted"].as_boolean().is_distinct_from(True)))
            changed = previous - remaining
            assert 0 <= changed <= 1000
            if changed:
                chunks.append(changed)
            previous = remaining
            if not inserted and remaining == 1:
                # Both a new row below the diagnostic cursor and a previously
                # compacted row made heavy again must be found from the start.
                cursor = session.get(AggregateRetentionState, card.id).pending_cursor
                assert cursor is not None and cursor.int > 0
                template = session.get(Detector, model_ids[0])
                extra_model = {column.key: getattr(template, column.key) for column in Detector.__table__.columns}
                extra_model["id"] = uuid4()
                session.add(Detector(**extra_model))
                session.flush()
                session.add(DetectorOutcome(id=UUID(int=0), detector_id=extra_model["id"], working_point_id=ids[0],
                    window_index=0, revision=0, kind="skipped", reason="late_storage_fixture", data={"features": [17]}))
                old = session.scalar(select(DetectorOutcome).where(DetectorOutcome.working_point_id == ids[0],
                    DetectorOutcome.data["compacted"].as_boolean().is_(True), DetectorOutcome.id < cursor))
                assert old is not None and old.id < cursor
                old.data = {"features": [19], "late": False}
                session.commit()
                assert_remaining_index(db, trace, "detector_outcomes", "ix_outcome_retention_remaining")
                previous += 2
                inserted = True
            if session.get(AggregateRetentionState, card.id).pending_closure_id is None:
                break
    assert inserted and remaining == 0 and read_state(context, card.id)["pending_closure_id"] is None
    # The original 5001 rows are unchanged; reinflation adds one extra update
    # and the new lower UUID adds one row. No turn hides a 5000-row UPDATE.
    assert len(chunks) == 6 and sum(chunks) == 5003
    with db.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(DetectorOutcome).where(DetectorOutcome.working_point_id == ids[0])) == 5002
        assert session.get(Finding, finding_id).data == projection
        assert list(session.scalars(select(AggregateMembership.event_id).where(AggregateMembership.closure_id == ids[0]))) == member_ids
        for identity in (model_ids[0], model_ids[-1]):
            model = session.get(Detector, identity)
            assert model.last_consumed_projection == model.last_evaluated_projection == projection
            assert model.execution_state == "stopped" and model.checkpoint is model.consumed_index is None
        assert session.get(AggregateClosure, ids[1]).payload_state == "present"
        assert session.get(AggregateRetentionState, card.id).auto_cleared_total == 1
