"""Pure worker/admission tests plus explicit PostgreSQL discovery cases."""

from collections import deque
from concurrent.futures import Future
from contextlib import nullcontext
from threading import Event
from types import SimpleNamespace
from uuid import UUID

import pytest
from app.aggregation.worker_admission import (
    ADMISSION_FOREGROUND_BURST,
    FOREGROUND_BURST,
    MIB,
    READY_LIMIT,
    WorkerAdmission,
)
from app.core.errors import DomainError
from app.services.implementations import aggregate_execution
from app.services.implementations.aggregate_worker import AggregateWorker


class ControlledExecutor:
    def __init__(self):
        self.tasks = deque()

    def submit(self, call, *args):
        future = Future()
        self.tasks.append((future, call, args))
        return future

    def finish(self, count=None):
        for _ in range(len(self.tasks) if count is None else count):
            future, call, args = self.tasks.popleft()
            future.set_result(call(*args))


class ImmediateExecutor:
    def __init__(self, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def submit(self, call, *args):
        future = Future()
        future.set_result(call(*args))
        return future


class StopProbe:
    def __init__(self, *, stop_when_idle=False):
        self.stopped, self.stop_when_idle, self.waits = False, stop_when_idle, []

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.waits.append(seconds)
        if self.stop_when_idle:
            self.set()
        return self.stopped


def worker_service(identities, maintenance=()):
    foreground, retained, seen, errors = deque(identities), deque(maintenance), [], []

    def due(queue, limit, exclude_ids):
        return [identity for identity in queue if identity not in exclude_ids][:limit]

    def advance(identity, *, execution_started=None):
        assert execution_started is not None
        foreground.remove(identity)
        foreground.append(identity)
        seen.append(("foreground", identity))
        return {"state": "closed"}

    def maintain(identity):
        retained.remove(identity)
        retained.append(identity)
        seen.append(("maintenance", identity))
        return {"status": "compacting", "pending": True}

    service = SimpleNamespace(
        due=lambda limit, exclude_ids=(): due(foreground, limit, exclude_ids),
        maintenance_due=lambda limit, exclude_ids=(): due(retained, limit, exclude_ids),
        advance=advance, maintain=maintain,
        record_worker_error=lambda *args: errors.append(args),
    )
    return service, seen, errors


def make_worker(service, **kwargs):
    return AggregateWorker(lambda: nullcontext(None), lambda *_: service, None, Event(), **kwargs)


def test_admission_quota_is_separate_from_durable_online_scan_history_burst():
    assert ADMISSION_FOREGROUND_BURST == 1
    assert FOREGROUND_BURST == aggregate_execution.FOREGROUND_BURST == 3


def test_global_memory_reservation_is_shared_and_never_exceeds_unchanged_worker_budget():
    admission = WorkerAdmission()
    admission.offer("foreground", [UUID(int=i) for i in range(1, 10)], 0)
    assert admission.acquire() and admission.acquire()
    assert admission.acquire() is None
    assert admission.summary()["reserved_bytes"] == 512 * MIB
    assert admission.capacity == 2
    assert WorkerAdmission(300).capacity == 1
    assert WorkerAdmission(32).reservation_bytes == 256 * MIB
    with pytest.raises(ValueError, match="shared 512 MiB"):
        WorkerAdmission(513)


def test_bounded_queue_rediscovers_all_targets_beyond_first_64_without_dropping_windows():
    identities = [UUID(int=i) for i in range(1, 194)]
    service, seen, _ = worker_service(identities)
    worker, executor = make_worker(service), ControlledExecutor()
    for _ in range(110):
        worker.tick(executor)
        assert len(worker.admission.ready) <= READY_LIMIT
        assert len(worker.pending) <= 2
        executor.finish()
    assert {identity for _, identity in seen} == set(identities)
    assert worker.admission.high_water == READY_LIMIT


@pytest.mark.parametrize("operation_memory_mb,capacity", [(256, 2), (512, 1)])
def test_continuous_foreground_backlog_allows_real_maintenance_turns_for_every_target(
        operation_memory_mb, capacity):
    identities = [UUID(int=i) for i in range(1, 31)]
    service, seen, errors = worker_service(identities, identities)
    worker = make_worker(service, operation_memory_mb=operation_memory_mb)
    executor = ControlledExecutor()
    for _ in range(100):
        worker.tick(executor)
        active = list(worker.admission.active.values())
        assert len(active) <= worker.admission.capacity == capacity
        assert sum(job.kind == "maintenance" for job in active) <= 1
        assert len({job.aggregate_id for job in active}) == len(active)
        assert worker.admission.summary()["reserved_bytes"] <= 512 * MIB
        executor.finish()
    assert {identity for kind, identity in seen if kind == "maintenance"} == set(identities)
    assert {identity for kind, identity in seen if kind == "foreground"} == set(identities)
    assert all(code is None for _, code in errors)


def test_active_maintenance_does_not_bank_foreground_debt_ahead_of_queued_same_target():
    admission = WorkerAdmission()
    target, b, e, f, g = (UUID(int=index) for index in range(1, 6))
    admission.offer("maintenance", [target], 0)
    admission.offer("foreground", [b, target, e, f, g], 0)
    assert admission.acquire().aggregate_id == b
    maintenance = admission.acquire()
    assert (maintenance.kind, maintenance.aggregate_id) == ("maintenance", target)
    admission.release(b)

    # F(target) was queued before M(target) started, and remains blocked by that
    # active turn. Other targets can use the second slot while it runs.
    for identity in (e, f, g):
        foreground = admission.acquire()
        assert (foreground.kind, foreground.aggregate_id) == ("foreground", identity)
        assert admission.summary()["reserved_bytes"] == 512 * MIB
        assert admission.summary()["maintenance_active"] == 1
        assert admission.foreground_streak == 0
        assert len({job.aggregate_id for job in admission.active.values()}) == 2
        admission.release(identity)

    admission.release(target)
    admission.offer("maintenance", [target], 1)
    next_job = admission.acquire()
    assert (next_job.kind, next_job.aggregate_id, next_job.queued_at) == ("foreground", target, 0), (
        "foreground work while maintenance was already active must not give a new maintenance turn priority",
        next_job)


@pytest.mark.parametrize("operation_memory_mb,capacity", [(256, 2), (512, 1)])
def test_runnable_waiting_maintenance_keeps_one_foreground_bound_and_fifo(
        operation_memory_mb, capacity):
    admission = WorkerAdmission(operation_memory_mb)
    target, b, c, d, e = (UUID(int=index) for index in range(1, 6))
    admission.offer("maintenance", [target], 0)
    admission.offer("foreground", [b, c, d, e], 0)
    first = admission.acquire()
    assert (first.kind, first.aggregate_id) == ("foreground", b)
    assert admission.capacity == capacity
    if capacity == 1:
        assert admission.acquire() is None
        admission.release(b)
    maintenance = admission.acquire()
    assert (maintenance.kind, maintenance.aggregate_id) == ("maintenance", target)
    assert admission.summary()["reserved_bytes"] == 512 * MIB
    assert admission.summary()["maintenance_active"] == 1
    assert admission.acquire() is None
    if capacity == 2:
        admission.release(b)
    else:
        admission.release(target)
    for identity in (c, d, e):
        foreground = admission.acquire()
        assert (foreground.kind, foreground.aggregate_id) == ("foreground", identity)
        assert admission.summary()["reserved_bytes"] <= 512 * MIB
        assert admission.summary()["active"] <= capacity
        admission.release(identity)
    if capacity == 2:
        admission.release(target)
    assert not admission.active and not admission.ready


def test_waiting_maintenance_keeps_progress_while_its_target_foreground_is_active():
    admission = WorkerAdmission()
    target, other = UUID(int=1), UUID(int=2)
    admission.offer("foreground", [target], 0)
    # Offer before F(target) starts: active targets are intentionally excluded
    # from discovery, while a previously queued M may wait behind that F.
    admission.offer("maintenance", [target], 1)
    assert admission.acquire().aggregate_id == target
    admission.offer("foreground", [other], 1)
    assert admission.acquire().aggregate_id == other

    # The queued M(target) is blocked while F(target) is active. F(other)
    # must not repeatedly erase the global fairness progress of that waiter.
    admission.release(target)
    admission.offer("foreground", [target], 2)
    maintenance = admission.acquire()
    assert (maintenance.kind, maintenance.aggregate_id, maintenance.queued_at) == ("maintenance", target, 1)
    assert admission.summary()["maintenance_active"] == 1
    assert admission.summary()["reserved_bytes"] == 512 * MIB


def test_waiting_maintenance_after_busy_slots_allows_only_one_new_foreground_start():
    admission = WorkerAdmission()
    target, b, c, d, e = (UUID(int=index) for index in range(1, 6))
    admission.offer("foreground", [b, c], 0)
    assert admission.acquire().aggregate_id == b
    assert admission.acquire().aggregate_id == c
    admission.offer("maintenance", [target], 1)
    admission.offer("foreground", [d, e], 1)
    assert admission.acquire() is None  # Admission never preempts an active job.
    admission.release(b)
    assert admission.acquire().aggregate_id == d
    admission.release(c)
    maintenance = admission.acquire()
    assert (maintenance.kind, maintenance.aggregate_id) == ("maintenance", target)
    assert admission.summary()["maintenance_active"] == 1
    assert admission.summary()["reserved_bytes"] == 512 * MIB
    admission.release(d)
    foreground = admission.acquire()
    assert (foreground.kind, foreground.aggregate_id) == ("foreground", e)


@pytest.mark.parametrize("operation_memory_mb,capacity", [(256, 2), (512, 1)])
def test_one_target_continuous_foreground_and_maintenance_both_progress_without_overlap(
        operation_memory_mb, capacity):
    identity = UUID(int=1)
    service, seen, errors = worker_service([identity], [identity])
    worker = make_worker(service, operation_memory_mb=operation_memory_mb)
    executor = ControlledExecutor()
    for _ in range(48):
        worker.tick(executor)
        assert len(worker.admission.active) == 1
        assert worker.admission.capacity == capacity
        assert worker.admission.summary()["reserved_bytes"] <= 512 * MIB
        assert len(worker.admission.ready) <= READY_LIMIT
        assert len(executor.tasks) == 1
        executor.finish()
    assert seen == [("foreground", identity), ("maintenance", identity)] * 24
    assert not errors


def test_one_slow_job_keeps_other_slot_available_for_the_whole_ready_queue():
    identities = [UUID(int=i) for i in range(1, 31)]
    service, seen, _ = worker_service(identities)
    worker, executor = make_worker(service), ControlledExecutor()
    worker.tick(executor)
    blocked = executor.tasks.popleft()
    for _ in range(35):
        executor.finish()
        worker.tick(executor)
    assert {identity for _, identity in seen} == set(identities[1:])
    assert identities[0] in worker.pending
    executor.tasks.append(blocked)
    executor.finish()


def test_foreground_failure_backoff_and_maintenance_failure_backoff_are_independent(monkeypatch):
    clock, identity = [100.0], UUID(int=1)
    monkeypatch.setattr("app.services.implementations.aggregate_worker.time.monotonic", lambda: clock[0])
    service, seen, errors = worker_service([identity], [identity])

    def fail_foreground(target, *, execution_started=None):
        assert execution_started is not None
        seen.append(("foreground", target))
        raise DomainError("aggregate_timeout", "synthetic", 503)

    service.advance = fail_foreground
    worker = make_worker(service)
    worker.tick()
    assert worker.failures[identity] == (1, 100.5)
    assert identity not in worker.maintenance_failures
    assert errors == [(identity, "aggregate_timeout")]
    worker.tick()
    assert seen.count(("foreground", identity)) == 1
    assert seen.count(("maintenance", identity)) == 2
    clock[0] = 100.5
    worker.tick()
    assert worker.failures[identity] == (2, 101.5)
    assert errors[-1] == (identity, "aggregate_timeout")

    def fail_maintenance(_):
        raise OSError("synthetic connection unavailable")

    service.maintain = fail_maintenance
    worker.tick()
    assert worker.maintenance_failures[identity] == (1, 101.5)
    assert worker.failures[identity] == (2, 101.5)


def test_idle_other_turn_does_not_delay_ready_backlog_but_true_idle_still_backs_off(monkeypatch):
    identity, clock = UUID(int=1), [100.0]
    monkeypatch.setattr("app.services.implementations.aggregate_worker.time.monotonic", lambda: clock[0])
    service, seen, _ = worker_service([identity])
    original = service.advance
    ready = [True]

    def advance(target, *, execution_started=None):
        original(target, execution_started=execution_started)
        return {"state": "history_waiting", "work_available": ready[0]}

    service.advance = advance
    worker = make_worker(service)
    worker.tick()
    worker.tick()
    assert len(seen) == 2 and not worker.failures
    ready[0] = False
    worker.tick()
    worker.tick()
    assert len(seen) == 3 and worker.failures[identity] == (0, 100.25)


@pytest.mark.parametrize("command", ["paused", "cleanup"])
def test_queued_job_reads_current_lifecycle_instead_of_caching_candidate_before_admission(command):
    identities = [UUID(int=i) for i in range(1, 4)]
    service, seen, _ = worker_service(identities)
    state, committed = {"value": "active"}, []
    original = service.advance

    def advance(identity, *, execution_started=None):
        original(identity, execution_started=execution_started)
        if state["value"] == "active":
            committed.append(identity)
        return {"state": "closed" if state["value"] == "active" else "paused"}

    service.advance = advance
    worker, executor = make_worker(service), ControlledExecutor()
    worker.tick(executor)
    executor.finish()
    state["value"] = command
    worker.tick(executor)
    executor.finish()
    assert identities[2] in [identity for _, identity in seen]
    assert identities[2] not in committed


def test_restart_rebuilds_bounded_queue_from_durable_due_and_stop_does_not_submit_new_work():
    identities = [UUID(int=i) for i in range(1, 80)]
    service, seen, _ = worker_service(identities)
    old, executor = make_worker(service), ControlledExecutor()
    old.tick(executor)
    executor.finish()
    old.stop.set()
    old.tick(executor)
    assert not executor.tasks
    restarted = make_worker(service)
    for _ in range(42):
        restarted.tick(executor)
        executor.finish()
    assert {identity for _, identity in seen} == set(identities)


def test_ready_completion_dispatches_backlog_without_a_fixed_per_turn_sleep(monkeypatch):
    identity, stop = UUID(int=1), StopProbe()
    service, seen, _ = worker_service([identity])
    original = service.advance

    def advance(target, *, execution_started=None):
        result = original(target, execution_started=execution_started)
        if len(seen) == 30:
            stop.set()
        return result

    service.advance = advance
    monkeypatch.setattr("app.services.implementations.aggregate_worker.ThreadPoolExecutor", ImmediateExecutor)
    worker = AggregateWorker(lambda: nullcontext(None), lambda *_: service, None, stop)
    worker.run()
    assert len(seen) == 30
    assert stop.waits == [], "ready futures must wake the coordinator without a 50ms floor"
    assert len(worker.pending) <= 2


def test_idle_loop_keeps_its_interruptible_bounded_wait(monkeypatch):
    stop = StopProbe(stop_when_idle=True)
    service, _, _ = worker_service([])
    monkeypatch.setattr("app.services.implementations.aggregate_worker.ThreadPoolExecutor", ImmediateExecutor)
    worker = AggregateWorker(lambda: nullcontext(None), lambda *_: service, None, stop)
    worker.run()
    assert stop.waits == [.05]


@pytest.mark.parametrize("status", ["closed", "already_closed", "candidate_cancelled", "scan_cancelled", "attempt_running"])
def test_completed_or_fenced_candidate_does_not_open_another_metadata_write(status):
    identity = UUID(int=1)
    service, _, _ = worker_service([identity])
    service.advance = lambda _identity, **_kwargs: {"state": status}
    service.record_worker_error = lambda *_args: pytest.fail("must not write metadata after terminal/fenced result")
    worker = make_worker(service)
    worker.tick()
    assert not worker.pending


@pytest.fixture(scope="module")
def worker_lifecycle_migrated():
    # Import the module, not its autouse fixture into this module's globals:
    # existing pure cases must remain runnable without TEST_DATABASE_URL.
    import test_event_time_pipeline as pipeline

    pipeline.migrated.__wrapped__()


@pytest.fixture
def worker_lifecycle_database(worker_lifecycle_migrated):
    import test_event_time_pipeline as pipeline

    yield from pipeline.context.__wrapped__()


def lifecycle_worker_targets(context):
    from datetime import UTC, datetime, timedelta

    from test_event_time_pipeline import create, write_event

    anchor = (datetime.now(UTC) - timedelta(minutes=15)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    write_event(context, (anchor + timedelta(seconds=301)).isoformat(), offset=1)
    return create(context, anchor)[0], create(context, anchor)[0]


def change_worker_target(context, identity, action):
    from uuid import uuid4

    from app.aggregate_composition import build_aggregate_service

    database, catalog, _, _ = context
    with database.session_factory() as session:
        service = build_aggregate_service(session, catalog)
        if action == "pause":
            service.aggregate_command(identity, UUID(int=1), str(uuid4()), action)
        else:
            service.lifecycle(identity, action, UUID(int=1))


def durable_discovery_worker(context, operation_memory_mb):
    from app.aggregate_composition import build_aggregate_service

    database, catalog, _, _ = context
    calls = []

    def build(session, current_catalog):
        service = build_aggregate_service(session, current_catalog)
        original = service.advance

        def advance(identity, **kwargs):
            calls.append(identity)
            return original(identity, **kwargs)

        service.advance = advance
        return service

    return AggregateWorker(database.session_factory, build, catalog, Event(),
        operation_memory_mb=operation_memory_mb), calls


def run_durable_discovery_turns(worker, executor, count=18):
    from app.services.implementations import aggregate_worker as worker_module

    # Only the coordinator's retry clock advances deterministically. Replace
    # this module reference, never time.monotonic on the shared time module.
    # Execution/service/DB/event clocks remain real; queue durations from this
    # test are not latency measurements.
    coordinator = [worker_module.time.monotonic()]

    def coordinator_tick():
        coordinator[0] += 1
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(worker_module, "time", SimpleNamespace(monotonic=lambda: coordinator[0]))
            worker.tick(executor)

    for _ in range(count):
        coordinator_tick()
        active = list(worker.admission.active.values())
        assert len(active) <= worker.admission.capacity
        assert len({job.aggregate_id for job in active}) == len(active)
        assert sum(job.kind == "maintenance" for job in active) <= 1
        assert worker.admission.summary()["reserved_bytes"] <= 512 * MIB
        executor.finish()  # Outside the patch: _execute receives real time and T.
    worker.stop.set()
    coordinator_tick()  # Collect completed futures, without another dispatch.
    assert not worker.pending and not worker.admission.active


@pytest.mark.parametrize("action", ["archive", "pause"])
@pytest.mark.parametrize("operation_memory_mb", [256, 512])
def test_actual_discovery_advances_active_target_without_assigning_inactive_foreground(
        worker_lifecycle_database, action, operation_memory_mb):
    from app.aggregate_composition import build_aggregate_service
    from app.models import AggregateClosure, AggregateSchedule
    from sqlalchemy import select

    context = worker_lifecycle_database
    database, catalog, _, _ = context
    active, inactive = lifecycle_worker_targets(context)
    change_worker_target(context, inactive.id, action)
    with database.session_factory() as session:
        schedule = session.get(AggregateSchedule, inactive.schedule_id)
        before = (schedule.next_index, schedule.turn, schedule.scan_after)
    worker, calls = durable_discovery_worker(context, operation_memory_mb)
    run_durable_discovery_turns(worker, ControlledExecutor())

    # This fails against the former repository predicate: stopped identities
    # were repeatedly discovered and executed as scan/idle instead of excluded.
    assert inactive.id not in calls, (
        "inactive no-work jobs must not compete with the active aggregate; "
        f"active={calls.count(active.id)} inactive={calls.count(inactive.id)} total={len(calls)}")
    assert active.id in calls
    with database.session_factory() as session:
        service = build_aggregate_service(session, catalog)
        assert active.id in service.due() and inactive.id not in service.due()
        schedule = session.get(AggregateSchedule, inactive.schedule_id)
        assert (schedule.next_index, schedule.turn, schedule.scan_after) == before
        closures = list(session.scalars(select(AggregateClosure).where(
            AggregateClosure.aggregate_id.in_([active.id, inactive.id])).order_by(AggregateClosure.window_index)))
        assert [(row.window_index, row.calculation_status) for row in closures if row.aggregate_id == active.id] == [
            (0, "computed"), (1, "computed")]
        assert not [row for row in closures if row.aggregate_id == inactive.id]
        assert service.point_at(active.id, 0).event_count == service.point_at(active.id, 1).event_count == 1


@pytest.mark.parametrize("action", ["archive", "pause"])
@pytest.mark.parametrize("operation_memory_mb", [256, 512])
def test_actual_discovery_stale_queued_target_is_fenced_then_not_rediscovered(
        worker_lifecycle_database, action, operation_memory_mb):
    from app.aggregate_composition import build_aggregate_service
    from app.models import AggregateClosure, AggregateSchedule
    from sqlalchemy import select

    context = worker_lifecycle_database
    database, catalog, _, _ = context
    active, stale = lifecycle_worker_targets(context)
    worker, calls = durable_discovery_worker(context, operation_memory_mb)
    worker._discover()
    assert stale.id in [job.aggregate_id for job in worker.admission.ready if job.kind == "foreground"]
    change_worker_target(context, stale.id, action)
    with database.session_factory() as session:
        before_index = session.get(AggregateSchedule, stale.schedule_id).next_index
    run_durable_discovery_turns(worker, ControlledExecutor())

    assert calls.count(stale.id) == 1, "an already queued identity may finish once, never acquire a new inactive job"
    assert active.id in calls
    with database.session_factory() as session:
        service = build_aggregate_service(session, catalog)
        # Check actual discovery without the worker's temporary retry exclusion;
        # no-work backoff must not hide an incorrectly eligible stale identity.
        assert stale.id not in service.due()
        assert session.get(AggregateSchedule, stale.schedule_id).next_index == before_index
        assert list(session.scalars(select(AggregateClosure).where(AggregateClosure.aggregate_id == stale.id))) == []
        assert service.point_at(active.id, 0).calculation_status == "computed"
