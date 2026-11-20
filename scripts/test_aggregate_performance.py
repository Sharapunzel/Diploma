"""Permanent load-selection, clock isolation and measurement regressions."""

import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Event
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
import yaml

from scripts.aggregate_performance import (
    PerformanceBudget,
    acknowledged_release,
    armed_release,
    assert_computed_load_point,
    assert_job_trace_health,
    assert_worker_resources,
    bounded_online_intervals,
    commit_delay_upper_bound,
    container_monotonic,
    controlled_clock_overlay,
    distribution,
    expected_load_values,
    first_clear_plan,
    history_gc_acknowledged,
    load_cases,
    load_rule,
    maintenance_overlap,
    measured_job_trace,
    pending_diagnostics,
    run_aggregate_performance,
    selected_load_cases,
    validate_performance_arguments,
    wait_for_history_clear,
    wave_times,
    write_clock,
)


def performance_time(monkeypatch):
    from scripts import aggregate_performance as performance

    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(performance.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(performance.time, "sleep", lambda seconds: setattr(clock, "now", clock.now + seconds))
    return performance, clock


@pytest.mark.parametrize("cap,expected", [(None, 2), (30, 2), (5, 2), (10, 2), (.5, .5)])
def test_profile_remaining_budget_caps_every_blocking_call_and_preserves_smaller_limits(monkeypatch, cap, expected):
    _performance, clock = performance_time(monkeypatch)
    clock.now = 1198
    calls = []

    def operation(*args, **kwargs):
        calls.append((args, kwargs))
        clock.now += .25
        return "committed"

    assert PerformanceBudget(1200).call(operation, "owned", cap=cap, capture=True) == "committed"
    assert calls == [(("owned",), {"timeout": expected, "capture": True})]


def test_expired_profile_starts_no_blocking_work_and_late_success_is_rejected(monkeypatch):
    _performance, clock = performance_time(monkeypatch)
    budget = PerformanceBudget(1200)
    clock.now = 1200
    with pytest.raises(RuntimeError, match="profile budget"):
        budget.call(lambda **_kwargs: pytest.fail("Expired work must not begin"), cap=30)
    clock.now = 1199

    def late_command(*, timeout):
        assert timeout == 1
        clock.now += 1
        return "success"

    with pytest.raises(RuntimeError, match="profile budget"):
        budget.call(late_command, cap=30)


@pytest.mark.parametrize("overall,local,error", [(2, 120, RuntimeError), (1200, 2, AssertionError)])
def test_wait_rejects_late_positive_predicates_at_both_deadlines(monkeypatch, overall, local, error):
    _performance, clock = performance_time(monkeypatch)
    budget = PerformanceBudget(overall)

    def predicate():
        clock.now = 2
        return "ready"

    with pytest.raises(error):
        budget.wait(predicate, local, "Condition timed out")
    assert budget.limits == []


def test_wait_caps_nested_fixture_calls_by_its_remaining_time_and_does_not_call_expired_predicate(monkeypatch):
    _performance, clock = performance_time(monkeypatch)
    budget = PerformanceBudget(1200)

    def predicate():
        clock.now = 119.5

        def fixture(*, timeout):
            assert timeout == .5  # Narrow the existing 30s fixture cap.
            clock.now += .25
            return "ready"

        return budget.call(fixture, cap=30)

    assert budget.wait(predicate, 120, "Condition timed out") == "ready"
    assert budget.limits == [] and budget.remaining() == 1080.25
    with pytest.raises(AssertionError, match="Condition timed out"):
        budget.wait(lambda: pytest.fail("Expired predicate must not run"), 0, "Condition timed out")


@pytest.mark.parametrize("start,expired,error,scope", [
    (0, 120, AssertionError, "local"), (1190, 1200, RuntimeError, "global")])
def test_wait_failure_reports_the_limiting_clock_before_popping_without_extending_it(
        monkeypatch, capsys, start, expired, error, scope):
    import builtins

    _performance, clock = performance_time(monkeypatch)
    budget = PerformanceBudget(1200, started=0)
    clock.now = start
    contexts_at_print = []
    original_print = builtins.print

    def capture_print(*args, **kwargs):
        contexts_at_print.append(tuple(budget.limits))
        original_print(*args, **kwargs)

    monkeypatch.setattr(builtins, "print", capture_print)
    calls = []

    def late_predicate():
        calls.append(clock.now)
        clock.now = expired
        return "late success"

    with pytest.raises(error):
        budget.wait(late_predicate, 120, "before-proof not ready")
    assert calls == [start] and clock.now == expired and budget.limits == []
    assert contexts_at_print == [((start + 120, "before-proof not ready"),)]
    line, = capsys.readouterr().out.splitlines()
    report = json.loads(line.removeprefix("Controlled aggregate bounded wait failure: "))
    assert report["limiting_scope"] == scope and report["error_type"] == error.__name__
    assert report["local_elapsed_seconds"] == expired - start
    assert report["local_remaining_seconds"] == max(0, start + 120 - expired)
    assert report["global_elapsed_seconds"] == expired
    assert report["global_remaining_seconds"] == 1200 - expired


def test_wait_diagnostic_preserves_the_original_predicate_exception_and_hides_its_message(monkeypatch, capsys):
    _performance, clock = performance_time(monkeypatch)
    budget = PerformanceBudget(1200)
    failure = ValueError("private-payload-must-not-be-logged")

    def failed_predicate():
        clock.now = 3
        raise failure

    with pytest.raises(ValueError) as caught:
        budget.wait(failed_predicate, 120, "before-proof not ready")
    assert caught.value is failure and budget.limits == [] and clock.now == 3
    output = capsys.readouterr().out
    assert "private-payload" not in output and '"error_type": "ValueError"' in output
    assert '"local_remaining_seconds": 117' in output


def test_before_proof_failure_keeps_last_completed_readiness_without_extra_rpc_or_payload_dump(
        monkeypatch, tmp_path, capsys):
    performance, clock = performance_time(monkeypatch)
    case = load_cases()[2]
    sources = [str(UUID(int=index + 1)) for index in range(case.sources)]
    identities = [str(UUID(int=index + 100)) for index in range(case.aggregates)]
    missing = identities[-1]
    actions, inspections, creates = [], [], []
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(performance, "load_cases", lambda: (case,))

    def command(args, *, env, timeout, capture=False):
        if args[-1] == "aggregate-worker":
            return ""
        action = args[-1]
        payload = json.loads(next(value.split("=", 1)[1] for value in args
            if value.startswith("AGGREGATE_LOAD_PAYLOAD=")))
        actions.append(action)
        if action == "setup":
            return json.dumps({"sources": sources, "actor": str(UUID(int=40)), "anchor": anchor.isoformat(),
                "clock_at": (anchor + timedelta(seconds=520 * 300)).isoformat(), "heavy_history_rows": 10001})
        if action in {"write", "history"}:
            return json.dumps({"offset": payload["offset"] + payload.get("count", 0), "seconds": 0})
        if action == "create":
            identity = identities[len(creates)]
            creates.append(identity)
            return json.dumps({"card": {"id": identity, "online_start_index": 520}, "seconds": 0})
        assert action == "inspect" and payload == {"aggregates": identities, "sources": sources}
        inspections.append(timeout)
        first = len(inspections) == 1
        clock.now = 1 if first else 120
        return json.dumps({"aggregates": {identity: {"old_payloads": int(identity != missing or not first),
            "membership_details": "private-membership", "features": "private-vector"}
            for identity in identities}, "capture_pending": 7 if first else 0})

    client = SimpleNamespace(request=lambda *_args, **_kwargs: SimpleNamespace(status_code=200,
        json=lambda: {"csrf_token": "private-auth-token"}))
    monkeypatch.setattr(performance.httpx, "Client", lambda **_kwargs: nullcontext(client))
    with pytest.raises(AssertionError, match="controlled load condition timed out"):
        run_aggregate_performance(["compose"], {"AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json"),
            "PUBLIC_ORIGIN": "http://owned.invalid"}, False, tmp_path, command)
    assert actions[-2:] == ["inspect", "inspect"] and inspections == [30, 30]
    assert creates == identities and clock.now == 120
    output = capsys.readouterr().out
    assert "private-" not in output
    lines = output.splitlines()
    started = json.loads(next(line.removeprefix("Controlled aggregate budget starting: ") for line in lines
        if line.startswith("Controlled aggregate budget starting: ")))
    assert started["elapsed_basis"] == "host_monotonic" and started["budget_seconds"] == 1200
    case_start = json.loads(next(line.removeprefix("Controlled aggregate case starting: ") for line in lines
        if line.startswith("Controlled aggregate case starting: ")))
    assert case_start["source_ids"] == sources
    failures = [json.loads(line.removeprefix("Controlled aggregate before-proof failure: ")) for line in lines
        if line.startswith("Controlled aggregate before-proof failure: ")]
    assert failures == [{"case": case.name, "last_completed_readiness": {
        "requested_count": 20, "present_count": 19, "missing_ids": [missing], "capture_pending": 7,
        "global_elapsed_seconds": 1, "global_remaining_seconds": 1199,
        "local_elapsed_seconds": 1, "local_remaining_seconds": 119, "limiting_scope": "local"}}]


def test_actual_load_rejects_expiry_after_compose_stop_before_any_seed(monkeypatch, tmp_path):
    _performance, clock = performance_time(monkeypatch)
    calls = []

    def command(args, *, env, timeout):
        calls.append(args)
        assert env == {"AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json")} and timeout == 1200
        clock.now = 1200
        return ""

    with pytest.raises(RuntimeError, match="profile budget"):
        run_aggregate_performance(["compose"], {"AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json")},
            False, tmp_path, command)
    assert calls == [["compose", "stop", "aggregate-worker"]]
    assert not list(tmp_path.iterdir())


def test_actual_load_checks_expiry_before_final_success_even_after_last_selection(monkeypatch, tmp_path):
    performance, clock = performance_time(monkeypatch)

    def finished_cases():
        clock.now = 1200
        return []

    monkeypatch.setattr(performance, "load_cases", finished_cases)
    with pytest.raises(RuntimeError, match="profile budget"):
        run_aggregate_performance([], {"AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json")}, False, tmp_path,
            lambda *_args, **_kwargs: pytest.fail("No useful work remains"))


def clock_module():
    path = Path(__file__).resolve().parents[1] / "backend/tests/aggregate_clock_worker.py"
    spec = importlib.util.spec_from_file_location("aggregate_clock_worker", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def maintenance_budget_setup(monkeypatch):
    from app.aggregation.event_time import EventTimeBudgetError
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )

    module = clock_module()
    metrics = module.MaintenanceMetrics()
    monkeypatch.setattr(EventTimeBudgetError, "__init__", EventTimeBudgetError.__init__)
    for method in ("maintain", "_save"):
        monkeypatch.setattr(AggregateRetentionService, method, getattr(AggregateRetentionService, method))
    for method in ("compact_memberships", "compact_outcomes"):
        monkeypatch.setattr(SqlAlchemyAggregateRepository, method, getattr(SqlAlchemyAggregateRepository, method))

    def install(maintain, session=None):
        monkeypatch.setattr(AggregateRetentionService, "maintain", maintain)
        module.install_maintenance_metrics(metrics)
        return AggregateRetentionService(SqlAlchemyAggregateRepository(session), SimpleNamespace(rollback=lambda: None))

    return SimpleNamespace(module=module, metrics=metrics, install=install, error=EventTimeBudgetError)


def test_maintenance_budget_keeps_the_swallowed_first_factor_and_original_exceptions(
        maintenance_budget_setup, monkeypatch):
    from app.aggregation.event_time import OperationBudget

    setup = maintenance_budget_setup
    initialized, raised = [], []
    original_init = setup.error.__init__

    def initialize(error, *args, **kwargs):
        initialized.append((error, args, kwargs))
        return original_init(error, *args, **kwargs)

    monkeypatch.setattr(setup.error, "__init__", initialize)
    result = {"status": "error", "reason": "retention_budget"}

    def maintain(service, identity):
        service.repository.maintenance_budget = lambda _budget: None
        try:
            service.repository.retention_dependencies(identity, None)
        except setup.error as error:
            raised.append(error)  # The protection-plan catch swallowed this first one.
        try:
            OperationBudget(0, clock=lambda: 0).check()
        except setup.error as error:
            raised.append(error)
        service.uow.rollback()
        return result

    service = setup.install(maintain, SimpleNamespace(scalar=lambda _query: 256 * 1024**2))
    rollback = service.uow.rollback
    assert service.maintain(UUID(int=1)) is result
    assert [error.factor for error in raised] == ["protection_bytes", "operation_seconds"]
    assert initialized == [(raised[0], ("protection_bytes",), {}), (raised[1], ("operation_seconds",), {})]
    progress = setup.metrics.snapshot()
    event, = progress["budget_errors"]
    assert event["aggregate_id"] == str(UUID(int=1)) and event["factor"] == "protection_bytes"
    assert event["turn_total_seconds"] is None and event["sequence"] == 1
    assert any(frame["module"] == "app.repositories.sqlalchemy.aggregate_retention"
        and frame["function"] == "retention_dependencies" for frame in event["stack"])
    assert 0 < len(event["stack"]) <= 8
    assert all(set(frame) == {"module", "function", "line"} and frame["line"] > 0 for frame in event["stack"])
    assert progress["sequence"] == 0 and progress["spans"] == []
    assert setup.metrics.current.get() is setup.metrics.completion.get() is None
    assert service.uow.rollback is rollback


def test_maintenance_budget_context_excludes_a_concurrent_foreground_error_and_resets(maintenance_budget_setup):
    setup = maintenance_budget_setup
    barrier = Barrier(2)
    result = {"status": "error"}

    def maintain(_service, _identity):
        barrier.wait(timeout=2)
        barrier.wait(timeout=2)
        try:
            raise setup.error("protection_bytes")
        except setup.error:
            return result

    service = setup.install(maintain)

    def foreground():
        barrier.wait(timeout=2)
        assert setup.metrics.current.get() is setup.metrics.completion.get() is None
        error = setup.error("candidate_windows")
        barrier.wait(timeout=2)
        return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        maintenance = executor.submit(service.maintain, UUID(int=1))
        unrelated = executor.submit(foreground)
        assert maintenance.result(timeout=5) is result
        assert unrelated.result(timeout=5).factor == "candidate_windows"
    setup.error("operation_seconds")  # Outside maintain, on the caller thread too.
    events = setup.metrics.snapshot()["budget_errors"]
    assert len(events) == 1 and events[0]["factor"] == "protection_bytes"
    assert setup.metrics.current.get() is setup.metrics.completion.get() is None


@pytest.mark.parametrize("failure", ["capture", "record"])
@pytest.mark.parametrize("diagnostic_error", [RuntimeError, MemoryError])
def test_budget_diagnostic_failure_cannot_replace_the_original_exception_or_leak_context(
        maintenance_budget_setup, monkeypatch, failure, diagnostic_error):
    setup = maintenance_budget_setup
    raised, initialized = [], []
    original_init = setup.error.__init__

    def initialize(error, *args, **kwargs):
        initialized.append((error, args, kwargs))
        return original_init(error, *args, **kwargs)

    monkeypatch.setattr(setup.error, "__init__", initialize)

    def maintain(_service, _identity):
        error = setup.error(factor="operation_seconds")
        raised.append(error)
        raise error

    def broken(*_args, **_kwargs):
        raise diagnostic_error("private diagnostic failure")

    service = setup.install(maintain)
    monkeypatch.setattr(setup.metrics, "capture_budget_error" if failure == "capture" else "record_budget_error", broken)
    rollback = service.uow.rollback
    with pytest.raises(setup.error) as caught:
        service.maintain(UUID(int=1))
    assert caught.value is raised[0] and caught.value.args == ("operation_seconds",)
    assert initialized == [(raised[0], (), {"factor": "operation_seconds"})]
    assert caught.value.factor == "operation_seconds" and service.uow.rollback is rollback
    assert setup.metrics.current.get() is setup.metrics.completion.get() is None


def test_budget_constructor_does_not_reenter_the_metrics_lock(maintenance_budget_setup):
    from app.aggregation.event_time import OperationBudget

    setup = maintenance_budget_setup

    class GuardLock:
        held = False
        reentries = 0

        def __enter__(self):
            if self.held:
                self.reentries += 1
                raise RuntimeError("non-reentrant lock")
            self.held = True

        def __exit__(self, *_args):
            self.held = False

    setup.metrics.lock = GuardLock()

    def maintain(_service, _identity):
        with setup.metrics.lock:
            try:
                OperationBudget(0, clock=lambda: 0).check()
            except setup.error:
                return "unchanged"

    service = setup.install(maintain)
    assert service.maintain(UUID(int=1)) == "unchanged"
    event, = setup.metrics.snapshot()["budget_errors"]
    assert event["factor"] == "operation_seconds" and event["stack"]
    assert setup.metrics.lock.reentries == 0


def test_budget_events_keep_the_last_ten_turns_without_changing_physical_progress():
    metrics = clock_module().MaintenanceMetrics()
    for index in range(15):
        metrics.record_budget_error({"aggregate_id": str(UUID(int=index + 1)),
            "factor": "operation_seconds", "stack": []})
    progress = metrics.snapshot()
    assert [row["sequence"] for row in progress["budget_errors"]] == list(range(6, 16))
    assert progress["budget_errors"][-1]["aggregate_id"] == str(UUID(int=15))
    assert progress["budget_error_sequence"] == 15 and progress["dropped_budget_errors"] == 5
    assert progress["sequence"] == 0 and progress["spans"] == [] and progress["phases"] == {}


def test_import_and_service_factories_do_not_install_or_stack_global_error_hooks(monkeypatch):
    from app.aggregation.event_time import EventTimeBudgetError
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )

    original = EventTimeBudgetError.__init__
    module = clock_module()
    assert EventTimeBudgetError.__init__ is original
    with monkeypatch.context() as scoped:
        scoped.setattr(EventTimeBudgetError, "__init__", original)
        for method in ("maintain", "_save"):
            scoped.setattr(AggregateRetentionService, method, getattr(AggregateRetentionService, method))
        for method in ("compact_memberships", "compact_outcomes"):
            scoped.setattr(SqlAlchemyAggregateRepository, method, getattr(SqlAlchemyAggregateRepository, method))
        module.install_maintenance_metrics(module.MaintenanceMetrics())
        installed = EventTimeBudgetError.__init__
        assert installed is not original
        for _ in range(3):
            AggregateRetentionService(None, None)
            clock_module()
            assert EventTimeBudgetError.__init__ is installed
    assert EventTimeBudgetError.__init__ is original


def load_module():
    path = Path(__file__).resolve().parents[1] / "backend/tests/aggregate_load_runtime.py"
    spec = importlib.util.spec_from_file_location("aggregate_load_runtime", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def arguments(**changes):
    return SimpleNamespace(**{"aggregate_performance": True, "profile": "smoke-development",
        "aggregate_only": False, "detector_only": False, "browser_only": False,
        "rcf": False, "wal": False, "pytest": None, "browser_spec": None, **changes})


@pytest.mark.parametrize("profile", ["smoke-development", "smoke-production"])
def test_load_is_an_explicit_selection_of_the_same_two_profiles(profile):
    validate_performance_arguments(arguments(profile=profile))


@pytest.mark.parametrize("changes", [{"profile": "backend-db"}, {"profile": "end-to-end"},
    {"aggregate_only": True}, {"detector_only": True}, {"browser_only": True},
    {"rcf": True}, {"wal": True}, {"pytest": []}, {"browser_spec": ["detectors.live.spec.ts"]}])
def test_load_rejects_incompatible_selections_before_preparation(changes):
    with pytest.raises(ValueError):
        validate_performance_arguments(arguments(**changes))


@pytest.mark.parametrize("selection", [["frontend", "--aggregate-performance"],
    ["smoke-development", "--aggregate-performance", "--aggregate-only"]])
def test_actual_runner_rejects_invalid_load_before_docker_or_run_directory(tmp_path, monkeypatch, selection):
    from scripts import agent_test_profiles as profiles

    monkeypatch.setattr(profiles, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["agent_test_profiles.py", *selection])
    monkeypatch.setattr(profiles, "inventory", lambda *_: pytest.fail("Docker inventory must not run"))
    with pytest.raises(SystemExit) as caught:
        profiles.main()
    assert caught.value.code == 2
    assert not (tmp_path / ".test_runs").exists()


def test_actual_performance_branch_cleans_its_files_after_prepare_failure(tmp_path, monkeypatch):
    from scripts import agent_test_profiles as profiles

    before = {kind: [] for kind in ("containers", "networks", "volumes", "images", "builders", "default_cache")}
    project = "diploma_t21_controlled_load"

    def prepare(_run, _profile, *, include_oidc):
        assert include_oidc is False
        return ({"POSTGRES_PASSWORD": "synthetic", "POSTGRES_BIND": "127.0.0.1:1:5432", "KAFKA_TEST_BOOTSTRAP_SERVERS": "127.0.0.1:2",
            "PROXY_BIND": "127.0.0.1:3:8080"}, ["docker", "compose", "-p", project], project)

    def fail_config(*_args):
        raise RuntimeError("synthetic controlled preparation failure")

    monkeypatch.setattr(profiles, "ROOT", tmp_path)
    monkeypatch.setattr(profiles, "inventory", lambda *_: before)
    monkeypatch.setattr(profiles, "prepare", prepare)
    monkeypatch.setattr(profiles, "command", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(profiles, "check_config", fail_config)
    monkeypatch.setattr(profiles.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=""))
    monkeypatch.setattr(sys, "argv", ["agent_test_profiles.py", "smoke-development", "--aggregate-performance"])
    with pytest.raises(RuntimeError, match="controlled preparation failure"):
        profiles.main()
    assert list((tmp_path / ".test_runs/runs").iterdir()) == []


def test_matrix_keeps_per_source_density_all_stages_and_substantive_rule_mix():
    cases = load_cases()
    assert [case.aggregates for case in cases] == [1, 10, 20, 30, 30]
    assert {case.events_per_source_window for case in cases} == {2, 1000, 5000}
    assert any(case.aggregates == 30 and case.sources == 1 and case.events_per_source_window == 5000 for case in cases)
    assert any(case.aggregates == 30 and case.sources == 3 and case.events_per_source_window == 5000 for case in cases)
    assert cases[2].shifted and cases[-1].shifted
    assert load_rule(0)["features"][1]["field"] == "process.pid"
    assert load_rule(1)["auxiliary"] and load_rule(1)["where"]["op"] == "all"
    assert {feature["op"] for feature in load_rule(2)["features"]} >= {"median", "percentile", "mode_count", "variance"}


def test_controlled_load_rules_use_the_real_catalog_grammar_and_unchanged_limits():
    from app.aggregation.rules import AggregateRule, Limits, compile_rule
    from app.aggregation.time_policy import execution_policy
    from app.ecs import PackagedEcsCatalog

    catalog, limits = PackagedEcsCatalog.load(), Limits()
    compiled = [compile_rule(AggregateRule.model_validate(load_rule(index)), catalog, limits) for index in range(3)]
    assert len({rule.sha256 for rule in compiled}) == 3
    assert limits.preview_rows == 100000 and limits.total_memory_mb == 256
    assert limits.work_mem_mb == 32 and limits.array_elements == 4096
    assert execution_policy(300)["hard_budget_seconds"] == 30


def test_latency_summary_uses_all_real_nonnegative_samples_and_retains_maximum():
    assert distribution([1, 2, 3, 4, 10]) == {"samples": 5, "p50": 3, "p95": 10, "max": 10}
    for values in ([], [-1], [float("nan")], [float("inf")]):
        with pytest.raises(ValueError):
            distribution(values)


def test_commit_measurement_is_independent_of_host_container_and_virtual_utc_offsets():
    step = {"completed_at": "1900-01-01T00:00:00Z", "virtual_deadline": "9999-01-01T00:00:00Z",
        "commit_observed_monotonic": 103.25}
    assert commit_delay_upper_bound(step, 100.0) == 3.25
    with pytest.raises(ValueError, match="preceded"):
        commit_delay_upper_bound(step, 104.0)
    with pytest.raises(ValueError, match="real monotonic"):
        commit_delay_upper_bound({**step, "commit_observed_monotonic": float("nan")}, 100.0)


def test_maintenance_overlap_counts_real_committed_rows_and_distinguishes_the_history_target():
    def span(closure, start, sql_end, commit, rows):
        return {"closure_id": closure, "phase": "memberships", "started_monotonic": start,
            "sql_completed_monotonic": sql_end, "committed_monotonic": commit, "rows_changed": rows}

    intervals = [{"released": 100.0, "commit_started": 102.9, "commit_ack": 103.0},
        {"released": 104.0, "commit_started": 104.9, "commit_ack": 105.0}]
    spans = [span("small", 100.5, 101, 101.1, 2), span("heavy", 102, 102.5, 102.8, 5000),
        span("heavy", 105, 105.1, 105.2, 1)]
    result = maintenance_overlap(spans, intervals, {"heavy"})
    assert result["normal_gc"] == {"contained_online_samples": 1, "contained_committed_spans": 2,
        "contained_phases": {"memberships": {"committed_spans": 2, "rows_changed": 5002}},
        "observed_intersection_online_samples": 1, "observed_intersection_committed_spans": 2}
    assert result["target_history"] == {"contained_online_samples": 1, "contained_committed_spans": 1,
        "contained_phases": {"memberships": {"committed_spans": 1, "rows_changed": 5000}},
        "observed_intersection_online_samples": 1, "observed_intersection_committed_spans": 1}
    # A later observer upper bound cannot extend the actual online commit for
    # overlap evidence. Touching endpoints and entirely later GC do not count.
    assert maintenance_overlap(spans[2:], intervals, {"heavy"})["target_history"]["contained_online_samples"] == 0
    # ACK observations bound commit from above. A GC commit after the point's
    # pre-commit start is only a possible intersection and cannot satisfy proof.
    ack_only = maintenance_overlap([span("heavy", 102.95, 102.96, 102.97, 1)], intervals, {"heavy"})
    assert ack_only["target_history"]["contained_online_samples"] == 0
    assert ack_only["target_history"]["observed_intersection_online_samples"] == 1
    deduplicated = [{**spans[1], "sequence": 7}] * 2
    assert maintenance_overlap(deduplicated, intervals, {"heavy"})["target_history"]["contained_committed_spans"] == 1
    for invalid in ({**spans[0], "rows_changed": 0}, {**spans[0], "committed_monotonic": 99},
            {**spans[0], "started_monotonic": float("nan")}):
        with pytest.raises(ValueError):
            maintenance_overlap([invalid], intervals, {"heavy"})


def test_contained_last_single_row_of_10001_target_cannot_satisfy_heavy_overlap():
    spans = [{"closure_id": "heavy", "phase": "memberships", "rows_changed": rows,
        "started_monotonic": start, "sql_completed_monotonic": start + .1,
        "committed_monotonic": start + .2} for rows, start in ((5000, 98), (5000, 99), (1, 101))]
    interval = {"released": 100, "commit_started": 103, "commit_ack": 103.1}
    result = maintenance_overlap(spans, [interval], {"heavy"})
    assert result["target_history"]["contained_online_samples"] == 1
    assert result["target_history_heavy"]["contained_online_samples"] == 0
    assert result["target_history_heavy"]["contained_phases"] == {}


def test_release_probe_runs_inside_the_actual_worker_before_the_atomic_file_write():
    calls = []

    def command(args, **kwargs):
        calls.append((args, kwargs))
        return "12345.125\n"

    assert container_monotonic(["compose"], {"owned": "yes"}, command) == 12345.125
    args, kwargs = calls[0]
    assert args[:5] == ["compose", "exec", "-T", "aggregate-worker", "python"]
    assert args[-1] == "import time; print(time.monotonic())"
    assert kwargs == {"env": {"owned": "yes"}, "capture": True, "timeout": 5}


def test_repeated_case_setup_uses_distinct_real_connection_identities(monkeypatch):
    from app.models import KafkaConnection

    module = load_module()
    connections = []

    def add_all(items):
        connections.extend(item for item in items if isinstance(item, KafkaConnection))

    session = SimpleNamespace(add_all=add_all, add=lambda _: None, flush=lambda: None)
    database = SimpleNamespace(session_factory=SimpleNamespace(begin=lambda: nullcontext(session)))
    monkeypatch.setenv("AGGREGATE_LOAD_USER", "synthetic-load-user")
    monkeypatch.setenv("AGGREGATE_LOAD_PASSWORD", "synthetic-load-password")
    monkeypatch.setattr(module, "hash_password", lambda _: "synthetic-hash")
    for _ in range(5):
        seeded = module.setup(database, {"sources": 1})
        assert seeded["heavy_history_rows"] == 10001 and seeded["history_windows"] == 520
    assert len({connection.cluster_identity for connection in connections}) == 5
    assert all(str(connection.id) in connection.cluster_identity for connection in connections)


def history_response(status, aggregate, identity, *, reason="automatic"):
    metadata = {"window_index": 8, "origin": "initial_history", "time_basis": "event_time"}
    body = ({"id": identity, "aggregate_id": aggregate, "payload_state": "present",
        "event_count": 10001, **metadata} if status == 200 else
        {"code": "aggregate_point_cleared", "details": {**metadata, "origin_point_id": identity,
            "clear_reason": reason, "clear_policy_version": "model_history_v1",
            "cleared_at": "2026-11-16T00:00:00Z"}})
    return SimpleNamespace(status_code=status, json=lambda: body)


def test_gc_wave_requires_a_fresh_verified_rest_transition_without_pausing_work():
    calls = []
    sequence = iter([history_response(410, "already", "old-a"), history_response(200, "live", "old-b"),
        history_response(200, "live", "old-b"), history_response(410, "live", "old-b")])

    def get(path, *, timeout):
        assert 0 < timeout <= 10
        calls.append(path)
        return next(sequence)

    result = wait_for_history_clear(SimpleNamespace(get=get), {"already": "old-a", "live": "old-b"}, 10001,
        float("inf"))
    assert result == {"aggregate_id": "live", "point_id": "old-b", "history_rows": 10001,
        "observed_transition": "200_to_automatic_410"}
    assert calls == ["/api/v1/aggregates/already/points/old-a", *["/api/v1/aggregates/live/points/old-b"] * 3]


def test_history_observer_can_use_another_fresh_transition_while_first_target_stays_present(monkeypatch):
    _performance, clock = performance_time(monkeypatch)
    calls, reads = [], {"slow": 0, "fast": 0}

    def get(path, *, timeout):
        assert 0 < timeout <= 10
        aggregate = path.split("/")[4]
        calls.append(path)
        reads[aggregate] += 1
        clock.now += 1
        status = 410 if aggregate == "fast" and reads[aggregate] >= 2 else 200
        return history_response(status, aggregate, "old-" + aggregate)

    result = wait_for_history_clear(SimpleNamespace(get=get), {"slow": "old-slow", "fast": "old-fast"},
        10001, 1200)
    assert result == {"aggregate_id": "fast", "point_id": "old-fast", "history_rows": 10001,
        "observed_transition": "200_to_automatic_410"}
    assert calls == [f"/api/v1/aggregates/{identity}/points/old-{identity}"
        for identity in ("slow", "fast", "slow", "fast")]
    assert clock.now == 4


def test_history_observer_cannot_pair_one_targets_200_with_another_targets_initial_410(monkeypatch):
    _performance, clock = performance_time(monkeypatch)

    def get(path, *, timeout):
        assert timeout == min(10, 120 - clock.now)
        aggregate = path.split("/")[4]
        clock.now += 1
        return history_response(200 if aggregate == "present" else 410, aggregate, "old-" + aggregate)

    with pytest.raises(AssertionError, match="History transition observation timed out"):
        wait_for_history_clear(SimpleNamespace(get=get), {"present": "old-present", "cleared": "old-cleared"},
            10001, 1200)
    assert clock.now == 120


@pytest.mark.parametrize("overall,error", [(100, RuntimeError), (1200, AssertionError)])
def test_history_transition_uses_one_shared_deadline_across_target_polls(monkeypatch, overall, error):
    _performance, clock = performance_time(monkeypatch)
    deadline, timeouts = min(overall, 120), []

    def get(path, *, timeout):
        assert timeout == min(10, deadline - clock.now)
        timeouts.append(timeout)
        aggregate = path.split("/")[4]
        clock.now += 1
        # A genuine second-target transition returned exactly at either deadline
        # cannot release a wave. No target or poll gets a fresh local budget.
        status = 410 if aggregate == "second" and clock.now >= deadline else 200
        return history_response(status, aggregate, "old-" + aggregate)

    with pytest.raises(error):
        wait_for_history_clear(SimpleNamespace(get=get), {"first": "old-first", "second": "old-second"},
            10001, overall)
    assert clock.now == deadline and timeouts[-1] == 1
    assert len(timeouts) == deadline


def test_already_cleared_history_or_wrong_cause_cannot_trigger_a_wave():
    client = SimpleNamespace(get=lambda _, **_kwargs: history_response(410, "a", "point"))
    with pytest.raises(AssertionError, match="already cleared"):
        wait_for_history_clear(client, {"a": "point"}, 10001, float("inf"))
    sequence = iter([history_response(200, "a", "point"), history_response(410, "a", "point", reason="manual")])
    with pytest.raises(AssertionError):
        wait_for_history_clear(SimpleNamespace(get=lambda _, **_kwargs: next(sequence)),
            {"a": "point"}, 10001, float("inf"))


@pytest.mark.parametrize("overall,first_at,late_at,error", [(100, 99, 100, RuntimeError),
    (1200, 119, 120, AssertionError)])
def test_rest_transition_cannot_release_a_wave_after_either_deadline(monkeypatch, overall, first_at, late_at, error):
    _performance, clock = performance_time(monkeypatch)
    timeouts = []

    def get(_path, *, timeout):
        timeouts.append(timeout)
        status = 200 if len(timeouts) == 1 else 410
        clock.now = first_at if status == 200 else late_at
        return history_response(status, "a", "point")

    with pytest.raises(error):
        wait_for_history_clear(SimpleNamespace(get=get), {"a": "point"}, 10001, overall)
    assert timeouts == [10, 1]


def test_late_history_helper_keeps_the_global_deadline_without_inventing_a_new_elapsed_origin(monkeypatch, capsys):
    _performance, clock = performance_time(monkeypatch)
    clock.now = 1190
    timeouts = []

    def get(_path, *, timeout):
        timeouts.append(timeout)
        clock.now = 1200
        return history_response(410, "a", "point")

    with pytest.raises(RuntimeError, match="profile budget"):
        wait_for_history_clear(SimpleNamespace(get=get), {"a": "point"}, 10001, 1200)
    assert timeouts == [10] and clock.now == 1200
    line, = capsys.readouterr().out.splitlines()
    report = json.loads(line.removeprefix("Controlled aggregate bounded wait failure: "))
    assert report["limiting_scope"] == "global" and report["global_remaining_seconds"] == 0
    assert report["local_elapsed_seconds"] == 10 and report["local_remaining_seconds"] == 110
    assert "global_elapsed_seconds" not in report


def test_online_interval_report_keeps_every_canonical_identity_and_all_real_clock_bounds():
    intervals = [{"aggregate_id": str(UUID(int=index % 30 + 1)), "window_index": 520 + index // 30,
        "point_id": str(UUID(int=index + 100)), "released": 100.0 + index // 30,
        "commit_started": 100.1 + index // 30, "commit_ack": 100.2 + index // 30,
        "commit_observed": 100.3 + index // 30} for index in range(90)]
    assert bounded_online_intervals(intervals, 90) is intervals
    assert len(json.dumps(intervals).encode("utf-8")) < 128 * 1024
    for invalid in (intervals[:-1], [intervals[0]] * 90,
            [{**intervals[0], "raw": "not-permitted"}, *intervals[1:]],
            [{**intervals[0], "commit_ack": 99}, *intervals[1:]]):
        with pytest.raises(AssertionError):
            bounded_online_intervals(invalid, 90)


@pytest.mark.parametrize("shift", [0, -1, -2])
def test_frozen_bootstrap_keeps_520_history_windows_after_more_than_300_seconds_of_preparation(monkeypatch, shift):
    from app.aggregation.event_time import online_start
    from app.models import User
    from app.schemas.aggregates import (
        AggregatePreviewRequest,
        CreateAggregateRequest,
        ValidateRequest,
    )

    module = load_module()
    started = datetime(2026, 11, 16, tzinfo=UTC)
    bootstrap, anchor = module.load_timeline(started)
    physical = [started + timedelta(seconds=900)]
    actor = User(id=UUID(int=1), username="owned-load-user",
        role_id=UUID("00000000-0000-4000-8000-000000000001"))
    calls, policy_times = [], []
    repository = SimpleNamespace(now=lambda: physical[0],
        add_retention_state=lambda _aggregate, now: policy_times.append(now))

    def validate(body):
        assert isinstance(body, ValidateRequest)
        assert body.schedule.anchor_utc == anchor + timedelta(seconds=shift)
        calls.append("validate")
        return SimpleNamespace(rule_hash="a" * 64)

    def preview(body):
        assert isinstance(body, AggregatePreviewRequest) and body.time_basis == "event_time"
        calls.append("preview")

    def create(body, actual_actor, key):
        assert isinstance(body, CreateAggregateRequest) and body.time_basis == "event_time"
        assert actual_actor == actor.id and UUID(key)
        calls.append("create")
        index = online_start(body.schedule.anchor_utc, body.schedule.duration_seconds, repository.now())
        repository.add_retention_state(SimpleNamespace(created_at=repository.now()), repository.now())
        return SimpleNamespace(online_start_index=index, model_dump=lambda **_: {"online_start_index": index})

    service = SimpleNamespace(repository=repository, validate=validate, preview=preview, create=create)
    monkeypatch.setattr("app.aggregate_composition.build_aggregate_service", lambda *_: service)
    monkeypatch.setattr("app.ecs.PackagedEcsCatalog.load", lambda: None)
    monkeypatch.setenv("AGGREGATE_LOAD_USER", actor.username)
    database = SimpleNamespace(session_factory=lambda: nullcontext(SimpleNamespace(get=lambda *_: actor)))
    payload = {"actor": str(actor.id), "source": str(UUID(int=2)), "rule": load_rule(0),
        "name": "Controlled", "anchor": anchor.isoformat(), "clock_at": bootstrap.isoformat(), "anchor_shift": shift}
    assert module.create(database, payload)["card"]["online_start_index"] == 520
    assert calls == ["validate", "preview", "create"]
    assert policy_times == [physical[0]] and policy_times[0] != bootstrap


def test_shifted_deadline_grids_keep_the_exact_same_source_events_in_each_300_second_window():
    from app.aggregation.event_time import bounds, close_deadline

    bootstrap, anchor = load_module().load_timeline(datetime(2026, 11, 16, tzinfo=UTC))
    for wave in range(3):
        index, event_at, simultaneous_close = wave_times(anchor, wave)
        deadlines = set()
        for shift in (0, -1, -2):
            start, end = bounds(anchor + timedelta(seconds=shift), 300, index)
            assert start <= event_at < end
            assert end - start == timedelta(seconds=300)
            deadlines.add(close_deadline(end, 300))
        assert deadlines == {simultaneous_close - timedelta(seconds=shift) for shift in range(3)}
    assert wave_times(anchor, 0)[1] == bootstrap


def test_incremental_waves_do_not_seed_future_exclusions_into_earlier_snapshots():
    from app.aggregation.event_time import event_timestamp

    bootstrap, anchor = load_module().load_timeline(datetime(2026, 11, 16, tzinfo=UTC))
    reference = bootstrap
    for wave in range(3):
        index, event_at, deadline = wave_times(anchor, wave)
        assert index == 520 + wave
        assert event_timestamp({"@timestamp": event_at.isoformat()}, [], True, reference) == (event_at, None)
        assert deadline - event_at == timedelta(seconds=600)
        reference = deadline
    # This was the rejected pre-seed: it is genuinely future relative to the
    # initial reference and must remain excluded by the unchanged classifier.
    assert event_timestamp({"@timestamp": wave_times(anchor, 2)[1].isoformat()}, [], True, bootstrap)[1] == "event_time_future"


def test_test_only_overlay_mounts_only_owned_files_readonly_and_preserves_resources(tmp_path):
    overlay, clock = controlled_clock_overlay(tmp_path)
    assert clock.is_relative_to(tmp_path)
    body = yaml.safe_load(overlay.read_text(encoding="utf-8"))
    assert set(body["services"]) == {"aggregate-worker", "detector-worker"}
    for kind in ("aggregate", "detector"):
        service = body["services"][kind + "-worker"]
        assert service["command"] == ["python", "/tmp/aggregate-clock-worker.py", kind]
        assert not {"mem_limit", "cpus", "ports", "build"} & service.keys()
        for mount in service["volumes"]:
            assert mount["read_only"] and Path(mount["source"]).is_relative_to(tmp_path)
    assert json.loads(clock.read_text(encoding="utf-8")) == {"aggregates": {}}


def test_clock_targets_are_explicit_and_new_moments_replace_only_the_owned_atomic_file(tmp_path):
    path = tmp_path / "clock.json"
    identity, other = UUID(int=1), UUID(int=2)
    write_clock(path, {str(identity): "2026-11-16T12:00:00+05:00"})
    clock = clock_module().ScopedClock(path)
    assert clock.now(identity) == datetime(2026, 11, 16, 7, tzinfo=UTC)
    assert clock.now(other) is None
    write_clock(path, {str(identity): "2026-11-16T13:00:00+05:00"})
    assert clock.now(identity) == datetime(2026, 11, 16, 8, tzinfo=UTC)
    assert not path.with_suffix(".next").exists()


@pytest.mark.parametrize("body", [
    {"all": "2026-11-16T00:00:00Z"},
    {"aggregates": {"not-a-uuid": "2026-11-16T00:00:00Z"}},
    {"aggregates": {str(UUID(int=1)): "2026-11-16T00:00:00"}},
    {"aggregates": {str(UUID(int=i)): "2026-11-16T00:00:00Z" for i in range(31)}},
])
def test_unknown_or_unbounded_clock_cannot_silently_advance_any_target(tmp_path, body):
    path = tmp_path / "clock.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError):
        clock_module().ScopedClock(path).now(UUID(int=1))


def test_composed_clock_keeps_other_aggregate_and_real_elapsed_clocks_unchanged(tmp_path, monkeypatch):
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )

    identity, other = UUID(int=1), UUID(int=2)
    path = tmp_path / "clock.json"
    virtual, real = datetime(2026, 11, 17, tzinfo=UTC), datetime(2026, 11, 16, tzinfo=UTC)
    write_clock(path, {str(identity): virtual.isoformat()})
    monkeypatch.setattr(SqlAlchemyAggregateRepository, "get", lambda _self, target: target)
    monkeypatch.setattr(SqlAlchemyAggregateRepository, "now", lambda _self: real)
    monkeypatch.setattr(SqlAlchemyDetectorRepository, "aggregate_context", lambda *_args, **_kwargs: {"now": real})
    monkeypatch.setattr(AggregateRetentionService, "maintain", AggregateRetentionService.maintain)
    module = clock_module()
    module.install_clock(module.ScopedClock(path))
    aggregate = SqlAlchemyAggregateRepository(None)
    detector = SqlAlchemyDetectorRepository(None)
    assert aggregate.get(identity) == identity and aggregate.now() == virtual
    assert detector.aggregate_context(identity)["now"] == virtual
    assert aggregate.get(other) == other and aggregate.now() == real
    assert detector.aggregate_context(other)["now"] == real


def test_maintenance_uses_physical_clock_and_restores_the_previous_target_even_after_error(tmp_path, monkeypatch):
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )

    identity, previous = UUID(int=1), UUID(int=2)
    path = tmp_path / "clock.json"
    virtual, real = datetime(2026, 11, 17, tzinfo=UTC), datetime(2026, 11, 16, tzinfo=UTC)
    write_clock(path, {str(identity): virtual.isoformat()})
    monkeypatch.setattr(SqlAlchemyAggregateRepository, "get", lambda _self, target: target)
    monkeypatch.setattr(SqlAlchemyAggregateRepository, "now", lambda _self: real)
    monkeypatch.setattr(SqlAlchemyDetectorRepository, "aggregate_context", lambda *_args, **_kwargs: {"now": real})
    observed = []

    def maintain(service, target):
        service.repository.get(target)
        observed.append(service.repository.now())
        if service.fail:
            raise RuntimeError("synthetic failed maintenance")
        return observed[-1]

    monkeypatch.setattr(AggregateRetentionService, "maintain", maintain)
    module = clock_module()
    clock = module.ScopedClock(path)
    module.install_clock(clock)
    clock.target.set(previous)
    service = AggregateRetentionService(SqlAlchemyAggregateRepository(None), None)
    service.fail = False
    assert service.maintain(identity) == real
    assert clock.target.get() == previous and clock.now(identity) == virtual
    service.fail = True
    with pytest.raises(RuntimeError, match="failed maintenance"):
        service.maintain(identity)
    assert observed == [real, real]
    assert clock.target.get() == previous and clock.now(identity) == virtual


def test_window_and_maintenance_clock_domains_are_thread_local(tmp_path):
    identity = UUID(int=1)
    path = tmp_path / "clock.json"
    virtual = datetime(2026, 11, 17, tzinfo=UTC)
    write_clock(path, {str(identity): virtual.isoformat()})
    clock, barrier = clock_module().ScopedClock(path), Barrier(2)

    def read(domain):
        with clock.scope(domain):
            clock.target.set(identity)
            barrier.wait(timeout=2)
            return clock.now()

    with ThreadPoolExecutor(max_workers=2) as executor:
        physical = executor.submit(read, "maintenance")
        logical = executor.submit(read, "window")
        assert physical.result(timeout=3) is None
        assert logical.result(timeout=3) == virtual
    assert clock.target.get() is None and clock.domain.get() == "window"


def test_online_metrics_are_bounded_use_real_measurements_and_preserve_unknown_memory(tmp_path, monkeypatch):
    from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_execution import (
        AggregateExecutionService,
    )
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )
    from app.services.implementations.aggregate_worker import AggregateWorker

    module = clock_module()
    resource = SimpleNamespace(RUSAGE_SELF=0, getrusage=lambda _: SimpleNamespace(
        ru_maxrss=128, ru_utime=.3, ru_stime=.2))
    monkeypatch.setitem(sys.modules, "resource", resource)
    original_path = Path

    def owned_path(value):
        return original_path(tmp_path / original_path(value).name)

    monkeypatch.setattr(module, "Path", owned_path)
    # Register restoration before the test-only composition replaces __init__.
    monkeypatch.setattr(AggregateWorker, "__init__", AggregateWorker.__init__)
    monkeypatch.setattr(AggregateWorker, "_execute", AggregateWorker._execute)
    monkeypatch.setattr(EventTimeBudgetError, "__init__", EventTimeBudgetError.__init__)
    monkeypatch.setattr(AggregateExecutionService, "advance", AggregateExecutionService.advance)
    for method in ("maintain", "_save"):
        monkeypatch.setattr(AggregateRetentionService, method, getattr(AggregateRetentionService, method))
    for method in ("compact_memberships", "compact_outcomes"):
        monkeypatch.setattr(SqlAlchemyAggregateRepository, method, getattr(SqlAlchemyAggregateRepository, method))
    original_result = {"status": "error", "reason": "retention_budget"}

    def maintain(_service, _identity):
        try:
            OperationBudget(0, clock=lambda: 0).check()
        except EventTimeBudgetError:
            return original_result

    monkeypatch.setattr(AggregateRetentionService, "maintain", maintain)
    module.install_metrics()
    observed = []
    worker = AggregateWorker(None, None, None, None, observe=observed.append)
    admission = {"active": 2, "queued": 64, "reserved_bytes": 512 * 1024 * 1024}
    for index in range(300):
        worker.observe({"admission": admission, "steps": [{"point_id": str(UUID(int=index)),
            "origin": "online", "queue_seconds": .1, "completed_at": "2026-11-16T00:00:00Z",
            "timings": {"total": .2}}]})
    metrics = json.loads((tmp_path / "aggregate-performance.json").read_text(encoding="utf-8"))
    assert len(metrics["closed"]) == 256 and str(UUID(int=0)) not in metrics["closed"]
    assert metrics["max_active"] == 2 and metrics["max_queued"] == 64
    assert metrics["max_reserved_bytes"] == 512 * 1024 * 1024
    assert metrics["peak_memory_bytes"] is None and metrics["peak_rss_bytes"] == 128 * 1024
    assert metrics["cpu_seconds"] == .5 and len(observed) == 300
    assert metrics["maintenance"] == {"sequence": 0, "dropped_spans": 0, "spans": [],
        "large_spans": [], "dropped_large_spans": 0, "phases": {}, "heavy_membership_ack_rows": {},
        "dropped_heavy_membership_acks": 0, "budget_errors": [], "budget_error_sequence": 0,
        "dropped_budget_errors": 0}
    assert all(step["point_commit_start_monotonic"] is None
        and step["point_commit_ack_monotonic"] is None for step in metrics["closed"].values())
    # The full 300 online writes above stay intact. A later first history-only
    # observation must publish even without online or maintenance progress.
    worker.observe({"admission": admission, "steps": [{"aggregate_id": "history-target", "point_id": "history-point",
        "origin": "initial_history", "timings": {"selection": .12}}]})
    history_metrics = json.loads((tmp_path / "aggregate-performance.json").read_text(encoding="utf-8"))
    assert history_metrics["history_first"]["history-target"]["timings"] == {"selection": .12}
    assert len(history_metrics["closed"]) == 256 and len(observed) == 301
    service = AggregateRetentionService(None, SimpleNamespace(rollback=lambda: None))
    monkeypatch.setattr(worker, "_maintain", lambda identity: service.maintain(identity))
    original_clock = module.time.monotonic
    clock_calls = []

    def clock():
        clock_calls.append(True)
        return original_clock()

    monkeypatch.setattr(module.time, "monotonic", clock)
    outcome = worker._execute(SimpleNamespace(kind="maintenance", aggregate_id=UUID(int=1)))
    assert len(clock_calls) == 2  # Only the original worker's start/completion reads.
    assert outcome["result"] is original_result
    assert set(outcome) == {"result", "started", "completed", "started_at", "completed_at"}
    worker.observe({"admission": admission, "steps": []})
    failed = json.loads((tmp_path / "aggregate-performance.json").read_text(encoding="utf-8"))
    event, = failed["maintenance"]["budget_errors"]
    assert event["factor"] == "operation_seconds"
    assert event["turn_total_seconds"] == max(0, outcome["completed"] - outcome["started"])
    assert failed["maintenance"]["sequence"] == 0 and failed["maintenance"]["spans"] == []
    assert failed["maintenance"]["budget_error_sequence"] == 1
    assert len(failed["closed"]) == 256 and len(observed) == 302


@pytest.mark.parametrize("version", [1, 2])
def test_kernel_cgroup_highwater_uses_the_actual_v1_or_v2_counter_and_limit(tmp_path, monkeypatch, version):
    module = clock_module()
    monkeypatch.setattr(module, "Path", lambda value: tmp_path / value.lstrip("/"))
    directory = tmp_path / "sys/fs/cgroup" / ("memory" if version == 1 else "")
    directory.mkdir(parents=True)
    peak_name = "memory.max_usage_in_bytes" if version == 1 else "memory.peak"
    limit_name = "memory.limit_in_bytes" if version == 1 else "memory.max"
    (directory / peak_name).write_text("310329344\n", encoding="utf-8")
    (directory / limit_name).write_text("805306368\n", encoding="utf-8")
    # Current usage and process RSS are different measures, never fallbacks.
    (directory / "memory.current").write_text("123\n", encoding="utf-8")
    result = module.cgroup_memory()
    assert result["peak_memory_bytes"] == 310329344 and result["memory_limit_bytes"] == 805306368
    assert result["memory_cgroup_version"] == version and result["memory_metric_error"] is None
    assert result["memory_peak_source"].endswith(peak_name)


def test_unknown_or_overbudget_cgroup_cannot_pass_using_a_small_process_rss(tmp_path, monkeypatch):
    module = clock_module()
    monkeypatch.setattr(module, "Path", lambda value: tmp_path / value.lstrip("/"))
    base = {"max_reserved_bytes": 256 * 1024**2, "max_active": 1, "max_queued": 1,
        "peak_rss_bytes": 138600448}
    with pytest.raises(AssertionError):
        assert_worker_resources({**base, **module.cgroup_memory()})
    directory = tmp_path / "sys/fs/cgroup/memory"
    directory.mkdir(parents=True)
    (directory / "memory.max_usage_in_bytes").write_text("900000000", encoding="utf-8")
    (directory / "memory.limit_in_bytes").write_text("805306368", encoding="utf-8")
    with pytest.raises(AssertionError):
        assert_worker_resources({**base, **module.cgroup_memory()})
    (directory / "memory.max_usage_in_bytes").write_text("unreadable", encoding="utf-8")
    invalid = module.cgroup_memory()
    assert invalid["peak_memory_bytes"] is None and invalid["memory_metric_error"] == "cgroup_memory_unreadable"
    with pytest.raises(AssertionError):
        assert_worker_resources({**base, **invalid})


def test_expired_window_diagnostics_are_bounded_readonly_and_do_not_dump_private_payloads(capsys):
    calls = []
    metrics = {"closed": {"point": {"aggregate_id": "aggregate", "point_id": "point",
        "timings": {"total": 11}, "features": "private-vector"}},
        "maintenance": {"sequence": 1, "phases": {"memberships": {"rows_changed": 5000}}},
        "raw": "private-raw"}

    def command(args, **kwargs):
        calls.append((args, kwargs))
        return json.dumps(metrics)

    def get(path, *, timeout):
        calls.append((path, timeout))
        return SimpleNamespace(raise_for_status=lambda: None,
            json=lambda: {"next_index": 521, "lag_seconds": 11, "raw": "private-card"})

    pending_diagnostics(["compose"], {}, command, SimpleNamespace(get=get), load_cases()[1], 0,
        {"a", "b", "c", "d"})
    output = capsys.readouterr().out
    assert "private-" not in output and '"total": 11' in output and '"lag_seconds": 11' in output
    assert len(calls) == 4 and calls[0][1]["timeout"] == 5
    assert all(timeout == .5 for _path, timeout in calls[1:])
    assert calls[0][0][-2:] == ["cat", "/tmp/aggregate-performance.json"]


@pytest.mark.parametrize("failure", [None, "rollback", "commit", "later_error"])
@pytest.mark.parametrize("phase", ["memberships", "outcomes"])
def test_maintenance_metrics_publish_only_committed_original_physical_updates_and_restore_methods(monkeypatch, failure, phase):
    from app.aggregation.event_time import EventTimeBudgetError
    from app.models import AggregateMembership, DetectorOutcome
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )
    from sqlalchemy import update

    module = clock_module()
    metrics, executed, commits, rollbacks = module.MaintenanceMetrics(), [], [], []
    rows = [5000, 1, 0]

    def execute(statement):
        executed.append(statement)
        return SimpleNamespace(rowcount=rows.pop(0))

    session = SimpleNamespace(execute=execute)
    original_execute = session.execute

    def commit():
        commits.append(True)
        if failure == "commit" and len(commits) == 1:
            raise RuntimeError("unconfirmed commit")

    def rollback():
        rollbacks.append(True)

    uow = SimpleNamespace(commit=commit, rollback=rollback)

    def compact(repository, _point_id, _budget, automatic=False):
        assert automatic
        statement = (update(AggregateMembership).values(fingerprint=None) if phase == "memberships"
            else update(DetectorOutcome).values(late=False))
        result = repository.session.execute(statement)
        return UUID(int=result.rowcount) if result.rowcount else None

    def save(service, *_args):
        service.uow.commit()
        return {"status": "compacting"}

    def maintain(service, identity):
        getattr(service.repository, "compact_" + phase)(identity, None, automatic=True)
        if failure == "rollback":
            service.uow.rollback()
        if failure == "commit":
            with pytest.raises(RuntimeError, match="unconfirmed"):
                service._save(None, None)
            service._save(None, None)
        else:
            service._save(None, None)
        if failure == "later_error":
            getattr(service.repository, "compact_" + phase)(identity, None, automatic=True)
            service.uow.rollback()
            # A diagnostic commit must not publish the rolled-back chunk.
            service._save(None, None)
        return {"status": "error" if failure else "compacting"}

    monkeypatch.setattr(AggregateRetentionService, "maintain", maintain)
    monkeypatch.setattr(EventTimeBudgetError, "__init__", EventTimeBudgetError.__init__)
    monkeypatch.setattr(AggregateRetentionService, "_save", save)
    for method in ("compact_memberships", "compact_outcomes"):
        monkeypatch.setattr(SqlAlchemyAggregateRepository, method,
            compact if method == "compact_" + phase else lambda *_args, **_kwargs: None)
    module.install_maintenance_metrics(metrics)
    service = AggregateRetentionService(SqlAlchemyAggregateRepository(session), uow)
    original_rollback = uow.rollback
    service.maintain(UUID(int=1))
    progress = metrics.snapshot()
    assert session.execute is original_execute and uow.rollback is original_rollback
    assert metrics.current.get() is None
    assert len(executed) == (2 if failure == "later_error" else 1)
    assert progress["sequence"] == (0 if failure in {"rollback", "commit"} else 1)
    expected_acks = {str(UUID(int=1)): 5000} if phase == "memberships" and progress["sequence"] else {}
    assert progress["heavy_membership_ack_rows"] == expected_acks
    if progress["spans"]:
        span = progress["spans"][0]
        assert span["rows_changed"] == 5000 and span["closure_id"] == str(UUID(int=1))
        assert span["started_monotonic"] <= span["sql_completed_monotonic"] <= span["committed_monotonic"]
    # Unrelated SQL and calls outside the original maintain scope are not
    # attributed to physical maintenance; the wrapper does not add a query.
    session.execute(update(DetectorOutcome).values(late=False))
    assert len(executed) == (3 if failure == "later_error" else 2)


def test_maintenance_progress_and_online_commit_bounds_are_bounded_without_inventing_unknowns(monkeypatch):
    from app.services.implementations.aggregate_execution import (
        AggregateExecutionService,
    )

    module = clock_module()
    metrics = module.MaintenanceMetrics()
    for index in range(300):
        metrics.publish({"aggregate_id": str(UUID(int=1)), "phases": [{"closure_id": str(UUID(int=index)),
            "phase": "memberships", "rows_changed": 1, "started_monotonic": 1, "sql_completed_monotonic": 2}]})
        metrics.record_point_commit(UUID(int=index), index + .5, index + 1)
    progress = metrics.snapshot()
    assert len(progress["spans"]) == 256 and progress["dropped_spans"] == 44
    assert progress["phases"]["memberships"] == {"committed_spans": 300, "rows_changed": 300}
    assert metrics.point_commit(str(UUID(int=0))) is None
    assert metrics.point_commit(str(UUID(int=299))) == {"started": 299.5, "acknowledged": 300}
    heavy = {"aggregate_id": str(UUID(int=1)), "phases": [{"closure_id": str(UUID(int=1000)),
        "phase": "memberships", "rows_changed": 5000, "started_monotonic": 1, "sql_completed_monotonic": 2}]}
    metrics.publish(heavy)
    for index in range(300):
        metrics.publish({"aggregate_id": str(UUID(int=1)), "phases": [{"closure_id": str(UUID(int=index)),
            "phase": "memberships", "rows_changed": 1, "started_monotonic": 1, "sql_completed_monotonic": 2}]})
    progress = metrics.snapshot()
    assert all(span["rows_changed"] == 1 for span in progress["spans"])
    assert len(progress["large_spans"]) == 1 and progress["large_spans"][0]["rows_changed"] == 5000

    uow = SimpleNamespace(commit=lambda: None)
    original_commit = uow.commit
    identity = UUID(int=1000)

    def advance(service, fail=False):
        service.uow.commit()
        if fail:
            raise RuntimeError("original advance failed")
        return {"state": "closed", "origin": "online", "point_id": identity}

    monkeypatch.setattr(AggregateExecutionService, "advance", advance)
    module.install_online_commit_metrics(metrics)
    service = AggregateExecutionService(None, uow, None, None)
    service.advance()
    assert metrics.point_commit(str(identity)) is not None and uow.commit is original_commit
    with pytest.raises(RuntimeError, match="original advance failed"):
        service.advance(fail=True)
    assert uow.commit is original_commit


def test_heavy_membership_ack_snapshot_includes_both_full_portions_and_final_single_row(monkeypatch):
    _performance, clock = performance_time(monkeypatch)
    metrics = clock_module().MaintenanceMetrics()
    identity = str(UUID(int=1))

    def publish(rows, *, point=identity, phase="memberships"):
        metrics.publish({"aggregate_id": "own", "phases": [{"closure_id": point, "phase": phase,
            "rows_changed": rows, "started_monotonic": 0, "sql_completed_monotonic": .1}]})

    for rows in (1, 2, 3):
        publish(rows, point="small" + str(rows))
    publish(5000, phase="outcomes")
    assert metrics.snapshot()["heavy_membership_ack_rows"] == {}
    publish(5000)
    publish(5000)
    stale = metrics.snapshot()
    assert stale["heavy_membership_ack_rows"] == {identity: 10000}
    assert not history_gc_acknowledged(stale, {identity}, 10001)
    publish(1)
    current = metrics.snapshot()
    assert current["heavy_membership_ack_rows"] == {identity: 10001}
    assert stale["heavy_membership_ack_rows"] == {identity: 10000}

    snapshots = iter([stale, current])
    reads = []

    def final_read():
        value = next(snapshots)
        reads.append(value)
        return value if history_gc_acknowledged(value, {identity}, 10001) else None

    assert PerformanceBudget(1200).wait(final_read, 10, "Final metrics timed out") is current
    assert len(reads) == 2 and clock.now == .05
    # The tail completes freshness but still cannot prove heavy overlap.
    assert all(span["rows_changed"] > 3 for span in current["large_spans"])


def test_heavy_membership_ack_map_is_bounded_and_overflow_cannot_pass_final_read():
    metrics = clock_module().MaintenanceMetrics()
    targets = {str(UUID(int=index + 1)) for index in range(30)}
    for index in range(31):
        metrics.publish({"aggregate_id": "own", "phases": [{"closure_id": str(UUID(int=index + 1)),
            "phase": "memberships", "rows_changed": 5000, "started_monotonic": 1, "sql_completed_monotonic": 2}]})
    progress = metrics.snapshot()
    assert len(progress["heavy_membership_ack_rows"]) == 30
    assert progress["dropped_heavy_membership_acks"] == 1
    with pytest.raises(AssertionError):
        history_gc_acknowledged(progress, targets, 5000)


@pytest.mark.parametrize("rows", [10002, -1, 0, 10001.0, True])
def test_final_gc_read_rejects_overshoot_or_invalid_ack_counters(rows):
    with pytest.raises(AssertionError):
        history_gc_acknowledged({"heavy_membership_ack_rows": {"target": rows},
            "dropped_heavy_membership_acks": 0}, {"target"}, 10001)


def test_final_gc_read_requires_every_target_and_never_accepts_stale_metrics_after_ten_seconds(monkeypatch):
    _performance, clock = performance_time(monkeypatch)
    partial = {"heavy_membership_ack_rows": {"first": 10001}, "dropped_heavy_membership_acks": 0}
    assert not history_gc_acknowledged(partial, {"first", "second"}, 10001)
    complete = {"heavy_membership_ack_rows": {"first": 10001, "second": 10001},
        "dropped_heavy_membership_acks": 0}

    def late_read():
        clock.now = 10
        return complete if history_gc_acknowledged(complete, {"first", "second"}, 10001) else None

    with pytest.raises(AssertionError, match="Final metrics timed out"):
        PerformanceBudget(1200).wait(late_read, 10, "Final metrics timed out")


@pytest.mark.parametrize("group,names", [
    ("baseline-through-20", ["baseline-1", "shared-10-sparse", "distributed-20"]),
    ("shared-30", ["shared-30-dense"]), ("distributed-30", ["distributed-30-dense"])])
def test_approved_load_groups_keep_every_unchanged_case_exactly_once(group, names):
    assert [case.name for case in selected_load_cases(group)] == names
    combined = [case for value in ("baseline-through-20", "shared-30", "distributed-30")
        for case in selected_load_cases(value)]
    assert tuple(combined) == load_cases() == selected_load_cases()
    validate_performance_arguments(arguments(aggregate_performance_group=group))


@pytest.mark.parametrize("changes", [{"aggregate_performance": False, "aggregate_performance_group": "shared-30"},
    {"aggregate_performance_group": "unknown"}, {"aggregate_performance_group": "baseline-1"}])
def test_load_group_cannot_create_an_unapproved_or_nonperformance_selection(changes):
    with pytest.raises(ValueError):
        validate_performance_arguments(arguments(**changes))


@pytest.mark.parametrize("selection", [["smoke-development", "--aggregate-performance-group", "shared-30"],
    ["smoke-development", "--aggregate-performance", "--aggregate-performance-group", "unknown"]])
def test_actual_runner_rejects_group_misuse_before_any_inventory_or_directory(tmp_path, monkeypatch, selection):
    from scripts import agent_test_profiles as profiles

    monkeypatch.setattr(profiles, "ROOT", tmp_path)
    monkeypatch.setattr(profiles, "inventory", lambda *_: pytest.fail("No Docker for invalid selection"))
    monkeypatch.setattr(sys, "argv", ["agent_test_profiles.py", *selection])
    with pytest.raises(SystemExit) as caught:
        profiles.main()
    assert caught.value.code == 2 and not (tmp_path / ".test_runs").exists()


def test_next_close_wait_keeps_its_original_release_origin_and_global_cap(monkeypatch, capsys):
    performance, clock = performance_time(monkeypatch)
    clock.now = 20  # Control/writer work already consumed twenty seconds.
    budget = performance.PerformanceBudget(1200)
    with pytest.raises(AssertionError, match="next close"), budget.until(300, "next close", started=0):
        budget.call(lambda **kwargs: setattr(clock, "now", 300), cap=30)
    record = json.loads(capsys.readouterr().out.split(": ", 1)[1])
    assert record["local_elapsed_seconds"] == 300 and record["global_remaining_seconds"] == 900
    assert record["limiting_scope"] == "local" and clock.now == 300


def test_fixed_writer_oracle_checks_real_counts_formula_and_exact_statistics():
    assert expected_load_values(0, 11037, 1000) == {"events": 1000, "pids": 97}
    assert expected_load_values(1, 0, 2) == {"bytes": 21, "average": Decimal("10.5"), "failures": 1}
    assert expected_load_values(2, 0, 2) == {"median": Decimal("10.5"), "p95": Decimal("10.95"),
        "mode": 1, "share": Decimal(".5"), "variance": Decimal(".25"), "average": Decimal("10.5")}
    assert expected_load_values(2, 100, 1000)["variance"] == Decimal("83333.25")
    assert expected_load_values(2, 100, 5000)["variance"] == Decimal("2083333.25")
    assert expected_load_values(2, 100, 5000)["share"] == Decimal(".0104")


def computed_load_point():
    return {"id": str(UUID(int=3)), "aggregate_id": str(UUID(int=1)), "window_index": 520,
        "origin": "online", "time_basis": "event_time", "calculation_status": "computed",
        "calculation_reason": None, "payload_state": "present", "completeness": "complete", "event_count": 2,
        "close_not_before_at": "2026-11-17T00:00:00Z", "features": [
            {"key": "events", "value": 2, "reason": None, "invalid_count": 0},
            {"key": "pids", "value": 2, "reason": None, "invalid_count": 0}],
        "execution": {"policy_version": "window_execution_v1", "soft_latency_seconds": 5,
            "hard_budget_seconds": 30, "calculation_status": "computed", "attempt_id": str(UUID(int=2)),
            "origin": "online", "window_index": 520, "elapsed_seconds": .2,
            "calculation_reason": None, "measured_through": "before_commit",
            "started_at": "2026-11-17T00:00:00Z", "execution_deadline_at": "2026-11-17T00:00:30Z"}}


@pytest.mark.parametrize("change", [None, "uncomputed", "wrong_value", "null", "boolean", "invalid", "order",
    "expired_execution", "renewed_deadline", "wrong_measurement"])
def test_positive_load_cannot_pass_using_terminal_failure_or_inexact_features(change):
    point = computed_load_point()
    if change == "uncomputed":
        point.update(calculation_status="uncomputed", calculation_reason="execution_budget_exceeded")
    elif change == "wrong_value":
        point["features"][0]["value"] = 1
    elif change == "null":
        point["features"][0]["value"] = None
    elif change == "boolean":
        point["features"][0]["value"] = True
    elif change == "invalid":
        point["features"][0]["invalid_count"] = 1
    elif change == "order":
        point["features"].reverse()
    elif change == "expired_execution":
        point["execution"]["elapsed_seconds"] = 30.001
    elif change == "renewed_deadline":
        point["execution"]["execution_deadline_at"] = "2026-11-17T00:00:31Z"
    elif change == "wrong_measurement":
        point["execution"]["measured_through"] = "commit_ack"
    def check():
        assert_computed_load_point(point, expected_load_values(0, 0, 2), aggregate_id=str(UUID(int=1)),
            window_index=520, close_at=datetime(2026, 11, 17, tzinfo=UTC), count=2)
    if change is None:
        check()
    else:
        with pytest.raises(AssertionError):
            check()


def traced_load_step():
    return {"aggregate_id": str(UUID(int=1)), "point_id": str(UUID(int=2)), "queue_seconds": 1.2,
        "total_seconds": .5, "point_commit_start_monotonic": 12.5, "point_commit_ack_monotonic": 12.6,
        "commit_observed_monotonic": 12.8, "job_trace": {"job_id": "worker:1",
            "aggregate_id": str(UUID(int=1)), "kind": "foreground", "iteration_started_monotonic": 11,
            "discovery_started_monotonic": 11.2, "discovery_completed_monotonic": 11.5,
            "append_before_monotonic": 11.6, "append_after_monotonic": 11.7,
            "acquire_before_monotonic": 12, "acquire_after_monotonic": 12.1,
            "execute_started_monotonic": 12.2, "execute_completed_monotonic": 12.7}}


def test_job_measurements_separate_discovery_fifo_dispatch_and_soft_excess():
    result = measured_job_trace(traced_load_step(), 6, 300)
    assert result["soft_latency_exceeded"] and result["commit_ack_delay_seconds"] == pytest.approx(6.6)
    assert result["discovery_seconds"] == pytest.approx(.3)
    assert result["discovery_to_execute_seconds"] == pytest.approx(1.2)
    assert result["fifo_wait_lower_seconds"] == pytest.approx(.3)
    assert result["fifo_wait_upper_seconds"] == pytest.approx(.5)
    assert result["dispatch_lower_seconds"] == pytest.approx(.1)
    assert result["dispatch_upper_seconds"] == pytest.approx(.2)
    # A job can already exist when the controlled deadline is released.
    assert measured_job_trace(traced_load_step(), 12.3, 300)["release_to_execute_seconds"] < 0


def test_confirmed_late_ack_is_not_relabelled_as_an_execution_timeout():
    step = traced_load_step()
    step.update(point_commit_start_monotonic=42.1, point_commit_ack_monotonic=43,
        commit_observed_monotonic=43.3, total_seconds=31)
    step["job_trace"]["execute_completed_monotonic"] = 43.2
    assert measured_job_trace(step, 6, 300)["late_commit_ack"]
    step["point_commit_start_monotonic"] = 42.3
    with pytest.raises(AssertionError, match="computed_publication_started_after_hard_budget"):
        measured_job_trace(step, 6, 300)


def test_next_close_is_a_hard_control_bound_even_when_soft_is_only_diagnostic():
    step = traced_load_step()
    for key in ("point_commit_start_monotonic", "point_commit_ack_monotonic", "commit_observed_monotonic"):
        step[key] += 300
    for key in step["job_trace"]:
        if key.endswith("_monotonic"):
            step["job_trace"][key] += 300
    with pytest.raises(AssertionError, match="next_close_deadline_missed"):
        measured_job_trace(step, 12.6, 300)


def test_thirty_heavy_targets_do_not_lose_ack_slots_to_observed_incidental_seven_row_closures():
    metrics = clock_module().MaintenanceMetrics()
    targets = {str(UUID(int=index + 1)) for index in range(30)}
    def publish(identity, rows):
        metrics.publish({"aggregate_id": "own", "phases": [{"closure_id": identity, "phase": "memberships",
            "rows_changed": rows, "started_monotonic": 1, "sql_completed_monotonic": 2}]})
    for index, identity in enumerate(sorted(targets)):
        publish("incidental-" + str(index), 7)
        publish(identity, 5000)
        publish(identity, 5000)
    partial = metrics.snapshot()
    assert len(partial["heavy_membership_ack_rows"]) == 30 and partial["dropped_heavy_membership_acks"] == 0
    assert not history_gc_acknowledged(partial, targets, 10001)
    for identity in targets:
        publish(identity, 1)
    assert history_gc_acknowledged(metrics.snapshot(), targets, 10001)
    assert sum(span["rows_changed"] == 7 for span in metrics.snapshot()["large_spans"]) == 30


@pytest.mark.parametrize("phase_enabled", [False, True])
def test_real_worker_trace_correlates_one_job_without_extra_service_calls_or_moving_its_origin(monkeypatch, phase_enabled):
    from app.services.implementations.aggregate_worker import AggregateWorker

    module = clock_module()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock.now)
    identity, point = UUID(int=1), UUID(int=2)
    calls = []
    class Service:
        def maintenance_due(self, *_args, **_kwargs):
            calls.append("maintenance_due")
            clock.now += .2
            return []
        def due(self, *_args, **_kwargs):
            calls.append("due")
            clock.now += .3
            return [identity]
        def advance(self, target, **kwargs):
            calls.append("advance")
            assert target == identity
            assert kwargs == {"execution_started": .5}
            clock.now += .5
            return {"state": "closed", "point_id": point, "origin": "online", "timings": {"total": .5}}
        def record_worker_error(self, *_args):
            calls.append("record_worker_error")
            pytest.fail("A confirmed closed job must not open another metadata write")
    service = Service()
    worker = AggregateWorker(lambda: nullcontext(None), lambda *_: service, None, Event())
    phase = module.GenerationPhaseCapture("owned-run") if phase_enabled else None
    if phase is not None:
        phase.observe_marker(_generation_marker(), {identity: None})
    trace = module.JobTrace(phase)
    trace.install(worker)
    worker.tick()
    step, = worker.steps
    record = step["job_trace"]
    assert calls == ["maintenance_due", "due", "advance"]
    assert record["discovery_started_monotonic"] == .2 and record["discovery_completed_monotonic"] == .5
    assert record["append_before_monotonic"] == record["append_after_monotonic"] == .5
    assert record["acquire_before_monotonic"] == record["acquire_after_monotonic"] == .5
    assert record["execute_started_monotonic"] == .5 and record["execute_completed_monotonic"] == 1.
    assert step["queue_seconds"] == .5 and step["total_seconds"] == .5
    assert trace.snapshot() == {"live": 0, "max_live": 1, "issued": 1, "dropped": 0,
        "error_count": 0, "errors": []}
    assert trace.discovery.get() is None and worker.build(None, None) is service
    if phase is not None:
        assert phase.snapshot()["counters"]["foreground"] == {name: 1 for name in phase.STAGES}
        assert phase.snapshot()["counters"]["maintenance"] == {name: 0 for name in phase.STAGES}


def test_job_trace_live_memory_is_bounded_by_the_existing_admission_capacity():
    from app.services.implementations.aggregate_worker import AggregateWorker

    module = clock_module()
    worker = AggregateWorker(None, None, None, Event())
    trace = module.JobTrace()
    trace.install(worker)
    worker.admission.offer("foreground", [UUID(int=i + 1) for i in range(70)], 0)
    assert len(worker.admission.ready) == 64
    assert worker.admission.acquire() is not None and worker.admission.acquire() is not None
    worker.admission.offer("foreground", [UUID(int=i + 100) for i in range(10)], 0)
    assert trace.snapshot()["live"] == trace.snapshot()["max_live"] == 66
    assert trace.snapshot()["dropped"] == 0 and len(worker.admission.ready) == 64


def test_trace_failure_is_reported_without_replacing_original_admission(monkeypatch):
    from app.services.implementations.aggregate_worker import AggregateWorker

    module = clock_module()
    worker = AggregateWorker(None, None, None, Event())
    trace = module.JobTrace()
    trace.install(worker)
    def broken(*_args):
        raise ValueError("private diagnostic detail")
    monkeypatch.setattr(trace, "offered", broken)
    worker.admission.offer("foreground", [UUID(int=1)], 0)
    assert len(worker.admission.ready) == 1
    assert trace.snapshot()["error_count"] == 1 and trace.snapshot()["errors"] == ["ValueError"]


@pytest.mark.parametrize("change", [None, "error", "dropped", "overflow"])
def test_trace_diagnostics_cannot_silently_lose_or_corrupt_correlated_measurements(change):
    metrics = {"job_trace": {"live": 2, "max_live": 66, "dropped": 0, "error_count": 0, "errors": []}}
    if change == "error":
        metrics["job_trace"].update(error_count=1, errors=["ValueError"])
    elif change == "dropped":
        metrics["job_trace"]["dropped"] = 1
    elif change == "overflow":
        metrics["job_trace"]["max_live"] = 67
    if change is None:
        assert_job_trace_health(metrics)
    else:
        with pytest.raises(AssertionError):
            assert_job_trace_health(metrics)


def first_clear_setup(tmp_path, shifted=False):
    module = clock_module()
    path = tmp_path / "clock.json"
    case = load_cases()[2 if shifted else 1]
    cards = [{"id": str(UUID(int=index + 1))} for index in range(case.aggregates)]
    moment = datetime(2026, 11, 17, tzinfo=UTC)
    moments = {card["id"]: moment.isoformat() for card in cards}
    deadlines = {card["id"]: moment + timedelta(seconds=300 - index % 3 if shifted else 300)
        for index, card in enumerate(cards)}
    points = {card["id"]: str(UUID(int=index + 100)) for index, card in enumerate(cards)}
    plan = first_clear_plan("owned-run", case, cards, deadlines, points)
    write_clock(path, moments)
    clock = module.ScopedClock(path)
    release = module.FirstClearRelease(clock, "owned-run")
    return module, path, moments, plan, clock, release


@pytest.mark.parametrize("failure", [None, "rollback", "commit", "foreign", "before_arm", "duplicate"])
def test_first_clear_release_uses_only_its_prearmed_original_commit_ack_and_restores_scope(
        tmp_path, monkeypatch, failure):
    from app.aggregation.event_time import EventTimeBudgetError
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )

    module, path, moments, plan, clock, release = first_clear_setup(tmp_path)
    elapsed = SimpleNamespace(now=100.)
    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed.now)
    if failure != "before_arm":
        write_clock(path, moments, plan)
    seen = []

    def clear(closure, *_args, **_kwargs):
        assert release.snapshot()["receipt"] is None
        seen.append("claim_sql")
        if failure == "before_arm":
            write_clock(path, moments, plan)  # Too late for this claim.
        return True

    def commit():
        seen.append("commit_call")
        assert release.snapshot()["receipt"] is None or failure == "duplicate"
        elapsed.now += .1
        if failure == "commit":
            raise RuntimeError("unconfirmed commit")
        seen.append("commit_ack")

    def rollback():
        seen.append("rollback")

    repository = SimpleNamespace(clear_payload=clear)
    uow = SimpleNamespace(commit=commit, rollback=rollback)

    def maintain(service, identity):
        closure = UUID(plan["closure_id"]) if failure != "foreign" else UUID(int=9999)
        service.repository.clear_payload(SimpleNamespace(origin_point_id=closure), None, automatic=True)
        if failure == "rollback":
            service.uow.rollback()
        service.uow.commit()
        if failure == "duplicate":
            first = release.snapshot()["receipt"]
            service.uow.commit()  # Unrelated later commit cannot release twice.
            assert release.snapshot()["receipt"] == first
        return {"status": "compacting"}

    monkeypatch.setattr(AggregateRetentionService, "maintain", maintain)
    monkeypatch.setattr(AggregateRetentionService, "_save", AggregateRetentionService._save)
    monkeypatch.setattr(EventTimeBudgetError, "__init__", EventTimeBudgetError.__init__)
    for method in ("compact_memberships", "compact_outcomes"):
        monkeypatch.setattr(SqlAlchemyAggregateRepository, method, getattr(SqlAlchemyAggregateRepository, method))
    metrics = module.MaintenanceMetrics()
    module.install_maintenance_metrics(metrics, release)
    service = AggregateRetentionService(repository, uow)
    if failure == "commit":
        with pytest.raises(RuntimeError, match="unconfirmed"):
            service.maintain(UUID(plan["aggregate_id"]))
    else:
        service.maintain(UUID(plan["aggregate_id"]))
    assert uow.commit is commit and uow.rollback is rollback and repository.clear_payload is clear
    assert metrics.current.get() is None
    receipt = release.snapshot()["receipt"]
    if failure in {None, "duplicate"}:
        assert seen[:3] == ["claim_sql", "commit_call", "commit_ack"]
        assert receipt["commit_ack_monotonic"] == receipt["groups"][0]["released_monotonic"] == 100.1
        assert acknowledged_release({"first_clear_release": release.snapshot()}, plan) == receipt
        assert clock.now(UUID(plan["aggregate_id"])) == datetime.fromisoformat(
            plan["groups"][0]["moments"][plan["aggregate_id"]])
    else:
        assert receipt is None and clock.now(UUID(plan["aggregate_id"])) == datetime.fromisoformat(
            moments[plan["aggregate_id"]])
    write_clock(path, moments)
    assert release.snapshot()["armed"] is False and release.plan is None and release.overrides == {}
    release.close()
    assert clock.release is None and release.receipt is None and release.used is None


def test_first_clear_shifted_release_preserves_order_real_spacing_and_no_gc_wait(tmp_path, monkeypatch):
    module, path, moments, plan, clock, release = first_clear_setup(tmp_path, shifted=True)
    elapsed = SimpleNamespace(now=10.)
    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed.now)
    monkeypatch.setattr(module.time, "sleep", lambda *_args: pytest.fail("The ACK hook must never delay GC"))
    write_clock(path, moments, plan)
    scope = release.claim(plan["aggregate_id"], plan["closure_id"])
    release.acknowledged({**scope, "nonce": str(UUID(int=999))}, 10.)
    assert release.snapshot()["receipt"] is None
    release.acknowledged(scope, 10.)
    first = release.snapshot()["receipt"]
    assert len(first["groups"]) == 1 and len(first["released"]) == 6
    assert acknowledged_release({"first_clear_release": release.snapshot()}, plan) is None
    elapsed.now = 10.149
    clock.now(UUID(plan["aggregate_id"]))
    assert len(release.snapshot()["receipt"]["groups"]) == 1
    elapsed.now = 10.2
    clock.now(UUID(plan["aggregate_id"]))
    elapsed.now = 10.349
    clock.now(UUID(plan["aggregate_id"]))
    assert len(release.snapshot()["receipt"]["groups"]) == 2
    elapsed.now = 10.4
    clock.now(UUID(plan["aggregate_id"]))
    receipt = acknowledged_release({"first_clear_release": release.snapshot()}, plan)
    assert [group["released_monotonic"] for group in receipt["groups"]] == [10., 10.2, 10.4]
    for expected, actual in zip(plan["groups"], receipt["groups"], strict=True):
        assert actual["aggregate_ids"] == list(expected["moments"])
    assert len(receipt["released"]) == 20 and clock.now(UUID(int=9999)) is None
    release.acknowledged(scope, 11.)
    assert release.snapshot()["receipt"] == receipt
    span = {"sequence": 1, "closure_id": plan["closure_id"], "phase": "memberships", "rows_changed": 5000,
        "started_monotonic": 10.05, "sql_completed_monotonic": 10.2, "committed_monotonic": 10.3}
    assert maintenance_overlap([span], [{"released": 10., "commit_started": 10.5,
        "commit_ack": 10.6}], {plan["closure_id"]})["target_history_heavy"]["contained_online_samples"] == 1
    release.close()


def test_case_arm_finally_removes_the_request_after_failure_and_does_not_replay_old_signals(tmp_path):
    _module, path, moments, plan, clock, release = first_clear_setup(tmp_path)
    assert release.claim(plan["aggregate_id"], plan["closure_id"]) is None
    with pytest.raises(RuntimeError, match="case failed"), armed_release(path, moments, plan):
        scope = release.claim(plan["aggregate_id"], plan["closure_id"])
        raise RuntimeError("case failed")
    assert json.loads(path.read_text(encoding="utf-8")) == {"aggregates": moments}
    release.acknowledged(scope, 1.)
    assert release.snapshot() == {"sequence": 1, "armed": False, "receipt": None}
    assert clock.now(UUID(plan["aggregate_id"])) == datetime.fromisoformat(moments[plan["aggregate_id"]])
    release.close()


def test_default_first_clear_plan_keeps_legacy_identity_and_initial_410_never_arms(tmp_path):
    _module, _path, _moments, plan, _clock, release = first_clear_setup(tmp_path)
    assert plan["aggregate_id"] == str(UUID(int=1)) and plan["closure_id"] == str(UUID(int=100))
    calls = []
    client = SimpleNamespace(get=lambda path, **_kwargs: history_response(410,
        plan["aggregate_id"], plan["closure_id"]))
    with pytest.raises(AssertionError, match="already cleared"):
        wait_for_history_clear(client, {plan["aggregate_id"]: plan["closure_id"]}, 10001,
            10**15, on_present=lambda *_args: calls.append(True))
    assert calls == [] and release.plan is None
    release.close()


def test_first_clear_arm_has_one_bounded_deadline_without_refresh_on_rewrite(tmp_path, monkeypatch):
    module, path, moments, plan, _clock, release = first_clear_setup(tmp_path)
    elapsed = SimpleNamespace(now=10.)
    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed.now)
    plan["valid_for_seconds"] = .1
    write_clock(path, moments, plan)
    scope = release.claim(plan["aggregate_id"], plan["closure_id"])
    deadline = release.plan["deadline"]
    elapsed.now = 10.2
    write_clock(path, moments, plan)
    assert release.claim(plan["aggregate_id"], plan["closure_id"]) is None
    assert release.plan["deadline"] == deadline
    release.acknowledged(scope, elapsed.now)
    assert release.snapshot()["receipt"] is None and release.overrides == {}
    release.close()


@pytest.mark.parametrize("field,value", [("run_id", "foreign-run"), ("wave_index", 520),
    ("aggregate_id", str(UUID(int=999))), ("case_id", "unknown-case"), ("valid_for_seconds", 121)])
def test_first_clear_arm_rejects_foreign_or_unbounded_context_without_advancing_clocks(tmp_path, field, value):
    _module, path, moments, plan, clock, release = first_clear_setup(tmp_path)
    write_clock(path, moments, {**plan, field: value})
    with pytest.raises(AssertionError):
        clock.now(UUID(plan["aggregate_id"]))
    assert release.plan is None and release.overrides == {}
    release.close()


def test_concurrent_first_clear_signals_release_once_and_keep_maintenance_clocks_physical(tmp_path, monkeypatch):
    module, path, moments, plan, clock, release = first_clear_setup(tmp_path)
    monkeypatch.setattr(module.time, "monotonic", lambda: 10.)
    write_clock(path, moments, plan)
    scope = release.claim(plan["aggregate_id"], plan["closure_id"])
    barrier = Barrier(2)

    def signal():
        with clock.scope("maintenance"):
            barrier.wait(timeout=2)
            release.acknowledged(scope, 10.)
            assert clock.now(UUID(plan["aggregate_id"])) is None
        return release.snapshot()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = [future.result(timeout=5) for future in (executor.submit(signal), executor.submit(signal))]
    assert first == second and first["sequence"] == 1
    assert len(first["receipt"]["groups"]) == 1 and len(first["receipt"]["released"]) == 10
    write_clock(path, moments)
    assert release.snapshot()["armed"] is False and release.overrides == {}
    release.close()


def maintenance_trace_record(index=1):
    record = dict(traced_load_step()["job_trace"])
    record.update(job_id=f"{'a' * 32}:{index}", kind="maintenance")
    return record


def capture_bytes(snapshot):
    return len(json.dumps(snapshot, allow_nan=False).encode("ascii"))


def test_maintenance_capture_keeps_a_fixed_tail_without_changing_legacy_trace_or_reading_clocks(monkeypatch):
    module = clock_module()
    trace = module.JobTrace()
    before = trace.snapshot()
    def forbidden():
        pytest.fail("Metadata capture must reuse existing measurements")
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=forbidden, sleep=forbidden))
    for index in range(1, 301):
        trace.maintenance_jobs.append(maintenance_trace_record(index))
    snapshot = trace.maintenance_jobs.snapshot()
    assert snapshot["record_limit"] == snapshot["retained"] == 256
    assert snapshot["byte_limit"] == 192 * 1024
    assert snapshot["observed"] == 300 and snapshot["evicted"] == 44 and snapshot["invalid"] == 0
    assert [record["job_id"] for record in snapshot["records"]] == [f"{'a' * 32}:{index}" for index in range(45, 301)]
    assert capture_bytes(snapshot) <= snapshot["byte_limit"]
    assert snapshot["retained_bytes"] == len(json.dumps(snapshot["records"]).encode("ascii"))
    assert trace.snapshot() == before


def test_maintenance_capture_enforces_the_byte_cap_including_counter_growth():
    module = clock_module()
    capture = module.MaintenanceJobCapture()
    for index in range(1, 257):
        record = maintenance_trace_record(index)
        record.update({key: 10**300 for key in capture.TIMESTAMPS})
        capture.append(record)
    snapshot = capture.snapshot()
    assert snapshot["observed"] == 256 and 0 < snapshot["retained"] < 256
    assert snapshot["evicted"] == 256 - snapshot["retained"] and snapshot["invalid"] == 0
    assert capture_bytes(snapshot) <= 192 * 1024
    for _ in range(300):
        capture.reject()
    rejected = capture.snapshot()
    assert rejected["observed"] == 556 and rejected["invalid"] == 300
    assert capture_bytes(rejected) <= 192 * 1024
    assert rejected["observed"] == rejected["retained"] + rejected["evicted"] + rejected["invalid"]


def test_maintenance_capture_projects_scalars_and_returns_independent_snapshots():
    module = clock_module()
    capture = module.MaintenanceJobCapture()
    record = maintenance_trace_record()
    record.update(raw=object(), checkpoint={"secret": object()}, state={"arbitrary": object()})
    record["discovery_started_monotonic"] = None
    capture.append(record)
    snapshot = capture.snapshot()
    captured, = snapshot["records"]
    assert set(captured) == {"job_id", "aggregate_id", "kind", *capture.TIMESTAMPS}
    assert captured["discovery_started_monotonic"] is None
    assert captured["execute_completed_monotonic"] == record["execute_completed_monotonic"]
    captured["job_id"] = "changed snapshot"
    record["aggregate_id"] = "changed input"
    assert capture.snapshot()["records"][0]["job_id"] == f"{'a' * 32}:1"
    assert capture.snapshot()["records"][0]["aggregate_id"] == str(UUID(int=1))
    assert "secret" not in json.dumps(capture.snapshot()) and capture.snapshot()["invalid"] == 0


@pytest.mark.parametrize("field,value", [("job_id", "private " * 20), ("job_id", "not-a-job"),
    ("aggregate_id", "private identity"), ("kind", "foreground"),
    ("execute_started_monotonic", float("nan")), ("execute_started_monotonic", float("inf")),
    ("execute_started_monotonic", True)])
def test_invalid_maintenance_metadata_is_counted_without_retaining_unsafe_values(field, value):
    capture = clock_module().MaintenanceJobCapture()
    record = maintenance_trace_record()
    record[field] = value
    capture.append(record)
    assert capture.snapshot() == {"record_limit": 256, "byte_limit": 192 * 1024,
        "observed": 1, "evicted": 0, "invalid": 1, "retained": 0, "retained_bytes": 2, "records": []}


def test_maintenance_capture_failure_preserves_the_original_completed_record_and_legacy_counters(monkeypatch):
    module = clock_module()
    trace = module.JobTrace()
    job = SimpleNamespace(aggregate_id=UUID(int=1), kind="maintenance", queued_at=1.)
    trace.offered(job, "maintenance", 2., 3.)
    def failed_capture(_record):
        raise RuntimeError("private capture failure")
    monkeypatch.setattr(trace.maintenance_jobs, "append", failed_capture)
    record = trace.completed(job, {"started": 4., "completed": 5.})
    assert record["execute_started_monotonic"] == 4. and record["execute_completed_monotonic"] == 5.
    assert trace.snapshot() == {"live": 0, "max_live": 1, "issued": 1, "dropped": 0,
        "error_count": 0, "errors": []}
    snapshot = trace.maintenance_jobs.snapshot()
    assert snapshot["observed"] == snapshot["invalid"] == 1 and snapshot["records"] == []
    assert "private" not in json.dumps(snapshot)


@pytest.fixture
def captured_maintenance_worker(tmp_path, monkeypatch, request):
    from app.aggregation.event_time import EventTimeBudgetError
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_execution import (
        AggregateExecutionService,
    )
    from app.services.implementations.aggregate_retention import (
        AggregateRetentionService,
    )
    from app.services.implementations.aggregate_worker import AggregateWorker

    module = clock_module()
    metrics = module.MaintenanceMetrics()
    monkeypatch.setattr(module, "MaintenanceMetrics", lambda: metrics)
    monkeypatch.setitem(sys.modules, "resource", SimpleNamespace(RUSAGE_SELF=0,
        getrusage=lambda _: SimpleNamespace(ru_maxrss=128, ru_utime=.3, ru_stime=.2)))
    monkeypatch.setattr(module, "Path", lambda value: tmp_path / Path(value).name)
    monkeypatch.setattr(module, "cgroup_memory", lambda: {"peak_memory_bytes": None,
        "memory_limit_bytes": None, "memory_peak_source": None, "memory_cgroup_version": None,
        "memory_metric_error": "unavailable"})
    monkeypatch.setattr(AggregateWorker, "__init__", AggregateWorker.__init__)
    monkeypatch.setattr(AggregateWorker, "_execute", AggregateWorker._execute)
    monkeypatch.setattr(EventTimeBudgetError, "__init__", EventTimeBudgetError.__init__)
    monkeypatch.setattr(AggregateExecutionService, "advance", AggregateExecutionService.advance)
    monkeypatch.setattr(AggregateRetentionService, "_save", AggregateRetentionService._save)
    for method in ("compact_memberships", "compact_outcomes"):
        monkeypatch.setattr(SqlAlchemyAggregateRepository, method, getattr(SqlAlchemyAggregateRepository, method))
    scenario = SimpleNamespace(callback=None, calls=[])
    monkeypatch.setattr(AggregateRetentionService, "maintain",
        lambda service, identity: scenario.callback(service, identity))
    phase = module.GenerationPhaseCapture("owned-run") if getattr(request, "param", False) else None
    if phase is not None:
        phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    module.install_metrics(phase_capture=phase)
    worker = AggregateWorker(None, None, None, Event())

    def service():
        return AggregateRetentionService(SimpleNamespace(maintenance_budget=lambda _budget: scenario.calls.append("budget")),
            SimpleNamespace(commit=lambda: scenario.calls.append("commit"), rollback=lambda: scenario.calls.append("rollback")))

    worker._maintain = lambda identity: service().maintain(identity)
    return SimpleNamespace(module=module, metrics=metrics, worker=worker, scenario=scenario, phase=phase,
        service=service, error=EventTimeBudgetError, result_path=tmp_path / "aggregate-performance.json")


def offer_maintenance_job(setup, identity):
    job = SimpleNamespace(aggregate_id=identity, kind="maintenance", queued_at=9.)
    trace = setup.worker._controlled_job_trace
    token = trace.discovery.set({"maintenance": {"started": 9.1, "completed": 9.2}})
    try:
        trace.offered(job, "maintenance", 9.3, 9.4)
    finally:
        trace.discovery.reset(token)
    trace.update(job, acquire_before_monotonic=9.5, acquire_after_monotonic=9.6)
    return job


@pytest.mark.parametrize("outcome", ["commit", "rollback", "idle"])
def test_maintenance_capture_links_real_execution_and_publishes_without_extra_calls_or_clocks(
        captured_maintenance_worker, monkeypatch, outcome):
    setup = captured_maintenance_worker
    elapsed, reads = SimpleNamespace(now=10.), []
    def clock():
        reads.append(True)
        return elapsed.now
    monkeypatch.setattr(setup.module.time, "monotonic", clock)
    expected_result = {"status": "idle"} if outcome == "idle" else {"status": "error", "reason": "retention_budget"}
    def maintain(service, identity):
        assert identity == UUID(int=1)
        assert setup.metrics.current.get()["job_id"] == setup.metrics.completion.get()["job_id"]
        elapsed.now = 10.5
        if outcome == "idle":
            return expected_result
        setup.metrics.current.get()["phases"].append({"closure_id": str(UUID(int=2)), "phase": "memberships",
            "rows_changed": 5000, "started_monotonic": 10.1, "sql_completed_monotonic": 10.4})
        if outcome == "rollback":
            try:
                raise setup.error("operation_seconds")
            except setup.error:
                service.uow.rollback()
                return expected_result
        return service._save(SimpleNamespace(status="compacting", reason=None,
            next_check_at=None, pending_closure_id=UUID(int=2)), None)
    setup.scenario.callback = maintain
    job = offer_maintenance_job(setup, UUID(int=1))
    result = setup.worker._execute(job)
    record = setup.worker._controlled_job_trace.completed(job, result)
    setup.worker.observe({"admission": setup.worker.admission.summary(), "steps": []})
    saved = json.loads(setup.result_path.read_text(encoding="utf-8"))
    captured, = saved["maintenance_jobs"]["records"]
    assert captured == record and captured["execute_started_monotonic"] == 10.
    assert captured["execute_completed_monotonic"] == 10.5
    assert captured["discovery_started_monotonic"] == 9.1 and captured["append_after_monotonic"] == 9.4
    assert len(reads) == (3 if outcome == "commit" else 2)
    assert setup.metrics.completion.get() is None and setup.metrics.current.get() is None
    if outcome == "commit":
        span, = saved["maintenance"]["spans"]
        assert span["job_id"] == captured["job_id"] and span["rows_changed"] == 5000
        assert span["committed_monotonic"] == 10.5 and saved["maintenance"]["budget_errors"] == []
        assert setup.scenario.calls == ["budget", "commit"]
    else:
        assert result["result"] is expected_result and saved["maintenance"]["spans"] == []
        if outcome == "rollback":
            error, = saved["maintenance"]["budget_errors"]
            assert error["job_id"] == captured["job_id"] and error["turn_total_seconds"] == .5
            assert setup.scenario.calls == ["rollback"]
        else:
            assert saved["maintenance"]["budget_errors"] == [] and setup.scenario.calls == []
    assert saved["job_trace"] == {"live": 0, "max_live": 1, "issued": 1, "dropped": 0,
        "error_count": 0, "errors": []}


def test_maintenance_capture_keeps_job_links_thread_local_and_restores_previous_execution_context(
        captured_maintenance_worker):
    setup = captured_maintenance_worker
    barrier, seen = Barrier(2), {}
    jobs = [offer_maintenance_job(setup, UUID(int=index)) for index in (1, 2)]
    def maintain(service, identity):
        seen[identity] = (setup.metrics.completion.get()["job_id"], setup.metrics.current.get()["job_id"])
        barrier.wait(timeout=2)
        setup.metrics.current.get()["phases"].append({"closure_id": str(identity), "phase": "memberships",
            "rows_changed": 5000, "started_monotonic": 1., "sql_completed_monotonic": 2.})
        if identity == UUID(int=2):
            error = setup.error("operation_seconds")
            service.uow.rollback()
            raise error
        return service._save(SimpleNamespace(status="compacting", reason=None,
            next_check_at=None, pending_closure_id=identity), None)
    setup.scenario.callback = maintain

    def execute(job):
        previous = {"job_id": "previous context"}
        token = setup.metrics.completion.set(previous)
        try:
            result = setup.worker._execute(job)
            assert setup.metrics.completion.get() is previous and setup.metrics.current.get() is None
            return result
        finally:
            setup.metrics.completion.reset(token)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result(timeout=5) for future in [executor.submit(execute, job) for job in jobs]]
    trace = setup.worker._controlled_job_trace
    for job, result in zip(jobs, results, strict=True):
        trace.completed(job, result)
    snapshot = setup.metrics.snapshot()
    span, = snapshot["spans"]
    error, = snapshot["budget_errors"]
    assert span["job_id"] == seen[UUID(int=1)][0] == seen[UUID(int=1)][1]
    assert error["job_id"] == seen[UUID(int=2)][0] == seen[UUID(int=2)][1]
    assert span["job_id"] != error["job_id"] and results[1]["result"] == "failed"
    assert {row["job_id"] for row in trace.maintenance_jobs.snapshot()["records"]} == {span["job_id"], error["job_id"]}
    assert setup.metrics.current.get() is None and setup.metrics.completion.get() is None
    assert trace.snapshot()["error_count"] == 0


def test_direct_maintenance_without_execution_context_preserves_unknown_job_links(captured_maintenance_worker):
    setup = captured_maintenance_worker
    attempted = []
    def maintain(service, identity):
        assert setup.metrics.current.get()["job_id"] is None
        if attempted:
            try:
                raise setup.error("operation_seconds")
            except setup.error:
                service.uow.rollback()
                return {"status": "error", "reason": "retention_budget"}
        attempted.append(True)
        setup.metrics.current.get()["phases"].append({"closure_id": str(identity), "phase": "memberships",
            "rows_changed": 1, "started_monotonic": 1., "sql_completed_monotonic": 2.})
        return service._save(SimpleNamespace(status="compacting", reason=None,
            next_check_at=None, pending_closure_id=identity), None)
    setup.scenario.callback = maintain
    setup.service().maintain(UUID(int=1))
    setup.service().maintain(UUID(int=1))
    snapshot = setup.metrics.snapshot()
    assert snapshot["spans"][0]["job_id"] is None and snapshot["budget_errors"][0]["job_id"] is None
    assert setup.metrics.current.get() is None and setup.metrics.completion.get() is None


def test_failed_capture_identity_lookup_cannot_prevent_or_change_maintenance(captured_maintenance_worker, monkeypatch):
    setup = captured_maintenance_worker
    expected = {"status": "idle"}
    def maintain(_service, _identity):
        assert setup.metrics.completion.get()["job_id"] is None
        return expected
    def failed_lookup(_job):
        raise ValueError("private lookup failure")
    setup.scenario.callback = maintain
    job = offer_maintenance_job(setup, UUID(int=1))
    trace = setup.worker._controlled_job_trace
    monkeypatch.setattr(trace, "job_id", failed_lookup)
    result = setup.worker._execute(job)
    record = trace.completed(job, result)
    assert result["result"] is expected and record["job_id"] == f"{trace.identity}:1"
    assert setup.metrics.completion.get() is None and setup.metrics.current.get() is None
    assert trace.snapshot()["error_count"] == 0 and setup.scenario.calls == []
    assert "private" not in json.dumps(trace.maintenance_jobs.snapshot())


def test_history_selector_skips_initial_410_then_arms_one_present_identity_and_stops_scanning():
    identities = [(str(UUID(int=index)), str(UUID(int=index + 100))) for index in (1, 2, 3)]
    replies = {identities[0]: [410], identities[1]: [200, 200, 410]}
    reads, armed = [], []

    def get(path, **_kwargs):
        pair = (path.split("/")[-3], path.split("/")[-1])
        reads.append(pair)
        return history_response(replies[pair].pop(0), *pair)

    result = wait_for_history_clear(SimpleNamespace(get=get), dict(identities), 10001, 10**15,
        on_present=lambda *pair: armed.append(pair))
    assert result == {"aggregate_id": identities[1][0], "point_id": identities[1][1],
        "history_rows": 10001, "observed_transition": "200_to_automatic_410"}
    assert armed == [identities[1]] and reads == [identities[0], identities[1], identities[1], identities[1]]


@pytest.mark.parametrize("failure", ["arm", "after_selection"])
def test_history_selector_never_falls_back_after_selecting_a_valid_200(failure):
    identities = [(str(UUID(int=index)), str(UUID(int=index + 100))) for index in (1, 2, 3)]
    reads, armed = [], []

    def get(path, **_kwargs):
        pair = (path.split("/")[-3], path.split("/")[-1])
        reads.append(pair)
        assert pair != identities[2], "No other candidate may be read after selection"
        if pair == identities[0]:
            return history_response(410, *pair)
        if armed:
            raise RuntimeError("selected claim missed")
        return history_response(200, *pair)

    def arm(*pair):
        armed.append(pair)
        if failure == "arm":
            raise RuntimeError("selected arm failed")

    with pytest.raises(RuntimeError, match="selected"):
        wait_for_history_clear(SimpleNamespace(get=get), dict(identities), 10001, 10**15, on_present=arm)
    assert armed == [identities[1]] and identities[2] not in reads


def test_history_selector_with_all_initial_410_never_creates_an_arm():
    identities = [(str(UUID(int=index)), str(UUID(int=index + 100))) for index in (1, 2, 3)]
    reads, armed = [], []

    def get(path, **_kwargs):
        pair = (path.split("/")[-3], path.split("/")[-1])
        reads.append(pair)
        return history_response(410, *pair)

    with pytest.raises(AssertionError, match="already cleared"):
        wait_for_history_clear(SimpleNamespace(get=get), dict(identities), 10001, 10**15,
            on_present=lambda *pair: armed.append(pair))
    assert reads == identities and armed == []


@pytest.mark.parametrize("overall,selected_at,late_at,error", [
    (1200, 119., 120., AssertionError), (105, 104., 105., RuntimeError)])
def test_history_selection_and_same_identity_wait_share_the_original_deadline(
        monkeypatch, overall, selected_at, late_at, error):
    _performance, clock = performance_time(monkeypatch)
    identities = [(str(UUID(int=index)), str(UUID(int=index + 100))) for index in (1, 2, 3)]
    reads, timeouts, armed = [], [], []

    def get(path, *, timeout):
        pair = (path.split("/")[-3], path.split("/")[-1])
        reads.append(pair)
        timeouts.append(timeout)
        assert pair != identities[2]
        if pair == identities[0]:
            clock.now = 100.
            return history_response(410, *pair)
        if not armed:
            clock.now = selected_at
            return history_response(200, *pair)
        clock.now = late_at
        return history_response(410, *pair)

    with pytest.raises(error):
        wait_for_history_clear(SimpleNamespace(get=get), dict(identities), 10001, overall,
            on_present=lambda *pair: armed.append(pair))
    assert armed == [identities[1]] and reads == [identities[0], identities[1], identities[1]]
    assert timeouts[-1] == 1. and clock.now == late_at


def test_explicit_first_clear_selection_arms_only_its_before_proof_identity_and_preserves_all_groups(
        tmp_path, monkeypatch):
    module, path, moments, default_plan, _clock, release = first_clear_setup(tmp_path)
    monkeypatch.setattr(module.time, "monotonic", lambda: 10.)
    cards = [{"id": identity} for identity in moments]
    deadlines = {identity: datetime.fromisoformat(value)
        for identity, value in default_plan["groups"][0]["moments"].items()}
    old_points = {card["id"]: str(UUID(int=index + 100)) for index, card in enumerate(cards)}
    chosen = cards[1]["id"]
    plan = first_clear_plan("owned-run", load_cases()[1], cards, deadlines, old_points,
        selected_id=chosen, valid_for_seconds=1.)
    assert plan["aggregate_id"] == chosen and plan["closure_id"] == old_points[chosen]
    assert plan["groups"] == default_plan["groups"] and plan["valid_for_seconds"] == 1.
    with armed_release(path, moments, plan):
        assert release.claim(cards[0]["id"], old_points[cards[0]["id"]]) is None
        scope = release.claim(chosen, old_points[chosen])
        release.acknowledged(scope, 10.)
        receipt = acknowledged_release({"first_clear_release": release.snapshot()}, plan)
        assert receipt["aggregate_id"] == chosen and set(receipt["released"]) == set(moments)
    assert release.snapshot()["armed"] is False
    release.close()


@pytest.mark.parametrize("in_cards,in_proof", [(True, False), (False, True), (False, False)])
def test_explicit_first_clear_selection_rejects_identity_outside_the_case_or_before_proof(
        tmp_path, in_cards, in_proof):
    _module, _path, moments, default_plan, _clock, release = first_clear_setup(tmp_path)
    chosen = str(UUID(int=9999))
    cards = [{"id": identity} for identity in moments] + ([{"id": chosen}] if in_cards else [])
    deadlines = {identity: datetime.fromisoformat(value)
        for identity, value in default_plan["groups"][0]["moments"].items()}
    old_points = {identity: str(UUID(int=index + 100)) for index, identity in enumerate(moments)}
    if in_proof:
        old_points[chosen] = str(UUID(int=9998))
    with pytest.raises(AssertionError):
        first_clear_plan("owned-run", load_cases()[1], cards, deadlines, old_points, selected_id=chosen)
    assert release.plan is None and release.receipt is None
    release.close()


class CaptureFailure(RuntimeError):
    pass


class FaultyCapture(dict):
    def __init__(self, values, failure):
        super().__init__(values)
        self.failure = failure
        self.site_attempts = 0
        if failure == "marker_setter":

            class BrokenMarkers(dict):
                def __setitem__(self, _key, _value):
                    raise CaptureFailure("diagnostic-marker-setter-only")

            dict.__setitem__(self, "markers", BrokenMarkers())

    def __getitem__(self, key):
        if (
            self.failure == "record"
            and key == "responses"
            or self.failure == "markers"
            and key == "markers"
            or self.failure == "batches"
            and key == "batch_seconds"
        ):
            raise CaptureFailure("diagnostic-getter-only")
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if key == "assert_site":
            self.site_attempts += 1
        if (
            self.failure == "site"
            and key == "assert_site"
            or self.failure == "selected"
            and key == "selected"
        ):
            raise CaptureFailure("diagnostic-setter-only")
        super().__setitem__(key, value)


def history_failure_capture(failure=None):
    return FaultyCapture(
        {
            "responses_observed": 0,
            "responses_evicted": 0,
            "responses": [],
            "markers": {},
            "selected": None,
            "assert_site": None,
        },
        failure,
    )


@pytest.mark.parametrize("field", ["checked_monotonic", "last_call_checks"])
@pytest.mark.parametrize("access", ["read", "write"])
def test_optional_budget_metadata_cannot_change_success_failure_or_clock_checks(
    monkeypatch, field, access
):
    performance, clock = performance_time(monkeypatch)
    reads = []

    def monotonic():
        reads.append(clock.now)
        return clock.now

    monkeypatch.setattr(performance.time, "monotonic", monotonic)

    class BrokenBudget(PerformanceBudget):
        def __getattribute__(self, name):
            if access == "read" and name == field:
                raise CaptureFailure("optional-budget-read")
            return super().__getattribute__(name)

        def __setattr__(self, name, value):
            if access == "write" and name == field:
                raise CaptureFailure("optional-budget-write")
            super().__setattr__(name, value)

    result = object()
    budget = BrokenBudget(1200)
    assert budget.call(lambda **_kwargs: result) is result
    assert reads == [0, 0]
    reads.clear()
    assert budget.wait(lambda: result, 10, "unchanged condition") is result
    assert reads == [0] * 5 and budget.limits == []
    original = ValueError("actual-operation-secret")

    def failed(**_kwargs):
        raise original

    with pytest.raises(ValueError) as caught:
        budget.call(failed)
    assert caught.value is original


def test_on_ready_is_nonfatal_but_predicate_and_real_clock_errors_keep_their_identity(monkeypatch):
    performance, _clock = performance_time(monkeypatch)
    result, callbacks = object(), []
    budget = PerformanceBudget(1200)

    def broken_ready(checked):
        callbacks.append(checked)
        raise CaptureFailure("optional-ready-only")

    assert budget.wait(lambda: result, 10, "ready", on_ready=broken_ready) is result
    assert callbacks == [0] and budget.limits == []
    original = ValueError("actual-predicate-secret")

    def failed_predicate():
        raise original

    with pytest.raises(ValueError) as caught:
        budget.wait(failed_predicate, 10, "actual predicate", on_ready=broken_ready)
    assert caught.value is original and callbacks == [0]
    clock_failure = RuntimeError("actual-clock-failed")

    def failed_clock():
        raise clock_failure

    monkeypatch.setattr(performance.time, "monotonic", failed_clock)
    with pytest.raises(RuntimeError) as caught:
        budget.remaining()
    assert caught.value is clock_failure
    with pytest.raises(KeyboardInterrupt):
        performance._capture_safely(lambda: (_ for _ in ()).throw(KeyboardInterrupt()))


@pytest.mark.parametrize(
    "failure", [None, "record", "markers", "marker_setter", "selected", "site", "uuid"]
)
def test_selector_metadata_failures_preserve_success_and_actual_arm_exception(monkeypatch, failure):
    _performance, clock = performance_time(monkeypatch)
    pair = (str(UUID(int=1)), str(UUID(int=101)))
    if failure == "uuid":
        pair = ("legacy-noncanonical-aggregate", "legacy-noncanonical-point")
    capture, reads, armed = history_failure_capture(failure), [], []
    replies = iter([200, 410])

    def get(path, *, timeout):
        reads.append((path, timeout))
        clock.now += 0.25
        return history_response(next(replies), *pair)

    result = wait_for_history_clear(
        SimpleNamespace(get=get),
        dict([pair]),
        10001,
        1200,
        on_present=lambda *identity: armed.append(identity),
        capture=capture,
    )
    assert result == {
        "aggregate_id": pair[0],
        "point_id": pair[1],
        "history_rows": 10001,
        "observed_transition": "200_to_automatic_410",
    }
    assert armed == [pair] and len(reads) == 2 and clock.now == 0.5
    original = ValueError("actual-arm-secret")

    def broken_arm(*_args):
        raise original

    with pytest.raises(ValueError) as caught:
        wait_for_history_clear(
            SimpleNamespace(get=lambda *_args, **_kwargs: history_response(200, *pair)),
            dict([pair]),
            10001,
            1200,
            on_present=broken_arm,
            capture=capture,
        )
    assert caught.value is original


def test_assert_site_failure_preserves_the_same_original_assertion_and_single_predicate_evaluation(
    monkeypatch,
):
    _performance, _clock = performance_time(monkeypatch)
    pair = (str(UUID(int=1)), str(UUID(int=101)))
    original, evaluated = AssertionError("actual-validation-secret"), []

    class FailedTruth:
        def __bool__(self):
            evaluated.append(1)
            raise original

    body = history_response(410, *pair).json()
    body["details"]["cleared_at"] = FailedTruth()
    capture = history_failure_capture("site")
    client = SimpleNamespace(
        get=lambda *_args, **_kwargs: SimpleNamespace(status_code=410, json=lambda: body)
    )
    with pytest.raises(AssertionError) as caught:
        wait_for_history_clear(client, dict([pair]), 10001, 1200, capture=capture)
    assert caught.value is original and evaluated == [1] and capture.site_attempts == 1


@pytest.mark.parametrize(
    "date", ["not-an-ISO-date", "0001-01-01T00:00:00+23:59", "9999-12-31T23:59:59-23:59"]
)
def test_bad_diagnostic_dates_are_null_without_changing_a_real_rest_transition(monkeypatch, date):
    _performance, _clock = performance_time(monkeypatch)
    pair, statuses = (str(UUID(int=1)), str(UUID(int=101))), iter([200, 410])
    capture = history_failure_capture()

    def get(*_args, **_kwargs):
        response = history_response(next(statuses), *pair)
        if response.status_code == 410:
            response.json()["details"]["cleared_at"] = date
        return response

    assert (
        wait_for_history_clear(
            SimpleNamespace(get=get), dict([pair]), 10001, 1200, capture=capture
        )["observed_transition"]
        == "200_to_automatic_410"
    )
    assert capture["responses"][-1]["cleared_at"] is None


def test_selector_capture_is_bounded_and_does_not_add_gets_or_clock_reads(monkeypatch):
    performance, clock = performance_time(monkeypatch)
    pair, reads, calls = (str(UUID(int=1)), str(UUID(int=101))), [], []

    def monotonic():
        calls.append(clock.now)
        return clock.now

    monkeypatch.setattr(performance.time, "monotonic", monotonic)

    def run(capture):
        replies = iter([200] * 34 + [410])
        clock.now = 0
        calls.clear()
        reads.clear()

        def get(path, *, timeout):
            reads.append((path, timeout))
            clock.now += 0.01
            response = history_response(next(replies), *pair)
            response.json()["private-body"] = {
                "raw_events": ["secret-event"],
                "checkpoint": "secret-state",
            }
            return response

        result = wait_for_history_clear(
            SimpleNamespace(get=get), dict([pair]), 10001, 1200, capture=capture
        )
        return result, list(reads), list(calls)

    baseline = run(None)
    capture = history_failure_capture()
    assert run(capture) == baseline
    assert capture["responses_observed"] == 35 and capture["responses_evicted"] == 5
    assert len(capture["responses"]) == 30
    assert all(
        set(row)
        == {
            "aggregate_id",
            "point_id",
            "status",
            "call_enter_check",
            "call_return_check",
            "cleared_at",
        }
        for row in capture["responses"]
    )
    assert "secret" not in json.dumps(capture) and "private-body" not in json.dumps(capture)


def d30_setup_failure(
        monkeypatch, tmp_path, *, emit_failure=None, shared=False, density=None,
        capture_failure=None):
    import builtins
    from dataclasses import replace

    performance, clock = performance_time(monkeypatch)
    if capture_failure is not None:
        safe = performance._capture_safely

        def faulty_metadata(operation):
            value = safe(operation)
            if isinstance(value, dict) and value.get("version") == 1:
                return FaultyCapture(value, capture_failure)
            return value

        monkeypatch.setattr(performance, "_capture_safely", faulty_metadata)
    case = load_cases()[-2 if shared else -1]
    if density is not None:
        case = replace(case, events_per_source_window=density)
    identities = [str(UUID(int=100 + index)) for index in range(case.aggregates)]
    sources = [str(UUID(int=10 + index)) for index in range(case.sources)]
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    creates, validated, diagnostics, batches = [], [], [], []
    original = AssertionError("actual-selector-secret-do-not-log")
    monkeypatch.setattr(performance, "selected_load_cases", lambda _group: (case,))

    def command(args, *, env, timeout, capture=False):
        clock.now += 0.25
        if args[-1] == "aggregate-worker":
            return ""
        if args[-1] == "import time; print(time.monotonic())":
            return str(clock.now)
        action = args[-1]
        payload = json.loads(
            next(
                value.split("=", 1)[1]
                for value in args
                if value.startswith("AGGREGATE_LOAD_PAYLOAD=")
            )
        )
        if action == "setup":
            return json.dumps(
                {
                    "sources": sources,
                    "actor": str(UUID(int=40)),
                    "anchor": anchor.isoformat(),
                    "clock_at": (anchor + timedelta(seconds=520 * 300)).isoformat(),
                    "heavy_history_rows": 10001,
                }
            )
        if action in {"write", "history"}:
            if (
                action == "write"
                and payload["timestamp"] == (anchor + timedelta(seconds=521 * 300)).isoformat()
            ):
                batches.append(payload["count"])
            return json.dumps(
                {"offset": payload["offset"] + payload.get("count", 0), "seconds": 0.125}
            )
        if action == "create":
            identity = identities[len(creates)]
            creates.append(identity)
            return json.dumps(
                {
                    "card": {
                        "id": identity,
                        "online_start_index": 520,
                        "anchor_utc": (
                            anchor + timedelta(seconds=payload["anchor_shift"])
                        ).isoformat(),
                    },
                    "seconds": 0,
                }
            )
        assert action == "inspect"
        return json.dumps(
            {
                "aggregates": {
                    identity: {
                        "old_point_id": str(UUID(int=1000 + index)),
                        "old_payloads": 1,
                        "old_snapshots": 1,
                        "membership_pairs": 10001,
                        "membership_details": 10001,
                        "old_payload_state": "present",
                    }
                    for index, identity in enumerate(identities)
                },
                "capture_pending": 0,
            }
        )

    def get(path, **_kwargs):
        if "/by-index/" in path:
            identity = path.split("/")[4]
            return SimpleNamespace(
                status_code=200,
                raise_for_status=lambda: None,
                json=lambda: {
                    "id": str(UUID(int=2000 + identities.index(identity))),
                    "execution": {},
                },
            )
        raise original

    def validate(point, _expected, *, aggregate_id, window_index, **_kwargs):
        assert window_index == 520 and point["id"] == str(
            UUID(int=2000 + identities.index(aggregate_id))
        )
        validated.append(aggregate_id)

    client = SimpleNamespace(
        get=get,
        request=lambda *_args, **_kwargs: SimpleNamespace(
            status_code=200, json=lambda: {"csrf_token": "auth-secret-do-not-log"}
        ),
    )
    monkeypatch.setattr(performance.httpx, "Client", lambda **_kwargs: nullcontext(client))
    monkeypatch.setattr(performance, "assert_computed_load_point", validate)
    monkeypatch.setattr(
        performance,
        "pending_diagnostics",
        lambda *_args, **_kwargs: diagnostics.append(sys.exception()),
    )
    dumps = json.dumps
    if emit_failure == "json":

        def broken_dumps(value, *args, **kwargs):
            if (
                isinstance(value, dict)
                and value.get("version") == 1
                and value.get("response_limit") == 30
            ):
                raise CaptureFailure("diagnostic-encoder-only")
            return dumps(value, *args, **kwargs)

        monkeypatch.setattr(performance.json, "dumps", broken_dumps)
    if emit_failure == "print":

        def broken_print(*args, **kwargs):
            if args and str(args[0]).startswith("Controlled aggregate D30 failure capture:"):
                raise CaptureFailure("diagnostic-print-only")
            builtins.print(*args, **kwargs)

        monkeypatch.setattr(performance, "print", broken_print, raising=False)
    with pytest.raises(AssertionError) as caught:
        run_aggregate_performance(
            ["compose"],
            {
                "AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json"),
                "PUBLIC_ORIGIN": "http://owned.invalid",
            },
            False,
            tmp_path,
            command,
        )
    assert caught.value is original and diagnostics == [original]
    assert set(validated) == set(identities) and len(validated) == case.aggregates
    assert batches == [1000] * (case.sources * (case.events_per_source_window // 1000))


@pytest.mark.parametrize("emit_failure", [None, "json", "print"])
def test_d30_failure_emit_is_nonfatal_and_keeps_original_exception_and_successful_w0(
    monkeypatch, tmp_path, capsys, emit_failure
):
    d30_setup_failure(monkeypatch, tmp_path, emit_failure=emit_failure)
    output = capsys.readouterr().out
    assert "actual-selector-secret" not in output and "auth-secret" not in output
    lines = [
        line
        for line in output.splitlines()
        if line.startswith("Controlled aggregate D30 failure capture:")
    ]
    if emit_failure is None:
        (line,) = lines
        report = json.loads(line.split(": ", 1)[1])
        assert report["batch_seconds"] == [0.125] * 15 and report["batches_observed"] == 15
        assert report["responses_observed"] == 0 and report["arm_write_completed"] is False
        markers = report["markers"]
        assert (
            markers["last_write_call_return_check"] - markers["first_write_call_enter_check"]
            == 15 * 0.25
        )
        assert markers["wave_started_after_write"] == markers["last_write_call_return_check"]
        assert markers["markers_ready_wait_check"] == markers["selection_scope_enter_check"]
        assert markers["markers_ready_wait_check"] > markers["wave_started_after_write"]
        assert len(line.encode("utf-8")) + 2 <= report["byte_limit"] == 16384
    else:
        assert lines == []


def test_d30_failure_carrier_does_not_activate_for_shared_case(monkeypatch, tmp_path, capsys):
    d30_setup_failure(monkeypatch, tmp_path, shared=True)
    assert "Controlled aggregate D30 failure capture:" not in capsys.readouterr().out


def test_d30_batch_metadata_stays_at_fifteen_when_the_fixture_reports_more(
    monkeypatch, tmp_path, capsys
):
    d30_setup_failure(monkeypatch, tmp_path, density=6000)
    (line,) = [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("Controlled aggregate D30 failure capture:")
    ]
    report = json.loads(line.split(": ", 1)[1])
    assert report["batches_observed"] == 18 and report["batches_dropped"] == 3
    assert report["batch_seconds"] == [0.125] * 15
    assert len(line.encode("utf-8")) + 2 <= 16384


@pytest.mark.parametrize("capture_failure", ["markers", "batches"])
def test_d30_collection_failures_preserve_real_write_results_and_original_error(
        monkeypatch, tmp_path, capsys, capture_failure):
    d30_setup_failure(monkeypatch, tmp_path, capture_failure=capture_failure)
    assert "actual-selector-secret" not in capsys.readouterr().out


def test_optional_cleared_at_uses_only_loaded_state_and_survives_bad_metadata():
    module = load_module()
    value = datetime(2026, 1, 1, tzinfo=UTC)

    class NoDescriptor:
        @property
        def cleared_at(self):
            pytest.fail("optional capture must not invoke an ORM descriptor or SQL")

    row = NoDescriptor()
    vars(row)["cleared_at"] = value
    assert module._loaded_cleared_at(row) == value.isoformat()
    del vars(row)["cleared_at"]
    assert module._loaded_cleared_at(row) is None

    class BrokenState:
        def __getattribute__(self, key):
            if key == "__dict__":
                raise CaptureFailure("optional-state-only")
            return super().__getattribute__(key)

    assert module._loaded_cleared_at(BrokenState()) is None
    assert module._loaded_cleared_at(None) is None


@pytest.mark.parametrize("actual_failure", [False, True])
def test_inspection_optional_date_cannot_change_success_queries_or_the_original_failure(
    actual_failure,
):
    module = load_module()
    identity, point, source = UUID(int=1), UUID(int=101), UUID(int=10)
    original, calls = ValueError("actual-inspection-secret"), []

    class BrokenDateState:
        origin_point_id = point
        schedule_id = UUID(int=2)
        payload_state, clear_reason = "cleared", "automatic"

        def __getattribute__(self, key):
            if key == "__dict__":
                raise CaptureFailure("optional-loaded-date-only")
            return super().__getattribute__(key)

    schedule = SimpleNamespace(next_index=521, execution_generation=0, active_cleanup_id=None)
    state = SimpleNamespace(
        auto_cleared_total=9,
        next_check_at=None,
        pending_closure_id=None,
        pending_phase=None,
        status="idle",
        reason="model_history_reserve",
    )
    scalars = iter([BrokenDateState(), schedule, 520, 0, 0, 0, 10001, 0, 0])

    class Session:
        def execute(self, _statement):
            calls.append("execute")
            return SimpleNamespace(one=lambda: (30, 0), all=list)

        def get(self, _model, _key):
            calls.append("get")
            return state

        def scalar(self, _statement):
            calls.append("scalar")
            value = next(scalars)
            if actual_failure and calls.count("scalar") == 9:
                raise original
            return value

    database = SimpleNamespace(session_factory=lambda: nullcontext(Session()))
    payload = {"aggregates": [str(identity)], "sources": [str(source)]}
    if actual_failure:
        with pytest.raises(ValueError) as caught:
            module.inspect(database, payload)
        assert caught.value is original
    else:
        result = module.inspect(database, payload)
        row = result["aggregates"][str(identity)]
        assert row["old_cleared_at"] is None and row["membership_pairs"] == 10001
        assert row["membership_details"] == 0 and result["capture_pending"] == 0
    assert calls.count("execute") == 5 and calls.count("scalar") == 9 and calls.count("get") == 1


@pytest.mark.parametrize("optimized", [False, True])
def test_all_initial_410_keeps_its_unconditional_failure_even_when_site_capture_fails(
    tmp_path, optimized
):
    import os
    import subprocess

    program = r"""
from types import SimpleNamespace
from uuid import UUID
from scripts.aggregate_performance import wait_for_history_clear
class BrokenCapture(dict):
    def __setitem__(self, key, value):
        if key == "assert_site":
            raise RuntimeError("diagnostic-site-only")
        return super().__setitem__(key, value)
pair = str(UUID(int=1)), str(UUID(int=101))
body = {"code":"aggregate_point_cleared","details":{"origin_point_id":pair[1],
    "clear_reason":"automatic","clear_policy_version":"model_history_v1",
    "cleared_at":"2026-01-01T00:00:00Z","window_index":8,
    "origin":"initial_history","time_basis":"event_time"}}
capture = BrokenCapture(responses_observed=0,responses_evicted=0,responses=[],markers={})
client = SimpleNamespace(get=lambda *args,**kwargs:
    SimpleNamespace(status_code=410,json=lambda:body))
try:
    wait_for_history_clear(client,dict([pair]),10001,10**15,capture=capture)
except AssertionError as error:
    expected = ("All old history targets were already cleared "
        "without an observed 200-to-410 transition")
    if str(error) != expected:
        raise SystemExit(2)
else:
    raise SystemExit(3)
"""
    env = dict(os.environ)
    env.update(
        TEMP=str(tmp_path),
        TMP=str(tmp_path),
        TMPDIR=str(tmp_path),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONUTF8="1",
        PYTHONIOENCODING="utf-8",
        PYTHONPYCACHEPREFIX=str(tmp_path / "pycache"),
    )
    env.pop("PYTHONOPTIMIZE", None)
    command = [sys.executable, "-B", *(["-O"] if optimized else []), "-c", program]
    result = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _generation_marker(state="begin", **changes):
    return {"run_id": "owned-run", "aggregate_id": str(UUID(int=1)), "nonce": str(UUID(int=2)),
        "phase": "generation", "state": state, **changes}


def _generation_job(sequence, kind="foreground", **changes):
    module = clock_module()
    return {"job_id": "a" * 32 + ":" + str(sequence), "aggregate_id": str(UUID(int=1)), "kind": kind,
        **{key: 10 + index / 10 for index, key in enumerate(module.MaintenanceJobCapture.TIMESTAMPS)}, **changes}


def test_generation_boundaries_use_the_existing_atomic_clock_read_and_exactly_two_timestamps(tmp_path, monkeypatch):
    module = clock_module()
    clock = module.ScopedClock(tmp_path / "clock.json")
    phase = clock.phase_capture = module.GenerationPhaseCapture("owned-run")
    stamps = Mock(side_effect=[100., 120.])
    monkeypatch.setattr(module.time, "monotonic", stamps)
    def publish(marker, moment):
        temporary = clock.path.with_suffix(".next")
        temporary.write_text(json.dumps({"aggregates": {str(UUID(int=1)): moment}, "generation_phase": marker}))
        temporary.replace(clock.path)
        clock.refresh()
    publish(_generation_marker(), "2026-11-18T00:00:00+00:00")
    assert clock.values[UUID(int=1)] == datetime(2026, 11, 18, tzinfo=UTC)
    assert phase.snapshot()["state"] == "active" and phase.sequence == 1
    clock.refresh()
    publish(_generation_marker(), "2026-11-18T00:00:01+00:00")  # Replay never resets.
    assert phase.snapshot()["begin_observed_monotonic"] == 100. and stamps.call_count == 1
    publish(_generation_marker("end", outcome="complete"), "2026-11-18T00:00:01+00:00")
    assert phase.snapshot()["complete"] and phase.sequence == 2 and stamps.call_count == 2


@pytest.mark.parametrize("change", [{"run_id": "foreign"}, {"aggregate_id": str(UUID(int=9))},
    {"phase": "resume"}, {"nonce": "invalid"}, {"raw": "secret"}])
def test_generation_foreign_or_malformed_markers_are_nonfatal_and_do_not_start_capture(change, monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    stamp = Mock(side_effect=AssertionError("foreign marker must not read a clock"))
    monkeypatch.setattr(module.time, "monotonic", stamp)
    phase.safe(phase.observe_marker, _generation_marker(**change), {UUID(int=1): None})
    assert phase.snapshot() is None and phase.error_count == 1 and not stamp.called


def test_generation_scope_replay_and_missing_boundaries_never_report_complete(monkeypatch):
    module = clock_module()
    stamps = Mock(side_effect=[100., 101., 102.])
    monkeypatch.setattr(module.time, "monotonic", stamps)
    phase = module.GenerationPhaseCapture("owned-run")
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    phase.observe_marker(_generation_marker("end", nonce=str(UUID(int=3))), {UUID(int=1): None})
    assert phase.snapshot()["state"] == "active" and not phase.snapshot()["complete"]
    phase.observe_marker(_generation_marker("end", outcome="failed"), {UUID(int=1): None})
    assert phase.snapshot()["boundaries_complete"] and not phase.snapshot()["complete"]
    frozen = phase.snapshot()
    phase.observe_marker(_generation_marker("end", outcome="complete"), {UUID(int=1): None})
    assert phase.snapshot() == frozen
    fragment = module.GenerationPhaseCapture("owned-run")
    fragment.observe_marker(_generation_marker("end", outcome="complete"), {UUID(int=1): None})
    assert fragment.snapshot()["counters"] is None
    assert not fragment.snapshot()["boundaries_complete"] and not fragment.snapshot()["complete"]
    assert stamps.call_count == 3


def test_generation_full_counters_survive_carrier_eviction_and_long_resume_without_new_clocks(monkeypatch):
    module = clock_module()
    phase, carrier = module.GenerationPhaseCapture("owned-run"), module.MaintenanceJobCapture()
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    with monkeypatch.context() as patch:
        patch.setattr(module.time, "monotonic", Mock(side_effect=AssertionError("new job clock")))
        for sequence in range(1, 521):
            row = _generation_job(sequence, "maintenance", execute_completed_monotonic=11 + sequence / 100)
            phase.discovered("maintenance", 10., 10.1, [UUID(int=1)])
            for stage in ("offered", "admitted", "completed"):
                phase.job_event(stage, row)
            carrier.append(row)
    phase.observe_marker(_generation_marker("end", outcome="complete"), {UUID(int=1): None})
    saved = phase.snapshot()
    for sequence in range(521, 900):
        row = _generation_job(sequence, "maintenance")
        carrier.append(row)
        phase.job_event("offered", row)
    assert phase.snapshot() == saved
    assert saved["counters"]["maintenance"] == {name: 520 for name in phase.STAGES}
    assert saved["observed_jobs"] == 520 and saved["pending_jobs"] == 0
    assert len(saved["latest_jobs"]) == len(saved["slowest_jobs"]) == 10
    assert saved["latest_jobs"][-1]["job_id"].endswith(":520") and saved["records_omitted"] >= 500
    assert not any(row["job_id"].endswith(":520") for row in carrier.snapshot()["records"])
    saved["counters"]["maintenance"]["completed"] = -1
    assert phase.snapshot()["counters"]["maintenance"]["completed"] == 520


def test_generation_preexisting_pending_commit_links_and_wait_reasons_are_explicit(monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    phase.live_provider = lambda: [_generation_job(1), _generation_job(2, aggregate_id=str(UUID(int=9)))]
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    assert phase.snapshot()["preexisting_jobs"] == phase.snapshot()["pending_jobs"] == 1
    assert phase.snapshot()["counters"]["foreground"]["offered"] == 0
    row = _generation_job(1)
    phase.job_event("completed", row, {"point_id": str(UUID(int=3)), "origin": "online"})
    phase.point_ack(UUID(int=3), 10.75, 10.8)
    phase.physical({"aggregate_id": str(UUID(int=1)), "job_id": row["job_id"], "sequence": 4,
        "closure_id": str(UUID(int=3)), "phase": "memberships", "started_monotonic": 10.2,
        "sql_completed_monotonic": 10.5, "committed_monotonic": 10.6})
    phase.physical({"aggregate_id": str(UUID(int=1)), "sequence": 5, "factor": "operation_seconds"}, error=True)
    target = UUID(int=1)
    admission = SimpleNamespace(active={target: SimpleNamespace(kind="foreground")}, ready=[], capacity=1)
    phase.no_admission(admission)
    admission.active = {UUID(int=9): SimpleNamespace(kind="foreground")}
    phase.no_admission(admission)
    admission.active = {}
    phase.no_admission(admission)
    saved = phase.snapshot()
    assert saved["pending_jobs"] == 0 and saved["missing_links"] == 1
    assert saved["acquire_none_observations"] == {"target_active": 1, "capacity_full": 1,
        "not_queued": 1, "maintenance_active": 0, "unknown": 0}
    assert saved["latest_jobs"][0]["point_commit_ack_monotonic"] == 10.8
    assert saved["span_links"][0]["job_id"] == row["job_id"]
    assert saved["budget_error_links"][0]["job_id"] is None
    assert saved["duration_sums_seconds"]["foreground"]["commit_ack"] == pytest.approx(.05)
    assert saved["duration_sums_seconds"]["maintenance"]["commit_ack"] == pytest.approx(.1)


def test_generation_serialization_bound_and_failures_are_diagnostic_only(monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    for sequence in range(1, 40):
        row = _generation_job(sequence)
        phase.job_event("offered", row)
        phase.job_event("completed", row)
    phase.BYTE_LIMIT = 4000
    saved = phase.snapshot()
    assert len(json.dumps(saved, allow_nan=False).encode("ascii")) + 32 <= 4000
    assert saved["byte_cap_dropped"] > 0 and saved["observed_jobs"] == 39
    def broken():
        raise ValueError("private diagnostic detail")
    phase.safe(broken)
    assert phase.snapshot()["capture_error_types"] == ["ValueError"]
    assert "private" not in json.dumps(phase.snapshot())
    monkeypatch.setattr(phase, "_snapshot", broken)
    assert phase.snapshot()["state"] == "unavailable" and not phase.snapshot()["boundaries_complete"]
    class DiagnosticAbort(BaseException):
        pass
    failure = DiagnosticAbort()
    with pytest.raises(DiagnosticAbort) as caught:
        phase.safe(lambda: (_ for _ in ()).throw(failure))
    assert caught.value is failure


def test_generation_real_ack_hook_reuses_the_existing_publish_clock_and_records_unlinked_work(monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    stamp = Mock(return_value=10.9)
    monkeypatch.setattr(module.time, "monotonic", stamp)
    metrics = module.MaintenanceMetrics(phase)
    pending = {"aggregate_id": str(UUID(int=1)), "job_id": "a" * 32 + ":1", "phases": [
        {"phase": "outcomes", "closure_id": str(UUID(int=3)), "rows_changed": 2,
            "started_monotonic": 10.5, "sql_completed_monotonic": 10.8}]}
    metrics.publish(pending)
    metrics.record_budget_error({"aggregate_id": str(UUID(int=1)), "job_id": "a" * 32 + ":1",
        "factor": "operation_seconds"})
    assert stamp.call_count == 1 and pending["phases"] == []
    saved = phase.snapshot()
    assert saved["span_observed"] == saved["budget_error_observed"] == 1
    assert saved["span_links"][0]["committed_monotonic"] == 10.9
    job = SimpleNamespace(aggregate_id=UUID(int=1), kind="foreground")
    phase.unlinked("admitted", job)
    phase.unlinked("completed", job)
    saved = phase.snapshot()
    assert saved["counters"]["foreground"]["admitted"] == saved["counters"]["foreground"]["completed"] == 1
    assert saved["missing_links"] == 2


@pytest.mark.parametrize("captured_maintenance_worker", [True], indirect=True)
@pytest.mark.parametrize("fault", ["write", "serialization"])
def test_generation_marker_only_export_failure_is_nonfatal_and_does_not_repeat_io(
        captured_maintenance_worker, monkeypatch, fault):
    setup = captured_maintenance_worker
    if fault == "write":
        written = Mock(side_effect=OSError("private export failed"))
        monkeypatch.setattr(Path, "write_text", written)
    else:
        original = setup.module.json.dumps
        def encode(body, **kwargs):
            if isinstance(body, dict) and "generation_phase" in body:
                raise TypeError("private export failed")
            return original(body, **kwargs)
        monkeypatch.setattr(setup.module.json, "dumps", encode)
    payload = {"admission": setup.worker.admission.summary(), "steps": []}
    assert setup.worker.observe(payload) is None
    assert setup.phase.error_count == 1
    assert setup.worker.observe(payload) is None  # No retry/poll for better metadata.
    if fault == "write":
        assert written.call_count == 1
    else:
        assert "generation_phase" not in json.loads(setup.result_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("canonical_end", ["complete", "failed", None])
def test_canonical_clock_capture_survives_generation_and_keeps_boundary_jobs_explicit(
        tmp_path, monkeypatch, canonical_end):
    module = clock_module()
    clock = module.ScopedClock(tmp_path / "clock.json")
    phase = clock.phase_capture = module.GenerationPhaseCapture("owned-run")
    target = UUID(int=1)
    values = {str(target): "2026-11-18T00:00:00+00:00"}
    marker = _generation_marker(phase="canonical", nonce=str(UUID(int=3)))
    stamps = Mock(side_effect=[10., 20., 30., 40.])
    resources = Mock(side_effect=[
        {"process_cpu_seconds": 1.5, "cgroup_memory_bytes": 1000, "cgroup_cpu_seconds": 2.5},
        {"process_cpu_seconds": 4., "cgroup_memory_bytes": 1100, "cgroup_cpu_seconds": 5.5}])
    monkeypatch.setattr(module.time, "monotonic", stamps)
    monkeypatch.setattr(module, "canonical_resources", resources)

    def publish(name, body):
        temporary = clock.path.with_suffix(".next")
        temporary.write_text(json.dumps({"aggregates": values, name + "_phase": body}))
        temporary.replace(clock.path)
        clock.version = None
        clock.refresh()

    publish("canonical", marker)
    assert clock.values[target] == datetime(2026, 11, 18, tzinfo=UTC)
    crossing = _generation_job(9)
    phase.job_event("offered", crossing)
    phase.live_provider = lambda: [crossing]
    for sequence, (kind, step, operation) in enumerate((
            ("foreground", {"origin": "initial_history"}, "history"),
            ("foreground", {"captured_operation": "scan"}, "scan"),
            ("foreground", {}, "other_foreground"),
            ("maintenance", {}, "maintenance")), start=1):
        row = _generation_job(sequence, kind)
        phase.job_event("offered", row)
        phase.job_event("completed", row, step)
        totals = phase.snapshot("canonical")["work_by_operation"][operation]
        assert totals["completed"] == 1 and totals["duration_sums_seconds"]["execution"] == pytest.approx(.1)
    if canonical_end is not None:
        publish("canonical", {**marker, "state": "end", "outcome": canonical_end})
    canonical = phase.snapshot("canonical")
    assert canonical["pending_jobs"] == 1 and canonical["boundaries_complete"] == (canonical_end is not None)
    assert canonical["complete"] == (canonical_end == "complete")
    assert canonical["resource_boundaries"]["begin"]["process_cpu_seconds"] == 1.5
    assert canonical["resource_boundaries"]["end"] == (
        {"process_cpu_seconds": 4., "cgroup_memory_bytes": 1100, "cgroup_cpu_seconds": 5.5}
        if canonical_end is not None else None)
    assert len(json.dumps(canonical).encode("ascii")) + 32 <= 64 * 1024
    publish("generation", _generation_marker())
    # The old in-flight job is covered explicitly as present at the next
    # boundary. A later completion cannot change the retained canonical facts.
    assert phase.snapshot("generation")["preexisting_jobs"] == 1
    phase.job_event("completed", crossing)
    publish("generation", _generation_marker("end", outcome="complete"))
    assert phase.snapshot("canonical") == canonical
    generation = phase.snapshot("generation")
    assert generation["pending_jobs"] == 0 and generation["complete"]
    assert "work_by_operation" not in generation and "resource_boundaries" not in generation
    assert stamps.call_count == (4 if canonical_end is not None else 3)
    assert resources.call_count == (2 if canonical_end is not None else 1)


def test_stale_canonical_marker_does_not_reset_generation_when_previous_snapshot_unavailable(monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    values = {UUID(int=1): None}
    monkeypatch.setattr(module, "canonical_resources", lambda: {"process_cpu_seconds": 2.})
    marker = _generation_marker(phase="canonical", nonce=str(UUID(int=3)))
    phase.observe_marker(marker, values)
    with monkeypatch.context() as patch:
        patch.setattr(phase, "_snapshot", Mock(side_effect=ValueError("private artifact detail")))
        phase.observe_marker(_generation_marker(), values)
    assert phase.snapshot("canonical")["state"] == "unavailable"
    assert phase.snapshot("canonical")["capture_error"] == "ValueError"
    phase.observe_marker({**marker, "state": "end", "outcome": "complete"}, values)
    assert phase.snapshot("generation")["state"] == "active"
    assert phase.snapshot("canonical")["boundaries_complete"] is False


def test_canonical_transport_pressure_retains_totals_before_dropping_optional_phase(monkeypatch):
    module = clock_module()
    phase = {"counters": {"foreground": {"completed": 10}}, "retained_job_ids": 1, "records_omitted": 9,
        "slowest_jobs": [{"job_id": "a" * 32 + ":1", "irrelevant_detail_for_this_check": "x" * 400}]}
    body = {"history_first": {"untouched": True}, "canonical_phase": phase}
    monkeypatch.setattr(module, "WORKER_METRICS_READER_LIMIT", 512)
    compact = json.loads(module.encode_worker_metrics(body))
    assert compact["canonical_phase"]["counters"] == phase["counters"]
    assert compact["canonical_phase"]["records_omitted"] == 10
    assert compact["canonical_phase"]["byte_cap_dropped"] == 1
    assert compact["canonical_phase"]["slowest_jobs"] == []
    assert body["canonical_phase"]["retained_job_ids"] == 1
    monkeypatch.setattr(module, "WORKER_METRICS_READER_LIMIT", 64)
    assert json.loads(module.encode_worker_metrics(body)) == {"history_first": {"untouched": True}}


def test_generation_counts_only_explicit_online_results_and_available_timings(monkeypatch):
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    values = {UUID(int=1): None}
    phase.observe_marker(_generation_marker(), values)
    point = str(UUID(int=3))
    online = {"state": "closed", "origin": "online", "point_id": point}
    results = (
        ({"state": "complete", "result": {**online, "calculation_status": "computed",
             "execution": {"timings": {"snapshot": .11}},
             "timings": {"total": .5, "save": .04, "commit": .02, "private": "value"}}}, {}),
        ({"state": "complete", "result": {**online, "calculation_status": "computed"}}, {}),
        ({"state": "complete", "result": {**online, "calculation_status": "uncomputed",
             "execution": {"timings": {"snapshot": float("nan")}},
             "timings": {"save": True, "commit": -.005}}}, {}),
        ({"state": "complete", "result": online}, {}),
        ({"state": "complete", "result": {**online, "state": "already_closed",
             "calculation_status": "computed"}}, {}),
        ({"state": "complete", "result": {**online, "point_id": None,
             "calculation_status": "computed"}}, {}),
        ({"state": "complete", "result": {**online, "origin": "initial_history",
             "calculation_status": "computed"}}, {}),
        ({"state": "waiting"}, {"captured_operation": "scan"}),
        ({"state": "complete", "result": {"state": "history_waiting"}}, {}),
        ({"state": "waiting"}, {}),
        ("waiting", {}),
        ("failed", {}),
    )
    with monkeypatch.context() as patch:
        patch.setattr(module.time, "monotonic", Mock(side_effect=AssertionError("new job clock")))
        for index, (result, step) in enumerate(results, start=1):
            phase.job_event("completed", _generation_job(index), step, result)
        phase.unlinked("completed", SimpleNamespace(aggregate_id=UUID(int=1), kind="foreground"))
        phase.job_event("completed", _generation_job(14, kind="maintenance"), worker_result=results[0][0])
    phase.observe_marker(_generation_marker("end", outcome="failed"), values)
    saved = phase.snapshot()
    assert saved["online_results"] == {"computed": 2, "uncomputed": 1, "unknown": 1}
    assert saved["other_foreground_results"] == {"scan": 1, "history": 1, "waiting": 3, "unknown": 4}
    assert sum(saved["online_results"].values()) + sum(saved["other_foreground_results"].values()) == (
        saved["counters"]["foreground"]["completed"])
    assert saved["online_timings"] == {
        "snapshot": {"samples": 1, "seconds": .11},
        "save": {"samples": 1, "seconds": .04},
        "commit": {"samples": 1, "seconds": .02},
    }
    assert saved["counters"]["maintenance"]["completed"] == 1 and saved["missing_links"] == 1
    assert "private" not in json.dumps(saved, allow_nan=False)
    phase.job_event("completed", _generation_job(15), worker_result=results[0][0])
    assert phase.snapshot() == saved


def test_generation_missing_timing_evidence_stays_null_and_scan_tag_is_scoped():
    module = clock_module()
    phase = module.GenerationPhaseCapture("owned-run")
    phase.observe_marker(_generation_marker(), {UUID(int=1): None})
    scan = {"state": "scanned", "records": 1, "work_available": False}
    phase.classify_foreground(scan)
    assert phase.operation.get() is None
    operation = {}
    token = phase.operation.set(operation)
    try:
        phase.classify_foreground(scan)
        assert operation == {"kind": "scan"}
    finally:
        phase.operation.reset(token)
    assert phase.operation.get() is None
    assert phase.snapshot()["online_timings"] == {
        name: {"samples": 0, "seconds": None} for name in ("snapshot", "save", "commit")
    }
