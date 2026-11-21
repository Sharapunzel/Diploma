"""Permanent selector, fixture and early-failure checks for smoke browser mode."""
import runpy
from itertools import pairwise
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import browser_test_scenarios as browser

FIXTURE_CONTAINER = "a" * 64


def arguments(**changes):
    return SimpleNamespace(**{"profile":"smoke-development", "browser_only":True, "browser_spec":None,
        "detector_only":False,"aggregate_only":False,"rcf":False,"wal":False,"pytest":None, **changes})


def test_mode_selection_is_explicit_and_includes_all_standard_families():
    dev = browser.validate_browser_arguments(arguments())
    assert {"auth.spec.ts","events.spec.ts","resources.spec.ts","normalizers.spec.ts",
            "aggregates.live.spec.ts","aggregates.ui.spec.ts","detectors.ui.spec.ts","detectors.live.spec.ts"} <= set(dev)
    for family in ("resources", "normalizers"):
        assert {p.name for p in (browser.ROOT / "frontend/browser").glob(f"{family}.*.spec.ts")} <= set(dev)
    assert browser.validate_browser_arguments(arguments(profile="smoke-production")) == list(browser.PRODUCTION)
    assert "aggregates.live.spec.ts" in browser.PRODUCTION
    assert {"findings.ui.spec.ts", "findings.live.spec.ts"} <= set(dev)
    assert browser.validate_browser_arguments(arguments(
        profile="smoke-production", browser_spec=["findings.live.spec.ts"])) == ["findings.live.spec.ts"]
    assert "external.live.spec.ts" not in dev
    assert "deadline.spec.ts" not in dev
    assert "agents.tls.spec.ts" not in dev


@pytest.mark.parametrize("changes", [{"browser_only":False,"browser_spec":["auth.spec.ts"]},
    {"profile":"frontend"},{"detector_only":True},{"aggregate_only":True},{"rcf":True},{"wal":True},{"pytest":[]},
    {"browser_spec":["../auth.spec.ts"]},{"browser_spec":["external.live.spec.ts"]},
    {"profile":"smoke-production","browser_spec":["auth.spec.ts"]},{"browser_spec":[]}])
def test_incompatible_selection_fails_before_preparation(changes):
    with pytest.raises(ValueError):
        browser.validate_browser_arguments(arguments(**changes))


def test_required_fixture_is_error_not_skip(tmp_path):
    path = tmp_path / "fixture.json"
    with pytest.raises(FileNotFoundError):
        browser.require_fixture(path,["aggregate"])
    path.write_text('{"aggregate":null}',encoding="utf-8")
    with pytest.raises(RuntimeError):
        browser.require_fixture(path,["aggregate"])


def test_findings_prepare_only_for_live_without_workers_and_rejects_missing_fixture():
    command = Mock(return_value="{}")
    assert browser.prepare_findings(["compose"], {}, command, ["findings.ui.spec.ts"], "admin", "secret") is None
    command.assert_not_called()
    with pytest.raises(RuntimeError, match="findings browser fixture"):
        browser.prepare_findings(["compose"], {}, command, ["findings.live.spec.ts"], "admin", "secret")
    args = command.call_args.args[0]
    assert args[0:3] == ["compose", "exec", "-T"]
    assert "api" in args and "detector-worker" not in args and "aggregate-worker" not in args
    assert command.call_args.kwargs["timeout"] == 30


def test_history_readiness_requires_recent_complete_working_windows():
    points=[{"kind":"working","window_index":i,"origin":"online", "time_basis":"event_time",
             "payload_state":"present","end_utc":f"{i:03}","completeness":"complete"} for i in range(16)]
    assert browser.ready_history(points)
    assert not browser.ready_history(points[:15])
    assert not browser.ready_history([*points,{**points[-1],"window_index":16,"completeness":"unknown"}])
    assert not browser.ready_history([{**p,"kind":"historical"} for p in points])
    assert not browser.ready_history([{**p,"window_index":p["window_index"] * 2} for p in points])
    assert not browser.ready_history([{**p,"window_index":0} for p in points])
    assert not browser.ready_history([{**p,"time_basis":"availability_at"} for p in points])
    assert not browser.ready_history([{**p,"payload_state":"cleared"} for p in points])


def test_aggregate_preparation_keeps_old_seed_and_prepares_saved_window_at_launch(tmp_path, monkeypatch):
    import json
    from datetime import UTC, datetime

    from scripts import aggregate_test_scenarios

    data = {"sources":["one", "two"], "empty_source":"empty",
            "saved_window_sources":["saved-one", "saved-two"],
            "verified_from":"2026-11-15T00:00:00+00:00"}
    commit = "2026-11-15T00:00:04.123456+00:00"
    command = Mock(side_effect=[json.dumps(data), "", commit, "", commit])
    now = datetime(2026, 11, 15, 0, 0, 4, tzinfo=UTC)
    prepared = aggregate_test_scenarios.aggregate_runtime_smoke(
        [], {}, False, command, prepare_only=True, saved_window=True)
    assert prepared["anchor"] == "2000-01-01T00:00:00+00:00"
    seed = command.call_args_list[1].args[0]
    assert "SMOKE_EVENT_AT=" + prepared["anchor"] in seed
    assert 'SMOKE_SOURCES=["one", "two"]' in seed
    assert not any(item.startswith("SMOKE_START=") for item in seed)
    path = tmp_path / "aggregate-fixture.json"
    path.write_text(json.dumps(prepared), encoding="utf-8")
    monkeypatch.setattr(aggregate_test_scenarios, "datetime", SimpleNamespace(
        now=lambda zone: now, fromisoformat=datetime.fromisoformat))
    aggregate_test_scenarios.prepare_saved_window_fixture([], {}, command, path)
    final = json.loads(path.read_text(encoding="utf-8"))
    assert final["sources"] == prepared["sources"] and final["anchor"] == prepared["anchor"]
    assert final["saved_window"] == {"sources": ["saved-one", "saved-two"],
        "anchor": "2026-11-14T23:59:54+00:00"}
    for call in command.call_args_list[3:]:
        assert 'SMOKE_SOURCES=["saved-one", "saved-two"]' in call.args[0]
    assert "SMOKE_EVENT_AT=" + final["saved_window"]["anchor"] in command.call_args_list[3].args[0]
    # The actual commands remain reusable Python programs, not ad hoc SQL seeds.
    for call in command.call_args_list:
        arguments = call.args[0]
        compile(arguments[arguments.index("-c") + 1], "browser-preparation", "exec")


def test_auto_history_capture_requires_real_payload_and_snapshot_before_deletion(monkeypatch):
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}
    facts = {"ready": True, "old_point_id": "real-point", "old_window_index": 0,
        "canonical_history_count": 520, "old_payloads": 1, "old_snapshots": 1,
        "payload_state": "present", "old_point": {"id": "real-point"}}
    check, readback = Mock(), Mock(side_effect=[{"ready": False, "canonical_history_count": 0}, facts])
    monkeypatch.setattr(browser, "check_fixture", check)
    monkeypatch.setattr(browser, "auto_history_facts", readback)
    now = [0.0]
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    browser.capture_auto_history([], {}, Mock(), config, 60, fixture_container=FIXTURE_CONTAINER)
    assert check.call_count == 1 and readback.call_count == 2
    assert config["auto_cleanup"]["old_point_id"] == "real-point"
    assert config["auto_cleanup"]["old_point"] == {"id": "real-point"}
    assert config["auto_cleanup"]["seed_windows"] == 520


def test_auto_history_waits_for_all_canonical_closures_and_keeps_before_proof_after_gc(monkeypatch):
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}
    old_point = {"id": "original", "completeness": "complete", "features": [{"value": 1}]}
    before = {"ready": True, "old_point_id": "original", "old_window_index": 0, "old_payloads": 1,
        "old_snapshots": 1, "payload_state": "present", "old_point": old_point, "canonical_history_count": 1}
    readback = Mock(side_effect=[before, {"canonical_history_count": 519},
        {"canonical_history_count": 520, "old_payloads": 0, "old_snapshots": 0,
            "payload_state": "cleared", "old_point": None}])
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser, "auto_history_facts", readback)
    now = [0.0]
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    browser.capture_auto_history([], {}, Mock(), config, 60, fixture_container=FIXTURE_CONTAINER)
    assert [call.kwargs["readiness_only"] for call in readback.call_args_list] == [False, True, True]
    assert config["auto_cleanup"]["canonical_history_count"] == 520
    assert config["auto_cleanup"]["old_payloads"] == config["auto_cleanup"]["old_snapshots"] == 1
    assert config["auto_cleanup"]["payload_state"] == "present" and config["auto_cleanup"]["old_point"] == old_point


@pytest.mark.parametrize("count", [None, True, -1, 521, 520.0])
def test_auto_history_invalid_canonical_count_cannot_admit_browser(monkeypatch, count):
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser, "auto_history_facts", Mock(return_value={"ready": True,
        "old_payloads": 1, "old_snapshots": 1, "old_point": {"id": "present"}, "canonical_history_count": count}))
    monkeypatch.setattr(browser.time, "monotonic", lambda: 0)
    with pytest.raises(RuntimeError, match="count is unavailable or invalid"):
        browser.capture_auto_history([], {}, Mock(), {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}, 60, fixture_container=FIXTURE_CONTAINER)


@pytest.mark.parametrize("count", [519, 520])
def test_auto_history_results_cannot_reset_the_original_deadline(monkeypatch, count):
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}
    monkeypatch.setattr(browser, "check_fixture", Mock())
    now = [0]
    def facts(*_args, readiness_only, **_kwargs):
        if readiness_only:
            now[0] = 60
            return {"canonical_history_count": count}
        return {"ready": True, "old_payloads": 1, "old_snapshots": 1,
            "old_point": {"id": "present"}, "canonical_history_count": 1}
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    with pytest.raises(RuntimeError, match="preparation/test budget"):
        browser.capture_auto_history([], {}, Mock(), config, 60, fixture_container=FIXTURE_CONTAINER)
    assert config["auto_cleanup"]["old_payloads"] == 1
    assert config["auto_cleanup"]["canonical_history_count"] == 1


def test_new_canonical_proof_and_count_are_independent_of_finished_startup_sixty(monkeypatch):
    now = [100]
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}
    before = {"id": "before", "payload_state": "present"}
    def facts(*_args, readiness_only, **_kwargs):
        if readiness_only:
            now[0] = 239
            return {"canonical_history_count": 520, "old_point": None, "old_payloads": 0}
        return {"ready": True, "old_payloads": 1, "old_snapshots": 1,
            "old_point": before, "canonical_history_count": 1}
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    browser.capture_auto_history([], {}, Mock(), config, 240, fixture_container=FIXTURE_CONTAINER, started=0)
    assert now[0] > 60 and config["auto_cleanup"]["canonical_history_count"] == 520
    assert config["auto_cleanup"]["old_point"] is before
    assert config["auto_cleanup"]["old_payloads"] == config["auto_cleanup"]["old_snapshots"] == 1


@pytest.mark.parametrize("late_phase", ["proof", "canonical"])
def test_canonical_deadline_rejects_both_late_before_proof_and_late_full_count(monkeypatch, late_phase):
    now = [0]
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520}}
    def facts(*_args, readiness_only, **_kwargs):
        if readiness_only:
            now[0] = 240
            return {"canonical_history_count": 520}
        if late_phase == "proof":
            now[0] = 240
        return {"ready": True, "old_payloads": 1, "old_snapshots": 1,
            "old_point": {"id": "before"}, "canonical_history_count": 1}
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    with pytest.raises(RuntimeError, match="preparation/test budget"):
        browser.capture_auto_history([], {}, Mock(), config, 240, fixture_container=FIXTURE_CONTAINER)
    assert ("old_point" in config["auto_cleanup"]) is (late_phase == "canonical")


def test_owned_commands_share_the_remaining_budget_without_expanding_short_timeouts(monkeypatch):
    now = [100]
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    command = Mock(return_value="done")
    bounded = browser.bounded_browser_command(command, 120)
    assert bounded(["short"], timeout=5) == "done"
    assert command.call_args.kwargs["timeout"] == 5
    now[0] = 119
    bounded(["last"], timeout=30)
    assert command.call_args.kwargs["timeout"] == 1
    now[0] = 120
    with pytest.raises(RuntimeError, match="budget exhausted"):
        bounded(["never-start"])
    assert command.call_count == 2


def test_submillisecond_remainder_never_disables_playwright_global_timeout(monkeypatch):
    monkeypatch.setattr(browser.time, "monotonic", lambda: 1199.9999)
    with pytest.raises(RuntimeError, match="budget exhausted"):
        browser.browser_timeout_milliseconds(1200)


def test_auto_history_readiness_only_rpc_does_not_resend_the_before_dto():
    command = Mock(return_value='{"canonical_history_count":520}')
    result = browser.auto_history_facts([], {}, command,
        {"aggregate": "owned", "old_point": {"id": "before-proof"}}, readiness_only=True, timeout=3.5)
    args = command.call_args.args[0]
    assert args[:6] == ["exec", "-T", "api", "python", "-B", "-c"]
    assert args[-2:] == ["owned", "3.5"]
    assert command.call_args.kwargs["timeout"] == 3.5
    assert "before-proof" not in " ".join(args)
    assert result == {"canonical_history_count": 520}
    compile(args[args.index("-c") + 1], "auto-readiness-helper", "exec")


def test_canonical_observer_cadence_and_failed_probe_keep_last_confirmed_count(monkeypatch):
    now = [0.0]
    count_starts, health_starts = [], []
    failure = RuntimeError("owned readiness timed out")
    config = {"auto_cleanup": {"aggregate": "owned", "seed_windows": 520, "old_point": {"id": "early"}}}
    observation = browser.CanonicalObservation(0, 240)

    def facts(*_args, **_kwargs):
        count_starts.append(now[0])
        now[0] += .04
        if len(count_starts) == 4:
            raise failure
        return {"canonical_history_count": len(count_starts), "computed_count": len(count_starts),
            "uncomputed_count": 0, "connect_seconds": .005, "sql_seconds": .002}

    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(browser.time, "sleep", lambda delta: now.__setitem__(0, now[0] + delta))
    monkeypatch.setattr(browser, "check_fixture", lambda *_args: health_starts.append(now[0]) or True)
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    with pytest.raises(RuntimeError) as caught:
        browser.capture_auto_history([], {}, Mock(), config, 240, fixture_container=FIXTURE_CONTAINER, observation=observation)
    assert caught.value is failure
    assert all(end - start >= 1 for start, end in pairwise(count_starts))
    assert len(health_starts) == 2 and health_starts[1] - health_starts[0] >= 2
    assert observation.calls["count"]["attempted"] == 4
    assert observation.calls["count"]["failed"] == 1
    assert observation.calls["count"]["sql_seconds_samples"] == 3
    assert observation.samples[-1][2:] == [3, 3, 0]
    assert config["auto_cleanup"]["canonical_history_count"] == 3


@pytest.mark.parametrize("changes", [{"old_payloads": 0}, {"old_snapshots": 0}])
def test_auto_history_capture_rejects_incomplete_physical_presence(monkeypatch, changes):
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser.time, "monotonic", lambda: 0)
    monkeypatch.setattr(browser, "auto_history_facts", Mock(return_value={
        "ready": True, "old_payloads": 1, "old_snapshots": 1, **changes}))
    with pytest.raises(AssertionError):
        browser.capture_auto_history([], {}, Mock(), {"auto_cleanup": {"seed_windows": 520}}, 60, fixture_container=FIXTURE_CONTAINER)


def test_auto_history_seed_above_real_reserve_is_mandatory_and_capture_is_bounded(monkeypatch):
    with pytest.raises(RuntimeError, match="unchanged reserve"):
        browser.capture_auto_history([], {}, Mock(), {"auto_cleanup": {"seed_windows": 512}}, 60, fixture_container=FIXTURE_CONTAINER)
    monkeypatch.setattr(browser, "check_fixture", Mock())
    monkeypatch.setattr(browser, "auto_history_facts", Mock(return_value={"ready": False}))
    monkeypatch.setattr(browser.time, "monotonic", lambda: 60)
    with pytest.raises(RuntimeError, match="preparation/test budget"):
        browser.capture_auto_history([], {}, Mock(), {"auto_cleanup": {"seed_windows": 520}}, 60, fixture_container=FIXTURE_CONTAINER)


@pytest.mark.parametrize("production", [False, True])
@pytest.mark.parametrize("startup_elapsed", [2, 50])
def test_browser_preparation_and_dev_or_prod_use_one_overall_deadline(tmp_path, monkeypatch, production, startup_elapsed):
    import json
    import sys

    styles = tmp_path / "frontend/src/app/styles.css"
    styles.parent.mkdir(parents=True)
    styles.write_text("/* owned test stylesheet */", encoding="utf-8")
    now = [100.0]
    monkeypatch.setattr(browser, "ROOT", tmp_path)
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setitem(sys.modules, "aggregate_test_scenarios", SimpleNamespace(aggregate_runtime_smoke=Mock()))
    monkeypatch.setitem(sys.modules, "aggregate_performance", SimpleNamespace(write_clock=Mock()))
    initial = {"aggregate": "ordinary", "source": "ordinary-source", "anchor": "2026-11-16T00:00:00Z",
        "auto_cleanup": {"aggregate": "auto", "seed_windows": 520, "clock_at": "2026-11-16T00:00:00Z"}}
    def fixture_read(*_args, **kwargs):
        assert kwargs["timeout"] == 5
        now[0] += startup_elapsed
        return SimpleNamespace(returncode=0, stdout=json.dumps(initial))
    monkeypatch.setattr(browser.subprocess, "run", fixture_read)
    monkeypatch.setattr(browser, "check_fixture", Mock(return_value=True))
    monkeypatch.setattr(browser, "resolve_fixture_container", Mock(return_value=FIXTURE_CONTAINER))
    def command(args, **kwargs):
        assert 0 < kwargs["timeout"] <= 1200
        if "up" in args:
            assert now[0] == 103 + startup_elapsed
            assert kwargs["timeout"] == 300
        now[0] += 1
        return "{}"
    def capture(_compose, _env, _command, config, deadline, *, fixture_container, started, observation):
        assert fixture_container == FIXTURE_CONTAINER
        assert started == 103 + startup_elapsed and deadline == started + 300
        assert observation.started == started and observation.deadline == deadline
        assert browser.remaining_browser_budget(deadline) == 299
        config["auto_cleanup"].update(old_point={"id": "before"}, canonical_history_count=520)
        now[0] = 300
    monkeypatch.setattr(browser, "capture_auto_history", capture)
    readiness = []
    def ready(_env, _config, _check, _predicate, *, timeout, deadline):
        readiness.append((timeout, deadline))
        now[0] += 50
    monkeypatch.setattr(browser, "wait_current_history", ready)
    executions = []
    def execute(_args, **kwargs):
        if "up" in _args:
            assert now[0] == 103 + startup_elapsed
            assert kwargs["timeout"] == 300 and kwargs["deadline"] == now[0] + 300
            kwargs["check"]()
            now[0] += 1
            return
        assert _args[-2:] == ["--global-timeout", "950000"]
        executions.append((kwargs["timeout"], kwargs["deadline"]))
    def production_run(_compose, _env, _project, _run, _selected, _command, **kwargs):
        kwargs["ready"]()
        executions.append((browser.remaining_browser_budget(kwargs["deadline"]), kwargs["deadline"]))
    monkeypatch.setattr(browser, "monitored_command", execute)
    monkeypatch.setattr(browser, "production_browsers", production_run)
    monkeypatch.setattr(browser, "auto_history_facts", Mock(side_effect=lambda *_args, **kwargs:
        {"verified": True} if kwargs.get("verify") else {"ready": False, "canonical_history_count": 0}))
    monkeypatch.setattr(browser, "capture_worker_diagnostics", Mock())
    env = {"AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json"), "PUBLIC_ORIGIN": "http://example.test"}
    browser.run_browsers([], env, "owned", tmp_path, production, ["detectors.live.spec.ts"], command, Mock())
    assert readiness == [(60, 1300)]
    assert executions == [(950, 1300)]


@pytest.mark.parametrize("late_health", [False, True])
def test_config_or_late_health_cannot_move_capture_past_startup_sixty(tmp_path, monkeypatch, late_health):
    import json
    import sys

    now = [0]
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(browser.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setitem(sys.modules, "aggregate_test_scenarios", SimpleNamespace(aggregate_runtime_smoke=Mock()))
    def config_file(*_args, **_kwargs):
        now[0] += 5
        return SimpleNamespace(returncode=0, stdout=json.dumps({"aggregate": "owned"}))
    monkeypatch.setattr(browser.subprocess, "run", config_file)
    def status(*_args, **_kwargs):
        if late_health:
            now[0] = 60
            return True
        return False
    check = Mock(side_effect=status)
    monkeypatch.setattr(browser, "check_fixture", check)
    monkeypatch.setattr(browser, "resolve_fixture_container", Mock(return_value=FIXTURE_CONTAINER))
    canonical = Mock()
    monkeypatch.setattr(browser, "capture_auto_history", canonical)
    worker_up = Mock()
    monkeypatch.setattr(browser, "monitored_command", worker_up)
    with pytest.raises(RuntimeError, match="budget exhausted"):
        browser.run_browsers([], {}, "owned", tmp_path, False, ["detectors.live.spec.ts"], Mock(), Mock())
    assert check.called and now[0] <= 65
    canonical.assert_not_called()
    worker_up.assert_not_called()


@pytest.fixture
def auto_up_context(tmp_path, monkeypatch):
    import json
    import sys

    styles = tmp_path / "frontend/src/app/styles.css"
    styles.parent.mkdir(parents=True)
    styles.write_text("/* owned test stylesheet */", encoding="utf-8")
    state = {"stage": "before", "now": 0, "events": [], "reads": []}
    before = {"id": "before-point", "payload_state": "present", "features": [{"value": 3}]}
    config = {"aggregate": "ordinary", "source": "source", "anchor": "2026-11-16T00:00:00Z",
        "auto_cleanup": {"aggregate": "owned-auto", "seed_windows": 520, "clock_at": "2026-11-16T00:00:00Z"}}
    monkeypatch.setattr(browser, "ROOT", tmp_path)
    monkeypatch.setattr(browser.time, "monotonic", lambda: state["now"])
    sleep = Mock(side_effect=AssertionError("A one-shot up callback must not wait for a point"))
    monkeypatch.setattr(browser.time, "sleep", sleep)
    monkeypatch.setitem(sys.modules, "aggregate_test_scenarios", SimpleNamespace(aggregate_runtime_smoke=Mock()))
    monkeypatch.setitem(sys.modules, "aggregate_performance", SimpleNamespace(write_clock=Mock()))
    monkeypatch.setattr(browser.subprocess, "run", Mock(return_value=SimpleNamespace(
        returncode=0, stdout=json.dumps(config))))
    check = Mock(return_value=True)
    monkeypatch.setattr(browser, "check_fixture", check)
    monkeypatch.setattr(browser, "resolve_fixture_container", Mock(return_value=FIXTURE_CONTAINER))
    def facts(*_args, readiness_only=False, verify=False, **_kwargs):
        if verify:
            return {"verified": True}
        state["reads"].append(readiness_only)
        if readiness_only:
            assert state["stage"] == "cleared"
            return {"canonical_history_count": 520, "old_point": None, "old_payloads": 0, "old_snapshots": 0}
        if state["stage"] == "before":
            return {"ready": False, "canonical_history_count": 0}
        assert state["stage"] == "present", "A captured point must not be fetched again after GC"
        return {"ready": True, "canonical_history_count": 12, "old_point_id": before["id"],
            "old_payloads": 1, "old_snapshots": 1, "old_point": before, "payload_state": "present"}
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    monkeypatch.setattr(browser, "wait_current_history", Mock())
    diagnostics = Mock(side_effect=lambda *_args: state["events"].append("diagnostic"))
    monkeypatch.setattr(browser, "capture_worker_diagnostics", diagnostics)
    env = {"PUBLIC_ORIGIN": "http://example.test", "AGGREGATE_CLOCK_FILE": str(tmp_path / "clock.json")}
    return SimpleNamespace(state=state, before=before, check=check, diagnostics=diagnostics, sleep=sleep,
        run=lambda: browser.run_browsers([], env, "owned", tmp_path, False,
            ["detectors.live.spec.ts"], Mock(return_value="{}"), Mock()), path=tmp_path)


def test_present_proof_observed_inside_up_survives_gc_before_up_returns(auto_up_context, monkeypatch):
    import json

    context = auto_up_context
    def monitored(args, **kwargs):
        if "up" in args:
            try:
                kwargs["check"]()  # Before Popen: no point, exactly one RPC then return.
                assert context.state["reads"] == [False]
                context.state["stage"] = "present"
                context.state["now"] += 2
                kwargs["check"]()  # The worker commits old0 while up --wait is running.
                context.state["stage"] = "cleared"
                context.state["now"] += 2
                kwargs["check"]()  # Natural GC already removed old0; proof is not overwritten.
            finally:
                context.state["events"].append("up-finally")
        else:
            context.state["events"].append("browser")
    monkeypatch.setattr(browser, "monitored_command", monitored)
    context.run()
    result = json.loads((context.path / "detector-browser.json").read_text(encoding="utf-8"))["auto_cleanup"]
    assert context.state["reads"] == [False, False, True]
    assert result["old_point"] == context.before and result["old_point_id"] == context.before["id"]
    assert result["old_payloads"] == result["old_snapshots"] == 1
    assert result["canonical_history_count"] == 520
    assert context.state["events"] == ["up-finally", "browser", "diagnostic"]
    context.sleep.assert_not_called()


@pytest.mark.parametrize("failure", ["fixture_process_stopped", "fixture_progress_stale", "late_rpc", "already_cleared"])
def test_failed_early_up_callback_never_admits_history_or_browser(auto_up_context, monkeypatch, failure):
    context = auto_up_context
    original_facts = browser.auto_history_facts
    def facts(*args, **kwargs):
        if failure == "already_cleared":
            raise RuntimeError("Old auto point was not observed before deletion")
        if failure == "late_rpc":
            context.state["now"] = 300
            context.state["stage"] = "present"
        return original_facts(*args, **kwargs)
    monkeypatch.setattr(browser, "auto_history_facts", facts)
    if failure.startswith("fixture_"):
        context.check.side_effect = [True, RuntimeError(failure)]  # Startup ready, then failed up callback.
    def monitored(args, **kwargs):
        assert "up" in args, "A failed callback must not continue to browser assertions"
        try:
            kwargs["check"]()
        finally:
            context.state["events"].append("up-finally")
    monkeypatch.setattr(browser, "monitored_command", monitored)
    with pytest.raises(RuntimeError):
        context.run()
    assert context.state["events"] == ["up-finally", "diagnostic"]
    assert not (context.path / "detector-browser.json").exists()
    context.diagnostics.assert_called_once()
    context.sleep.assert_not_called()


def test_hmr_verification_preserves_unrelated_changes(tmp_path):
    css = tmp_path / "styles.css"
    original = b"--color-page: #eef5f2;\n/* user's existing style */\n"
    css.write_bytes(original)
    browser.verify_hmr_restore(css, original)
    css.write_bytes(original.replace(b"#eef5f2", b"#e4f7e4"))
    with pytest.raises(RuntimeError, match="HMR"):
        browser.verify_hmr_restore(css, original)
    assert css.read_bytes() == original
    foreign = original + b"/* concurrent user edit */\n"
    css.write_bytes(foreign)
    with pytest.raises(RuntimeError, match="preserved"):
        browser.verify_hmr_restore(css, original)
    assert css.read_bytes() == foreign


def test_missing_mandatory_spec_is_not_silently_omitted(tmp_path,monkeypatch):
    monkeypatch.setattr(browser,"ROOT",tmp_path)
    with pytest.raises(ValueError,match="Missing mandatory"):
        browser.browser_specs(True)


def test_production_browser_container_removed_after_failed_run(tmp_path):
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json","package-lock.json","playwright.config.ts","browser/detectors.live.spec.ts"):
        (source / name).write_text("{}",encoding="utf-8")
    fixture = tmp_path / "fixture.json"
    fixture.write_text("{}",encoding="utf-8")
    command = Mock(side_effect=["proxy-id", "browser-id", "", RuntimeError("browser failed")])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(browser,"ROOT",tmp_path)
        cleanup = Mock()
        patch.setattr(browser.subprocess,"run",cleanup)
        with pytest.raises(RuntimeError,match="browser failed"):
            browser.production_browsers([], {"DETECTOR_BROWSER_CONFIG":str(fixture),"KAFKA_CA_FILE":str(fixture),"TASK21_ADMIN_PASSWORD":"synthetic","HTTPS_BIND":"127.0.0.1:55443:443","PUBLIC_ORIGIN":"https://localhost:55443"}, "owned",tmp_path,["detectors.live.spec.ts"],command)
        assert cleanup.call_args.args[0] == ["docker","rm","-f","owned-detector-browser"]
        browser_command = command.call_args_list[1].args[0]
        assert "PLAYWRIGHT_BASE_URL=https://localhost:55443" in browser_command
        assert "NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem" in browser_command
        assert not any("NODE_TLS_REJECT_UNAUTHORIZED" in arg for arg in browser_command)
        prepare_script = command.call_args_list[2].args[0][-1]
        assert "Components: main" in prepare_script
        assert prepare_script.count("Dir::Etc::sourcelist=/tmp/browser-ubuntu.sources") == 2
        assert "libnss3-tools socat" in prepare_script
        assert "certutil" in prepare_script and "update-ca-certificates" in prepare_script
        test_script = command.call_args.args[0][-1]
        assert "TCP-LISTEN:55443" in test_script
        assert "TCP:127.0.0.1:443" in test_script
        assert "ignoreHTTPSErrors" not in test_script


def test_browser_modules_copy_local_dependency_tree_and_reject_escape(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "one.spec.ts").write_text('import { helper } from "./nested/helper";', encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested/helper.ts").write_text('export { value } from "../value";', encoding="utf-8")
    (source / "value.ts").write_text("export const value = 1;", encoding="utf-8")
    destination = tmp_path / "dest"
    browser.copy_browser_modules(source, destination, ["one.spec.ts"])
    assert (destination / "nested/helper.ts").read_bytes() == (source / "nested/helper.ts").read_bytes()
    assert (destination / "value.ts").is_file()
    (source / "bad.spec.ts").write_text('import "../outside";', encoding="utf-8")
    (tmp_path / "outside.ts").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unsafe"):
        browser.copy_browser_modules(source, destination, ["bad.spec.ts"])


@pytest.mark.parametrize("failed", [False, True])
def test_production_aggregate_config_paths_ca_export_and_cleanup(tmp_path, monkeypatch, failed):
    import json
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json", "package-lock.json", "playwright.config.ts", "browser/aggregates.live.spec.ts"):
        (source / name).write_text("{}", encoding="utf-8")
    detector = tmp_path / "detector.json"
    detector.write_text("{}", encoding="utf-8")
    aggregate = tmp_path / "aggregate.json"
    exported = tmp_path / "owned.json"
    aggregate.write_text(json.dumps({"username":"test", "password":"synthetic", "sources":["one","two"],
        "saved_window_sources":["saved-one", "saved-two"],
        "anchor":"2026-01-01T00:00:00Z", "result":str(exported)}), encoding="utf-8")
    calls = []
    def prepare_saved_window(_compose, _env, _command, path):
        assert calls[-1][:2] == ["docker", "exec"] and "npm ci" in calls[-1][-1]
        config = json.loads(path.read_text(encoding="utf-8"))
        config["saved_window"] = {"sources": config.pop("saved_window_sources"),
            "anchor": "2026-11-20T09:00:00Z"}
        path.write_text(json.dumps(config), encoding="utf-8")

    def command(args, **kwargs):
        calls.append(args)
        if args[:2] == ["docker", "exec"] and "npx playwright test" in args[-1]:
            (tmp_path / "browser-results/aggregate-browser-owned.json").write_text('["owned-id"]', encoding="utf-8")
            if failed:
                raise RuntimeError("browser failed")
        return "proxy-id" if "ps" in args else ""
    cleanup = Mock()
    monkeypatch.setattr(browser,"ROOT",tmp_path)
    monkeypatch.setattr(browser.subprocess,"run",cleanup)
    monkeypatch.setattr(browser, "prepare_saved_window_fixture", prepare_saved_window)
    env = {"DETECTOR_BROWSER_CONFIG":str(detector), "AGGREGATE_BROWSER_CONFIG":str(aggregate), "KAFKA_CA_FILE":str(detector), "TASK21_ADMIN_PASSWORD":"synthetic", "HTTPS_BIND":"127.0.0.1:55443:443", "PUBLIC_ORIGIN":"https://localhost:55443"}
    if failed:
        with pytest.raises(RuntimeError, match="browser failed"):
            browser.production_browsers([],env,"owned",tmp_path,["aggregates.live.spec.ts"],command)
    else:
        browser.production_browsers([],env,"owned",tmp_path,["aggregates.live.spec.ts"],command)
    config = json.loads((tmp_path / "detector-browser-context/aggregate-fixture.json").read_text(encoding="utf-8"))
    assert config["result"] == "/results/aggregate-browser-owned.json"
    assert config["saved_window"]["sources"] == ["saved-one", "saved-two"]
    run = next(args for args in calls if args[:2] == ["docker","run"])
    assert "AGGREGATE_BROWSER_CONFIG=/test/aggregate-fixture.json" in run
    assert "DETECTOR_BROWSER_CONFIG=/test/fixture.json" in run
    assert "NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem" in run
    assert exported.read_text(encoding="utf-8") == '["owned-id"]'
    assert cleanup.call_args.args[0] == ["docker","rm","-f","owned-detector-browser"]


@pytest.mark.parametrize("failed", [False, True])
def test_browser_working_gate_releases_its_transaction_on_every_exit(failed):
    from unittest.mock import MagicMock

    from scripts.browser_detector_fixture import working_closure_gate
    database = MagicMock()
    transaction = database.session_factory.begin.return_value
    if failed:
        with pytest.raises(RuntimeError, match="proof failed"), working_closure_gate(database):
            raise RuntimeError("proof failed")
        assert transaction.__exit__.call_args.args[0] is RuntimeError
    else:
        with working_closure_gate(database) as proof:
            assert proof is transaction.__enter__.return_value
        assert transaction.__exit__.call_args.args == (None, None, None)
    statements = [str(call.args[0]) for call in transaction.__enter__.return_value.execute.call_args_list]
    assert any("lock_timeout" in statement for statement in statements)
    assert any("statement_timeout" in statement for statement in statements)
    assert "LOCK TABLE logs.source_interruptions IN SHARE ROW EXCLUSIVE MODE" in statements


def test_browser_aggregate_diagnostics_keep_last10_target_steps_unknown_memory_and_no_vectors(tmp_path, monkeypatch, capsys):
    import json
    import sys

    metrics = {"peak_memory_bytes": None, "peak_rss_bytes": 123, "raw": "private-raw",
        "history_first": {"target": {"aggregate_id": "target", "origin": "initial_history",
            "timings": {"selection": .2, "raw": "private-history"}}},
        "closed": {str(index): {"aggregate_id": "target", "point_id": str(index), "origin": "online",
            "timings": {"selection": .1, "raw": "private-timing"}, "features": "private-vector"}
            for index in range(25)},
        "maintenance": {"spans": [{"aggregate_id": "target", "phase": "memberships", "rows_changed": 1,
            "checkpoint": "private-checkpoint"}] * 25}}
    heartbeat = {"admission": {"active": 1}, "steps": [{"aggregate_id": "target", "state": "complete",
        "origin": "initial_history", "timings": {"selection": .3, "raw": "private-step"}}] * 25}
    (tmp_path / "aggregate-performance.json").write_text(json.dumps(metrics), encoding="utf-8")
    (tmp_path / "aggregate-worker.json").write_text(json.dumps(heartbeat), encoding="utf-8")
    clock = tmp_path / "clock.json"
    clock.write_text(json.dumps({"aggregates": {"target": "2026-11-16T00:00:00Z"}}), encoding="utf-8")
    monkeypatch.setenv("AGGREGATE_TEST_CLOCK", str(clock))
    monkeypatch.setattr(sys, "argv", ["diagnostics", "aggregate", "target"])
    script = browser.WORKER_DIAGNOSTIC_SCRIPT.replace("/tmp/aggregate-performance.json",
        (tmp_path / "aggregate-performance.json").as_posix()).replace("/tmp/aggregate-worker.json",
        (tmp_path / "aggregate-worker.json").as_posix())
    script_path = tmp_path / "worker-diagnostics.py"
    script_path.write_text(script, encoding="utf-8")
    runpy.run_path(str(script_path))
    output = capsys.readouterr().out
    facts = json.loads(output)
    assert "private-" not in output
    assert [row["point_id"] for row in facts["online_last10"]] == [str(index) for index in range(15, 25)]
    assert len(facts["steps_last10"]) == len(facts["maintenance_last10"]) == 10
    assert facts["history_first"]["timings"] == {"selection": .2}
    assert facts["steps_last10"][-1]["origin"] == "initial_history"
    assert facts["steps_last10"][-1]["timings"] == {"selection": .3}
    assert facts["resources"]["peak_memory_bytes"] is None and facts["resources"]["peak_rss_bytes"] == 123
    assert facts["maintenance_budget_errors"] is None and facts["worker_budget_errors_dropped"] is None
    assert facts["clock"]["target_at"] == "2026-11-16T00:00:00Z" and len(facts["clock"]["stat_last10"]) == 10


def test_browser_budget_diagnostics_bound_target_frames_and_never_emit_exception_payloads(tmp_path, monkeypatch, capsys):
    import json
    import sys

    stack = [{"module": "app.aggregation.event_time", "function": "milliseconds", "line": index + 1,
        "filename": "private-path", "source": "private-source", "locals": "private-vector"} for index in range(12)]
    events = [{"aggregate_id": "target", "sequence": index + 1, "factor": "operation_seconds",
        "turn_total_seconds": .9, "stack": stack, "error": "private-checkpoint", "argv": "private-argv"}
        for index in range(25)]
    events.insert(0, {"aggregate_id": "private-other-aggregate", "factor": "private-other"})
    events[-1].update(factor="private / vector", stack=None, turn_total_seconds=float("inf"))
    metrics = {"maintenance": {"budget_errors": events, "budget_error_sequence": 26, "dropped_budget_errors": 16}}
    metrics_path, heartbeat_path = tmp_path / "performance.json", tmp_path / "worker.json"
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")
    heartbeat_path.write_text("{}", encoding="utf-8")
    monkeypatch.delenv("AGGREGATE_TEST_CLOCK", raising=False)
    monkeypatch.setattr(sys, "argv", ["diagnostics", "aggregate", "target"])
    script = browser.WORKER_DIAGNOSTIC_SCRIPT.replace("/tmp/aggregate-performance.json", metrics_path.as_posix()).replace(
        "/tmp/aggregate-worker.json", heartbeat_path.as_posix())
    script_path = tmp_path / "worker-diagnostics.py"
    script_path.write_text(script, encoding="utf-8")
    runpy.run_path(str(script_path))
    output = capsys.readouterr().out
    facts = json.loads(output)
    assert "private" not in output
    errors = facts["maintenance_budget_errors"]
    assert [row["sequence"] for row in errors] == list(range(16, 26))
    assert all(row["aggregate_id"] == "target" for row in errors)
    assert errors[0]["factor"] == "operation_seconds" and errors[0]["turn_total_seconds"] == .9
    assert len(errors[0]["stack"]) == 8 and errors[0]["stack"][0] == {
        "module": "app.aggregation.event_time", "function": "milliseconds", "line": 1}
    assert errors[-1]["factor"] is None and errors[-1]["stack"] == []
    assert errors[-1]["turn_total_seconds"] is None
    assert facts["worker_budget_error_sequence"] == 26 and facts["worker_budget_errors_dropped"] == 16


def test_browser_detector_diagnostics_filter_target_ids_and_keep_numeric_runtime_phases(tmp_path, monkeypatch, capsys):
    import json
    import sys

    rows = [{"detector_id": "owned", "execution_state": "running", "consumed_index": index,
        "transition": {"read_seconds": .1, "raw": "private-read", "runtime": {"checkpoint_seconds": .2,
            "checkpoint": "private-checkpoint"}}} for index in range(25)]
    rows.append({"detector_id": "other", "raw": "private-other"})
    path = tmp_path / "detector-worker.json"
    path.write_text(json.dumps({"phase": "evaluating", "steps": rows}), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["diagnostics", "detector", "target", json.dumps(["owned"])])
    script = browser.WORKER_DIAGNOSTIC_SCRIPT.replace("/tmp/detector-worker.json", path.as_posix())
    script_path = tmp_path / "worker-diagnostics.py"
    script_path.write_text(script, encoding="utf-8")
    runpy.run_path(str(script_path))
    output = capsys.readouterr().out
    facts = json.loads(output)
    assert "private-" not in output and len(facts["steps_last10"]) == 10
    assert facts["steps_last10"][-1]["consumed_index"] == 24
    assert facts["steps_last10"][-1]["transition"] == {"read_seconds": .1}
    assert facts["steps_last10"][-1]["runtime"] == {"checkpoint_seconds": .2}


@pytest.mark.parametrize("result", [RuntimeError("unavailable"), "[]", "not-json"])
def test_browser_worker_diagnostic_failure_is_bounded_and_cannot_mask_the_browser_result(result, capsys):
    command = Mock(side_effect=result if isinstance(result, Exception) else None,
        return_value=None if isinstance(result, Exception) else result)
    browser.capture_worker_diagnostics([], {}, command, {"auto_cleanup": {"aggregate": "owned"}})
    assert command.call_count == 3
    assert [call.args[0][2] for call in command.call_args_list] == ["aggregate-worker", "api", "detector-worker"]
    for call in command.call_args_list:
        args = call.args[0]
        assert args[:2] == ["exec", "-T"]
        assert args[3:6] == ["python", "-B", "-c"]
        assert call.kwargs["timeout"] in {5, 10}
        compile(args[6], "browser-diagnostics", "exec")
    assert capsys.readouterr().out.count("unavailable") == 3


def test_positions_readback_uses_the_exact_old_point_captured_before_collection(capsys):
    aggregate_id = "00000000-0000-4000-8000-000000000001"
    point_id = "00000000-0000-5000-8000-000000000002"
    command = Mock(side_effect=["{}", '{"detectors":[]}', "{}"])
    browser.capture_worker_diagnostics([], {}, command,
        {"auto_cleanup": {"aggregate": aggregate_id, "old_point_id": point_id}})
    positions = command.call_args_list[1]
    assert positions.args[0][2] == "api"
    assert positions.args[0][-2:] == [aggregate_id, point_id]
    assert positions.kwargs["timeout"] == 10
    assert "Browser aggregate positions:" in capsys.readouterr().out


def test_preparation_diagnostics_with_missing_auto_target_do_not_mask_its_failure(capsys):
    command = Mock()
    browser.capture_worker_diagnostics([], {}, command, {"auto_cleanup": {"seed_windows": 512}})
    command.assert_not_called()
    assert "auto_target_missing" in capsys.readouterr().out


_REAL_AUTO_WORKER_DIAGNOSTICS = browser.capture_worker_diagnostics
_AUTO_CAPTURE_TARGET = "00000000-0000-4000-8000-000000000001"
_AUTO_CAPTURE_OTHER = "00000000-0000-4000-8000-000000000002"
_AUTO_CAPTURE_POINT = "00000000-0000-5000-8000-000000000003"
_AUTO_CAPTURE_STAMPS = (
    "iteration_started_monotonic",
    "discovery_started_monotonic",
    "discovery_completed_monotonic",
    "append_before_monotonic",
    "append_after_monotonic",
    "acquire_before_monotonic",
    "acquire_after_monotonic",
    "execute_started_monotonic",
    "execute_completed_monotonic",
)


def _auto_capture_trace(sequence, kind="maintenance", target=_AUTO_CAPTURE_TARGET):
    return {
        "job_id": f"{'a' * 32}:{sequence}",
        "aggregate_id": target,
        "kind": kind,
        **{key: 100.0 + index / 10 for index, key in enumerate(_AUTO_CAPTURE_STAMPS)},
        "events": "private-events",
        "checkpoint": "private-checkpoint",
    }


def _auto_capture_inputs(count=3):
    traces = [_auto_capture_trace(index + 1) for index in range(count)]
    foreground = _auto_capture_trace(count + 1, "foreground")
    step = {
        "aggregate_id": _AUTO_CAPTURE_TARGET,
        "point_id": _AUTO_CAPTURE_POINT,
        "origin": "online",
        "state": "complete",
        "timings": {"selection": 0.1},
        "job_trace": foreground,
    }
    metrics = {
        "closed": {_AUTO_CAPTURE_POINT: step},
        "job_trace": {
            "live": 2,
            "max_live": 4,
            "issued": 1000,
            "dropped": 0,
            "error_count": 0,
            "errors": ["private-errors"],
        },
        "maintenance_jobs": {
            "record_limit": 256,
            "byte_limit": 192 * 1024,
            "observed": 999,
            "evicted": 999 - count,
            "invalid": 0,
            "retained": count,
            "retained_bytes": 1000,
            "records": traces,
        },
        "maintenance": {
            "spans": [
                {
                    "aggregate_id": _AUTO_CAPTURE_TARGET,
                    "closure_id": _AUTO_CAPTURE_POINT,
                    "sequence": 7,
                    "job_id": traces[-1]["job_id"],
                    "phase": "memberships",
                    "rows_changed": 1,
                    "events": "private-event",
                }
            ],
            "budget_errors": [
                {
                    "aggregate_id": _AUTO_CAPTURE_TARGET,
                    "sequence": 8,
                    "job_id": traces[-1]["job_id"],
                    "factor": "operation_seconds",
                    "raw": "private-error",
                }
            ],
        },
    }
    return metrics, {"admission": {"active": 1}, "steps": [step]}


def _auto_capture_projector():
    import json
    import math

    scope = {"json": json, "math": math}
    exec(browser.AUTO_WORKER_JOB_CAPTURE, scope)  # noqa: S102 - trusted repository script, isolated mocks
    return scope["auto_worker_job_capture"], scope


def _run_auto_diagnostic_script(
    monkeypatch, metrics, heartbeat, *, projector_fault=None, detector=False
):
    import datetime as datetime_module
    import io
    import json
    import socket
    import sys
    import threading
    from pathlib import Path

    inputs = {
        "/tmp/aggregate-performance.json": json.dumps(metrics).encode(),
        "/tmp/aggregate-worker.json": json.dumps(heartbeat).encode(),
        "/tmp/detector-worker.json": b'{"steps":[]}',
    }
    opens, output, datetime_calls = [], [], []

    class FixedDatetime(datetime_module.datetime):
        @classmethod
        def now(cls, tz=None):
            datetime_calls.append(tz)
            return cls(2026, 11, 17, 0, 0, 0, tzinfo=tz)

    def opened(path, mode):
        assert mode == "rb"
        name = path.as_posix()
        opens.append(name)
        assert name in inputs
        return io.BytesIO(inputs[name])

    script = browser.WORKER_DIAGNOSTIC_SCRIPT
    if projector_fault is not None:
        replacement = "def auto_worker_job_capture(*args):\n    raise _diagnostic_fault\n"
        assert script.startswith(browser.AUTO_WORKER_JOB_CAPTURE)
        script = replacement + script[len(browser.AUTO_WORKER_JOB_CAPTURE) :]
    namespace = {"print": lambda value: output.append(value), "_diagnostic_fault": projector_fault}
    with monkeypatch.context() as patch:
        patch.delenv("AGGREGATE_TEST_CLOCK", raising=False)
        arguments = ["diagnostic", "detector" if detector else "aggregate", _AUTO_CAPTURE_TARGET]
        patch.setattr(sys, "argv", [*arguments, "[]"] if detector else arguments)
        patch.setattr(Path, "open", opened)
        patch.setattr(datetime_module, "datetime", FixedDatetime)
        for name in ("monotonic", "perf_counter", "time", "sleep"):
            patch.setattr(
                browser.time, name, Mock(side_effect=AssertionError("new diagnostic clock/poll"))
            )
        patch.setattr(
            browser.subprocess, "run", Mock(side_effect=AssertionError("new diagnostic process"))
        )
        patch.setattr(
            browser.subprocess, "Popen", Mock(side_effect=AssertionError("new diagnostic process"))
        )
        patch.setattr(
            socket, "socket", Mock(side_effect=AssertionError("new diagnostic network/SQL"))
        )
        patch.setattr(
            threading, "Thread", Mock(side_effect=AssertionError("new diagnostic thread"))
        )
        exec(script, namespace)  # noqa: S102 - trusted repository diagnostic under strict IO mocks
    assert len(output) == 1
    assert datetime_calls == [
        datetime_module.UTC
    ]  # The single existing observation, not a new clock call.
    facts = json.loads(output[0])
    assert facts["observed_at"] == "2026-11-17T00:00:00+00:00"
    return facts, opens


def test_auto_job_capture_preserves_exact_links_and_nine_existing_measurements_without_raw_state():
    import copy
    import json

    metrics, heartbeat = _auto_capture_inputs()
    foreign = _auto_capture_trace(99, target=_AUTO_CAPTURE_OTHER)
    metrics["maintenance_jobs"]["records"].append(foreign)
    metrics["closed"]["foreign"] = {"aggregate_id": _AUTO_CAPTURE_OTHER, "job_trace": foreign}
    heartbeat["steps"].append({"aggregate_id": _AUTO_CAPTURE_OTHER, "job_trace": foreign})
    before = copy.deepcopy((metrics, heartbeat))
    projector, _scope = _auto_capture_projector()
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert len(captured["maintenance_jobs"]) == 3
    for projected, original in zip(
        captured["maintenance_jobs"], metrics["maintenance_jobs"]["records"], strict=False
    ):
        assert projected == {
            key: original[key] for key in ("job_id", "aggregate_id", "kind", *_AUTO_CAPTURE_STAMPS)
        }
    foreground = captured["online_jobs"][0]
    assert foreground["point_id"] == _AUTO_CAPTURE_POINT
    assert foreground["trace"] == captured["step_jobs"][0]["trace"]
    assert foreground["trace"]["kind"] == "foreground"
    assert captured["span_links"] == [
        {"sequence": 7, "job_id": "a" * 32 + ":3", "closure_id": _AUTO_CAPTURE_POINT}
    ]
    assert captured["budget_error_links"] == [{"sequence": 8, "job_id": "a" * 32 + ":3"}]
    assert captured["maintenance_source"]["observed"] == 999
    assert captured["maintenance_source"]["evicted"] == 996
    assert captured["trace_counters"]["issued"] == 1000
    assert (metrics, heartbeat) == before
    encoded = json.dumps(captured)
    assert "private-" not in encoded and _AUTO_CAPTURE_OTHER not in encoded
    assert set(captured["trace_counters"]) == {
        "live",
        "max_live",
        "issued",
        "dropped",
        "error_count",
    }


def test_auto_job_capture_keeps_missing_links_and_source_wide_evictions_unknown_without_fake_zero():
    metrics, heartbeat = _auto_capture_inputs()
    del metrics["closed"][_AUTO_CAPTURE_POINT]["job_trace"]
    del metrics["maintenance"]["spans"][0]["job_id"]
    metrics["maintenance_jobs"]["records"] = []
    metrics["maintenance_jobs"].update(evicted=777, retained=0)
    projector, _scope = _auto_capture_projector()
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert captured["online_jobs"][0]["trace"] is None
    assert captured["step_jobs"][0]["trace"] is None
    assert captured["span_links"][0]["job_id"] is None
    assert captured["maintenance_jobs"] == []
    assert captured["maintenance_source"]["evicted"] == 777
    unavailable = projector({}, {}, _AUTO_CAPTURE_TARGET)
    assert all(value is None for value in unavailable["maintenance_source"].values())
    assert all(value is None for value in unavailable["trace_counters"].values())
    assert projector(None, {}, _AUTO_CAPTURE_TARGET) is None
    assert projector({}, None, _AUTO_CAPTURE_TARGET) is None


@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), 10**100, "private-number"])
def test_auto_job_capture_invalid_numbers_are_unknown_and_never_escape_the_closed_schema(value):
    import json

    metrics, heartbeat = _auto_capture_inputs()
    for key in _AUTO_CAPTURE_STAMPS:
        metrics["maintenance_jobs"]["records"][0][key] = value
    metrics["maintenance_jobs"]["observed"] = value
    metrics["job_trace"]["issued"] = value
    projector, _scope = _auto_capture_projector()
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert all(captured["maintenance_jobs"][0][key] is None for key in _AUTO_CAPTURE_STAMPS)
    assert captured["maintenance_source"]["observed"] is None
    assert captured["trace_counters"]["issued"] is None
    assert "private-number" not in json.dumps(captured, allow_nan=False)


@pytest.mark.parametrize(
    "job",
    [
        None,
        "private-job",
        "a" * 32 + ":0",
        "a" * 32 + ":" + "1" * 21,
        "A" * 32 + ":1",
        "a" * 32 + ":1/private",
    ],
)
def test_auto_job_capture_invalid_controlled_job_ids_are_counted_without_payloads(job):
    metrics, heartbeat = _auto_capture_inputs()
    metrics["maintenance_jobs"]["records"][0]["job_id"] = job
    projector, _scope = _auto_capture_projector()
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert captured["invalid_records"] == 1 and len(captured["maintenance_jobs"]) == 2


def test_auto_job_capture_caps_all_record_tails_and_encoded_envelope_with_crlf():
    import copy
    import json

    metrics, heartbeat = _auto_capture_inputs(280)
    for row in metrics["maintenance_jobs"]["records"]:
        row.update({key: 5.555555555555555e-100 for key in _AUTO_CAPTURE_STAMPS})
    step = heartbeat["steps"][0]
    metrics["closed"] = {str(index): copy.deepcopy(step) for index in range(25)}
    heartbeat["steps"] = [copy.deepcopy(step) for _ in range(25)]
    metrics["maintenance"]["spans"] *= 25
    metrics["maintenance"]["budget_errors"] *= 25
    before = copy.deepcopy((metrics, heartbeat))
    projector, _scope = _auto_capture_projector()
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert captured["source_records_omitted"] == 24
    assert len(captured["maintenance_jobs"]) <= 256
    assert all(
        len(captured[key]) <= 10
        for key in ("online_jobs", "step_jobs", "span_links", "budget_error_links")
    )
    assert len(json.dumps(captured, allow_nan=False).encode("ascii")) + 32 <= 192 * 1024
    assert (metrics, heartbeat) == before


def test_auto_job_capture_overflow_discards_oldest_metadata_and_counts_it_without_changing_source():
    import copy
    import json

    metrics, heartbeat = _auto_capture_inputs(256)
    before = copy.deepcopy((metrics, heartbeat))
    projector, scope = _auto_capture_projector()

    # Valid JSON whitespace conservatively exercises the byte cap without new fields,
    # enlarging source rings, changing production constants, or constructing raw state.
    def encoder(value, **kwargs):
        return json.dumps(value, indent=16, **kwargs)

    scope["json"] = SimpleNamespace(dumps=encoder)
    captured = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert captured["byte_cap_omitted"] > 0
    assert len(encoder(captured, allow_nan=False).encode("ascii")) + 32 <= 192 * 1024
    assert captured["byte_cap_omitted"] == 256 - len(captured["maintenance_jobs"])
    assert (
        captured["maintenance_jobs"][0]["job_id"]
        == "a" * 32 + f":{captured['byte_cap_omitted'] + 1}"
    )
    assert captured["maintenance_jobs"][-1]["job_id"] == "a" * 32 + ":256"
    assert captured["maintenance_source"]["evicted"] == before[0]["maintenance_jobs"]["evicted"]
    assert (metrics, heartbeat) == before


def test_auto_job_capture_assembled_script_uses_only_existing_reads_and_preserves_legacy_output(
    monkeypatch,
):
    metrics, heartbeat = _auto_capture_inputs()
    facts, opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert opens == ["/tmp/aggregate-performance.json", "/tmp/aggregate-worker.json"]
    assert facts["admission"] == heartbeat["admission"]
    assert facts["online_last10"][0]["point_id"] == _AUTO_CAPTURE_POINT
    assert facts["steps_last10"][0]["timings"] == {"selection": 0.1}
    assert facts["job_capture"]["online_jobs"][0]["trace"]["job_id"] == "a" * 32 + ":4"
    detector, detector_opens = _run_auto_diagnostic_script(
        monkeypatch, metrics, heartbeat, detector=True
    )
    assert "job_capture" not in detector
    assert detector_opens == ["/tmp/detector-worker.json"]


@pytest.mark.parametrize(
    "failure", [RuntimeError("private-projection"), MemoryError("private-projection")]
)
def test_auto_job_capture_whole_projection_failure_is_optional_and_leaves_legacy_fields(
    monkeypatch, failure
):
    metrics, heartbeat = _auto_capture_inputs()
    facts, opens = _run_auto_diagnostic_script(
        monkeypatch, metrics, heartbeat, projector_fault=failure
    )
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]
    assert facts["online_last10"][0]["point_id"] == _AUTO_CAPTURE_POINT
    assert opens == ["/tmp/aggregate-performance.json", "/tmp/aggregate-worker.json"]


def test_auto_job_capture_size_serialization_failure_keeps_legacy_diagnostics(
    monkeypatch,
):
    import json

    metrics, heartbeat = _auto_capture_inputs()
    original = json.dumps

    def fail_new_object(value, **kwargs):
        if isinstance(value, dict) and "maintenance_jobs" in value and "byte_limit" in value:
            raise MemoryError("private-size-serialization")
        return original(value, **kwargs)

    monkeypatch.setattr(json, "dumps", fail_new_object)
    facts, opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]
    assert len(opens) == 2


def test_auto_job_capture_bad_uuid_is_nonfatal_for_assembled_script(monkeypatch):
    metrics, heartbeat = _auto_capture_inputs()
    metrics["closed"][_AUTO_CAPTURE_POINT]["point_id"] = "g" * 36
    facts, _opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]


def test_auto_job_capture_uuid_converter_failure_is_nonfatal_and_does_not_emit_private_error(
    monkeypatch,
):
    import json
    import uuid

    metrics, heartbeat = _auto_capture_inputs()
    monkeypatch.setattr(uuid, "UUID", Mock(side_effect=MemoryError("private-conversion")))
    facts, _opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]
    assert "private-conversion" not in json.dumps(facts)


def test_auto_job_capture_unexpected_counter_mapping_is_nonfatal_for_assembled_script(monkeypatch):
    metrics, heartbeat = _auto_capture_inputs()
    metrics["job_trace"] = "private-checkpoint"
    facts, _opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]


def test_auto_job_capture_size_encoding_failure_is_nonfatal_and_keeps_old_fields(monkeypatch):
    import json

    metrics, heartbeat = _auto_capture_inputs()
    original = json.dumps

    class BrokenEncoding(str):
        def encode(self, *_args, **_kwargs):
            raise MemoryError("private-encoding")

    def dumps(value, **kwargs):
        encoded = original(value, **kwargs)
        if isinstance(value, dict) and "maintenance_jobs" in value and "byte_limit" in value:
            return BrokenEncoding(encoded)
        return encoded

    monkeypatch.setattr(json, "dumps", dumps)
    facts, _opens = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    assert facts["job_capture"] is None
    assert facts["admission"] == heartbeat["admission"]


@pytest.mark.parametrize("browser_failed", [False, True])
@pytest.mark.parametrize(
    "diagnostic_fault", ["projection", "metadata", "uuid", "encode", "command", "serialize", "emit"]
)
def test_auto_job_diagnostics_preserve_actual_browser_success_or_same_original_exception(
    auto_up_context, monkeypatch, capsys, browser_failed, diagnostic_fault
):
    import json

    context = auto_up_context
    original = RuntimeError("original-browser-failure")
    commands, projected = [], []

    def diagnostic_command(args, **kwargs):
        commands.append(args[2])
        if diagnostic_fault == "command" and args[2] == "aggregate-worker":
            raise MemoryError("private-diagnostic-command")
        if args[2] == "aggregate-worker" and diagnostic_fault in {
            "projection",
            "metadata",
            "uuid",
            "encode",
        }:
            import uuid

            metrics, heartbeat = _auto_capture_inputs()
            original_dumps = json.dumps

            class BrokenEncoding(str):
                def encode(self, *_args, **_kwargs):
                    raise MemoryError("private-encoding")

            def dumps(value, **options):
                encoded = original_dumps(value, **options)
                if (
                    isinstance(value, dict)
                    and "maintenance_jobs" in value
                    and "byte_limit" in value
                ):
                    return BrokenEncoding(encoded)
                return encoded

            if diagnostic_fault == "metadata":
                metrics["job_trace"] = "private-metadata"
            with monkeypatch.context() as patch:
                if diagnostic_fault == "uuid":
                    patch.setattr(uuid, "UUID", Mock(side_effect=MemoryError("private-uuid")))
                if diagnostic_fault == "encode":
                    patch.setattr(json, "dumps", dumps)
                facts, _opens = _run_auto_diagnostic_script(
                    monkeypatch,
                    metrics,
                    heartbeat,
                    projector_fault=MemoryError("private-projection")
                    if diagnostic_fault == "projection"
                    else None,
                )
            projected.append(facts)
            return json.dumps(facts)
        return '{"job_capture":null}' if args[2] == "aggregate-worker" else '{"detectors":[]}'

    def diagnostics(_compose, _env, _command, _config):
        _REAL_AUTO_WORKER_DIAGNOSTICS(
            [],
            {},
            diagnostic_command,
            {
                "auto_cleanup": {
                    "aggregate": _AUTO_CAPTURE_TARGET,
                    "old_point_id": _AUTO_CAPTURE_POINT,
                }
            },
        )

    monkeypatch.setattr(browser, "capture_worker_diagnostics", diagnostics)
    if diagnostic_fault == "serialize":

        def dumps(value, **kwargs):
            if isinstance(value, dict) and "job_capture" in value:
                raise MemoryError("private-diagnostic-envelope")
            return json.dumps(value, **kwargs)

        monkeypatch.setattr(browser, "json", SimpleNamespace(loads=json.loads, dumps=dumps))
    if diagnostic_fault == "emit":
        import builtins

        def printing(value, **kwargs):
            if str(value).startswith("Browser aggregate worker diagnostics:"):
                raise OSError("private-diagnostic-output")
            return builtins.print(value, **kwargs)

        monkeypatch.setattr(browser, "print", printing, raising=False)

    def monitored(args, **kwargs):
        if "up" in args:
            context.state["stage"] = "present"
            kwargs["check"]()
            context.state["stage"] = "cleared"
            context.state["now"] += 1
        elif browser_failed:
            raise original

    monkeypatch.setattr(browser, "monitored_command", monitored)
    if browser_failed:
        with pytest.raises(RuntimeError) as caught:
            context.run()
        assert caught.value is original
    else:
        assert context.run() is None
    assert commands == ["aggregate-worker", "api", "detector-worker"]
    if diagnostic_fault in {"projection", "metadata", "uuid", "encode"}:
        assert len(projected) == 1 and projected[0]["job_capture"] is None
    output = capsys.readouterr().out
    assert (
        "Browser aggregate positions:" in output
        and "Browser detector worker diagnostics:" in output
    )
    assert "private-diagnostic" not in output


def test_auto_job_projection_does_not_swallow_base_exception(monkeypatch):
    class DiagnosticAbort(BaseException):
        pass

    metrics, heartbeat = _auto_capture_inputs()
    failure = DiagnosticAbort()
    with pytest.raises(DiagnosticAbort) as caught:
        _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat, projector_fault=failure)
    assert caught.value is failure


def _generation_capture_input():
    return {"scope": {"run_id": "owned-run", "aggregate_id": _AUTO_CAPTURE_TARGET,
            "nonce": _AUTO_CAPTURE_OTHER, "phase": "generation"},
        "state": "ended", "outcome": "complete", "complete": True, "begin_observed_monotonic": 100.,
        "end_observed_monotonic": 200., "first_event_monotonic": 100.1, "last_event_monotonic": 199.,
        "counters": {kind: {name: 520 for name in ("discovered", "offered", "admitted", "completed")}
            for kind in ("foreground", "maintenance")},
        "duration_sums_seconds": {kind: {name: 1. for name in ("discovery", "append", "queue", "execution", "commit_ack")}
            for kind in ("foreground", "maintenance")},
        "intervals_unavailable": {}, "acquire_none_observations": {},
        "observed_jobs": 1040, "pending_jobs": 0, "missing_links": 0, "capture_error_count": 0,
        "latest_jobs": [_auto_capture_trace(521, "foreground")], "slowest_jobs": [_auto_capture_trace(1)],
        "span_links": [{"job_id": "a" * 32 + ":1", "sequence": 1, "closure_id": _AUTO_CAPTURE_POINT,
            "phase": "memberships", "started_monotonic": 100., "sql_completed_monotonic": 100.2,
            "committed_monotonic": 100.3}],
        "budget_error_links": [], "raw": "private-phase", "checkpoint": "private-phase-checkpoint"}


def test_generation_projection_survives_empty_late_carriers_and_uses_only_existing_diagnostic_reads(monkeypatch):
    import json

    metrics, heartbeat = _auto_capture_inputs()
    metrics["generation_phase"] = _generation_capture_input()
    metrics["maintenance_jobs"]["records"] = []
    metrics["closed"] = {}
    heartbeat["steps"] = []
    facts, opened = _run_auto_diagnostic_script(monkeypatch, metrics, heartbeat)
    captured = facts["job_capture"]
    phase = captured["generation_phase"]
    assert phase["complete"] and phase["boundaries_complete"]
    assert phase["counters"]["foreground"]["completed"] == 520
    assert phase["latest_jobs"][0]["job_id"].endswith(":521")
    assert phase["span_links"][0]["committed_monotonic"] == 100.3
    assert captured["maintenance_jobs"] == captured["online_jobs"] == captured["step_jobs"] == []
    assert opened == ["/tmp/aggregate-performance.json", "/tmp/aggregate-worker.json"]
    assert "private-" not in json.dumps(phase)
    assert len(json.dumps(phase, allow_nan=False).encode("ascii")) + 32 <= 64 * 1024


@pytest.mark.parametrize("change", [{"state": "active", "end_observed_monotonic": None},
    {"begin_observed_monotonic": None}, {"end_observed_monotonic": 99.}, {"capture_error_count": 1},
    {"missing_links": 1}, {"outcome": "failed"}])
def test_generation_projection_never_turns_incomplete_or_failed_phase_into_complete(change):
    metrics, heartbeat = _auto_capture_inputs()
    metrics["generation_phase"] = {**_generation_capture_input(), **change}
    projector, _ = _auto_capture_projector()
    result = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET, "owned-run")
    assert result["generation_phase"]["complete"] is False
    assert len(result["maintenance_jobs"]) == 3


@pytest.mark.parametrize("change", [{"run_id": "foreign"}, {"aggregate_id": _AUTO_CAPTURE_OTHER},
    {"nonce": "invalid"}, {"phase": "resume"}])
def test_generation_projection_scope_failure_does_not_drop_legacy_diagnostics(change):
    metrics, heartbeat = _auto_capture_inputs()
    phase = metrics["generation_phase"] = _generation_capture_input()
    phase["scope"].update(change)
    projector, _ = _auto_capture_projector()
    result = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET, "owned-run")
    assert result["generation_phase"]["capture_error"] == "invalid_scope"
    assert result["generation_phase"]["complete"] is False and len(result["maintenance_jobs"]) == 3


def test_generation_projection_is_closed_bounded_and_nonfatal_on_malformed_nested_metadata():
    import json

    metrics, heartbeat = _auto_capture_inputs(256)
    phase = metrics["generation_phase"] = _generation_capture_input()
    phase["latest_jobs"] = [_auto_capture_trace(index + 1) for index in range(1000)]
    phase["slowest_jobs"] = list(phase["latest_jobs"])
    phase["capture_error_types"] = ["private-error", "ValueError", "x" * 65]
    projector, _ = _auto_capture_projector()
    result = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET, "owned-run")
    captured = result["generation_phase"]
    assert len(captured["latest_jobs"]) == len(captured["slowest_jobs"]) == 10
    assert captured["projection_dropped"] == 1980 and captured["capture_error_types"] == ["ValueError"]
    assert len(json.dumps(captured, allow_nan=False).encode("ascii")) + 32 <= 64 * 1024
    assert len(json.dumps(result, allow_nan=False).encode("ascii")) + 32 <= 192 * 1024
    assert "private-" not in json.dumps(captured)
    phase["counters"]["foreground"] = ["private-malformed"]
    result = projector(metrics, heartbeat, _AUTO_CAPTURE_TARGET)
    assert result["generation_phase"]["capture_error"] == "projection_invalid"
    assert len(result["maintenance_jobs"]) == 256


def test_generation_spec_end_marker_is_in_finally_before_protection_or_resume_without_another_clock_jump():
    import re

    source = (browser.ROOT / "frontend/browser/detectors.live.spec.ts").read_text(encoding="utf-8")
    source = re.sub(r"\s+", " ", source)
    protected = re.search(r"await withPhaseEnd\(\s*\(\) =>\s*generation\.run\(", source).start()
    begins = list(re.finditer(r'advanceClock\(generationClock,\s*\{\s*\.\.\.phaseScope,\s*state: "begin",?\s*\}\s*\)', source))
    ends = list(re.finditer(r'writeClock\(generationClock,\s*\{\s*\.\.\.phaseScope,\s*state: "end",\s*outcome,?\s*\}\s*\)', source))
    assert len(begins) == len(ends) == 1
    begin, end = begins[0].start(), ends[0].start()
    assert protected < begin < end < source.index("const beforeProtectionCheck", end)
    assert end < source.index("const resume = autoBudget.child(300_000)")
    assert "generationJumpCommitted" in source[begin:end]
