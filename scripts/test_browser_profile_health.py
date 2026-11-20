"""Permanent regressions for live fixture loss and post-preparation readiness."""
import json
import math
import os
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from scripts import browser_detector_fixture as fixture
from scripts import browser_profile_health as health
from scripts import browser_test_scenarios as browser

FIXTURE_CONTAINER = "a" * 64


def test_health_uses_the_resolved_project_container_without_restarting_compose():
    compose = ["docker", "compose", "-p", "owned"]
    env = {}
    command = Mock(side_effect=[f"{FIXTURE_CONTAINER}\n", '{"ready":true}', '{"ready":true}'])
    container = health.resolve_fixture_container(compose, env, command)
    command.assert_called_once_with([*compose, "ps", "-q", "detector-worker"],
        env=env, capture=True, timeout=5)
    for _ in range(2):
        assert health.check_fixture(container, env, command)
        command.assert_called_with(["docker", "exec", FIXTURE_CONTAINER, "python",
            "/tmp/browser_detector_fixture.py", "--status"], env=env, capture=True, timeout=5)


@pytest.mark.parametrize("raw", ["", "abc", f"{FIXTURE_CONTAINER}\n{'b' * 64}"])
def test_health_rejects_a_missing_or_ambiguous_container(raw):
    with pytest.raises(RuntimeError, match="missing or ambiguous"):
        health.resolve_fixture_container(["docker", "compose"], {}, Mock(return_value=raw))


def test_health_timeout_is_propagated_even_during_startup():
    failure = RuntimeError("Command timed out after 5s")
    command = Mock(side_effect=failure)
    with pytest.raises(RuntimeError) as caught:
        health.check_fixture(FIXTURE_CONTAINER, {}, command,
            allow_starting=True, allow_capture_starting=True)
    assert caught.value is failure
    assert command.call_args.kwargs["timeout"] == 5


@pytest.mark.parametrize("failure,reason", [("dead", "fixture_process_stopped"),
    ("stale", "fixture_progress_stale"), ("failed", "fixture_failed")])
def test_stopped_fixture_after_initial_readiness_is_not_healthy(tmp_path, monkeypatch, failure, reason):
    path = tmp_path / "status.json"
    status = {"pid": 123, "state": "running", "phase": "interval_committed",
              "sequence": 80, "updated": 100, "capture_at": 100, "error": None}
    monkeypatch.setattr(fixture.time, "monotonic", lambda: 101)
    monkeypatch.setattr(fixture.os, "kill", Mock())
    path.write_text(json.dumps(status), encoding="utf-8")
    assert fixture.read_status(path)["ready"]
    if failure == "dead":
        monkeypatch.setattr(fixture.os, "kill", Mock(side_effect=ProcessLookupError))
    elif failure == "stale":
        status["updated"] = 0
    else:
        status.update(state="failed", phase="stopped", error="RuntimeError")
    path.write_text(json.dumps(status), encoding="utf-8")
    assert fixture.read_status(path)["reason"] == reason
    assert fixture.read_status(path)["ready"] is False


def test_capture_loss_fails_safely_and_startup_grace_cannot_hide_it():
    command = Mock(return_value=json.dumps({"ready": False, "reason": "decoder_unavailable",
        "phase": "materializing_barriers", "sequence": 50, "raw": "secret-raw-payload"}))
    with pytest.raises(RuntimeError, match="decoder_unavailable") as caught:
        health.check_fixture(FIXTURE_CONTAINER, {}, command, allow_starting=True)
    assert "secret-raw-payload" not in str(caught.value)
    command.return_value = json.dumps({"ready": False, "reason": "fixture_status_unavailable"})
    health.check_fixture(FIXTURE_CONTAINER, {}, command, allow_starting=True)
    with pytest.raises(RuntimeError, match="fixture_status_unavailable"):
        health.check_fixture(FIXTURE_CONTAINER, {}, command)


def test_starting_capture_is_observed_but_only_materialized_ready_can_admit_startup():
    command = Mock(return_value=json.dumps({"ready": False, "reason": "fixture_capture_starting"}))
    assert health.check_fixture(FIXTURE_CONTAINER, {}, command, allow_capture_starting=True) is False
    command.return_value = json.dumps({"ready": True})
    assert health.check_fixture(FIXTURE_CONTAINER, {}, command, allow_capture_starting=True) is True


def test_failure_status_exposes_only_bounded_database_code(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    monkeypatch.setattr(fixture, "STATUS", path)
    fixture.publish_status("failed", "gating_snapshot", 9,
                           error="OperationalError", sqlstate="55P03")
    facts = fixture.read_status(path)
    assert facts["phase"] == "gating_snapshot" and facts["sqlstate"] == "55P03"
    fixture.publish_status("failed", "gating_snapshot", error="OperationalError",
                           sqlstate="driver SQL with private raw")
    assert fixture.read_status(path)["sqlstate"] is None


def test_status_cli_does_not_need_any_application_or_site_packages(tmp_path):
    # -S excludes SQLAlchemy/FastAPI/site packages. This probe must stay cheap
    # even during npm preparation in the resource-limited browser profile.
    result = subprocess.run([sys.executable, "-S", "-B", fixture.__file__, "--status"],
        env={**os.environ, "BROWSER_FIXTURE_STATUS": str(tmp_path / "absent.json")},
        text=True, capture_output=True, check=True, timeout=5)
    assert json.loads(result.stdout) == {"ready": False, "reason": "fixture_status_unavailable"}


def test_completed_startup_steps_show_progress_but_stalled_start_is_still_rejected(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    now = [100.0]
    monkeypatch.setattr(fixture, "STATUS", path)
    monkeypatch.setattr(fixture, "capture_at", None)
    monkeypatch.setattr(fixture.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(fixture.os, "kill", Mock())
    fixture.publish_status("running", "starting")
    for phase in ("sqlalchemy_imported", "source_committed", "catalog_loaded"):
        now[0] += 7
        fixture.publish_status("running", phase)
        facts = fixture.read_status(path)
        assert facts == {"ready": False, "reason": "fixture_capture_starting",
                         "phase": phase, "sequence": 0}
        health.check_fixture(FIXTURE_CONTAINER, {}, Mock(return_value=json.dumps(facts)), allow_capture_starting=True)
    # Actual completed steps can take more than 10s in total without pretending
    # that capture is ready. A single stalled step still has the original cap.
    now[0] += 11
    facts = fixture.read_status(path)
    assert facts["reason"] == "fixture_progress_stale" and facts["phase"] == "catalog_loaded"
    with pytest.raises(RuntimeError, match="fixture_progress_stale"):
        health.check_fixture(FIXTURE_CONTAINER, {}, Mock(return_value=json.dumps(facts)),
                             allow_starting=True, allow_capture_starting=True)


def test_fixture_database_timeouts_preserve_existing_connection_options():
    from sqlalchemy.engine import make_url

    raw = make_url("postgresql+psycopg://synthetic:synthetic@postgres/diploma_test").update_query_dict(
        {"application_name": "browser-fixture", "options": "-c timezone=UTC"})
    bounded = make_url(fixture.bounded_database_url(raw))
    assert bounded.database == "diploma_test" and bounded.password == raw.password
    assert bounded.query["connect_timeout"] == "5"
    assert bounded.query["application_name"] == "browser-fixture"
    assert bounded.query["options"] == "-c timezone=UTC -c lock_timeout=1000 -c statement_timeout=3000"


def test_fresh_process_heartbeat_does_not_replace_materialization_progress(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    path.write_text(json.dumps({"pid": 123, "state": "running", "phase": "materializing_markers",
        "sequence": 80, "updated": 100, "capture_at": 80, "error": None}), encoding="utf-8")
    monkeypatch.setattr(fixture.time, "monotonic", lambda: 101)
    monkeypatch.setattr(fixture.os, "kill", Mock())
    assert fixture.read_status(path)["reason"] == "fixture_capture_stale"


@pytest.mark.parametrize("finished", [False, True])
def test_fixture_loss_aborts_owned_command_and_cannot_hide_behind_zero_exit(monkeypatch, tmp_path, finished):
    process = Mock(pid=123456, returncode=0 if finished else None)
    process.poll.return_value = 0 if finished else None
    process.wait.return_value = 0
    spawn = Mock(return_value=process)
    monkeypatch.setattr(health.subprocess, "Popen", spawn)
    kill = Mock()
    monkeypatch.setattr(health.subprocess, "run", kill)
    monkeypatch.setattr(health.os, "killpg", kill, raising=False)
    check = Mock(side_effect=[None, RuntimeError("fixture_process_stopped")])
    with pytest.raises(RuntimeError, match="fixture_process_stopped"):
        health.monitored_command(["owned-browser"], env={}, cwd=tmp_path, timeout=10, check=check)
    assert check.call_count == 2
    assert kill.called is (not finished)
    if not finished:
        process.wait.assert_called_once_with(timeout=5)


def test_history_from_before_browser_install_is_not_enough(monkeypatch):
    import httpx

    now = datetime.now(UTC)
    old = [{"kind": "working", "window_index":16-i, "origin":"online", "time_basis":"event_time",
            "payload_state":"present", "completeness": "complete", "end_utc":
            (now - timedelta(seconds=i + 20)).isoformat()} for i in range(16)]
    current = [{"kind": "working", "window_index":32-i, "origin":"online", "time_basis":"event_time",
                "payload_state":"present", "completeness": "complete", "end_utc":
                (now + timedelta(seconds=2 - i)).isoformat()} for i in range(16)]
    assert browser.ready_history(old)
    assert not browser.ready_history(old, not_before=now)
    assert not browser.ready_history([{**p, "origin":"initial_history"} for p in current], not_before=now)
    client = MagicMock()
    client.__enter__.return_value = client
    client.get.side_effect = [Mock(json=lambda: {"items": old}), Mock(json=lambda: {"items": current})]
    monkeypatch.setattr(httpx, "Client", Mock(return_value=client))
    monkeypatch.setattr(health.time, "sleep", Mock())
    check = Mock()
    health.wait_current_history({"PUBLIC_ORIGIN": "https://example.test", "KAFKA_CA_FILE": "owned-ca"},
        {"username": "synthetic", "password": "synthetic", "aggregate": "owned"}, check, browser.ready_history)
    assert client.get.call_count == 2 and check.call_count == 3


def test_command_initial_health_check_cannot_restart_the_shared_deadline(monkeypatch, tmp_path):
    now = [100]
    monkeypatch.setattr(health.time, "monotonic", lambda: now[0])
    spawn = Mock()
    monkeypatch.setattr(health.subprocess, "Popen", spawn)
    def check():
        now[0] = 103
    with pytest.raises(RuntimeError, match="profile budget"):
        health.monitored_command(["never-start"], env={}, cwd=tmp_path,
            timeout=1200, deadline=102, check=check)
    spawn.assert_not_called()


@pytest.mark.parametrize("elapsed", [0, 3])
def test_fresh_history_uses_remaining_global_time_and_rejects_late_ready(monkeypatch, elapsed):
    import httpx

    now = [100]
    monkeypatch.setattr(health.time, "monotonic", lambda: now[0])
    client = MagicMock()
    client.__enter__.return_value = client
    def read(*_args, **kwargs):
        assert kwargs["timeout"] == 2
        now[0] += elapsed
        return Mock(json=lambda: {"items": ["ready"]})
    client.get.side_effect = read
    monkeypatch.setattr(httpx, "Client", Mock(return_value=client))
    arguments = ({"PUBLIC_ORIGIN": "http://example.test", "KAFKA_CA_FILE": "owned-ca"},
        {"username": "synthetic", "password": "synthetic", "aggregate": "owned"}, Mock(), Mock(return_value=True))
    if elapsed:
        with pytest.raises(RuntimeError, match="did not become ready"):
            health.wait_current_history(*arguments, timeout=60, deadline=102)
    else:
        health.wait_current_history(*arguments, timeout=60, deadline=102)
    assert client.post.call_args.kwargs["timeout"] == 2


@pytest.mark.parametrize("exhausted", [False, True])
def test_production_install_readiness_and_tests_do_not_reset_the_callers_deadline(tmp_path, monkeypatch, exhausted):
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json", "package-lock.json", "playwright.config.ts", "browser/detectors.live.spec.ts"):
        (source / name).write_text("{}", encoding="utf-8")
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text("{}", encoding="utf-8")
    now, phases = [100], []
    monkeypatch.setattr(browser, "ROOT", tmp_path)
    monkeypatch.setattr(browser.time, "monotonic", lambda: now[0])
    cleanup = Mock()
    monkeypatch.setattr(browser.subprocess, "run", cleanup)
    def command(args, **kwargs):
        if args[0] != "docker":
            return "proxy-id"
        if args[1] == "run":
            assert kwargs["timeout"] == 30
        elif "npm ci" in args[-1]:
            phases.append(("install", kwargs["timeout"]))
            now[0] = 198
        elif "playwright test" in args[-1]:
            assert args[-1].endswith(" --global-timeout 1000")
            phases.append(("test", kwargs["timeout"]))
        return ""
    def ready(*, timeout):
        phases.append(("readiness", timeout))
        now[0] += 2 if exhausted else 1
    env = {"DETECTOR_BROWSER_CONFIG": str(fixture_path), "KAFKA_CA_FILE": str(fixture_path),
        "TASK21_ADMIN_PASSWORD": "synthetic", "HTTPS_BIND": "127.0.0.1:55443:443",
        "PUBLIC_ORIGIN": "https://localhost:55443"}
    if exhausted:
        with pytest.raises(RuntimeError, match="budget exhausted"):
            browser.production_browsers(["compose"], env, "owned", tmp_path,
                ["detectors.live.spec.ts"], command, ready=ready, deadline=200)
    else:
        browser.production_browsers(["compose"], env, "owned", tmp_path,
            ["detectors.live.spec.ts"], command, ready=ready, deadline=200)
    assert phases == [("install", 100), ("readiness", 2), *([] if exhausted else [("test", 1)])]
    assert cleanup.call_args.args[0] == ["docker", "rm", "-f", "owned-detector-browser"]


def test_runner_timeline_reuses_supplied_budget_without_io_or_another_clock(monkeypatch, capsys):
    config = {"runner_timeline": {"run_id": "owned-run", "boundaries": []}}
    monkeypatch.setattr(health.time, "monotonic", Mock(side_effect=AssertionError("new clock")))
    for index in range(20):
        health.browser_timeline_boundary(config, "Playwright_launch", 950.25 - index, elapsed=249.75 + index)
    assert len(config["runner_timeline"]["boundaries"]) == 16
    first = config["runner_timeline"]["boundaries"][0]
    assert first == {"boundary": "Playwright_launch", "remaining_seconds": 950.25,
        "elapsed_seconds": 249.75, "budget_scope": "global"}
    assert '"clock_domain": "browser_runner_monotonic"' in capsys.readouterr().out


def test_runner_timeline_failure_cannot_replace_browser_result(monkeypatch):
    failure = Mock(side_effect=ValueError("private diagnostic error"))
    monkeypatch.setattr(health.json, "dumps", failure)
    assert health.browser_timeline_boundary({"runner_timeline": {"run_id": "owned", "boundaries": []}},
        "Playwright_launch", 950.) is None


def test_production_rechecks_history_after_install_before_any_live_test(tmp_path, monkeypatch):
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json", "package-lock.json", "playwright.config.ts", "browser/detectors.live.spec.ts"):
        (source / name).write_text("{}", encoding="utf-8")
    owned_fixture = tmp_path / "fixture.json"
    owned_fixture.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(browser, "ROOT", tmp_path)
    cleanup = Mock()
    monkeypatch.setattr(browser.subprocess, "run", cleanup)
    events = []
    def command(args, **kwargs):
        if args[0] != "docker":
            return "proxy-id"
        if args[1] == "run":
            events.append("container")
        elif "npm ci" in args[-1]:
            events.append("install")
        elif "playwright test" in args[-1]:
            events.append("test")
        return ""
    def ready(**_kwargs):
        events.append("readiness")
        raise RuntimeError("fixture_process_stopped_after_install")
    with pytest.raises(RuntimeError, match="after_install"):
        browser.production_browsers(["compose"], {"DETECTOR_BROWSER_CONFIG": str(owned_fixture),
            "KAFKA_CA_FILE": str(owned_fixture), "TASK21_ADMIN_PASSWORD": "synthetic",
            "HTTPS_BIND": "127.0.0.1:55443:443", "PUBLIC_ORIGIN": "https://localhost:55443"},
            "owned", tmp_path, ["detectors.live.spec.ts"], command, ready=ready)
    assert events == ["container", "install", "readiness"]
    assert cleanup.call_args.args[0] == ["docker", "rm", "-f", "owned-detector-browser"]


_CAPTURE_TARGET = "00000000-0000-0000-0000-000000000001"
_CAPTURE_NONCE = "00000000-0000-0000-0000-000000000002"


def _phase_input(name):
    return {"scope": {"run_id": "owned-run", "aggregate_id": _CAPTURE_TARGET,
        "nonce": _CAPTURE_NONCE, "phase": name}, "state": "ended", "outcome": "complete",
        "begin_observed_monotonic": 1., "end_observed_monotonic": 2., "complete": True,
        "missing_links": 0, "capture_error_count": 0}


def _worker_projection(json_module=json):
    scope = {"json": json_module, "math": math}
    exec(health.AUTO_WORKER_JOB_CAPTURE, scope)  # noqa: S102 - trusted repository script
    return scope["auto_worker_job_capture"]


def test_pinned_canonical_summary_keeps_numeric_operations_and_resource_boundaries_with_generation():
    canonical = {**_phase_input("canonical"), "resource_boundaries": {
        "begin": {"process_cpu_seconds": 0.25, "cgroup_cpu_seconds": 3., "cgroup_version": 2,
            "cgroup_memory_bytes": 20_000, "cgroup_memory_peak_bytes": 30_000,
            "cgroup_memory_limit_bytes": 768 * 1024**2, "credentials": "private-secret"},
        "end": {"process_cpu_seconds": 1.25, "cgroup_cpu_seconds": 4.75, "cgroup_version": 2,
            "cgroup_memory_bytes": 25_000, "cgroup_memory_peak_bytes": 35_000,
            "cgroup_memory_limit_bytes": "private-invalid-number"}},
        "work_by_operation": {"history": {"completed": 9,
            "duration_sums_seconds": {"discovery": 0.01, "queue": 0.15, "execution": 1.8,
                "payload": "private-raw"}, "intervals_unavailable": {"append": 2}},
            "scan": {"completed": 3}, "maintenance": {"completed": 1},
            "private-foreign-operation": {"completed": 91}},
        "checkpoint": "private-checkpoint"}
    metrics = {"canonical_phase": canonical, "generation_phase": _phase_input("generation")}
    original = deepcopy(metrics)
    captured = _worker_projection()(metrics, None, _CAPTURE_TARGET, "owned-run")
    phase = captured["canonical_phase"]
    assert phase["boundaries_complete"] and phase["complete"] and captured["generation_phase"]["complete"]
    assert phase["resource_boundaries"]["begin"] == {
        key: value for key, value in canonical["resource_boundaries"]["begin"].items() if key != "credentials"}
    assert phase["resource_boundaries"]["end"]["cgroup_memory_limit_bytes"] is None
    assert phase["work_by_operation"]["history"] == {"completed": 9,
        "duration_sums_seconds": {"discovery": 0.01, "append": None, "queue": 0.15, "execution": 1.8},
        "intervals_unavailable": {"discovery": None, "append": 2, "queue": None, "execution": None}}
    assert phase["work_by_operation"]["scan"]["completed"] == 3
    assert phase["work_by_operation"]["maintenance"]["completed"] == 1
    assert phase["work_by_operation"]["online"] is None
    assert captured["step_jobs"] == []
    assert "private-" not in json.dumps(captured)
    assert metrics == original


@pytest.mark.parametrize("problem", ["invalid_scope", "projection_invalid", "open_boundary"])
def test_canonical_projection_reports_scope_errors_or_partial_capture_without_losing_generation(problem):
    canonical = _phase_input("canonical")
    if problem == "invalid_scope":
        canonical["scope"]["run_id"] = "foreign-run"
    elif problem == "projection_invalid":
        canonical["counters"] = {"foreground": ["private-malformed"]}
    else:
        canonical.update(state="active", end_observed_monotonic=None,
            resource_boundaries={"begin": {"cgroup_cpu_seconds": 1.}})
    captured = _worker_projection()({"canonical_phase": canonical,
        "generation_phase": _phase_input("generation")}, {}, _CAPTURE_TARGET, "owned-run")
    phase = captured["canonical_phase"]
    assert not phase["complete"] and not phase["boundaries_complete"]
    if problem == "open_boundary":
        assert phase["state"] == "active" and phase["end_observed_monotonic"] is None
        assert phase["resource_boundaries"]["end"] is None
    else:
        assert phase["capture_error"] == problem
    assert captured["generation_phase"]["complete"]
    assert "private-" not in json.dumps(captured)


def test_worker_envelope_preserves_both_phase_summaries_when_trimming_late_records():
    # Valid JSON whitespace stresses the byte ceilings with the existing record limits.
    encoder = SimpleNamespace(dumps=lambda value, **kwargs: json.dumps(value, indent=16, **kwargs))
    def maintenance_job(index):
        return {"job_id": f"{'a' * 32}:{index}", "aggregate_id": _CAPTURE_TARGET, "kind": "maintenance"}
    jobs = [maintenance_job(index) for index in range(1, 257)]
    phases = {name + "_phase": {**_phase_input(name), "latest_jobs": jobs[-10:], "slowest_jobs": jobs[:10],
        "counters": {"maintenance": {"completed": 256}}} for name in ("canonical", "generation")}
    phases["generation_phase"]["online_results"] = {"computed": 1, "uncomputed": 0, "unknown": 0}
    metrics = {**phases, "maintenance_jobs": {"records": jobs}}
    captured = _worker_projection(encoder)(metrics, {}, _CAPTURE_TARGET, "owned-run")
    assert captured["byte_cap_omitted"] > 0
    assert len(encoder.dumps(captured, allow_nan=False).encode("ascii")) + 32 <= 192 * 1024
    for key in phases:
        phase = captured[key]
        assert phase["complete"] and phase["counters"]["maintenance"]["completed"] == 256
        assert len(encoder.dumps(phase, allow_nan=False).encode("ascii")) + 32 <= 64 * 1024
    assert captured["generation_phase"]["online_results"] == {"computed": 1, "uncomputed": 0, "unknown": 0}
    assert "online_results" not in captured["canonical_phase"]


def test_generation_projection_preserves_explicit_online_results_and_timing_sample_coverage():
    generation = {**_phase_input("generation"),
        "online_results": {"computed": 5, "uncomputed": 2, "unknown": 1, "raw": "private-point"},
        "other_foreground_results": {"scan": 3, "history": 4, "waiting": 2, "unknown": 1},
        "online_timings": {"snapshot": {"samples": 6, "seconds": 0.35, "raw": "private-timing"},
            "save": {"samples": 0, "seconds": 0.0}, "commit": {"samples": 1, "seconds": 0.0},
            "credentials": "private-secret"}}
    metrics = {"generation_phase": generation}
    original = deepcopy(metrics)
    captured = _worker_projection()(metrics, {}, _CAPTURE_TARGET, "owned-run")["generation_phase"]
    assert captured["online_results"] == {"computed": 5, "uncomputed": 2, "unknown": 1}
    assert captured["other_foreground_results"] == {"scan": 3, "history": 4, "waiting": 2, "unknown": 1}
    assert captured["online_timings_scope"] == "completed_target_online_results"
    assert captured["online_timings"] == {"snapshot": {"samples": 6, "seconds": 0.35},
        "save": {"samples": 0, "seconds": None}, "commit": {"samples": 1, "seconds": 0.0}}
    assert "private-" not in json.dumps(captured)
    assert metrics == original


def test_generation_projection_retains_phase_data_when_online_summary_is_absent_or_malformed():
    generation = _phase_input("generation")
    project = _worker_projection()
    result = project({"generation_phase": generation}, {}, _CAPTURE_TARGET)["generation_phase"]
    assert result["online_results"] is result["online_timings"] is result["online_timings_scope"] is None
    generation.update(online_results={"computed": "unknown", "uncomputed": False, "unknown": -1},
        other_foreground_results=[], online_timings={"snapshot": {"samples": 1, "seconds": math.inf},
            "save": {"samples": [], "seconds": 1.0}, "commit": "invalid"})
    result = project({"generation_phase": generation}, {}, _CAPTURE_TARGET)["generation_phase"]
    assert result["complete"] and result["boundaries_complete"]
    assert result["online_results"] == {"computed": None, "uncomputed": None, "unknown": None}
    assert result["other_foreground_results"] is None
    assert result["online_timings"] == {"snapshot": {"samples": 1, "seconds": None},
        "save": {"samples": None, "seconds": None}, "commit": None}
