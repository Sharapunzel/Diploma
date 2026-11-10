"""Permanent regressions for live fixture loss and post-preparation readiness."""
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, Mock

import pytest

from scripts import browser_detector_fixture as fixture
from scripts import browser_profile_health as health
from scripts import browser_test_scenarios as browser


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
        health.check_fixture([], {}, command, allow_starting=True)
    assert "secret-raw-payload" not in str(caught.value)
    command.return_value = json.dumps({"ready": False, "reason": "fixture_status_unavailable"})
    health.check_fixture([], {}, command, allow_starting=True)
    with pytest.raises(RuntimeError, match="fixture_status_unavailable"):
        health.check_fixture([], {}, command)


def test_status_cli_does_not_need_any_application_or_site_packages(tmp_path):
    # -S excludes SQLAlchemy/FastAPI/site packages. This probe must stay cheap
    # even during npm preparation in the resource-limited browser profile.
    result = subprocess.run([sys.executable, "-S", "-B", fixture.__file__, "--status"],
        env={**os.environ, "BROWSER_FIXTURE_STATUS": str(tmp_path / "absent.json")},
        text=True, capture_output=True, check=True, timeout=5)
    assert json.loads(result.stdout) == {"ready": False, "reason": "fixture_status_unavailable"}


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
    old = [{"kind": "working", "completeness": "complete", "end_utc":
            (now - timedelta(seconds=i + 20)).isoformat()} for i in range(16)]
    current = [{"kind": "working", "completeness": "complete", "end_utc":
                (now + timedelta(seconds=2 - i)).isoformat()} for i in range(16)]
    assert browser.ready_history(old)
    assert not browser.ready_history(old, not_before=now)
    client = MagicMock()
    client.__enter__.return_value = client
    client.get.side_effect = [Mock(json=lambda: {"items": old}), Mock(json=lambda: {"items": current})]
    monkeypatch.setattr(httpx, "Client", Mock(return_value=client))
    monkeypatch.setattr(health.time, "sleep", Mock())
    check = Mock()
    health.wait_current_history({"PUBLIC_ORIGIN": "https://example.test", "KAFKA_CA_FILE": "owned-ca"},
        {"username": "synthetic", "password": "synthetic", "aggregate": "owned"}, check, browser.ready_history)
    assert client.get.call_count == 2 and check.call_count == 3


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
