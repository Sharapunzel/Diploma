"""Bounded supervision of the existing smoke profile's detector fixture."""
import json
import os
import signal
import subprocess
import time
from datetime import UTC, datetime


def check_fixture(compose, env, command, *, allow_starting=False, allow_capture_starting=False):
    facts = json.loads(command([*compose, "exec", "-T", "detector-worker", "python",
        "/tmp/browser_detector_fixture.py", "--status"], env=env, capture=True, timeout=5))
    if facts.get("ready") is not True:
        if allow_starting and facts.get("reason") == "fixture_status_unavailable":
            return
        if allow_capture_starting and facts.get("reason") == "fixture_capture_starting":
            return
        # The fixture's status CLI emits only lifecycle/capture codes, no raw log.
        safe = {key: facts.get(key) for key in ("reason", "phase", "sequence", "error")}
        raise RuntimeError("Detector browser fixture/capture stopped: " + json.dumps(safe))


def monitored_command(args, *, env, cwd, timeout, check):
    """Stop this owned command tree promptly if fixture/capture stops."""
    check()
    options = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt"
               else {"start_new_session": True})
    process = subprocess.Popen(args, env=env, cwd=cwd, **options)
    deadline, next_check = time.monotonic() + timeout, 0.0
    try:
        while process.poll() is None:
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError("Browser command exceeded its profile budget")
            if now >= next_check:
                check()
                next_check = now + 2
            time.sleep(.1)
        check()  # A zero exit code cannot hide a fixture that just died.
        if process.returncode:
            raise RuntimeError(f"Browser command failed ({process.returncode})")
    finally:
        if process.poll() is None:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               check=False, capture_output=True)
            else:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name != "nt":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=5)


def wait_current_history(env, config, check, ready_history, *, timeout=60):
    """Require a new proven working window after browser preparation finishes."""
    import httpx

    not_before = datetime.now(UTC)
    deadline = time.monotonic() + timeout
    with httpx.Client(base_url=env["PUBLIC_ORIGIN"],
                      verify=env["KAFKA_CA_FILE"] if env["PUBLIC_ORIGIN"].startswith("https:") else True,
                      timeout=5) as client:
        check()
        response = client.post("/api/v1/auth/local/login", json={
            "username": config["username"], "password": config["password"]})
        response.raise_for_status()
        while True:
            check()
            response = client.get(f"/api/v1/aggregates/{config['aggregate']}/points?limit=50&offset=0")
            response.raise_for_status()
            if ready_history(response.json()["items"], not_before=not_before):
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("Recent proven history after browser preparation did not become ready")
            time.sleep(.25)
