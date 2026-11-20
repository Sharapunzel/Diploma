"""Bounded supervision of the existing smoke profile's detector fixture."""
import json
import os
import re
import signal
import subprocess
import time
from datetime import UTC, datetime


def browser_timeline_boundary(config, boundary, remaining, *, elapsed=None, budget_scope="global"):
    """Use an already measured runner budget value; never read another clock."""
    try:
        timeline = config["runner_timeline"]
        item = {"boundary": boundary, "remaining_seconds": remaining,
            "elapsed_seconds": elapsed, "budget_scope": budget_scope}
        if len(timeline["boundaries"]) < 16:
            timeline["boundaries"].append(item)
        print("Browser runner timeline: " + json.dumps({"run_id": timeline["run_id"],
            "clock_domain": "browser_runner_monotonic", **item}), flush=True)
    except Exception:  # noqa: BLE001 - optional metadata cannot alter a test result
        return


def resolve_fixture_container(compose, env, command):
    identity = command([*compose, "ps", "-q", "detector-worker"], env=env,
        capture=True, timeout=5).strip()
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise RuntimeError("Detector browser fixture container is missing or ambiguous")
    return identity


def check_fixture(container, env, command, *, allow_starting=False, allow_capture_starting=False):
    # The fixture belongs to one Compose project and one container lifetime.
    # Bypass repeated Compose startup without following a replacement writer.
    facts = json.loads(command(["docker", "exec", container, "python",
        "/tmp/browser_detector_fixture.py", "--status"], env=env, capture=True, timeout=5))
    if facts.get("ready") is not True:
        if allow_starting and facts.get("reason") == "fixture_status_unavailable":
            return False
        if allow_capture_starting and facts.get("reason") == "fixture_capture_starting":
            return False
        # The fixture's status CLI emits only lifecycle/capture codes, no raw log.
        safe = {key: facts.get(key) for key in ("reason", "phase", "sequence", "error", "sqlstate")}
        raise RuntimeError("Detector browser fixture/capture stopped: " + json.dumps(safe))
    return True


def monitored_command(args, *, env, cwd, timeout, check, deadline=None):
    """Stop this owned command tree promptly if fixture/capture stops."""
    deadline = min(deadline, time.monotonic() + timeout) if deadline is not None else time.monotonic() + timeout
    check()
    if time.monotonic() >= deadline:
        raise RuntimeError("Browser command exceeded its profile budget")
    options = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt"
               else {"start_new_session": True})
    process = subprocess.Popen(args, env=env, cwd=cwd, **options)
    next_check = 0.0
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
        if time.monotonic() >= deadline:
            raise RuntimeError("Browser command exceeded its profile budget")
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


def wait_current_history(env, config, check, ready_history, *, timeout=60, deadline=None):
    """Require a new proven working window after browser preparation finishes."""
    import httpx

    not_before = datetime.now(UTC)
    deadline = min(deadline, time.monotonic() + timeout) if deadline is not None else time.monotonic() + timeout
    def remaining():
        available = deadline - time.monotonic()
        if available <= 0:
            raise RuntimeError("Recent proven history after browser preparation did not become ready")
        return min(5, available)
    with httpx.Client(base_url=env["PUBLIC_ORIGIN"],
                      verify=env["KAFKA_CA_FILE"] if env["PUBLIC_ORIGIN"].startswith("https:") else True,
                      timeout=5) as client:
        check()
        response = client.post("/api/v1/auth/local/login", json={
            "username": config["username"], "password": config["password"]}, timeout=remaining())
        response.raise_for_status()
        while True:
            check()
            response = client.get(f"/api/v1/aggregates/{config['aggregate']}/points?limit=50&offset=0",
                timeout=remaining())
            response.raise_for_status()
            remaining()
            if ready_history(response.json()["items"], not_before=not_before):
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("Recent proven history after browser preparation did not become ready")
            time.sleep(.25)


# Appended to the existing container diagnostic script; consumes loaded dicts only.
AUTO_WORKER_JOB_CAPTURE = r'''
def auto_worker_job_capture(metrics, heartbeat, target, expected_run=None):
    from uuid import UUID

    maximum = 192 * 1024
    stamps = (
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

    def integer(value):
        return value if type(value) is int and 0 <= value <= 2**63 - 1 else None

    def identity(value):
        return value if type(value) is str and len(value) == 36 and str(UUID(value)) == value else None

    def job_id(value):
        if type(value) is not str or len(value) > 64 or not value.isascii():
            return None
        prefix, separator, number = value.partition(":")
        return (
            value
            if (
                separator == ":"
                and len(prefix) == 32
                and all(char in "0123456789abcdef" for char in prefix)
                and number.isdecimal()
                and len(number) <= 20
                and int(number) > 0
            )
            else None
        )

    def trace(row):
        if (
            not isinstance(row, dict)
            or row.get("aggregate_id") != target
            or row.get("kind") not in ("foreground", "maintenance")
            or job_id(row.get("job_id")) is None
        ):
            return None
        result = {"job_id": job_id(row["job_id"]), "aggregate_id": target, "kind": row["kind"]}
        for key in stamps:
            value = row.get(key)
            result[key] = (
                value
                if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1e15
                else None
            )
        return result

    def number(value):
        return value if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1e15 else None

    def phase_capture(source, phase_name):
        if not isinstance(source, dict):
            return None
        scope = source.get("scope")
        scope = scope if isinstance(scope, dict) else {}
        run_id = scope.get("run_id")
        if (scope.get("aggregate_id") != target or scope.get("phase") != phase_name
                or identity(scope.get("nonce")) is None or type(run_id) is not str or not 0 < len(run_id) <= 64
                or not run_id.isascii() or not all(char.isalnum() or char in "_-" for char in run_id)
                or expected_run is not None and run_id != expected_run):
            return {"state": "unavailable", "complete": False, "boundaries_complete": False,
                "capture_error": "invalid_scope"}
        state = source.get("state")
        state = state if state in {"active", "ended", "unavailable"} else "unavailable"
        begin, end = (number(source.get(key)) for key in ("begin_observed_monotonic", "end_observed_monotonic"))
        outcome = source.get("outcome") if source.get("outcome") in {"complete", "failed"} else None
        complete_boundaries = state == "ended" and begin is not None and end is not None and end >= begin
        result = {"byte_limit": 64 * 1024, "record_limit": 10,
            "scope": {"run_id": run_id, "aggregate_id": target, "nonce": scope["nonce"], "phase": phase_name},
            "state": state, "outcome": outcome, "clock_domain": "aggregate_worker_monotonic",
            "begin_observed_monotonic": begin, "end_observed_monotonic": end,
            "first_event_monotonic": number(source.get("first_event_monotonic")),
            "last_event_monotonic": number(source.get("last_event_monotonic")),
            "boundaries_complete": complete_boundaries, "complete": complete_boundaries and outcome == "complete"
                and source.get("complete") is True and integer(source.get("missing_links")) == 0
                and integer(source.get("capture_error_count")) == 0,
            "observation_lag_seconds": None, "sums_are_overlapping_not_wall_time": True,
            "job_intervals_may_cross_boundaries": True,
            "counters_are_observations_not_unique_discoveries": True, "projection_dropped": 0}
        for key in ("preexisting_jobs", "observed_jobs", "pending_jobs", "pending_ids_omitted", "missing_links",
                "invalid_records", "capture_error_count", "span_observed", "span_dropped", "budget_error_observed",
                "budget_error_dropped", "byte_cap_dropped", "retained_job_ids", "records_omitted"):
            result[key] = integer(source.get(key))
        for key, names, project in (
                ("counters", ("discovered", "offered", "admitted", "completed"), integer),
                ("duration_sums_seconds", ("discovery", "append", "queue", "execution", "commit_ack"), number),
                ("intervals_unavailable", ("discovery", "append", "queue", "execution", "commit_ack"), integer)):
            groups = source.get(key)
            result[key] = {kind: {name: project((groups.get(kind) or {}).get(name)) for name in names}
                for kind in ("foreground", "maintenance")} if isinstance(groups, dict) else None
        if phase_name == "generation":
            for key, labels in (("online_results", ("computed", "uncomputed", "unknown")),
                    ("other_foreground_results", ("scan", "history", "waiting", "unknown"))):
                counts = source.get(key)
                result[key] = {label: integer(counts.get(label)) for label in labels} if isinstance(counts, dict) else None
            timings = source.get("online_timings")
            result["online_timings"] = None
            result["online_timings_scope"] = None
            if isinstance(timings, dict):
                result["online_timings_scope"] = "completed_target_online_results"
                result["online_timings"] = {}
                for name in ("snapshot", "save", "commit"):
                    component = timings.get(name)
                    if not isinstance(component, dict):
                        result["online_timings"][name] = None
                        continue
                    samples = integer(component.get("samples"))
                    result["online_timings"][name] = {"samples": samples,
                        "seconds": number(component.get("seconds")) if samples else None}
        if phase_name == "canonical":
            def fields(row, names, project):
                return {key: project(row.get(key)) for key in names} if isinstance(row, dict) else None

            operations = source.get("work_by_operation")
            result["work_by_operation"] = None
            if isinstance(operations, dict):
                intervals = ("discovery", "append", "queue", "execution")
                result["work_by_operation"] = {}
                for operation in ("history", "scan", "online", "other_foreground", "maintenance"):
                    row = operations.get(operation)
                    result["work_by_operation"][operation] = {
                        "completed": integer(row.get("completed")),
                        "duration_sums_seconds": fields(row.get("duration_sums_seconds"), intervals, number),
                        "intervals_unavailable": fields(row.get("intervals_unavailable"), intervals, integer),
                    } if isinstance(row, dict) else None
            boundaries = source.get("resource_boundaries")
            result["resource_boundaries"] = None
            if isinstance(boundaries, dict):
                result["resource_boundaries"] = {}
                for boundary in ("begin", "end"):
                    row = boundaries.get(boundary)
                    counters = fields(row, ("cgroup_memory_bytes", "cgroup_memory_peak_bytes",
                        "cgroup_memory_limit_bytes", "cgroup_version"), integer)
                    if counters is not None:
                        counters.update(fields(row, ("process_cpu_seconds", "cgroup_cpu_seconds"), number))
                    result["resource_boundaries"][boundary] = counters
        waits = source.get("acquire_none_observations")
        result["acquire_none_observations"] = {name: integer(waits.get(name))
            for name in ("target_active", "capacity_full", "maintenance_active", "not_queued", "unknown")
        } if isinstance(waits, dict) else None
        errors = source.get("capture_error_types")
        result["capture_error_types"] = [value for value in errors[-8:] if type(value) is str
            and value.isascii() and value.isidentifier() and len(value) <= 64] if isinstance(errors, list) else []
        pending = source.get("pending_ids")
        result["pending_ids"] = [job_id(value) for value in pending[-10:] if job_id(value) is not None
        ] if isinstance(pending, list) else []
        for key in ("latest_jobs", "slowest_jobs"):
            result[key] = []
            rows = source.get(key)
            for row in rows[-10:] if isinstance(rows, list) else []:
                projected = trace(row)
                if projected is None:
                    result["projection_dropped"] += 1
                    continue
                projected.update(point_id=identity(row.get("point_id")),
                    origin=row.get("origin") if row.get("origin") in {"online", "initial_history"} else None)
                for name in ("rank_seconds", "point_commit_start_monotonic", "point_commit_ack_monotonic"):
                    projected[name] = number(row.get(name))
                result[key].append(projected)
            result["projection_dropped"] += max(0, len(rows) - 10) if isinstance(rows, list) else 0
        for key in ("span_links", "budget_error_links"):
            result[key] = []
            rows = source.get(key)
            for row in rows[-10:] if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    result["projection_dropped"] += 1
                    continue
                link = {"sequence": integer(row.get("sequence")), "job_id": job_id(row.get("job_id"))}
                if key == "span_links":
                    link.update(closure_id=identity(row.get("closure_id")),
                        phase=row.get("phase") if row.get("phase") in {"memberships", "outcomes"} else None)
                    link.update({name: number(row.get(name)) for name in
                        ("started_monotonic", "sql_completed_monotonic", "committed_monotonic")})
                else:
                    factor = row.get("factor")
                    link["factor"] = factor if type(factor) is str and factor.isascii() and factor.isidentifier() and len(factor) <= 64 else None
                result[key].append(link)
            result["projection_dropped"] += max(0, len(rows) - 10) if isinstance(rows, list) else 0
        while len(json.dumps(result, allow_nan=False).encode("ascii")) + 32 > 64 * 1024:
            for key in ("latest_jobs", "slowest_jobs", "span_links", "budget_error_links", "pending_ids"):
                if result[key]:
                    result[key].pop(0)
                    result["projection_dropped"] += 1
                    break
            else:
                return {"state": "unavailable", "complete": False, "boundaries_complete": False,
                    "capture_error": "projection_overflow"}
        return result

    if identity(target) is None or not isinstance(metrics, dict):
        return None
    if not isinstance(heartbeat, dict):
        if not any(isinstance(metrics.get(key), dict) for key in ("canonical_phase", "generation_phase")):
            return None
        heartbeat = {}
    carrier = metrics.get("maintenance_jobs")
    carrier = carrier if isinstance(carrier, dict) else {}
    source = carrier.get("records")
    source = source if isinstance(source, list) else []
    def project_phase(phase_name):
        try:
            return phase_capture(metrics.get(phase_name + "_phase"), phase_name)
        except Exception:
            return {"state": "unavailable", "complete": False, "boundaries_complete": False,
                "capture_error": "projection_invalid"}

    result = {
        "byte_limit": maximum,
        "maintenance_record_limit": 256,
        "online_record_limit": 10,
        "step_record_limit": 10,
        "span_link_limit": 10,
        "error_link_limit": 10,
        "trace_counters": {
            key: integer((metrics.get("job_trace") or {}).get(key))
            for key in ("live", "max_live", "issued", "dropped", "error_count")
        },
        "maintenance_source": {
            key: integer(carrier.get(key))
            for key in (
                "record_limit",
                "byte_limit",
                "observed",
                "evicted",
                "invalid",
                "retained",
                "retained_bytes",
            )
        },
        "source_records_omitted": max(0, len(source) - 256),
        "invalid_records": 0,
        "byte_cap_omitted": 0,
        "maintenance_jobs": [],
        "online_jobs": [],
        "step_jobs": [],
        "span_links": [],
        "budget_error_links": [],
        "canonical_phase": project_phase("canonical"),
        "generation_phase": project_phase("generation"),
    }
    for row in source[-256:]:
        if not isinstance(row, dict) or row.get("aggregate_id") != target:
            continue
        projected = trace(row)
        if projected is None or projected["kind"] != "maintenance":
            result["invalid_records"] += 1
        else:
            result["maintenance_jobs"].append(projected)
    closed = metrics.get("closed")
    closed = closed if isinstance(closed, dict) else {}
    for row in [
        item
        for item in closed.values()
        if isinstance(item, dict) and item.get("aggregate_id") == target
    ][-10:]:
        result["online_jobs"].append(
            {"point_id": identity(row.get("point_id")), "trace": trace(row.get("job_trace"))}
        )
    steps = heartbeat.get("steps")
    steps = steps if isinstance(steps, list) else []
    for row in [
        item for item in steps if isinstance(item, dict) and item.get("aggregate_id") == target
    ][-10:]:
        result["step_jobs"].append(
            {"point_id": identity(row.get("point_id")), "trace": trace(row.get("job_trace"))}
        )
    progress = metrics.get("maintenance")
    progress = progress if isinstance(progress, dict) else {}
    for source_key, destination in (
        ("spans", "span_links"),
        ("budget_errors", "budget_error_links"),
    ):
        rows = progress.get(source_key)
        rows = rows if isinstance(rows, list) else []
        for row in [
            item for item in rows if isinstance(item, dict) and item.get("aggregate_id") == target
        ][-10:]:
            link = {"sequence": integer(row.get("sequence")), "job_id": job_id(row.get("job_id"))}
            if source_key == "spans":
                link["closure_id"] = identity(row.get("closure_id"))
            result[destination].append(link)
    # Includes object envelope plus field-key/separator/CRLF with 32 bytes reserved.
    while len(json.dumps(result, allow_nan=False).encode("ascii")) + 32 > maximum:
        for key in (
            "maintenance_jobs",
            "online_jobs",
            "step_jobs",
            "span_links",
            "budget_error_links",
        ):
            if result[key]:
                result[key].pop(0)
                result["byte_cap_omitted"] += 1
                break
        else:
            return None
    return result
'''
