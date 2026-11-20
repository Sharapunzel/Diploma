"""Controlled aggregate load on the existing isolated Compose profile."""

import json
import math
import secrets
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from subprocess import SubprocessError
from uuid import UUID, uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]


def _capture_safely(operation):
    """Best-effort metadata only; never wrap actual validation, clocks or arm."""
    try:
        return operation()
    except Exception:  # noqa: BLE001 - diagnostic failures must preserve the real outcome
        return None


class PerformanceBudget:
    """One real deadline for useful work, including blocking calls and waits."""

    def __init__(self, deadline, *, started=None):
        self.deadline = deadline
        self.started = started
        self.limits = []
        _capture_safely(lambda: setattr(self, "checked_monotonic", None))
        _capture_safely(lambda: setattr(self, "last_call_checks", (None, None)))

    def observation(self, *, started=None, deadline=None):
        now = time.monotonic()
        result = {"global_remaining_seconds": max(0, self.deadline - now)}
        if self.started is not None:
            result["global_elapsed_seconds"] = now - self.started
        if deadline is not None:
            result.update({"local_elapsed_seconds": now - started,
                "local_remaining_seconds": max(0, deadline - now),
                "limiting_scope": "global" if self.deadline <= deadline else "local"})
        return result

    def remaining(self, cap=None):
        now = time.monotonic()
        _capture_safely(lambda: setattr(self, "checked_monotonic", now))
        remaining = self.deadline - now
        if remaining <= 0:
            raise RuntimeError("Controlled aggregate load exhausted its bounded profile budget")
        for deadline, message in self.limits:
            assert now < deadline, message
            remaining = min(remaining, deadline - now)
        return remaining if cap is None else min(cap, remaining)

    def call(self, operation, *args, cap=None, **kwargs):
        timeout = self.remaining(cap)
        started_check = _capture_safely(lambda: self.checked_monotonic)
        _capture_safely(lambda: setattr(self, "last_call_checks", (started_check, None)))
        result = operation(*args, timeout=timeout, **kwargs)
        self.remaining()
        _capture_safely(
            lambda: setattr(self, "last_call_checks", (started_check, self.checked_monotonic))
        )
        return result

    def sleep(self, seconds):
        time.sleep(self.remaining(seconds))
        self.remaining()

    @contextmanager
    def limit(self, seconds, message):
        started = time.monotonic()
        deadline = started + seconds
        with self.until(deadline, message, started=started):
            yield

    @contextmanager
    def until(self, deadline, message, *, started):
        """An existing phase origin, including work before its polling loop."""
        self.limits.append((deadline, message))
        try:
            self.remaining()
            yield
            self.remaining()
        except Exception as error:
            print("Controlled aggregate bounded wait failure: " + json.dumps({"condition": message,
                "error_type": type(error).__name__,
                **self.observation(started=started, deadline=deadline)}), flush=True)
            raise
        finally:
            self.limits.pop()

    def wait(self, predicate, seconds, message, *, on_ready=None):
        with self.limit(seconds, message):
            while True:
                self.remaining()
                value = predicate()
                self.remaining()
                if value:
                    if on_ready is not None:
                        _capture_safely(lambda: on_ready(self.checked_monotonic))
                    return value
                self.sleep(.05)


def validate_performance_arguments(args):
    group = getattr(args, "aggregate_performance_group", None)
    if group is not None and not args.aggregate_performance:
        raise ValueError("--aggregate-performance-group requires --aggregate-performance")
    if not args.aggregate_performance:
        return
    if args.profile not in {"smoke-development", "smoke-production"}:
        raise ValueError("--aggregate-performance requires an existing smoke profile")
    if (args.aggregate_only or args.detector_only or args.browser_only or args.rcf or args.wal
            or args.pytest is not None or args.browser_spec is not None):
        raise ValueError("--aggregate-performance cannot combine with other test selections")
    selected_load_cases(group)


def controlled_clock_overlay(run):
    """Only owned test files are mounted; production commands remain intact."""
    import yaml

    directory = run / "aggregate-clock"
    directory.mkdir()
    clock = directory / "clock.json"
    clock.write_text('{"aggregates":{}}', encoding="utf-8")
    entrypoint = directory / "worker.py"
    entrypoint.write_bytes((ROOT / "backend/tests/aggregate_clock_worker.py").read_bytes())
    overlay = run / "aggregate-clock.yml"
    overlay.write_text(yaml.safe_dump({"services": {
        name: {"command": ["python", "/tmp/aggregate-clock-worker.py", kind],
            "environment": {"AGGREGATE_TEST_CLOCK": "/tmp/aggregate-clock/clock.json",
                "AGGREGATE_TEST_RUN_ID": run.name},
            "volumes": [
                {"type": "bind", "source": str(directory.resolve()), "target": "/tmp/aggregate-clock", "read_only": True},
                {"type": "bind", "source": str(entrypoint.resolve()), "target": "/tmp/aggregate-clock-worker.py", "read_only": True}]}
        for name, kind in (("aggregate-worker", "aggregate"), ("detector-worker", "detector"))
    }}, sort_keys=False), encoding="utf-8")
    return overlay, clock


def write_clock(path, moments, first_clear_release=None, *, canonical_phase=None):
    temporary = path.with_suffix(".next")
    body = {"aggregates": moments}
    if first_clear_release is not None:
        body["first_clear_release"] = first_clear_release
    if canonical_phase is not None:
        body["canonical_phase"] = canonical_phase
    temporary.write_text(json.dumps(body), encoding="utf-8")
    temporary.replace(path)


@dataclass(frozen=True)
class LoadCase:
    name: str
    aggregates: int
    events_per_source_window: int
    sources: int
    shifted: bool = False


def load_cases():
    return (
        LoadCase("baseline-1", 1, 5000, 1),
        LoadCase("shared-10-sparse", 10, 2, 1),
        LoadCase("distributed-20", 20, 1000, 3, True),
        LoadCase("shared-30-dense", 30, 5000, 1),
        LoadCase("distributed-30-dense", 30, 5000, 3, True),
    )


def selected_load_cases(group=None):
    """Explicit bounded runs; their union retains the entire unchanged matrix."""
    cases = load_cases()
    if group is None:
        return cases
    groups = {"baseline-through-20": cases[:3], "shared-30": cases[3:4], "distributed-30": cases[4:5]}
    if group not in groups:
        raise ValueError("Unknown controlled aggregate performance group")
    return groups[group]


def load_rule(index):
    if index % 3 == 0:
        return {"features": [{"key": "events", "name": "Events", "op": "count"},
            {"key": "pids", "name": "PIDs", "op": "distinct_count", "field": "process.pid",
             "where": {"op": "exists", "field": "process.pid"}}]}
    if index % 3 == 1:
        leaf = {"aggregate": {"op": "sum", "field": "network.bytes",
            "where": {"op": "has_any", "field": "tags", "value": ["controlled"]}}}
        return {"where": {"op": "all", "args": [
            {"op": "has_any", "field": "event.category", "value": ["authentication", "web"]},
            {"op": "gte", "field": "network.bytes", "value": 0}]},
            "auxiliary": [{"key": "n", "name": "Window size", "op": "count"}],
            "features": [
                {"key": "bytes", "name": "Bytes", "op": "formula", "expr": leaf},
                {"key": "average", "name": "Shared leaf average", "op": "formula",
                    "expr": {"op": "divide", "args": [leaf, {"ref": "n"}]}},
                {"key": "failures", "name": "Failures", "op": "count",
                    "where": {"op": "eq", "field": "event.outcome", "value": "failure"}}]}
    return {"features": [
        {"key": "median", "name": "Median", "op": "median", "field": "network.bytes"},
        {"key": "p95", "name": "Percentile", "op": "percentile", "field": "network.bytes", "percentile": .95},
        {"key": "mode", "name": "Mode", "op": "mode_count", "field": "process.pid"},
        {"key": "share", "name": "Mode share", "op": "mode_share", "field": "process.pid"},
        {"key": "variance", "name": "Variance", "op": "variance", "field": "network.bytes"},
        {"key": "average", "name": "Average", "op": "avg", "field": "network.bytes"}]}


def distribution(values):
    ordered = sorted(values)
    if not ordered or any(not math.isfinite(value) or value < 0 for value in ordered):
        raise ValueError("load timings require finite nonnegative real-clock samples")
    return {"samples": len(ordered), "p50": ordered[math.ceil(.5 * len(ordered)) - 1],
        "p95": ordered[math.ceil(.95 * len(ordered)) - 1], "max": ordered[-1]}


def expected_load_values(rule_index, offset, count):
    """Independent exact oracle for the real writer's consecutive coordinates."""
    if type(offset) is not int or offset < 0 or type(count) is not int or count <= 0:
        raise ValueError("load oracle requires a bounded positive coordinate range")
    first, last = Decimal(10 + offset), Decimal(10 + offset + count - 1)
    with localcontext() as context:
        context.prec = 100
        total = (first + last) * count / 2
        if rule_index % 3 == 0:
            return {"events": Decimal(count), "pids": Decimal(min(count, 97))}
        if rule_index % 3 == 1:
            successes = (offset + count - 1) // 4 - (offset - 1) // 4
            return {"bytes": total, "average": total / count, "failures": Decimal(count - successes)}
        frequency = (count + 96) // 97
        return {"median": (first + last) / 2, "p95": first + Decimal(".95") * (count - 1),
            "mode": Decimal(frequency), "share": Decimal(frequency) / count,
            "variance": Decimal(count * count - 1) / 12, "average": (first + last) / 2}


def assert_computed_load_point(point, expected, *, aggregate_id, window_index, close_at, count):
    assert point["aggregate_id"] == aggregate_id and point["window_index"] == window_index, point
    assert point["origin"] == "online" and point["time_basis"] == "event_time", point
    assert point["calculation_status"] == "computed" and point["calculation_reason"] is None, point
    assert point["payload_state"] == "present" and point["completeness"] == "complete", point
    assert point["event_count"] == count, point
    assert datetime.fromisoformat(point["close_not_before_at"]) == close_at, point
    assert [feature["key"] for feature in point["features"]] == list(expected), point
    for feature in point["features"]:
        value = feature["value"]
        assert value is not None and type(value) is not bool and feature["reason"] is None \
            and feature["invalid_count"] == 0, feature
        assert Decimal(str(value)) == expected[feature["key"]], feature
    execution = point["execution"]
    assert execution["policy_version"] == "window_execution_v1" and execution["soft_latency_seconds"] == 5
    assert execution["hard_budget_seconds"] == 30 and execution["calculation_status"] == "computed"
    assert execution["calculation_reason"] is None and execution["measured_through"] == "before_commit"
    assert UUID(execution["attempt_id"]) and execution["origin"] == "online" \
        and execution["window_index"] == window_index
    assert type(execution["elapsed_seconds"]) in (int, float) and math.isfinite(execution["elapsed_seconds"]) \
        and 0 <= execution["elapsed_seconds"] <= execution["hard_budget_seconds"]
    started_at = datetime.fromisoformat(execution["started_at"])
    deadline_at = datetime.fromisoformat(execution["execution_deadline_at"])
    assert started_at.utcoffset() == deadline_at.utcoffset() == timedelta(0)
    assert (deadline_at - started_at).total_seconds() == execution["hard_budget_seconds"]


def measured_job_trace(step, released, duration):
    """Use measured boundaries; never infer enqueue/start from queue plus core."""
    trace = step["job_trace"]
    assert isinstance(trace["job_id"], str) and 0 < len(trace["job_id"]) <= 80
    assert trace["aggregate_id"] == step["aggregate_id"] and trace["kind"] == "foreground"
    names = ("iteration_started_monotonic", "discovery_started_monotonic", "discovery_completed_monotonic",
        "append_before_monotonic", "append_after_monotonic", "acquire_before_monotonic",
        "acquire_after_monotonic", "execute_started_monotonic")
    values = [trace[name] for name in names]
    values.extend([step["point_commit_start_monotonic"], step["point_commit_ack_monotonic"],
        trace["execute_completed_monotonic"], step["commit_observed_monotonic"]])
    assert all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in values), trace
    assert values == sorted(values), trace
    assert math.isclose(step["queue_seconds"], values[7] - values[0], abs_tol=1e-8), trace
    assert math.isclose(step["total_seconds"], values[10] - values[7], abs_tol=1e-8), trace
    hard = min(30.0, .8 * duration)
    assert values[8] - values[7] <= hard, {"reason": "computed_publication_started_after_hard_budget", **trace}
    ack_delay = values[9] - released
    assert 0 <= ack_delay < duration, {"reason": "next_close_deadline_missed", "ack_delay": ack_delay, **trace}
    return {**trace, "point_id": step["point_id"], "released_monotonic": released,
        "commit_started_monotonic": values[8], "commit_ack_monotonic": values[9],
        "commit_observed_monotonic": values[11], "discovery_seconds": values[2] - values[1],
        "fifo_wait_lower_seconds": max(0, values[5] - values[4]),
        "fifo_wait_upper_seconds": values[6] - values[3],
        "dispatch_lower_seconds": max(0, values[7] - values[6]),
        "dispatch_upper_seconds": values[7] - values[5],
        "release_to_discovery_seconds": values[1] - released,
        "release_to_execute_seconds": values[7] - released,
        "discovery_to_execute_seconds": values[7] - values[0],
        "commit_ack_delay_seconds": ack_delay, "soft_latency_exceeded": ack_delay > 5,
        "late_commit_ack": values[9] - values[7] > hard}


def assert_job_trace_health(metrics):
    trace = metrics["job_trace"]
    assert trace["dropped"] == trace["error_count"] == 0 and trace["errors"] == [], trace
    assert 0 <= trace["live"] <= trace["max_live"] <= 66, trace


def container_monotonic(compose, env, command):
    # Probe the same container/time namespace as the measured worker. The
    # marker precedes the atomic clock-file write, so the resulting interval
    # conservatively includes control transport rather than subtracting VM/host
    # UTC clocks (which need not have the same offset).
    value = float(command([*compose, "exec", "-T", "aggregate-worker", "python", "-c",
        "import time; print(time.monotonic())"], env=env, capture=True, timeout=5).strip())
    if not math.isfinite(value) or value < 0:
        raise ValueError("worker release probe must be a real monotonic clock")
    return value


def commit_delay_upper_bound(step, released):
    observed = step["commit_observed_monotonic"]
    if not all(isinstance(value, (int, float)) and math.isfinite(value) for value in (observed, released)):
        raise ValueError("commit delay requires the same container's real monotonic clocks")
    delay = observed - released
    if delay < 0:
        raise ValueError("measured commit preceded its controlled deadline release")
    return delay


def maintenance_overlap(spans, intervals, heavy_closures):
    """Intersect actual committed GC progress with measured online intervals."""
    def moment(value):
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("maintenance overlap requires real monotonic timestamps")
        return value

    for interval in intervals:
        if not moment(interval["released"]) <= moment(interval["commit_started"]) <= moment(interval["commit_ack"]):
            raise ValueError("online commit preceded its measured release")
    # The large-progress ring deliberately preserves heavy evidence after
    # ordinary tiny chunks have evicted it from the general bounded ring.
    spans = list({span.get("sequence", ("sample", index)): span for index, span in enumerate(spans)}.values())
    result = {}
    for name, selected in (("normal_gc", spans),
            ("target_history", [span for span in spans if span["closure_id"] in heavy_closures]),
            ("target_history_heavy", [span for span in spans
                if span["closure_id"] in heavy_closures and span["rows_changed"] > 3])):
        samples, observed_samples, matched, observed, phases = set(), set(), [], [], {}
        for span in selected:
            start, sql_end, committed = (moment(span[key]) for key in
                ("started_monotonic", "sql_completed_monotonic", "committed_monotonic"))
            if not start <= sql_end <= committed or type(span["rows_changed"]) is not int or span["rows_changed"] <= 0:
                raise ValueError("maintenance span requires confirmed positive physical progress")
            overlaps = {index for index, interval in enumerate(intervals)
                if interval["released"] <= start < committed <= interval["commit_started"]}
            intersections = {index for index, interval in enumerate(intervals)
                if max(start, interval["released"]) < min(committed, interval["commit_ack"])}
            if intersections:
                observed_samples.update(intersections)
                observed.append(span)
            if overlaps:
                samples.update(overlaps)
                matched.append(span)
                phase = phases.setdefault(span["phase"], {"committed_spans": 0, "rows_changed": 0})
                phase["committed_spans"] += 1
                phase["rows_changed"] += span["rows_changed"]
        result[name] = {"contained_online_samples": len(samples), "contained_committed_spans": len(matched),
            "contained_phases": phases, "observed_intersection_online_samples": len(observed_samples),
            "observed_intersection_committed_spans": len(observed)}
    return result


def wave_times(anchor, wave):
    index = 520 + wave
    start = anchor + timedelta(seconds=index * 300)
    return index, start, start + timedelta(seconds=600)


def wait_for_history_clear(
    client, old_points, rows, overall_deadline, *, on_present=None, capture=None
):
    """Observe one genuine present-to-cleared REST transition, without holding work."""
    budget = PerformanceBudget(overall_deadline)

    def checked(condition, site, details):
        try:
            assert condition, details
        except AssertionError:
            if capture is not None:
                _capture_safely(lambda: capture.__setitem__("assert_site", site))
            raise

    def cleared(aggregate_id, point_id):
        response = budget.call(client.get, f"/api/v1/aggregates/{aggregate_id}/points/{point_id}", cap=10)
        record = None
        if capture is not None:

            def record_response():
                capture["responses_observed"] += 1
                if len(capture["responses"]) == 30:
                    capture["responses"].pop(0)
                    capture["responses_evicted"] += 1
                value = {
                    "aggregate_id": str(UUID(aggregate_id)),
                    "point_id": str(UUID(point_id)),
                    "status": response.status_code if 100 <= response.status_code <= 599 else None,
                    "call_enter_check": budget.last_call_checks[0],
                    "call_return_check": budget.last_call_checks[1],
                    "cleared_at": None,
                }
                capture["responses"].append(value)
                return value

            record = _capture_safely(record_response)
        body = response.json()
        if response.status_code == 200:
            checked(
                body["id"] == point_id and body["aggregate_id"] == aggregate_id,
                "present_identity",
                body,
            )
            checked(
                body["payload_state"] == "present" and body["event_count"] == rows,
                "present_shape",
                body,
            )
            metadata = body
        else:
            checked(
                response.status_code == 410 and body["code"] == "aggregate_point_cleared",
                "cleared_status",
                body,
            )
            metadata = body["details"]
            checked(
                metadata["origin_point_id"] == point_id and metadata["clear_reason"] == "automatic",
                "cleared_identity",
                body,
            )
            checked(
                metadata["clear_policy_version"] == "model_history_v1" and metadata["cleared_at"],
                "cleared_policy",
                body,
            )
            if record is not None:

                def record_cleared_at():
                    value = metadata["cleared_at"]
                    if isinstance(value, str) and len(value) <= 40:
                        parsed = datetime.fromisoformat(value)
                        if parsed.tzinfo is not None:
                            record["cleared_at"] = parsed.astimezone(UTC).isoformat()

                _capture_safely(record_cleared_at)
        checked(
            metadata["window_index"] == 8 and metadata["origin"] == "initial_history",
            "window_origin",
            body,
        )
        checked(metadata["time_basis"] == "event_time", "time_basis", body)
        budget.remaining()
        return response.status_code == 410

    with budget.limit(120, "History transition observation timed out"):
        if capture is not None:
            _capture_safely(
                lambda: capture["markers"].__setitem__(
                    "selection_scope_enter_check", budget.checked_monotonic
                )
            )
        candidates, seen_present, selected = list(old_points.items()), set(), None
        while candidates:
            remaining = []
            for aggregate_id, point_id in candidates:
                pair = (aggregate_id, point_id)
                if not cleared(aggregate_id, point_id):
                    seen_present.add(pair)
                    remaining.append(pair)
                    if selected is None and on_present is not None:
                        selected = pair
                        if capture is not None:
                            _capture_safely(
                                lambda aggregate_id=aggregate_id, point_id=point_id: (
                                    capture.__setitem__(
                                        "selected",
                                        {
                                            "aggregate_id": str(UUID(aggregate_id)),
                                            "point_id": str(UUID(point_id)),
                                        },
                                    )
                                )
                            )
                        on_present(aggregate_id, point_id)
                        # Selection ends the scan. A missed claim or failed arm
                        # cannot fall back to another target after this 200.
                        break
                elif pair in seen_present:
                    return {"aggregate_id": aggregate_id, "point_id": point_id,
                        "history_rows": rows, "observed_transition": "200_to_automatic_410"}
                # Initial 410 retires only this candidate and never triggers a
                # wave using another point's previously observed 200.
            candidates = remaining
        try:
            raise AssertionError(
                "All old history targets were already cleared "
                "without an observed 200-to-410 transition"
            )
        except AssertionError:
            if capture is not None:
                _capture_safely(lambda: capture.__setitem__("assert_site", "all_initial_410"))
            raise


def first_clear_plan(run_id, case, cards, deadlines, old_points, *, valid_for_seconds=120, selected_id=None):
    """Fix the selected before-proof identity; a missed claim never retargets."""
    selected = cards[0]["id"] if selected_id is None else selected_id
    assert selected in old_points and any(card["id"] == selected for card in cards)
    groups = []
    for group in ((2, 1, 0) if case.shifted else (0,)):
        groups.append({"after_seconds": len(groups) * .15, "moments": {
            card["id"]: deadlines[card["id"]].isoformat() for index, card in enumerate(cards)
            if not case.shifted or index % 3 == group}})
    return {"run_id": run_id, "case_id": case.name, "wave_index": 521,
        "aggregate_id": selected, "closure_id": old_points[selected], "nonce": str(uuid4()), "groups": groups,
        "valid_for_seconds": valid_for_seconds}


def acknowledged_release(metrics, plan):
    """Require this arm's actual markers, never a stale metrics-file signal."""
    snapshot = metrics.get("first_clear_release")
    if snapshot is None or snapshot.get("receipt") is None:
        return None
    receipt = snapshot["receipt"]
    scope = {key: plan[key] for key in ("run_id", "case_id", "wave_index", "aggregate_id", "closure_id", "nonce")}
    assert all(receipt.get(key) == value for key, value in scope.items()), "Foreign first-clear release receipt"
    expected = {identity for group in plan["groups"] for identity in group["moments"]}
    released = receipt["released"]
    assert released.keys() <= expected and len(receipt["groups"]) <= len(plan["groups"])
    previous = receipt["commit_ack_monotonic"]
    assert isinstance(previous, (int, float)) and math.isfinite(previous) and previous >= 0
    for index, group in enumerate(receipt["groups"]):
        marker = group["released_monotonic"]
        assert group["after_seconds"] == plan["groups"][index]["after_seconds"]
        assert set(group["aggregate_ids"]) == set(plan["groups"][index]["moments"])
        assert isinstance(marker, (int, float)) and math.isfinite(marker) and marker >= previous + (.15 if index else 0)
        assert all(released.get(identity) == marker for identity in group["aggregate_ids"])
        previous = marker
    return receipt if released.keys() == expected else None


@contextmanager
def armed_release(clock_file, moments, plan):
    """Remove the case's arm after success, rollback, timeout or other failure."""
    write_clock(clock_file, moments, plan)
    try:
        yield
    finally:
        write_clock(clock_file, moments)


def history_gc_acknowledged(maintenance, targets, rows):
    """Require the collector snapshot to include every target's last actual ACK."""
    counters = maintenance["heavy_membership_ack_rows"]
    assert 0 < len(targets) <= 30
    assert len(counters) <= 30 and maintenance["dropped_heavy_membership_acks"] == 0, {
        "counter_count": len(counters), "dropped": maintenance["dropped_heavy_membership_acks"]}
    assert all(type(value) is int and value > 0 for value in counters.values()), counters
    assert all(counters.get(identity, 0) <= rows for identity in targets), counters
    return all(counters.get(identity) == rows for identity in targets)


def bounded_online_intervals(intervals, expected):
    assert len(intervals) == expected and 0 < expected <= 90, "Every measured canonical interval must be retained"
    assert len({(item["aggregate_id"], item["window_index"]) for item in intervals}) == expected
    for item in intervals:
        assert set(item) == {"aggregate_id", "point_id", "window_index", "released", "commit_started",
            "commit_ack", "commit_observed"}
        assert UUID(item["aggregate_id"]) and UUID(item["point_id"])
        assert type(item["window_index"]) is int and item["window_index"] >= 0
        moments = [item[key] for key in ("released", "commit_started", "commit_ack", "commit_observed")]
        assert all(isinstance(value, (int, float)) and math.isfinite(value) and value >= 0 for value in moments)
        assert moments == sorted(moments), "Commit bounds must follow their real release"
    assert len(json.dumps(intervals, separators=(",", ":")).encode("utf-8")) <= 128 * 1024
    return intervals


def assert_worker_resources(metrics):
    summary = {key: metrics.get(key) for key in ("peak_memory_bytes", "memory_metric_error", "memory_peak_source",
        "memory_cgroup_version", "memory_limit_bytes", "peak_rss_bytes", "max_reserved_bytes", "max_active", "max_queued")}
    assert metrics["peak_memory_bytes"] is not None and metrics["peak_memory_bytes"] <= 768 * 1024 * 1024, summary
    assert metrics["memory_metric_error"] is None and metrics["memory_limit_bytes"] is not None \
        and 0 < metrics["memory_limit_bytes"] <= 768 * 1024 * 1024, summary
    assert metrics["max_reserved_bytes"] <= 512 * 1024 * 1024, summary
    assert metrics["max_active"] <= 2 and metrics["max_queued"] <= 64, summary


def pending_diagnostics(compose, env, command, client, case, wave, pending, *, index=None, released=None,
        reason="online_deadline"):
    """After expiry, allow only these separate 5s/3x.5s diagnostic read caps."""
    print("Controlled aggregate failure: " + json.dumps({"case": case.name, "reason": reason,
        "wave": wave, "index": index, "pending": sorted(pending), "released_monotonic": released}), flush=True)
    try:
        metrics = json.loads(command([*compose, "exec", "-T", "aggregate-worker", "cat",
            "/tmp/aggregate-performance.json"], env=env, capture=True, timeout=5))
        print("Controlled aggregate failure metrics: " + json.dumps({
            "worker": {key: metrics.get(key) for key in ("max_active", "max_queued", "max_reserved_bytes",
                "peak_memory_bytes", "memory_limit_bytes", "memory_peak_source", "memory_metric_error", "peak_rss_bytes")},
            "closed_last30": [{key: step.get(key) for key in ("aggregate_id", "point_id", "queue_seconds", "timings",
                "commit_observed_monotonic", "point_commit_start_monotonic", "point_commit_ack_monotonic")}
                for step in list(metrics.get("closed", {}).values())[-30:]],
            "maintenance": {key: metrics.get("maintenance", {}).get(key) for key in
                ("sequence", "dropped_spans", "dropped_large_spans", "phases")},
            "large_gc_last8": metrics.get("maintenance", {}).get("large_spans", [])[-8:]}), flush=True)
    except (RuntimeError, OSError, ValueError, SubprocessError) as error:
        print("Controlled aggregate failure metrics unavailable: " + type(error).__name__, flush=True)
    for identity in sorted(pending)[:3]:
        try:
            response = client.get(f"/api/v1/aggregates/{identity}", timeout=.5)
            response.raise_for_status()
            card = response.json()
            print("Controlled aggregate pending position: " + json.dumps({"aggregate_id": identity,
                **{key: card.get(key) for key in ("next_index", "lag_seconds", "next_close_not_before_at",
                    "paused", "last_error", "execution_mode")}}), flush=True)
        except (ValueError, httpx.HTTPError) as error:
            print("Controlled aggregate pending readback unavailable: " + type(error).__name__, flush=True)


def run_aggregate_performance(compose, env, production, run, command, *, case_group=None):
    """Run the controlled matrix; never derive real lag from virtual timestamps."""
    overall_started = time.monotonic()
    overall_deadline = overall_started + 1200
    budget = PerformanceBudget(overall_deadline, started=overall_started)
    cases = selected_load_cases(case_group)
    print("Controlled aggregate budget starting: " + json.dumps({"budget_seconds": 1200,
        "case_group": case_group or "all", "case_ids": [case.name for case in cases],
        "host_started_monotonic": overall_started, "host_deadline_monotonic": overall_deadline,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "elapsed_basis": "host_monotonic", **budget.observation()}), flush=True)
    clock_file = Path(env["AGGREGATE_CLOCK_FILE"])
    helper = (ROOT / "backend/tests/aggregate_load_runtime.py").read_text(encoding="utf-8")
    budget.remaining()

    def bounded_command(*args, **kwargs):
        return budget.call(command, *args, cap=kwargs.pop("timeout", None), **kwargs)

    reports = []
    for case in cases:
        budget.remaining()
        # Register every explicit UUID clock before its first real worker read.
        # Otherwise a just-created target can briefly fall back to physical
        # window time and permanently close unknown initial-history snapshots.
        bounded_command([*compose, "stop", "aggregate-worker"], env=env)
        case_started = time.monotonic()
        username, password = "aggregate-load-" + uuid4().hex, secrets.token_urlsafe(24)

        def fixture(action, payload, username=username, password=password):
            output = bounded_command([*compose, "exec", "-T", "-e", f"AGGREGATE_LOAD_USER={username}",
                "-e", f"AGGREGATE_LOAD_PASSWORD={password}", "-e", "AGGREGATE_LOAD_PAYLOAD=" + json.dumps(payload),
                "api", "python", "-c", helper, action], env=env, capture=True, timeout=30)
            return json.loads(output.strip().splitlines()[-1])

        seeded = fixture("setup", {"sources": case.sources})
        print("Controlled aggregate case starting: " + json.dumps({"case": case.name,
            "source_ids": seeded["sources"][:3], "case_elapsed_seconds": time.monotonic() - case_started,
            **budget.observation()}), flush=True)
        heavy_rows = seeded["heavy_history_rows"]
        assert heavy_rows == 10001
        anchor = datetime.fromisoformat(seeded["anchor"])
        offsets = {identity: 0 for identity in seeded["sources"]}
        ingest = []

        def write(source, timestamp, count, offsets=offsets, ingest=ingest, capture=None):
            for start in range(0, count, 1000):
                facts = fixture("write", {"source": source, "timestamp": timestamp.isoformat(),
                    "count": min(1000, count - start), "offset": offsets[source]})
                offsets[source] = facts["offset"]
                ingest.append(facts["seconds"])
                if capture is not None:

                    def record_batch(facts=facts):
                        capture["batches_observed"] += 1
                        if len(capture["batch_seconds"]) < 15:
                            value = facts["seconds"]
                            capture["batch_seconds"].append(
                                value
                                if isinstance(value, (int, float))
                                and not isinstance(value, bool)
                                and math.isfinite(value)
                                and 0 <= value <= 1200
                                else None
                            )
                        else:
                            capture["batches_dropped"] += 1
                        capture["markers"].setdefault(
                            "first_write_call_enter_check", budget.last_call_checks[0]
                        )
                        capture["markers"]["last_write_call_return_check"] = (
                            budget.last_call_checks[1]
                        )

                    _capture_safely(record_batch)

        for source in seeded["sources"]:
            # One genuinely large old membership forces bounded physical GC.
            write(source, anchor + timedelta(seconds=8 * 300), heavy_rows)
            facts = fixture("history", {"source": source, "anchor": seeded["anchor"], "offset": offsets[source]})
            offsets[source] = facts["offset"]
            ingest.append(facts["seconds"])

        origin = env["PUBLIC_ORIGIN"]
        with httpx.Client(base_url=origin, verify=env["KAFKA_CA_FILE"] if production else True, timeout=10) as client:
            csrf = {}

            def request(method, path, body=None, status=200, key=None, csrf=csrf):
                headers = dict(csrf) if method != "GET" else {}
                if key:
                    headers["Idempotency-Key"] = key
                response = budget.call(client.request, method, "/api/v1/" + path, json=body, headers=headers, cap=10)
                assert response.status_code == status, (path, response.status_code, response.text[:300])
                return response.json()

            request("POST", "auth/local/login", {"username": username, "password": password})
            csrf.update({"X-CSRF-Token": request("GET", "auth/session")["csrf_token"]})
            cards, moments, creates = [], {}, []
            for index in range(case.aggregates):
                created = fixture("create", {"source": seeded["sources"][index % case.sources],
                    "actor": seeded["actor"], "rule": load_rule(index), "name": f"{case.name} {index}",
                    "anchor": seeded["anchor"], "clock_at": seeded["clock_at"],
                    "anchor_shift": -(index % 3) if case.shifted else 0})
                card = created["card"]
                creates.append(created["seconds"])
                assert card["online_start_index"] == 520, card
                cards.append(card)
                moments[card["id"]] = seeded["clock_at"]
                write_clock(clock_file, moments)

            identities = [card["id"] for card in cards]
            assert len(set(identities)) == case.aggregates
            bounded_command([*compose, "up", "-d", "--no-deps", "--wait", "aggregate-worker"], env=env)
            inspect_payload = {"aggregates": identities, "sources": seeded["sources"]}

            def wait(predicate, seconds=120, case=case, *, on_ready=None):
                return budget.wait(
                    predicate,
                    seconds,
                    (case.name, "controlled load condition timed out"),
                    on_ready=on_ready,
                )

            last_completed_readiness = None

            def before_ready(inspect_payload=inspect_payload, identities=identities):
                nonlocal last_completed_readiness
                facts = fixture("inspect", inspect_payload)
                if not facts:
                    return None
                deadline = budget.limits[-1][0]
                missing = [identity for identity in identities
                    if facts["aggregates"].get(identity, {}).get("old_payloads") != 1]
                last_completed_readiness = {"requested_count": len(identities),
                    "present_count": len(identities) - len(missing), "missing_ids": missing[:30],
                    "capture_pending": facts["capture_pending"],
                    **budget.observation(started=deadline - 120, deadline=deadline)}
                return facts if all(value["old_payloads"] == 1 for value in facts["aggregates"].values()) \
                    and facts["capture_pending"] == 0 else None

            try:
                before = wait(before_ready)
            except Exception:
                print("Controlled aggregate before-proof failure: " + json.dumps({"case": case.name,
                    "last_completed_readiness": last_completed_readiness}), flush=True)
                raise
            assert all(value["membership_pairs"] == value["membership_details"] == heavy_rows
                and value["old_payload_state"] == "present" and value["old_snapshots"] == 1
                for value in before["aggregates"].values()), before
            release_times, health, waves, wave_progress, executions = {}, [], [], [], {}
            history_release = None
            for wave in range(3):
                target_index, event_at, close_at = wave_times(anchor, wave)
                deadlines = {card["id"]: datetime.fromisoformat(card["anchor_utc"]) + timedelta(
                    seconds=(target_index + 2) * 300) for card in cards}
                assert {int((close_at - deadline).total_seconds()) for deadline in deadlines.values()} == (
                    {0, 1, 2} if case.shifted else {0})
                # Seed just this real wave before its deadline is released.
                # Later waves cannot create future-time exclusions in earlier
                # snapshots; the unchanged classifier decides completeness.
                wave_offsets = dict(offsets)
                failure_capture = (
                    _capture_safely(
                        lambda: {
                            "version": 1,
                            "response_limit": 30,
                            "batch_limit": 15,
                            "byte_limit": 16384,
                            "responses_observed": 0,
                            "responses_evicted": 0,
                            "batches_observed": 0,
                            "batches_dropped": 0,
                            "responses": [],
                            "batch_seconds": [],
                            "markers": {},
                            "selected": None,
                            "arm_write_completed": False,
                            "assert_site": None,
                        }
                    )
                    if wave == 1 and case.name == "distributed-30-dense"
                    else None
                )
                for source in seeded["sources"]:
                    write(source, event_at, case.events_per_source_window, capture=failure_capture)
                # Start the unchanged next-close cap before any causal release
                # transport. Waiting for the claim cannot extend the online cap.
                wave_started = time.monotonic()
                if failure_capture is not None:
                    _capture_safely(
                        lambda failure_capture=failure_capture, wave_started=wave_started: (
                            failure_capture["markers"].__setitem__(
                                "wave_started_after_write", wave_started
                            )
                        )
                    )
                if wave == 1 and case.aggregates >= 10:
                    old_points = {identity: value["old_point_id"] for identity, value in before["aggregates"].items()}
                    plan = None
                    try:
                        with budget.until(wave_started + 120, "Prepared history release timed out", started=wave_started), \
                                budget.until(wave_started + 300, (case.name, wave, "next online close deadline"),
                                started=wave_started), ExitStack() as release_scope:
                            # The real writer has committed and its real markers
                            # must be materialized before the arm becomes visible.
                            wait(
                                lambda inspect_payload=inspect_payload: fixture(
                                    "inspect", inspect_payload)["capture_pending"] == 0,
                                on_ready=(
                                    lambda checked, failure_capture=failure_capture:
                                        failure_capture["markers"].__setitem__(
                                            "markers_ready_wait_check", checked)
                                ) if failure_capture is not None else None)

                            def arm(aggregate_id, point_id, moments=moments, cards=cards,
                                    deadlines=deadlines, old_points=old_points, case=case,
                                    failure_capture=failure_capture):
                                nonlocal plan
                                assert plan is None and old_points[aggregate_id] == point_id
                                plan = first_clear_plan(run.name, case, cards, deadlines, old_points,
                                    valid_for_seconds=budget.remaining(120), selected_id=aggregate_id)
                                release_scope.enter_context(armed_release(clock_file, moments, plan))
                                if failure_capture is not None:
                                    _capture_safely(
                                        lambda: failure_capture.__setitem__(
                                            "arm_write_completed", True))

                            history_release = wait_for_history_clear(client,
                                old_points, heavy_rows,
                                min(overall_deadline, wave_started + 120),
                                on_present=arm, capture=failure_capture)
                            assert plan is not None

                            def release_receipt(plan=plan):
                                value = json.loads(bounded_command([*compose, "exec", "-T", "aggregate-worker", "cat",
                                    "/tmp/aggregate-performance.json"], env=env, capture=True, timeout=5))
                                return acknowledged_release(value, plan)

                            receipt = wait(release_receipt, seconds=10)
                            for group in plan["groups"]:
                                moments.update(group["moments"])
                            release_times.update({(identity, target_index): marker
                                for identity, marker in receipt["released"].items()})
                            history_release["release"] = receipt
                    except (AssertionError, RuntimeError, ValueError, httpx.HTTPError) as error:
                        if failure_capture is not None:
                            try:
                                # Scalar metadata only; no error repr, bodies or frame locals.
                                function, line = "unknown", None
                                trace = error.__traceback__
                                allowed = {
                                    "remaining",
                                    "call",
                                    "wait",
                                    "cleared",
                                    "wait_for_history_clear",
                                    "arm",
                                    "first_clear_plan",
                                    "acknowledged_release",
                                    "run_aggregate_performance",
                                }
                                while trace is not None:
                                    name = trace.tb_frame.f_code.co_name
                                    if (
                                        trace.tb_frame.f_code.co_filename == __file__
                                        and name in allowed
                                    ):
                                        function, line = name, trace.tb_lineno
                                    trace = trace.tb_next
                                failure_capture["failure"] = {
                                    "function": function,
                                    "line": line,
                                    "kind": "assertion"
                                    if isinstance(error, AssertionError)
                                    else "runtime"
                                    if isinstance(error, RuntimeError)
                                    else "value"
                                    if isinstance(error, ValueError)
                                    else "http",
                                }
                                prefix = "Controlled aggregate D30 failure capture: "
                                encoded = json.dumps(failure_capture, allow_nan=False)
                                while (
                                    len(prefix.encode("utf-8")) + len(encoded.encode("utf-8")) + 2
                                    > 16384
                                    and failure_capture["responses"]
                                ):
                                    failure_capture["responses"].pop(0)
                                    failure_capture["responses_evicted"] += 1
                                    encoded = json.dumps(failure_capture, allow_nan=False)
                                if (
                                    len(prefix.encode("utf-8")) + len(encoded.encode("utf-8")) + 2
                                    <= 16384
                                ):
                                    print(prefix + encoded, flush=True)
                            except Exception:  # noqa: BLE001 - preserve the real outcome
                                pass
                        pending_diagnostics(compose, env, command, client, case, wave, set(identities), index=8,
                            reason="history_transition_not_observed")
                        raise
                else:
                    for group in ((2, 1, 0) if case.shifted else (0,)):
                        released = container_monotonic(compose, env, bounded_command)
                        for index, card in enumerate(cards):
                            if not case.shifted or index % 3 == group:
                                moments[card["id"]] = deadlines[card["id"]].isoformat()
                                release_times[(card["id"], target_index)] = released
                        write_clock(clock_file, moments)
                        if case.shifted:
                            budget.sleep(.15)
                # Ingest remains live while ordinary/history/GC tasks compete.
                # These valid old events exercise durable late discovery without
                # changing the exact density of the measured online windows.
                for source in seeded["sources"]:
                    write(source, anchor, 2)
                pending = set(identities)
                points = {}
                try:
                    with budget.until(wave_started + 300,
                            (case.name, wave, "next online close deadline"), started=wave_started):
                        while pending:
                            started = time.monotonic()
                            request("GET", "auth/session")
                            health.append(time.monotonic() - started)
                            for identity in tuple(pending):
                                response = budget.call(client.get,
                                    f"/api/v1/aggregates/{identity}/points/by-index/{target_index}", cap=10)
                                if response.status_code == 409:
                                    continue
                                response.raise_for_status()
                                point = response.json()
                                index = identities.index(identity)
                                source = seeded["sources"][index % case.sources]
                                expected = expected_load_values(index, wave_offsets[source], case.events_per_source_window)
                                assert_computed_load_point(point, expected, aggregate_id=identity,
                                    window_index=target_index, close_at=deadlines[identity],
                                    count=case.events_per_source_window)
                                executions[point["id"]] = point["execution"]
                                points[identity] = point["id"]
                                pending.remove(identity)
                            budget.remaining()
                            if pending:
                                budget.sleep(.01)
                except (AssertionError, RuntimeError, httpx.HTTPError):
                    pending_diagnostics(compose, env, command, client, case, wave, pending, index=target_index,
                        released={identity: release_times[(identity, target_index)] for identity in identities})
                    raise
                waves.append(points)
                progress = {"window_index": target_index, "expected": case.aggregates,
                    "computed": len(points), "uncomputed": 0, "pending": len(pending),
                    "host_elapsed_since_first_release": time.monotonic() - wave_started}
                assert progress["computed"] == case.aggregates and progress["pending"] == 0, progress
                wave_progress.append(progress)
                print("Controlled aggregate wave complete: " + json.dumps({"case": case.name, **progress,
                    **budget.observation()}), flush=True)
                budget.sleep(1)

            expected_points = {point for points in waves for point in points.values()}

            def read_metrics(expected_points=expected_points):
                value = json.loads(bounded_command([*compose, "exec", "-T", "aggregate-worker", "cat",
                    "/tmp/aggregate-performance.json"], env=env, capture=True, timeout=5))
                return value if expected_points <= value["closed"].keys() else None

            metrics = wait(read_metrics, seconds=10)
            samples, phases, queues, intervals, traces, whole = [], {}, [], [], [], []
            for wave, points in enumerate(waves):
                for identity, point_id in points.items():
                    step = metrics["closed"][point_id]
                    released = release_times[(identity, wave_times(anchor, wave)[0])]
                    delay = commit_delay_upper_bound(step, released)
                    samples.append(delay)
                    traces.append(measured_job_trace(step, released, 300))
                    whole.append(step["total_seconds"])
                    intervals.append({"aggregate_id": identity, "point_id": point_id,
                        "window_index": wave_times(anchor, wave)[0],
                        "released": released, "commit_started": step["point_commit_start_monotonic"],
                        "commit_ack": step["point_commit_ack_monotonic"],
                        "commit_observed": step["commit_observed_monotonic"]})
                    queues.append(step["queue_seconds"])
                    for phase, seconds in step["timings"].items():
                        phases.setdefault(phase, []).append(seconds)
            delays = distribution(samples)
            assert len(samples) == case.aggregates * 3
            assert len(traces) == len(samples) and len({item["job_id"] for item in traces}) == len(samples)
            assert len(json.dumps(traces, separators=(",", ":")).encode()) <= 256 * 1024
            assert_job_trace_health(metrics)
            cold_history = {}
            for identity in identities:
                for phase, seconds in metrics["history_first"][identity]["timings"].items():
                    cold_history.setdefault(phase, []).append(seconds)
            heavy_closures = {value["old_point_id"] for value in before["aggregates"].values()}
            measured = {"case": vars(case), "commit_delay_seconds": delays,
                "heavy_history_rows": heavy_rows, "history_gc_release": history_release,
                "online_intervals": bounded_online_intervals(intervals, case.aggregates * 3),
                "commit_delay_basis": "same_container_monotonic_prewrite_to_observed_commit_upper_bound",
                "elapsed_seconds": time.monotonic() - case_started,
                "case_group": case_group or "all", "wave_progress": wave_progress,
                "execution_attempts": executions, "job_traces": traces,
                "whole_task_seconds": distribution(whole),
                "commit_ack_delay_seconds": distribution([item["commit_ack_delay_seconds"] for item in traces]),
                "soft_latency_seconds": 5,
                "soft_latency_exceeded_samples": sum(item["soft_latency_exceeded"] for item in traces),
                "late_commit_ack_samples": sum(item["late_commit_ack"] for item in traces),
                "queue_semantics": "legacy age from discovery iteration start; includes discovery SQL",
                "discovery_to_execute_seconds": distribution(queues),
                "discovery_seconds": distribution([item["discovery_seconds"] for item in traces]),
                "fifo_wait_lower_seconds": distribution([item["fifo_wait_lower_seconds"] for item in traces]),
                "fifo_wait_upper_seconds": distribution([item["fifo_wait_upper_seconds"] for item in traces]),
                "dispatch_lower_seconds": distribution([item["dispatch_lower_seconds"] for item in traces]),
                "dispatch_upper_seconds": distribution([item["dispatch_upper_seconds"] for item in traces]),
                "queue_seconds": distribution(queues), "phases": {phase: distribution(values) for phase, values in phases.items()},
                "ingest_batch_seconds": distribution(ingest), "api_seconds": distribution(health),
                "create_service_seconds": distribution(creates),
                "first_history_phases": {phase: distribution(values) for phase, values in cold_history.items()},
                "maintenance_overlap": maintenance_overlap(metrics["maintenance"]["spans"]
                    + metrics["maintenance"]["large_spans"], intervals, heavy_closures),
                "worker": {key: value for key, value in metrics.items() if key != "closed"}, "before": before}
            (run / "aggregate-performance-current.json").write_text(json.dumps(measured, indent=2), encoding="utf-8")
            print("Controlled aggregate measured phases: " + json.dumps(measured), flush=True)
            assert_worker_resources(metrics)
            after = wait(lambda inspect_payload=inspect_payload: (facts if all(value["old_payloads"] == value["old_snapshots"] ==
                value["membership_details"] == 0 and value["auto_cleared_total"] > 0
                for value in facts["aggregates"].values()) else None)
                if (facts := fixture("inspect", inspect_payload)) else None)
            assert all(value["membership_pairs"] == heavy_rows and value["old_payload_state"] == "cleared"
                and value["clear_reason"] == "automatic" and value["active_cleanup_id"] is None
                and value["next_index"] >= 523 and value["terminal_uncomputed"] == 0
                and value["initial_history_closed"] >= before["aggregates"][identity]["initial_history_closed"]
                and value["execution_generation"] == before["aggregates"][identity]["execution_generation"]
                for identity, value in after["aggregates"].items()), after
            assert 0 < after["connections"].get("controlled-aggregate-worker", 0) <= 3, after["connections"]
            assert sum(after["connections"].values()) <= 30, after["connections"]
            def read_final_metrics(read_metrics=read_metrics, heavy_closures=heavy_closures, heavy_rows=heavy_rows):
                value = read_metrics()
                return value if value is not None and history_gc_acknowledged(
                    value["maintenance"], heavy_closures, heavy_rows) else None

            final_metrics = wait(read_final_metrics, seconds=10)
            assert_worker_resources(final_metrics)
            assert_job_trace_health(final_metrics)
            overlap = maintenance_overlap(final_metrics["maintenance"]["spans"]
                + final_metrics["maintenance"]["large_spans"], intervals, heavy_closures)
            report = {**measured, "maintenance_overlap": overlap,
                "worker": {key: value for key, value in final_metrics.items() if key != "closed"},
                "after": after}
            reports.append(report)
            (run / "aggregate-performance.json").write_text(json.dumps(reports, indent=2), encoding="utf-8")
            print("Controlled aggregate performance: " + json.dumps(report), flush=True)
            if case.aggregates >= 10:
                assert overlap["target_history_heavy"]["contained_online_samples"] > 0, (case.name,
                    "No measured online interval overlapped committed heavy history GC", overlap)
            for identity in identities:
                request("DELETE", f"aggregates/{identity}")
            write_clock(clock_file, {})
    budget.remaining()
    return reports
