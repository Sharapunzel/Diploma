"""Browser selection and preparation within the existing smoke Compose lifecycle."""
import json
import math
import os
import re
import secrets
import subprocess
import time
from itertools import pairwise
from pathlib import Path
from uuid import uuid4

if __package__:
    from .aggregate_test_scenarios import prepare_saved_window_fixture
    from .browser_profile_health import (
        AUTO_WORKER_JOB_CAPTURE,
        browser_timeline_boundary,
        check_fixture,
        monitored_command,
        resolve_fixture_container,
        wait_current_history,
    )
else:
    from aggregate_test_scenarios import prepare_saved_window_fixture
    from browser_profile_health import (
        AUTO_WORKER_JOB_CAPTURE,
        browser_timeline_boundary,
        check_fixture,
        monitored_command,
        resolve_fixture_container,
        wait_current_history,
    )

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = ("detectors.live.spec.ts", "aggregates.live.spec.ts", "findings.live.spec.ts", "proxy.session.spec.ts")

WORKER_DIAGNOSTIC_SCRIPT = AUTO_WORKER_JOB_CAPTURE + '''
import json, math, os, sys, time
from datetime import UTC, datetime
from pathlib import Path

def read(path, maximum=1048576):
    try:
        with Path(path).open('rb') as handle:
            data = handle.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError('bounded diagnostic input exceeded')
        result = json.loads(data)
        if not isinstance(result, dict):
            raise ValueError('diagnostic file must contain an object')
        return result, None
    except (OSError, ValueError):
        return None, 'unavailable'

def fields(value, names):
    return {key: value.get(key) for key in names}

def timings(value):
    if not isinstance(value, dict):
        return None
    return {key: item for key, item in list(value.items())[:32]
        if isinstance(item, (int, float)) and not isinstance(item, bool)}

def budget_errors(rows, target):
    result = []
    for row in rows[-10:]:
        if not isinstance(row, dict) or row.get('aggregate_id') != target:
            continue
        factor = row.get('factor')
        factor = factor if isinstance(factor, str) and len(factor) <= 64 and factor.isascii() and factor.isidentifier() else None
        stack = []
        frames = row.get('stack')
        for frame in frames[:8] if isinstance(frames, list) else []:
            if not isinstance(frame, dict):
                continue
            module, function, line = frame.get('module'), frame.get('function'), frame.get('line')
            if (not isinstance(module, str) or len(module) > 160 or not module.startswith('app.')
                    or not all(part.isascii() and part.isidentifier() for part in module.split('.'))
                    or not isinstance(function, str) or len(function) > 128
                    or not (function.isascii() and function.isidentifier() or function in
                        {'<lambda>', '<genexpr>', '<listcomp>', '<dictcomp>', '<setcomp>'})
                    or type(line) is not int or line <= 0):
                continue
            stack.append({'module': module, 'function': function, 'line': line})
        total, sequence = row.get('turn_total_seconds'), row.get('sequence')
        total = total if isinstance(total, (int, float)) and not isinstance(total, bool) and math.isfinite(total) and total >= 0 else None
        result.append({'aggregate_id': target, 'factor': factor, 'stack': stack,
            'sequence': sequence if type(sequence) is int and sequence > 0 else None, 'turn_total_seconds': total})
    return result

mode, target = sys.argv[1:3]
result = {'mode': mode, 'aggregate_id': target, 'observed_at': datetime.now(UTC).isoformat()}
if mode == 'aggregate':
    metrics, error = read('/tmp/aggregate-performance.json')
    heartbeat, heartbeat_error = read('/tmp/aggregate-worker.json')
    result.update(metrics_error=error, heartbeat_error=heartbeat_error,
        admission=heartbeat.get('admission') if heartbeat else None,
        resources=fields(metrics, ['max_active', 'max_queued', 'max_reserved_bytes', 'peak_memory_bytes',
            'memory_limit_bytes', 'memory_cgroup_version', 'memory_peak_source', 'memory_metric_error',
            'peak_rss_bytes', 'cpu_seconds']) if metrics else None,
        online_last10=None, history_first=None, steps_last10=None, maintenance_last10=None,
        maintenance_budget_errors=None, worker_budget_error_sequence=None, worker_budget_errors_dropped=None)
    if metrics:
        first = metrics.get('history_first', {}).get(target)
        if first:
            result['history_first'] = {**fields(first, ['aggregate_id', 'point_id', 'origin', 'started_at',
                'completed_at', 'queue_seconds', 'total_seconds']), 'timings': timings(first.get('timings'))}
        result['online_last10'] = []
        for step in [row for row in metrics.get('closed', {}).values() if row.get('aggregate_id') == target][-10:]:
            result['online_last10'].append({**fields(step, ['aggregate_id', 'point_id', 'origin', 'started_at',
                'completed_at', 'queue_seconds', 'total_seconds', 'point_commit_start_monotonic',
                'point_commit_ack_monotonic', 'commit_observed_monotonic']), 'timings': timings(step.get('timings'))})
        result['maintenance_last10'] = [fields(row, ['aggregate_id', 'closure_id', 'phase', 'rows_changed',
            'started_monotonic', 'sql_completed_monotonic', 'committed_monotonic'])
            for row in metrics.get('maintenance', {}).get('spans', []) if row.get('aggregate_id') == target][-10:]
        progress = metrics.get('maintenance', {})
        if isinstance(progress.get('budget_errors'), list):
            result['maintenance_budget_errors'] = budget_errors(progress['budget_errors'], target)
        for field, key in [('worker_budget_error_sequence', 'budget_error_sequence'),
                ('worker_budget_errors_dropped', 'dropped_budget_errors')]:
            value = progress.get(key)
            result[field] = value if type(value) is int and value >= 0 else None
    if heartbeat:
        result['steps_last10'] = [{**fields(row, ['aggregate_id', 'kind', 'state', 'point_id', 'origin', 'queue_seconds',
            'total_seconds', 'started_at', 'completed_at']), 'timings': timings(row.get('timings'))}
            for row in heartbeat.get('steps', [])
            if row.get('aggregate_id') == target][-10:]
    try:
        result['job_capture'] = auto_worker_job_capture(metrics, heartbeat, target,
            sys.argv[3] if len(sys.argv) > 3 else None)
    except Exception:
        result['job_capture'] = None
    clock = os.environ.get('AGGREGATE_TEST_CLOCK')
    result['clock'] = {'file': clock, 'target_at': None, 'error': 'not_configured', 'stat_last10': None}
    if clock:
        body, clock_error = read(clock, 65536)
        result['clock'].update(target_at=body.get('aggregates', {}).get(target) if body else None,
            error=clock_error, stat_last10=[])
        for _ in range(10):
            try:
                started = time.perf_counter()
                status = Path(clock).stat()
                result['clock']['stat_last10'].append({'seconds': time.perf_counter() - started,
                    'mtime_ns': status.st_mtime_ns, 'bytes': status.st_size})
            except OSError:
                result['clock']['error'] = 'stat_unavailable'
                break
else:
    identities = set(json.loads(sys.argv[3]))
    heartbeat, error = read('/tmp/detector-worker.json')
    result.update(heartbeat_error=error, detector_ids=sorted(identities), steps_last10=None,
        phase=heartbeat.get('phase') if heartbeat else None)
    if heartbeat and identities:
        result['steps_last10'] = []
        for row in [item for item in heartbeat.get('steps', []) if item.get('detector_id') in identities][-10:]:
            transition = row.get('transition')
            result['steps_last10'].append({**fields(row, ['detector_id', 'started_at', 'execution_state',
                'consumed_index', 'evaluated_index', 'backlog', 'seconds', 'state_seconds', 'result']),
                'transition': timings(transition), 'runtime': timings(transition.get('runtime'))
                    if isinstance(transition, dict) else None})
print(json.dumps(result))
'''

POSITION_DIAGNOSTIC_SCRIPT = '''
import json, sys
from uuid import UUID
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from app.adapters.wal_bootstrap import read_setting
from app.db import Database
from app.models import (Aggregate, AggregateClosure, AggregateSchedule, AggregateHistorySpan,
    AggregateRetentionState, AggregateSnapshot, Detector, HistoricalAggregatePoint, WorkingWindowPoint)

url = read_setting('DATABASE_URL')
assert make_url(url).database == 'diploma_test'
database = Database(url)
try:
    identity = UUID(sys.argv[1])
    expected_old = UUID(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
    with database.session_factory() as session:
        session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        session.execute(text("SET LOCAL statement_timeout='2000ms'"))
        session.execute(text("SET LOCAL lock_timeout='1000ms'"))
        schedule = session.execute(select(Aggregate.id.label('aggregate_id'),
            AggregateSchedule.id.label('schedule_id'), Aggregate.last_error, AggregateSchedule.next_index,
            AggregateSchedule.online_start_index, AggregateSchedule.turn, AggregateSchedule.paused,
            AggregateSchedule.active_cleanup_id).join(
                AggregateSchedule, Aggregate.schedule_id == AggregateSchedule.id).where(Aggregate.id == identity)).mappings().first()
        retention = session.execute(select(*(getattr(AggregateRetentionState, key) for key in (
            'status', 'reason', 'last_checked_at', 'next_check_at', 'retry_count', 'pending_closure_id',
            'pending_phase', 'keep_from_index', 'protected_from_index', 'reserve_windows',
            'auto_cleared_total', 'history_revision'))).where(
                AggregateRetentionState.aggregate_id == identity)).mappings().first()
        old = session.execute(select(*(getattr(AggregateClosure, key) for key in ('origin_point_id',
            'schedule_id', 'window_index', 'origin', 'time_basis', 'point_kind', 'payload_state',
            'cleared_at', 'clear_reason', 'clear_policy_version'))).where(
                AggregateClosure.aggregate_id == identity, AggregateClosure.window_index == 0)).mappings().first()
        point_id = old['origin_point_id'] if old else expected_old
        old_facts = {'expected_point_id': expected_old, 'closure': dict(old) if old else None,
            'identity_matches': old['origin_point_id'] == expected_old if old and expected_old else None,
            'working_payloads': None, 'historical_payloads': None, 'snapshots': None}
        if point_id is not None:
            old_facts.update(working_payloads=session.scalar(select(func.count()).select_from(
                WorkingWindowPoint).where(WorkingWindowPoint.id == point_id)),
                historical_payloads=session.scalar(select(func.count()).select_from(
                    HistoricalAggregatePoint).where(HistoricalAggregatePoint.id == point_id)),
                snapshots=session.scalar(select(func.count()).select_from(
                    AggregateSnapshot).where(AggregateSnapshot.closure_id == point_id)))
        remaining = session.scalar(select(func.coalesce(func.sum(func.greatest(
            AggregateHistorySpan.last_index - AggregateHistorySpan.next_index + 1, 0)), 0)).where(
                AggregateHistorySpan.aggregate_id == identity))
        models = session.execute(select(*(getattr(Detector, key) for key in ('id', 'execution_state', 'state',
            'consumed_index', 'evaluated_index', 'failure_code'))).where(Detector.aggregate_id == identity)
            .order_by(Detector.id).limit(11)).mappings().all()
        print(json.dumps({'observed_at': session.scalar(select(func.clock_timestamp())),
            'aggregate_id': str(identity), 'schedule': dict(schedule) if schedule else None,
            'retention': dict(retention) if retention else None, 'old0': old_facts, 'history_remaining': remaining,
            'detectors': [dict(row) for row in models[:10]], 'detectors_truncated': len(models) > 10}, default=str))
finally:
    database.dispose()
'''


def capture_worker_diagnostics(compose, env, command, config):
    """Retain bounded existing telemetry before the profile removes its workers."""
    auto = config.get("auto_cleanup")
    target = auto.get("aggregate") if isinstance(auto, dict) else None
    if not isinstance(target, str) or not target:
        print('Browser aggregate worker diagnostics: {"unavailable":"auto_target_missing"}', flush=True)
        return

    def emit(label, value):
        try:
            print(label + json.dumps(value), flush=True)
        except Exception:  # noqa: BLE001, S110 - optional output must preserve the browser result
            pass  # Diagnostic serialization/output cannot replace the browser result.

    def execute(service, script, *args, timeout=5):
        try:
            output = command([*compose, "exec", "-T", service, "python", "-B", "-c", script, *args],
                env=env, capture=True, timeout=timeout)
            result = json.loads(output.strip().splitlines()[-1])
            if not isinstance(result, dict):
                raise TypeError("diagnostic result must be an object")
            return result
        except Exception as error:  # noqa: BLE001 - diagnostic failures remain metadata
            return {"unavailable": type(error).__name__}

    aggregate = execute("aggregate-worker", WORKER_DIAGNOSTIC_SCRIPT, "aggregate", target,
        str(auto.get("run_id", "")))
    emit("Browser aggregate worker diagnostics: ", aggregate)
    positions = execute("api", POSITION_DIAGNOSTIC_SCRIPT, target,
        str(config["auto_cleanup"].get("old_point_id", "")), timeout=10)
    emit("Browser aggregate positions: ", positions)
    identities = [str(row["id"]) for row in positions.get("detectors", [])
        if isinstance(row, dict) and row.get("id")]
    detector = execute("detector-worker", WORKER_DIAGNOSTIC_SCRIPT, "detector", target, json.dumps(identities))
    emit("Browser detector worker diagnostics: ", detector)


def browser_specs(production, requested=None):
    directory = ROOT / "frontend/browser"
    development = ["auth.spec.ts", "events.spec.ts", "resources.spec.ts", "normalizers.spec.ts",
                   "aggregates.live.spec.ts", "aggregates.ui.spec.ts", "detectors.ui.spec.ts",
                   "detectors.live.spec.ts", "findings.ui.spec.ts", "findings.live.spec.ts"]
    development += sorted(p.name for p in directory.glob("resources.*.spec.ts"))
    development += sorted(p.name for p in directory.glob("normalizers.*.spec.ts"))
    allowed = list(PRODUCTION) if production else development
    for name in allowed:
        if not (directory / name).is_file():
            raise ValueError(f"Missing mandatory browser spec: {name}")
    selected = requested if requested is not None else allowed
    if not selected or len(set(selected)) != len(selected) or any(name not in allowed for name in selected):
        raise ValueError("Browser specs must belong to the selected smoke mode without duplicates")
    return selected


def validate_browser_arguments(args):
    if args.browser_spec is not None and not args.browser_only:
        raise ValueError("--browser-spec requires --browser-only")
    if not args.browser_only:
        return None
    if args.profile not in {"smoke-development", "smoke-production"}:
        raise ValueError("--browser-only requires a smoke profile")
    if args.detector_only or args.aggregate_only or args.rcf or args.wal or args.pytest is not None:
        raise ValueError("--browser-only cannot combine with detector/aggregate/rcf/wal/pytest selection")
    return browser_specs(args.profile == "smoke-production", args.browser_spec)


def require_fixture(path, fields):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or any(not data.get(field) for field in fields):
        raise RuntimeError(f"Required browser fixture fields missing: {fields}")
    return data


def prepare_findings(compose, env, command, selected, username, password):
    if "findings.live.spec.ts" not in selected:
        return None
    fixture = (ROOT / "backend/tests/findings_fixture.py").read_text(encoding="utf-8")
    result = json.loads(command([
        *compose, "exec", "-T", "-e", f"BROWSER_USERNAME={username}",
        "-e", f"BROWSER_PASSWORD={password}", "api", "python", "-B", "-c", fixture,
    ], env=env, capture=True, timeout=30))
    if not isinstance(result, dict) or any(not result.get(key) for key in (
        "guest", "source", "source_name", "detectors", "detector_name", "findings",
        "start", "stop", "from", "to",
    )):
        raise RuntimeError("Required findings browser fixture is unavailable")
    return result


def ready_history(items, *, not_before=None):
    working = sorted((p for p in items if p["kind"] == "working"),
                     key=lambda p:p["window_index"], reverse=True)[:16]
    if len(working) != 16 or any(
        p["completeness"] != "complete" or p["time_basis"] != "event_time"
        or p["payload_state"] != "present" for p in working
    ):
        return False
    if any(left["window_index"] != right["window_index"] + 1
           for left, right in pairwise(working)):
        return False
    if not_before is not None:
        from datetime import datetime
        return (working[0]["origin"] == "online" and
                datetime.fromisoformat(working[0]["end_utc"]) >= not_before)
    return True


def verify_hmr_restore(css, original):
    current = css.read_bytes()
    if current == original:
        return
    own_change = original.replace(b"--color-page: #eef5f2;", b"--color-page: #e4f7e4;", 1)
    if own_change != original and current == own_change:
        css.write_bytes(original)
    # Preserve any unrelated bytes, including concurrent user changes.
    raise RuntimeError("HMR did not restore the original stylesheet bytes; unrelated changes preserved")


def remaining_browser_budget(deadline, maximum=None):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("Browser preparation/test budget exhausted")
    return min(remaining, maximum) if maximum is not None else remaining


def bounded_browser_command(command, deadline):
    def execute(args, **kwargs):
        kwargs["timeout"] = remaining_browser_budget(deadline, kwargs.get("timeout"))
        result = command(args, **kwargs)
        remaining_browser_budget(deadline)
        return result
    return execute


def browser_timeout_milliseconds(deadline):
    milliseconds = int(remaining_browser_budget(deadline) * 1000)
    if milliseconds <= 0:
        raise RuntimeError("Browser preparation/test budget exhausted")
    return milliseconds


def auto_history_facts(compose, env, command, auto, *, verify=False, readiness_only=False, timeout=15):
    if readiness_only:
        script = (ROOT / "backend/tests/ml/browser_canonical_count.py").read_text(encoding="utf-8")
        args = [*compose, "exec", "-T", "api", "python", "-B", "-c", script,
            auto["aggregate"], str(timeout)]
    else:
        script = (ROOT / "backend/tests/ml/event_time_runtime.py").read_text(encoding="utf-8")
        args = [*compose, "exec", "-T", "-e", "EVENT_TIME_PAYLOAD=" + json.dumps({**auto, "verify": verify}),
            "api", "python", "-c", script, "browser_auto_facts"]
    output = command(args, env=env, capture=True, timeout=timeout)
    return json.loads(output.strip().splitlines()[-1])


def required_auto_history(config):
    auto = config.get("auto_cleanup")
    if not isinstance(auto, dict) or auto.get("seed_windows", 0) <= 512:
        raise RuntimeError("Required real auto history above the unchanged reserve is unavailable")
    if auto["seed_windows"] != 520:
        raise RuntimeError("Required browser auto history must contain exactly 520 canonical windows")
    return auto


class CanonicalObservation:
    """One host clock for cadence and bounded whole-phase measurements."""

    def __init__(self, started, deadline):
        self.started, self.deadline = started, deadline
        self.last_started = {}
        self.calls = {}
        self.samples = []
        self.up_wait_seconds = None
        self.marker_errors = 0
        self.failed_operation = None

    def due(self, kind, period):
        return time.monotonic() >= self.last_started.get(kind, -math.inf) + period

    def call(self, kind, operation):
        started = time.monotonic()
        previous = self.last_started.get(kind)
        self.last_started[kind] = started
        result = None
        try:
            result = operation()
            return result
        finally:
            if result is None:
                self.failed_operation = kind
            try:
                self.record(kind, started, previous, result)
            except Exception:  # noqa: BLE001, S110 - observation must preserve the RPC result
                pass

    def record(self, kind, started, previous, result):
        ended = time.monotonic()
        stats = self.calls.setdefault(kind, {"attempted": 0, "failed": 0, "elapsed_seconds": 0.0,
            "min_interval_seconds": None, "max_interval_seconds": None})
        stats["attempted"] += 1
        stats["failed"] += result is None
        stats["elapsed_seconds"] += ended - started
        if previous is not None:
            interval = started - previous
            for key, compare in (("min_interval_seconds", min), ("max_interval_seconds", max)):
                stats[key] = interval if stats[key] is None else compare(stats[key], interval)
        if kind != "count" or not isinstance(result, dict) or ended >= self.deadline:
            return
        count = result.get("canonical_history_count")
        computed, uncomputed = result.get("computed_count"), result.get("uncomputed_count")
        if type(count) is not int or not 0 <= count <= 520:
            return
        if type(computed) is not int or type(uncomputed) is not int or min(computed, uncomputed) < 0 or computed + uncomputed != count:
            computed = uncomputed = None
        for key in ("connect_seconds", "sql_seconds"):
            duration = result.get(key)
            if type(duration) in (int, float) and math.isfinite(duration) and 0 <= duration <= 15:
                stats[key] = stats.get(key, 0.0) + duration
                stats[key + "_samples"] = stats.get(key + "_samples", 0) + 1
                stats.setdefault("first_" + key, duration)
        if "connect_seconds" in result and "sql_seconds" in result:
            stats.setdefault("first_light_rpc_seconds", ended - started)
        else:
            stats["before_proof_rpc_seconds"] = stats.get("before_proof_rpc_seconds", 0.0) + ended - started
        if len(self.samples) < 240:
            self.samples.append([round(started - self.started, 6), round(ended - self.started, 6),
                count, computed, uncomputed])

    def check_health(self, operation):
        if self.due("health", 2):
            self.call("health", operation)

    def finish(self, outcome, aggregate_id):
        try:
            ended = time.monotonic()
            print("Browser canonical observation: " + json.dumps({"aggregate_id": aggregate_id,
                "clock_domain": "browser_runner_monotonic", "outcome": outcome,
                "elapsed_seconds": ended - self.started, "remaining_seconds": max(0, self.deadline - ended),
                "up_wait_seconds": self.up_wait_seconds, "marker_errors": self.marker_errors,
                "failed_operation": self.failed_operation,
                "calls": self.calls, "sample_columns": ["rpc_started_seconds", "rpc_returned_seconds",
                    "canonical_history_count", "computed", "uncomputed"], "count_samples": self.samples},
                allow_nan=False), flush=True)
        except Exception:  # noqa: BLE001, S110 - optional summary cannot replace a failure
            pass


def try_capture_auto_history(compose, env, command, config, deadline, *, observation):
    """One bounded RPC: preserve the first real proof or read only its count."""
    auto = required_auto_history(config)
    captured = "old_point" in auto
    phase_command = bounded_browser_command(command, deadline)
    timeout = remaining_browser_budget(deadline, 15)
    facts = observation.call("count", lambda: auto_history_facts(compose, env, phase_command, auto,
        readiness_only=captured, timeout=timeout))
    remaining_browser_budget(deadline)
    if not captured and facts.get("ready"):
        assert facts["old_payloads"] == facts["old_snapshots"] == 1
        config["auto_cleanup"] = {**auto, **facts}
        print("Auto history before deletion: " + json.dumps({**{key: value for key, value in facts.items()
            if key != "old_point"}, "elapsed_seconds": time.monotonic() - observation.started,
            "remaining_seconds": remaining_browser_budget(deadline)}), flush=True)
        captured = True
    count = facts.get("canonical_history_count")
    if type(count) is not int or not 0 <= count <= auto["seed_windows"]:
        raise RuntimeError("Auto canonical history count is unavailable or invalid")
    if captured:
        config["auto_cleanup"]["canonical_history_count"] = count
    return captured and count == auto["seed_windows"]


def capture_auto_history(compose, env, command, config, deadline, *, fixture_container, observation=None, started=None):
    """Keep an existing early proof and require all canonical headers in time."""
    auto = required_auto_history(config)
    started = time.monotonic() if started is None else started
    if observation is None:
        observation = CanonicalObservation(started, deadline)
    phase_command = bounded_browser_command(command, deadline)
    while True:
        remaining_browser_budget(deadline)
        observation.check_health(lambda: check_fixture(fixture_container, env, phase_command))
        if observation.due("count", 1) and try_capture_auto_history(compose, env, command, config,
                deadline, observation=observation):
            elapsed = time.monotonic() - started
            remaining = remaining_browser_budget(deadline)
            print("Auto canonical history materialized: " + json.dumps({"canonical_history_count": auto["seed_windows"],
                "expected": auto["seed_windows"], "includes_cleared": True,
                "elapsed_seconds": elapsed, "remaining_seconds": remaining}), flush=True)
            browser_timeline_boundary(config, "canonical_complete", remaining, elapsed=elapsed, budget_scope="canonical")
            return
        time.sleep(min(.1, remaining_browser_budget(deadline)))


def run_browsers(compose, env, project, run, production, selected, command, oidc_setup):
    deadline = time.monotonic() + 1200
    diagnostic_command = command
    command = bounded_browser_command(command, deadline)
    from aggregate_test_scenarios import aggregate_runtime_smoke
    print(f"Browser mode={'production' if production else 'development'}; specs={len(selected)}: {', '.join(selected)}", flush=True)
    npm = "npm.cmd" if os.name == "nt" else "npm"
    username, guest, password = "task21admin", "detector-browser-guest", secrets.token_urlsafe(24)
    config = {"username":username,"guest":guest,"password":password,
        "runner_timeline": {"run_id": run.name, "clock_domain": "browser_runner_monotonic",
            "budget_seconds": 1200, "boundaries": [], "auto_entry_remaining_seconds": None,
            "auto_entry_unknown_reason": "no_shared_monotonic_origin_or_entry_transport"}}
    aggregate_config = None
    if any(name.startswith("aggregates.") for name in selected):
        aggregate_config = aggregate_runtime_smoke(compose, env, production, command, prepare_only=True,
            saved_window="aggregates.live.spec.ts" in selected)
        if not isinstance(aggregate_config, dict) or any(not aggregate_config.get(k) for k in ("username", "password", "sources", "anchor")):
            raise RuntimeError("Required aggregate browser fixture is unavailable")
    need_detectors = any(name.startswith("detectors.") for name in selected)
    if not need_detectors:
        create = "import os,app.cli as cli; cli.getpass.getpass=lambda prompt: os.environ['BROWSER_PASSWORD']; raise SystemExit(cli.create_admin('task21admin','Browser admin'))"
        command([*compose,"exec","-T","-e",f"BROWSER_PASSWORD={password}","api","python","-c",create],env=env)
    if need_detectors:
        command([*compose, "stop", "aggregate-worker"], env=env)
        command([*compose, "cp", str(ROOT / "scripts/browser_detector_fixture.py"), "detector-worker:/tmp/browser_detector_fixture.py"], env=env)
        command([*compose, "exec", "-d", "-e", f"BROWSER_USERNAME={username}", "-e", f"BROWSER_GUEST={guest}", "-e", f"BROWSER_PASSWORD={password}",
            "-e", "BROWSER_AUTO_HISTORY=" + ("1" if "detectors.live.spec.ts" in selected else "0"),
            "detector-worker", "sh", "-c", "python /tmp/browser_detector_fixture.py >/tmp/browser-detector-fixture.log 2>&1"], env=env)
        preparation_started = time.monotonic()
        fixture_deadline = min(deadline, preparation_started + 60)
        fixture_command = bounded_browser_command(command, fixture_deadline)
        startup_deadline = min(fixture_deadline, preparation_started + 5)
        fixture_container = resolve_fixture_container(compose, env, fixture_command)
        while True:
            result = subprocess.run([*compose, "exec", "-T", "detector-worker", "cat", "/tmp/detector-browser-fixture.json"], env=env, text=True, capture_output=True, check=False, timeout=remaining_browser_budget(fixture_deadline, 5))
            remaining_browser_budget(fixture_deadline)
            if result.returncode == 0:
                config.update(json.loads(result.stdout))
            capture_ready = check_fixture(fixture_container, env, fixture_command,
                allow_starting=time.monotonic() < startup_deadline, allow_capture_starting=True)
            fixture_remaining = remaining_browser_budget(fixture_deadline)
            if result.returncode == 0 and capture_ready:
                browser_timeline_boundary(config, "fixture_ready", fixture_remaining,
                    elapsed=fixture_deadline - fixture_remaining - preparation_started, budget_scope="fixture")
                break
            if time.monotonic() >= fixture_deadline:
                raise RuntimeError("Detector browser history preparation exceeded its profile budget")
            time.sleep(.5)
        if "detectors.live.spec.ts" in selected:
            from aggregate_performance import write_clock
            clock_file = Path(env["AGGREGATE_CLOCK_FILE"])
            auto = config["auto_cleanup"]
            auto["run_id"] = run.name
            auto["clock_file"] = str(clock_file)
        worker_up = [*compose, "up", "-d", "--no-deps", "--wait", "aggregate-worker"]
        if "detectors.live.spec.ts" in selected:
            canonical_started = time.monotonic()
            canonical_deadline = min(deadline, canonical_started + 300)
            observation = CanonicalObservation(canonical_started, canonical_deadline)
            scope = {"run_id": run.name, "aggregate_id": auto["aggregate"],
                "nonce": str(uuid4()), "phase": "canonical"}
            moments = {auto["aggregate"]: auto["clock_at"]}
            browser_timeline_boundary(config, "canonical_start", deadline - canonical_started,
                elapsed=canonical_started - (deadline - 1200))
            print("Auto canonical history starting: " + json.dumps({"expected": 520,
                "elapsed_seconds": 0, "remaining_seconds": remaining_browser_budget(canonical_deadline)}), flush=True)
            try:
                phase_command = bounded_browser_command(command, canonical_deadline)
                def check_and_capture():
                    observation.check_health(lambda: check_fixture(fixture_container, env, phase_command))
                    if "old_point" not in required_auto_history(config) and observation.due("count", 1):
                        try_capture_auto_history(compose, env, command, config,
                            canonical_deadline, observation=observation)
                outcome = "failed"
                try:
                    write_clock(clock_file, moments, canonical_phase={**scope, "state": "begin", "outcome": None})
                    up_started = time.monotonic()
                    try:
                        monitored_command(worker_up, env=env, cwd=ROOT,
                            timeout=remaining_browser_budget(canonical_deadline), deadline=canonical_deadline,
                            check=check_and_capture)
                    finally:
                        observation.up_wait_seconds = time.monotonic() - up_started
                    capture_auto_history(compose, env, command, config, canonical_deadline,
                            fixture_container=fixture_container, started=canonical_started, observation=observation)
                    outcome = "complete"
                finally:
                    try:
                        write_clock(clock_file, moments, canonical_phase={**scope, "state": "end", "outcome": outcome})
                    except Exception:  # noqa: BLE001 - an unavailable end marker must not replace the gate result
                        observation.marker_errors += 1
                    observation.finish(outcome, auto["aggregate"])
            except Exception as error:
                auto_status = config.get("auto_cleanup", {})
                print("Auto history preparation failed: " + json.dumps({"status": "failed",
                    "canonical_history_count": auto_status.get("canonical_history_count"),
                    "expected": 520, "before_point_id": auto_status.get("old_point_id"),
                    "before_proof_captured": "old_point" in auto_status,
                    "elapsed_seconds": time.monotonic() - canonical_started,
                    "remaining_seconds": max(0, canonical_deadline - time.monotonic()),
                    "reason": type(error).__name__}), flush=True)
                capture_worker_diagnostics(compose, env, diagnostic_command, config)
                raise
        else:
            command(worker_up, env=env)
    findings_config = prepare_findings(compose, env, command, selected, username, password)
    if findings_config:
        config["findings"] = findings_config
    fixture = run / "detector-browser.json"
    fixture.write_text(json.dumps(config), encoding="utf-8")
    if need_detectors:
        require_fixture(fixture, ["username", "guest", "password", "aggregate", "source", "anchor"])
    if aggregate_config:
        aggregate_file = run / "aggregate-browser.json"
        aggregate_file.write_text(json.dumps({**aggregate_config,"result":str(run / "aggregate-browser-owned.json")}), encoding="utf-8")
    browser_env = {**env, "DETECTOR_BROWSER_CONFIG":str(fixture), "BROWSER_FIXTURES_REQUIRED":"1", "PLAYWRIGHT_BASE_URL":env["PUBLIC_ORIGIN"], "PLAYWRIGHT_USERNAME":username, "PLAYWRIGHT_PASSWORD":password, "TASK21_ADMIN_PASSWORD":password, "TASK21_SECURE_COOKIE":"true" if production else "false"}
    if aggregate_config:
        browser_env["AGGREGATE_BROWSER_CONFIG"] = str(aggregate_file)
    css = ROOT / "frontend/src/app/styles.css"
    original_css = css.read_bytes()
    check = (lambda: check_fixture(fixture_container, env, command)) if need_detectors else None
    ready = (lambda timeout=60: wait_current_history(env, config, check, ready_history,
        timeout=timeout, deadline=deadline)) if need_detectors else None
    try:
        if not production:
            for candidate in (r"C:\Program Files\Google\Chrome\Application\chrome.exe", r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
                if Path(candidate).is_file():
                    browser_env["PLAYWRIGHT_CHROME_PATH"] = candidate
                    break
            if "auth.spec.ts" in selected:
                remaining_browser_budget(deadline)
                oidc_setup(compose, env, False, keep_active=True)
                remaining_browser_budget(deadline)
                browser_env["PLAYWRIGHT_OIDC"] = "1"
            if ready:
                ready()
            if aggregate_config and "saved_window_sources" in aggregate_config:
                prepare_saved_window_fixture(compose, env, command, aggregate_file)
            execute = monitored_command if check else command
            global_timeout_ms = browser_timeout_milliseconds(deadline)
            launch_remaining = remaining_browser_budget(deadline)
            browser_timeline_boundary(config, "Playwright_launch", launch_remaining,
                elapsed=1200 - launch_remaining)
            execute([npm,"run","test:browser","--",*selected,"--workers=1","--output",str(run / "browser-results"),
                "--global-timeout", str(global_timeout_ms)], env=browser_env, cwd=ROOT / "frontend", timeout=launch_remaining, **({"check":check,"deadline":deadline} if check else {}))
        else:
            production_browsers(compose, browser_env, project, run, selected, command,
                check=check, ready=ready, deadline=deadline)
        if "detectors.live.spec.ts" in selected:
            facts = auto_history_facts(compose, env, command, config["auto_cleanup"], verify=True)
            print("Auto history physical payload after browser: " + json.dumps(facts), flush=True)
    finally:
        if "detectors.live.spec.ts" in selected:
            capture_worker_diagnostics(compose, env, diagnostic_command, config)
        if need_detectors:
            try:
                diagnostic_command([*compose, "cp", str(ROOT / "scripts/browser_history_diagnostics.py"),
                         "detector-worker:/tmp/browser_history_diagnostics.py"], env=env, timeout=15)
                facts = diagnostic_command([*compose, "exec", "-T", "detector-worker", "python",
                                 "/tmp/browser_history_diagnostics.py"], env=env, capture=True, timeout=15)
                print("Detector browser read-only diagnostics: " + facts,flush=True)
            except RuntimeError:
                print("Detector browser diagnostic snapshot unavailable",flush=True)
        # auth.spec's own finally restores its exact input; detect any failure.
        verify_hmr_restore(css, original_css)


def copy_browser_modules(source, destination, selected):
    """Copy selected specs and their local TS imports, confined to browser/."""
    pending = [source / name for name in selected]
    copied = set()
    root = source.resolve()
    while pending:
        path = pending.pop().resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise RuntimeError("Missing or unsafe browser module")
        if path in copied:
            continue
        copied.add(path)
        text = path.read_text(encoding="utf-8")
        target = destination / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())
        for module in re.findall(r'''(?:from\s*|import\s*\(|import\s*)["'](\.[^"']+)["']''', text):
            relative = path.parent / module
            candidates = [relative, Path(str(relative) + ".ts"), relative.with_suffix(".ts"), relative / "index.ts"]
            dependency = next((p for p in candidates if p.is_file()), None)
            if dependency is None:
                raise RuntimeError("Missing browser import")
            pending.append(dependency)


def production_browsers(compose, env, project, run, selected, command, *, check=None, ready=None, deadline=None):
    deadline = time.monotonic() + 1200 if deadline is None else deadline
    command = bounded_browser_command(command, deadline)
    context = run / "detector-browser-context"
    context.mkdir()
    source = ROOT / "frontend"
    for name in ("package.json", "package-lock.json", "playwright.config.ts"):
        (context / name).write_bytes((source / name).read_bytes())
    (context / "browser").mkdir()
    copy_browser_modules(source / "browser", context / "browser", selected)
    config = json.loads(Path(env["DETECTOR_BROWSER_CONFIG"]).read_text(encoding="utf-8"))
    clock_mounts = []
    if config.get("auto_cleanup"):
        clock_file = Path(config["auto_cleanup"]["clock_file"]).resolve()
        if not clock_file.is_relative_to(run.resolve()):
            raise RuntimeError("Controlled browser clock must belong to this run")
        clock_mounts = ["-v", f"{clock_file.parent}:/clock"]
        config["auto_cleanup"]["clock_file"] = "/clock/clock.json"
    (context / "fixture.json").write_text(json.dumps(config), encoding="utf-8")
    aggregate_args = []
    aggregate_result = None
    aggregate_fixture = context / "aggregate-fixture.json"
    saved_window = False
    if any(name.startswith("aggregates.") for name in selected):
        aggregate = require_fixture(Path(env["AGGREGATE_BROWSER_CONFIG"]), ["username", "password", "sources", "anchor", "result"])
        aggregate_result = Path(aggregate["result"]).resolve()
        if not aggregate_result.is_relative_to(run.resolve()):
            raise RuntimeError("Aggregate result must belong to the current run")
        aggregate["result"] = "/results/aggregate-browser-owned.json"
        aggregate_fixture.write_text(json.dumps(aggregate), encoding="utf-8")
        saved_window = "saved_window_sources" in aggregate
        aggregate_args = ["-e", "AGGREGATE_BROWSER_CONFIG=/test/aggregate-fixture.json"]
    results = run / "browser-results"
    results.mkdir()
    proxy = command([*compose,"ps","-q","proxy"], env=env, capture=True).strip()
    if not proxy:
        raise RuntimeError("Production browser proxy is unavailable")
    container = project + "-detector-browser"
    public_port = env["HTTPS_BIND"].split(":")[1]
    # Keep the signed Noble release, updates and security repositories for both packages.
    apt = "apt-get -o Dir::Etc::sourcelist=/tmp/browser-ubuntu.sources -o Dir::Etc::sourceparts=-"
    prepare = (
        "set -e; "
        "sed 's/^Components:.*/Components: main/' /etc/apt/sources.list.d/ubuntu.sources "
        "> /tmp/browser-ubuntu.sources; "
        f"{apt} -o Acquire::Languages=none update >/dev/null; "
        f"{apt} install -y --no-install-recommends libnss3-tools socat >/dev/null; "
        "npm ci --ignore-scripts --no-audit --no-fund; "
        "mkdir -p /root/.pki/nssdb; "
        "certutil -d sql:/root/.pki/nssdb -N --empty-password; "
        "certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n proxy-test -i /tmp/proxy-ca.pem; "
        "cp /tmp/proxy-ca.pem /usr/local/share/ca-certificates/proxy-test.crt; "
        "update-ca-certificates >/dev/null"
    )
    script = f"socat TCP-LISTEN:{public_port},bind=127.0.0.1,reuseaddr,fork TCP:127.0.0.1:443 & npx playwright test " + " ".join("browser/"+name for name in selected) + " --workers=1 --output /results"
    try:
        command(["docker","run","-d","--rm","--name",container,"--user","0:0","--memory","2g","--cpus","2","--network",f"container:{proxy}","--label",f"com.docker.compose.project={project}","-w","/test","-v",f"{context.resolve()}:/test","-v",f"{results.resolve()}:/results",*clock_mounts,"-v",f"{Path(env['KAFKA_CA_FILE']).resolve()}:/tmp/proxy-ca.pem:ro","-e","NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem","-e","DETECTOR_BROWSER_CONFIG=/test/fixture.json",*aggregate_args,"-e","BROWSER_FIXTURES_REQUIRED=1","-e",f"PLAYWRIGHT_BASE_URL={env['PUBLIC_ORIGIN']}","-e","TASK21_SECURE_COOKIE=true","-e",f"TASK21_ADMIN_PASSWORD={env['TASK21_ADMIN_PASSWORD']}","mcr.microsoft.com/playwright:v1.63.0-noble","sleep","1800"], env=env, timeout=30)
        def execute(script, *, browser=False):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Production browser preparation/test budget exhausted")
            if browser:
                script += " --global-timeout " + str(browser_timeout_milliseconds(deadline))
            browser_timeline_boundary(config, "Playwright_launch" if browser else "dependencies_start", remaining)
            args = ["docker", "exec", container, "sh", "-c", script]
            if check:
                monitored_command(args, env=env, cwd=ROOT, timeout=remaining, check=check, deadline=deadline)
            else:
                command(args, env=env, timeout=remaining)
        execute(prepare)
        if ready:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Production browser preparation/test budget exhausted")
            browser_timeline_boundary(config, "dependencies_complete_readiness_start", remaining)
            ready(timeout=min(60, remaining))
        if saved_window:
            prepare_saved_window_fixture(compose, env, command, aggregate_fixture)
        execute(script, browser=True)
    finally:
        subprocess.run(["docker","rm","-f",container],env=env,check=False,capture_output=True)
        exported = results / "aggregate-browser-owned.json"
        if aggregate_result is not None and exported.is_file():
            aggregate_result.write_bytes(exported.read_bytes())
