"""Test-only clock composition for the existing workers in isolated profiles.

Only explicitly listed aggregate identities receive a logical UTC clock. All
budgets, SQL, threads, model execution and timing measurements use real clocks.
No production module imports this file and no HTTP test endpoint is installed.
"""

import json
import math
import os
import sys
import time
from collections import OrderedDict, deque
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from uuid import UUID, uuid4

WORKER_METRICS_READER_LIMIT = 1024 * 1024
PHASE_FIELDS = ("canonical_phase", "generation_phase")


class ScopedClock:
    def __init__(self, path):
        self.path = Path(path)
        self.version = None
        self.values = {}
        self.target = ContextVar("aggregate_test_clock_target", default=None)
        self.domain = ContextVar("aggregate_test_clock_domain", default="window")
        self.release = None
        self.phase_capture = None
        self.refresh_lock = Lock()

    @contextmanager
    def scope(self, domain):
        if domain not in {"window", "maintenance"}:
            raise ValueError("unknown controlled clock domain")
        token = self.domain.set(domain)
        target = self.target.set(self.target.get())
        try:
            yield
        finally:
            self.target.reset(target)
            self.domain.reset(token)

    def refresh(self):
        with self.refresh_lock:
            stat = self.path.stat()
            version = (stat.st_mtime_ns, stat.st_size)
            if stat.st_size > 65536:
                raise ValueError("controlled aggregate clock exceeds bounded JSON transport")
            if version != self.version:
                body = json.loads(self.path.read_text(encoding="utf-8"))
                if ("aggregates" not in body or set(body) - {
                        "aggregates", "first_clear_release", "canonical_phase", "generation_phase"}
                        or not isinstance(body["aggregates"], dict) or len(body["aggregates"]) > 30):
                    raise ValueError("controlled clock requires at most 30 explicit aggregate identities")
                values = {}
                for key, value in body["aggregates"].items():
                    parsed = datetime.fromisoformat(value)
                    if parsed.tzinfo is None:
                        raise ValueError("controlled clock requires an absolute UTC moment")
                    values[UUID(key)] = parsed.astimezone(UTC)
                if self.release is not None:
                    self.release.arm(body.get("first_clear_release"), values)
                if self.phase_capture is not None:
                    for name in ("canonical", "generation"):
                        self.phase_capture.safe(self.phase_capture.observe_marker, body.get(name + "_phase"), values,
                            expected_phase=name)
                self.values, self.version = values, version

    def now(self, identity=None):
        if self.domain.get() == "maintenance":
            return None
        self.refresh()
        identity = UUID(str(identity)) if identity is not None else self.target.get()
        if self.release is not None:
            value = self.release.now(identity)
            if value is not None:
                return value
        return self.values.get(identity)


class FirstClearRelease:
    """One prepared wave, released only by its own first-clear commit ACK."""

    def __init__(self, clock, run_id):
        self.clock, self.run_id = clock, run_id
        self.lock = Lock()
        self.plan = self.receipt = None
        self.overrides = {}
        self.used = None
        self.sequence = 0
        clock.release = self

    def arm(self, body, values):
        with self.lock:
            if body is None:
                if self.plan is not None:
                    self.sequence += 1
                self.plan = None
                self.overrides.clear()
                return
            assert set(body) == {"run_id", "case_id", "wave_index", "aggregate_id", "closure_id", "nonce", "groups",
                "valid_for_seconds"}
            assert body["run_id"] == self.run_id and body["case_id"] in {
                "shared-10-sparse", "distributed-20", "shared-30-dense", "distributed-30-dense"}
            assert body["wave_index"] == 521 and UUID(body["nonce"])
            assert type(body["valid_for_seconds"]) in {int, float} and 0 < body["valid_for_seconds"] <= 120
            assert UUID(body["aggregate_id"]) in values and UUID(body["closure_id"])
            groups = body["groups"]
            assert isinstance(groups, list) and len(groups) in {1, 3}
            identities = []
            for index, group in enumerate(groups):
                assert set(group) == {"after_seconds", "moments"}
                assert group["after_seconds"] == index * .15
                assert isinstance(group["moments"], dict) and group["moments"]
                for key, value in group["moments"].items():
                    identity, moment = UUID(key), datetime.fromisoformat(value)
                    assert identity in values and moment.tzinfo is not None
                    assert moment.astimezone(UTC) > values[identity]
                    identities.append(identity)
            assert len(identities) == len(set(identities)) == len(values)
            scope = {key: body[key] for key in ("run_id", "case_id", "wave_index", "aggregate_id", "closure_id", "nonce")}
            if self.plan is not None and self.plan["scope"] == scope:
                return
            assert self.plan is None, "An active release cannot be replaced"
            assert scope != self.used, "A consumed first-clear signal cannot be rearmed"
            self.plan = {"scope": scope, "groups": groups, "ack": None, "released": {},
                "deadline": time.monotonic() + body["valid_for_seconds"]}
            self.receipt = None
            self.overrides.clear()

    def claim(self, aggregate_id, closure_id):
        self.clock.refresh()
        with self.lock:
            if self.plan is None or self.plan["ack"] is not None or time.monotonic() >= self.plan["deadline"]:
                return None
            scope = self.plan["scope"]
            return dict(scope) if (str(aggregate_id), str(closure_id)) == (
                scope["aggregate_id"], scope["closure_id"]) else None

    def acknowledged(self, scope, acknowledged):
        self.clock.refresh()
        with self.lock:
            if (self.plan is None or self.plan["scope"] != scope or self.plan["ack"] is not None
                    or time.monotonic() >= self.plan["deadline"]):
                return
            self.plan["ack"] = acknowledged
            self.used = dict(scope)
            self.receipt = {**scope, "commit_ack_monotonic": acknowledged, "groups": [], "released": {}}
            self._release_group(0)

    def _release_group(self, index):
        group = self.plan["groups"][index]
        marker = time.monotonic()  # Before changing any target's test clock.
        assert marker >= self.plan["ack"] + group["after_seconds"]
        for identity, value in group["moments"].items():
            self.overrides[UUID(identity)] = datetime.fromisoformat(value).astimezone(UTC)
            self.plan["released"][identity] = marker
            self.receipt["released"][identity] = marker
        self.receipt["groups"].append({"after_seconds": group["after_seconds"],
            "released_monotonic": marker, "aggregate_ids": list(group["moments"])})
        self.sequence += 1

    def now(self, identity):
        with self.lock:
            if (self.plan is not None and self.plan["ack"] is not None
                    and time.monotonic() < self.plan["deadline"]):
                for index, group in enumerate(self.plan["groups"]):
                    if not set(group["moments"]) <= self.plan["released"].keys():
                        previous = self.receipt["groups"][-1]["released_monotonic"]
                        if time.monotonic() >= previous + .15:
                            self._release_group(index)
                        break
            return self.overrides.get(identity)

    def snapshot(self):
        self.clock.refresh()
        with self.lock:
            return {"sequence": self.sequence, "armed": self.plan is not None,
                "receipt": json.loads(json.dumps(self.receipt))}

    def close(self):
        with self.lock:
            self.plan = self.receipt = self.used = None
            self.overrides.clear()
        self.clock.release = None


def install_clock(clock):
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
    from app.services.implementations.aggregate_retention import AggregateRetentionService

    original_get = SqlAlchemyAggregateRepository.get
    original_now = SqlAlchemyAggregateRepository.now
    original_context = SqlAlchemyDetectorRepository.aggregate_context
    original_maintain = AggregateRetentionService.maintain

    def aggregate_get(repository, identity, *args, **kwargs):
        clock.target.set(UUID(str(identity)))
        return original_get(repository, identity, *args, **kwargs)

    def aggregate_now(repository):
        return clock.now() or original_now(repository)

    def detector_context(repository, identity, *args, **kwargs):
        clock.target.set(UUID(str(identity)))
        context = original_context(repository, identity, *args, **kwargs)
        context["now"] = clock.now(identity) or context["now"]
        return context

    def maintain(service, identity, *args, **kwargs):
        with clock.scope("maintenance"):
            return original_maintain(service, identity, *args, **kwargs)

    SqlAlchemyAggregateRepository.get = aggregate_get
    SqlAlchemyAggregateRepository.now = aggregate_now
    SqlAlchemyDetectorRepository.aggregate_context = detector_context
    AggregateRetentionService.maintain = maintain


class MaintenanceMetrics:
    """Confirmed physical progress and rare errors, without extra reads or clocks."""

    def __init__(self, phase=None):
        self.current = ContextVar("aggregate_test_maintenance_metrics", default=None)
        self.phase = phase
        self.completion = ContextVar("aggregate_test_maintenance_completion", default=None)
        self.lock = Lock()
        self.spans = deque(maxlen=256)
        self.large_spans = deque(maxlen=256)
        self.sequence = 0
        self.dropped = 0
        self.large_dropped = 0
        self.phases = {}
        self.heavy_membership_ack_rows = {}
        self.dropped_heavy_membership_acks = 0
        self.point_commits = OrderedDict()
        self.budget_errors = deque(maxlen=10)
        self.budget_error_sequence = 0
        self.dropped_budget_errors = 0

    def capture_budget_error(self, factor):
        pending = self.current.get()
        if pending is None or pending.get("first_budget_error") is not None:
            return
        event = {"aggregate_id": pending["aggregate_id"], "job_id": pending.get("job_id"),
            "factor": factor if isinstance(factor, str)
            and len(factor) <= 64 and factor.isascii() and factor.isidentifier() else None, "stack": []}
        pending["first_budget_error"] = event
        completion = self.completion.get()
        if completion is not None and completion.get("first_budget_error") is None:
            completion["first_budget_error"] = event
        frame = sys._getframe(1)
        try:
            for _ in range(64):
                if frame is None or len(event["stack"]) == 8:
                    break
                module = frame.f_globals.get("__name__", "")
                if isinstance(module, str) and module.startswith("app."):
                    event["stack"].append({"module": module[:160],
                        "function": frame.f_code.co_name[:128], "line": frame.f_lineno})
                frame = frame.f_back
        finally:
            del frame

    def record_budget_error(self, event, total_seconds=None):
        if event is None:
            return
        with self.lock:
            self.budget_error_sequence += 1
            self.dropped_budget_errors += len(self.budget_errors) == self.budget_errors.maxlen
            self.budget_errors.append({**event, "sequence": self.budget_error_sequence,
                "turn_total_seconds": total_seconds})
            if self.phase is not None:
                self.phase.safe(self.phase.physical, self.budget_errors[-1], error=True)

    def record_point_commit(self, identity, started, acknowledged):
        with self.lock:
            self.point_commits[str(identity)] = {"started": started, "acknowledged": acknowledged}
            while len(self.point_commits) > 256:
                self.point_commits.popitem(last=False)

    def point_commit(self, identity):
        with self.lock:
            return self.point_commits.get(identity)

    def publish(self, pending):
        committed = time.monotonic()
        with self.lock:
            for phase in pending["phases"]:
                if phase["rows_changed"] <= 0:
                    continue
                self.sequence += 1
                self.dropped += len(self.spans) == self.spans.maxlen
                span = {**phase, "aggregate_id": pending["aggregate_id"], "job_id": pending.get("job_id"),
                    "sequence": self.sequence, "committed_monotonic": committed}
                self.spans.append(span)
                if self.phase is not None:
                    self.phase.safe(self.phase.physical, span)
                if span["phase"] == "memberships":
                    identity = span["closure_id"]
                    if identity in self.heavy_membership_ack_rows:
                        self.heavy_membership_ack_rows[identity] += span["rows_changed"]
                    # Only a first full portion identifies the fixed 10001-row
                    # load target. Incidental seven-row closures must not use
                    # the bounded thirty-target ACK registry. Once registered,
                    # every subsequent ACK, including the last single row, counts.
                    elif span["rows_changed"] == 5000:
                        if len(self.heavy_membership_ack_rows) < 30:
                            self.heavy_membership_ack_rows[identity] = span["rows_changed"]
                        else:
                            self.dropped_heavy_membership_acks += 1
                if span["rows_changed"] > 3:
                    self.large_dropped += len(self.large_spans) == self.large_spans.maxlen
                    self.large_spans.append(span)
                total = self.phases.setdefault(span["phase"], {"committed_spans": 0, "rows_changed": 0})
                total["committed_spans"] += 1
                total["rows_changed"] += span["rows_changed"]
        pending["phases"].clear()

    def snapshot(self):
        with self.lock:
            return {"sequence": self.sequence, "dropped_spans": self.dropped,
                "spans": list(self.spans), "large_spans": list(self.large_spans),
                "dropped_large_spans": self.large_dropped,
                "heavy_membership_ack_rows": dict(self.heavy_membership_ack_rows),
                "dropped_heavy_membership_acks": self.dropped_heavy_membership_acks,
                "budget_errors": list(self.budget_errors), "budget_error_sequence": self.budget_error_sequence,
                "dropped_budget_errors": self.dropped_budget_errors,
                "phases": {key: dict(value) for key, value in self.phases.items()}}


def install_maintenance_metrics(metrics, release=None):
    from sqlalchemy.sql.dml import Update

    from app.aggregation.event_time import EventTimeBudgetError
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    from app.services.implementations.aggregate_retention import AggregateRetentionService

    original_maintain = AggregateRetentionService.maintain
    original_save = AggregateRetentionService._save
    original_error_init = EventTimeBudgetError.__init__

    def error_init(error, *args, **kwargs):
        with suppress(AttributeError, LookupError, MemoryError, RuntimeError, TypeError, ValueError):
            if len(args) == 1 and not kwargs:
                metrics.capture_budget_error(args[0])
            elif not args and set(kwargs) == {"factor"}:
                metrics.capture_budget_error(kwargs["factor"])
        return original_error_init(error, *args, **kwargs)

    def maintain(service, identity, *args, **kwargs):
        completion = metrics.completion.get()
        pending = {"aggregate_id": str(identity), "phases": [],
            "job_id": completion.get("job_id") if completion is not None else None}
        token = metrics.current.set(pending)
        original_rollback = service.uow.rollback
        original_commit = service.uow.commit if release is not None else None
        original_clear = service.repository.clear_payload if release is not None else None
        claimed = []

        def clear(closure, *args, **kwargs):
            # Capture the armed identity before the real claim SQL. An arm
            # arriving during or after an earlier claim cannot replay it.
            scope = release.claim(identity, closure.origin_point_id) if release is not None else None
            result = original_clear(closure, *args, **kwargs)
            if result and kwargs.get("automatic") is True and scope is not None:
                claimed.append(scope)
            return result

        def commit():
            try:
                result = original_commit()
            except BaseException:
                claimed.clear()
                raise
            acknowledged = time.monotonic()
            accepted = list(claimed)
            claimed.clear()
            for scope in accepted:
                release.acknowledged(scope, acknowledged)
            return result

        def rollback():
            pending["phases"].clear()
            claimed.clear()
            return original_rollback()

        service.uow.rollback = rollback
        if release is not None:
            service.uow.commit = commit
            service.repository.clear_payload = clear
        try:
            return original_maintain(service, identity, *args, **kwargs)
        finally:
            service.uow.rollback = original_rollback
            if release is not None:
                service.uow.commit = original_commit
                service.repository.clear_payload = original_clear
            metrics.current.reset(token)
            if metrics.completion.get() is None:
                with suppress(AttributeError, LookupError, MemoryError, RuntimeError, TypeError, ValueError):
                    metrics.record_budget_error(pending.get("first_budget_error"))

    def save(service, *args, **kwargs):
        pending = metrics.current.get()
        try:
            result = original_save(service, *args, **kwargs)
        except BaseException:
            if pending is not None:
                pending["phases"].clear()
            raise
        if pending is not None:
            metrics.publish(pending)
        return result

    def instrument(original, phase, table):
        def compact(repository, point_id, *args, **kwargs):
            pending = metrics.current.get()
            if pending is None:
                return original(repository, point_id, *args, **kwargs)
            started = time.monotonic()
            rows = 0
            original_execute = repository.session.execute

            def execute(statement, *args, **kwargs):
                nonlocal rows
                result = original_execute(statement, *args, **kwargs)
                if isinstance(statement, Update) and statement.table.name == table:
                    rows += max(0, result.rowcount)
                return result

            repository.session.execute = execute
            try:
                cursor = original(repository, point_id, *args, **kwargs)
            finally:
                repository.session.execute = original_execute
            pending["phases"].append({"closure_id": str(point_id), "phase": phase,
                "rows_changed": rows, "started_monotonic": started,
                "sql_completed_monotonic": time.monotonic(), "cursor_present": cursor is not None})
            return cursor
        return compact

    for phase, table in (("memberships", "aggregate_memberships"), ("outcomes", "detector_outcomes")):
        method = "compact_" + phase
        setattr(SqlAlchemyAggregateRepository, method,
            instrument(getattr(SqlAlchemyAggregateRepository, method), phase, table))
    AggregateRetentionService.maintain = maintain
    AggregateRetentionService._save = save
    EventTimeBudgetError.__init__ = error_init


def install_online_commit_metrics(metrics):
    from app.services.implementations.aggregate_execution import AggregateExecutionService

    original_advance = AggregateExecutionService.advance

    def advance(service, *args, **kwargs):
        original_commit = service.uow.commit
        committed = None

        def commit():
            nonlocal committed
            started = time.monotonic()
            result = original_commit()
            committed = (started, time.monotonic())
            return result

        service.uow.commit = commit
        try:
            result = original_advance(service, *args, **kwargs)
        finally:
            service.uow.commit = original_commit
        if metrics.phase is not None:
            metrics.phase.safe(metrics.phase.classify_foreground, result)
        if result.get("state") == "closed" and result.get("origin") == "online" and committed is not None:
            metrics.record_point_commit(result["point_id"], *committed)
        return result

    AggregateExecutionService.advance = advance


def cgroup_memory():
    """Read the kernel high-water counter of Docker's mounted memory cgroup."""
    for version, directory, peak_name, limit_name in (
            (2, "/sys/fs/cgroup", "memory.peak", "memory.max"),
            (1, "/sys/fs/cgroup/memory", "memory.max_usage_in_bytes", "memory.limit_in_bytes")):
        peak_file = Path(directory) / peak_name
        if not peak_file.is_file():
            continue
        result = {"peak_memory_bytes": None, "memory_limit_bytes": None,
            "memory_peak_source": str(peak_file), "memory_cgroup_version": version, "memory_metric_error": None}
        try:
            peak = int(peak_file.read_text().strip())
            limit = (Path(directory) / limit_name).read_text().strip()
            if peak < 0:
                raise ValueError("negative cgroup high-water counter")
            result.update(peak_memory_bytes=peak, memory_limit_bytes=None if limit == "max" else int(limit))
        except (OSError, ValueError):
            result["memory_metric_error"] = "cgroup_memory_unreadable"
        return result
    return {"peak_memory_bytes": None, "memory_limit_bytes": None, "memory_peak_source": None,
        "memory_cgroup_version": None, "memory_metric_error": "cgroup_memory_unavailable"}


def canonical_resources():
    """Cumulative CPU and memory readings at an accepted worker boundary."""
    memory = cgroup_memory()
    version = memory["memory_cgroup_version"]
    current, cpu = None, None
    if version in {1, 2}:
        memory_path = ("/sys/fs/cgroup/memory.current" if version == 2
            else "/sys/fs/cgroup/memory/memory.usage_in_bytes")
        with suppress(OSError, ValueError):
            current = int(Path(memory_path).read_text().strip())
    if version == 2:
        with suppress(OSError, ValueError, StopIteration):
            cpu = next(int(value) for key, value in
                (line.split() for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines())
                if key == "usage_usec") / 1e6
    elif version == 1:
        for controller in ("cpuacct", "cpu,cpuacct"):
            with suppress(OSError, ValueError):
                cpu = int(Path("/sys/fs/cgroup", controller, "cpuacct.usage").read_text().strip()) / 1e9
                break
    return {"process_cpu_seconds": time.process_time(), "cgroup_cpu_seconds": cpu,
        "cgroup_memory_bytes": current, "cgroup_memory_peak_bytes": memory["peak_memory_bytes"],
        "cgroup_memory_limit_bytes": memory["memory_limit_bytes"], "cgroup_version": version}


class MaintenanceJobCapture:
    """A bounded scalar-only tail, separate from the legacy live trace counters."""

    RECORD_LIMIT = 256
    BYTE_LIMIT = 192 * 1024
    TIMESTAMPS = ("iteration_started_monotonic", "discovery_started_monotonic",
        "discovery_completed_monotonic", "append_before_monotonic", "append_after_monotonic",
        "acquire_before_monotonic", "acquire_after_monotonic", "execute_started_monotonic",
        "execute_completed_monotonic")

    def __init__(self):
        self.lock = Lock()
        self.records = deque()
        self.record_bytes = self.observed = self.evicted = self.invalid = 0

    @staticmethod
    def identity(job_id):
        if type(job_id) is not str or len(job_id) > 64 or not job_id.isascii():
            raise ValueError("invalid controlled job identity")
        prefix, separator, number = job_id.partition(":")
        if (separator != ":" or len(prefix) != 32 or any(char not in "0123456789abcdef" for char in prefix)
                or not number.isdecimal() or len(number) > 20 or int(number) <= 0):
            raise ValueError("invalid controlled job identity")
        return job_id

    @classmethod
    def project(cls, record):
        job_id, aggregate_id = cls.identity(record["job_id"]), record["aggregate_id"]
        if type(aggregate_id) is not str or str(UUID(aggregate_id)) != aggregate_id or record["kind"] != "maintenance":
            raise ValueError("invalid controlled maintenance identity")
        result = {"job_id": job_id, "aggregate_id": aggregate_id, "kind": "maintenance"}
        for key in cls.TIMESTAMPS:
            value = record.get(key)
            if value is not None and (type(value) not in {int, float} or not math.isfinite(value)):
                raise ValueError("invalid controlled measurement")
            result[key] = value
        return result

    @classmethod
    def metadata(cls, count, record_bytes, observed, evicted, invalid):
        return {"record_limit": cls.RECORD_LIMIT, "byte_limit": cls.BYTE_LIMIT,
            "observed": observed, "evicted": evicted, "invalid": invalid, "retained": count,
            "retained_bytes": 2 + record_bytes + 2 * max(0, count - 1), "records": []}

    def reject(self):
        with self.lock:
            self._store(deque(self.records), self.record_bytes, self.observed + 1, self.evicted, self.invalid + 1)

    def _store(self, records, amount, observed, evicted, invalid):
        while True:
            metadata = self.metadata(len(records), amount, observed, evicted, invalid)
            size = len(json.dumps(metadata).encode("ascii")) + metadata["retained_bytes"] - 2
            if len(records) <= self.RECORD_LIMIT and size <= self.BYTE_LIMIT:
                break
            _old, removed = records.popleft()
            amount -= removed
            evicted += 1
        self.records, self.record_bytes = records, amount
        self.observed, self.evicted, self.invalid = observed, evicted, invalid

    def append(self, record):
        try:
            projected = self.project(record)
            size = len(json.dumps(projected, allow_nan=False).encode("ascii"))
        except Exception:  # noqa: BLE001 - malformed diagnostics never replace the original work
            self.reject()
            return
        with self.lock:
            records = deque(self.records)
            records.append((projected, size))
            self._store(records, self.record_bytes + size, self.observed + 1, self.evicted, self.invalid)

    def snapshot(self):
        with self.lock:
            result = self.metadata(len(self.records), self.record_bytes, self.observed, self.evicted, self.invalid)
            result["records"] = [dict(record) for record, _size in self.records]
            return result


class GenerationPhaseCapture:
    """One active test phase and the retained canonical summary.

    All event times are already measured by existing hooks. Only accepted clock
    boundaries read monotonic(), once each. Encoded metadata is bounded; records
    are not events, vectors, model state or a claim about unobserved wall time.
    """

    BYTE_LIMIT = 64 * 1024
    RECORD_LIMIT = 10
    STAGES = ("discovered", "offered", "admitted", "completed")
    REASONS = ("target_active", "capacity_full", "maintenance_active", "not_queued", "unknown")
    OPERATIONS = ("history", "scan", "online", "other_foreground", "maintenance")
    INTERVALS = (("discovery", "discovery_started_monotonic", "discovery_completed_monotonic"),
        ("append", "append_before_monotonic", "append_after_monotonic"),
        ("queue", "append_after_monotonic", "acquire_before_monotonic"),
        ("execution", "execute_started_monotonic", "execute_completed_monotonic"))

    def __init__(self, run_id):
        self.run_id = run_id
        self.lock = Lock()
        self.sequence = 0
        self.live_provider = list
        self.canonical_frozen = None
        self.operation = ContextVar("captured_foreground_operation", default=None)
        self._reset()

    def _reset(self):
        self.scope = None
        self.begin = self.end = self.first_event = self.last_event = None
        self.outcome = None
        self.ended = False
        self.latest = OrderedDict()
        self.slowest = {}
        self.pending = set()
        self.preexisting = 0
        self.observed = self.byte_dropped = self.invalid = self.missing_links = 0
        self.error_count = 0
        self.errors = deque(maxlen=8)
        self.spans = deque(maxlen=10)
        self.budget_errors = deque(maxlen=10)
        self.span_count = self.budget_error_count = 0
        self.counts = {kind: {key: 0 for key in self.STAGES} for kind in ("foreground", "maintenance")}
        self.sums = {kind: {key: 0.0 for key in ("discovery", "append", "queue", "execution", "commit_ack")}
            for kind in self.counts}
        self.unavailable = {kind: {key: 0 for key in self.sums[kind]} for kind in self.counts}
        self.waits = {key: 0 for key in self.REASONS}
        self.frozen = None
        self.resources = dict.fromkeys(("begin", "end"))
        self.operations = {operation: {"completed": 0,
            "duration_sums_seconds": {name: 0.0 for name, _, _ in self.INTERVALS},
            "intervals_unavailable": {name: 0 for name, _, _ in self.INTERVALS}} for operation in self.OPERATIONS}
        self.online_results = dict.fromkeys(("computed", "uncomputed", "unknown"), 0)
        self.other_foreground_results = dict.fromkeys(("scan", "history", "waiting", "unknown"), 0)
        self.online_timings = {name: {"samples": 0, "seconds": None} for name in ("snapshot", "save", "commit")}
        self.completions = 0

    def safe(self, operation, *args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except Exception as error:  # noqa: BLE001 - test metadata cannot replace the original work
            with self.lock:
                self.error_count += 1
                self.errors.append(type(error).__name__[:64])
            return None

    @staticmethod
    def number(value):
        return value if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1e15 else None

    @staticmethod
    def uuid(value):
        return value if type(value) is str and len(value) == 36 and str(UUID(value)) == value else None

    def observe_marker(self, marker, values, *, expected_phase=None):
        if marker is None:
            return
        if not isinstance(marker, dict) or set(marker) - {"run_id", "aggregate_id", "nonce", "phase", "state", "outcome"}:
            raise ValueError("invalid phase marker schema")
        scope = {key: marker.get(key) for key in ("run_id", "aggregate_id", "nonce", "phase")}
        if (scope["run_id"] != self.run_id or type(self.run_id) is not str or len(self.run_id) > 64
                or scope["phase"] not in {"canonical", "generation"}
                or expected_phase is not None and scope["phase"] != expected_phase
                or self.uuid(scope["aggregate_id"]) is None
                or UUID(scope["aggregate_id"]) not in values or self.uuid(scope["nonce"]) is None
                or marker.get("state") not in {"begin", "end"}
                or marker.get("outcome") not in {None, "complete", "failed"}):
            raise ValueError("invalid phase marker scope")
        # Fetch only existing bounded trace state, outside the phase lock: a
        # trace callback never needs to acquire its own lock recursively.
        starts = marker["state"] == "begin"
        transition = (self.scope is not None and self.scope["phase"] == "canonical"
            and scope["phase"] == "generation" and self.scope["aggregate_id"] == scope["aggregate_id"])
        initial = self.live_provider() if starts and (self.scope is None or transition) else []
        with self.lock:
            if transition:
                # Preserve the end snapshot, or the available partial interval
                # if generation replaced the clock document before end was observed.
                self.canonical_frozen = self._capture_snapshot()
                self._reset()
            if self.scope is not None and (self.scope != scope or self.ended):
                return
            if starts:
                if self.scope is not None:
                    return  # Replay must not move the boundary or reset counters.
                self.scope = scope
                self.begin = time.monotonic()
                self.sequence += 1
                if scope["phase"] == "canonical":
                    self._resource_boundary("begin")
                for row in initial[:66]:
                    if row.get("aggregate_id") == scope["aggregate_id"]:
                        projected = self.project(row)
                        self.preexisting += 1
                        self.pending.add(projected["job_id"])
                        self._remember(projected)
            else:
                self.scope = scope
                self.end = time.monotonic()
                self.ended = True
                self.outcome = marker.get("outcome")
                if scope["phase"] == "canonical":
                    self._resource_boundary("end")
                self.frozen = self._snapshot()
                self.sequence += 1

    def _resource_boundary(self, key):
        try:
            self.resources[key] = {name: self.number(value) for name, value in canonical_resources().items()}
        except Exception as error:  # noqa: BLE001 - boundary metadata cannot change worker results
            self.error_count += 1
            self.errors.append(type(error).__name__[:64])

    def canonical_active(self):
        return self.scope is not None and self.scope["phase"] == "canonical" and not self.ended

    def classify_foreground(self, result):
        captured = self.operation.get()
        if captured is None or self.scope is None or self.ended or not isinstance(result, dict):
            return
        state, origin = result.get("state"), result.get("origin")
        if state in ("scanned", "scan_cancelled"):
            captured["kind"] = "scan"
        elif self.canonical_active():
            if origin == "initial_history" or state == "history_waiting":
                captured["kind"] = "history"
            elif origin == "online":
                captured["kind"] = "online"

    def project(self, record):
        if (record.get("kind") not in self.counts or self.uuid(record.get("aggregate_id")) is None):
            raise ValueError("invalid phase trace")
        result = {"job_id": MaintenanceJobCapture.identity(record.get("job_id")),
            "aggregate_id": record["aggregate_id"], "kind": record["kind"]}
        for key in MaintenanceJobCapture.TIMESTAMPS:
            result[key] = self.number(record.get(key))
        return result

    def _event_times(self, row):
        for key, value in row.items():
            if key.endswith("monotonic") and self.number(value) is not None:
                self.first_event = value if self.first_event is None else min(self.first_event, value)
                self.last_event = value if self.last_event is None else max(self.last_event, value)

    def _remember(self, row):
        key = row["job_id"]
        previous = self.latest.pop(key, None) or self.slowest.get(key) or {}
        self.latest[key] = {**previous, **row}
        while len(self.latest) > self.RECORD_LIMIT:
            self.latest.popitem(last=False)
        self._event_times(row)

    def _interval(self, kind, name, first, last):
        first, last = self.number(first), self.number(last)
        if first is None or last is None or last < first:
            self.unavailable[kind][name] += 1
        else:
            self.sums[kind][name] += last - first

    def discovered(self, kind, started, completed, identities):
        with self.lock:
            if self.scope is None or self.ended:
                return
            if isinstance(identities, (list, tuple)):
                found = sum(str(value) == self.scope["aggregate_id"] for value in identities)
                self.counts[kind]["discovered"] += found
                if found:
                    self._event_times({"discovery_started_monotonic": started,
                        "discovery_completed_monotonic": completed})
            else:
                self.missing_links += 1

    def unlinked(self, stage, job):
        with self.lock:
            if self.scope is not None and not self.ended and str(job.aggregate_id) == self.scope["aggregate_id"]:
                self.counts[job.kind][stage] += 1
                self.missing_links += 1
                if self.scope["phase"] == "generation" and stage == "completed" and job.kind == "foreground":
                    self.other_foreground_results["unknown"] += 1

    def trace_failure(self, error):
        with self.lock:
            if self.scope is not None and not self.ended:
                self.error_count += 1
                self.errors.append(type(error).__name__[:64])

    def _generation_result(self, worker_result, step):
        wrapper = worker_result if isinstance(worker_result, dict) else {}
        result = wrapper.get("result")
        result = result if isinstance(result, dict) else {}
        state, origin = result.get("state"), result.get("origin")
        if state == "closed" and origin == "online" and self.uuid(str(result.get("point_id"))) is not None:
            status = result.get("calculation_status")
            self.online_results[status if status in ("computed", "uncomputed") else "unknown"] += 1
            receipt = result.get("execution")
            snapshot = receipt.get("timings") if isinstance(receipt, dict) else None
            publication = result.get("timings")
            for name, timings in (("snapshot", snapshot), ("save", publication), ("commit", publication)):
                duration = self.number(timings.get(name)) if isinstance(timings, dict) else None
                if duration is not None:
                    totals = self.online_timings[name]
                    totals["samples"] += 1
                    totals["seconds"] = (totals["seconds"] or 0.0) + duration
        elif (step or {}).get("captured_operation") == "scan" or state in ("scanned", "scan_cancelled"):
            self.other_foreground_results["scan"] += 1
        elif origin == "initial_history":
            self.other_foreground_results["history"] += 1
        elif (worker_result == "waiting" or wrapper.get("state") == "waiting"
                or state in ("history_waiting", "waiting_close_deadline")):
            self.other_foreground_results["waiting"] += 1
        else:
            # already_closed can refer to existing DB state without a new
            # online result. Its pointer alone must not count as a publication.
            self.other_foreground_results["unknown"] += 1

    def job_event(self, stage, record, step=None, worker_result=None):
        with self.lock:
            if self.scope is None or self.ended or record.get("aggregate_id") != self.scope["aggregate_id"]:
                return
            projected = self.project(record)
            kind, identity = projected["kind"], projected["job_id"]
            self.counts[kind][stage] += 1
            if stage == "offered":
                self.observed += 1
                if len(self.pending) < 66:
                    self.pending.add(identity)
                else:
                    self.missing_links += 1
            if stage == "completed":
                self.pending.discard(identity)
                if isinstance(step, dict):
                    projected["point_id"] = self.uuid(step.get("point_id"))
                    projected["origin"] = step.get("origin") if step.get("origin") in {"online", "initial_history"} else None
                if self.scope["phase"] == "canonical":
                    operation = "maintenance" if kind == "maintenance" else (step or {}).get("captured_operation")
                    if operation not in self.OPERATIONS:
                        operation = "history" if projected.get("origin") == "initial_history" else (
                            "online" if projected.get("origin") == "online" else "other_foreground")
                    operation_totals = self.operations[operation]
                    operation_totals["completed"] += 1
                    self.completions += 1
                    if self.completions % 32 == 0:
                        self.sequence += 1  # Periodic partial checkpoint on the existing export path.
                    for name, first, last in self.INTERVALS:
                        beginning, ending = projected[first], projected[last]
                        if beginning is not None and ending is not None and ending >= beginning:
                            operation_totals["duration_sums_seconds"][name] += ending - beginning
                        else:
                            operation_totals["intervals_unavailable"][name] += 1
                elif kind == "foreground":
                    self._generation_result(worker_result, step)
                for name, first, last in self.INTERVALS:
                    self._interval(kind, name, projected[first], projected[last])
                first, last = projected.get("iteration_started_monotonic"), projected.get("execute_completed_monotonic")
                projected["rank_seconds"] = last - first if first is not None and last is not None and last >= first else None
                if projected["rank_seconds"] is not None:
                    self.slowest[identity] = projected
                    self.slowest = dict(sorted(self.slowest.items(),
                        key=lambda item: (item[1]["rank_seconds"], item[0]), reverse=True)[:self.RECORD_LIMIT])
            self._remember(projected)

    def no_admission(self, admission):
        with self.lock:
            if self.scope is None or self.ended:
                return
            target = UUID(self.scope["aggregate_id"])
            ready = [job for job in admission.ready if job.aggregate_id == target]
            reason = "unknown"
            if target in admission.active:
                reason = "target_active"
            elif len(admission.active) >= admission.capacity:
                reason = "capacity_full"
            elif not ready:
                reason = "not_queued"
            elif ready[0].kind == "maintenance" and any(job.kind == "maintenance" for job in admission.active.values()):
                reason = "maintenance_active"
            self.waits[reason] += 1

    def physical(self, row, error=False):
        with self.lock:
            if self.scope is None or self.ended or row.get("aggregate_id") != self.scope["aggregate_id"]:
                return
            key = row.get("job_id")
            try:
                key = MaintenanceJobCapture.identity(key)
            except (TypeError, ValueError):
                key = None
                self.missing_links += 1
            if error:
                self.budget_error_count += 1
                self.budget_errors.append({"sequence": self.counter(row.get("sequence")), "job_id": key,
                    "factor": row.get("factor") if type(row.get("factor")) is str
                        and row["factor"].isascii() and row["factor"].isidentifier() and len(row["factor"]) <= 64 else None})
            else:
                self.span_count += 1
                span = {"sequence": self.counter(row.get("sequence")), "job_id": key, "closure_id": self.uuid(row.get("closure_id")),
                    "phase": row.get("phase") if row.get("phase") in {"memberships", "outcomes"} else None}
                span.update({name: self.number(row.get(name)) for name in
                    ("started_monotonic", "sql_completed_monotonic", "committed_monotonic")})
                self.spans.append(span)
                self._event_times(span)
                self._interval("maintenance", "commit_ack", span["sql_completed_monotonic"], span["committed_monotonic"])

    def point_ack(self, identity, started, acknowledged):
        with self.lock:
            if self.scope is None or self.ended:
                return
            for record in reversed(self.latest.values()):
                if record.get("point_id") == str(identity):
                    record.update(point_commit_start_monotonic=self.number(started),
                        point_commit_ack_monotonic=self.number(acknowledged))
                    if record["job_id"] in self.slowest:
                        self.slowest[record["job_id"]].update(point_commit_start_monotonic=self.number(started),
                            point_commit_ack_monotonic=self.number(acknowledged))
                    self._interval("foreground", "commit_ack", started, acknowledged)
                    return
            # The real commit precedes _completed; its link is supplied by the
            # existing observe hook when the online point first enters closed.

    def _snapshot(self):
        result = {"byte_limit": self.BYTE_LIMIT, "record_limit": self.RECORD_LIMIT,
            "scope": dict(self.scope), "state": "ended" if self.ended else "active", "outcome": self.outcome,
            "clock_domain": "aggregate_worker_monotonic", "begin_observed_monotonic": self.begin,
            "end_observed_monotonic": self.end, "first_event_monotonic": self.first_event,
            "last_event_monotonic": self.last_event, "boundaries_complete": self.begin is not None and self.end is not None,
            "complete": self.begin is not None and self.end is not None and self.outcome == "complete"
                and self.missing_links == 0 and self.error_count == 0,
            "job_intervals_may_cross_boundaries": True,
            "counters_are_observations_not_unique_discoveries": True,
            "observation_lag_seconds": None, "preexisting_jobs": self.preexisting if self.begin is not None else None,
            "counters": {kind: dict(values) for kind, values in self.counts.items()} if self.begin is not None else None,
            "duration_sums_seconds": {kind: dict(values) for kind, values in self.sums.items()} if self.begin is not None else None,
            "intervals_unavailable": {kind: dict(values) for kind, values in self.unavailable.items()} if self.begin is not None else None,
            "sums_are_overlapping_not_wall_time": True,
            "acquire_none_observations": dict(self.waits) if self.begin is not None else None,
            "observed_jobs": self.observed if self.begin is not None else None,
            "pending_jobs": len(self.pending) if self.begin is not None else None,
            "pending_ids": sorted(self.pending)[:10], "pending_ids_omitted": max(0, len(self.pending) - 10),
            "missing_links": self.missing_links, "invalid_records": self.invalid,
            "capture_error_count": self.error_count, "capture_error_types": list(self.errors),
            "span_observed": self.span_count, "span_dropped": max(0, self.span_count - len(self.spans)),
            "budget_error_observed": self.budget_error_count,
            "budget_error_dropped": max(0, self.budget_error_count - len(self.budget_errors)),
            "latest_jobs": list(self.latest.values()), "slowest_jobs": list(self.slowest.values()),
            "span_links": list(self.spans), "budget_error_links": list(self.budget_errors), "byte_cap_dropped": 0}
        if self.scope["phase"] == "canonical":
            result["resource_boundaries"] = self.resources
            result["work_by_operation"] = self.operations if self.begin is not None else None
        else:
            result["online_results"] = self.online_results if self.begin is not None else None
            result["other_foreground_results"] = self.other_foreground_results if self.begin is not None else None
            result["online_timings"] = self.online_timings if self.begin is not None else None
        while True:
            result["retained_job_ids"] = len({row["job_id"] for key in ("latest_jobs", "slowest_jobs") for row in result[key]})
            result["records_omitted"] = max(0, self.observed + self.preexisting - result["retained_job_ids"])
            if len(json.dumps(result, allow_nan=False).encode("ascii")) + 32 <= self.BYTE_LIMIT:
                break
            for key in ("latest_jobs", "slowest_jobs", "span_links", "budget_error_links", "pending_ids"):
                if result[key]:
                    result[key].pop(0)
                    result["byte_cap_dropped"] += 1
                    break
            else:
                raise ValueError("phase metadata envelope cannot fit")
        return result

    @staticmethod
    def counter(value):
        return value if type(value) is int and 0 <= value <= 2**63 - 1 else None

    def _capture_snapshot(self):
        try:
            return json.loads(json.dumps(self.frozen if self.frozen is not None else self._snapshot(), allow_nan=False))
        except Exception as error:  # noqa: BLE001 - never mask a calculation or teardown
            return {"byte_limit": self.BYTE_LIMIT, "scope": dict(self.scope), "state": "unavailable",
                "boundaries_complete": False, "capture_error": type(error).__name__[:64]}

    def snapshot(self, phase=None):
        with self.lock:
            if phase == "canonical" and self.canonical_frozen is not None:
                return json.loads(json.dumps(self.canonical_frozen, allow_nan=False))
            if self.scope is None or phase is not None and self.scope["phase"] != phase:
                return None
            return self._capture_snapshot()


class JobTrace:
    """Test-only correlation of existing calls; no extra DB reads or worker jobs."""

    def __init__(self, phase=None):
        self.identity = uuid4().hex
        self.sequence = 0
        self.live = {}
        self.lock = Lock()
        self.discovery = ContextVar("aggregate_test_discovery", default=None)
        self.errors = deque(maxlen=8)
        self.error_count = self.dropped = self.high_water = 0
        self.maintenance_jobs = MaintenanceJobCapture()
        self.phase = phase

    def live_snapshot(self):
        with self.lock:
            return [dict(row) for row in self.live.values()]

    def safe(self, operation, *args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except Exception as error:  # noqa: BLE001 - diagnostics never replace the original work
            with self.lock:
                self.error_count += 1
                self.errors.append(type(error).__name__)
            if self.phase is not None:
                self.phase.safe(self.phase.trace_failure, error)
            return None

    def offered(self, job, kind, before, after):
        scope = self.discovery.get() or {}
        with self.lock:
            if len(self.live) >= 66:  # Existing maximum: 64 ready and two active.
                self.dropped += 1
                if self.phase is not None:
                    self.phase.safe(self.phase.unlinked, "offered", job)
                return
            self.sequence += 1
            self.live[id(job)] = {"job_id": f"{self.identity}:{self.sequence}",
                "aggregate_id": str(job.aggregate_id), "kind": kind,
                "iteration_started_monotonic": job.queued_at,
                "discovery_started_monotonic": scope.get(kind, {}).get("started"),
                "discovery_completed_monotonic": scope.get(kind, {}).get("completed"),
                "append_before_monotonic": before, "append_after_monotonic": after}
            self.high_water = max(self.high_water, len(self.live))
            record = dict(self.live[id(job)])
        if self.phase is not None:
            self.phase.safe(self.phase.job_event, "offered", record)

    def update(self, job, **values):
        record = None
        with self.lock:
            if id(job) in self.live:
                self.live[id(job)].update(values)
                record = dict(self.live[id(job)])
        if self.phase is not None and record is not None:
            self.phase.safe(self.phase.job_event, "admitted", record)
        elif self.phase is not None:
            self.phase.safe(self.phase.unlinked, "admitted", job)

    def job_id(self, job):
        with self.lock:
            value = self.live.get(id(job), {}).get("job_id")
        try:
            return MaintenanceJobCapture.identity(value)
        except (TypeError, ValueError):
            return None

    def discovered(self, kind, started, completed, identities=None):
        self.discovery.get()[kind] = {"started": started, "completed": completed}
        if self.phase is not None:
            self.phase.safe(self.phase.discovered, kind, started, completed, identities)

    def completed(self, job, outcome):
        with self.lock:
            record = self.live.pop(id(job), None)
        if record is not None:
            record.update(execute_started_monotonic=outcome["started"],
                execute_completed_monotonic=outcome["completed"])
            if job.kind == "maintenance":
                try:
                    self.maintenance_jobs.append(record)
                except Exception:  # noqa: BLE001 - capture failure does not change the legacy trace or worker
                    self.maintenance_jobs.reject()
        return record

    def snapshot(self):
        with self.lock:
            return {"live": len(self.live), "max_live": self.high_water, "issued": self.sequence,
                "dropped": self.dropped, "error_count": self.error_count, "errors": list(self.errors)}

    def install(self, worker):
        original_build, original_discover = worker.build, worker._discover
        original_offer, original_acquire = worker.admission.offer, worker.admission.acquire
        original_completed = worker._completed
        trace = self

        class DiscoveryService:
            def __init__(self, service):
                self.service = service

            def __getattr__(self, name):
                original = getattr(self.service, name)
                if name not in {"due", "maintenance_due"}:
                    return original

                def read(*args, **kwargs):
                    kind = "foreground" if name == "due" else "maintenance"
                    started = time.monotonic()
                    identities = None
                    try:
                        identities = original(*args, **kwargs)
                        return identities
                    finally:
                        trace.safe(trace.discovered, kind, started, time.monotonic(), identities)
                return read

        def build(*args, **kwargs):
            service = original_build(*args, **kwargs)
            return DiscoveryService(service) if trace.discovery.get() is not None else service

        def discover():
            token = trace.discovery.set({})
            try:
                return original_discover()
            finally:
                trace.discovery.reset(token)

        def offer(kind, identities, queued_at):
            known = {id(job) for job in worker.admission.ready}
            before = time.monotonic()
            try:
                return original_offer(kind, identities, queued_at)
            finally:
                after = time.monotonic()
                for job in worker.admission.ready:
                    if id(job) not in known:
                        trace.safe(trace.offered, job, kind, before, after)

        def acquire():
            before = time.monotonic()
            job = original_acquire()
            after = time.monotonic()
            if job is not None:
                trace.safe(trace.update, job, acquire_before_monotonic=before, acquire_after_monotonic=after)
            elif trace.phase is not None:
                trace.phase.safe(trace.phase.no_admission, worker.admission)
            return job

        def completed(identity, outcome):
            job = worker.admission.active[identity]
            result = original_completed(identity, outcome)
            record = trace.safe(trace.completed, job, outcome)
            if record is not None:
                worker.steps[-1]["job_trace"] = record
                if trace.phase is not None:
                    step = worker.steps[-1]
                    if outcome.get("captured_operation") is not None:
                        step = {**step, "captured_operation": outcome["captured_operation"]}
                    trace.phase.safe(trace.phase.job_event, "completed", record, step, outcome.get("result"))
            elif trace.phase is not None:
                trace.phase.safe(trace.phase.unlinked, "completed", job)
            return result

        worker.build, worker._discover = build, discover
        worker.admission.offer, worker.admission.acquire = offer, acquire
        worker._completed = completed


def encode_worker_metrics(body):
    """Keep optional phase evidence inside the existing metrics reader limit."""
    encoded = json.dumps(body)
    if len(encoded) <= WORKER_METRICS_READER_LIMIT or not any(key in body for key in PHASE_FIELDS):
        return encoded
    compact = dict(body)
    for key in PHASE_FIELDS:
        if key in compact:
            phase = compact[key]
            fields = ("latest_jobs", "slowest_jobs", "span_links", "budget_error_links")
            compact[key] = {**phase, **{name: [] for name in fields},
                "byte_cap_dropped": phase.get("byte_cap_dropped", 0) + sum(len(phase.get(field, [])) for field in fields),
                "records_omitted": phase.get("records_omitted", 0) + phase.get("retained_job_ids", 0),
                "retained_job_ids": 0}
    encoded = json.dumps(compact)
    if len(encoded) <= WORKER_METRICS_READER_LIMIT:
        return encoded
    # Existing legacy evidence has first claim on the transport. A missing
    # optional phase is unavailable, never filled with fabricated zero counts.
    return json.dumps({key: value for key, value in body.items() if key not in PHASE_FIELDS})


def install_metrics(release=None, phase_capture=None):
    import resource

    from app.services.implementations.aggregate_worker import AggregateWorker

    original_init = AggregateWorker.__init__
    original_execute = AggregateWorker._execute
    maintenance = MaintenanceMetrics()
    maintenance.phase = phase_capture
    install_maintenance_metrics(maintenance, release)
    install_online_commit_metrics(maintenance)

    def execute(worker, job):
        if job.kind != "maintenance":
            if phase_capture is None or phase_capture.scope is None or phase_capture.ended:
                return original_execute(worker, job)
            captured = {}
            operation_token = phase_capture.operation.set(captured)
            try:
                outcome = original_execute(worker, job)
                if captured and isinstance(outcome, dict):
                    outcome["captured_operation"] = captured["kind"]
                return outcome
            finally:
                phase_capture.operation.reset(operation_token)
        try:
            job_id = worker._controlled_job_trace.job_id(job)
        except Exception:  # noqa: BLE001 - a failed metadata lookup cannot prevent maintenance
            job_id = None
        completion = {"job_id": job_id}
        token = maintenance.completion.set(completion)
        outcome = None
        try:
            outcome = original_execute(worker, job)
            return outcome
        finally:
            try:
                with suppress(AttributeError, LookupError, MemoryError, RuntimeError, TypeError, ValueError):
                    total = max(0, outcome["completed"] - outcome["started"]) if isinstance(outcome, dict) else None
                    maintenance.record_budget_error(completion.get("first_budget_error"), total)
            finally:
                maintenance.completion.reset(token)

    def initialize(worker, *args, **kwargs):
        original_observe = kwargs.pop("observe", None)
        trace = JobTrace(phase_capture)
        if phase_capture is not None:
            phase_capture.live_provider = trace.live_snapshot
        closed = OrderedDict()
        history = OrderedDict()
        metrics = {"max_active": 0, "max_queued": 0, "max_reserved_bytes": 0,
            "peak_memory_bytes": None, "peak_rss_bytes": 0}
        maintenance_sequence = 0
        maintenance_job_sequence = 0
        release_sequence = 0
        budget_error_sequence = 0
        phase_sequence = 0

        def observe(payload):
            nonlocal maintenance_sequence, maintenance_job_sequence, budget_error_sequence, release_sequence, phase_sequence
            if original_observe:
                original_observe(payload)
            admission = payload["admission"]
            for destination, source in (("max_active", "active"), ("max_queued", "queued"),
                    ("max_reserved_bytes", "reserved_bytes")):
                metrics[destination] = max(metrics[destination], admission[source])
            memory = cgroup_memory()
            peak = memory.pop("peak_memory_bytes")
            metrics.update(memory)
            if peak is not None:
                metrics["peak_memory_bytes"] = max(metrics["peak_memory_bytes"] or 0, peak)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            metrics["peak_rss_bytes"] = max(metrics["peak_rss_bytes"], usage.ru_maxrss * 1024)
            metrics["cpu_seconds"] = usage.ru_utime + usage.ru_stime
            changed = False
            for step in payload["steps"]:
                if step.get("origin") == "initial_history" and step["aggregate_id"] not in history:
                    history[step["aggregate_id"]] = step
                    changed = True
                    while len(history) > 30:
                        history.popitem(last=False)
                if step.get("origin") == "online" and step.get("point_id") and step["point_id"] not in closed:
                    point_commit = maintenance.point_commit(step["point_id"]) or {}
                    closed[step["point_id"]] = {**step, "commit_observed_monotonic": time.monotonic(),
                        "point_commit_start_monotonic": point_commit.get("started"),
                        "point_commit_ack_monotonic": point_commit.get("acknowledged")}
                    if phase_capture is not None:
                        phase_capture.safe(phase_capture.point_ack, step["point_id"],
                            point_commit.get("started"), point_commit.get("acknowledged"))
                    while len(closed) > 256:
                        closed.popitem(last=False)
                    changed = True
            progress = maintenance.snapshot()
            maintenance_jobs = trace.maintenance_jobs.snapshot()
            if maintenance_jobs["observed"] != maintenance_job_sequence:
                changed = True
                maintenance_job_sequence = maintenance_jobs["observed"]
            first_clear = release.snapshot() if release is not None else None
            if first_clear is not None and first_clear["sequence"] != release_sequence:
                changed = True
                release_sequence = first_clear["sequence"]
            if progress["sequence"] != maintenance_sequence:
                changed = True
                maintenance_sequence = progress["sequence"]
            if progress["budget_error_sequence"] != budget_error_sequence:
                changed = True
                budget_error_sequence = progress["budget_error_sequence"]
            legacy_changed = changed
            if phase_capture is not None and phase_capture.sequence != phase_sequence:
                changed = True
                phase_sequence = phase_capture.sequence
            if changed:
                temporary = Path("/tmp/aggregate-performance.next")
                body = {**metrics, "closed": closed, "history_first": history,
                    "maintenance": progress, "job_trace": trace.snapshot(), "maintenance_jobs": maintenance_jobs}
                if first_clear is not None:
                    body["first_clear_release"] = first_clear
                if phase_capture is not None:
                    for key in ("canonical", "generation"):
                        phase = phase_capture.safe(phase_capture.snapshot, key)
                        if phase is not None:
                            body[key + "_phase"] = phase
                try:
                    try:
                        encoded = encode_worker_metrics(body)
                    except Exception as error:  # Preserve the original metadata path.
                        if not any(key in body for key in PHASE_FIELDS):
                            raise
                        body.pop("canonical_phase", None)
                        body.pop("generation_phase", None)
                        phase_capture.safe(phase_capture.trace_failure, error)
                        encoded = json.dumps(body)
                    temporary.write_text(encoded, encoding="utf-8")
                    temporary.replace(Path("/tmp/aggregate-performance.json"))
                except Exception as error:  # A marker-only export is best effort.
                    if legacy_changed:
                        raise
                    phase_capture.safe(phase_capture.trace_failure, error)

        original_init(worker, *args, observe=observe, **kwargs)
        worker._controlled_job_trace = trace
        trace.install(worker)

    AggregateWorker.__init__ = initialize
    AggregateWorker._execute = execute


def main():
    from sqlalchemy.engine import make_url

    from app.adapters.wal_bootstrap import read_setting

    url = make_url(read_setting("DATABASE_URL"))
    if url.database != "diploma_test":
        raise ValueError("controlled clock requires the disposable profile database")
    clock = ScopedClock(os.environ["AGGREGATE_TEST_CLOCK"])
    install_clock(clock)
    release = None
    if sys.argv[1:] == ["aggregate"]:
        if os.environ.get("AGGREGATE_TEST_RUN_ID"):
            release = FirstClearRelease(clock, os.environ["AGGREGATE_TEST_RUN_ID"])
            clock.phase_capture = GenerationPhaseCapture(os.environ["AGGREGATE_TEST_RUN_ID"])
        install_metrics(release, clock.phase_capture)
        from app.aggregate_worker import main as worker_main
        # Observe the already-published beginning before the first discovery.
        # Worker imports and Docker up/wait stay in the separate host interval.
        if clock.phase_capture is not None:
            clock.phase_capture.safe(clock.refresh)
    elif sys.argv[1:] == ["detector"]:
        from app.detector_worker import main as worker_main
    else:
        raise ValueError("controlled clock composes only the two existing workers")
    os.environ["DATABASE_URL"] = url.update_query_dict({
        "application_name": "controlled-" + sys.argv[1] + "-worker"}).render_as_string(hide_password=False)
    os.environ.pop("DATABASE_URL_FILE", None)
    try:
        worker_main()
    finally:
        if release is not None:
            release.close()


if __name__ == "__main__":
    main()
