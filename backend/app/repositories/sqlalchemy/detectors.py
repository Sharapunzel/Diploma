"""Detector SQL and locking. Never commits; caller owns the UnitOfWork."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from functools import wraps
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.orm import defer

from ...detection.contracts import DetectionError, StaleCandidate, digest
from ...detection.execution import (
    LOCK_CLASS,
    LOCK_ID,
    ExecutionOwner,
    reason_details,
    require_owner,
    safe_reason,
)
from ...models import (
    Aggregate,
    AggregateSchedule,
    Detector,
    DetectorCoordinator,
    DetectorOutcome,
    DetectorRecovery,
    DetectorWarmup,
    Finding,
    WorkingWindowPoint,
)


def snapshot(row):
    return deepcopy({column.key: utc(getattr(row, column.key)) for column in row.__table__.columns})


def utc(value):
    return value.astimezone(UTC) if isinstance(value, datetime) else value


def single_flush(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        # Revision and data are one UPDATE, even when candidate validation reads
        # other rows before the explicit final flush.
        with self.session.no_autoflush:
            return method(self, *args, **kwargs)
    return call


class SqlAlchemyDetectorRepository:
    def __init__(self, session):
        self.session = session

    def check_owner(self, owner):
        require_owner(owner)
        self.session.execute(select(func.set_config("lock_timeout", "5000", True)))
        self.session.execute(select(func.set_config("statement_timeout", "30000", True)))
        row = self._row(DetectorCoordinator, 1, True)
        held = self.session.scalar(text("""SELECT EXISTS (SELECT 1 FROM pg_locks
            WHERE locktype='advisory' AND classid=:class AND objid=:key AND objsubid=2
            AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
            AND pid=:pid AND granted AND mode='ExclusiveLock')"""),
            {"class": LOCK_CLASS, "key": LOCK_ID, "pid": owner.backend_pid})
        if (not held or row.owner_id != owner.owner_id or row.generation != owner.generation
                or row.backend_pid != owner.backend_pid):
            raise StaleCandidate("ownership_lost")
        import json
        self.session.execute(select(func.set_config("diploma.detector_owner", json.dumps({
            "id": str(owner.owner_id), "generation": owner.generation, "pid": owner.backend_pid}), True)))

    def acquire_owner(self, owner_id, backend_pid):
        self.session.execute(select(func.set_config("lock_timeout", "5000", True)))
        self.session.execute(select(func.set_config("statement_timeout", "30000", True)))
        row = self._row(DetectorCoordinator, 1, True)
        owner = ExecutionOwner(owner_id, row.generation + 1, backend_pid)
        # Prove the session lock before publishing the next fencing generation.
        held = self.session.scalar(text("""SELECT EXISTS (SELECT 1 FROM pg_locks
            WHERE locktype='advisory' AND classid=:class AND objid=:key AND objsubid=2
            AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
            AND pid=:pid AND granted AND mode='ExclusiveLock')"""),
            {"class": LOCK_CLASS, "key": LOCK_ID, "pid": backend_pid})
        if not held:
            raise StaleCandidate("ownership_lost")
        row.generation, row.owner_id, row.backend_pid = owner.generation, owner_id, backend_pid
        row.acquired_at = self.session.scalar(select(func.clock_timestamp()))
        self.session.flush()
        return owner

    def candidates(self, after=None, limit=100):
        query = select(Detector.id).where(Detector.execution_state.in_(("requested", "starting", "running")))
        if after is not None:
            query = query.where(Detector.id > after)
        return list(self.session.scalars(query.order_by(Detector.id).limit(limit)))

    def recovery(self, detector_id):
        return [snapshot(row) for row in self.session.scalars(select(DetectorRecovery).where(
            DetectorRecovery.detector_id == detector_id).order_by(DetectorRecovery.started_at, DetectorRecovery.id))]

    def last_result(self, detector_id, index):
        if index is None:
            return None
        row = self.session.scalar(select(DetectorOutcome).where(DetectorOutcome.detector_id == detector_id,
            DetectorOutcome.window_index == index))
        if row is None:
            return None
        points = self._points(select(WorkingWindowPoint).where(WorkingWindowPoint.id == row.working_point_id))
        return {"outcome": snapshot(row), "point": points[0] if points else None}

    def record_recovery(self, context, reason, generation):
        detector, schedule = context["detector"], context["schedule"]
        if detector["evaluation_start_index"] is None:
            return
        first = (detector["consumed_index"] + 1 if detector["consumed_index"] is not None
                 else detector["evaluation_start_index"])
        # scheduled_at = anchor + (index + 1)*duration, inclusive at DB now.
        elapsed = (context["now"] - schedule["anchor_utc"]) // timedelta(seconds=schedule["duration_seconds"])
        last = elapsed - 1
        if last >= first:
            self.session.add(DetectorRecovery(id=uuid4(), detector_id=detector["id"],
                first_index=first, last_index=last, reason=reason, started_at=context["now"],
                coordinator_generation=generation))
            self.session.flush()

    @single_flush
    def control(self, context, state, reason=None, new_request=False):
        detector = self._row(Detector, context["detector"]["id"], True)
        if detector.revision != context["detector"]["revision"]:
            raise StaleCandidate("stale_detector_candidate")
        detector.execution_state = state
        detector.failure_code = reason if state == "failed" else None
        detector.execution_generation += 1
        detector.revision += 1
        detector.updated_at = context["now"]
        if state == "failed":
            self._failure(detector, safe_reason(reason), reason)
        if new_request:
            detector.start_request_id = uuid4()
            detector.start_requested_at = context["now"]
            if detector.checkpoint is None:
                detector.active_attempt_id = None
        if state == "running":
            detector.execution_started_at = context["now"]
            detector.state, detector.reason = "ready", None
        if state == "stopped":
            detector.stopped_at = context["now"]
        self.session.flush()

    def _failure(self, detector, code, reason=None):
        detector.failure_code = code
        detector.last_failure = {"code": code, "at": utc(detector.updated_at).isoformat(),
            "start_request_id": str(detector.start_request_id) if detector.start_request_id else None,
            "details": reason_details(reason or code)}

    def _row(self, model, identity, lock=False):
        query = select(model).where(model.id == identity).execution_options(populate_existing=True)
        row = self.session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise DetectionError("detector_context_not_found")
        return row

    def aggregate_context(self, aggregate_id, lock=False):
        if lock:
            self.session.execute(select(func.set_config("lock_timeout", "5000", True)))
            self.session.execute(select(func.set_config("statement_timeout", "30000", True)))
        aggregate = self._row(Aggregate, aggregate_id)
        schedule = self._row(AggregateSchedule, aggregate.schedule_id, lock)
        if lock:
            self.session.refresh(aggregate)
        return {"aggregate": snapshot(aggregate), "schedule": snapshot(schedule),
                "now": self.session.scalar(select(func.clock_timestamp())).astimezone(UTC)}

    def read_context(self, detector_id):
        detector = self._row(Detector, detector_id)
        return {**self.aggregate_context(detector.aggregate_id), "detector": snapshot(detector)}

    def lock_context(self, detector_id):
        detector = self._row(Detector, detector_id)
        context = self.aggregate_context(detector.aggregate_id, True)
        detector = self._row(Detector, detector_id, True)
        return {**context, "detector": snapshot(detector)}

    def create(self, values):
        self.session.add(Detector(**values))
        self.session.flush()

    def _points(self, query):
        # Technical UTC transport preserves Python's full supported date range.
        rows = self.session.execute(query.options(defer(WorkingWindowPoint.event_time_min),
            defer(WorkingWindowPoint.event_time_max)).add_columns(
            func.timezone("UTC", WorkingWindowPoint.event_time_min),
            func.timezone("UTC", WorkingWindowPoint.event_time_max)))
        result = []
        for point, minimum, maximum in rows:
            data = deepcopy({column.key: utc(getattr(point, column.key))
                for column in point.__table__.columns
                if column.key not in {"event_time_min", "event_time_max"}})
            data["event_time_min"] = minimum.replace(tzinfo=UTC) if minimum else None
            data["event_time_max"] = maximum.replace(tzinfo=UTC) if maximum else None
            result.append(data)
        return result

    def next_working_point(self, schedule_id, after_index, evaluation_start_index):
        query = select(WorkingWindowPoint).where(
            WorkingWindowPoint.schedule_id == schedule_id,
            WorkingWindowPoint.window_index >= evaluation_start_index)
        if after_index is not None:
            query = query.where(WorkingWindowPoint.window_index > after_index)
        rows = self._points(query.order_by(WorkingWindowPoint.window_index).limit(1))
        return rows[0] if rows else None

    def working_inputs(self, schedule_id, starts):
        query = select(WorkingWindowPoint).where(
            WorkingWindowPoint.schedule_id == schedule_id,
            WorkingWindowPoint.start_utc.in_(starts))
        from sqlalchemy import Text
        size = self.session.scalar(select(func.sum(func.octet_length(
            WorkingWindowPoint.features.cast(Text)))).where(
                WorkingWindowPoint.schedule_id == schedule_id, WorkingWindowPoint.start_utc.in_(starts)))
        if (size or 0) > 64 * 1024**2:
            raise DetectionError("resource_budget:warmup_input_bytes")
        return self._points(query.order_by(WorkingWindowPoint.window_index))

    def attempt(self, attempt_id):
        return snapshot(self._row(DetectorWarmup, attempt_id))

    def _checked(self, context, attempt_id=None):
        self.check_owner(context.get("ownership"))
        expected = context["detector"]
        detector = self._row(Detector, expected["id"], True)
        if (detector.revision != expected["revision"] or detector.config_hash != expected["config_hash"]
                or detector.consumed_index != expected["consumed_index"]
                or detector.state == "stopped"
                or detector.execution_state not in {"starting", "running"}
                or detector.execution_generation != expected["execution_generation"]
                or detector.start_request_id != expected["start_request_id"]
                or detector.active_attempt_id != expected["active_attempt_id"]
                or attempt_id is not None and detector.active_attempt_id != attempt_id):
            raise StaleCandidate("stale_detector_candidate")
        detector.revision += 1
        detector.updated_at = self.session.scalar(select(func.clock_timestamp()))
        return detector

    @single_flush
    def select_attempt(self, context, attempt):
        detector = self._checked(context)
        detector.active_attempt_id = attempt["id"]
        detector.state, detector.reason = "warming", None
        self.session.add(DetectorWarmup(**attempt))
        self.session.flush()

    @single_flush
    def save_warmup_inputs(self, context, attempt_id, inputs, status, reason):
        detector = self._checked(context, attempt_id)
        attempt = self._row(DetectorWarmup, attempt_id, True)
        if attempt.status != "preparing":
            raise StaleCandidate("warmup_inputs_already_committed")
        attempt.inputs, attempt.inputs_hash = inputs, digest(inputs)
        attempt.status, attempt.reason = status, reason
        detector.state = "warming" if status == "prepared" else "waiting"
        detector.reason = reason
        if reason:
            detector.execution_state, detector.failure_code = "failed", safe_reason(reason)
            self._failure(detector, detector.failure_code, reason)
        self.session.flush()

    @single_flush
    def save_warmup(self, context, attempt_id, checkpoint, evaluation_start_index):
        detector = self._checked(context, attempt_id)
        attempt = self._row(DetectorWarmup, attempt_id, True)
        if attempt.status != "prepared" or digest(attempt.inputs) != attempt.inputs_hash:
            raise StaleCandidate("warmup_inputs_changed")
        detector.checkpoint = checkpoint
        detector.state, detector.reason = "ready", None
        detector.evaluation_start_index = evaluation_start_index
        detector.accelerated_used = detector.accelerated
        detector.execution_state, detector.failure_code = "running", None
        detector.execution_started_at = detector.updated_at
        attempt.status = "ready"
        self.session.flush()

    @single_flush
    def save_transition(self, context, point, checkpoint, outcome, finding):
        detector = self._checked(context)
        if detector.state != "ready" or detector.execution_state != "running":
            raise StaleCandidate("detector_not_ready")
        stored = self._row(WorkingWindowPoint, point["id"])
        if stored.schedule_id != context["schedule"]["id"] or stored.window_index != point["window_index"]:
            raise StaleCandidate("input_identity_changed")
        detector.checkpoint, detector.consumed_index = checkpoint, point["window_index"]
        provenance = [snapshot(row) for row in self.session.scalars(select(DetectorRecovery).where(
            DetectorRecovery.detector_id == detector.id,
            DetectorRecovery.first_index <= point["window_index"],
            DetectorRecovery.last_index >= point["window_index"]))]
        provenance = [{**r, "id": str(r["id"]), "detector_id": str(r["detector_id"]),
                       "started_at": r["started_at"].isoformat()} for r in provenance]
        outcome = {**outcome, "data": {**outcome["data"], "recovery": provenance, "late": bool(provenance)}}
        if outcome["kind"] == "evaluated":
            detector.evaluated_index = point["window_index"]
        self.session.add(DetectorOutcome(id=uuid4(), detector_id=detector.id,
            working_point_id=point["id"], window_index=point["window_index"],
            revision=detector.revision, **outcome))
        if finding is not None:
            finding = {**finding, "recovery": provenance}
            self.session.add(Finding(id=uuid4(), detector_id=detector.id,
                working_point_id=point["id"], created_at=detector.updated_at, late=bool(provenance), data=finding))
        self.session.flush()

    @single_flush
    def diagnose(self, context, state, reason):
        detector = self._checked(context)
        detector.state, detector.reason = state, safe_reason(reason)
        if state in {"error", "waiting"}:
            detector.execution_state, detector.failure_code = "failed", safe_reason(reason)
            self._failure(detector, detector.failure_code, reason)
        self.session.flush()

    @single_flush
    def stop_for_aggregate(self, aggregate_id, reason, at):
        # AggregateService already owns schedule-lock, shared with all final saves.
        rows = self.session.scalars(select(Detector).where(Detector.aggregate_id == aggregate_id)
            .order_by(Detector.id).with_for_update().execution_options(populate_existing=True))
        for detector in rows:
            if detector.state != "stopped":
                detector.state, detector.reason = "stopped", reason
                detector.execution_state = "terminated"
                detector.execution_generation += 1
                detector.revision += 1
                detector.updated_at = at
        self.session.flush()
