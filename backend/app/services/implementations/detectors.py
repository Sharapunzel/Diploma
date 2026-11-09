"""Candidate calculation outside locks; final CAS under the aggregate mutex."""

import time
from dataclasses import asdict
from datetime import datetime, timedelta
from functools import wraps
from uuid import uuid4, uuid5

from sqlalchemy.exc import DBAPIError

from ...aggregation.rules import RuleError
from ...detection.contracts import (
    DetectionError,
    ModelConfig,
    ModelFactory,
    StaleCandidate,
    digest,
    feature_vector,
)
from ...detection.execution import REASONS, require_owner
from ...detection.history import warmup_plan, warmup_summary
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.detectors import DetectorRepository


def transaction(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception:
            # No changed Java instance is retained or retried after rollback/commit uncertainty.
            self.uow.rollback()
            raise
    return run


def active(context):
    if context["aggregate"]["state"] != "active":
        raise DetectionError("aggregate_inactive")


def unarchived_start(context, index):
    schedule = context["schedule"]
    left = schedule["anchor_utc"] + timedelta(seconds=index * schedule["duration_seconds"])
    for span in sorted(context["aggregate"]["skipped_ranges"], key=lambda s: s["start_utc"]):
        start, end = datetime.fromisoformat(span["start_utc"]), datetime.fromisoformat(span["end_utc"])
        if span["reason"] == "intentional_archive" and start <= left < end:
            left = max(left, end)
    return left


class DetectorService:
    def __init__(self, repository: DetectorRepository, history, windows, capture,
                 uow: UnitOfWork, models: ModelFactory, owner=None):
        self.repository, self.history, self.windows = repository, history, windows
        self.capture, self.uow, self.models = capture, uow, models
        self.owner = owner

    def _execution(self, context):
        require_owner(self.owner)
        self.repository.check_owner(self.owner)
        if context["detector"]["execution_state"] not in {"starting", "running"}:
            raise DetectionError("execution_not_authorized")
        context["ownership"] = self.owner
        self.uow.rollback()
        return context

    def _lock(self, context, attempt_id=None, *, failure=False):
        self.repository.check_owner(self.owner)
        current = self.repository.lock_context(context["detector"]["id"])
        if (not failure and context["detector"]["execution_state"] == "running"
                and current["aggregate"]["state"] == "archived"):
            # Archive invalidates this candidate, not the existing permission.
            raise StaleCandidate("aggregate_archived_during_execution")
        if not failure:
            active(current)
        old, new = context["detector"], current["detector"]
        if (new["revision"] != old["revision"] or new["config_hash"] != old["config_hash"]
                or new["consumed_index"] != old["consumed_index"]
                or new["execution_generation"] != old["execution_generation"]
                or new["start_request_id"] != old["start_request_id"]
                or new["active_attempt_id"] != old["active_attempt_id"]
                or new["execution_state"] not in ({"requested", "starting", "running"} if failure else {"starting", "running"})
                or attempt_id is not None and new["active_attempt_id"] != attempt_id):
            raise StaleCandidate("stale_detector_candidate")
        current["ownership"] = self.owner
        return current

    def _commit_candidate(self):
        # Recheck the physical session lock at the final transaction boundary,
        # including a connection lost after flush. The coordinator row remains
        # locked until commit, serializing this save against takeover publication.
        self.repository.check_owner(self.owner)
        self.uow.commit()

    @transaction
    def start(self, detector_id):
        context = self.repository.lock_context(detector_id)
        if context["aggregate"]["state"] == "deleted" or context["detector"]["state"] == "stopped":
            raise DetectionError("aggregate_deleted")
        active(context)
        detector = context["detector"]
        if detector["execution_state"] not in {"requested", "starting", "running"}:
            if detector["checkpoint"] is not None and detector["execution_state"] == "stopped":
                self.repository.record_recovery(context, "manual_resume", 0)
            self.repository.control(context, "requested", new_request=True)
        self.uow.commit()
        return self.state(detector_id)

    @transaction
    def stop(self, detector_id):
        context = self.repository.lock_context(detector_id)
        if context["detector"]["execution_state"] not in {"stopped", "terminated"}:
            self.repository.control(context, "stopped", "manual_stop")
        self.uow.commit()
        return self.state(detector_id)

    @transaction
    def acquire_owner(self, owner_id, backend_pid):
        owner = self.repository.acquire_owner(owner_id, backend_pid)
        self.uow.commit()
        return owner

    @transaction
    def recover(self, detector_id):
        self.repository.check_owner(self.owner)
        context = self.repository.lock_context(detector_id)
        state = context["detector"]["execution_state"]
        if state == "starting":
            self.repository.control(context, "failed", "start_interrupted")
        elif state == "running":
            self.repository.record_recovery(context, "crash_recovery", self.owner.generation)
        self._commit_candidate()

    @transaction
    def begin_start(self, detector_id):
        self.repository.check_owner(self.owner)
        context = self.repository.lock_context(detector_id)
        active(context)
        if context["detector"]["execution_state"] != "requested":
            self.uow.rollback()
            return {"state": "unchanged"}
        self.repository.control(context, "starting")
        self._commit_candidate()
        return {"state": "starting"}

    @transaction
    def resume(self, detector_id):
        context = self._execution(self.repository.read_context(detector_id))
        detector = context["detector"]
        active(context)
        if detector["checkpoint"] is None:
            raise DetectionError("checkpoint_invalid")
        self.models.restore(detector["checkpoint"], detector["config"], detector["feature_keys"], detector["rule_hash"])
        current = self._lock(context)
        self.repository.record_recovery(current, "manual_resume", self.owner.generation)
        self.repository.control(current, "running")
        self._commit_candidate()
        return {"state": "running"}

    @transaction
    def fail(self, detector_id, reason, expected=None):
        context = expected or self._execution(self.repository.read_context(detector_id))
        current = self._lock(context, failure=True)
        if current["detector"]["execution_state"] in {"requested", "starting"}:
            self.repository.control(current, "failed", reason)
        else:
            self.repository.diagnose(current, "error", reason)
        self._commit_candidate()

    @transaction
    def candidates(self, after=None, limit=100):
        result = self.repository.candidates(after, limit)
        self.uow.rollback()
        return result

    def _model_error(self, context, reason):
        """Record a failure only if this candidate still owns the same revision."""
        self.uow.rollback()
        if str(reason) == "ownership_lost":
            return
        try:
            current = self._lock(context)
        except DetectionError:
            self.uow.rollback()
            return  # Deletion or a newer success/attempt wins over this diagnostic.
        self.repository.diagnose(current, "error", reason)
        self._commit_candidate()

    @transaction
    def create(self, aggregate_id, actor, name, config=None, accelerated=False, step=None):
        context = self.repository.aggregate_context(aggregate_id, True)
        active(context)
        aggregate, schedule = context["aggregate"], context["schedule"]
        if aggregate["source_type"] != "kafka":
            raise DetectionError("own_source_required")
        features = aggregate["rule"]["features"]
        keys = [feature["key"] for feature in features]
        config = ModelConfig(**({"dimensions": len(keys)} | (config or {})))
        if config.dimensions != len(keys) or len(set(keys)) != len(keys):
            raise DetectionError("feature_order_or_dimension")
        if not isinstance(name, str) or not name.strip() or len(name) > 200:
            raise DetectionError("invalid_detector_name")
        if type(accelerated) is not bool:
            raise DetectionError("invalid_warmup_mode")
        duration = schedule["duration_seconds"]
        step = step if accelerated else duration
        if type(step) is not int or not 0 < step <= duration:
            raise DetectionError("invalid_warmup_step")
        if max(config.minimum_inputs, 1 + (7 * duration + step - 1) // step) > 4096:
            raise DetectionError("resource_budget:warmup_windows")
        identity = uuid4()
        self.repository.create({"id": identity, "aggregate_id": aggregate_id, "name": name,
            "created_by": actor, "created_at": context["now"], "updated_at": context["now"],
            "config": asdict(config), "config_hash": digest(asdict(config)), "feature_keys": keys,
            "feature_names": [f["name"] for f in features], "rule_hash": aggregate["rule_hash"],
            "state": "waiting", "reason": "warmup_required", "revision": 0,
            "accelerated": accelerated, "step_seconds": step, "accelerated_used": False})
        self.uow.commit()
        return identity

    @transaction
    def state(self, detector_id):
        with self.uow.read_snapshot():
            context = self.repository.read_context(detector_id)
            attempt_id = context["detector"]["active_attempt_id"]
            if attempt_id is not None:
                attempt = self.repository.attempt(attempt_id)
                context["warmup"] = {**attempt, "summary": warmup_summary(attempt, context["detector"])}
            context["recovery"] = self.repository.recovery(detector_id)
            detector, schedule = context["detector"], context["schedule"]
            index = (detector["consumed_index"] + 1 if detector["consumed_index"] is not None
                     else detector["evaluation_start_index"])
            scheduled = (unarchived_start(context, index) + timedelta(seconds=schedule["duration_seconds"])
                         if index is not None else None)
            code = detector["failure_code"] or ("manual_stop" if detector["execution_state"] == "stopped" else
                "aggregate_deleted" if detector["execution_state"] == "terminated" else
                "warmup_required" if detector["execution_state"] == "created" else None)
            point = (self.repository.next_working_point(schedule["id"], detector["consumed_index"],
                     detector["evaluation_start_index"]) if index is not None else None)
            if code is None and context["aggregate"]["state"] != "active":
                code = "aggregate_inactive"
            if code is None and detector["execution_state"] == "running" and point is None:
                code = "working_point_pending"
            if code is None and detector["execution_state"] == "running" and point is not None:
                if unarchived_start(context, index) < point["start_utc"]:
                    code = "working_point_gap"
                elif point["end_utc"] > context["now"]:
                    code = "working_point_not_completed"
            context["diagnostics"] = {"code": code, "message": REASONS.get(code),
                "last_failure": detector["last_failure"],
                "next_scheduled_at": scheduled,
                "backlog": max(0, int((context["now"] - scheduled).total_seconds()) // schedule["duration_seconds"] + 1)
                           if scheduled is not None and context["now"] >= scheduled else 0,
                "lag_seconds": max(0, (context["now"] - scheduled).total_seconds()) if scheduled else None,
                "waiting_for_point": index is not None and point is None,
                "next_point": point,
                "last_consumed": self.repository.last_result(detector_id, detector["consumed_index"]),
                "last_evaluated": self.repository.last_result(detector_id, detector["evaluated_index"])}
        return context

    @transaction
    def prepare_warmup(self, detector_id):
        deadline = time.monotonic() + 30
        context = self._execution(self.repository.read_context(detector_id))
        active(context)
        detector, schedule = context["detector"], context["schedule"]
        if detector["checkpoint"] is not None or detector["state"] == "stopped":
            raise DetectionError("warmup_already_completed_or_stopped")
        attempt = self.repository.attempt(detector["active_attempt_id"]) if detector["active_attempt_id"] else None
        # A new explicit request gets a new attempt; a pending barrier within the
        # same accepted request keeps its immutable cutoff and batch identity.
        reuse = (attempt is not None and attempt["status"] == "preparing"
                 and attempt["created_at"] >= detector["start_requested_at"])
        if reuse:
            plan, attempt_id = attempt["plan"], attempt["id"]
        else:
            try:
                plan = warmup_plan(schedule["anchor_utc"], schedule["duration_seconds"], context["now"],
                                   ModelConfig(**detector["config"]).minimum_inputs, detector["step_seconds"],
                                   detector["accelerated"])
            except DetectionError as error:
                current = self._lock(context)
                self.repository.diagnose(current, "waiting", str(error))
                self._commit_candidate()
                return {"state": "waiting", "reason": str(error)}
            attempt_id = uuid4()
            attempt = {"id": attempt_id, "detector_id": detector_id,
                "cutoff": datetime.fromisoformat(plan["cutoff"]), "plan": plan,
                "inputs": [], "inputs_hash": digest([]), "status": "preparing", "reason": None,
                "created_at": context["now"]}
        self.uow.rollback()
        self.repository.check_owner(self.owner)
        owner = self.repository.aggregate_context(context["aggregate"]["id"], True)
        active(owner)
        # Barrier phase: schedule -> Source -> Detector, all in the same UoW.
        if not self.windows.lock_source(owner["aggregate"]["source_identity"]):
            raise DetectionError("source_unavailable")
        current = self._lock(context)
        if not reuse:
            self.repository.select_attempt(current, attempt)
        marker_id = uuid5(attempt_id, "warmup")
        self.windows.ensure_barrier(marker_id, owner["aggregate"]["source_identity"], current["now"])
        self._commit_candidate()
        context = self.repository.read_context(detector_id)
        self.uow.rollback()
        # Already saved working snapshots need no new WAL materialization.
        reason, inputs = None, []
        try:
            try:
                inputs, reason = self.history.materialize(context, attempt, deadline)
            except DetectionError as error:
                if str(error) != "barrier_pending":
                    raise
                while time.monotonic() < deadline:
                    status = self.capture.status()
                    barrier = self.windows.barrier(marker_id)
                    ready = status.ready and barrier and barrier.availability_at is not None
                    self.uow.rollback()
                    if ready:
                        break
                    time.sleep(.05)
                inputs, reason = self.history.materialize(context, attempt, deadline)
        except StaleCandidate:
            raise
        except DetectionError as error:
            reason = str(error)
        except RuleError as error:
            # This attempt was already committed before materialization. Record
            # confirmed input/budget failures against that refreshed revision.
            reason = (f"resource_budget:{error.path}"
                      if error.code == "resource_budget_exceeded" else "history_quality")
        except DBAPIError as error:
            if getattr(error.orig, "sqlstate", None) != "57014":
                raise  # Infrastructure uncertainty is resolved by re-election.
            reason = "resource_budget:preparation_seconds"
        self.uow.rollback()
        if reason == "barrier_pending":
            self.uow.rollback()
            return {"attempt_id": attempt_id, "state": "waiting", "reason": reason}
        current = self._lock(context, attempt_id)
        self.repository.save_warmup_inputs(current, attempt_id, inputs,
                                           "waiting" if reason else "prepared", reason)
        self._commit_candidate()
        return {"attempt_id": attempt_id, "state": "waiting" if reason else "prepared",
                "reason": reason, "inputs": len(inputs), "plan": plan}

    @transaction
    def warmup(self, detector_id):
        context = self._execution(self.repository.read_context(detector_id))
        active(context)
        detector = context["detector"]
        if detector["checkpoint"] is not None:
            raise DetectionError("warmup_already_completed")
        if detector["active_attempt_id"] is None:
            raise DetectionError("warmup_inputs_required")
        attempt = self.repository.attempt(detector["active_attempt_id"])
        if attempt["status"] != "prepared" or digest(attempt["inputs"]) != attempt["inputs_hash"]:
            raise DetectionError("warmup_inputs_not_prepared")
        self.uow.rollback()
        rows = [feature_vector(item["features"], detector["feature_keys"], item["completeness"])
                for item in attempt["inputs"]]
        try:
            checkpoint = self.models.warmup(detector["config"], detector["feature_keys"], detector["rule_hash"], rows)
            if checkpoint["body"]["affine"] != attempt["inputs"][0]["affine"]:
                raise DetectionError("warmup_transform_mismatch")
        except StaleCandidate:
            raise
        except ValueError as error:
            current = self._lock(context, attempt["id"])
            self.repository.diagnose(current, "waiting", str(error))
            self._commit_candidate()
            return {"state": "waiting", "reason": str(error)}
        current = self._lock(context, attempt["id"])
        self.repository.save_warmup(current, attempt["id"], checkpoint, attempt["plan"]["evaluation_start_index"])
        self._commit_candidate()
        return {"state": "ready"}

    @transaction
    def advance_one(self, detector_id):
        context = self._execution(self.repository.read_context(detector_id))
        detector, schedule, aggregate = context["detector"], context["schedule"], context["aggregate"]
        if detector["execution_state"] != "running":
            raise DetectionError("execution_not_authorized")
        if aggregate["state"] != "active" or detector["state"] != "ready":
            self.uow.rollback()
            return {"state": "waiting", "reason": "inactive_or_not_ready"}
        point = self.repository.next_working_point(schedule["id"], detector["consumed_index"],
                                                   detector["evaluation_start_index"])
        self.uow.rollback()
        if point is None:
            return {"state": "waiting", "reason": "working_point_pending"}
        if point["end_utc"] > context["now"]:
            return {"state": "waiting", "reason": "working_point_not_completed"}
        expected = (detector["consumed_index"] + 1 if detector["consumed_index"] is not None
                    else detector["evaluation_start_index"])
        # Every missing index must be covered by an intentional archived range.
        left = unarchived_start(context, expected)
        right = point["start_utc"]
        if left < right:
            return {"state": "waiting", "reason": "working_point_gap"}
        reason, result, finding = None, None, None
        checkpoint = detector["checkpoint"]
        try:
            model = self.models.restore(checkpoint, detector["config"], detector["feature_keys"], detector["rule_hash"])
        except DetectionError as error:
            self._model_error(context, error)
            raise
        try:
            values = feature_vector(point["features"], detector["feature_keys"], point["completeness"])
            # Always restore a fresh model; never reuse a process-mutated instance.
            result = model.process(values)
            checkpoint = model.checkpoint(str(point["id"]))
            if result["grade"] > 0:
                finding = {**result, "exact_values": values, "feature_keys": detector["feature_keys"],
                    "feature_names": detector["feature_names"], "detector_name": detector["name"],
                    "aggregate_name": aggregate["name"], "source_name": aggregate["source_name"],
                    "source_id": str(aggregate["source_identity"]), "rule_hash": detector["rule_hash"],
                    "start": point["start_utc"].isoformat(), "end": point["end_utc"].isoformat(),
                    "event_time_min": point["event_time_min"].isoformat() if point["event_time_min"] else None,
                    "event_time_max": point["event_time_max"].isoformat() if point["event_time_max"] else None,
                    "event_count": point["event_count"],
                    "event_range_reason": ("empty_window" if point["event_count"] == 0 else
                        "unavailable" if point["event_time_min"] is None or point["event_time_max"] is None else None),
                    "accelerated_used": detector["accelerated_used"], "origin": "working_point"}
        except ValueError as error:
            from ...detection.numeric import NumericQualityError
            if isinstance(error, NumericQualityError) or str(error).startswith(("coverage:", "feature_")):
                reason = str(error)
            else:
                self._model_error(context, error)
                raise
        current = self._lock(context)
        self.repository.save_transition(current, point, checkpoint,
            {"kind": "skipped" if reason else "evaluated", "reason": reason,
             "data": {"start": point["start_utc"].isoformat(), "end": point["end_utc"].isoformat(),
                      "actual_started_at": context["now"].isoformat(),
                      "actual_completed_at": current["now"].isoformat(),
                      "features": point["features"], "result": result}}, finding)
        self._commit_candidate()
        return {"state": "skipped" if reason else "evaluated", "reason": reason,
                "point_id": point["id"], "finding": finding}
