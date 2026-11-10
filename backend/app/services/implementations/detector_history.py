"""Materialize a whole warmup batch from one repeatable, read-only snapshot."""

import time
from dataclasses import replace
from datetime import datetime
from uuid import uuid5

from ...aggregation.budget import estimate
from ...aggregation.results import interpret_result
from ...aggregation.rules import AggregateRule, Limits, compile_rule
from ...detection.contracts import DetectionError, StaleCandidate, canonical, feature_vector
from ...detection.history import plan_windows
from ...detection.numeric import Affine, NumericQualityError
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.aggregates import AggregateRepository
from ...repositories.protocols.detectors import DetectorRepository
from .saved_coverage import evaluate_saved_coverage


def input_snapshot(point, basis="availability_at"):
    return {"point_id": str(point["id"]) if point.get("id") else None,
            "start": point["start_utc"].isoformat(), "end": point["end_utc"].isoformat(),
            "features": point["features"], "completeness": point["completeness"],
            "reason": point["completeness_reason"], "evidence": point["evidence_refs"],
            "basis": basis, "event_count": point["event_count"],
            "event_time_min": point["event_time_min"].isoformat() if point["event_time_min"] else None,
            "event_time_max": point["event_time_max"].isoformat() if point["event_time_max"] else None}


class DetectorHistoryService:
    def __init__(self, repository: DetectorRepository, aggregates: AggregateRepository,
                 evidence, uow: UnitOfWork, catalog, limits=Limits()):
        self.repository, self.aggregates, self.evidence = repository, aggregates, evidence
        self.uow, self.catalog, self.limits = uow, catalog, limits

    def _read_budget(self, deadline):
        remaining = int((deadline - time.monotonic()) * 1000)
        if remaining <= 0:
            raise DetectionError("resource_budget:preparation_seconds")
        self.aggregates.read_budget(remaining)

    def _calculation_budget(self, deadline):
        self._read_budget(deadline)
        remaining = int((deadline - time.monotonic()) * 1000)
        if remaining <= 0:
            raise DetectionError("resource_budget:preparation_seconds")
        self.aggregates.limits(replace(self.limits,
            working_timeout_ms=min(self.limits.working_timeout_ms, remaining)), False)

    def _basis(self, first, last):
        progress = self.aggregates.progress()
        if (progress and progress.availability_verified_from_at is not None
                and first >= progress.availability_verified_from_at):
            return "availability_at"
        if progress and progress.legacy_cutover_at is not None and last <= progress.legacy_cutover_at:
            raise DetectionError("legacy_coverage_unproven")
        raise DetectionError("historical_time_basis_unknown")

    def _snapshot_metadata(self, context, attempt, deadline):
        self._read_budget(deadline)
        fresh, saved = self.repository.history_metadata(context["detector"]["id"], attempt["id"])
        if (fresh["aggregate"]["state"] != "active"
                or any(fresh["detector"][key] != context["detector"][key] for key in (
                    "revision", "config_hash", "execution_generation", "active_attempt_id"))):
            raise StaleCandidate("stale_detector_candidate")
        if saved["plan"] != attempt["plan"] or saved["cutoff"] != attempt["cutoff"]:
            raise StaleCandidate("stale_warmup_plan")
        return fresh["aggregate"]

    def _ready(self, source_id, attempt, deadline):
        """Short READ COMMITTED probes; no snapshot or lifecycle lock survives."""
        try:
            self._read_budget(deadline)
            barrier = self.aggregates.history_barrier(
                uuid5(attempt["id"], "warmup"), source_id, attempt["cutoff"])
            if barrier is None:
                return False
            self._read_budget(deadline)
            reference = self.aggregates.now()
            self._read_budget(deadline)
            return self.evidence.checkpoint(source_id, None, attempt["cutoff"],
                                            barrier.availability_at, reference) is not None
        finally:
            self.uow.rollback()

    def materialize(self, context, attempt, deadline):
        def check_budget():
            if time.monotonic() >= deadline:
                raise DetectionError("resource_budget:preparation_seconds")

        plan, aggregate = attempt["plan"], context["aggregate"]
        windows = list(plan_windows(plan))
        inputs = []
        with self.uow.read_snapshot():
            aggregate = self._snapshot_metadata(context, attempt, deadline)
            self._read_budget(deadline)
            if plan["step"] == plan["duration"]:
                working = self.repository.working_inputs(context["schedule"]["id"],
                    [left for left, _ in windows], before_read=lambda: self._read_budget(deadline))
                # A closed unknown point keeps its original barrier forever.
                # Reuse only a covered batch; otherwise recompute the whole
                # history against this attempt's barrier without editing points.
                if (len(working) == plan["count"]
                        and all(point["completeness"] == "complete" for point in working)):
                    inputs = [input_snapshot(point) for point in working]
            if not inputs:
                self._read_budget(deadline)
                self._basis(windows[0][0], windows[-1][1])
        while not inputs:
            while not self._ready(aggregate["source_identity"], attempt, deadline):
                check_budget()
                time.sleep(min(.05, max(0, deadline - time.monotonic())))
            # A preflight authorizes a fresh snapshot, never a partial batch or
            # a proof imported from another transaction. Retry only readiness
            # races, keeping the same attempt, cutoff, barrier and deadline.
            with self.uow.read_snapshot():
                self._read_budget(deadline)
                reference = self.aggregates.now()
                aggregate = self._snapshot_metadata(context, attempt, deadline)
                self._read_budget(deadline)
                basis = self._basis(windows[0][0], windows[-1][1])
                self._read_budget(deadline)
                barrier = self.aggregates.history_barrier(
                    uuid5(attempt["id"], "warmup"), aggregate["source_identity"], attempt["cutoff"])
                if barrier is None:
                    continue
                self._read_budget(deadline)
                if self.evidence.checkpoint(aggregate["source_identity"], None,
                        attempt["cutoff"], barrier.availability_at, reference) is None:
                    continue
                compiled = compile_rule(AggregateRule.model_validate(aggregate["rule"]),
                                        self.catalog, self.limits)
                for start, end in windows:
                    self._read_budget(deadline)
                    if self.aggregates.markerless(aggregate["source_identity"], start, end):
                        raise DetectionError("availability_boundary_violation")
                    rows, total, width = self.aggregates.estimate_rows(
                        aggregate["source_identity"], start, end, basis,
                        before_read=lambda: self._calculation_budget(deadline))
                    estimate(compiled, rows, total, plan["duration"], self.limits, False, width)
                    self._calculation_budget(deadline)
                    values = interpret_result(self.aggregates.calculate(
                        aggregate["source_identity"], start, end, basis, compiled), compiled, plan["duration"])
                    estimate(compiled, values.pop("observed_rows"), total, plan["duration"],
                             self.limits, False, values.pop("observed_width"))
                    completeness, reason, refs = evaluate_saved_coverage(self.evidence,
                        aggregate["source_identity"], start, end, barrier.availability_at,
                        reference_at=reference, check_budget=lambda: self._read_budget(deadline))
                    if reason == "kafka_checkpoint_unavailable":
                        inputs = []
                        break
                    inputs.append(input_snapshot({"start_utc": start, "end_utc": end,
                        **values, "completeness": completeness, "completeness_reason": reason,
                        "evidence_refs": refs}, basis))
                    if len(canonical(inputs).encode()) > 64 * 1024**2:
                        raise DetectionError("resource_budget:warmup_input_bytes")
        if time.monotonic() >= deadline:
            raise DetectionError("resource_budget:preparation_seconds")
        if len(canonical(inputs).encode()) > 64 * 1024**2:
            raise DetectionError("resource_budget:warmup_input_bytes")
        reason = None
        for item in inputs:
            check_budget()
            try:
                item["exact"] = feature_vector(item["features"], context["detector"]["feature_keys"],
                                               item["completeness"])
            except ValueError as error:
                reason = str(error)
                item["quality_reason"] = reason
        actual_coverage = (datetime.fromisoformat(inputs[-1]["end"])
                           - datetime.fromisoformat(inputs[0]["start"])).total_seconds()
        if context["detector"]["accelerated"] and actual_coverage < 8 * plan["duration"]:
            reason = "insufficient_actual_coverage"
        if reason is None:
            try:
                transform = Affine.fit([item["exact"] for item in inputs], check_budget=check_budget)
                inputs[0]["affine"] = {"algorithm": "min_range_decimal_float32_v1",
                    "center": [str(x) for x in transform.center],
                    "scale": [str(x) for x in transform.scale]}
                constant_coordinates = []
                for i, key in enumerate(context["detector"]["feature_keys"]):
                    for item in inputs:
                        check_budget()
                        if item["exact"][i] != inputs[0]["exact"][i]:
                            break
                    else:
                        constant_coordinates.append(key)
                inputs[0]["constant_coordinates"] = constant_coordinates
                for item in inputs:
                    item["ml_input"] = list(transform.convert(item["exact"], check_budget=check_budget))
            except NumericQualityError as error:
                reason = str(error)
        if len(canonical(inputs).encode()) > 64 * 1024**2:
            raise DetectionError("resource_budget:warmup_input_bytes")
        if time.monotonic() >= deadline:
            raise DetectionError("resource_budget:preparation_seconds")
        return inputs, reason
