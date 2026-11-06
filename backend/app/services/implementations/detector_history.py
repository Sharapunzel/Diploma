"""Materialize a whole warmup batch from one repeatable, read-only snapshot."""

import time
from dataclasses import replace
from datetime import datetime

from ...aggregation.budget import estimate
from ...aggregation.results import interpret_result
from ...aggregation.rules import AggregateRule, Limits, compile_rule
from ...detection.contracts import DetectionError, canonical, feature_vector
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

    def materialize(self, context, attempt, deadline):
        def check_budget():
            if time.monotonic() >= deadline:
                raise DetectionError("resource_budget:preparation_seconds")

        plan, aggregate = attempt["plan"], context["aggregate"]
        windows = list(plan_windows(plan))
        inputs = []
        with self.uow.read_snapshot():
            if plan["step"] == plan["duration"]:
                working = self.repository.working_inputs(context["schedule"]["id"],
                                                         [left for left, _ in windows])
                if len(working) == plan["count"]:
                    inputs = [input_snapshot(point) for point in working]
            if not inputs:
                progress = self.aggregates.progress()
                first, last = windows[0][0], windows[-1][1]
                if (progress and progress.availability_verified_from_at is not None
                        and first >= progress.availability_verified_from_at):
                    basis = "availability_at"
                elif progress and progress.legacy_cutover_at is not None and last <= progress.legacy_cutover_at:
                    raise DetectionError("legacy_coverage_unproven")
                else:
                    raise DetectionError("historical_time_basis_unknown")
                barrier = self.aggregates.coverage_barrier(aggregate["source_identity"], last)
                if barrier is None:
                    raise DetectionError("barrier_pending")
                compiled = compile_rule(AggregateRule.model_validate(aggregate["rule"]),
                                        self.catalog, self.limits)
                self.aggregates.limits(self.limits, False)
                for start, end in windows:
                    if time.monotonic() >= deadline:
                        raise DetectionError("resource_budget:preparation_seconds")
                    if self.aggregates.markerless(aggregate["source_identity"], start, end):
                        raise DetectionError("availability_boundary_violation")
                    remaining = int((deadline - time.monotonic()) * 1000)
                    if remaining <= 0:
                        raise DetectionError("resource_budget:preparation_seconds")
                    self.aggregates.limits(replace(self.limits,
                        working_timeout_ms=min(self.limits.working_timeout_ms, remaining)), False)
                    rows, total, width = self.aggregates.estimate_rows(
                        aggregate["source_identity"], start, end, basis)
                    estimate(compiled, rows, total, plan["duration"], self.limits, False, width)
                    remaining = int((deadline - time.monotonic()) * 1000)
                    if remaining <= 0:
                        raise DetectionError("resource_budget:preparation_seconds")
                    self.aggregates.limits(replace(self.limits,
                        working_timeout_ms=min(self.limits.working_timeout_ms, remaining)), False)
                    values = interpret_result(self.aggregates.calculate(
                        aggregate["source_identity"], start, end, basis, compiled), compiled, plan["duration"])
                    estimate(compiled, values.pop("observed_rows"), total, plan["duration"],
                             self.limits, False, values.pop("observed_width"))
                    completeness, reason, refs = evaluate_saved_coverage(self.evidence,
                        aggregate["source_identity"], start, end, barrier.availability_at)
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
