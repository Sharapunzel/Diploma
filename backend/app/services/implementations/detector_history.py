"""Warmup uses closed canonical snapshots; overlap never imports late events."""

import time
from datetime import datetime, timedelta

from ...aggregation.budget import estimate
from ...aggregation.event_time import EventTimeBudgetError, OperationBudget, warmup_indices
from ...aggregation.results import interpret_result
from ...aggregation.rules import AggregateRule, Limits, compile_rule
from ...detection.contracts import DetectionError, StaleCandidate, canonical, feature_vector
from ...detection.history import plan_windows
from ...detection.numeric import Affine, NumericQualityError
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.aggregates import AggregateRepository
from ...repositories.protocols.detectors import DetectorRepository


def input_snapshot(point, basis="event_time"):
    if point.get("calculation_status", "computed") != "computed":
        raise DetectionError("warmup_window_uncomputed")
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

    def plan(self, context, deadline):
        with self.uow.read_snapshot():
            self.aggregates.read_budget(max(1, int((deadline-time.monotonic())*1000)))
            last = self.aggregates.canonical_last_index(context["schedule"]["id"])
            if last is None:
                raise DetectionError("insufficient_history")
            d, s = context["detector"], context["schedule"]
            try:
                plan = warmup_indices(last, s["duration_seconds"], d["step_seconds"],
                    d["config"]["sample_size"], d["config"]["output_after"], d["accelerated"])
            except EventTimeBudgetError as error:
                raise DetectionError("resource_budget:warmup_windows") from error
            if not plan["complete"]:
                raise DetectionError("insufficient_history")
            self.aggregates.read_budget(max(1, int((deadline-time.monotonic())*1000)))
            closures = self.aggregates.warmup_closures(s["id"], plan["base_first"], plan["base_last"])
            if len(closures) != plan["base_last"] - plan["base_first"] + 1:
                raise DetectionError("warmup_canonical_coverage_missing")
        cutoff = s["anchor_utc"] + timedelta(seconds=plan["cutoff_seconds"])
        return {"cutoff": cutoff.isoformat(),
            "first_start": (s["anchor_utc"] + timedelta(seconds=plan["starts"][0])).isoformat(),
            "count": plan["count"], "step": d["step_seconds"], "duration": s["duration_seconds"],
            "coverage_seconds": s["duration_seconds"] + (plan["count"]-1)*d["step_seconds"],
            "evaluation_start_index": max(s["online_start_index"], last+1), "base_closures": closures}

    def revalidate_plan(self, context, plan, deadline):
        """Called while schedule -> detector locks still protect publication."""
        anchor, duration = context["schedule"]["anchor_utc"], plan["duration"]
        first = (datetime.fromisoformat(plan["first_start"]) - anchor) // timedelta(seconds=duration)
        last = (datetime.fromisoformat(plan["cutoff"]) - anchor) // timedelta(seconds=duration) - 1
        self.aggregates.read_budget(max(1, int((deadline-time.monotonic())*1000)))
        actual = self.aggregates.warmup_closures(context["schedule"]["id"], first, last)
        if len(actual) != last-first+1 or ("base_closures" in plan and actual != plan["base_closures"]):
            raise StaleCandidate("warmup_plan_payload_changed")

    def materialize(self, context, attempt, deadline):
        budget = OperationBudget(max(0, deadline-time.monotonic()))
        def check_budget():
            try:
                budget.check()
            except EventTimeBudgetError as error:
                raise DetectionError("resource_budget:preparation_seconds") from error
        plan, aggregate = attempt["plan"], context["aggregate"]
        windows = list(plan_windows(plan))
        inputs = []
        with self.uow.read_snapshot():
            self.aggregates.read_budget(budget.milliseconds())
            fresh, saved = self.repository.history_metadata(context["detector"]["id"], attempt["id"])
            if (fresh["aggregate"]["execution_mode"] != "current"
                    or fresh["aggregate"]["state"] != "active" or fresh["schedule"]["paused"]
                    or fresh["schedule"]["active_cleanup_id"]
                    or fresh["schedule"]["execution_generation"] != context["schedule"]["execution_generation"]
                    or fresh["detector"]["revision"] != context["detector"]["revision"]
                    or saved["plan"] != plan):
                raise StaleCandidate("stale_warmup_candidate")
            if plan["step"] == plan["duration"]:
                working = self.repository.working_inputs(context["schedule"]["id"],
                    [left for left, _ in windows], before_read=lambda: self.aggregates.read_budget(budget.milliseconds()))
                if len(working) != plan["count"]:
                    raise DetectionError("warmup_canonical_coverage_missing")
                inputs = [input_snapshot(point, point["time_basis"]) for point in working]
            else:
                compiled = compile_rule(AggregateRule.model_validate(aggregate["rule"]), self.catalog, self.limits)
                anchor = context["schedule"]["anchor_utc"]
                for start, end in windows:
                    first = (start-anchor)//timedelta(seconds=plan["duration"])
                    last = (end-anchor-timedelta(microseconds=1))//timedelta(seconds=plan["duration"])
                    statistics, identities = self.aggregates.overlap_statistics(context["schedule"]["id"],
                        start, end, list(range(first,last+1)), compiled, budget)
                    estimate(compiled, statistics.observed_rows, statistics.observed_rows,
                        plan["duration"], self.limits, False, statistics.observed_width)
                    values = interpret_result(statistics, compiled, plan["duration"], check_budget=check_budget)
                    values.pop("observed_width")
                    values.pop("observed_rows")
                    inputs.append(input_snapshot({"start_utc": start,"end_utc": end, **values,
                        "completeness": "complete", "completeness_reason": "closed_membership_overlap",
                        "evidence_refs": {"base_closures": [str(x) for x in identities]}}, "event_time"))
                    check_budget()
                    if len(canonical(inputs).encode()) > 64*1024**2:
                        raise DetectionError("resource_budget:warmup_input_bytes")
        check_budget()
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
