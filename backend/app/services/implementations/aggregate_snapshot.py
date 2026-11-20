"""One candidate from a single read-only repeatable-read transaction."""

from datetime import timedelta
from time import perf_counter

from ...aggregation.budget import estimate
from ...aggregation.event_time import MAX_MEMORY_BYTES, MAX_SNAPSHOT_BYTES, EventTimeBudgetError
from ...aggregation.received_snapshot import received_quality
from ...aggregation.results import interpret_result
from ...aggregation.rules import AggregateRule, compile_rule
from ...core.errors import DomainError


def coordinate_memory(coordinates, base, multiplier, budget):
    total = 0
    for index, coordinate in enumerate(coordinates):
        if index % 128 == 0:
            budget.check()
        total += base + multiplier * len((coordinate.get("topic") or "").encode())
    budget.check()
    return total


class AggregateSnapshotService:
    def __init__(self, repository, uow, catalog, limits):
        self.repository, self.uow, self.catalog, self.limits = repository, uow, catalog, limits

    def build(self, source_id, rule, start, end, duration, origin, cohort, budget, preview=False):
        began = perf_counter()
        timings = {}
        with self.uow.read_snapshot():
            self.repository.read_budget(budget.milliseconds())
            reference = self.repository.now()
            self.repository.read_budget(budget.milliseconds())
            if self.repository.source(source_id) is None:
                raise DomainError("source_unavailable", "Источник больше недоступен", 409)
            phase = perf_counter()
            budget.check()
            compiled = compile_rule(AggregateRule.model_validate(rule), self.catalog, self.limits)
            budget.check()
            timings["compile"] = perf_counter() - phase
            self.repository.limits(self.limits, preview, budget=budget)
            self.repository.read_budget(budget.milliseconds())
            manifest, coordinates, rows, width = self.repository.snapshot_events(
                source_id, start, end, reference, budget, preview=preview, timings=timings)
            total = self.repository.event_source_rows(source_id, budget)
            cost = estimate(compiled, rows, total, duration, self.limits, preview, width)
            cohort_memory = coordinate_memory(coordinates, 1536, 4, budget)
            if cost["estimated_memory_bytes"] + rows * 512 + cohort_memory > MAX_MEMORY_BYTES:
                raise EventTimeBudgetError("total_memory")
            phase = perf_counter()
            statistics = self.repository.event_statistics(source_id, start, end, reference, compiled, budget)
            timings["statistics"] = perf_counter() - phase
            phase = perf_counter()
            values = interpret_result(statistics, compiled, duration, check_budget=budget.check)
            timings["interpret"] = perf_counter() - phase
            estimate(compiled, values["observed_rows"], total, duration, self.limits, preview,
                values.pop("observed_width"))
            if origin == "initial_history" and cohort is not None:
                load_start, load_end = cohort["load_start"], cohort["load_end"]
                if coordinates:
                    load_start = min(load_start, *(c["load_start"] for c in coordinates))
                    load_end = max(load_end, *(c["load_end"] for c in coordinates))
            elif origin == "initial_history" and coordinates:
                load_start = min(c["load_start"] for c in coordinates)
                load_end = max(c["load_end"] for c in coordinates)
            else:
                load_start, load_end = start, reference
            proof_coordinates = list(coordinates)
            phase = perf_counter()
            if origin in {"initial_history", "preview"}:
                remaining_memory = MAX_MEMORY_BYTES - cost["estimated_memory_bytes"] - rows * 512
                remaining_memory -= coordinate_memory(proof_coordinates, 2048, 8, budget)
                proof_coordinates.extend(self.repository.event_cohorts(
                    source_id, start, end, reference, budget, remaining_memory))
            timings["cohorts"] = perf_counter() - phase
            if cohort and cohort.get("coordinates"):
                proof_coordinates.append(cohort["coordinates"])
            if proof_coordinates:
                if origin == "preview":
                    load_start = min(c["load_start"] for c in proof_coordinates)
                    load_end = max(c["load_end"] for c in proof_coordinates)
                else:
                    load_start = min(load_start, *(c.get("load_start", load_start) for c in proof_coordinates))
                    load_end = max(load_end, *(c.get("load_end", load_end) for c in proof_coordinates))
            proof_memory = coordinate_memory(proof_coordinates, 2048, 8, budget)
            if cost["estimated_memory_bytes"] + rows * 512 + proof_memory > MAX_MEMORY_BYTES:
                raise EventTimeBudgetError("total_memory")
            # Include the last technical microsecond; @timestamp bounds never
            # stand in for the November load cohort of an October archive.
            phase = perf_counter()
            facts = self.repository.received_facts(source_id, load_start,
                min(reference, load_end + timedelta(microseconds=1)), reference, budget, proof_coordinates)
            timings["facts"] = perf_counter() - phase
            if preview and rows + sum(facts["exclusions"].values()) > 100000:
                raise EventTimeBudgetError("preview_rows")
            quality, reason = received_quality(facts)
            encoded_coordinates = []
            for index, coordinate in enumerate(proof_coordinates):
                if index % 128 == 0:
                    budget.check()
                encoded_coordinates.append({**coordinate,
                    "load_start": coordinate.get("load_start", load_start).isoformat(),
                    "load_end": coordinate.get("load_end", load_end).isoformat()})
            metadata = {"snapshot_reference_at": reference.isoformat(), "origin": origin,
                "received_quality": quality, "delivery_coverage": "not_attested",
                "load_start": load_start.isoformat(), "load_end": load_end.isoformat(),
                "membership_count": rows, "fingerprint_algorithm": "sha256_jsonb_pair_v1",
                "coordinates": encoded_coordinates, "facts": facts}
            # This bound also covers results produced by exact statistics.
            from ...detection.contracts import canonical
            budget.check()
            encoded_values = {**values, **{name: values[name].isoformat() if values[name] else None
                for name in ("event_time_min", "event_time_max")}}
            if len(canonical({"values": encoded_values, "metadata": metadata}).encode()) + rows * 160 > MAX_SNAPSHOT_BYTES:
                raise EventTimeBudgetError("snapshot_bytes")
            budget.check()
            timings["total"] = perf_counter() - began
            return {"reference": reference, "values": values, "manifest": manifest, "timings": timings,
                "metadata": metadata, "completeness": quality, "completeness_reason": reason,
                "rule_hash": compiled.sha256, "budget": cost}
