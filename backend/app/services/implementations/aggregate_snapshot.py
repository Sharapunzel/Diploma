"""One candidate from a single read-only repeatable-read transaction."""

from datetime import timedelta

from ...aggregation.budget import estimate
from ...aggregation.event_time import MAX_MEMORY_BYTES, MAX_SNAPSHOT_BYTES, EventTimeBudgetError
from ...aggregation.received_snapshot import received_quality
from ...aggregation.results import interpret_result
from ...aggregation.rules import AggregateRule, compile_rule
from ...core.errors import DomainError


class AggregateSnapshotService:
    def __init__(self, repository, uow, catalog, limits):
        self.repository, self.uow, self.catalog, self.limits = repository, uow, catalog, limits

    def build(self, source_id, rule, start, end, duration, origin, cohort, budget, preview=False):
        with self.uow.read_snapshot():
            self.repository.read_budget(budget.milliseconds())
            reference = self.repository.now()
            if self.repository.source(source_id) is None:
                raise DomainError("source_unavailable", "Источник больше недоступен", 409)
            compiled = compile_rule(AggregateRule.model_validate(rule), self.catalog, self.limits)
            self.repository.limits(self.limits, preview)
            manifest, coordinates, rows, width = self.repository.snapshot_events(
                source_id, start, end, reference, budget)
            total = self.repository.event_source_rows(source_id, budget)
            cost = estimate(compiled, rows, total, duration, self.limits, preview, width)
            cohort_memory = sum(1536 + 4 * len((c.get("topic") or "").encode()) for c in coordinates)
            if cost["estimated_memory_bytes"] + len(manifest) * 512 + cohort_memory > MAX_MEMORY_BYTES:
                raise EventTimeBudgetError("total_memory")
            statistics = self.repository.event_statistics(source_id, start, end, reference, compiled, budget)
            values = interpret_result(statistics, compiled, duration)
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
            if origin in {"initial_history", "preview"}:
                remaining_memory = MAX_MEMORY_BYTES - cost["estimated_memory_bytes"] - len(manifest) * 512
                remaining_memory -= sum(2048 + 8 * len((c.get("topic") or "").encode()) for c in proof_coordinates)
                proof_coordinates.extend(self.repository.event_cohorts(
                    source_id, start, end, reference, budget, remaining_memory))
            if cohort and cohort.get("coordinates"):
                proof_coordinates.append(cohort["coordinates"])
            if proof_coordinates:
                if origin == "preview":
                    load_start = min(c["load_start"] for c in proof_coordinates)
                    load_end = max(c["load_end"] for c in proof_coordinates)
                else:
                    load_start = min(load_start, *(c.get("load_start", load_start) for c in proof_coordinates))
                    load_end = max(load_end, *(c.get("load_end", load_end) for c in proof_coordinates))
            proof_memory = sum(2048 + 8 * len((c.get("topic") or "").encode()) for c in proof_coordinates)
            if cost["estimated_memory_bytes"] + len(manifest) * 512 + proof_memory > MAX_MEMORY_BYTES:
                raise EventTimeBudgetError("total_memory")
            # Include the last technical microsecond; @timestamp bounds never
            # stand in for the November load cohort of an October archive.
            facts = self.repository.received_facts(source_id, load_start,
                min(reference, load_end + timedelta(microseconds=1)), reference, budget, proof_coordinates)
            if preview and rows + sum(facts["exclusions"].values()) > 100000:
                raise EventTimeBudgetError("preview_rows")
            quality, reason = received_quality(facts)
            metadata = {"snapshot_reference_at": reference.isoformat(), "origin": origin,
                "received_quality": quality, "delivery_coverage": "not_attested",
                "load_start": load_start.isoformat(), "load_end": load_end.isoformat(),
                "membership_count": len(manifest), "fingerprint_algorithm": "sha256_jsonb_pair_v1",
                "coordinates": [{**c, "load_start": c.get("load_start", load_start).isoformat(),
                    "load_end": c.get("load_end", load_end).isoformat()} for c in proof_coordinates], "facts": facts}
            # This bound also covers results produced by exact statistics.
            from ...detection.contracts import canonical
            encoded_values = {**values, **{name: values[name].isoformat() if values[name] else None
                for name in ("event_time_min", "event_time_max")}}
            if len(canonical({"values": encoded_values, "metadata": metadata}).encode()) + len(manifest) * 160 > MAX_SNAPSHOT_BYTES:
                raise EventTimeBudgetError("snapshot_bytes")
            budget.check()
            return {"reference": reference, "values": values, "manifest": manifest,
                "metadata": metadata, "completeness": quality, "completeness_reason": reason,
                "rule_hash": compiled.sha256, "budget": cost}
