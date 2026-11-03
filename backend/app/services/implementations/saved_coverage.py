"""Evaluate source and Kafka evidence captured before the closing WAL barrier."""

from datetime import datetime, timedelta
from uuid import UUID

from ...repositories.protocols.evidence import CoverageEvidenceRepository
from .input_attestations import verify_attestation


def evaluate_saved_coverage(
    evidence: CoverageEvidenceRepository, source_id: UUID, start: datetime, end: datetime, barrier_at: datetime
) -> tuple[str, str, dict]:
    """Default to unknown unless signed intervals and independently captured Kafka proof agree."""
    stopped = evidence.interruptions(source_id, start, end, barrier_at)
    if stopped:
        return "interrupted", "known_source_or_processing_stop", {
            "interruptions": [str(item) for item in stopped],
        }
    pairs = evidence.attestations(source_id, start, end, barrier_at)
    valid = []
    for item, probe in pairs:
        try:
            decoded = verify_attestation(probe, item.signed_payload, item.signature)
        except ValueError:
            return "unknown", "invalid_input_attestation", {}
        expected = (item.id, item.probe_id, item.source_identity, item.input_generation,
                    item.sequence, item.start_utc, item.end_utc, item.state, item.topic_identity,
                    item.start_positions, item.end_positions)
        if decoded != expected:
            return "unknown", "input_attestation_mismatch", {}
        valid.append(item)
    interruptions = [item for item in valid if item.state == "interrupted"]
    if interruptions:
        return "interrupted", "input_interrupted", {"attestations": [str(item.id) for item in interruptions]}
    if not valid:
        return "unknown", "input_coverage_unavailable", {}
    first = valid[0]
    cursor = start
    previous = None
    used = []
    for item in valid:
        if item.start_utc > cursor:
            return "unknown", "input_coverage_gap", {}
        if previous is not None and (
            item.probe_id != first.probe_id or item.input_generation != first.input_generation
            or item.topic_identity != first.topic_identity or item.sequence != previous.sequence + 1
            or item.start_positions != previous.end_positions
        ):
            return "unknown", "input_sequence_or_generation_gap", {}
        used.append(item)
        cursor = max(cursor, item.end_utc)
        previous = item
        if cursor >= end:
            break
    if cursor < end:
        return "unknown", "input_coverage_gap", {}
    heartbeats = evidence.consumer_heartbeats(source_id, first.topic_identity, start, end)
    candidates = [item for item in heartbeats if item.observed_at <= start]
    if not candidates:
        return "unknown", "consumer_coverage_unavailable", {}
    previous = candidates[-1]
    used_heartbeats = [previous]
    for item in heartbeats:
        if item.observed_at <= previous.observed_at:
            continue
        if (item.worker_instance != previous.worker_instance
                or item.observed_at - previous.observed_at > timedelta(seconds=10)):
            return "unknown", "consumer_coverage_gap", {}
        used_heartbeats.append(item)
        previous = item
        if item.observed_at >= end:
            break
    if previous.observed_at < end:
        return "unknown", "consumer_coverage_gap", {}
    checkpoint = evidence.checkpoint(source_id, first.topic_identity, end, barrier_at)
    if checkpoint is None:
        return "unknown", "kafka_checkpoint_unavailable", {}
    positions = checkpoint.partitions
    if not isinstance(positions, dict) or positions.keys() != first.start_positions.keys():
        return "unknown", "kafka_partition_set_mismatch", {}
    for partition, beginning in first.start_positions.items():
        final = used[-1].end_positions[partition]
        sample = positions.get(partition)
        if (not isinstance(sample, dict) or set(sample) != {"low", "high", "committed"}
                or any(type(sample.get(name)) is not int for name in ("low", "high", "committed"))
                or not 0 <= sample["low"] <= sample["high"]
                or sample["low"] > beginning or sample["high"] != sample["committed"]
                or sample["committed"] < final):
            return "unknown", "kafka_backlog_or_retention", {}
        part = int(partition)
        if evidence.has_gap(source_id, first.topic_identity, part, beginning, final):
            return "unknown", "kafka_gap_detected", {}
        total, failures = evidence.processing_counts(
            source_id, first.topic_identity, part, beginning, final,
        )
        if failures or total != final - beginning:
            return "unknown", "kafka_processing_incomplete", {}
    return "complete", "verified_input_and_kafka_coverage", {
        "attestations": [str(item.id) for item in used], "kafka_checkpoint": str(checkpoint.id),
        "consumer_heartbeats": [str(item.id) for item in used_heartbeats],
    }
