"""Quality of the received snapshot is distinct from delivery attestation."""


def received_quality(facts):
    if facts["interruptions"]:
        return "interrupted", "known_source_or_processing_stop"
    if facts["gaps"]:
        return "unknown", "known_delivery_gap"
    if facts["processing_failures"]:
        return "unknown", "processing_failures"
    if facts["exclusions"]:
        return "unknown", "event_time_exclusions"
    if facts["backlog"]:
        return "unknown", "delivery_backlog"
    return "complete", "received_event_snapshot"
