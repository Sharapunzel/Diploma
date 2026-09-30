import base64
from datetime import UTC, datetime
from uuid import UUID

from ...core.errors import DomainError
from ...repositories.protocols.diagnostics import DiagnosticsRepository
from ...schemas.diagnostics import (
    OperationalEventDTO,
    OperationalEventPage,
    Overview,
    ProcessingErrorDTO,
    ProcessingErrorPage,
    RawPayloadDTO,
    SourceState,
    SourceStatePage,
)

PAYLOAD_LIMIT = 700 * 1024
SAFE_WORKER_STATES = {"waiting", "processing"}
REASON_DESCRIPTIONS = {
    'all_partitions_stopped': 'All partitions for this source are stopped.',
    'cluster_identity_conflict': 'The Kafka cluster identity does not match the saved configuration.',
    'cluster_identity_unavailable': 'Kafka did not provide a cluster identity.',
    'gap_recovery_unavailable': 'The Kafka offset gap could not be recovered safely.',
    'invalid_position': 'Kafka returned an invalid read position.',
    'kafka_commit_unavailable': 'The processing position could not be committed to Kafka.',
    'kafka_consumer_unavailable': 'A Kafka consumer could not be created.',
    'kafka_metadata_unavailable': 'Kafka metadata could not be retrieved.',
    'kafka_pause_unavailable': 'The affected Kafka partition could not be paused.',
    'kafka_poll_unavailable': 'The next Kafka message could not be retrieved.',
    'kafka_position_unavailable': 'The available Kafka position could not be determined.',
    'kafka_revoke_unavailable': 'Kafka partition assignment could not be completed.',
    'offset_missing_within_watermarks': 'An expected offset is missing within the available Kafka range.',
    'partition_stopped': 'One or more source partitions are stopped.',
    'position_outside_available_range': 'The saved offset is outside the available Kafka range.',
    'position_unknown_after_history': 'The read position cannot be safely recovered from saved history.',
    'processing_configuration_invalid': 'The source processing configuration is invalid.',
    'processing_not_durable': 'The processing result could not be saved reliably.',
    'retention_gap_detected': 'Kafka removed offsets before the application could read them.',
    'retention_gap_storage_unavailable': 'The Kafka gap details could not be saved.',
    'retention_history_unavailable': 'The Kafka gap history could not be checked.',
    'retention_watermark_regressed': 'Kafka availability bounds conflict with saved history.',
    'source_configuration_changed': 'The source configuration changed during processing.',
    'source_disabled': 'The source is disabled.',
    'storage_unavailable': 'The diagnostics or processing store is temporarily unavailable.',
    'supervisor_unavailable': 'The Kafka supervisor is not running in this application process.',
    'topic_identity_history_mismatch': 'The topic identity does not match saved history.',
    'topic_identity_unavailable': 'Kafka did not provide a topic identity.',
    'topic_identity_unavailable_after_history': 'The topic identity is unavailable although history exists.',
    'topic_not_found': 'The topic does not exist in Kafka.',
    'topic_recreated': 'The topic was recreated with a new generation.',
    'worker_not_started': 'The enabled source does not have a worker yet.',
    'worker_starting': 'The source worker is starting.',
    'worker_state_unavailable': 'The worker state is temporarily unavailable.',
    'worker_stopped': 'The source worker is stopped.',
    'stopped_by_lifecycle': 'The worker stopped while the source was disabled or reconfigured.',
    'consumer_runtime_failure': 'The worker stopped because of an internal runtime failure.',
}
UNKNOWN_REASON_DESCRIPTION = "Reason unavailable; check the source state and application logs."

def describe_reason(code: str | None) -> str | None:
    if code is None:
        return None
    return REASON_DESCRIPTIONS.get(code, UNKNOWN_REASON_DESCRIPTION)


class DiagnosticsServiceImpl:
    def __init__(self, repository: DiagnosticsRepository, supervisor) -> None:
        self.repository = repository
        self.supervisor = supervisor

    def _states(self, sources):
        captured = datetime.now(UTC)
        snapshots = self.supervisor.diagnostic_snapshots()
        connection_names = self.repository.connection_names(
            list({item.connection_id for item in sources if item.connection_id is not None})
        )
        result = []
        for source in sources:
            snapshot = snapshots.get(source.id) if snapshots is not None else None
            partitions = []
            if not source.is_enabled:
                status, reason = "not_running", "source_disabled"
            elif snapshot is None:
                status, reason = (
                    ("unknown", "supervisor_unavailable")
                    if snapshots is None
                    else ("not_running", "worker_not_started")
                )
            else:
                worker_state, stopped, known_partitions, alive = snapshot
                status, reason = self._worker_status(
                    worker_state, bool(stopped), alive
                )
                stopped_map = dict(stopped)
                partitions = [
                    {
                        "partition": part,
                        "status": "stopped"
                        if part in stopped_map
                        else (
                            "retrying"
                            if worker_state.status == "retrying"
                            else "running"
                            if alive and worker_state.status in SAFE_WORKER_STATES
                            else "unknown"
                        ),
                        "reason_code": stopped_map.get(part)
                        or (
                            worker_state.reason
                            if worker_state.status == "retrying"
                            else "worker_starting"
                            if worker_state.status == "starting"
                            else None
                        ),
                        "reason_description": describe_reason(
                            stopped_map.get(part)
                            or (
                                worker_state.reason
                                if worker_state.status == "retrying"
                                else "worker_starting"
                                if worker_state.status == "starting"
                                else None
                            )
                        ),
                    }
                    for part in known_partitions
                ]
            result.append(
                SourceState(
                    source_id=source.id,
                    source_name=source.name,
                    connection_id=source.connection_id,
                    connection_name=connection_names.get(source.connection_id),
                    topic=source.topic_name,
                    topic_id=source.kafka_topic_identity,
                    is_enabled=source.is_enabled,
                    is_archived=source.is_archived,
                    worker_status=status,
                    reason_code=reason,
                    reason_description=describe_reason(reason),
                    captured_at=captured,
                    partitions=partitions,
                )
            )
        return result

    @staticmethod
    def _worker_status(worker_state, has_stopped_partition: bool, alive: bool):
        if has_stopped_partition:
            return "stopped", "partition_stopped"
        if not alive:
            if worker_state.status == "stopped":
                return "stopped", worker_state.reason or "worker_stopped"
            return "unknown", "worker_state_unavailable"
        if worker_state.status == "retrying":
            return "retrying", worker_state.reason
        if worker_state.status == "stopped":
            return "stopped", worker_state.reason
        if worker_state.status in SAFE_WORKER_STATES:
            return "running", worker_state.reason
        return "unknown", "worker_starting"

    def source_states(self, limit: int, offset: int) -> SourceStatePage:
        items, total = self.repository.sources(limit, offset)
        return SourceStatePage(items=self._states(items), total=total, limit=limit, offset=offset)

    def source_state(self, source_id: UUID):
        source = self.repository.source(source_id)
        if source is None:
            raise DomainError("source_not_found", "Kafka source not found", 404)
        return self._states([source])[0]

    def errors(self, source_id, stage, start, end, limit, offset):
        rows, total = self.repository.errors(source_id, stage, start, end, limit, offset)
        return ProcessingErrorPage(
            items=[ProcessingErrorDTO.from_error(row) for row in rows],
            total=total,
            limit=limit,
            offset=offset,
        )

    def error(self, error_id: UUID):
        item = self.repository.error(error_id)
        if item is None:
            raise DomainError("processing_error_not_found", "Processing error not found", 404)
        return ProcessingErrorDTO.from_error(item)

    def raw_payload(self, error_id: UUID):
        item = self.repository.error(error_id)
        if item is None:
            raise DomainError("processing_error_not_found", "Processing error not found", 404)
        payload = item.raw_payload
        returned = payload[:PAYLOAD_LIMIT]
        return RawPayloadDTO(
            error_id=error_id,
            data=base64.b64encode(returned).decode("ascii"),
            original_size=len(payload),
            returned_size=len(returned),
            truncated=len(payload) > len(returned),
        )

    def events(self, source_id, kind, start, end, limit, offset):
        rows, total = self.repository.events(source_id, kind, start, end, limit, offset)
        return OperationalEventPage(
            items=[OperationalEventDTO.from_event(row) for row in rows],
            total=total,
            limit=limit,
            offset=offset,
        )

    def overview(self, limit: int):
        snapshots = self.supervisor.diagnostic_overview_snapshots()
        data = self.repository.overview(limit, list(snapshots) if snapshots is not None else [])
        counts = {name: 0 for name in ("running", "retrying", "stopped", "not_running", "unknown")}
        registered = data["registered_sources"]
        enabled = data["enabled_sources"]
        counts["not_running"] = registered - enabled
        if snapshots is None:
            counts["unknown"] = enabled
        else:
            enabled_worker_ids = data["enabled_worker_source_ids"]
            for source_id in enabled_worker_ids:
                state, has_stopped_partition, alive = snapshots[source_id]
                worker_status, _ = self._worker_status(state, has_stopped_partition, alive)
                counts[worker_status] += 1
            counts["not_running"] += enabled - len(enabled_worker_ids)
        return Overview(
            registered_sources=registered,
            enabled_sources=enabled,
            archived_sources=data["archived_sources"],
            worker_counts=counts,
            recent_errors=[ProcessingErrorDTO.from_error(row) for row in data["errors"]],
            recent_events=[OperationalEventDTO.from_event(row) for row in data["events"]],
            captured_at=datetime.now(UTC),
        )
