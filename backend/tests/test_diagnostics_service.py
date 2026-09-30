import base64
from types import SimpleNamespace
from uuid import uuid4

from app.services.implementations.consumers import ConsumerWorkerState
from app.services.implementations.diagnostics import (
    PAYLOAD_LIMIT,
    UNKNOWN_REASON_DESCRIPTION,
    DiagnosticsServiceImpl,
    describe_reason,
)


class Repository:
    def __init__(self, source=None, error=None):
        self._source = source
        self._error = error

    def connection_names(self, source_ids):
        return {self._source.connection_id: "connection"} if self._source and source_ids else {}

    def overview(self, recent_limit, worker_source_ids):
        return {
            "registered_sources": 1000,
            "enabled_sources": 2,
            "archived_sources": 0,
            "enabled_worker_source_ids": set(worker_source_ids),
            "errors": [],
            "events": [],
        }

    def source(self, source_id):
        return self._source if self._source and self._source.id == source_id else None

    def error(self, error_id):
        return self._error if self._error and self._error.id == error_id else None


class Supervisor:
    def __init__(self, snapshots):
        self.snapshots = snapshots

    def diagnostic_snapshots(self):
        return self.snapshots

    def diagnostic_overview_snapshots(self):
        return self.snapshots


def test_worker_snapshots_distinguish_disabled_missing_and_stopped_partition():
    disabled = SimpleNamespace(
        id=uuid4(),
        name="disabled",
        connection_id=uuid4(),
        topic_name="one",
        kafka_topic_identity="topic-id",
        is_enabled=False,
        is_archived=False,
    )
    enabled = SimpleNamespace(
        id=uuid4(),
        name="enabled",
        connection_id=uuid4(),
        topic_name="two",
        kafka_topic_identity="topic-id-2",
        is_enabled=True,
        is_archived=False,
    )
    supervisor = Supervisor(
        {enabled.id: (ConsumerWorkerState("waiting"), ((3, "bad_offset"),), (2, 3), True)}
    )
    service = DiagnosticsServiceImpl(Repository(disabled), supervisor)
    disabled_state = service.source_state(disabled.id)
    assert disabled_state.worker_status == "not_running"
    assert disabled_state.reason_code == "source_disabled"

    service = DiagnosticsServiceImpl(Repository(enabled), supervisor)
    state = service.source_state(enabled.id)
    assert state.worker_status == "stopped"
    assert state.connection_name == "connection"
    assert state.reason_description
    assert [(item.partition, item.status, item.reason_code) for item in state.partitions] == [
        (2, "running", None),
        (3, "stopped", "bad_offset"),
    ]
    assert state.partitions[1].reason_description

    unknown_reason = describe_reason("future_internal_code")
    assert unknown_reason == UNKNOWN_REASON_DESCRIPTION
    assert "future_internal_code" not in unknown_reason

    service = DiagnosticsServiceImpl(Repository(enabled), Supervisor(None))
    unknown = service.source_state(enabled.id)
    assert unknown.worker_status == "unknown"
    assert unknown.reason_code == "supervisor_unavailable"
    assert unknown.reason_description == describe_reason("supervisor_unavailable")

    retrying = SimpleNamespace(
        id=uuid4(), name="retrying", connection_id=uuid4(), topic_name="three",
        kafka_topic_identity="topic-id-3", is_enabled=True, is_archived=False,
    )
    retry_state = DiagnosticsServiceImpl(
        Repository(retrying),
        Supervisor({
            retrying.id: (
                ConsumerWorkerState("retrying", "kafka_poll_unavailable"), (), (), True
            )
        }),
    ).source_state(retrying.id)
    assert retry_state.worker_status == "retrying"
    assert retry_state.reason_description == describe_reason("kafka_poll_unavailable")


def test_overview_uses_compact_worker_snapshots_and_exact_counts():
    running_id, stopped_id = uuid4(), uuid4()
    snapshots = {
        running_id: (ConsumerWorkerState("waiting"), False, True),
        stopped_id: (ConsumerWorkerState("waiting"), True, True),
    }
    overview = DiagnosticsServiceImpl(Repository(), Supervisor(snapshots)).overview(5)
    assert overview.registered_sources == 1000
    assert overview.enabled_sources == 2
    assert overview.worker_counts == {
        "running": 1,
        "retrying": 0,
        "stopped": 1,
        "not_running": 998,
        "unknown": 0,
    }
    assert overview.worker_counts_scope == "application_process"


def test_raw_payload_is_byte_exact_and_bounded():
    error = SimpleNamespace(id=uuid4(), raw_payload=b"\xff" * (PAYLOAD_LIMIT + 8))
    service = DiagnosticsServiceImpl(Repository(error=error), Supervisor(None))
    result = service.raw_payload(error.id)
    assert base64.b64decode(result.data) == error.raw_payload[:PAYLOAD_LIMIT]
    assert result.original_size == PAYLOAD_LIMIT + 8
    assert result.returned_size == PAYLOAD_LIMIT
    assert result.truncated is True

    exact = SimpleNamespace(id=uuid4(), raw_payload=b"\xff\x00")
    result = DiagnosticsServiceImpl(Repository(error=exact), Supervisor(None)).raw_payload(exact.id)
    assert base64.b64decode(result.data) == b"\xff\x00"
    assert result.truncated is False
