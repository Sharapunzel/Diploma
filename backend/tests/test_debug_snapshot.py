import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.core.errors import DomainError
from app.kafka import KafkaMetadata, KafkaMetadataError, KafkaTopic
from app.kafka_debug import DebugReadError, DebugRecord
from app.services.implementations.debug_snapshot import SourceSnapshotServiceImpl


class Sources:
    def __init__(self, source):
        self.source = source

    def find_by_id(self, _):
        return self.source

    def find_by_id_for_update(self, _):
        return self.source


class Connections:
    def __init__(self, connection):
        self.connection = connection

    def find_by_id(self, _):
        return self.connection


class Metadata:
    def __init__(self, identity="generation-a"):
        self.identity = identity
        self.calls = 0

    def metadata_for_topic(self, _, topic, __):
        self.calls += 1
        return KafkaMetadata(1, (KafkaTopic(topic, 2, self.identity),), 1)


class Reader:
    def __init__(self, records):
        self.records = records
        self.calls = 0

    def read(self, *_):
        self.calls += 1
        return self.records


def setup(records):
    source = SimpleNamespace(id=uuid4(), source_type="kafka", is_archived=False,
                             kafka_topic_identity="generation-a", connection_id=uuid4(),
                             topic_name="logs", is_enabled=False)
    connection = SimpleNamespace(bootstrap_servers=["broker:9092"],
                                 security_protocol="PLAINTEXT")
    metadata, reader = Metadata(), Reader(records)
    service = SourceSnapshotServiceImpl(Sources(source), Connections(connection),
                                        metadata, reader, 2)
    return service, source, metadata, reader


def test_snapshot_preserves_envelope_time_and_disabled_source():
    payload = json.dumps({"timestamp": "2024-01-01T01:02:03+00:00", "log": "hello"}).encode()
    service, source, metadata, reader = setup([DebugRecord(0, 4, payload),
                                               DebugRecord(1, 7, b"not-json")])
    result = service.read(source.id)
    assert result.records[0].timestamp == "2024-01-01T01:02:03+00:00"
    assert result.records[0].log == "hello"
    assert result.records[1].issue == "invalid_envelope"
    assert result.records[1].log is None
    assert result.skipped == 1
    assert metadata.calls == 2 and reader.calls == 1


def test_rejects_external_and_topic_change_before_read():
    service, source, metadata, reader = setup([])
    source.source_type = "external"
    with pytest.raises(DomainError) as error:
        service.read(source.id)
    assert error.value.code == "source_not_found"
    source.source_type = "kafka"
    metadata.identity = "generation-b"
    with pytest.raises(DomainError) as error:
        service.read(source.id)
    assert error.value.code == "source_generation_changed"
    assert reader.calls == 0


def test_empty_snapshot_and_archived_source():
    service, source, _, _ = setup([])
    assert service.read(source.id).records == []
    source.is_archived = True
    with pytest.raises(DomainError):
        service.read(source.id)


def test_topic_generation_change_after_read_discards_snapshot():
    service, source, metadata, reader = setup([])
    original = reader.read

    def changed(*args):
        metadata.identity = "generation-b"
        return original(*args)

    reader.read = changed
    with pytest.raises(DomainError) as error:
        service.read(source.id)
    assert error.value.code == "source_generation_changed"


def test_oversized_record_is_never_exposed():
    service, source, _, _ = setup([DebugRecord(0, 0, b"x" * 131073)])
    record = service.read(source.id).records[0]
    assert record.issue == "message_too_large"
    assert record.log is None and record.timestamp is None


def test_response_size_limit_marks_records_without_exposing_values():
    payload = json.dumps({"timestamp": "2024-01-01T00:00:00Z", "log": "é" * 30000},
                         ensure_ascii=False).encode("utf-8")
    service, source, _, _ = setup([DebugRecord(0, offset, payload) for offset in range(50)])
    result = service.read(source.id)
    assert len(result.records) == 50
    assert result.skipped > 0
    assert all(record.log is None for record in result.records if record.issue == "response_limit")


@pytest.mark.parametrize(("kind", "code", "status"), [
    ("timeout", "kafka_debug_unavailable", 504),
    ("tls", "kafka_tls_failed", 503),
    ("authentication", "kafka_authentication_failed", 503),
    ("authorization", "kafka_acl_denied", 503),
    ("incomplete", "kafka_snapshot_incomplete", 409),
    ("byte_limit", "kafka_snapshot_too_large", 413),
])
def test_reader_failures_are_safe_and_distinct(kind, code, status):
    service, source, _, reader = setup([])

    def fail(*_):
        raise DebugReadError(kind)

    reader.read = fail
    with pytest.raises(DomainError) as error:
        service.read(source.id)
    assert error.value.code == code
    assert error.value.status_code == status


def test_metadata_security_failure_is_classified_for_snapshot():
    service, source, _, _ = setup([])

    class FailedMetadata:
        def metadata_for_topic(self, *_):
            raise KafkaMetadataError("configuration")

    service.metadata = FailedMetadata()
    with pytest.raises(DomainError) as error:
        service.read(source.id)
    assert error.value.code == "kafka_security_configuration_invalid"
    assert error.value.status_code == 503
