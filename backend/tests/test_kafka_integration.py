import os
import time
from uuid import UUID, uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, _ConsumerGroupTopicPartitions
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_auth_foundation import admin_headers, create_normalizer, login

from app.config import Settings
from app.db import Database
from app.kafka import ConfluentKafkaMetadataClient, KafkaConnectionConfig, KafkaMetadataError
from app.main import create_app
from app.models import KafkaOperationalEvent, ParsedLog, ProcessedKafkaRecord, Source


@pytest.fixture(scope="module")
def integration_database():
    raw_url = os.environ["TEST_DATABASE_URL"]
    database = Database(raw_url)
    yield database
    database.dispose()


@pytest.fixture
def integration_client(integration_database):
    from test_auth_foundation import clear_auth_state, run_alembic

    run_alembic(os.environ["TEST_DATABASE_URL"], "upgrade", "head")
    clear_auth_state(integration_database)
    settings = Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"])
    with TestClient(create_app(settings, database=integration_database)) as client:
        yield client
    clear_auth_state(integration_database)


def test_real_kafka_metadata_adapter():
    bootstrap_servers = tuple(
        item.strip()
        for item in os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        if item.strip()
    )
    assert bootstrap_servers

    metadata = ConfluentKafkaMetadataClient().metadata(
        KafkaConnectionConfig(bootstrap_servers, "PLAINTEXT"),
        timeout=5,
    )

    assert metadata.broker_count >= 1
    assert metadata.cluster_identity
    assert all(topic.name for topic in metadata.topics)


def test_real_kafka_api_source_lifecycle(integration_client, integration_database):
    bootstrap = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = f"diploma-task4-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap, "security.protocol": "PLAINTEXT"})
    create_future = admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])[topic_name]
    create_future.result(10)
    connection_id = None
    source_id = None
    headers = None
    try:
        headers = admin_headers(integration_client, integration_database)
        connection_response = integration_client.post(
            "/api/v1/kafka-connections",
            json={"name": f"real-{uuid4().hex}", "bootstrap_servers": [bootstrap]},
            headers=headers,
        )
        assert connection_response.status_code == 201, connection_response.text
        connection = connection_response.json()
        connection_id = connection["id"]
        assert integration_client.post(f"/api/v1/kafka-connections/{connection_id}/test", headers=headers).status_code == 200
        deadline = time.monotonic() + 10
        while True:
            topics = integration_client.get(f"/api/v1/kafka-connections/{connection_id}/topics").json()
            if any(item["name"] == topic_name for item in topics["items"]):
                break
            if time.monotonic() >= deadline:
                raise AssertionError("created Kafka topic did not appear in metadata")
            time.sleep(0.1)
        sources = integration_client.get(
            f"/api/v1/sources?connection_id={connection_id}"
        ).json()
        assert sources["total"] == 0
        normalizer = create_normalizer(integration_client, headers, f"real-{uuid4().hex}")
        source_response = integration_client.post(
            "/api/v1/sources",
            json={"name": f"source-{uuid4().hex}", "connection_id": connection_id, "topic_name": topic_name},
            headers=headers,
        )
        assert source_response.status_code == 201, source_response.text
        source = source_response.json()
        source_id = source["id"]
        assert source["is_enabled"] is False and source["normalizer_id"] is None
        assert integration_client.get(f"/api/v1/kafka-connections/{connection_id}/topics").json()["total"] >= 1
        assigned = integration_client.put(
            f"/api/v1/sources/{source_id}/normalizer",
            json={"normalizer_id": normalizer["id"]},
            headers=headers,
        )
        assert assigned.status_code == 200, assigned.text
        enabled = integration_client.post(f"/api/v1/sources/{source_id}/enable", headers=headers)
        assert enabled.status_code == 200, enabled.text
        assert enabled.json()["is_enabled"] is True
        disabled = integration_client.post(f"/api/v1/sources/{source_id}/disable", headers=headers)
        assert disabled.status_code == 200 and disabled.json()["is_enabled"] is False
    finally:
        if source_id is not None and headers is not None:
            integration_client.delete(f"/api/v1/sources/{source_id}", headers=headers)
        if connection_id is not None and headers is not None:
            integration_client.delete(f"/api/v1/kafka-connections/{connection_id}", headers=headers)
        delete_error = None
        try:
            admin.delete_topics([topic_name])[topic_name].result(10)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                delete_error = error
        if delete_error is not None:
            raise delete_error


def test_real_kafka_consumer_persists_before_offset_commit(integration_database):
    from test_auth_foundation import clear_auth_state, run_alembic

    bootstrap = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = f"diploma-task8-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap, "security.protocol": "PLAINTEXT"})
    admin.create_topics([
        NewTopic(topic_name, num_partitions=1, replication_factor=1)
    ])[topic_name].result(10)
    run_alembic(os.environ["TEST_DATABASE_URL"], "upgrade", "head")
    clear_auth_state(integration_database)
    settings = Settings(
        environment="test",
        database_url=os.environ["TEST_DATABASE_URL"],
        kafka_consumer_poll_timeout_seconds=0.1,
        kafka_consumer_retry_delay_seconds=0.1,
        kafka_supervisor_sync_seconds=0.1,
    )
    connection_id = None
    source_id = None
    consumer_group = None
    try:
        with TestClient(
            create_app(settings, database=integration_database, start_consumers=True)
        ) as client:
            headers = admin_headers(client, integration_database)
            connection = client.post(
                "/api/v1/kafka-connections",
                json={"name": f"task8-{uuid4().hex}", "bootstrap_servers": [bootstrap]},
                headers=headers,
            )
            assert connection.status_code == 201, connection.text
            connection_id = connection.json()["id"]
            normalizer = create_normalizer(client, headers, f"task8-{uuid4().hex}")
            source = client.post(
                "/api/v1/sources",
                json={
                    "name": f"task8-source-{uuid4().hex}",
                    "connection_id": connection_id,
                    "topic_name": topic_name,
                },
                headers=headers,
            )
            assert source.status_code == 201, source.text
            source_id = source.json()["id"]
            consumer_group = f"diploma-source-{source_id}"
            assert client.put(
                f"/api/v1/sources/{source_id}/normalizer",
                json={"normalizer_id": normalizer["id"]},
                headers=headers,
            ).status_code == 200
            assert client.post(
                f"/api/v1/sources/{source_id}/enable", headers=headers
            ).status_code == 200
            producer = Producer({"bootstrap.servers": bootstrap})
            producer.produce(
                topic_name,
                b'{"timestamp":"2026-03-02T11:59:00Z","log":"real event"}',
            )
            assert producer.flush(10) == 0
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                with integration_database.session_factory() as session:
                    stored = session.scalar(
                        select(ParsedLog).where(ParsedLog.source_id == source_id)
                    )
                    record = session.scalar(
                        select(ProcessedKafkaRecord).where(
                            ProcessedKafkaRecord.source_id == source_id
                        )
                    )
                if stored is not None and record is not None:
                    break
                time.sleep(0.1)
            else:
                raise AssertionError("Kafka message was not stored durably")
            assert stored.kafka_offset == 0
            assert record.result_status in {"complete", "partial"}
        assert consumer_group is not None

        def committed_offset() -> int:
            request = _ConsumerGroupTopicPartitions(
                consumer_group, [TopicPartition(topic_name, 0)]
            )
            offsets = admin.list_consumer_group_offsets([request])[consumer_group].result(10)
            partition = next(
                item for item in offsets.topic_partitions
                if item.topic == topic_name and item.partition == 0
            )
            return partition.offset

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and committed_offset() != 1:
            time.sleep(0.1)
        assert committed_offset() == 1

        with TestClient(
            create_app(settings, database=integration_database, start_consumers=True)
        ) as restarted_client:
            assert login(restarted_client, "admin").status_code == 200
            restarted_headers = {
                "X-CSRF-Token": restarted_client.get(
                    "/api/v1/auth/session"
                ).json()["csrf_token"]
            }
            producer = Producer({"bootstrap.servers": bootstrap})
            producer.produce(
                topic_name,
                b'{"timestamp":"2026-03-02T11:59:00Z","log":"after restart"}',
            )
            assert producer.flush(10) == 0
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                with integration_database.session_factory() as session:
                    resumed = session.scalar(
                        select(ParsedLog).where(
                            ParsedLog.source_id == source_id,
                            ParsedLog.kafka_offset == 1,
                        )
                    )
                if resumed is not None:
                    break
                time.sleep(0.1)
            else:
                raise AssertionError("consumer did not resume from confirmed offset")
            assert restarted_client.post(
                f"/api/v1/sources/{source_id}/disable", headers=restarted_headers
            ).status_code == 200
        assert committed_offset() == 2
    finally:
        clear_auth_state(integration_database)
        try:
            admin.delete_topics([topic_name])[topic_name].result(10)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise


def test_real_kafka_retention_and_recreated_topic_generation(integration_database):
    from test_auth_foundation import clear_auth_state, run_alembic

    bootstrap = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = f"diploma-task8-gap-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap, "security.protocol": "PLAINTEXT"})
    admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])[topic_name].result(10)
    run_alembic(os.environ["TEST_DATABASE_URL"], "upgrade", "head")
    clear_auth_state(integration_database)
    settings = Settings(
        environment="test", database_url=os.environ["TEST_DATABASE_URL"],
        kafka_consumer_poll_timeout_seconds=0.1,
        kafka_consumer_retry_delay_seconds=0.1,
        kafka_supervisor_sync_seconds=0.1,
    )
    metadata_client = ConfluentKafkaMetadataClient()
    config = KafkaConnectionConfig((bootstrap,), "PLAINTEXT")

    def eventually(predicate, reason, timeout=25):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.1)
        raise AssertionError(reason)

    def topic_id():
        try:
            metadata = metadata_client.metadata(config, 3)
        except KafkaMetadataError:
            return None
        return next((item.identity for item in metadata.topics if item.name == topic_name), None)

    def stored(source_id, offset):
        with integration_database.session_factory() as session:
            return session.scalar(select(ParsedLog).where(
                ParsedLog.source_id == source_id, ParsedLog.kafka_offset == offset
            ))

    producer = Producer({"bootstrap.servers": bootstrap})
    try:
        old_topic_id = eventually(topic_id, "initial topic ID unavailable")
        with TestClient(create_app(settings, database=integration_database, start_consumers=True)) as client:
            headers = admin_headers(client, integration_database)
            connection = client.post(
                "/api/v1/kafka-connections",
                json={"name": f"gap-{uuid4().hex}", "bootstrap_servers": [bootstrap]},
                headers=headers,
            )
            assert connection.status_code == 201, connection.text
            connection_id = connection.json()["id"]
            normalizer = create_normalizer(client, headers, f"gap-{uuid4().hex}")
            old = client.post(
                "/api/v1/sources",
                json={"name": "old", "connection_id": connection_id, "topic_name": topic_name},
                headers=headers,
            )
            assert old.status_code == 201, old.text
            old_source_id = UUID(old.json()["id"])
            assert client.put(
                f"/api/v1/sources/{old_source_id}/normalizer",
                json={"normalizer_id": normalizer["id"]}, headers=headers,
            ).status_code == 200
            producer.produce(topic_name, b'{"timestamp":"2026-03-02T11:59:00Z","log":"first event"}')
            assert producer.flush(10) == 0
            assert client.post(f"/api/v1/sources/{old_source_id}/enable", headers=headers).status_code == 200
            eventually(lambda: stored(old_source_id, 0), "first Kafka record not stored")

            def committed():
                request = _ConsumerGroupTopicPartitions(
                    f"diploma-source-{old_source_id}", [TopicPartition(topic_name, 0)]
                )
                rows = admin.list_consumer_group_offsets([request])[
                    f"diploma-source-{old_source_id}"
                ].result(10)
                return rows.topic_partitions[0].offset

            eventually(lambda: committed() == 1, "first Kafka offset not committed")
            assert client.post(f"/api/v1/sources/{old_source_id}/disable", headers=headers).status_code == 200
            for number in (1, 2, 3):
                producer.produce(
                    topic_name,
                    (f'{{"timestamp":"2026-03-02T11:59:00Z","log":"record {number}"}}').encode(),
                )
            assert producer.flush(10) == 0
            future = admin.delete_records([TopicPartition(topic_name, 0, 3)])
            next(iter(future.values())).result(10)
            inspector = Consumer({
                "bootstrap.servers": bootstrap, "group.id": f"inspect-{uuid4().hex}",
                "enable.auto.commit": False,
            })
            try:
                eventually(
                    lambda: inspector.get_watermark_offsets(TopicPartition(topic_name, 0), timeout=3)[0] == 3,
                    "test topic low watermark did not reach 3",
                )
            finally:
                inspector.close()
            assert client.post(f"/api/v1/sources/{old_source_id}/enable", headers=headers).status_code == 200
            eventually(lambda: stored(old_source_id, 3), "consumer did not resume from low watermark")
            with integration_database.session_factory() as session:
                gap = session.scalar(select(KafkaOperationalEvent).where(
                    KafkaOperationalEvent.source_identity == old_source_id,
                    KafkaOperationalEvent.kind == "retention_gap",
                ))
                assert gap is not None and (gap.offset_start, gap.offset_end) == (1, 3)
                assert gap.old_topic_identity == old_topic_id
                assert session.scalar(select(ParsedLog).where(
                    ParsedLog.source_id == old_source_id,
                    ParsedLog.kafka_offset.in_((1, 2)),
                )) is None

            admin.delete_topics([topic_name])[topic_name].result(10)
            eventually(lambda: topic_id() is None, "old topic remained visible")
            admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])[topic_name].result(10)
            new_topic_id = eventually(lambda: (found if (found := topic_id()) != old_topic_id else None),
                                      "recreated topic ID did not change")

            def archived():
                with integration_database.session_factory() as session:
                    source = session.get(Source, old_source_id)
                    return source.is_archived and not source.is_enabled

            eventually(archived, "old source was not archived", timeout=35)
            with integration_database.session_factory() as session:
                event = session.scalar(select(KafkaOperationalEvent).where(
                    KafkaOperationalEvent.source_identity == old_source_id,
                    KafkaOperationalEvent.kind == "topic_recreated",
                ))
                assert event is not None and event.new_topic_identity == new_topic_id
            topics = client.get(f"/api/v1/kafka-connections/{connection_id}/topics").json()
            assert next(item for item in topics["items"] if item["name"] == topic_name)["is_registered"] is False
            new = client.post(
                "/api/v1/sources",
                json={"name": "new", "connection_id": connection_id, "topic_name": topic_name},
                headers=headers,
            )
            assert new.status_code == 201, new.text
            new_source_id = UUID(new.json()["id"])
            assert new_source_id != old_source_id and not new.json()["is_enabled"]
            assert client.put(
                f"/api/v1/sources/{new_source_id}/normalizer",
                json={"normalizer_id": normalizer["id"]}, headers=headers,
            ).status_code == 200
            assert client.post(f"/api/v1/sources/{new_source_id}/enable", headers=headers).status_code == 200
            producer.produce(topic_name, b'{"timestamp":"2026-03-02T11:59:00Z","log":"new generation"}')
            assert producer.flush(10) == 0
            eventually(lambda: stored(new_source_id, 0), "new generation record not stored")
            with integration_database.session_factory() as session:
                generations = set(session.scalars(select(ProcessedKafkaRecord.kafka_topic_identity).where(
                    ProcessedKafkaRecord.kafka_topic == topic_name,
                    ProcessedKafkaRecord.kafka_offset == 0,
                )))
                assert generations == {old_topic_id, new_topic_id}
    finally:
        clear_auth_state(integration_database)
        try:
            admin.delete_topics([topic_name])[topic_name].result(10)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise


def test_kafka_timeout_and_unexpected_errors_are_classified(monkeypatch):
    class TimeoutAdmin:
        def __init__(self, _config):
            pass

        def list_topics(self, timeout):
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT))

    monkeypatch.setattr("confluent_kafka.admin.AdminClient", TimeoutAdmin)
    client = ConfluentKafkaMetadataClient()
    try:
        client.metadata(KafkaConnectionConfig(("localhost:9092",), "PLAINTEXT"), 1)
    except KafkaMetadataError as error:
        assert error.kind == "timeout"
    else:
        raise AssertionError("timeout must be classified")

    class UnavailableAdmin(TimeoutAdmin):
        def list_topics(self, timeout):
            raise KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN))

    monkeypatch.setattr("confluent_kafka.admin.AdminClient", UnavailableAdmin)
    client = ConfluentKafkaMetadataClient()
    try:
        client.metadata(KafkaConnectionConfig(("localhost:9092",), "PLAINTEXT"), 1)
    except KafkaMetadataError as error:
        assert error.kind == "unavailable"
    else:
        raise AssertionError("non-timeout Kafka errors must be classified")

    class BrokenAdmin(TimeoutAdmin):
        def list_topics(self, timeout):
            raise RuntimeError("adapter bug")

    monkeypatch.setattr("confluent_kafka.admin.AdminClient", BrokenAdmin)
    client = ConfluentKafkaMetadataClient()
    try:
        client.metadata(KafkaConnectionConfig(("localhost:9092",), "PLAINTEXT"), 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("unexpected adapter errors must propagate")


def test_topic_metadata_reuses_admin_client_without_full_cluster_listing(monkeypatch):
    from types import SimpleNamespace

    created = []

    class Future:
        def __init__(self, owner):
            self.owner = owner

        def result(self, timeout):
            return SimpleNamespace(topic_id=self.owner.topic_id, partitions=(0,))

    class Admin:
        def __init__(self, config):
            self.topic_id = "first-id"
            self.full_calls = 0
            self.targeted_calls = 0
            created.append(self)

        def list_topics(self, timeout):
            self.full_calls += 1
            return SimpleNamespace(
                cluster_id="cluster-id",
                brokers={0: object()},
                topics={"events": SimpleNamespace(partitions={0: object()}, error=None)},
            )

        def describe_topics(self, collection):
            self.targeted_calls += 1
            return {"events": Future(self)}

    monkeypatch.setattr("confluent_kafka.admin.AdminClient", Admin)
    client = ConfluentKafkaMetadataClient()
    config = KafkaConnectionConfig(("localhost:9092",), "PLAINTEXT")
    assert client.metadata(config, 1).topics[0].identity == "first-id"
    for _ in range(8):
        assert client.metadata_for_topic(config, "events", 1).topics[0].identity == "first-id"
    assert len(created) == 1
    assert created[0].full_calls == 1
    assert created[0].targeted_calls == 9
    created[0].topic_id = "replacement-id"
    assert client.metadata_for_topic(config, "events", 1).topics[0].identity == "replacement-id"
