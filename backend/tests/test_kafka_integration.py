import os
import time
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.testclient import TestClient
from test_auth_foundation import admin_headers, create_normalizer

from app.config import Settings
from app.db import Database
from app.kafka import ConfluentKafkaMetadataClient, KafkaConnectionConfig, KafkaMetadataError
from app.main import create_app


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
    try:
        client.metadata(KafkaConnectionConfig(("localhost:9092",), "PLAINTEXT"), 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("unexpected adapter errors must propagate")
