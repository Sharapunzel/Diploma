from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError
from cryptography.fernet import Fernet

from app.agent_credentials import decrypt_pending, encrypt_pending, new_password, principal
from app.agent_kafka_admin import ConfluentAgentKafkaAdmin
from app.core.errors import DomainError
from app.kafka import KafkaConnectionConfig
from app.schemas.agents import AgentCreate, AgentTopicRequest
from app.services.implementations.agents import AgentLifecycleServiceImpl


def test_agent_topic_request_rejects_wildcards_and_invalid_names():
    with pytest.raises(ValueError):
        AgentTopicRequest(connection_id=uuid4(), topic_name="logs-*", action="attach")
    with pytest.raises(ValueError):
        AgentTopicRequest(connection_id=uuid4(), topic_name="../other", action="create")


def test_agent_create_requires_distinct_topics():
    connection_id = uuid4()
    item = {"connection_id": connection_id, "topic_name": "auth", "action": "attach"}
    request = AgentCreate(name="edge-01", topics=[item])
    assert request.topics[0].topic_name == "auth"
    with pytest.raises(ValueError, match="duplicate Kafka topic assignment"):
        AgentCreate(name="edge-01", topics=[item, item])


def test_pending_agent_credentials_are_encrypted_and_one_time_ready(tmp_path):
    key_file = tmp_path / "key"
    key_file.write_bytes(Fernet.generate_key())
    password = new_password()
    username = principal("c5eb19b9-e8e9-4c57-8d6e-619dc6bcedb9", 2)
    encrypted = encrypt_pending(key_file, username, password)
    assert password not in encrypted
    assert decrypt_pending(key_file, encrypted) == (username, password)


def test_idempotency_key_binds_action_agent_and_canonical_request():
    agent_id = uuid4()
    topic_id = uuid4()
    request = {"agent_id": str(agent_id), "topic_id": str(topic_id)}
    agent = SimpleNamespace(id=agent_id)
    operation = SimpleNamespace(kind="remove_topic", payload={"request": request})
    service = object.__new__(AgentLifecycleServiceImpl)
    service.repository = Mock()
    service.repository.by_request_key.return_value = (agent, operation)

    assert service._prior("key", "remove_topic", agent_id, request) == (agent, operation)
    for kind, target, payload in [
        ("add_topic", agent_id, request),
        ("remove_topic", uuid4(), request),
        ("remove_topic", agent_id, {**request, "topic_id": str(uuid4())}),
    ]:
        with pytest.raises(DomainError) as error:
            service._prior("key", kind, target, payload)
        assert error.value.code == "idempotency_key_conflict"


def test_identical_create_replays_existing_operation_and_changed_body_conflicts():
    connection_id = uuid4()
    request = AgentCreate(
        name="edge",
        topics=[{"connection_id": connection_id, "topic_name": "logs", "action": "create"}],
    )
    agent = SimpleNamespace(id=uuid4())
    operation = SimpleNamespace(
        kind="register", payload={"request": request.model_dump(mode="json")}
    )
    service = object.__new__(AgentLifecycleServiceImpl)
    service.repository = Mock()
    service.repository.by_request_key.return_value = (agent, operation)
    assert service.create(request, "key") == (agent, operation)
    with pytest.raises(DomainError) as error:
        service.create(request.model_copy(update={"name": "other"}), "key")
    assert error.value.code == "idempotency_key_conflict"


def test_removing_missing_scram_credential_is_idempotent():
    client = Mock()
    client.delete_acls.return_value = {"acl": Mock(result=Mock(return_value=[]))}
    missing = Exception(SimpleNamespace(code=lambda: KafkaError.RESOURCE_NOT_FOUND))
    client.describe_user_scram_credentials.return_value = {
        "agent": Mock(result=Mock(side_effect=missing))
    }
    admin = object.__new__(ConfluentAgentKafkaAdmin)
    admin._client = Mock(return_value=client)
    admin.remove_principal(KafkaConnectionConfig(("localhost:9092",), "SASL_PLAINTEXT"), "agent", 1)
    client.alter_user_scram_credentials.assert_not_called()


def test_removing_present_scram_credential_requests_deletion():
    client = Mock()
    client.delete_acls.return_value = {"acl": Mock(result=Mock(return_value=[]))}
    client.describe_user_scram_credentials.return_value = {
        "agent": Mock(result=Mock(return_value=SimpleNamespace(scram_credential_infos=[object()])))
    }
    client.alter_user_scram_credentials.return_value = {
        "agent": Mock(result=Mock(return_value=None))
    }
    admin = object.__new__(ConfluentAgentKafkaAdmin)
    admin._client = Mock(return_value=client)
    admin.remove_principal(KafkaConnectionConfig(("localhost:9092",), "SASL_PLAINTEXT"), "agent", 1)
    client.alter_user_scram_credentials.assert_called_once()
