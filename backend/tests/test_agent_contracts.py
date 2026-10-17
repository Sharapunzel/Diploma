from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError
from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID

from app.agent_credentials import decrypt_pending, encrypt_pending, new_password, principal
from app.agent_kafka_admin import ConfluentAgentKafkaAdmin
from app.api.v1.endpoints.agents import agent_dto, agent_transport
from app.config import Settings
from app.core.errors import DomainError
from app.kafka import KafkaConnectionConfig
from app.schemas.agents import AgentCreate, AgentTopicRequest
from app.services.implementations.agents import AgentLifecycleServiceImpl


def _certificate(ca: bool) -> tuple[bytes, bytes]:
    key = Ed25519PrivateKey.generate()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .sign(key, algorithm=None)
    )
    return (
        certificate.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def _transport_settings(tmp_path):
    ca_file = tmp_path / "ca.pem"
    settings = Settings(
        _env_file=None,
        environment="test",
        kafka_security_protocol="SASL_SSL",
        kafka_runtime_username="reader",
        kafka_runtime_password_file=tmp_path / "reader",
        kafka_ca_file=ca_file,
        kafka_agent_public_host="broker.example",
        kafka_agent_public_port=9094,
        kafka_admin_username="private-admin",
        kafka_admin_password_file=tmp_path / "admin",
    )
    return settings, ca_file


def test_public_agent_transport_contains_only_listener_and_public_ca(tmp_path):
    settings, ca_file = _transport_settings(tmp_path)
    certificate, _ = _certificate(ca=True)
    ca_file.write_bytes(certificate)
    response = agent_transport(settings=settings, _=None)
    assert response.headers["Cache-Control"] == "no-store"
    assert b"broker.example:9094" in response.body
    assert b"BEGIN CERTIFICATE" in response.body
    assert b"private-admin" not in response.body
    assert b"/admin" not in response.body


@pytest.mark.parametrize("content", ["private-key", "malformed", "leaf", "oversized"])
def test_public_agent_transport_rejects_non_public_ca_content(tmp_path, content):
    settings, ca_file = _transport_settings(tmp_path)
    certificate, private_key = _certificate(ca=True)
    if content == "private-key":
        ca_file.write_bytes(certificate + private_key)
    elif content == "malformed":
        ca_file.write_bytes(b"-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n")
    elif content == "leaf":
        ca_file.write_bytes(_certificate(ca=False)[0])
    else:
        ca_file.write_bytes(certificate + b" " * 65536)
    with pytest.raises(DomainError) as error:
        agent_transport(settings=settings, _=None)
    assert error.value.code == "agent_transport_unavailable"


def test_plaintext_agent_transport_rejects_non_loopback():
    with pytest.raises(ValueError, match="loopback"):
        Settings(
            _env_file=None,
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_runtime_username="reader",
            kafka_runtime_password_file="reader",
            kafka_agent_public_host="remote.example",
            kafka_agent_public_port=9092,
        )


def test_agent_metadata_shows_only_claim_availability():
    agent = SimpleNamespace(
        id=uuid4(), name="edge", description=None, state="ready", access_revision=1,
        pending_credentials_ciphertext="encrypted-private-value", safe_error=None,
    )
    dto = agent_dto({"agent": agent, "topics": [], "operation_id": None})
    assert dto.credentials_claimable is True
    assert "encrypted-private-value" not in dto.model_dump_json()


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
