"""Real PostgreSQL/Kafka regressions for interrupted agent provisioning."""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import select
from test_auth_foundation import admin_headers
from test_kafka_integration import (
    kafka_test_admin_config,
    kafka_test_profile,
)

from app.agent_credentials import decrypt_pending
from app.agent_kafka_admin import AgentKafkaAdminProfile, ConfluentAgentKafkaAdmin
from app.agent_provisioner import AgentProvisioner
from app.models import FluentBitAgent, FluentBitAgentTopic, KafkaProvisioningOperation, Source
from app.repositories.sqlalchemy.connections import SqlAlchemyKafkaConnectionRepository
from app.schemas.agents import AgentCreate
from app.services.implementations.agents import AgentLifecycleServiceImpl

pytest_plugins = ("test_kafka_integration",)


def _worker(database, client, admin=None):
    profile = kafka_test_profile()
    real_admin = ConfluentAgentKafkaAdmin(
        AgentKafkaAdminProfile(
            profile.protocol,
            "diploma-kafka-admin",
            Path(os.environ["KAFKA_TEST_ADMIN_PASSWORD_FILE"]),
            profile.ca_file,
        )
    )
    return AgentProvisioner(
        database,
        admin or real_admin,
        client.app.state.kafka_client,
        timeout=10,
        credentials_key_file=client.app.state.settings.agent_credentials_key_file,
    )


def _setup(client, database, topic_count=2):
    headers = admin_headers(client, database)
    response = client.post(
        "/api/v1/kafka-connections",
        json={
            "name": f"recovery-{uuid4().hex}",
            "bootstrap_servers": [os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"]],
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    connection_id = response.json()["id"]
    names = [f"diploma-recovery-{uuid4().hex}" for _ in range(topic_count)]
    payload = {
        "name": f"agent-{uuid4().hex}",
        "topics": [
            {"connection_id": connection_id, "topic_name": name, "action": "create"}
            for name in names
        ],
    }
    response = client.post(
        "/api/v1/agents",
        json=payload,
        headers={**headers, "Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 202, response.text
    return headers, connection_id, names, response.json(), payload


def _retry_now(database, operation_id):
    with database.session_factory() as session:
        operation = session.get(KafkaProvisioningOperation, UUID(operation_id))
        operation.updated_at = datetime.now(UTC) - timedelta(minutes=3)
        session.commit()


def _cleanup(client, database, headers, connection_id, names, agent_id, worker):
    state = client.get(f"/api/v1/agents/{agent_id}").json()
    if state["state"] != "revoked":
        response = client.post(
            f"/api/v1/agents/{agent_id}/revoke",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        if response.status_code == 202 and worker.claim() == UUID(response.json()["id"]):
            worker.execute(UUID(response.json()["id"]))
    AdminClient(kafka_test_admin_config()).delete_topics(names)
    # Sources are retained by design; their connection remains reserved until
    # the test DB is removed by the temporary runner.


def _send(producer, topic):
    errors = []
    try:
        producer.produce(
            topic, b"recovery check", callback=lambda error, _message: errors.append(error)
        )
    except KafkaException as error:
        return error
    pending = producer.flush(10)
    return errors[-1] if errors else (KafkaError._AUTHENTICATION if pending else None)


def test_revoke_after_last_topic_removal_clears_all_revisions(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    worker = _worker(database, client)
    admin = AdminClient(kafka_test_admin_config())
    try:
        registration_id = UUID(agent["operation_id"])
        assert worker.claim() == registration_id
        worker.execute(registration_id)
        first = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        rotation = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/rotate",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert rotation.status_code == 202, rotation.text
        rotation_id = UUID(rotation.json()["id"])
        assert worker.claim() == rotation_id
        worker.execute(rotation_id)
        second = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        profile = kafka_test_profile()
        producer = Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
                "security.protocol": profile.protocol,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": second["username"],
                "sasl.password": second["password"],
                "error_cb": lambda _error: None,
            }
        )
        assert _send(producer, names[0]) is None
        topic_id = client.get(f"/api/v1/agents/{agent['id']}").json()["topics"][0]["id"]
        removal = client.delete(
            f"/api/v1/agents/{agent['id']}/topics/{topic_id}",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert removal.status_code == 202, removal.text
        removal_id = UUID(removal.json()["id"])
        assert worker.claim() == removal_id
        worker.execute(removal_id)
        assert not _assignment_snapshot(database, agent["id"])
        with database.session_factory() as session:
            record = session.get(FluentBitAgent, UUID(agent["id"]))
            assert record.connection_id == UUID(connection_id)
            assert SqlAlchemyKafkaConnectionRepository(session).has_agent_assignments(
                UUID(connection_id)
            )
        assert _send(producer, names[0]) is not None
        revoke = client.post(
            f"/api/v1/agents/{agent['id']}/revoke",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert revoke.status_code == 202, revoke.text
        revoke_id = UUID(revoke.json()["id"])
        real_admin = worker.admin

        class KafkaUnavailable:
            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def remove_principal(self, *args):
                raise RuntimeError("injected Kafka outage")

        worker.admin = KafkaUnavailable()
        assert worker.claim() == revoke_id
        worker.execute(revoke_id)
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "revocation_pending"
        _retry_now(database, str(revoke_id))

        class CrashAfterScramDelete:
            crashed = False

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def remove_principal(self, *args):
                real_admin.remove_principal(*args)
                if not self.crashed:
                    self.crashed = True
                    raise RuntimeError("injected after SCRAM deletion")

        worker = _worker(database, client, CrashAfterScramDelete())
        assert worker.claim() == revoke_id
        worker.execute(revoke_id)
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "revocation_pending"
        _retry_now(database, str(revoke_id))
        restarted = _worker(database, client)
        assert restarted.claim() == revoke_id
        restarted.execute(revoke_id)
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "revoked"
        with database.session_factory() as session:
            assert session.get(FluentBitAgent, UUID(agent["id"])).connection_id is None
            assert not SqlAlchemyKafkaConnectionRepository(session).has_agent_assignments(
                UUID(connection_id)
            )
        for credentials in (first, second):
            try:
                result = admin.describe_user_scram_credentials([credentials["username"]])[
                    credentials["username"]
                ].result(10)
            except KafkaException as error:
                assert error.args[0].code() == KafkaError.RESOURCE_NOT_FOUND
            else:
                assert not result.scram_credential_infos
        assert _send(producer, names[0]) is not None
        disconnected = Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
                "security.protocol": profile.protocol,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": second["username"],
                "sasl.password": second["password"],
                "error_cb": lambda _error: None,
            }
        )
        assert _send(disconnected, names[0]) is not None
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def test_two_created_topics_resume_after_first_commit_and_acl(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, payload = _setup(client, database)
    real_admin = _worker(database, client).admin

    class InterruptAfterFirstAcl:
        def __init__(self):
            self.created = []
            self.interrupted = False

        def __getattr__(self, name):
            return getattr(real_admin, name)

        def create_topic(self, config, name, operation_id, timeout):
            self.created.append(name)
            return real_admin.create_topic(config, name, operation_id, timeout)

        def grant_write(self, config, name, principal, timeout):
            real_admin.grant_write(config, name, principal, timeout)
            if name == names[1] and not self.interrupted:
                self.interrupted = True
                raise RuntimeError("injected after ACL")

    injected = InterruptAfterFirstAcl()
    worker = _worker(database, client, injected)
    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        with database.session_factory() as session:
            first_operation = session.get(KafkaProvisioningOperation, UUID(agent["operation_id"]))
            assert injected.interrupted, (injected.created, first_operation.safe_error)
        with database.session_factory() as session:
            assignments = list(
                session.scalars(
                    select(FluentBitAgentTopic).where(
                        FluentBitAgentTopic.agent_id == UUID(agent["id"])
                    )
                )
            )
            assert len(assignments) == 2
            assert all(item.topic_identity for item in assignments)
            assert any(item.state == "ready" for item in assignments)
        _retry_now(database, agent["operation_id"])
        restarted = _worker(database, client, injected)
        assert restarted.claim() == UUID(agent["operation_id"])
        restarted.execute(UUID(agent["operation_id"]))
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "ready"
        assert injected.created == names
        with database.session_factory() as session:
            assignments = list(
                session.scalars(
                    select(FluentBitAgentTopic).where(
                        FluentBitAgentTopic.agent_id == UUID(agent["id"])
                    )
                )
            )
            assert all(item.state == "ready" for item in assignments)
            assert all(
                session.get(Source, item.source_id).kafka_topic_identity == item.topic_identity
                for item in assignments
            )
        other = client.post(
            "/api/v1/agents",
            json=payload,
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert other.status_code == 409
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def test_rotation_and_revoke_resume_after_scram_and_acl_boundaries(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 2)
    worker = _worker(database, client)
    real_admin = worker.admin
    profile = kafka_test_profile()

    def producer_for(credentials):
        return Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
                "security.protocol": profile.protocol,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": credentials["username"],
                "sasl.password": credentials["password"],
                "error_cb": lambda _error: None,
            }
        )

    def rotate_request():
        response = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/rotate",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert response.status_code == 202, response.text
        return UUID(response.json()["id"])

    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        old_producer = producer_for(credentials)
        assert _send(old_producer, names[0]) is None

        rotation_id = rotate_request()

        class CrashAfterScramRemoval:
            crashed = False

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def remove_principal(self, *args):
                real_admin.remove_principal(*args)
                if not self.crashed:
                    self.crashed = True
                    raise RuntimeError("injected after SCRAM deletion")

        crash = CrashAfterScramRemoval()
        worker.admin = crash
        assert worker.claim() == rotation_id
        worker.execute(rotation_id)
        assert crash.crashed
        assert _rotation_conflicts(client, headers, agent["id"])
        _retry_now(database, str(rotation_id))
        restarted = _worker(database, client)
        assert restarted.claim() == rotation_id
        restarted.execute(rotation_id)
        assert _send(old_producer, names[0]) is not None
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        current_producer = producer_for(credentials)
        assert _send(current_producer, names[0]) is None

        rotation_id = rotate_request()

        class CrashAfterNewAcl:
            crashed = False

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def grant_write(self, config, topic, principal, timeout):
                real_admin.grant_write(config, topic, principal, timeout)
                if topic == names[0] and not self.crashed:
                    self.crashed = True
                    raise RuntimeError("injected after new ACL")

        acl_crash = CrashAfterNewAcl()
        restarted.admin = acl_crash
        assert restarted.claim() == rotation_id
        restarted.execute(rotation_id)
        assert acl_crash.crashed
        assert _rotation_conflicts(client, headers, agent["id"])
        _retry_now(database, str(rotation_id))
        worker = _worker(database, client)
        assert worker.claim() == rotation_id
        worker.execute(rotation_id)
        assert _send(current_producer, names[0]) is not None
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        final_producer = producer_for(credentials)
        assert _send(final_producer, names[0]) is None
        assert _send(final_producer, names[1]) is None
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["access_revision"] == 3

        rotation_id = rotate_request()

        class CrashAfterNewCredentials:
            crashed = False

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def set_credential(self, *args):
                real_admin.set_credential(*args)
                if not self.crashed:
                    self.crashed = True
                    raise RuntimeError("injected after new revision credentials")

        credentials_crash = CrashAfterNewCredentials()
        worker.admin = credentials_crash
        assert worker.claim() == rotation_id
        worker.execute(rotation_id)
        assert credentials_crash.crashed
        assert _rotation_conflicts(client, headers, agent["id"])
        _retry_now(database, str(rotation_id))
        worker = _worker(database, client)
        assert worker.claim() == rotation_id
        worker.execute(rotation_id)
        assert _send(final_producer, names[0]) is not None
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        final_producer = producer_for(credentials)
        assert _send(final_producer, names[0]) is None
        assert _send(final_producer, names[1]) is None
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["access_revision"] == 4

        response = client.post(
            f"/api/v1/agents/{agent['id']}/revoke",
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert response.status_code == 202, response.text
        revoke_id = UUID(response.json()["id"])

        class CrashDuringRevoke:
            calls = 0

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def remove_principal(self, config, principal, timeout):
                self.calls += 1
                if self.calls == 1:
                    for topic in names:
                        real_admin.remove_write(config, topic, principal, timeout)
                    raise RuntimeError("injected after ACL deletion")
                real_admin.remove_principal(config, principal, timeout)
                if self.calls == 2:
                    raise RuntimeError("injected after SCRAM deletion")

        revoke_crash = CrashDuringRevoke()
        worker.admin = revoke_crash
        assert worker.claim() == revoke_id
        worker.execute(revoke_id)
        assert _send(final_producer, names[0]) is not None
        for _ in range(2):
            _retry_now(database, str(revoke_id))
            assert worker.claim() == revoke_id
            worker.execute(revoke_id)
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "revoked"
        assert not _assignment_snapshot(database, agent["id"])
        assert _send(final_producer, names[0]) is not None
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def _rotation_conflicts(client, headers, agent_id):
    response = client.post(
        f"/api/v1/agents/{agent_id}/credentials/rotate",
        headers={**headers, "Idempotency-Key": str(uuid4())},
    )
    return response.status_code == 409


def test_concurrent_identical_key_returns_single_postgres_operation(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers = admin_headers(client, database)
    response = client.post(
        "/api/v1/kafka-connections",
        json={
            "name": f"race-{uuid4().hex}",
            "bootstrap_servers": [os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"]],
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    connection_id = response.json()["id"]
    name = f"diploma-race-{uuid4().hex}"
    request = AgentCreate(
        name="racing-agent",
        topics=[{"connection_id": connection_id, "topic_name": name, "action": "create"}],
    )
    key = str(uuid4())
    start = threading.Barrier(2)

    def create():
        with database.session_factory() as session:
            service = AgentLifecycleServiceImpl(
                session, client.app.state.kafka_client, client.app.state.settings
            )
            start.wait(10)
            agent, operation = service.create(request, key)
            return agent.id, operation.id

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(create)
        second = pool.submit(create)
        results = [first.result(30), second.result(30)]
    assert results[0] == results[1]
    agent_id, operation_id = results[0]
    worker = _worker(database, client)
    try:
        assert worker.claim() == operation_id
        worker.execute(operation_id)
        assert client.get(f"/api/v1/agents/{agent_id}").json()["state"] == "ready"
    finally:
        _cleanup(client, database, headers, connection_id, [name], str(agent_id), worker)


def test_credentials_created_before_worker_crash_reuse_same_secret(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    real_admin = _worker(database, client).admin

    class CrashAfterCredentials:
        crashed = False

        def __getattr__(self, attribute):
            return getattr(real_admin, attribute)

        def set_credential(self, *args):
            real_admin.set_credential(*args)
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("injected after credentials")

    worker = _worker(database, client, CrashAfterCredentials())
    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        with database.session_factory() as session:
            record = session.get(FluentBitAgent, UUID(agent["id"]))
            saved_ciphertext = record.pending_credentials_ciphertext
            expected = decrypt_pending(worker.credentials_key_file, saved_ciphertext)
        _retry_now(database, agent["operation_id"])
        restarted = _worker(database, client)
        assert restarted.claim() == UUID(agent["operation_id"])
        restarted.execute(UUID(agent["operation_id"]))
        response = client.post(f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers)
        assert response.status_code == 200
        assert (response.json()["username"], response.json()["password"]) == expected
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "ready"
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def test_add_topic_lost_create_response_keeps_existing_access(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    worker = _worker(database, client)
    added_name = f"diploma-added-uncertain-{uuid4().hex}"
    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        profile = kafka_test_profile()
        producer = Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
                "security.protocol": profile.protocol,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": credentials["username"],
                "sasl.password": credentials["password"],
                "error_cb": lambda _error: None,
            }
        )
        assert _send(producer, names[0]) is None
        response = client.post(
            f"/api/v1/agents/{agent['id']}/topics",
            headers={**headers, "Idempotency-Key": str(uuid4())},
            json={"connection_id": connection_id, "topic_name": added_name, "action": "create"},
        )
        assert response.status_code == 202, response.text
        operation_id = UUID(response.json()["id"])
        real_admin = worker.admin

        class LostResponse:
            crashed = False

            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def create_topic(self, *args):
                real_admin.create_topic(*args)
                if not self.crashed:
                    self.crashed = True
                    raise RuntimeError("injected lost add-topic response")

        lost = LostResponse()
        worker.admin = lost
        assert worker.claim() == operation_id
        worker.execute(operation_id)
        assert lost.crashed
        _retry_now(database, str(operation_id))
        restarted = _worker(database, client)
        assert restarted.claim() == operation_id
        restarted.execute(operation_id)
        assert (
            client.get(f"/api/v1/agents/{agent['id']}/operations/{operation_id}").json()["state"]
            == "failed"
        )
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "ready"
        assert [item.topic_name for item in _assignment_snapshot(database, agent["id"])] == names
        assert _send(producer, names[0]) is None
        assert _send(producer, added_name) is not None
    finally:
        _cleanup(
            client,
            database,
            headers,
            connection_id,
            [*names, added_name],
            agent["id"],
            _worker(database, client),
        )


def test_stale_worker_cannot_commit_after_owner_token_changes(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    real_admin = _worker(database, client).admin
    operation_id = UUID(agent["operation_id"])

    class LostLease:
        changed = False

        def __getattr__(self, attribute):
            return getattr(real_admin, attribute)

        def set_credential(self, *args):
            real_admin.set_credential(*args)
            if not self.changed:
                self.changed = True
                with database.session_factory() as session:
                    operation = session.get(KafkaProvisioningOperation, operation_id)
                    operation.owner_token = uuid4()
                    session.commit()

    first = _worker(database, client, LostLease())
    try:
        assert first.claim() == operation_id
        first.execute(operation_id)
        with database.session_factory() as session:
            operation = session.get(KafkaProvisioningOperation, operation_id)
            assert operation.state == "running"
            assert operation.payload.get("creation_started") is None
            assignment = _assignment_snapshot(database, agent["id"])[0]
            assert assignment.topic_identity is None
        _retry_now(database, str(operation_id))
        restarted = _worker(database, client)
        assert restarted.claim() == operation_id
        restarted.execute(operation_id)
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "ready"
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def test_live_worker_keeps_stale_job_from_second_provisioner(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    real_admin = _worker(database, client).admin
    entered = threading.Event()
    release = threading.Event()

    class SlowAdmin:
        def __getattr__(self, name):
            return getattr(real_admin, name)

        def set_credential(self, *args):
            entered.set()
            assert release.wait(20)
            return real_admin.set_credential(*args)

    first = _worker(database, client, SlowAdmin())
    second = _worker(database, client)
    try:
        assert first.claim() == UUID(agent["operation_id"])
        thread = threading.Thread(target=first.execute, args=(UUID(agent["operation_id"]),))
        thread.start()
        assert entered.wait(10)
        with database.session_factory() as session:
            operation = session.get(KafkaProvisioningOperation, UUID(agent["operation_id"]))
            operation.updated_at = datetime.now(UTC) - timedelta(minutes=3)
            session.commit()
        assert second.claim() is None
        release.set()
        thread.join(30)
        assert not thread.is_alive()
        assert client.get(f"/api/v1/agents/{agent['id']}").json()["state"] == "ready"
    finally:
        release.set()
        _cleanup(client, database, headers, connection_id, names, agent["id"], second)


def test_changed_topic_generation_retries_acl_revocation_after_restart(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    name = names[0]
    worker = _worker(database, client)
    admin = AdminClient(kafka_test_admin_config())
    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        credentials = client.post(
            f"/api/v1/agents/{agent['id']}/credentials/claim", headers=headers
        ).json()
        profile = kafka_test_profile()
        producer = Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
                "security.protocol": profile.protocol,
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": credentials["username"],
                "sasl.password": credentials["password"],
                "error_cb": lambda _error: None,
            }
        )
        assert _send(producer, name) is None
        old_id = client.get(f"/api/v1/agents/{agent['id']}").json()["topics"][0]["topic_identity"]
        admin.delete_topics([name])[name].result(10)
        deadline = time.monotonic() + 30
        while True:
            try:
                admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[
                    name
                ].result(10)
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
        config, _ = worker._connection_config_for_assignment(
            next(iter(_assignment_snapshot(database, agent["id"])))
        )
        deadline = time.monotonic() + 30
        while True:
            current = worker.metadata.metadata_for_topic(config, name, 10)
            topic = next(iter(current.topics), None)
            if topic is not None and topic.identity != old_id:
                break
            assert time.monotonic() < deadline
            time.sleep(0.5)

        real_admin = worker.admin

        class FailingRevoke:
            def __getattr__(self, attribute):
                return getattr(real_admin, attribute)

            def remove_write(self, *args):
                raise RuntimeError("injected Kafka outage")

        worker.admin = FailingRevoke()
        worker.reconcile_ready_assignments()
        assert _assignment_snapshot(database, agent["id"])[0].state == "revocation_pending"
        restarted = _worker(database, client)
        restarted.reconcile_ready_assignments()
        assert _assignment_snapshot(database, agent["id"])[0].state == "error"
        assert _send(producer, name) is not None
    finally:
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )


def _assignment_snapshot(database, agent_id):
    with database.session_factory() as session:
        return list(
            session.scalars(
                select(FluentBitAgentTopic).where(FluentBitAgentTopic.agent_id == UUID(agent_id))
            )
        )


def test_lost_create_response_cleans_access_before_releasing_reservation(
    integration_client, integration_database
):
    client, database = integration_client, integration_database
    headers, connection_id, names, agent, _ = _setup(client, database, 1)
    real_admin = _worker(database, client).admin

    class LostCreateResponse:
        def __init__(self):
            self.failed = False

        def __getattr__(self, attribute):
            return getattr(real_admin, attribute)

        def create_topic(self, *args):
            real_admin.create_topic(*args)
            if not self.failed:
                self.failed = True
                raise RuntimeError("injected lost create response")

    injected = LostCreateResponse()
    worker = _worker(database, client, injected)
    attached = None
    try:
        assert worker.claim() == UUID(agent["operation_id"])
        worker.execute(UUID(agent["operation_id"]))
        assert injected.failed
        assert len(_assignment_snapshot(database, agent["id"])) == 1
        _retry_now(database, agent["operation_id"])
        restarted = _worker(database, client)
        assert restarted.claim() == UUID(agent["operation_id"])
        restarted.execute(UUID(agent["operation_id"]))
        operation = client.get(
            f"/api/v1/agents/{agent['id']}/operations/{agent['operation_id']}"
        ).json()
        assert operation["state"] == "failed"
        assert operation["safe_error"] == "topic_creation_unconfirmed"
        assert not _assignment_snapshot(database, agent["id"])
        with database.session_factory() as session:
            source = session.scalar(select(Source).where(Source.topic_name == names[0]))
            assert source is not None and source.is_enabled is False
        response = client.post(
            "/api/v1/agents",
            json={
                "name": "explicitly-verified-topic",
                "topics": [
                    {"connection_id": connection_id, "topic_name": names[0], "action": "attach"}
                ],
            },
            headers={**headers, "Idempotency-Key": str(uuid4())},
        )
        assert response.status_code == 202, response.text
        attached = response.json()
        other_worker = _worker(database, client)
        assert other_worker.claim() == UUID(attached["operation_id"])
        other_worker.execute(UUID(attached["operation_id"]))
        assert client.get(f"/api/v1/agents/{attached['id']}").json()["state"] == "ready"
    finally:
        if attached is not None:
            _cleanup(
                client,
                database,
                headers,
                connection_id,
                names,
                attached["id"],
                _worker(database, client),
            )
        _cleanup(
            client, database, headers, connection_id, names, agent["id"], _worker(database, client)
        )
