"""Real broker proof for a signed, genuinely empty input interval.

Run with: python -B scripts/agent_test_profiles.py kafka-integration --pytest
    tests/test_window_kafka_evidence.py
"""

import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.kafka import (
    ConfluentKafkaConsumerFactory,
    ConfluentKafkaMetadataClient,
    KafkaAssignment,
    KafkaConnectionConfig,
    KafkaPartition,
)
from app.kafka_security import KafkaSecurityProfile
from app.models import KafkaCheckpoint, KafkaConnection, Source
from app.repositories.protocols.consumers import ConsumerSourceConfig
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository
from app.repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
from app.services.implementations.consumers import _SourceWorker
from app.services.implementations.input_attestations import (
    InputAttestationService,
    canonical_payload,
)
from app.services.implementations.saved_coverage import evaluate_saved_coverage


def kafka_test_profile() -> KafkaSecurityProfile:
    return KafkaSecurityProfile(
        os.environ["KAFKA_TEST_SECURITY_PROTOCOL"], "diploma-reader",
        Path(os.environ["KAFKA_TEST_READER_PASSWORD_FILE"]),
        Path(os.environ["KAFKA_TEST_CA_FILE"]) if os.getenv("KAFKA_TEST_CA_FILE") else None,
        tuple(os.environ["KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS"].split(",")),
    )


def kafka_test_admin_config() -> dict[str, str]:
    config = {
        "bootstrap.servers": os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ["KAFKA_TEST_SECURITY_PROTOCOL"],
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "diploma-kafka-admin",
        "sasl.password": Path(os.environ["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
    }
    if ca_file := os.getenv("KAFKA_TEST_CA_FILE"):
        config["ssl.ca.location"] = ca_file
    return config


def test_signed_empty_input_with_actual_broker_checkpoint_and_backlog(tmp_path):
    if not os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS"):
        pytest.skip("kafka-integration profile required")
    url = os.environ["TEST_DATABASE_URL"]
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                   env=env, check=True, capture_output=True)
    engine = create_engine(url)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    topic = f"task30-empty-{uuid4().hex}"
    admin = AdminClient(kafka_test_admin_config())
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(15)
    consumer = None
    try:
        profile = kafka_test_profile()
        assert profile is not None
        bootstrap = tuple(os.environ["KAFKA_TEST_BOOTSTRAP_SERVERS"].split(","))
        metadata_client = ConfluentKafkaMetadataClient(profile)
        metadata = metadata_client.metadata(KafkaConnectionConfig(bootstrap, profile.protocol), 10)
        topic_info = next(item for item in metadata.topics if item.name == topic)
        assert topic_info.identity and topic_info.partition_count == 1
        source_id, connection_id, probe_id, generation = (uuid4() for _ in range(4))
        with sessions.begin() as session:
            session.add(KafkaConnection(id=connection_id, name=str(connection_id),
                                        cluster_identity=metadata.cluster_identity,
                                        bootstrap_servers=list(bootstrap)))
            session.add(Source(id=source_id, name=str(source_id), connection_id=connection_id,
                               topic_name=topic, kafka_topic_identity=topic_info.identity,
                               is_enabled=True))
        private = Ed25519PrivateKey.generate()
        public = private.public_key().public_bytes(serialization.Encoding.Raw,
                                                   serialization.PublicFormat.Raw)
        with sessions() as session:
            InputAttestationService(SqlAlchemyInputAttestationRepository(session),
                                    SqlAlchemyUnitOfWork(session)).register_probe(
                probe_id, source_id, generation, public,
            )
        consumer = ConfluentKafkaConsumerFactory(security_profile=profile).create(
            KafkaConnectionConfig(bootstrap, profile.protocol), f"diploma-source-{source_id}",
        )
        partition = KafkaPartition(topic, 0, 0)
        consumer.subscribe(topic)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assignment = consumer.poll(0.1)
            if isinstance(assignment, KafkaAssignment):
                break
        else:
            raise AssertionError("consumer group assignment was not received")
        consumer.assign((partition,))
        consumer.commit((partition,), 10)
        config = ConsumerSourceConfig(source_id, connection_id, bootstrap, profile.protocol,
                                      topic, topic_info.identity)
        worker = _SourceWorker(config, sessions, None, metadata_client, None, 10, 0.1, 0.1)
        worker.consumer = consumer
        worker.expected_offsets[0] = 0
        worker._record_checkpoint()
        start = datetime.now(UTC)
        time.sleep(0.2)
        end = datetime.now(UTC)
        worker._record_checkpoint()
        report = {"id": str(uuid4()), "probe_id": str(probe_id),
                  "source_id": str(source_id), "input_generation": str(generation),
                  "sequence": 1, "start": start.isoformat(), "end": end.isoformat(),
                  "state": "covered", "topic_identity": topic_info.identity,
                  "start_positions": {"0": 0}, "end_positions": {"0": 0}}
        signature = private.sign(canonical_payload(report))
        with sessions() as session:
            InputAttestationService(SqlAlchemyInputAttestationRepository(session),
                                    SqlAlchemyUnitOfWork(session)).submit(report, signature)
        barrier_at = datetime.now(UTC)
        worker._record_checkpoint()
        with sessions() as session:
            assert session.scalar(select(KafkaCheckpoint.id).where(
                KafkaCheckpoint.source_identity == source_id)) is not None
            state, reason, refs = evaluate_saved_coverage(
                SqlAlchemyCoverageEvidenceRepository(session), source_id,
                start, end, barrier_at,
            )
            assert state == "complete", (state, reason)
            assert refs["attestations"] == [report["id"]]
        gap_start = datetime.now(UTC)
        worker._set_state("retrying", "kafka_poll_timeout")
        time.sleep(0.02)
        gap_end = datetime.now(UTC)
        worker._record_checkpoint()
        gap_report = dict(report, id=str(uuid4()), sequence=2,
                          start=gap_start.isoformat(), end=gap_end.isoformat())
        with sessions() as session:
            InputAttestationService(SqlAlchemyInputAttestationRepository(session),
                                    SqlAlchemyUnitOfWork(session)).submit(
                gap_report, private.sign(canonical_payload(gap_report)),
            )
        gap_barrier = datetime.now(UTC)
        worker._record_checkpoint()
        with sessions() as session:
            assert evaluate_saved_coverage(
                SqlAlchemyCoverageEvidenceRepository(session), source_id,
                gap_start, gap_end, gap_barrier,
            )[:2] == ("unknown", "consumer_coverage_gap")
        stopped_window_start = datetime.now(UTC)
        stopped_worker = _SourceWorker(config, sessions, None, metadata_client, None, 10, 0.1, 0.1)
        stopped_worker.stop(lifecycle=True)
        stopped_worker._run()
        stopped_worker._end_processing_stop()
        stopped_window_end = datetime.now(UTC)
        stopped_report = dict(report, id=str(uuid4()), sequence=3,
                              start=stopped_window_start.isoformat(), end=stopped_window_end.isoformat())
        with sessions() as session:
            InputAttestationService(SqlAlchemyInputAttestationRepository(session),
                                    SqlAlchemyUnitOfWork(session)).submit(
                stopped_report, private.sign(canonical_payload(stopped_report)),
            )
        stopped_barrier = datetime.now(UTC)
        worker._record_checkpoint()
        with sessions() as session:
            assert evaluate_saved_coverage(
                SqlAlchemyCoverageEvidenceRepository(session), source_id,
                stopped_window_start, stopped_window_end, stopped_barrier,
            )[0] == "interrupted"
        producer = Producer(kafka_test_admin_config())
        producer.produce(topic, b"late-unprocessed")
        assert producer.flush(10) == 0
        worker._record_checkpoint()
        with sessions() as session:
            state, reason, _ = evaluate_saved_coverage(
                SqlAlchemyCoverageEvidenceRepository(session), source_id,
                start, end, barrier_at,
            )
            assert (state, reason) == ("unknown", "kafka_backlog_or_retention")
    finally:
        if consumer is not None:
            consumer.close()
        admin.delete_topics([topic])[topic].result(15)
        engine.dispose()
