import os
import subprocess
import sys
from pathlib import Path

import pytest
from confluent_kafka import KafkaError
from pydantic import ValidationError

from app.config import Settings
from app.kafka import (
    ConfluentKafkaConsumerFactory,
    ConfluentKafkaMetadataClient,
    KafkaConnectionConfig,
    KafkaConsumerError,
    KafkaFailureObserver,
    KafkaMetadataError,
    client_security_options,
    diagnose_security_failure,
    kafka_failure_kind,
)
from app.kafka_debug import ConfluentDebugReader, DebugReadError
from app.kafka_security import KafkaSecurityConfigurationError, KafkaSecurityProfile

ROOT = Path(__file__).resolve().parents[2]


def test_server_profile_overrides_persisted_plaintext(tmp_path: Path):
    password_file = tmp_path / "reader_password"
    password_file.write_text("test-secret\n", encoding="utf-8")
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test-ca", encoding="utf-8")
    profile = KafkaSecurityProfile("SASL_SSL", "runtime-reader", password_file, ca_file)

    options = client_security_options(
        KafkaConnectionConfig(("kafka:9092",), "PLAINTEXT"), profile
    )

    assert options == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "runtime-reader",
        "sasl.password": "test-secret",
        "ssl.ca.location": str(ca_file),
        "ssl.endpoint.identification.algorithm": "https",
    }


def test_missing_profile_never_falls_back_to_persisted_plaintext():
    with pytest.raises(KafkaSecurityConfigurationError, match="profile is required"):
        client_security_options(
            KafkaConnectionConfig(("remote.example:9092",), "PLAINTEXT"), None
        )


@pytest.mark.parametrize(
    "bootstrap",
    ["127.0.0.1:9092", "localhost:9092", "[::1]:9092", "kafka:9092"],
)
def test_dev_sasl_plaintext_accepts_only_confirmed_local_addresses(
    tmp_path: Path, bootstrap: str
):
    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    options = client_security_options(
        KafkaConnectionConfig((bootstrap,), "PLAINTEXT"),
        KafkaSecurityProfile("SASL_PLAINTEXT", "reader", password_file),
    )
    assert options["security.protocol"] == "SASL_PLAINTEXT"


@pytest.mark.parametrize(
    "bootstrap", ["remote.example:9092", "127.0.0.2:9092", "kafka:9093"]
)
def test_dev_sasl_plaintext_refuses_unconfirmed_bootstrap(
    tmp_path: Path, bootstrap: str
):
    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    with pytest.raises(KafkaSecurityConfigurationError, match="restricted"):
        client_security_options(
            KafkaConnectionConfig((bootstrap,), "PLAINTEXT"),
            KafkaSecurityProfile("SASL_PLAINTEXT", "reader", password_file),
        )


def test_dev_sasl_plaintext_rejects_an_unsafe_server_allowlist(tmp_path: Path):
    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    profile = KafkaSecurityProfile(
        "SASL_PLAINTEXT", "reader", password_file,
        dev_allowed_bootstrap_servers=("broker.example:9092",),
    )
    with pytest.raises(KafkaSecurityConfigurationError, match="allowlist"):
        profile.client_options(("broker.example:9092",))


def test_metadata_consumer_and_debug_share_plaintext_address_policy(tmp_path: Path):
    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    profile = KafkaSecurityProfile("SASL_PLAINTEXT", "reader", password_file)
    config = KafkaConnectionConfig(("remote.example:9092",), "PLAINTEXT")

    with pytest.raises(KafkaMetadataError) as metadata_error:
        ConfluentKafkaMetadataClient(profile).metadata(config, 1)
    assert metadata_error.value.kind == "configuration"

    with pytest.raises(KafkaConsumerError) as consumer_error:
        ConfluentKafkaConsumerFactory(security_profile=profile).create(config, "group")
    assert consumer_error.value.kind == "configuration"

    with pytest.raises(DebugReadError) as debug_error:
        ConfluentDebugReader(profile).read(config, "events", 1)
    assert debug_error.value.kind == "configuration"


@pytest.mark.parametrize("protocol", ["PLAINTEXT", "SSL", "SASL_SSL"])
def test_insecure_or_incomplete_profile_fails_closed(tmp_path: Path, protocol: str):
    password_file = tmp_path / "reader_password"
    password_file.write_text("secret", encoding="utf-8")
    profile = KafkaSecurityProfile(protocol, "reader", password_file)
    with pytest.raises(ValueError):
        profile.client_options()


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        (KafkaError._SSL, "tls"),
        (KafkaError._AUTHENTICATION, "authentication"),
        (KafkaError.TOPIC_AUTHORIZATION_FAILED, "authorization"),
        (KafkaError.GROUP_AUTHORIZATION_FAILED, "authorization"),
        (KafkaError._TIMED_OUT, "timeout"),
        (KafkaError._TRANSPORT, "unavailable"),
    ],
)
def test_kafka_failure_categories_do_not_expose_broker_details(code: int, kind: str):
    assert kafka_failure_kind(KafkaError(code, "sensitive broker detail")) == kind


def test_observed_security_failures_are_scoped_to_one_client_and_request():
    mixed_bootstrap_client = KafkaFailureObserver()
    network_only_client = KafkaFailureObserver()
    mixed_bootstrap_client(KafkaError(KafkaError._TRANSPORT, "unreachable IPv6 address"))
    mixed_bootstrap_client(KafkaError(KafkaError._SSL, "certificate detail"))

    assert mixed_bootstrap_client.classify(
        KafkaError(KafkaError._ALL_BROKERS_DOWN, "redacted")
    ) == "tls"
    assert network_only_client.classify(
        KafkaError(KafkaError._ALL_BROKERS_DOWN, "redacted")
    ) == "unavailable"
    timed_out_request = KafkaFailureObserver()
    timed_out_request(KafkaError(KafkaError._SSL, "certificate detail"))
    assert timed_out_request.classify(KafkaError(KafkaError._TIMED_OUT)) == "tls"
    direct_failure = KafkaFailureObserver()
    direct_failure(KafkaError(KafkaError._AUTHENTICATION))
    assert direct_failure.classify(KafkaError(KafkaError._SSL)) == "tls"
    assert direct_failure.classify(KafkaError(KafkaError._ALL_BROKERS_DOWN)) == "unavailable"
    recovered_client = KafkaFailureObserver()
    recovered_client(KafkaError(KafkaError._SSL, "certificate detail"))
    recovered_client.clear()
    assert recovered_client.classify(KafkaError(KafkaError._ALL_BROKERS_DOWN)) == "unavailable"


def test_diagnostic_probes_preserve_tls_failure_with_unreachable_ipv6(
    monkeypatch, tmp_path: Path
):
    password_file = tmp_path / "password"
    password_file.write_text("secret", encoding="utf-8")
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test-ca", encoding="utf-8")
    families = []

    class Probe:
        def __init__(self, options):
            self.options = options
            self.polled = False
            families.append(options["broker.address.family"])

        def subscribe(self, topics):
            assert topics

        def poll(self, timeout):
            if not self.polled:
                self.polled = True
                code = (
                    KafkaError._SSL
                    if self.options["broker.address.family"] == "v4"
                    else KafkaError._TRANSPORT
                )
                self.options["error_cb"](KafkaError(code, "private broker detail"))
        def close(self):
            pass

    monkeypatch.setattr("confluent_kafka.Consumer", Probe)
    profile = KafkaSecurityProfile("SASL_SSL", "reader", password_file, ca_file)
    assert diagnose_security_failure(
        KafkaConnectionConfig(("localhost:19120",), "PLAINTEXT"), profile
    ) == "tls"
    assert families == ["v4", "v6"]


def test_dev_broker_refuses_non_loopback_publication():
    environment = {
        **os.environ,
        "DIPLOMA_KAFKA_MODE": "development",
        "KAFKA_BIND_ADDRESS": "0.0.0.0",
        "KAFKA_PUBLIC_HOST": "127.0.0.1",
        "KAFKA_HOST_PORT": "19092",
    }
    result = subprocess.run(
        [sys.executable, str(ROOT / "kafka" / "bootstrap.py")],
        env=environment, capture_output=True, text=True, timeout=5, check=False,
    )
    assert result.returncode == 1
    assert "Kafka secure startup failed" in result.stderr


def test_production_settings_reject_insecure_kafka_transport(tmp_path: Path):
    with pytest.raises(ValidationError, match="production Kafka requires SASL_SSL"):
        Settings(
            environment="production",
            session_cookie_secure=True,
            trusted_hosts=["example.test"],
            forwarded_allow_ips=["172.30.0.10"],
            oidc_state_secret="unique-production-state-secret-at-least-32-bytes",
            external_secret_key="FjzCZG1bo4T5WBYB_oBUrhyoSEkbxsOgCwMsNXCEsVw=",
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_runtime_username="diploma-reader",
            kafka_runtime_password_file=tmp_path / "reader_password",
        )


def test_direct_development_app_cannot_start_without_kafka_profile():
    from fastapi.testclient import TestClient

    from app.db import Database
    from app.main import create_app

    database = Database("sqlite://")
    app = create_app(Settings(environment="development"), database=database)
    try:
        with pytest.raises(
            RuntimeError, match="Kafka security profile is required"
        ), TestClient(app):
            pass
    finally:
        database.dispose()


def test_development_profile_rejects_missing_runtime_credentials():
    with pytest.raises(ValidationError, match="Kafka runtime credentials are required"):
        Settings(environment="development", kafka_security_protocol="SASL_PLAINTEXT")
