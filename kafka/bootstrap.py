"""Prepare a single-node Kafka broker without exporting plaintext secrets."""

import base64
import hashlib
import os
import secrets
import subprocess
import sys
from pathlib import Path

BIN = Path("/opt/kafka/bin")
DATA = Path("/var/lib/kafka/data")
CONFIG = Path("/tmp/diploma-kafka-server.properties")
ADMIN_CONFIG = Path("/tmp/diploma-kafka-admin.properties")


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ValueError(f"{name} is required")
    return value


def secret(name: str) -> str:
    value = Path(required(name)).read_text(encoding="utf-8").strip()
    if not value or any(character in value for character in "\r\n\\\";"):
        raise ValueError(f"invalid {name} file")
    return value


def properties(path: Path, lines: list[str]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write("\n".join(lines) + "\n")


def scram(name: str, password: str) -> str:
    salt = secrets.token_bytes(32)
    salted = hashlib.pbkdf2_hmac("sha512", password.encode(), salt, 8192)
    return ("SCRAM-SHA-512=[name=" + name + ",iterations=8192,salt=\""
            + base64.b64encode(salt).decode() + "\",saltedpassword=\""
            + base64.b64encode(salted).decode() + "\"]")


def main() -> None:
    mode = required("DIPLOMA_KAFKA_MODE")
    if mode not in {"development", "production"}:
        raise ValueError("invalid Kafka mode")
    bind = required("KAFKA_BIND_ADDRESS")
    if mode == "development" and bind not in {"127.0.0.1", "::1"}:
        raise ValueError("development Kafka must bind only to loopback")
    public_host = required("KAFKA_PUBLIC_HOST")
    public_port = int(required("KAFKA_HOST_PORT"))
    external_port = int(os.environ.get("KAFKA_EXTERNAL_PORT", "9094"))
    if not 1 <= public_port <= 65535 or any(c in public_host for c in "\r\n, /\\"):
        raise ValueError("invalid advertised Kafka endpoint")
    if not 1 <= external_port <= 65535 or external_port in {9092, 9093}:
        raise ValueError("invalid Kafka external listener port")
    admin_name = "diploma-kafka-admin"
    reader_name = "diploma-reader"
    admin_password = secret("KAFKA_ADMIN_PASSWORD_FILE")
    reader_password = secret("KAFKA_READER_PASSWORD_FILE")
    if admin_password == reader_password:
        raise ValueError("Kafka admin and reader passwords must differ")
    protocol = "SASL_SSL" if mode == "production" else "SASL_PLAINTEXT"
    lines = [
        "node.id=1", "process.roles=broker,controller",
        "controller.quorum.voters=1@127.0.0.1:9093",
        "controller.listener.names=CONTROLLER",
        f"listeners=INTERNAL://:9092,CONTROLLER://127.0.0.1:9093,EXTERNAL://:{external_port}",
        f"advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://{public_host}:{public_port}",
        f"listener.security.protocol.map=INTERNAL:{protocol},CONTROLLER:PLAINTEXT,EXTERNAL:{protocol}",
        "inter.broker.listener.name=INTERNAL",
        "sasl.enabled.mechanisms=SCRAM-SHA-512",
        "sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512",
        "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "allow.everyone.if.no.acl.found=false",
        f"super.users=User:{admin_name};User:ANONYMOUS",
        "log.dirs=/var/lib/kafka/data",
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "group.initial.rebalance.delay.ms=0",
        "num.partitions=3",
        "auto.create.topics.enable=false",
    ]
    for listener in ("internal", "external"):
        lines.append(
            f"listener.name.{listener}.scram-sha-512.sasl.jaas.config="
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{admin_name}" password="{admin_password}";'
        )
    if mode == "production":
        broker_pem = Path(required("KAFKA_BROKER_PEM_FILE"))
        ca_file = Path(required("KAFKA_CA_FILE"))
        if not broker_pem.is_file() or not ca_file.is_file():
            raise ValueError("Kafka TLS files are unavailable")
        lines.extend([
            "ssl.keystore.type=PEM",
            f"ssl.keystore.location={broker_pem}",
            "ssl.client.auth=none",
            "ssl.truststore.type=PEM",
            f"ssl.truststore.location={ca_file}",
        ])
    properties(CONFIG, lines)
    client_lines = [
        f"security.protocol={protocol}",
        "sasl.mechanism=SCRAM-SHA-512",
        ("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule "
         f'required username="{admin_name}" password="{admin_password}";'),
    ]
    if mode == "production":
        client_lines.extend(["ssl.truststore.type=PEM", f"ssl.truststore.location={ca_file}"])
    properties(ADMIN_CONFIG, client_lines)
    del admin_password, reader_password
    if not (DATA / "meta.properties").exists():
        command = [str(BIN / "kafka-storage.sh"), "format", "--config", str(CONFIG),
                   "--cluster-id", required("CLUSTER_ID")]
        command.extend(["--add-scram", scram(admin_name, secret("KAFKA_ADMIN_PASSWORD_FILE"))])
        command.extend(["--add-scram", scram(reader_name, secret("KAFKA_READER_PASSWORD_FILE"))])
        subprocess.run(command, check=True)
    os.execv(str(BIN / "kafka-server-start.sh"),
             [str(BIN / "kafka-server-start.sh"), str(CONFIG)])


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "acls":
            from acls import main as acl_main
            acl_main()
        else:
            main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Kafka secure startup failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
