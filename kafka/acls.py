"""Idempotent runtime-reader ACL bootstrap; admin secret stays in a file."""

import subprocess
import sys
from pathlib import Path

from bootstrap import ADMIN_CONFIG, BIN, properties, required, secret


def main() -> None:
    mode = required("DIPLOMA_KAFKA_MODE")
    if mode not in {"development", "production"}:
        raise ValueError("invalid Kafka mode")
    protocol = "SASL_SSL" if mode == "production" else "SASL_PLAINTEXT"
    admin_password = secret("KAFKA_ADMIN_PASSWORD_FILE")
    lines = [
        f"security.protocol={protocol}",
        "sasl.mechanism=SCRAM-SHA-512",
        ("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule "
         f'required username="diploma-kafka-admin" password="{admin_password}";'),
    ]
    if mode == "production":
        ca_file = Path(required("KAFKA_CA_FILE"))
        if not ca_file.is_file():
            raise ValueError("Kafka CA certificate is unavailable")
        lines.extend(["ssl.truststore.type=PEM", f"ssl.truststore.location={ca_file}"])
    properties(ADMIN_CONFIG, lines)
    base = [str(BIN / "kafka-acls.sh"), "--bootstrap-server", "kafka:9092",
            "--command-config", str(ADMIN_CONFIG)]
    principal = ["--allow-principal", "User:diploma-reader"]
    rules = [
        ["--topic", "*", "--operation", "Describe", "--operation", "Read"],
        ["--group", "diploma-source-", "--resource-pattern-type", "prefixed",
         "--operation", "Read"],
        ["--group", "diploma-debug-isolated", "--operation", "Read"],
    ]
    for rule in rules:
        subprocess.run(base + ["--add"] + principal + rule, check=True,
                       stdout=subprocess.DEVNULL)
    subprocess.run(base + ["--list"], check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Kafka ACL bootstrap failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
