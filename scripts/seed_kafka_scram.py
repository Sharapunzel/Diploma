"""Seed SCRAM users on a running legacy Kafka volume before switching listeners.

This is the only migration step that contacts the old listener. Passwords are
read from files and sent through the Kafka Admin protocol, never CLI arguments.
"""

import argparse
import sys
from pathlib import Path

from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialUpsertion,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--protocol", choices=("PLAINTEXT", "SSL"), required=True)
    parser.add_argument("--ca-file", type=Path)
    parser.add_argument("--admin-password-file", type=Path, required=True)
    parser.add_argument("--reader-password-file", type=Path, required=True)
    args = parser.parse_args()
    if args.protocol == "SSL" and (args.ca_file is None or not args.ca_file.is_file()):
        parser.error("SSL migration requires a trusted CA file")
    config = {"bootstrap.servers": args.bootstrap, "security.protocol": args.protocol}
    if args.ca_file:
        config["ssl.ca.location"] = str(args.ca_file)
        config["ssl.endpoint.identification.algorithm"] = "https"
    users = {
        "diploma-kafka-admin": args.admin_password_file,
        "diploma-reader": args.reader_password_file,
    }
    changes = []
    passwords = []
    for name, path in users.items():
        password = path.read_text(encoding="utf-8").strip()
        if not password or any(character in password for character in "\r\n\\\";"):
            raise ValueError(f"invalid password file for {name}")
        passwords.append(password)
        changes.append(UserScramCredentialUpsertion(
            name, ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192),
            password.encode("utf-8"),
        ))
    if passwords[0] == passwords[1]:
        raise ValueError("Kafka admin and reader passwords must differ")
    client = AdminClient(config)
    futures = client.alter_user_scram_credentials(changes, request_timeout=15)
    for future in futures.values():
        future.result(20)
    print("Kafka SCRAM credentials seeded; verify authentication after enabling the secure listener")


if __name__ == "__main__":
    try:
        main()
    except (KafkaException, OSError, ValueError) as error:
        print(f"Kafka SCRAM migration failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
