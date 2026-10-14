"""One-time agent credential and Kafka SCRAM helpers."""

from __future__ import annotations

import secrets


def new_password() -> str:
    return secrets.token_urlsafe(48)


def principal(agent_id: str, revision: int) -> str:
    return f"diploma-agent-{agent_id.replace('-', '')}-r{revision}"


def scram_password(username: str, password: str):
    from confluent_kafka.admin import (
        ScramCredentialInfo,
        ScramMechanism,
        UserScramCredentialUpsertion,
    )

    # Kafka receives the password and derives the SCRAM verifier with its own salt.
    return UserScramCredentialUpsertion(
        username,
        ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192),
        password.encode("utf-8"),
    )


def encrypt_pending(key_file, username: str, password: str) -> str:
    from cryptography.fernet import Fernet

    key = key_file.read_bytes().strip()
    return Fernet(key).encrypt(f"{username}\n{password}".encode()).decode()


def decrypt_pending(key_file, encrypted: str) -> tuple[str, str]:
    from cryptography.fernet import Fernet

    username, password = (
        Fernet(key_file.read_bytes().strip()).decrypt(encrypted.encode()).decode().split("\n", 1)
    )
    return username, password
