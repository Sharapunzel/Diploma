"""Kafka operations available only to the isolated provisioning process."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .kafka import KafkaConnectionConfig, KafkaMetadataError, kafka_failure_kind


@dataclass(frozen=True)
class AgentKafkaAdminProfile:
    protocol: str
    username: str
    password_file: Path
    ca_file: Path | None = None

    def options(self, bootstrap_servers: tuple[str, ...]) -> dict[str, str]:
        if self.protocol not in {"SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("Kafka provisioning requires SASL transport")
        if self.protocol == "SASL_PLAINTEXT" and any(
            not self._local(server) for server in bootstrap_servers
        ):
            raise ValueError("SASL_PLAINTEXT is restricted to local Kafka")
        password = self.password_file.read_text(encoding="utf-8").strip()
        if not password or not self.username:
            raise ValueError("Kafka provisioning credentials are unavailable")
        result = {
            "security.protocol": self.protocol,
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": self.username,
            "sasl.password": password,
        }
        if self.protocol == "SASL_SSL":
            if self.ca_file is None or not self.ca_file.is_file():
                raise ValueError("Kafka CA is unavailable")
            result["ssl.ca.location"] = str(self.ca_file)
            result["ssl.endpoint.identification.algorithm"] = "https"
        return result

    @staticmethod
    def _local(address: str) -> bool:
        if address == "kafka:9092":
            return True
        host, separator, port = address.rpartition(":")
        return bool(
            separator
            and host in {"localhost", "127.0.0.1", "[::1]"}
            and port.isdigit()
            and 0 < int(port) < 65536
        )


class ConfluentAgentKafkaAdmin:
    def __init__(self, profile: AgentKafkaAdminProfile) -> None:
        self.profile = profile

    def _client(self, config: KafkaConnectionConfig):
        from confluent_kafka.admin import AdminClient

        return AdminClient(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                **self.profile.options(config.bootstrap_servers),
                "allow.auto.create.topics": False,
            }
        )

    def create_topic(
        self, config: KafkaConnectionConfig, name: str, operation_id: str, timeout: float
    ) -> None:
        from confluent_kafka.admin import NewTopic

        # Kafka has no user-defined topic metadata for operation markers. If a
        # create response is lost, an existing name is therefore an explicit
        # recovery conflict, never evidence that this operation created it.
        del operation_id
        client = self._client(config)
        futures = client.create_topics(
            [NewTopic(name, num_partitions=3, replication_factor=1)],
            request_timeout=timeout,
        )
        self._resolve(futures, timeout, allow_exists=False)

    def grant_write(
        self, config: KafkaConnectionConfig, topic: str, principal: str, timeout: float
    ) -> None:
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import (
            AclBinding,
            AclOperation,
            AclPermissionType,
            ResourcePatternType,
            ResourceType,
        )

        try:
            client = self._client(config)
            binding = AclBinding(
                ResourceType.TOPIC,
                topic,
                ResourcePatternType.LITERAL,
                "User:" + principal,
                "*",
                AclOperation.WRITE,
                AclPermissionType.ALLOW,
            )
            self._resolve(client.create_acls([binding]), timeout)
        except KafkaException as error:
            raise KafkaMetadataError(kafka_failure_kind(error)) from error

    def remove_principal(
        self, config: KafkaConnectionConfig, principal: str, timeout: float
    ) -> None:
        from confluent_kafka import KafkaError
        from confluent_kafka.admin import (
            AclBindingFilter,
            AclOperation,
            AclPermissionType,
            ResourcePatternType,
            ResourceType,
        )

        client = self._client(config)
        binding = AclBindingFilter(
            ResourceType.ANY,
            None,
            ResourcePatternType.ANY,
            "User:" + principal,
            None,
            AclOperation.ANY,
            AclPermissionType.ANY,
        )
        self._resolve(client.delete_acls([binding]), timeout)
        try:
            result = client.describe_user_scram_credentials([principal])[principal].result(timeout)
        except Exception as error:
            kafka_error = error.args[0] if error.args else None
            if getattr(kafka_error, "code", lambda: None)() == KafkaError.RESOURCE_NOT_FOUND:
                return
            raise
        if getattr(result, "scram_credential_infos", ()):
            self._resolve(
                client.alter_user_scram_credentials(
                    [self._scram_delete(principal)], request_timeout=timeout
                ),
                timeout,
            )

    def remove_write(
        self, config: KafkaConnectionConfig, topic: str, principal: str, timeout: float
    ) -> None:
        from confluent_kafka.admin import (
            AclBindingFilter,
            AclOperation,
            AclPermissionType,
            ResourcePatternType,
            ResourceType,
        )

        binding = AclBindingFilter(
            ResourceType.TOPIC,
            topic,
            ResourcePatternType.LITERAL,
            "User:" + principal,
            None,
            AclOperation.WRITE,
            AclPermissionType.ALLOW,
        )
        client = self._client(config)
        futures = client.delete_acls([binding])
        self._resolve(futures, timeout)

    def set_credential(
        self, config: KafkaConnectionConfig, principal: str, password: str, timeout: float
    ) -> None:

        from .agent_credentials import scram_password

        client = self._client(config)
        credentials = client.alter_user_scram_credentials(
            [scram_password(principal, password)], request_timeout=timeout
        )
        self._resolve(credentials, timeout)

    @staticmethod
    def _scram_delete(principal: str):
        from confluent_kafka.admin import ScramMechanism, UserScramCredentialDeletion

        return UserScramCredentialDeletion(principal, ScramMechanism.SCRAM_SHA_512)

    @staticmethod
    def _resolve(futures, timeout: float, allow_exists: bool = True) -> None:
        from confluent_kafka import KafkaError

        for future in futures.values():
            try:
                future.result(timeout)
            except Exception as error:
                code = getattr(error, "code", lambda: None)()
                if allow_exists and code == KafkaError.TOPIC_ALREADY_EXISTS:
                    continue
                if code == KafkaError.TOPIC_ALREADY_EXISTS:
                    raise KafkaMetadataError("topic_exists_unexpectedly") from error
                if code == KafkaError._TIMED_OUT:
                    raise KafkaMetadataError("timeout") from error
                raise KafkaMetadataError("unavailable") from error
