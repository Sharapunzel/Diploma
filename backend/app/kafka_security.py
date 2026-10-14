"""Server-owned Kafka authentication settings shared by all client paths."""

from dataclasses import dataclass
from pathlib import Path


class KafkaSecurityConfigurationError(ValueError):
    """A server-owned Kafka security profile is missing or unsafe."""


@dataclass(frozen=True)
class KafkaSecurityProfile:
    protocol: str
    username: str
    password_file: Path
    ca_file: Path | None = None
    dev_allowed_bootstrap_servers: tuple[str, ...] = (
        "127.0.0.1:9092",
        "localhost:9092",
        "[::1]:9092",
        "kafka:9092",
    )

    def client_options(self, bootstrap_servers: tuple[str, ...] = ()) -> dict[str, str]:
        if self.protocol not in {"SASL_PLAINTEXT", "SASL_SSL"}:
            raise KafkaSecurityConfigurationError(
                "Kafka requires an authenticated SASL transport"
            )
        if self.protocol == "SASL_PLAINTEXT":
            allowed = {server.casefold() for server in self.dev_allowed_bootstrap_servers}
            if any(not self._is_local_broker_address(server) for server in allowed):
                raise KafkaSecurityConfigurationError(
                    "SASL_PLAINTEXT allowlist contains a non-local address"
                )
            rejected = [server for server in bootstrap_servers if server.casefold() not in allowed]
            if rejected:
                raise KafkaSecurityConfigurationError(
                    "SASL_PLAINTEXT is restricted to the configured local Kafka broker"
                )
        if not self.username or not self.password_file.is_file():
            raise KafkaSecurityConfigurationError(
                "Kafka runtime credentials are unavailable"
            )
        password = self.password_file.read_text(encoding="utf-8").strip()
        if not password:
            raise KafkaSecurityConfigurationError("Kafka runtime credentials are empty")
        options = {
            "security.protocol": self.protocol,
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": self.username,
            "sasl.password": password,
        }
        if self.protocol == "SASL_SSL":
            if self.ca_file is None or not self.ca_file.is_file():
                raise KafkaSecurityConfigurationError("Kafka CA certificate is unavailable")
            options["ssl.ca.location"] = str(self.ca_file)
            options["ssl.endpoint.identification.algorithm"] = "https"
        return options

    @staticmethod
    def _is_local_broker_address(server: str) -> bool:
        host, separator, port = server.rpartition(":")
        if not separator or not port.isdigit() or not 1 <= int(port) <= 65535:
            return False
        host = host.strip("[]").casefold()
        return host in {"127.0.0.1", "localhost", "::1"} or server.casefold() == "kafka:9092"
