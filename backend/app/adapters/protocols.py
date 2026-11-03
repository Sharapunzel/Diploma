"""Contracts shared by logical replication adapters and application services."""

from typing import Protocol

from .pgoutput import DecodedCommit


class LogicalReplicationStream(Protocol):
    def read_commit(self, timeout: float) -> DecodedCommit | None: ...
    def acknowledge(self, end_lsn: int) -> None: ...
