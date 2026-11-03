"""Persistence contract for decoded availability commits."""

from typing import Protocol

from ...adapters.pgoutput import DecodedCommit


class AvailabilityProgressRepository(Protocol):
    def apply_decoded_commit(self, commit: DecodedCommit) -> None: ...
    def write_heartbeat(self) -> None: ...
