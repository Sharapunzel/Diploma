"""Durably materialize marker commit times before sending slot feedback."""

from ...adapters.pgoutput import DecodedCommit
from ...adapters.protocols import LogicalReplicationStream
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.availability import AvailabilityProgressRepository


class AvailabilityMaterializer:
    def __init__(self, repository: AvailabilityProgressRepository, uow: UnitOfWork,
                 stream: LogicalReplicationStream) -> None:
        self.repository = repository
        self.uow = uow
        self.stream = stream

    def apply(self, commit: DecodedCommit) -> None:
        try:
            self.repository.apply_decoded_commit(commit)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise
        self.stream.acknowledge(commit.end_lsn)

    def heartbeat(self) -> None:
        try:
            self.repository.write_heartbeat()
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise
