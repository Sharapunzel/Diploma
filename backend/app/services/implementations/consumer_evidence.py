"""Persist consumer coverage and known processing interruptions."""

from uuid import UUID

from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.consumers import ConsumerStateRepository


class ConsumerEvidenceService:
    def __init__(self, repository: ConsumerStateRepository, uow: UnitOfWork) -> None:
        self.repository = repository
        self.uow = uow

    def has_active_probe(self, source_id: UUID) -> bool:
        return self.repository.has_active_input_probe(source_id)

    def record_checkpoint(self, source_id: UUID, topic_id: str, partitions: dict, worker_instance: UUID) -> bool:
        try:
            saved = self.repository.save_checkpoint(source_id, topic_id, partitions, worker_instance)
            self.uow.commit()
            return saved
        except Exception:
            self.uow.rollback()
            raise

    def mark_stopped(self, source_id: UUID) -> None:
        try:
            self.repository.mark_processing_stopped(source_id)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise

    def mark_resumed(self, source_id: UUID) -> None:
        try:
            self.repository.mark_processing_resumed(source_id)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise
