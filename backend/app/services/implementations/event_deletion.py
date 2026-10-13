from uuid import UUID

from ...core.errors import DomainError
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.events import EventQueryRepository, EventSourceRepository
from ...repositories.protocols.parsed_logs import ParsedLogRepository
from ..protocols.event_deletion import EventIdDecoder


class LocalEventDeletionService:
    def __init__(
        self,
        sources: EventSourceRepository,
        events: EventQueryRepository,
        ids: EventIdDecoder,
        logs: ParsedLogRepository,
        unit_of_work: UnitOfWork,
    ):
        self.sources = sources
        self.events = events
        self.ids = ids
        self.logs = logs
        self.unit_of_work = unit_of_work

    def delete(self, source_id: UUID, event_id: str) -> None:
        source = self.sources.source_type(source_id)
        if source is None or source[0] != "kafka":
            raise DomainError("event_not_found", "Event not found", 404)
        identity = self.ids.decode(event_id)
        try:
            log = self.events.get(source_id, identity)
            if log is None:
                raise DomainError("event_not_found", "Event not found", 404)
            self.logs.delete(log)
            self.unit_of_work.commit()
        except DomainError:
            self.unit_of_work.rollback()
            raise
        except Exception as error:
            self.unit_of_work.rollback()
            raise DomainError("internal_error", "Internal server error", 500) from error
