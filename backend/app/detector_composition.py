"""Shared composition root for detector application scenarios."""

from sqlalchemy.engine import make_url

from .adapters.model_runtime import ModelRuntime
from .aggregate_composition import build_aggregate_service
from .detection.model_process import BoundedModelFactory
from .repositories.sqlalchemy import SqlAlchemyUnitOfWork
from .repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
from .repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from .repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository
from .services.implementations.detector_history import DetectorHistoryService
from .services.implementations.detectors import DetectorService


def bounded_database_url(url):
    value = make_url(url)
    options = value.query.get("options", "")
    return value.update_query_dict({"connect_timeout": "5",
        "options": options + " -c statement_timeout=30000 -c lock_timeout=5000",
        "keepalives": "1", "keepalives_idle": "5", "keepalives_interval": "2",
        "keepalives_count": "2", "tcp_user_timeout": "5000"})


def build_detector_service(session, catalog, owner=None, models=None):
    repository = SqlAlchemyDetectorRepository(session)
    uow = SqlAlchemyUnitOfWork(session)
    aggregate = build_aggregate_service(session, catalog)
    history = DetectorHistoryService(repository, SqlAlchemyAggregateRepository(session),
        SqlAlchemyCoverageEvidenceRepository(session), uow, catalog, aggregate.budget_limits)
    return DetectorService(repository, history, aggregate.windows, aggregate.capture, uow, models or BoundedModelFactory(), owner)


def build_worker_detector_service(session, catalog, owner, cancel, transport=None):
    return build_detector_service(session, catalog, owner, BoundedModelFactory(cancel, transport))


class WorkerDetectorServices:
    """Composition scope for one owner; standby never starts a JVM."""
    def __init__(self):
        self.transport = None

    def __call__(self, session, catalog, owner, cancel):
        if self.transport is None:
            self.transport = ModelRuntime()
        return build_worker_detector_service(session, catalog, owner, cancel, self.transport)

    def close(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None
