"""Shared server composition for HTTP and background aggregate scenarios."""

import os

from sqlalchemy.orm import Session

from .adapters.replication_slot import PostgreSQLReplicationSlotAdapter
from .aggregation.rules import Limits
from .ecs import EcsCatalog
from .repositories.sqlalchemy import SqlAlchemyUnitOfWork
from .repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
from .repositories.sqlalchemy.capture import SqlAlchemyCaptureStateRepository
from .repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from .repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository
from .repositories.sqlalchemy.windows import SqlAlchemyWorkingWindowRepository
from .services.implementations.aggregates import AggregateService
from .services.implementations.wal_guard import CaptureGuard


def build_aggregate_service(session: Session, catalog: EcsCatalog) -> AggregateService:
    limits = Limits(**{name: int(os.environ["AGGREGATE_" + name.upper()])
                       for name in Limits.__dataclass_fields__
                       if "AGGREGATE_" + name.upper() in os.environ})
    if any(value <= 0 for value in vars(limits).values()) or limits.work_mem_mb > 32:
        raise ValueError("invalid aggregate resource limits")
    return AggregateService(SqlAlchemyAggregateRepository(session),
        SqlAlchemyWorkingWindowRepository(session), SqlAlchemyCoverageEvidenceRepository(session),
        CaptureGuard(SqlAlchemyCaptureStateRepository(session), PostgreSQLReplicationSlotAdapter(session)),
        SqlAlchemyUnitOfWork(session), catalog, limits,
        detectors=SqlAlchemyDetectorRepository(session))
