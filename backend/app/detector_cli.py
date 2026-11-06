"""Bounded administrative detector commands. No daemon and no HTTP surface."""

import argparse
import json
import os
from uuid import UUID

from .aggregate_composition import build_aggregate_service
from .db import Database
from .detection.contracts import DetectionError
from .detection.model_process import BoundedModelFactory
from .ecs import PackagedEcsCatalog
from .repositories.sqlalchemy import SqlAlchemyUnitOfWork
from .repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
from .repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from .repositories.sqlalchemy.evidence import SqlAlchemyCoverageEvidenceRepository
from .services.implementations.detector_history import DetectorHistoryService
from .services.implementations.detectors import DetectorService


def build_detector_service(session, catalog):
    repository = SqlAlchemyDetectorRepository(session)
    uow = SqlAlchemyUnitOfWork(session)
    aggregate = build_aggregate_service(session, catalog)
    history = DetectorHistoryService(repository, SqlAlchemyAggregateRepository(session),
        SqlAlchemyCoverageEvidenceRepository(session), uow, catalog, aggregate.budget_limits)
    return DetectorService(repository, history, aggregate.windows, aggregate.capture, uow, BoundedModelFactory())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "prepare-warmup", "warmup", "advance-one", "state"])
    parser.add_argument("id", type=UUID, help="Aggregate UUID for create, detector UUID otherwise")
    parser.add_argument("--actor", type=UUID)
    parser.add_argument("--name", default="RCF")
    parser.add_argument("--accelerated", action="store_true")
    parser.add_argument("--step", type=int)
    args = parser.parse_args()
    if args.action == "create" and args.actor is None:
        parser.error("create requires --actor")
    # Credentials are supplied by the administrator's environment, never CLI argv.
    from .config import settings
    database = Database(os.environ.get("DATABASE_URL", settings.database_url))
    try:
        with database.session_factory() as session:
            service = build_detector_service(session, PackagedEcsCatalog.load())
            if args.action == "create":
                result = service.create(args.id, args.actor, args.name,
                                        accelerated=args.accelerated, step=args.step)
            else:
                result = getattr(service, args.action.replace("-", "_"))(args.id)
            print(json.dumps(result, default=str, ensure_ascii=False))
        return 0
    except DetectionError as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        return 1
    except Exception:  # noqa: BLE001 - CLI boundary must not disclose DB/model internals
        print(json.dumps({"error": "detector_operation_failed; reread state before retry"}))
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
