"""Administrative detector requests; execution belongs to the managed worker."""

import argparse
import json
import os
from uuid import UUID

from .db import Database
from .detection.contracts import DetectionError
from .detection.execution import REASONS, safe_reason
from .detector_composition import bounded_database_url, build_detector_service
from .ecs import PackagedEcsCatalog


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "start", "stop", "prepare-warmup", "warmup", "advance-one", "state"])
    parser.add_argument("id", type=UUID, help="Aggregate UUID for create, detector UUID otherwise")
    parser.add_argument("--actor", type=UUID)
    parser.add_argument("--name", default="RCF")
    parser.add_argument("--accelerated", action="store_true")
    parser.add_argument("--step", type=int)
    args = parser.parse_args()
    if args.action == "create" and args.actor is None:
        parser.error("create requires --actor")
    if args.action in {"prepare-warmup", "warmup", "advance-one"}:
        print(json.dumps({"error": "managed_worker_required", "message": "Используйте start и state."}, ensure_ascii=False))
        return 1
    # Credentials are supplied by the administrator's environment, never CLI argv.
    from .adapters.wal_bootstrap import read_setting
    from .config import settings
    url = read_setting("DATABASE_URL") if os.environ.get("DATABASE_URL_FILE") else os.environ.get("DATABASE_URL", settings.database_url)
    database = Database(bounded_database_url(url))
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
        code = safe_reason(error)
        print(json.dumps({"error": code, "message": REASONS[code]}, ensure_ascii=False))
        return 1
    except Exception:  # noqa: BLE001 - CLI boundary must not disclose DB/model internals
        print(json.dumps({"error": "temporary_unavailable", "outcome": "unknown",
            "message": "Результат не подтверждён. Прочитайте state перед ручным повтором."}, ensure_ascii=False))
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
