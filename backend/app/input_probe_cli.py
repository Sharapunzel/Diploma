"""Private operator interface for authenticated input coverage reports.

Only the operator runs this process with database credentials. Probes provide
signed files separately and never receive those credentials.
"""

import argparse
import base64
import json
from pathlib import Path
from uuid import UUID

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .repositories.sqlalchemy import SqlAlchemyUnitOfWork
from .repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
from .services.implementations.input_attestations import InputAttestationService
from .wal_bootstrap import read_setting


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    register = commands.add_parser("register")
    register.add_argument("--probe", type=UUID, required=True)
    register.add_argument("--source", type=UUID, required=True)
    register.add_argument("--generation", type=UUID, required=True)
    register.add_argument("--public-key-file", type=Path, required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--report-file", type=Path, required=True)
    submit.add_argument("--signature-file", type=Path, required=True)
    args = parser.parse_args()
    engine = create_engine(read_setting("DATABASE_URL"), pool_pre_ping=True)
    try:
        with Session(engine) as session:
            service = InputAttestationService(
                SqlAlchemyInputAttestationRepository(session), SqlAlchemyUnitOfWork(session),
            )
            if args.action == "register":
                key = base64.b64decode(args.public_key_file.read_text(encoding="ascii").strip(), validate=True)
                service.register_probe(args.probe, args.source, args.generation, key)
            else:
                report = json.loads(args.report_file.read_text(encoding="utf-8"))
                signature = base64.b64decode(args.signature_file.read_text(encoding="ascii").strip(), validate=True)
                service.submit(report, signature)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
