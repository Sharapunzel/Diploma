"""PostgreSQL replication-slot state, separate from application tables."""

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.orm import Session

from .wal_bootstrap import SLOT


@dataclass(frozen=True, slots=True)
class ReplicationSlotState:
    active: bool
    wal_status: str | None
    safe_bytes: int | None
    confirmed_lsn: str | None
    confirmed_lsn_bytes: int | None
    retained_bytes: int | None


class ReplicationSlotAdapter(Protocol):
    def read_state(self) -> ReplicationSlotState | None: ...


class PostgreSQLReplicationSlotAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    def read_state(self) -> ReplicationSlotState | None:
        row = self.session.execute(text("""SELECT active, wal_status, safe_wal_size,
            confirmed_flush_lsn::text,
            pg_wal_lsn_diff(confirmed_flush_lsn, '0/0'::pg_lsn),
            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
            FROM pg_replication_slots
            WHERE slot_name=:slot AND plugin='pgoutput' AND database=current_database()"""),
            {"slot": SLOT}).one_or_none()
        if row is None:
            return None
        return ReplicationSlotState(bool(row[0]), row[1],
            int(row[2]) if row[2] is not None else None, row[3],
            int(row[4]) if row[4] is not None else None,
            int(row[5]) if row[5] is not None else None)
