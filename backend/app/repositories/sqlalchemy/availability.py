"""Atomic commit-time materialization for availability markers."""

import logging
import time
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from ...adapters.pgoutput import DecodedCommit, InvalidReplicationMessage
from ...models import AvailabilityMarker, ReplicationProgress

LOGGER = logging.getLogger(__name__)
COMMIT_VISIBILITY_SECONDS = 1.0


class SqlAlchemyAvailabilityProgressRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def write_heartbeat(self) -> None:
        progress = self.session.get(ReplicationProgress, 1)
        if progress is None:
            raise RuntimeError("replication progress missing")
        progress.heartbeat_at = self.session.scalar(select(func.clock_timestamp()))
        self.session.add(AvailabilityMarker(id=uuid4(), source_identity=UUID(int=0),
                                            kind="barrier", event_id=None))

    def apply_decoded_commit(self, commit: DecodedCommit) -> None:
        """Write the complete transaction and position in the caller's DB transaction."""
        # Serialize singleton progress even before its row exists.
        self.session.execute(text("SELECT pg_advisory_xact_lock(1380205360)"))
        progress = self.session.scalar(select(ReplicationProgress).where(
            ReplicationProgress.id == 1).with_for_update())
        if progress is None:
            progress = ReplicationProgress(id=1, durable_lsn=0)
            self.session.add(progress)
            self.session.flush()
        for values in commit.rows:
            if len(values) != 4:
                raise InvalidReplicationMessage("invalid marker shape")
            marker_id, source_id, kind, event_id = values
            try:
                marker_uuid = UUID(marker_id or "")
                source_uuid = UUID(source_id or "")
                event_uuid = UUID(event_id) if event_id else None
            except ValueError as exc:
                raise InvalidReplicationMessage("invalid marker identity") from exc
            marker = self._visible_marker(marker_uuid, commit)
            if (marker is None or marker.source_identity != source_uuid or marker.kind != kind
                    or marker.event_id != event_uuid):
                mismatched = (["missing"] if marker is None else [
                    name for name, actual, expected in (
                        ("source_identity", marker.source_identity, source_uuid),
                        ("kind", marker.kind, kind), ("event_id", marker.event_id, event_uuid),
                    ) if actual != expected
                ])
                raise InvalidReplicationMessage(
                    f"marker does not match durable record: xid={commit.xid} "
                    f"end_lsn={commit.end_lsn} marker={marker_uuid} fields={','.join(mismatched)}")
            if marker.availability_at is not None and marker.availability_at != commit.committed_at:
                raise InvalidReplicationMessage("inconsistent commit time for marker")
            marker.availability_at = commit.committed_at
            if (progress.boundary_barrier_id == marker_uuid
                    and progress.availability_verified_from_at is None):
                progress.availability_verified_from_at = commit.committed_at
                progress.boundary_origin = "upgrade_materialized_barrier"
        progress.durable_lsn = max(int(progress.durable_lsn), commit.end_lsn)
        self.session.flush()

    def _visible_marker(self, marker_id: UUID, commit: DecodedCommit):
        # PostgreSQL emits the durable commit record before ProcArray removes
        # the writer's XID (e.g. while SyncRep waits). A READ COMMITTED SELECT
        # can therefore miss that transaction's insert. Each retry takes a new
        # statement snapshot; it never acknowledges a missing/unverified row.
        query = select(AvailabilityMarker).where(
            AvailabilityMarker.id == marker_id).with_for_update()
        deadline = time.monotonic() + COMMIT_VISIBILITY_SECONDS
        reported = False
        while True:
            marker = self.session.scalar(query)
            if marker is not None:
                return marker
            active = self.session.scalar(text("""SELECT EXISTS (
                SELECT 1 FROM pg_locks WHERE locktype='transactionid'
                AND transactionid=CAST(:xid AS xid) AND mode='ExclusiveLock' AND granted
            )"""), {"xid": str(commit.xid)})
            if not active:
                # The writer may have finished between the missing-row read
                # and lock check. Recheck once in a fresh statement snapshot.
                return self.session.scalar(query)
            if not reported:
                LOGGER.info("availability_commit_visibility_wait xid=%s end_lsn=%s marker=%s",
                            commit.xid, commit.end_lsn, marker_id)
                reported = True
            if time.monotonic() >= deadline:
                raise InvalidReplicationMessage(
                    f"marker commit visibility deadline: xid={commit.xid} "
                    f"end_lsn={commit.end_lsn} marker={marker_id}")
            time.sleep(.005)
