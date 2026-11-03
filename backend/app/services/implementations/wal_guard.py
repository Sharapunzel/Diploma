"""Fail closed if the logical capture slot is unavailable or near its WAL budget."""

from dataclasses import dataclass
from datetime import datetime

from ...adapters.replication_slot import ReplicationSlotAdapter
from ...repositories.protocols.capture import CaptureStateRepository

MIN_SAFE_WAL_BYTES = 256 * 1024 * 1024
MAX_HEARTBEAT_SECONDS = 20


class CaptureUnavailable(RuntimeError):
    """New event writes must be retried while commit capture is unsafe."""


@dataclass(frozen=True, slots=True)
class CaptureStatus:
    ready: bool
    reason: str
    retained_bytes: int | None
    safe_bytes: int | None
    confirmed_lsn: str | None
    heartbeat_at: datetime | None


class CaptureGuard:
    def __init__(self, progress: CaptureStateRepository, slot: ReplicationSlotAdapter,
                 *, min_safe_bytes: int = MIN_SAFE_WAL_BYTES) -> None:
        self.progress = progress
        self.slot = slot
        self.min_safe_bytes = min_safe_bytes

    def status(self) -> CaptureStatus:
        saved = self.progress.read_durable_progress()
        slot = self.slot.read_state()
        if saved is None or slot is None:
            return CaptureStatus(False, "slot_or_progress_missing", None, None, None, None)
        durable_lsn, heartbeat, now = saved
        safe = slot.safe_bytes
        if slot.wal_status == "lost" or safe is None:
            reason = "wal_unavailable"
        elif slot.confirmed_lsn_bytes is None or slot.confirmed_lsn_bytes > durable_lsn:
            reason = "slot_ahead_of_materialization"
        elif (not slot.active or heartbeat is None
              or not 0 <= (now - heartbeat).total_seconds() <= MAX_HEARTBEAT_SECONDS):
            reason = "decoder_unavailable"
        elif safe < self.min_safe_bytes:
            reason = "wal_budget_exhaustion_approaching"
        else:
            reason = "ready"
        return CaptureStatus(reason == "ready", reason, slot.retained_bytes, safe,
                             slot.confirmed_lsn, heartbeat)
