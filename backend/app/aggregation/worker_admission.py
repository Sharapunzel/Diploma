"""Bounded aggregate work admission; durable windows remain in the database."""

from collections import deque
from dataclasses import dataclass
from uuid import UUID

MIB = 1024 * 1024
TASK_MEMORY_BYTES = 512 * MIB
MIN_RESERVATION_BYTES = 256 * MIB
READY_LIMIT = 64
FOREGROUND_BURST = 3
ADMISSION_FOREGROUND_BURST = 1


@dataclass(frozen=True)
class AggregateJob:
    aggregate_id: UUID
    kind: str
    queued_at: float


class WorkerAdmission:
    """Reserve the full per-operation budget before submitting to an executor.

    The unchanged 768 MiB worker retains 256 MiB for Python/catalog/transport.
    Foreground and maintenance share the remaining 512 MiB; maintenance has
    at most one active turn and cannot overlap foreground for the same target.
    Only identities enter this queue, never events, vectors or snapshots.
    """

    def __init__(self, operation_memory_mb=256):
        if type(operation_memory_mb) is not int:
            raise ValueError("aggregate worker operation budget must be a positive integer MiB")
        self.reservation_bytes = max(MIN_RESERVATION_BYTES, operation_memory_mb * MIB)
        if operation_memory_mb <= 0 or self.reservation_bytes > TASK_MEMORY_BYTES:
            raise ValueError("aggregate worker operation budget exceeds the shared 512 MiB admission budget")
        self.capacity = min(4, TASK_MEMORY_BYTES // self.reservation_bytes)
        self.ready = deque()
        self.active = {}
        self.foreground_streak = 0
        self.high_water = 0
        self.deferred = 0

    @property
    def available(self):
        return READY_LIMIT - len(self.ready)

    def excluded(self, kind):
        return {job.aggregate_id for job in self.ready if job.kind == kind} | set(self.active)

    def offer(self, kind, identities, now):
        if kind not in {"foreground", "maintenance"}:
            raise ValueError("unknown aggregate job kind")
        known = self.excluded(kind)
        for identity in identities:
            if identity in known:
                continue
            if not self.available:
                self.deferred += 1
                break
            if kind == "maintenance" and any(job.kind == kind for job in self.ready):
                break
            self.ready.append(AggregateJob(identity, kind, now))
            known.add(identity)
        self.high_water = max(self.high_water, len(self.ready))

    def acquire(self):
        if len(self.active) >= self.capacity:
            return None
        maintenance_active = any(job.kind == "maintenance" for job in self.active.values())
        choices = {}
        for index, job in enumerate(self.ready):
            if job.aggregate_id in self.active or job.kind == "maintenance" and maintenance_active:
                continue
            choices.setdefault(job.kind, (index, job))
        if not choices:
            return None
        kind = "foreground"
        if kind not in choices or (
            "maintenance" in choices and self.foreground_streak >= ADMISSION_FOREGROUND_BURST
        ):
            kind = "maintenance"
        index, job = choices[kind]
        del self.ready[index]
        self.active[job.aggregate_id] = job
        if kind == "foreground" and any(waiting.kind == "maintenance" for waiting in self.ready):
            self.foreground_streak += 1
        else:
            self.foreground_streak = 0
        return job

    def release(self, aggregate_id):
        return self.active.pop(aggregate_id)

    def summary(self):
        return {
            "queue_limit": READY_LIMIT,
            "queued": len(self.ready),
            "active": len(self.active),
            "maintenance_active": sum(job.kind == "maintenance" for job in self.active.values()),
            "capacity": self.capacity,
            "reserved_bytes": len(self.active) * self.reservation_bytes,
            "task_budget_bytes": TASK_MEMORY_BYTES,
            "reservation_bytes": self.reservation_bytes,
            "queue_high_water": self.high_water,
            "deferred_discovery": self.deferred,
            "reason": "admission_capacity" if len(self.active) >= self.capacity else None,
        }
