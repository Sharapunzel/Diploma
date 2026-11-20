"""Bounded retention persistence and narrow mutation primitives; no commits."""

from datetime import timedelta

from sqlalchemy import JSON, Text, case, delete, func, literal_column, select, update

from ...aggregation.event_time import MAX_SNAPSHOT_BYTES, EventTimeBudgetError
from ...aggregation.retention import POLICY_VERSION, REASONS, STATUSES, MembershipCompaction
from ...models import (
    AggregateClosure,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSnapshot,
    Detector,
    DetectorOutcome,
    DetectorWarmup,
    HistoricalAggregatePoint,
    WorkingWindowPoint,
)
from .aggregate_cleanup import document

DEPENDENCY_KEYS = ("id", "revision", "config_hash", "execution_state", "execution_generation",
    "start_request_id", "active_attempt_id", "consumed_index", "evaluation_start_index", "state", "failure_code")


class SqlAlchemyAggregateRetentionQueries:
    def maintenance_budget(self, budget):
        remaining = budget.milliseconds()
        with self.session.no_autoflush:
            self.session.execute(select(
                func.set_config("statement_timeout", str(remaining), True),
                func.set_config("lock_timeout", str(min(50, remaining)), True)))

    def add_retention_state(self, aggregate, now):
        self.session.add(AggregateRetentionState(aggregate_id=aggregate.id, policy_version=POLICY_VERSION,
            next_check_at=now + timedelta(seconds=(aggregate.id.int >> 96) % 60),
            status="scheduled", retry_count=0, protected_by=[], auto_cleared_total=0, history_revision=0))
        self.session.flush()

    def maintenance_due(self, limit=100, exclude_ids=()):
        query = select(AggregateRetentionState.aggregate_id).where(
            AggregateRetentionState.next_check_at <= func.clock_timestamp())
        if exclude_ids:
            query = query.where(AggregateRetentionState.aggregate_id.not_in(exclude_ids))
        return list(self.session.scalars(query.order_by(AggregateRetentionState.next_check_at,
            AggregateRetentionState.aggregate_id).limit(limit)))

    def retention_state(self, aggregate_id, budget=None, lock=False):
        if budget:
            self.maintenance_budget(budget)
        query = select(AggregateRetentionState).where(AggregateRetentionState.aggregate_id == aggregate_id)
        if lock:
            query = query.with_for_update().execution_options(populate_existing=True)
        return self.session.scalar(query)

    def last_closed_index(self, schedule_id, budget):
        self.maintenance_budget(budget)
        return self.session.scalar(select(func.max(AggregateClosure.window_index)).where(
            AggregateClosure.schedule_id == schedule_id, AggregateClosure.time_basis == "event_time"))

    def retention_headers(self, aggregate_id, budget, lock=False):
        """All members and dependency revisions, with deterministic row locks.

        Saved input/checkpoint/plan bytes are never decoded or hashed under the
        schedule mutex. The DB guard makes attempt id/owner/cutoff/plan immutable
        and prevents reopening prepared inputs. Therefore the same attempt id
        proves the full plan read in RR is still identical. Service writers also
        advance detector revision; status/input hash are compared independently.
        """
        headers, after = [], None
        while True:
            self.maintenance_budget(budget)
            query = select(*(getattr(Detector, key) for key in DEPENDENCY_KEYS)).where(
                Detector.aggregate_id == aggregate_id).order_by(Detector.id).limit(100)
            if after is not None:
                query = query.where(Detector.id > after)
            if lock:
                query = query.with_for_update()
            page = [dict(row) for row in self.session.execute(query).mappings()]
            if not page:
                break
            headers.extend(page)
            after = page[-1]["id"]
            if len(page) < 100:
                break
        ids = [row["active_attempt_id"] for row in headers if row["active_attempt_id"] is not None]
        attempts = []
        for offset in range(0, len(ids), 100):
            self.maintenance_budget(budget)
            attempts.extend(dict(row) for row in self.session.execute(select(
                DetectorWarmup.id, DetectorWarmup.detector_id, DetectorWarmup.status,
                DetectorWarmup.inputs_hash, DetectorWarmup.cutoff)
                .where(DetectorWarmup.id.in_(ids[offset:offset + 100])).order_by(DetectorWarmup.id)).mappings())
        return headers, sorted(attempts, key=lambda row: row["id"])

    def retention_dependencies(self, aggregate_id, budget):
        # Count the entire JSON transport before loading any of it. A checkpoint
        # makes the old warmup payload irrelevant, for both SQL and Python.
        self.maintenance_budget(budget)
        detector_size = sum(func.coalesce(func.octet_length(getattr(Detector, key).cast(Text)), 0)
            for key in ("checkpoint", "config", "feature_keys"))
        no_checkpoint = Detector.checkpoint.is_(None) | (Detector.checkpoint == JSON.NULL)
        attempt_size = case((no_checkpoint, func.coalesce(func.octet_length(func.to_jsonb(
            DetectorWarmup.__table__.table_valued()).cast(Text)), 0)), else_=0)
        total = self.session.scalar(select(func.coalesce(func.sum(detector_size + attempt_size + 8192), 0))
            .select_from(Detector).outerjoin(DetectorWarmup, DetectorWarmup.id == Detector.active_attempt_id)
            .where(Detector.aggregate_id == aggregate_id))
        # Include complete required attempts, not just inputs/plan, and the same
        # conservative x8 decoding/copy/validator allowance under both caps.
        if total > MAX_SNAPSHOT_BYTES or total * 8 > 256 * 1024**2:
            raise EventTimeBudgetError("protection_bytes")
        detectors = self.protection_detectors(aggregate_id, budget, maintenance=True)
        ids = [d["active_attempt_id"] for d in detectors
            if d["active_attempt_id"] is not None and d["checkpoint"] is None]
        attempts = {}
        for identity in ids:
            self.maintenance_budget(budget)
            row = self.session.get(DetectorWarmup, identity, populate_existing=True)
            if row is not None:
                attempts[identity] = document(row)
        return detectors, attempts

    def retention_candidate(self, aggregate_id, keep_from, budget, point_id=None, lock=False):
        self.maintenance_budget(budget)
        query = select(AggregateClosure).where(AggregateClosure.aggregate_id == aggregate_id,
            AggregateClosure.time_basis == "event_time", AggregateClosure.payload_state == "present",
            AggregateClosure.calculation_status == "computed",
            AggregateClosure.window_index < keep_from)
        if point_id is not None:
            query = query.where(AggregateClosure.origin_point_id == point_id)
        query = query.order_by(AggregateClosure.window_index, AggregateClosure.origin_point_id).limit(1)
        if lock:
            query = query.with_for_update().execution_options(populate_existing=True)
        row = self.session.scalar(query)
        return row

    def retention_pending_closure(self, aggregate_id, point_id, budget):
        self.maintenance_budget(budget)
        return self.session.scalar(select(AggregateClosure).where(
            AggregateClosure.aggregate_id == aggregate_id, AggregateClosure.origin_point_id == point_id
            ).with_for_update().execution_options(populate_existing=True))

    def retention_authorize(self, state, phase, budget):
        self.maintenance_budget(budget)
        state.pending_phase = phase
        state.authorization_txid = self.session.scalar(select(func.pg_current_xact_id().cast(Text)))
        self.session.flush()

    def clear_payload(self, closure, budget, automatic=False, state=None):
        """Atomic first logical clear; details remain whole until this commits."""
        if closure.calculation_status == "uncomputed":
            return False
        if not automatic and state is None:
            self.read_budget(min(1000, budget.milliseconds()))
            state = self.retention_state(closure.aggregate_id, lock=True)
            if state is None:
                raise ValueError("retention_state_missing")
        self.maintenance_budget(budget) if automatic else self.read_budget(min(1000, budget.milliseconds()))
        self.session.execute(delete(AggregateSnapshot).where(AggregateSnapshot.closure_id == closure.origin_point_id))
        self.maintenance_budget(budget) if automatic else self.read_budget(min(1000, budget.milliseconds()))
        model = WorkingWindowPoint if closure.point_kind == "working" else HistoricalAggregatePoint
        self.session.execute(delete(model).where(model.id == closure.origin_point_id))
        self.maintenance_budget(budget) if automatic else self.read_budget(min(1000, budget.milliseconds()))
        at = self.now()
        values = {"payload_state": "cleared", "cleared_at": at,
            "clear_reason": "automatic" if automatic else "manual",
            "clear_policy_version": POLICY_VERSION if automatic else None}
        changed = self.session.execute(update(AggregateClosure).where(
            AggregateClosure.origin_point_id == closure.origin_point_id,
            AggregateClosure.payload_state == "present").values(**values)).rowcount
        if changed and state is not None:
            state.history_revision += 1
            state.last_cleared_at = at
            if automatic:
                state.auto_cleared_total += 1
        return bool(changed)

    def compact_memberships(self, point_id, budget, automatic=False):
        apply_budget = lambda: self.maintenance_budget(budget) if automatic else self.read_budget(min(1000, budget.milliseconds()))
        apply_budget()
        # Always search the partial remaining-rows index from its beginning:
        # pending_cursor is diagnostic and is not a completeness boundary.
        ids = list(self.session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == point_id,
            AggregateMembership.fingerprint.is_not(None) | AggregateMembership.event_time.is_not(None)
            ).order_by(AggregateMembership.event_id).limit(5000)))
        changed = 0
        if ids:
            apply_budget()
            changed = self.session.execute(update(AggregateMembership).where(AggregateMembership.closure_id == point_id,
                AggregateMembership.event_id.in_(ids)).values(fingerprint=None, event_time=None)).rowcount
        # The manifest is published with its closure in one UoW; subsequent
        # arrivals are recorded in the separate late ledger. A short portion
        # therefore exhausts this closed membership, without an empty probe.
        # Exact multiples still require another search from the partial index.
        # Manual cleanup keeps its existing details-first commit progression.
        cursor = ids[-1] if ids and (not automatic or len(ids) == 5000) else None
        return MembershipCompaction(cursor, changed)

    def compact_outcomes(self, point_id, budget, automatic=False):
        apply_budget = lambda: self.maintenance_budget(budget) if automatic else self.read_budget(min(1000, budget.milliseconds()))
        apply_budget()
        # Each outcome UPDATE also runs per-row execution/ownership guards.
        # Five thousand fit a warm isolated statement but can exhaust the
        # automatic turn's remaining <1s budget in a cold/full workload.
        # Keep the manual portion and its independent 5s turn unchanged.
        limit = 1000 if automatic else 5000
        ids = list(self.session.scalars(select(DetectorOutcome.id).where(
            DetectorOutcome.working_point_id == point_id,
            # The static JSON key lets generic prepared plans prove the partial
            # index predicate; a bound key would lose that implication.
            DetectorOutcome.data[literal_column("'compacted'")].as_boolean().is_distinct_from(True)
            ).order_by(DetectorOutcome.id).limit(limit)))
        if ids:
            apply_budget()
            self.session.execute(update(DetectorOutcome).where(DetectorOutcome.id.in_(ids)).values(
                data=func.jsonb_build_object("compacted", True,
                    "start", DetectorOutcome.data["start"], "end", DetectorOutcome.data["end"],
                    "actual_started_at", DetectorOutcome.data["actual_started_at"],
                    "actual_completed_at", DetectorOutcome.data["actual_completed_at"],
                    "late", DetectorOutcome.data["late"], "recovery", DetectorOutcome.data["recovery"])))
        return ids[-1] if ids else None

    def retention_summary(self, aggregate_id):
        keys = ("policy_version", "status", "reason", "reserve_windows", "keep_from_index",
            "protected_from_index", "protected_total", "auto_cleared_total", "history_revision", "last_cleared_at")
        present = (AggregateClosure.aggregate_id == aggregate_id,
            AggregateClosure.payload_state == "present", AggregateClosure.time_basis == "event_time")
        # The revision and current range come from one statement snapshot. A GC
        # commit between two GET queries must not pair an old revision with the
        # newly shortened range.
        row = self.session.execute(select(*(getattr(AggregateRetentionState, key) for key in keys),
            AggregateRetentionState.last_checked_at, AggregateRetentionState.next_check_at,
            AggregateRetentionState.protected_by,
            select(func.min(AggregateClosure.window_index)).where(*present).scalar_subquery().label("retained_from_index"),
            select(func.max(AggregateClosure.window_index)).where(*present).scalar_subquery().label("retained_to_index"))
            .where(AggregateRetentionState.aggregate_id == aggregate_id)).mappings().first()
        if row is None:
            return {**dict.fromkeys(keys), "status": "error", "reason": "retention_state_missing",
                "checked_at": None, "next_check_at": None, "protected_by": [],
                "retained_from_index": None, "retained_to_index": None}
        return {**{key: row[key] for key in keys},
            "status": row["status"] if row["status"] in STATUSES else "error",
            "reason": row["reason"] if row["reason"] in REASONS else None,
            "checked_at": row["last_checked_at"], "next_check_at": row["next_check_at"],
            "protected_by": row["protected_by"][:10], "retained_from_index": row["retained_from_index"],
            "retained_to_index": row["retained_to_index"]}
