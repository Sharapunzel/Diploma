"""Remove only a fixture-owned aggregate graph, retaining other tests' data."""

from sqlalchemy import delete, select, update

from app.models import (
    Aggregate,
    AggregateCleanupOperation,
    AggregateClosure,
    AggregateCommand,
    AggregateHistorySpan,
    AggregateLateEvent,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSchedule,
    AggregateSnapshot,
    AggregateTimeExclusion,
    Detector,
    DetectorCommand,
    DetectorOutcome,
    DetectorRecovery,
    DetectorWarmup,
    Finding,
    HistoricalAggregatePoint,
    WorkingWindowPoint,
)


def delete_aggregate(session, identity):
    aggregate = session.get(Aggregate, identity)
    if aggregate is None:
        return
    detector_ids = list(session.scalars(select(Detector.id).where(Detector.aggregate_id == identity)))
    if detector_ids:
        for model in (DetectorCommand, DetectorOutcome, DetectorRecovery, Finding):
            session.execute(delete(model).where(model.detector_id.in_(detector_ids)))
        session.execute(update(Detector).where(Detector.id.in_(detector_ids)).values(
            active_attempt_id=None, revision=Detector.revision + 1))
        session.execute(delete(DetectorWarmup).where(DetectorWarmup.detector_id.in_(detector_ids)))
        session.execute(delete(Detector).where(Detector.id.in_(detector_ids)))
    closure_ids = select(AggregateClosure.origin_point_id).where(AggregateClosure.aggregate_id == identity)
    session.execute(delete(AggregateRetentionState).where(AggregateRetentionState.aggregate_id == identity))
    for model in (AggregateMembership, AggregateSnapshot, AggregateLateEvent):
        session.execute(delete(model).where(model.closure_id.in_(closure_ids)))
    session.execute(delete(AggregateClosure).where(AggregateClosure.aggregate_id == identity))
    session.execute(delete(WorkingWindowPoint).where(WorkingWindowPoint.schedule_id == aggregate.schedule_id))
    session.execute(delete(HistoricalAggregatePoint).where(HistoricalAggregatePoint.aggregate_id == identity))
    for model in (AggregateCommand, AggregateCleanupOperation, AggregateHistorySpan, AggregateTimeExclusion):
        session.execute(delete(model).where(model.aggregate_id == identity))
    schedule_id = aggregate.schedule_id
    session.delete(aggregate)
    session.flush()
    session.execute(delete(AggregateSchedule).where(AggregateSchedule.id == schedule_id))
