"""Read durable, independently recorded input and Kafka coverage evidence."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ...models import (
    InputAttestation,
    InputProbe,
    KafkaCheckpoint,
    KafkaOperationalEvent,
    ProcessedKafkaRecord,
    SourceInterruption,
)
from ..protocols.evidence import (
    AttestationEvidence,
    CheckpointEvidence,
    ConsumerHeartbeat,
    ProbeEvidence,
)


class SqlAlchemyCoverageEvidenceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def interruptions(self, source_id: UUID, start: datetime, end: datetime,
                      barrier_at: datetime) -> list[UUID]:
        return list(self.session.scalars(select(SourceInterruption.id).where(
            SourceInterruption.source_identity == source_id,
            SourceInterruption.started_at < end,
            SourceInterruption.started_at < barrier_at,
            or_(SourceInterruption.ended_at.is_(None), SourceInterruption.ended_at > start),
        )))

    def attestations(self, source_id: UUID, start: datetime, end: datetime,
                     barrier_at: datetime) -> list[tuple[AttestationEvidence, ProbeEvidence]]:
        pairs = self.session.execute(select(InputAttestation, InputProbe).join(
            InputProbe, InputProbe.id == InputAttestation.probe_id).where(
            InputAttestation.source_identity == source_id,
            InputAttestation.start_utc < end, InputAttestation.end_utc > start,
            InputAttestation.received_at < barrier_at,
        ).order_by(InputAttestation.start_utc, InputAttestation.sequence)).all()
        return [(AttestationEvidence(item.id, item.probe_id, item.source_identity,
                    item.input_generation, item.sequence, item.start_utc, item.end_utc,
                    item.state, item.topic_identity, item.start_positions, item.end_positions,
                    item.signed_payload, item.signature),
                 ProbeEvidence(probe.id, probe.source_identity, probe.input_generation,
                               probe.public_key, probe.is_active)) for item, probe in pairs]

    def checkpoint(self, source_id: UUID, topic: str | None, end: datetime,
                   barrier_at: datetime, reference_at: datetime | None = None) -> CheckpointEvidence | None:
        # None is used only by readiness; completeness always supplies the
        # signed attestation's exact topic identity in the final snapshot.
        clock = reference_at if reference_at is not None else func.clock_timestamp()
        query = select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source_id,
            KafkaCheckpoint.observed_at >= end,
            KafkaCheckpoint.observed_at >= barrier_at,
            KafkaCheckpoint.observed_at >= clock - timedelta(seconds=10),
            KafkaCheckpoint.observed_at <= clock,
        )
        if topic is not None:
            query = query.where(KafkaCheckpoint.topic_identity == topic)
        item = self.session.scalar(query.order_by(KafkaCheckpoint.observed_at.desc()).limit(1))
        return CheckpointEvidence(item.id, item.partitions) if item else None

    def consumer_heartbeats(self, source_id: UUID, topic: str, start: datetime,
                            end: datetime) -> list[ConsumerHeartbeat]:
        rows = self.session.scalars(select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source_id,
            KafkaCheckpoint.topic_identity == topic,
            KafkaCheckpoint.observed_at >= start - timedelta(seconds=10),
            KafkaCheckpoint.observed_at <= end + timedelta(seconds=10),
        ).order_by(KafkaCheckpoint.observed_at, KafkaCheckpoint.id))
        return [ConsumerHeartbeat(row.id, row.worker_instance, row.observed_at) for row in rows]

    def has_gap(self, source_id: UUID, topic: str, partition: int,
                beginning: int, final: int) -> bool:
        count = self.session.scalar(select(func.count()).select_from(KafkaOperationalEvent).where(
            KafkaOperationalEvent.source_identity == source_id,
            KafkaOperationalEvent.old_topic_identity == topic,
            or_(KafkaOperationalEvent.kind == "topic_recreated", (
                (KafkaOperationalEvent.kind == "retention_gap")
                & (KafkaOperationalEvent.kafka_partition == partition)
                & (KafkaOperationalEvent.offset_start < final)
                & (KafkaOperationalEvent.offset_end > beginning)
            )),
        ))
        return bool(count)

    def processing_counts(self, source_id: UUID, topic: str, partition: int,
                          beginning: int, final: int) -> tuple[int, int]:
        return self.session.execute(select(
            func.count(), func.count().filter(ProcessedKafkaRecord.result_status == "failed")
        ).where(
            ProcessedKafkaRecord.source_id == source_id,
            ProcessedKafkaRecord.kafka_topic_identity == topic,
            ProcessedKafkaRecord.kafka_partition == partition,
            ProcessedKafkaRecord.kafka_offset >= beginning,
            ProcessedKafkaRecord.kafka_offset < final,
        )).one()
