"""SQLAlchemy storage for registered probes and signed input reports."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from ...models import InputAttestation, InputProbe, Source
from ..protocols.evidence import ProbeEvidence


class SqlAlchemyInputAttestationRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def lock_source_probe(self, source_id: UUID, probe_id: UUID) -> tuple[str, str | None, ProbeEvidence] | None:
        source = self.session.scalar(select(Source).where(Source.id == source_id).with_for_update(read=True))
        probe = self.session.get(InputProbe, probe_id)
        if source is None or probe is None:
            return None
        return (source.source_type, source.kafka_topic_identity,
                ProbeEvidence(probe.id, probe.source_identity, probe.input_generation,
                              probe.public_key, probe.is_active))

    def db_now(self) -> datetime:
        return self.session.scalar(select(func.clock_timestamp()))

    def existing(self, attestation_id: UUID) -> tuple[dict, bytes] | None:
        item = self.session.get(InputAttestation, attestation_id)
        return (item.signed_payload, item.signature) if item else None

    def save(self, parsed: tuple, payload: dict, signature: bytes) -> None:
        attestation_id, probe_id, source_id, generation, sequence, start, end, state, topic, first, last = parsed
        self.session.add(InputAttestation(
            id=attestation_id, probe_id=probe_id, source_identity=source_id,
            input_generation=generation, sequence=sequence, start_utc=start, end_utc=end,
            state=state, topic_identity=topic, start_positions=first,
            end_positions=last, signed_payload=payload, signature=signature,
        ))
        self.session.flush()

    def lock_source_for_registration(self, source_id: UUID) -> str | None:
        source = self.session.scalar(select(Source).where(Source.id == source_id).with_for_update())
        return source.source_type if source else None

    def replace_probe(self, probe_id: UUID, source_id: UUID, generation: UUID, key: bytes) -> None:
        self.session.execute(update(InputProbe).where(
            InputProbe.source_identity == source_id,
            InputProbe.is_active.is_(True),
        ).values(is_active=False))
        self.session.add(InputProbe(id=probe_id, source_identity=source_id,
                                    input_generation=generation, public_key=key))
