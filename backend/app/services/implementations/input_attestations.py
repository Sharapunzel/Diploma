"""Authenticated input coverage reports from registered, source-specific probes."""

import json
from datetime import UTC, datetime
from uuid import UUID

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.evidence import ProbeEvidence
from ...repositories.protocols.input_attestations import InputAttestationRepository

_FIELDS = {"id", "probe_id", "source_id", "input_generation", "sequence", "start", "end",
           "state", "topic_identity", "start_positions", "end_positions"}


def canonical_payload(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("ascii")


def _positions(value: object) -> dict[str, int]:
    if not isinstance(value, dict) or not value or len(value) > 1024:
        raise ValueError("invalid partition positions")
    result = {}
    for key, offset in value.items():
        if (not isinstance(key, str) or not key.isascii() or not key.isdigit()
                or str(int(key)) != key or int(key) > 2147483647
                or type(offset) is not int or not 0 <= offset <= 9223372036854775807):
            raise ValueError("invalid partition position")
        result[key] = offset
    return result


def parse_attestation(payload: dict) -> tuple:
    if not isinstance(payload, dict) or set(payload) != _FIELDS:
        raise ValueError("invalid attestation fields")
    try:
        ids = tuple(UUID(payload[name]) for name in ("id", "probe_id", "source_id", "input_generation"))
        start, end = (datetime.fromisoformat(payload[name])
                      for name in ("start", "end"))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("invalid attestation identity or interval") from exc
    sequence, state, topic = payload["sequence"], payload["state"], payload["topic_identity"]
    if (type(sequence) is not int or not 0 <= sequence <= 9223372036854775807
            or not isinstance(state, str) or state not in {"covered", "interrupted"} or not isinstance(topic, str)
            or not topic.strip() or len(topic) > 512 or start.utcoffset() is None
            or end.utcoffset() is None or start.astimezone(UTC) >= end.astimezone(UTC)):
        raise ValueError("invalid attestation content")
    first, last = _positions(payload["start_positions"]), _positions(payload["end_positions"])
    if first.keys() != last.keys() or any(first[p] > last[p] for p in first):
        raise ValueError("inconsistent attested positions")
    return (*ids, sequence, start.astimezone(UTC), end.astimezone(UTC), state, topic, first, last)


def verify_attestation(probe: ProbeEvidence, payload: dict, signature: bytes) -> tuple:
    parsed = parse_attestation(payload)
    _, probe_id, source_id, generation, *_ = parsed
    if (not probe.is_active or (probe.id, probe.source_identity, probe.input_generation)
            != (probe_id, source_id, generation)):
        raise ValueError("probe does not authorize this input generation")
    try:
        Ed25519PublicKey.from_public_bytes(probe.public_key).verify(signature, canonical_payload(payload))
    except (InvalidSignature, ValueError, TypeError) as exc:
        raise ValueError("invalid attestation signature") from exc
    return parsed


def ingest_attestation(repository: InputAttestationRepository, payload: dict, signature: bytes) -> UUID:
    """Validate a signed report against a registered probe, then stage it for commit."""
    parsed = parse_attestation(payload)
    attestation_id, probe_id, source_id, _, _, _, end, _, topic, _, _ = parsed
    authority = repository.lock_source_probe(source_id, probe_id)
    if authority is None or authority[0] != "kafka":
        raise ValueError("unknown input or probe")
    if authority[1] != topic:
        raise ValueError("topic generation differs from the source")
    verify_attestation(authority[2], payload, signature)
    if end > repository.db_now():
        raise ValueError("coverage interval cannot end in the future")
    existing = repository.existing(attestation_id)
    if existing is not None:
        if existing != (payload, signature):
            raise ValueError("attestation identity conflict")
        return attestation_id
    repository.save(parsed, payload, signature)
    return attestation_id


class InputAttestationService:
    def __init__(self, repository: InputAttestationRepository, uow: UnitOfWork) -> None:
        self.repository = repository
        self.uow = uow

    def register_probe(self, probe_id: UUID, source_id: UUID, generation: UUID, key: bytes) -> None:
        try:
            if len(key) != 32 or self.repository.lock_source_for_registration(source_id) != "kafka":
                raise ValueError("source or Ed25519 public key is invalid")
            self.repository.replace_probe(probe_id, source_id, generation, key)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise

    def submit(self, payload: dict, signature: bytes) -> UUID:
        try:
            report_id = ingest_attestation(self.repository, payload, signature)
            self.uow.commit()
            return report_id
        except Exception:
            self.uow.rollback()
            raise
