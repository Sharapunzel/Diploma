"""Neutral detector contracts; no storage or HTTP dependencies."""

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Protocol


class DetectionError(ValueError):
    pass


class StaleCandidate(DetectionError):
    pass


def _normalized(item, check_budget):
    # PostgreSQL JSONB expands exponents and may decode integral numerals as int.
    # Hash the JSON numeric value, independent of that incidental Python type.
    if check_budget:
        check_budget()
    if isinstance(item, float) and item.is_integer():
        return int(Decimal(str(item)))
    if isinstance(item, dict):
        return {key: _normalized(child, check_budget) for key, child in item.items()}
    if isinstance(item, (tuple, list)):
        return [_normalized(child, check_budget) for child in item]
    return item


def canonical(value, *, check_budget=None):
    result = json.dumps(_normalized(value, check_budget), sort_keys=True,
                        separators=(",", ":"), allow_nan=False)
    if check_budget:
        check_budget()
    return result


def _canonical_chunks(value, check_budget):
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), allow_nan=False)
    for chunk in encoder.iterencode(_normalized(value, check_budget)):
        # The encoder may yield a whole large string. Do not retain a second
        # full encoded byte copy or do an uninterruptible whole-document join.
        for offset in range(0, len(chunk), 65536):
            if check_budget:
                check_budget()
            yield chunk[offset:offset + 65536].encode()
    if check_budget:
        check_budget()


def canonical_size(value, *, check_budget=None):
    return sum(len(chunk) for chunk in _canonical_chunks(value, check_budget))


def canonical_size_and_digest(value, *, check_budget):
    size = 0
    hashed = hashlib.sha256()
    for chunk in _canonical_chunks(value, check_budget):
        size += len(chunk)
        hashed.update(chunk)
    result = hashed.hexdigest()
    check_budget()
    return size, result


def digest(value, *, check_budget=None):
    if check_budget is None:
        return hashlib.sha256(canonical(value).encode()).hexdigest()
    hashed = hashlib.sha256()
    for chunk in _canonical_chunks(value, check_budget):
        hashed.update(chunk)
    result = hashed.hexdigest()
    if check_budget:
        check_budget()
    return result


@dataclass(frozen=True)
class ModelConfig:
    dimensions: int
    trees: int = 100
    sample_size: int = 256
    output_after: int = 256
    seed: int = 42

    def __post_init__(self):
        if any(type(v) is not int for v in asdict(self).values()):
            raise DetectionError("invalid_model_config")
        if min(self.dimensions, self.trees) <= 0 or self.sample_size < 2:
            raise DetectionError("invalid_model_config")
        if self.output_after < self.sample_size or not -(2**63) <= self.seed < 2**63:
            raise DetectionError("invalid_model_config")
        if self.minimum_inputs > 4096:
            raise DetectionError("resource_budget:warmup_windows")
        # Conservative allowance for trees, Java objects, point store and JSON.
        if self.dimensions * self.trees * self.sample_size * 256 > 256 * 1024**2:
            raise DetectionError("resource_budget:model_memory")

    @property
    def minimum_inputs(self):
        return max(2 * self.sample_size, self.output_after + 1)

    @property
    def settings(self):
        return {"version": 1, "forestMode": "STANDARD", "transformMethod": "NONE",
            "shingleSize": 1, "precision": "FLOAT_32", "parallelExecutionEnabled": False,
            "internalShinglingEnabled": True, "storeSequenceIndexesEnabled": False,
            "centerOfMassEnabled": False, "boundingBoxCacheFraction": 1.0,
            "initialAcceptFraction": 1.0, "anomalyRate": .01, "autoAdjust": False,
            "zFactor": 3.0, "scoreDifferencing": .5, "alertOnce": False,
            "absoluteThreshold": .8, "initialThreshold": 1.5,
            "startNormalization": min(10, max(1, self.output_after - 1)),
            "stopNormalization": 2147483647, "transformDecay": 1 / self.sample_size,
            "timeDecay": 1 / self.sample_size, "scoringStrategy": "EXPECTED_INVERSE_DEPTH",
            "weights": [1.] * self.dimensions, "nearExpectedIgnore": "release_defaults"}


def validate_persisted_config(value):
    """Saved state has every asdict field; create-time defaults remain separate."""
    if not isinstance(value, dict) or set(value) != set(ModelConfig.__dataclass_fields__):
        raise DetectionError("checkpoint_config_mismatch")
    return ModelConfig(**value)


def feature_vector(features, keys, completeness):
    from .numeric import exact

    if completeness != "complete":
        raise DetectionError("coverage:" + completeness)
    if (not isinstance(features, list) or not all(isinstance(f, dict) for f in features)
            or [f.get("key") for f in features] != list(keys)):
        raise DetectionError("feature_order_or_payload")
    if len(set(keys)) != len(keys):
        raise DetectionError("duplicate_feature")
    values = []
    for i, feature in enumerate(features):
        if feature.get("reason") or feature.get("invalid_count", 0):
            raise DetectionError(f"feature_quality:{keys[i]}")
        exact(feature.get("value"), i)
        values.append(feature["value"])
    return values


class DetectionModel(Protocol):
    def process(self, exact_values: list) -> dict: ...
    def checkpoint(self, boundary: str) -> dict: ...


class ModelFactory(Protocol):
    def warmup(self, config: dict, keys: list, rule_hash: str, rows: list) -> dict: ...
    def restore(self, checkpoint: dict, config: dict, keys: list, rule_hash: str) -> DetectionModel: ...
