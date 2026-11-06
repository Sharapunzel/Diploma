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


def canonical(value):
    # PostgreSQL JSONB expands exponents and may decode integral numerals as int.
    # Hash the JSON numeric value, independent of that incidental Python type.
    def normalized(item):
        if isinstance(item, float) and item.is_integer():
            return int(Decimal(str(item)))
        if isinstance(item, dict):
            return {key: normalized(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalized(child) for child in item]
        return item
    return json.dumps(normalized(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


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
