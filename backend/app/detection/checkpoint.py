"""Shared JVM-free checkpoint validation before restore or history protection."""

from decimal import Decimal

from .checkpoint_mapper import validate_mapper
from .contracts import (
    DetectionError,
    canonical,
    canonical_size,
    canonical_size_and_digest,
    digest,
    validate_persisted_config,
)
from .numeric import Affine
from .runtime_identity import validate_runtime

MAX_BYTES = 64 * 1024**2


def _unlimited():
    pass


class _BudgetFailure(Exception):
    def __init__(self, original):
        self.original = original


def matches_ml_carrier(converted, saved, *, check_budget=None):
    """Compare the existing JSON numeric carrier, without float-cast tolerance.

    JSONB can expand an integral float's decimal spelling into an integer which
    is not its exact binary value. The established canonical representation is
    shared with checkpoint hashing; a changed decimal carrier still fails.
    """
    check = check_budget or _unlimited
    check()
    if not isinstance(saved, list) or len(saved) != len(converted):
        return False
    for value in saved:
        check()
        if type(value) not in (int, float):
            return False
    return canonical(converted, check_budget=check) == canonical(saved, check_budget=check)


def affine_transform(saved, dimensions, check_budget=None):
    check = check_budget or _unlimited
    check()
    if (not isinstance(saved, dict) or saved["algorithm"] != "min_range_decimal_float32_v1"
            or not isinstance(saved["center"], list) or not isinstance(saved["scale"], list)
            or len(saved["center"]) != dimensions or len(saved["scale"]) != dimensions):
        raise DetectionError("checkpoint_affine")
    center, scale = [], []
    for raw_center, raw_scale in zip(saved["center"], saved["scale"], strict=True):
        check()
        if not isinstance(raw_center, str) or not isinstance(raw_scale, str):
            raise DetectionError("checkpoint_affine")
        value, unit = Decimal(raw_center), Decimal(raw_scale)
        if not value.is_finite() or not unit.is_finite() or unit <= 0:
            raise DetectionError("checkpoint_affine_history")
        center.append(value)
        scale.append(unit)
    return Affine(tuple(center), tuple(scale))


def _envelope(checkpoint, config, keys, rule_hash, check, hash_budget):
    model_config = validate_persisted_config(config)
    if (not isinstance(keys, list) or len(keys) != model_config.dimensions
            or any(not isinstance(key, str) or not key for key in keys)
            or len(set(keys)) != len(keys)):
        raise DetectionError("checkpoint_config_mismatch")
    checksum = None
    if hash_budget is not None and isinstance(checkpoint, dict) and checkpoint.keys() == {"body", "sha256"}:
        # The canonical envelope adds only the keys, punctuation and encoded
        # checksum. Count and hash the large body in the same traversal.
        body_size, checksum = canonical_size_and_digest(checkpoint["body"], check_budget=hash_budget)
        size = len(b'{"body":,"sha256":}') + body_size + canonical_size(checkpoint["sha256"], check_budget=hash_budget)
    elif hash_budget is not None:
        size = canonical_size(checkpoint, check_budget=hash_budget)
    else:
        size = len(canonical(checkpoint).encode())
    if size > MAX_BYTES:
        raise DetectionError("resource_budget:checkpoint_bytes")
    body = checkpoint["body"]
    if checksum is None:
        checksum = digest(body, check_budget=hash_budget)
    if checkpoint["sha256"] != checksum:
        raise DetectionError("checkpoint_checksum")
    if (body["config"] != config or validate_persisted_config(body["config"]) != model_config
            or body["keys"] != keys or body["rule_hash"] != rule_hash):
        raise DetectionError("checkpoint_config_mismatch")
    if digest(body["settings"], check_budget=hash_budget) != digest(model_config.settings, check_budget=hash_budget):
        raise DetectionError("checkpoint_settings_mismatch")
    validate_runtime(body["runtime"])
    if (type(body["updates"]) is not int or body["updates"] < model_config.minimum_inputs
            or not isinstance(body["boundary"], str) or not body["boundary"]):
        raise DetectionError("checkpoint_updates")
    forest = body["mapper"]["forestState"]
    for name, expected in (("dimensions", model_config.dimensions), ("numberOfTrees", model_config.trees),
            ("sampleSize", model_config.sample_size), ("outputAfter", model_config.output_after), ("shingleSize", 1)):
        check()
        if type(forest[name]) is not int or forest[name] != expected:
            raise DetectionError("checkpoint_mapper_config")
    if (body["mapper"]["forestMode"] != "STANDARD"
            or body["mapper"]["transformMethod"] != "NONE"):
        raise DetectionError("checkpoint_mapper_config")
    if type(forest["totalUpdates"]) is not int or forest["totalUpdates"] != body["updates"]:
        raise DetectionError("checkpoint_updates")
    return body, model_config


def _history(body, model_config, keys, check):
    transform = affine_transform(body["affine"], model_config.dimensions, check)
    if (not isinstance(body["history"], list)
            or not 0 < len(body["history"]) <= model_config.sample_size):
        raise DetectionError("checkpoint_affine_history")
    seen = [{} for _ in keys]
    for row in body["history"]:
        check()
        if (not isinstance(row["exact"], list) or len(row["exact"]) != len(keys)
                or any(not isinstance(x, str) for x in row["exact"])):
            raise DetectionError("checkpoint_affine_history")
        exact = []
        for value in row["exact"]:
            check()
            exact.append(Decimal(value))
        converted = transform.convert(exact, check_budget=check)
        if not matches_ml_carrier(converted, row["ml"], check_budget=check):
            raise DetectionError("checkpoint_affine_history")
        for i, value in enumerate(converted):
            check()
            if value in seen[i] and seen[i][value] != exact[i]:
                raise DetectionError("checkpoint_affine_history")
            seen[i][value] = exact[i]
    return transform


def _validate(checkpoint, config, keys, rule_hash, check_budget, *, prove_codec):
    budget = check_budget or _unlimited
    def check():
        try:
            budget()
        except Exception as error:
            # A caller may use ValueError/ArithmeticError-derived deadlines.
            # Parsing errors below must not change its exception identity/code.
            raise _BudgetFailure(error) from error
    try:
        check()
        # Keep the pre-existing fast canonical/digest path for execution without
        # a supplied deadline. GC uses interruptible streaming under its budget.
        hash_budget = check if check_budget is not None else None
        body, model_config = _envelope(checkpoint, config, keys, rule_hash, check, hash_budget)
        if prove_codec:
            validate_mapper(body["mapper"], model_config, body["updates"], check)
        transform = _history(body, model_config, keys, check)
        check()
        return body, transform
    except _BudgetFailure as error:
        raise error.original from None
    except DetectionError:
        raise
    except (KeyError, TypeError, ValueError, ArithmeticError, OverflowError) as error:
        raise DetectionError("checkpoint_corrupt") from error


def validate_checkpoint(checkpoint, config, keys, rule_hash, *, check_budget=None):
    """Full JVM-free dependency proof before releasing history; fail closed."""
    return _validate(checkpoint, config, keys, rule_hash, check_budget, prove_codec=True)


def validate_execution_checkpoint(checkpoint, config, keys, rule_hash):
    """Shared envelope/header/affine/history checks before native restoration.

    Codec buffers are restored and verified by the pinned JVM mapper. This
    entrypoint alone never proves that source history can be collected.
    """
    return _validate(checkpoint, config, keys, rule_hash, None, prove_codec=False)
