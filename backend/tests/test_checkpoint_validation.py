"""A damaged saved model never authorizes deletion of its original history."""

import hashlib
import json
import os
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest
from test_aggregate_retention_policy import checkpoint, detector, plan, prepared, schedule

from app.detection.aws_rcf import AwsModelFactory
from app.detection.checkpoint import matches_ml_carrier, validate_checkpoint
from app.detection.checkpoint_codec import floats
from app.detection.contracts import (
    DetectionError,
    canonical,
    canonical_size,
    canonical_size_and_digest,
    digest,
)
from app.detection.model_process import CandidateModel
from app.detection.protection import checkpoint_boundary
from app.detection.runtime_identity import expected_runtime


def saved_model():
    return checkpoint(detector(config={"dimensions": 1, "trees": 2,
        "sample_size": 8, "output_after": 8, "seed": 42}))


def rehash(model):
    model["config_hash"] = digest(model["config"])
    model["checkpoint"]["sha256"] = digest(model["checkpoint"]["body"])


def test_pinned_neutral_identity_matches_authoritative_runtime_lock():
    classpath = os.environ.get("RCF_CLASSPATH")
    manifest_dir = Path(classpath) if classpath is not None else Path(__file__).parents[1] / "ml"
    manifest = json.loads((manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    assert expected_runtime() == {"manifest": digest(manifest), "jre": "17.0.17+10",
                                  "jpype": "1.6.0", "aws": "4.4.0", "format": 1}


def test_current_structure_uses_same_validator_without_files_or_jvm(monkeypatch):
    model = saved_model()
    rehash(model)
    monkeypatch.setenv("RCF_CLASSPATH", "/unavailable-runtime")
    monkeypatch.setattr(Path, "read_text", lambda *_a, **_k: pytest.fail("read runtime file"))
    body, _ = validate_checkpoint(model["checkpoint"], model["config"], ["n"], model["rule_hash"])
    assert body is model["checkpoint"]["body"]
    assert checkpoint_boundary(model, lambda: None) == 41
    candidate = CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])
    assert candidate.request["checkpoint"] is model["checkpoint"]


@pytest.mark.parametrize("field,value", [("manifest", "0" * 64), ("jre", "17.0.18+1"),
    ("jpype", "1.7.0"), ("aws", "4.5.0"), ("format", 2), ("format", True),
    ("manifest", None), ("unknown", "future")])
def test_unknown_runtime_never_releases_history_or_reaches_java(monkeypatch, field, value):
    model = saved_model()
    model["checkpoint"]["body"]["runtime"][field] = value
    rehash(model)
    monkeypatch.setattr("app.detection.aws_rcf.runtime", lambda: pytest.fail("started JVM"))
    with pytest.raises(DetectionError, match="checkpoint_runtime_mismatch"):
        plan([model])
    with pytest.raises(DetectionError, match="checkpoint_runtime_mismatch"):
        AwsModelFactory().restore(model["checkpoint"], model["config"], ["n"], model["rule_hash"])


@pytest.mark.parametrize("path", [
    ("forestState", "pointStoreState"), ("forestState", "compactSamplerStates"),
    ("forestState", "compactRandomCutTreeStates"), ("forestState", "executionContext"),
    ("preprocessorStates",), ("predictorCorrectorState",), ("lastDescriptorState",)])
def test_missing_mapper_dependency_never_releases_history(path):
    model = saved_model()
    target = model["checkpoint"]["body"]["mapper"]
    for part in path[:-1]:
        target = target[part]
    del target[path[-1]]
    rehash(model)
    with pytest.raises(DetectionError):
        plan([model])


@pytest.mark.parametrize("path", [(), ("forestState",), ("forestState", "pointStoreState"),
    ("forestState", "compactSamplerStates", 0), ("forestState", "compactRandomCutTreeStates", 0),
    ("forestState", "compactRandomCutTreeStates", 0, "nodeStoreState"),
    ("preprocessorStates", 0), ("predictorCorrectorState",)])
def test_future_mapper_versions_fail_closed(path):
    model = saved_model()
    target = model["checkpoint"]["body"]["mapper"]
    for part in path:
        target = target[part]
    target["version"] = "future"
    rehash(model)
    with pytest.raises(DetectionError):
        plan([model])


@pytest.mark.parametrize("damage", ["updates", "trees", "sampler", "buffer", "node", "predictor",
    "deviation", "bool", "affine_bool", "history_bool", "missing_runtime", "header_only",
    "config_bool", "settings_bool", "store_bool", "predictor_bool"])
def test_damaged_current_mapper_and_affine_are_not_self_contained(damage):
    model = saved_model()
    body = model["checkpoint"]["body"]
    mapper, forest = body["mapper"], body["mapper"]["forestState"]
    if damage == "updates":
        forest["totalUpdates"] += 1
    elif damage == "trees":
        forest["compactRandomCutTreeStates"].pop()
    elif damage == "sampler":
        forest["compactSamplerStates"][0]["weight"] = []
    elif damage == "buffer":
        forest["pointStoreState"]["pointData"] = "invalid bytes!"
    elif damage == "node":
        forest["compactRandomCutTreeStates"][0]["nodeStoreState"] = {}
    elif damage == "predictor":
        mapper["predictorCorrectorState"]["thresholderStates"] = []
    elif damage == "deviation":
        mapper["preprocessorStates"][0]["deviationStates"][0]["sum"] = "broken"
    elif damage == "bool":
        forest["totalUpdates"] = True
    elif damage == "affine_bool":
        body["affine"]["scale"] = [True]
    elif damage == "history_bool":
        body["history"][0]["ml"] = [False]
    elif damage == "missing_runtime":
        body["runtime"] = {"format": 1}
    elif damage == "config_bool":
        body["config"] = {**body["config"], "dimensions": True}
    elif damage == "settings_bool":
        body["settings"]["version"] = True
    elif damage == "store_bool":
        forest["pointStoreState"]["dimensions"] = True
    elif damage == "predictor_bool":
        mapper["predictorCorrectorState"]["baseDimension"] = True
    else:
        body["mapper"] = {"forestState": {k: forest[k] for k in
            ("dimensions", "numberOfTrees", "sampleSize", "outputAfter", "shingleSize")},
            "forestMode": "STANDARD", "transformMethod": "NONE"}
    rehash(model)
    with pytest.raises(DetectionError):
        plan([model])


def test_structural_and_large_buffer_validation_obey_shared_deadline():
    model = saved_model()
    rehash(model)
    calls = [0]
    def exhausted():
        calls[0] += 1
        if calls[0] >= 100:
            raise TimeoutError("retention deadline")
    with pytest.raises(TimeoutError, match="retention deadline"):
        checkpoint_boundary(model, exhausted)
    assert calls[0] == 100


def test_mapper_checks_deadline_inside_large_saved_buffer():
    calls = [0]
    def interrupted():
        calls[0] += 1
        if calls[0] == 20:
            raise TimeoutError("mapper deadline")
    with pytest.raises(TimeoutError, match="mapper deadline"):
        floats("A" * (2 * 1024**2), (2 * 1024**2) * 3 // 16, interrupted)
    assert calls[0] == 20


@pytest.mark.parametrize("value", [None, True, {"f": [1., -0., 1e100, 1e-100, 1.5, 2**60 + 1]},
    {"z": "кириллица\n\t\\\"", "a": ([0, False], {"b": 1, "a": 2})},
    {"rows": [{"x": i, "nested": [i * .25, "a" * 100]} for i in range(1000)]}])
def test_streaming_digest_and_size_preserve_exact_existing_canonical_bytes(value):
    expected = canonical(value).encode()
    assert digest(value, check_budget=lambda: None) == hashlib.sha256(expected).hexdigest()
    assert canonical_size(value, check_budget=lambda: None) == len(expected)
    assert canonical_size_and_digest(value, check_budget=lambda: None) == (
        len(expected), hashlib.sha256(expected).hexdigest())


@pytest.mark.parametrize("encode", [digest, canonical_size_and_digest])
def test_streaming_digest_checks_encoding_chunks_after_normalization(encode):
    # The nested traversal has only two nodes; interruption must happen during
    # encoding/hashing of the large scalar, not only before json.dumps.
    calls = [0]
    def interrupted():
        calls[0] += 1
        if calls[0] == 8:
            raise TimeoutError("encoding deadline")
    with pytest.raises(TimeoutError, match="encoding deadline"):
        encode({"buffer": "a" * (2 * 1024**2)}, check_budget=interrupted)
    assert calls[0] == 8


@pytest.mark.parametrize("extra_fields", [False, True])
def test_budgeted_checkpoint_measures_exact_canonical_envelope(monkeypatch, extra_fields):
    import app.detection.checkpoint as parser

    model = saved_model()
    checkpoint = model["checkpoint"]
    if extra_fields:
        checkpoint["legacy"] = "кириллица"
    size = len(canonical(checkpoint).encode())
    monkeypatch.setattr(parser, "MAX_BYTES", size)
    validate_checkpoint(checkpoint, model["config"], ["n"], model["rule_hash"], check_budget=lambda: None)
    checkpoint["sha256"] = "0" * 64
    with pytest.raises(DetectionError, match="^checkpoint_checksum$"):
        validate_checkpoint(checkpoint, model["config"], ["n"], model["rule_hash"], check_budget=lambda: None)
    monkeypatch.setattr(parser, "MAX_BYTES", size - 1)
    with pytest.raises(DetectionError, match="^resource_budget:checkpoint_bytes$"):
        validate_checkpoint(checkpoint, model["config"], ["n"], model["rule_hash"], check_budget=lambda: None)


def test_failed_validation_does_not_mutate_committed_checkpoint():
    model = saved_model()
    rehash(model)
    model["checkpoint"]["body"]["mapper"]["version"] = "unknown"
    before = deepcopy(model)
    with pytest.raises(DetectionError):
        plan([model])
    assert model == before


@pytest.mark.parametrize("phase", ["canonical", "mapper", "affine"])
def test_valueerror_derived_deadline_identity_survives_all_parser_phases(monkeypatch, phase):
    import app.detection.checkpoint as parser

    class Deadline(ValueError):
        pass

    model, current = saved_model(), ["canonical"]
    rehash(model)
    for attribute, name in (("validate_mapper", "mapper"), ("affine_transform", "affine")):
        original = getattr(parser, attribute)
        def marked(*args, original=original, name=name, **kwargs):
            current[0] = name
            return original(*args, **kwargs)
        monkeypatch.setattr(parser, attribute, marked)
    sentinel, visits = Deadline("original deadline"), [0]
    def check():
        if current[0] == phase:
            visits[0] += 1
            if visits[0] == (10 if phase == "canonical" else 2):
                raise sentinel
    with pytest.raises(Deadline) as caught:
        parser.validate_checkpoint(model["checkpoint"], model["config"], ["n"], model["rule_hash"], check_budget=check)
    assert caught.value is sentinel


@pytest.mark.parametrize("damage", ["checksum", "config", "keys", "rule", "settings", "runtime"])
def test_candidate_preserves_envelope_diagnostic_without_jvm(damage):
    model = saved_model()
    body = model["checkpoint"]["body"]
    if damage == "config":
        body["config"] = {**body["config"], "seed": 99}
    elif damage == "keys":
        body["keys"] = ["other"]
    elif damage == "rule":
        body["rule_hash"] = "other"
    elif damage == "settings":
        body["settings"]["version"] = 2
    elif damage == "runtime":
        body["runtime"]["manifest"] = "0" * 64
    rehash(model)
    if damage == "checksum":
        model["checkpoint"]["sha256"] = "0" * 64
    with pytest.raises(DetectionError, match="^checkpoint_envelope_mismatch$"):
        CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])


def test_candidate_keeps_deep_history_error_precise():
    model = saved_model()
    model["checkpoint"]["body"]["history"][0]["ml"] = [99.]
    rehash(model)
    with pytest.raises(DetectionError, match="^checkpoint_affine_history$"):
        CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])


def jsonb_large_integral_history():
    model = saved_model()
    body = model["checkpoint"]["body"]
    body["affine"] = {"algorithm": "min_range_decimal_float32_v1", "center": ["1"], "scale": ["2"]}
    body["history"] = [{"exact": [str(2**60)], "ml": [float(2**59)]}]
    rehash(model)
    def parse_numeric(raw):
        value = Decimal(raw)
        return int(value) if value == value.to_integral_value() else float(raw)
    # json.dumps emits the float's decimal spelling; JSONB returns an integral
    # numeric as int even though that integer differs from the binary float.
    model["checkpoint"]["body"] = json.loads(json.dumps(body), parse_float=parse_numeric)
    return model


def test_jsonb_integral_ml_carrier_preserves_checkpoint_hash_and_exact_history():
    model = jsonb_large_integral_history()
    body = model["checkpoint"]["body"]
    assert body["history"] == [{"exact": ["1152921504606846976"], "ml": [576460752303423500]}]
    assert body["history"][0]["ml"][0] != float(2**59)
    assert model["checkpoint"]["sha256"] == digest(body)
    assert checkpoint_boundary(model, lambda: None) == 41
    assert CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])


def test_rehashed_one_unit_change_in_large_ml_carrier_is_rejected():
    model = jsonb_large_integral_history()
    model["checkpoint"]["body"]["history"][0]["ml"][0] += 1
    rehash(model)
    with pytest.raises(DetectionError, match="checkpoint_affine_history"):
        checkpoint_boundary(model, lambda: None)


def test_history_collision_uses_validated_float32_keys_for_mixed_json_carriers():
    model = jsonb_large_integral_history()
    model["checkpoint"]["body"]["history"].append({"exact": [str(2**60 + 1)], "ml": [float(2**59)]})
    rehash(model)
    with pytest.raises(DetectionError, match="checkpoint_affine_history"):
        checkpoint_boundary(model, lambda: None)


def test_carrier_comparison_preserves_valueerror_derived_budget_identity():
    class Deadline(ValueError):
        pass
    sentinel, visits = Deadline("carrier deadline"), [0]
    def check():
        visits[0] += 1
        if visits[0] == 4:
            raise sentinel
    with pytest.raises(Deadline) as caught:
        matches_ml_carrier((0.,), [0], check_budget=check)
    assert visits[0] == 4 and caught.value is sentinel


def test_prepared_carrier_comparison_preserves_raw_budget_identity(monkeypatch):
    from app.detection import protection

    class Deadline(ValueError):
        pass
    model = detector()
    row = prepared(model)
    sentinel, active, visits = Deadline("prepared carrier deadline"), [False], [0]
    original = protection.matches_ml_carrier
    def marked(*args, **kwargs):
        active[0] = True
        return original(*args, **kwargs)
    monkeypatch.setattr(protection, "matches_ml_carrier", marked)
    def check():
        if active[0]:
            visits[0] += 1
            if visits[0] == 4:
                raise sentinel
    with pytest.raises(Deadline) as caught:
        protection.prepared_boundary(model, schedule(), row, check)
    assert visits[0] == 4 and caught.value is sentinel


def test_execution_uses_common_checks_but_gc_still_requires_codec_proof(monkeypatch):
    import app.detection.checkpoint as parser

    model = saved_model()
    rehash(model)
    proof_calls, streamed_sizes = [], []
    original_proof, original_size = parser.validate_mapper, parser.canonical_size
    def prove(*args, **kwargs):
        proof_calls.append(True)
        return original_proof(*args, **kwargs)
    def size(*args, **kwargs):
        streamed_sizes.append(True)
        return original_size(*args, **kwargs)
    monkeypatch.setattr(parser, "validate_mapper", prove)
    monkeypatch.setattr(parser, "canonical_size", size)
    CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])
    assert proof_calls == streamed_sizes == []
    checkpoint_boundary(model, lambda: None)
    assert proof_calls == streamed_sizes == [True]


def test_execution_reaches_required_native_restore_while_gc_rejects_bad_buffer(monkeypatch):
    model = saved_model()
    model["checkpoint"]["body"]["mapper"]["forestState"]["pointStoreState"]["pointData"] = "AAAA"
    rehash(model)
    native_calls = []
    def require_native():
        native_calls.append(True)
        raise DetectionError("native_restore_required")
    monkeypatch.setattr("app.detection.aws_rcf.runtime", require_native)
    candidate = CandidateModel(model["checkpoint"], model["config"], ["n"], model["rule_hash"])
    assert candidate.request["checkpoint"] is model["checkpoint"]
    with pytest.raises(DetectionError, match="^native_restore_required$"):
        AwsModelFactory().restore(model["checkpoint"], model["config"], ["n"], model["rule_hash"])
    assert native_calls == [True]
    with pytest.raises(DetectionError):
        checkpoint_boundary(model, lambda: None)
