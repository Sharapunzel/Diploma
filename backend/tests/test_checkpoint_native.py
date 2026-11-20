"""Validate actual pinned AWS/Jackson output on ordinary JVM-free host runs.

The fixture is generated only by ml/rcf_gate.py::test_pinned_checkpoint_golden;
that mandatory native gate also checks deterministic regeneration and actual
restore/continuation. No hand-written forest is used as native parity evidence.
"""

import json
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

import pytest

from app.detection.checkpoint import validate_checkpoint
from app.detection.contracts import DetectionError, ModelConfig, digest
from app.detection.model_process import CandidateModel
from app.detection.protection import checkpoint_boundary


def native_checkpoint():
    return json.loads((Path(__file__).parent / "fixtures" / "aws_rcf_4_4_0_checkpoint.json").read_text(encoding="utf-8"))


def test_actual_native_checkpoint_validates_without_jvm_or_runtime_files(monkeypatch):
    saved = native_checkpoint()
    body = saved["body"]
    assert body["config"] == asdict(ModelConfig(dimensions=2, trees=20, sample_size=8, output_after=8, seed=42))
    assert body["keys"] == ["count", "bytes"]
    assert body["rule_hash"] == digest({"synthetic_native_golden": 1})
    assert body["updates"] == 16 and body["boundary"] == "warmup"
    monkeypatch.setenv("RCF_CLASSPATH", "/unavailable-runtime")
    monkeypatch.setattr(Path, "read_text", lambda *_a, **_k: pytest.fail("unexpected file read"))
    assert validate_checkpoint(saved, body["config"], body["keys"], body["rule_hash"])[0] is body
    assert CandidateModel(saved, body["config"], body["keys"], body["rule_hash"]).request["checkpoint"] is saved
    detector = {"checkpoint": saved, "config": body["config"], "config_hash": digest(body["config"]),
        "feature_keys": body["keys"], "rule_hash": body["rule_hash"], "consumed_index": None,
        "evaluation_start_index": 0}
    assert checkpoint_boundary(detector, lambda: None) == 0


@pytest.mark.parametrize("damage", ["point_bytes", "packed_header", "cut_bytes", "threshold_property",
    "descriptor_enum", "runtime", "config"])
def test_rehashed_damage_of_native_checkpoint_is_rejected(damage):
    saved = native_checkpoint()
    before = deepcopy(saved)
    body = saved["body"]
    forest = body["mapper"]["forestState"]
    if damage == "point_bytes":
        forest["pointStoreState"]["pointData"] = "AAAA"
    elif damage == "packed_header":
        forest["compactSamplerStates"][0]["pointIndex"] = [0, 1, 2**31 - 1, 0]
    elif damage == "cut_bytes":
        tree = next(tree for tree in forest["compactRandomCutTreeStates"] if tree["nodeStoreState"]["size"] > 0)
        tree["nodeStoreState"]["cutValueData"] = "AAAA"
    elif damage == "threshold_property":
        threshold = body["mapper"]["predictorCorrectorState"]["thresholderStates"][0]
        threshold["zFactor"] = threshold.pop("zfactor")
    elif damage == "descriptor_enum":
        body["mapper"]["lastDescriptorState"]["correctionMode"] = "future"
    elif damage == "runtime":
        body["runtime"]["manifest"] = "0" * 64
    else:
        del body["config"]["seed"]
    saved["sha256"] = digest(body)
    with pytest.raises(DetectionError):
        validate_checkpoint(saved, body["config"], body["keys"], body["rule_hash"])
    assert before != saved
