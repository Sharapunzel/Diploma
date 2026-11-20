"""Mandatory Linux/JVM gate. Not collected by the ordinary host file selection."""

import json
import math
import subprocess
import sys
from pathlib import Path

import pytest


def probe(points, dimensions, *, state=None, start=0):
    process = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("rcf_process.py"))],
        input=json.dumps({"points": points, "dimensions": dimensions, "state": state, "start": start}),
        text=True, capture_output=True, timeout=120, check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def differences(left, right, path=""):
    if isinstance(left, dict):
        if not isinstance(right, dict) or left.keys() != right.keys():
            return [(path, "keys differ")]
        return [d for key in left for d in differences(left[key], right[key], f"{path}.{key}")]
    if isinstance(left, list):
        if not isinstance(right, list) or len(left) != len(right):
            return [(path, "length differs")]
        return [d for i, (a, b) in enumerate(zip(left, right))
                for d in differences(a, b, f"{path}[{i}]")]
    if isinstance(left, float) and isinstance(right, (float, int)):
        if not math.isclose(left, right, rel_tol=1e-6, abs_tol=1e-8):
            return [(path, (left, right))]
    elif left != right:
        return [(path, (left, right))]
    return []


@pytest.mark.parametrize("dimensions", [1, 3])
def test_official_mapper_continuation(dimensions):
    # Deterministic, varying history; repeated vectors and controlled anomalies.
    points = [[round(math.sin(i * .17 + j) * .2 + .5, 6)
               for j in range(dimensions)] for i in range(672)]
    for i in (520, 550, 590, 630, 670):
        points[i] = [4.0 + j for j in range(dimensions)]
    for i in range(300, 320):
        points[i] = [.5] * dimensions
    prefix = probe(points[:512], dimensions)
    reference = probe(points, dimensions)
    restored_results = []
    state = prefix["state"]
    for begin, end in ((512, 552), (552, 612), (612, 672)):
        restored = probe(points[begin:end], dimensions, state=state, start=begin)
        restored_results.extend(restored["results"])
        state = restored["state"]
    expected = reference["results"][512:]
    assert any(row["grade"] > 0 for row in expected), "No controlled anomaly detected"
    for row in expected:
        assert math.isfinite(row["score"]) and row["score"] >= 0
        assert 0 <= row["grade"] <= 1 and 0 <= row["confidence"] <= 1
        if row["grade"] > 0:
            assert row["attribution"] is not None
    mismatch = differences(expected, restored_results, "continuation")
    decisions = [i + 512 for i, (a, b) in enumerate(zip(expected, restored_results))
                 if (a["grade"] > 0) != (b["grade"] > 0)]
    state_mismatch = differences(reference["state"], state, "state")
    assert not mismatch and not state_mismatch and not decisions, (
        f"JRE={reference['java']} JPype={reference['jpype']}; "
        f"decision differences={decisions}; output differences={len(mismatch)}: {mismatch[:8]}; "
        f"state differences={len(state_mismatch)}: {state_mismatch[:4]}"
    )


def test_constant_history_mapper_continuation():
    points = [[.5, .5]] * 512
    prefix = probe(points, 2)
    continued = probe([[.5, .5]] * 101, 2, state=prefix["state"], start=512)
    reference = probe(points + [[.5, .5]] * 101, 2)
    assert not differences(reference["results"][512:], continued["results"])
    assert not differences(reference["state"], continued["state"])


def test_pinned_checkpoint_golden(capsys, monkeypatch):
    """Repeatable native asset recipe on the existing backend-db --rcf profile.

    Select this node and capture output in the owned run directory. Its
    RCF_CHECKPOINT_GOLDEN marker contains only a deterministic synthetic model.
    On intentional regeneration, extract that JSON into the permanent fixture;
    never substitute a hand-written mapper. The complete RCF gate repeats the
    writer/hash/real-restore comparison against the checked-in asset.
    """
    from dataclasses import asdict
    from time import monotonic

    from app.aggregation.event_time import OperationBudget
    from app.detection.aws_rcf import AwsModelFactory
    from app.detection.checkpoint import validate_checkpoint
    from app.detection.contracts import DetectionError, ModelConfig, canonical, digest

    config = asdict(ModelConfig(dimensions=2, trees=20, sample_size=8, output_after=8, seed=42))
    keys, rule_hash = ["count", "bytes"], digest({"synthetic_native_golden": 1})
    rows = [[(i * 7) % 11, (i * i * 3) % 17] for i in range(16)]
    factory = AwsModelFactory()
    checkpoint = factory.warmup(config, keys, rule_hash, rows)
    # Export before the neutral check: an unexpected native schema must remain
    # inspectable after a failed gate, without weakening that gate's assertion.
    with capsys.disabled():
        print("RCF_CHECKPOINT_GOLDEN=" + canonical(checkpoint), flush=True)
    started = monotonic()
    validate_checkpoint(checkpoint, config, keys, rule_hash, check_budget=OperationBudget(.85).check)
    with capsys.disabled():
        print({"native_checkpoint_validation_seconds": monotonic() - started,
            "checkpoint_bytes": len(canonical(checkpoint).encode()), "sha256": checkpoint["sha256"]})
    restored = factory.restore(checkpoint, config, keys, rule_hash)
    result = restored.process([17, 23])
    asset = Path(__file__).parents[1] / "fixtures" / "aws_rcf_4_4_0_checkpoint.json"
    assert asset.is_file(), "The mandatory native checkpoint fixture is missing"
    saved = json.loads(asset.read_text(encoding="utf-8"))
    assert saved["sha256"] == checkpoint["sha256"]
    assert digest(saved["body"]) == saved["sha256"]
    assert factory.restore(saved, config, keys, rule_hash).process([17, 23]) == result
    damaged = json.loads(canonical(checkpoint))
    damaged["body"]["mapper"]["forestState"]["pointStoreState"]["pointData"] = "AAAA"
    damaged["sha256"] = digest(damaged["body"])
    with monkeypatch.context() as scope:
        scope.setattr("app.detection.checkpoint.validate_mapper",
            lambda *_a, **_k: pytest.fail("Execution duplicated the native codec proof"))
        # The common envelope/history checks pass. Only the actual pinned Java
        # mapper can reject these three bytes, so the negative proves the native
        # restoration boundary rather than the Python protection parser.
        with pytest.raises(DetectionError, match="checkpoint_corrupt"):
            factory.restore(damaged, config, keys, rule_hash)


def test_default_checkpoint_validation_budget(capsys):
    """A typical full production envelope must fit the maintenance work budget."""
    from dataclasses import asdict
    from time import monotonic

    from app.aggregation.event_time import OperationBudget
    from app.detection.aws_rcf import AwsModelFactory
    from app.detection.checkpoint import validate_checkpoint, validate_execution_checkpoint
    from app.detection.contracts import ModelConfig, canonical_size, digest

    defaults = ModelConfig(dimensions=2)
    config, keys = asdict(defaults), ["count", "bytes"]
    assert (defaults.trees, defaults.sample_size, defaults.output_after, defaults.minimum_inputs) == (100, 256, 256, 512)
    rule_hash = digest({"synthetic_default_validation_budget": 1})
    rows = [[i % 31, (i * 7) % 97] for i in range(defaults.minimum_inputs)]
    checkpoint = AwsModelFactory().warmup(config, keys, rule_hash, rows)
    size = canonical_size(checkpoint)
    started, validated = monotonic(), False
    try:
        validate_checkpoint(checkpoint, config, keys, rule_hash, check_budget=OperationBudget(.85).check)
        validated = True
    finally:
        elapsed = monotonic() - started
        with capsys.disabled():
            print({"default_checkpoint_validation_seconds": elapsed, "validated": validated,
                "checkpoint_bytes": size, "config": config}, flush=True)
    assert elapsed < .85
    unbudgeted_started = monotonic()
    validate_checkpoint(checkpoint, config, keys, rule_hash)
    with capsys.disabled():
        print({"default_checkpoint_unbudgeted_validation_seconds": monotonic() - unbudgeted_started,
            "checkpoint_bytes": size, "config": config}, flush=True)
    execution_started = monotonic()
    validate_execution_checkpoint(checkpoint, config, keys, rule_hash)
    with capsys.disabled():
        print({"default_checkpoint_execution_validation_seconds": monotonic() - execution_started,
            "checkpoint_bytes": size, "config": config}, flush=True)
