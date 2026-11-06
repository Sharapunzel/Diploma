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
