from datetime import UTC, datetime, timedelta

import pytest

from app.detection.contracts import DetectionError, ModelConfig, digest, feature_vector
from app.detection.history import plan_windows, warmup_plan, warmup_summary


def test_utc_plan_and_overlap_coverage():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    now = anchor + timedelta(days=5, seconds=27)
    plan = warmup_plan(anchor, 300, now, 512, 5)
    windows = list(plan_windows(plan))
    assert len(windows) == 512
    assert windows[-1][1] <= now
    assert windows[1][0] - windows[0][0] == timedelta(seconds=5)
    assert windows[-1][1] - windows[0][0] >= timedelta(seconds=8 * 300)
    other = warmup_plan(anchor, 300, now.astimezone(), 512, 5)
    assert other == plan


@pytest.mark.parametrize("step", [0, -1, 301, True, 1.5])
def test_bad_step(step):
    with pytest.raises(DetectionError):
        warmup_plan(datetime(2026, 1, 1, tzinfo=UTC), 300,
                    datetime(2026, 2, 1, tzinfo=UTC), 512, step)


def test_plan_budget_and_short_history():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(DetectionError, match="resource_budget"):
        warmup_plan(anchor, 604800, anchor + timedelta(days=60), 512, 1)
    with pytest.raises(DetectionError, match="insufficient_history"):
        warmup_plan(anchor, 300, anchor + timedelta(seconds=500), 512, 300)


@pytest.mark.parametrize("change", ["missing", "extra", "duplicate", "order", "null", "bool", "reason", "invalid"])
def test_feature_quality(change):
    features = [{"key": "a", "value": 0, "reason": None, "invalid_count": 0},
                {"key": "b", "value": 3, "reason": None, "invalid_count": 0}]
    if change == "missing": features.pop()
    if change == "extra": features.append({"key": "c"})
    if change == "duplicate": features[1]["key"] = "a"
    if change == "order": features.reverse()
    if change == "null": features[0]["value"] = None
    if change == "bool": features[0]["value"] = True
    if change == "reason": features[0]["reason"] = "invalid"
    if change == "invalid": features[0]["invalid_count"] = 1
    with pytest.raises(ValueError):
        feature_vector(features, ["a", "b"], "complete")


@pytest.mark.parametrize("coverage", ["unknown", "interrupted", "unverified"])
def test_numeric_payload_never_promotes_coverage(coverage):
    with pytest.raises(DetectionError, match="coverage"):
        feature_vector([{"key": "n", "value": 0}], ["n"], coverage)
    assert feature_vector([{"key": "n", "value": 0}], ["n"], "complete") == [0]


@pytest.mark.parametrize("changes", [{"trees": 0}, {"sample_size": 0}, {"dimensions": 0},
                                    {"output_after": 1}, {"dimensions": 10000}])
def test_model_config_budget(changes):
    with pytest.raises(DetectionError):
        ModelConfig(**({"dimensions": 6} | changes))


def test_checkpoint_checksum_survives_jsonb_integral_exponent_expansion():
    import json
    from decimal import Decimal
    body = {"values": [float(2**60), 1e100, -0.0, 1e-100, 1.5]}
    # JSONB renders integral exponent numerals without an exponent; psycopg's
    # JSON decoder then returns Python ints. It must remain the same envelope.
    read_back = {"values": [int(Decimal(str(x))) if x.is_integer() else x
                            for x in body["values"]]}
    assert digest(body) == digest(read_back)
    assert json.loads(json.dumps(body))["values"][-2:] == read_back["values"][-2:]


def test_summary_counts_interval_union_without_treating_overlap_as_independence():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    inputs = [{"start": (start + timedelta(seconds=i)).isoformat(),
               "end": (start + timedelta(seconds=i+10)).isoformat(),
               "completeness": "complete", "exact": [i], "ml_input": [i/2]}
              for i in range(3)]
    summary = warmup_summary({"plan": {"count": 3}, "inputs": inputs},
                             {"state": "ready", "reason": None})
    assert summary["eligible_windows"] == 3
    assert summary["eligible_coverage_seconds"] == 12
