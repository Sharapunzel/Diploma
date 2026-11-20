"""Model-history reserve and saved dependency validation, without a JVM/DB."""

from copy import deepcopy
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from checkpoint_fixtures import checkpoint_body

from app.aggregation.event_time import warmup_indices
from app.aggregation.retention import protection_plan
from app.detection.contracts import ModelConfig, digest
from app.detection.numeric import Affine
from app.detection.protection import attempt_bounds, prepared_boundary

ANCHOR = datetime(2026, 1, 1, tzinfo=UTC)


def detector(**updates):
    config = asdict(ModelConfig(dimensions=1, sample_size=8, output_after=8))
    return {"id": uuid4(), "name": "Independent model", "config": config,
        "config_hash": digest(config), "feature_keys": ["n"], "rule_hash": "f" * 64,
        "state": "waiting", "execution_state": "stopped", "failure_code": None,
        "accelerated": False, "step_seconds": 300, "checkpoint": None,
        "consumed_index": None, "evaluation_start_index": None, "active_attempt_id": None, **updates}


def schedule(duration=300):
    return {"anchor_utc": ANCHOR, "duration_seconds": duration, "online_start_index": 1001}


def checkpoint(model, consumed=40):
    config = ModelConfig(**model["config"])
    body = checkpoint_body(model, config)
    model.update(checkpoint={"body": body, "sha256": digest(body)}, state="ready",
        evaluation_start_index=0, consumed_index=consumed)
    return model


def attempt(model, last=1000, status="preparing"):
    shape = warmup_indices(last, 300, model["step_seconds"], model["config"]["sample_size"],
        model["config"]["output_after"], model["accelerated"])
    cutoff = ANCHOR + timedelta(seconds=shape["cutoff_seconds"])
    plan = {"first_start": (ANCHOR + timedelta(seconds=shape["starts"][0])).isoformat(),
        "cutoff": cutoff.isoformat(), "count": shape["count"], "step": model["step_seconds"],
        "duration": 300, "evaluation_start_index": max(1001, last + 1)}
    row = {"id": uuid4(), "detector_id": model["id"], "cutoff": cutoff,
        "plan": plan, "status": status, "inputs": [], "inputs_hash": digest([])}
    model["active_attempt_id"] = row["id"]
    return row


def prepared(model):
    row = attempt(model, status="prepared")
    rows = [[i + 1] for i in range(row["plan"]["count"])]
    transform = Affine.fit(rows)
    start = datetime.fromisoformat(row["plan"]["first_start"])
    for i, values in enumerate(rows):
        left = start + timedelta(seconds=i * model["step_seconds"])
        row["inputs"].append({"basis": "event_time", "start": left.isoformat(),
            "end": (left + timedelta(seconds=300)).isoformat(), "completeness": "complete",
            "features": [{"key": "n", "value": values[0], "reason": None, "invalid_count": 0}],
            "exact": values, "ml_input": list(transform.convert(values))})
    row["inputs"][0]["affine"] = {"algorithm": "min_range_decimal_float32_v1",
        "center": [str(x) for x in transform.center], "scale": [str(x) for x in transform.scale]}
    row["inputs_hash"] = digest(row["inputs"])
    return row


def plan(models=(), attempts=None, last=1000, present=1000):
    return protection_plan(schedule(), models, attempts or {}, last, present, lambda: None)


@pytest.mark.parametrize("models", [[], [detector()], [detector(), detector()]])
def test_default_reserve_is_512_even_for_no_or_small_models(models):
    result = plan(models)
    assert result["reserve_windows"] == 512
    assert result["keep_from_index"] == 489


def test_larger_config_raises_reserve_and_cleared_last_still_defines_grid():
    config = asdict(ModelConfig(dimensions=1, sample_size=400, output_after=400))
    model = detector(config=config, config_hash=digest(config))
    result = plan([model], last=1000, present=800)
    assert result["reserve_windows"] == 800 and result["keep_from_index"] == 1


@pytest.mark.parametrize("duration,step,n", [(300, 200, 4), (300, 1, 16), (5, 2, 16), (604800, 604800, 512)])
def test_accelerated_base_grid_reserve_proof(duration, step, n):
    actual = warmup_indices(10000, duration, step, n // 2, n // 2, True)
    assert actual["base_last"] - actual["base_first"] + 1 <= max(n, 9)
    if (duration, step, n) == (300, 200, 4):
        assert actual["count"] == 12
        assert actual["base_last"] - actual["base_first"] + 1 == 9
        assert actual["starts"][-1] + duration - actual["starts"][0] == 2500


@pytest.mark.parametrize("execution", ["running", "stopped", "requested", "starting", "failed"])
def test_checkpoint_unread_suffix_is_not_capped_by_reserve(execution):
    model = checkpoint(detector(execution_state=execution))
    result = plan([model])
    assert result["keep_from_index"] == 41
    assert result["protected_by"][0]["protected_from_index"] == 41


def test_all_dependencies_are_used_even_when_summary_shows_ten():
    models = [checkpoint(detector(), consumed=900) for _ in range(11)]
    models[-1]["consumed_index"] = 4
    result = plan(models)
    assert result["protected_total"] == 11 and len(result["protected_by"]) == 10
    assert result["keep_from_index"] == 5


@pytest.mark.parametrize("status", ["preparing", "waiting"])
def test_frozen_attempt_uses_old_cutoff_after_tail_advances(status):
    model = detector()
    row = attempt(model, last=50, status=status)
    result = plan([model], {row["id"]: row})
    assert result["keep_from_index"] == 35
    assert result["protected_by"][0]["reason"] == "frozen_warmup"


@pytest.mark.parametrize("accelerated,step", [(False, 300), (True, 200)])
def test_valid_prepared_batch_is_self_contained_with_one_common_affine(accelerated, step):
    model = detector(accelerated=accelerated, step_seconds=step)
    row = prepared(model)
    assert all("affine" not in item for item in row["inputs"][1:])
    assert prepared_boundary(model, schedule(), row, lambda: None) == 1001
    result = plan([model], {row["id"]: row})
    assert result["keep_from_index"] == 489
    assert result["protected_by"][0]["protected_from_index"] == 1001
    assert result["protected_by"][0]["protected_to_index"] is None


@pytest.mark.parametrize("damage", ["hash", "owner", "count", "time", "quality", "exact", "ml", "affine"])
def test_invalid_prepared_input_never_releases_sources(damage):
    model = detector()
    row = prepared(model)
    if damage == "hash":
        row["inputs_hash"] = "0" * 64
    elif damage == "owner":
        row["detector_id"] = uuid4()
    elif damage == "count":
        row["inputs"].pop()
    elif damage == "time":
        row["inputs"][1]["end"] = row["inputs"][0]["end"]
    elif damage == "quality":
        row["inputs"][1]["completeness"] = "unknown"
    elif damage == "exact":
        row["inputs"][1]["exact"] = [77]
    elif damage == "ml":
        row["inputs"][1]["ml_input"] = [77.0]
    else:
        row["inputs"][0]["affine"]["scale"] = ["0"]
    if damage != "hash":
        row["inputs_hash"] = digest(row["inputs"])
    with pytest.raises((ValueError, KeyError, TypeError)):
        plan([model], {row["id"]: row})


@pytest.mark.parametrize("damage", ["digest", "config", "mapper", "affine", "history", "cursor"])
def test_invalid_checkpoint_fails_closed(damage):
    model = deepcopy(checkpoint(detector()))
    body = model["checkpoint"]["body"]
    if damage == "digest":
        model["checkpoint"]["sha256"] = "0" * 64
    elif damage == "config":
        body["keys"] = ["other"]
    elif damage == "mapper":
        body["mapper"]["forestState"]["dimensions"] = 2
    elif damage == "affine":
        body["affine"]["scale"] = ["0"]
    elif damage == "history":
        body["history"][0]["ml"] = [99.0]
    else:
        model["consumed_index"] = model["evaluation_start_index"] = None
    if damage != "digest":
        model["checkpoint"]["sha256"] = digest(body)
    with pytest.raises((ValueError, KeyError, TypeError)):
        plan([model])


def test_only_explicit_immutable_retired_pair_is_ignored():
    model = detector(execution_state="terminated", failure_code="aggregate_time_semantics_retired", config={})
    assert plan([model])["protected_total"] == 0
    model["failure_code"] = "aggregate_deleted"
    with pytest.raises((ValueError, TypeError)):
        plan([model])


def test_ready_without_checkpoint_unknown_attempt_and_budget_do_not_release_history():
    with pytest.raises(ValueError):
        plan([detector(state="ready")])
    model = detector()
    row = attempt(model, status="unknown")
    with pytest.raises(ValueError):
        plan([model], {row["id"]: row})
    def exhausted():
        raise TimeoutError("budget")
    with pytest.raises(TimeoutError):
        protection_plan(schedule(), [detector()], {}, 1000, 1000, exhausted)


def test_digest_budget_interrupts_nested_visit_and_preserves_existing_digest():
    value = {"exact": [2**60 + 1, 1e30, 1.0], "nested": [{"x": [i, i + 1]} for i in range(100)]}
    expected = digest(value)
    visits = [0]
    def bounded():
        visits[0] += 1
        if visits[0] >= 12:
            raise TimeoutError("shared retention deadline")
    with pytest.raises(TimeoutError, match="shared retention deadline"):
        digest(value, check_budget=bounded)
    assert visits[0] == 12
    assert digest(value) == expected


@pytest.mark.parametrize("field", ["dimensions", "trees", "sample_size", "output_after", "seed"])
@pytest.mark.parametrize("state", ["created", "preparing", "prepared", "checkpoint"])
def test_missing_persisted_config_field_never_releases_history(field, state):
    model = detector()
    row = (prepared(model) if state == "prepared" else attempt(model)) if state in {"preparing", "prepared"} else None
    if state == "checkpoint":
        checkpoint(model)
    del model["config"][field]
    model["config_hash"] = digest(model["config"])
    if model["checkpoint"]:
        model["checkpoint"]["sha256"] = digest(model["checkpoint"]["body"])
    before = deepcopy(model)
    with pytest.raises(ValueError, match="checkpoint_config_mismatch"):
        plan([model], {row["id"]: row} if row else None)
    if row:
        with pytest.raises(ValueError, match="checkpoint_config_mismatch"):
            attempt_bounds(model, schedule(), row, lambda: None)
        if state == "prepared":
            with pytest.raises(ValueError, match="checkpoint_config_mismatch"):
                prepared_boundary(model, schedule(), row, lambda: None)
    assert model == before
