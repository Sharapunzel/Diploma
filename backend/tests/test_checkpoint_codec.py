"""Golden codec layouts and rehashed damaged content rejected without Java."""

from copy import deepcopy

import pytest
from checkpoint_fixtures import float_buffer, pack
from test_checkpoint_validation import rehash, saved_model

from app.detection.checkpoint_codec import packed
from app.detection.contracts import DetectionError, ModelConfig
from app.detection.protection import checkpoint_boundary


@pytest.mark.parametrize("encoded,expected", [([7, 4], [7, 4]), ([3, 3, 8], [3] * 8),
    ([0, 1, 8, 170], [0, 1] * 4), ([0, 2, 4, 69], [0, 2, 1, 2]), ([], [])])
def test_known_native_arraypacking_layout(encoded, expected):
    assert list(packed(encoded, 100, lambda: None, length=len(expected))) == expected


@pytest.mark.parametrize("damage", ["three_bytes", "aligned_truncation", "point_nan", "cut_nan",
    "raw_array", "declared_bomb", "truncated_word", "extra_word", "inactive_point", "reference_count",
    "location", "missing_location", "duplicate_index", "duplicate_zero", "cut_dimension", "child_bit",
    "unreachable_node", "empty_leaf", "root", "descriptor_enum", "descriptor_attribution",
    "preprocessor_enum", "preprocessor_time", "predictor_ignore", "predictor_sampling",
    "weights_short", "weights_long", "weights_time", "shingled_input_short", "shingled_point_null",
    "threshold_wrong_property"])
def test_rehashed_but_unrestorable_saved_content_never_releases_history(damage):
    model = saved_model()
    body = model["checkpoint"]["body"]
    forest = body["mapper"]["forestState"]
    store, sampler, tree = forest["pointStoreState"], forest["compactSamplerStates"][0], forest["compactRandomCutTreeStates"][0]
    nodes, pre, predictor = tree["nodeStoreState"], body["mapper"]["preprocessorStates"][0], body["mapper"]["predictorCorrectorState"]
    if damage == "three_bytes":
        store["pointData"] = "AAAA"
    elif damage == "aligned_truncation":
        store["pointData"] = float_buffer([0.])
    elif damage == "point_nan":
        store["pointData"] = float_buffer([float("nan"), 1.])
    elif damage == "cut_nan":
        nodes["cutValueData"] = float_buffer([float("nan")])
    elif damage == "raw_array":
        store["refCount"] = [2, 2] + [0] * 14
    elif damage == "declared_bomb":
        store["refCount"] = [0, 2, 2**31 - 1, 0]
    elif damage == "truncated_word":
        sampler["pointIndex"].pop()
    elif damage == "extra_word":
        sampler["pointIndex"].append(0)
    elif damage == "inactive_point":
        sampler["pointIndex"] = pack([2, 1, 0, 1, 0, 1, 0, 1])
    elif damage == "reference_count":
        sampler["pointIndex"] = pack([0, 0, 0, 1, 0, 1, 0, 1])
    elif damage == "location":
        store["locationList"] = [0, 2]
    elif damage == "missing_location":
        store["locationList"] = [0]
    elif damage == "duplicate_index":
        store["duplicateRefs"] = pack([16, 6, 1, 6])
    elif damage == "duplicate_zero":
        store["duplicateRefs"] = pack([0, 0, 1, 12])
    elif damage == "cut_dimension":
        nodes["cutDimension"] = [1]
    elif damage == "child_bit":
        nodes["leftIndex"] = [2]
    elif damage == "unreachable_node":
        nodes.update(size=2, leftIndex=[0, 1], rightIndex=[0, 0], cutDimension=[0, 0], cutValueData=float_buffer([.5, .5]))
    elif damage == "empty_leaf":
        nodes["cutValueData"] = float_buffer([2.])
    elif damage == "root":
        tree["root"] = 1
    elif damage == "descriptor_enum":
        body["mapper"]["lastDescriptorState"]["strategy"] = "future"
    elif damage == "descriptor_attribution":
        body["mapper"]["lastDescriptorState"]["attribution"] = None
    elif damage == "preprocessor_enum":
        pre["imputationMethod"] = "future"
    elif damage == "preprocessor_time":
        pre["previousTimeStamps"] = []
    elif damage == "predictor_ignore":
        predictor["ignoreNearExpected"] = [-1.] * 4
    elif damage == "weights_short":
        pre["weights"] = [1.]
    elif damage == "weights_long":
        pre["weights"] = [1., 1., 1.]
    elif damage == "weights_time":
        pre["weights"][-1] = .5
    elif damage == "shingled_input_short":
        pre["lastShingledInput"] = []
    elif damage == "shingled_point_null":
        pre["lastShingledPoint"] = None
    elif damage == "threshold_wrong_property":
        threshold = predictor["thresholderStates"][0]
        threshold["zFactor"] = threshold.pop("zfactor")
    else:
        predictor["samplingSuppport"] = .2
    rehash(model)
    before = deepcopy(model)
    with pytest.raises(DetectionError):
        checkpoint_boundary(model, lambda: None)
    assert model == before


def test_valid_single_leaf_component_and_duplicate_ref_surplus_are_supported():
    model = saved_model()
    forest = model["checkpoint"]["body"]["mapper"]["forestState"]
    forest["compactSamplerStates"][0]["pointIndex"] = pack([0] * 8)
    tree = forest["compactRandomCutTreeStates"][0]
    tree["root"] = 7
    tree["nodeStoreState"].update(size=0, canonicalAndNotALeaf=False,
        leftIndex=None, rightIndex=None, cutDimension=None, cutValueData=None)
    forest["pointStoreState"]["duplicateRefs"] = pack([0, 10, 1, 2])
    rehash(model)
    assert checkpoint_boundary(model, lambda: None) == 41


def test_create_time_config_defaults_are_preserved():
    assert ModelConfig(dimensions=1).sample_size == 256


def test_saved_preprocessor_has_augmented_time_weight_but_unaugmented_input():
    model = saved_model()
    pre = model["checkpoint"]["body"]["mapper"]["preprocessorStates"][0]
    assert pre["weights"] == [1., 1.]
    assert pre["lastShingledPoint"] == pre["lastShingledInput"] == [0.]
    rehash(model)
    assert checkpoint_boundary(model, lambda: None) == 41
