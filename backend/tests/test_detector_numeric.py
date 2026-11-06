from decimal import Decimal

import pytest

from app.detection.numeric import Affine, NumericQualityError


def test_neighboring_large_longs_are_subtracted_before_conversion():
    rows = [[2**60 + i] for i in range(3)]
    original = [row[:] for row in rows]
    scale = Affine.fit(rows)
    assert [scale.convert(row) for row in rows] == [(0.,), (.5,), (1.,)]
    assert scale.inverse((.5,)) == (Decimal(2**60 + 1),)
    assert rows == original


def test_extreme_decimal_ranges_and_constant_coordinate():
    rows = [[Decimal("1e-300"), Decimal("1e300"), 4],
            [Decimal("2e-300"), Decimal("2e300"), 4]]
    scale = Affine.fit(rows)
    assert scale.convert(rows[0]) == (0., 0., 0.)
    assert scale.convert(rows[1]) == (1., 1., 0.)
    assert scale.scale[2] == 1
    assert scale.inverse((1., 1., 0.)) == tuple(rows[1])


def test_collapse_is_rejected_in_history_and_later_sample_history():
    with pytest.raises(NumericQualityError, match="float32_collision"):
        Affine.fit([[0], [2**60], [2**60 + 1]])
    scale = Affine.fit([[0], [2**60]])
    with pytest.raises(NumericQualityError, match="float32_collision"):
        scale.convert([2**60 + 1], [([2**60], (1.,))])
    # Once the old input leaves the bounded history there is no global registry.
    assert scale.convert([2**60 + 1]) == (1.,)


@pytest.mark.parametrize("value", [True, None, "1", float("nan"), float("inf")])
def test_non_numeric_or_nonfinite(value):
    with pytest.raises(NumericQualityError):
        Affine.fit([[value]])


def test_underflow_and_overflow_do_not_process_or_clip():
    scale = Affine.fit([[0], [1]])
    for value, reason in [(Decimal("1e-100"), "underflow"), (Decimal("1e100"), "overflow")]:
        with pytest.raises(NumericQualityError, match=reason):
            scale.convert([value])
    assert scale.convert([2]) == (2.,)


def test_order_and_width_are_preserved_beyond_five_coordinates():
    scale = Affine.fit([list(range(8)), list(range(1, 9))])
    assert scale.convert(list(range(1, 9))) == (1.,) * 8
    with pytest.raises(NumericQualityError, match="dimension_mismatch"):
        scale.convert([1])


class BudgetExpired(Exception):
    pass


def test_fit_budget_stops_during_exact_conversion_and_preserves_inputs(monkeypatch):
    from app.detection import numeric

    rows = [[2**60 + i] for i in range(512)]
    original = [row[:] for row in rows]
    calls = 0
    convert_exact = numeric.exact
    def exact(value, coordinate):
        nonlocal calls
        calls += 1
        return convert_exact(value, coordinate)
    def check():
        if calls >= 100:
            raise BudgetExpired
    monkeypatch.setattr(numeric, "exact", exact)
    with pytest.raises(BudgetExpired):
        Affine.fit(rows, check_budget=check)
    assert calls == 100 and rows == original


def test_fit_budget_checks_collision_index_after_conversion(monkeypatch):
    converted = False
    convert = Affine.convert
    def conversion(self, *args, **kwargs):
        nonlocal converted
        result = convert(self, *args, **kwargs)
        converted = True
        return result
    def check():
        if converted:
            raise BudgetExpired
    monkeypatch.setattr(Affine, "convert", conversion)
    with pytest.raises(BudgetExpired):
        Affine.fit([[0], [1]], check_budget=check)


def test_conversion_budget_is_checked_inside_previous_sample_scan():
    calls = 0
    def check():
        nonlocal calls
        calls += 1
        if calls == 6:
            raise BudgetExpired
    previous = [([i], [0.]) for i in range(512)]
    scale = Affine.fit([[0], [1]])
    with pytest.raises(BudgetExpired):
        scale.convert([1], previous, check_budget=check)
    assert calls == 6
