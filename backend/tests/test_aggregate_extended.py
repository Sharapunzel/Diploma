"""Extended aggregate contracts and PostgreSQL regression cases (same DB profile)."""
from dataclasses import replace
from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_aggregate_sql import calculate

from app.aggregation.results import AggregateStatistics, FeatureStatistics, interpret_result
from app.aggregation.rules import (
    AggregateRule,
    Expression,
    Limits,
    RuleError,
    compile_rule,
    evaluate_expression,
    operator_catalog,
)

pytest_plugins = ["test_aggregate_sql"]


def feature(op, field='network.bytes', **params):
    return {'key': op, 'name': op, 'op': op, 'field': field, **params}


def count_where(condition):
    return {'where': condition, 'features': [{'key': 'n', 'name': 'N', 'op': 'count'}]}


def date_statistics_in_zone(database, catalog, values, zone, where=None, occurrences=None):
    """Exercise JSONB -> SQL -> psycopg datetime -> pure interpretation."""
    from sqlalchemy import func, literal, select, union_all
    from sqlalchemy.dialects.postgresql import JSONB

    from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement, read_statistics
    rule = AggregateRule.model_validate({'where': where, 'features': [
        feature(op, 'event.created') for op in
        ['distinct_count', 'mode_count', 'mode_share', 'present_count']]})
    compiled = compile_rule(rule, catalog)
    base = union_all(*[select(literal({'@timestamp': occurrences[i] if occurrences else '2000-01-01T00:00:00Z',
        'event': {'created': value}}, type_=JSONB).label('ecs_data')) for i, value in enumerate(values)]).subquery()
    with database.connect() as connection:
        connection.execute(select(func.set_config('TimeZone', zone, True)))
        row = connection.execute(aggregate_statement(base, compiled)).mappings().one()
    statistics = read_statistics(row, compiled)
    return interpret_result(statistics, compiled), statistics


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Asia/Yekaterinburg'])
@pytest.mark.parametrize('duplicates', [False, True])
def test_date_modes_use_absolute_moments_across_dst_fold(database, catalog, zone, duplicates):
    values = ['2026-11-01T05:30:00Z', '2026-11-01T06:30:00Z']
    if duplicates:
        values += ['2026-11-01T01:30:00-04:00', '2026-11-01T01:30:00-05:00']
    result, statistics = date_statistics_in_zone(database, catalog, values, zone)
    assert all(value.utcoffset().total_seconds() == 0
               for value in statistics.features['mode_count'].value)
    assert [item['value'] for item in result['features']] == [2, 2 if duplicates else 1, .5, len(values)]
    assert all(item['reason'] is None for item in result['features'])


def test_date_category_keys_stay_independent_of_driver_representation(catalog):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    compiled = compile_rule(AggregateRule.model_validate({'features': [
        feature(op, 'event.created') for op in ['mode_count', 'mode_share']]}), catalog)
    values = [datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo('America/New_York'), fold=fold)
              for fold in [0, 1]]
    assert values[0] == values[1]  # Python's fold equality must not define a category.
    statistics = AggregateStatistics(2, 2, 256, None, None,
        {op: FeatureStatistics(values, 2, 0) for op in ['mode_count', 'mode_share']})
    assert [item['value'] for item in interpret_result(statistics, compiled)['features']] == [1, .5]


@pytest.mark.parametrize('value', [
    '0001-01-01T00:00:00+01:00', '9999-12-31T23:59:59-01:00',
    '0001-01-01T00:00:00+01:00:01', '9999-12-31T23:59:59.999999-01:00:01',
])
def test_date_literals_outside_absolute_utc_range_are_rejected(catalog, value):
    from app.normalization.convert import convert_value
    normalized = convert_value(catalog.get('event.created'), value)
    assert normalized == value
    for op, literal_value in [('eq', value), ('in', [value]),
                              ('between', [value, value])]:
        with pytest.raises(RuleError) as error:
            compile_rule(AggregateRule.model_validate(count_where({
                'op': op, 'field': 'event.created', 'value': literal_value})), catalog)
        assert error.value.code == 'invalid_literal_type' and error.value.path == 'where.value'


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Pacific/Kiritimati'])
@pytest.mark.parametrize('value', [
    '0001-01-01T00:00:00+01:00', '9999-12-31T23:59:59-01:00',
])
def test_stored_date_outside_utc_range_is_invalid_input(database, catalog, zone, value):
    result, _ = date_statistics_in_zone(database, catalog,
        [value, '2000-01-01T00:00:00Z'], zone)
    assert result['event_count'] == 2
    for item in result['features']:
        assert item['value'] is None and item['reason'] == 'invalid_input_type'
        assert item['valid_count'] == 1 and item['invalid_count'] == 1
    assert result['event_time_min'].isoformat() == '2000-01-01T00:00:00+00:00'


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Pacific/Kiritimati'])
def test_valid_utc_boundaries_survive_driver_timezone(database, catalog, zone):
    from datetime import UTC, datetime
    values = ['0001-01-01T00:00:00Z', '9999-12-31T23:59:59.999999Z',
              '0001-01-01T01:00:01+01:00:01', '9999-12-31T22:59:58.999999-01:00:01']
    for condition in [None,
            {'op': 'between', 'field': 'event.created', 'value': values[:2]},
            {'op': 'in', 'field': 'event.created', 'value': values[2:]}]:
        result, _ = date_statistics_in_zone(database, catalog, values, zone,
            condition, occurrences=values)
        assert result['event_count'] == 4
        assert [item['value'] for item in result['features']] == [2, 2, .5, 4]
        assert result['event_time_min'] == datetime.min.replace(tzinfo=UTC)
        assert result['event_time_max'] == datetime.max.replace(tzinfo=UTC)


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Pacific/Kiritimati'])
@pytest.mark.parametrize('values', [
    ['0001-01-01T00:00:00Z', '0001-01-01T01:00:01+01:00:01'],
    ['9999-12-31T23:59:59.999999Z', '9999-12-31T22:59:58.999999-01:00:01'],
])
def test_utc_boundary_equality_keeps_offset_semantics(database, catalog, zone, values):
    from datetime import datetime
    result, _ = date_statistics_in_zone(database, catalog, values, zone,
        {'op': 'eq', 'field': 'event.created', 'value': values[1]}, occurrences=values)
    assert result['event_count'] == 2
    assert [item['value'] for item in result['features']] == [1, 2, 1, 2]
    assert result['event_time_min'] == result['event_time_max'] == datetime.fromisoformat(values[0])


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Pacific/Kiritimati'])
@pytest.mark.parametrize('op', ['has_any', 'has_all', 'has_none'])
def test_utc_date_array_boundaries_and_invalid_elements(database, catalog, zone, op):
    from sqlalchemy import func, literal, select
    from sqlalchemy.dialects.postgresql import JSONB

    from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement, read_statistics
    class DateArrayCatalog:
        def get(self, name):
            found = catalog.get(name)
            return replace(found, type='date') if name == 'event.category' else found
    typed = DateArrayCatalog()
    values = ['0001-01-01T00:00:00Z', '9999-12-31T23:59:59.999999Z']
    literals = ['0001-01-01T01:00:00+01:00', '9999-12-31T22:59:59.999999-01:00']
    compiled = compile_rule(AggregateRule.model_validate(count_where({
        'op': op, 'field': 'event.category', 'value': literals})), typed)
    with database.connect() as connection:
        connection.execute(select(func.set_config('TimeZone', zone, True)))
        for stored, expected_reason in [(values, None),
                (values + ['0001-01-01T00:00:00+01:00'], 'invalid_input_type')]:
            base = select(literal({'@timestamp': '2000-01-01T00:00:00Z',
                'event': {'category': stored}}, type_=JSONB).label('ecs_data')).subquery()
            row = connection.execute(aggregate_statement(base, compiled)).mappings().one()
            result = interpret_result(read_statistics(row, compiled), compiled)['features'][0]
            assert result['reason'] == expected_reason
            assert result['value'] == (None if expected_reason else int(op != 'has_none'))
        with pytest.raises(RuleError, match='invalid_literal_type'):
            compile_rule(AggregateRule.model_validate(count_where({
                'op': op, 'field': 'event.category',
                'value': ['9999-12-31T23:59:59-01:00']})), typed)


@pytest.mark.parametrize('zone', ['UTC', 'America/New_York', 'Asia/Yekaterinburg'])
@pytest.mark.parametrize('raw', [
    '1900-01-01T00:00:00+05:21:10', '1900-01-01T00:00:00-05:21:10',
    '1900-01-01T00:00:00.123456+05:21:10.123456',
    '1900-01-01T00:00:00+23:59:59',
])
def test_normalizer_dates_with_second_offsets(database, catalog, zone, raw):
    from datetime import UTC, datetime

    from app.normalization.convert import convert_value
    normalized = convert_value(catalog.get('event.created'), raw)
    assert normalized == raw
    utc = datetime.fromisoformat(normalized).astimezone(UTC).isoformat()
    # Either literal spelling must validate and match both representations.
    for value in [normalized, utc]:
        result, _ = date_statistics_in_zone(database, catalog, [normalized, utc], zone,
            {'op': 'eq', 'field': 'event.created', 'value': value})
        assert result['event_count'] == 2
        assert [item['value'] for item in result['features']] == [1, 2, 1, 2]
        assert all(item['reason'] is None for item in result['features'])


@pytest.mark.parametrize('value', [
    '1900-01-01T00:00:00', '1900-02-30T00:00:00+05:21:10',
    '1900-01-01T00:00:00+24:00:00', '1900-01-01T24:00:00Z',
    True, 3, {}, [],
])
def test_extended_date_parser_keeps_invalid_data_safe(database, catalog, value):
    with pytest.raises((RuleError, ValidationError)):
        compile_rule(AggregateRule.model_validate(count_where({
            'op': 'eq', 'field': 'event.created', 'value': value})), catalog)
    result, _ = date_statistics_in_zone(database, catalog, [value], 'America/New_York')
    assert all(item['value'] is None and item['reason'] == 'invalid_input_type'
               and item['invalid_count'] == 1 for item in result['features'])


@pytest.mark.parametrize('op,field,base_weight', [
    ('has_all', 'event.category', 2), ('has_none', 'event.category', 2),
    ('has_any', 'host.ip', 1),
])
def test_array_literal_scans_are_in_the_budget(catalog, op, field, base_weight):
    from app.aggregation.budget import estimate
    values = ([f'2001:db8::{i:x}' for i in range(256)] if field == 'host.ip'
              else [f'missing-{i}' for i in range(256)])
    budgets = []
    for size in [1, 256]:
        compiled = compile_rule(AggregateRule.model_validate(count_where({
            'op': op, 'field': field, 'value': values[:size]})), catalog)
        budget = estimate(compiled, 1, 1, 300, Limits(), False, 22000)
        assert budget['operator_weight'] == 1 + base_weight * size
        assert budget['cost'] == (1 + base_weight * size) * 86
        budgets.append(budget)
    assert budgets[1]['cost'] > budgets[0]['cost']
    with pytest.raises(RuleError, match='operator_cost'):
        estimate(compiled, 1, 1, 300, replace(Limits(), cost=1000), False, 22000)
    entry = next(item for item in operator_catalog() if item['name'] == op)
    assert 'len(value)' in entry['weight_formula']


def test_legacy_keyword_array_weights_and_hashes_stay_stable(catalog):
    for size in [1, 256]:
        compiled = compile_rule(AggregateRule.model_validate(count_where({
            'op': 'has_any', 'field': 'event.category', 'value': ['web'] * size})), catalog)
        assert compiled.weight == 2


@pytest.mark.parametrize('op,values,expected', [
    ('ratio', [3, 4], .75), ('percent', [.25], 25), ('per_second', [300], 1),
    ('abs', [-3], 3), ('clamp', [9, 1, 5], 5), ('clamp', [-1, 1, 5], 1),
    ('min_of', [4, 2, 3], 2), ('max_of', [4, 2, 3], 4),
])
def test_derived_operations(catalog, op, values, expected):
    expr = {'op': op, 'args': [{'value': v} for v in values]}
    compile_rule(AggregateRule.model_validate({'features': [
        {'key': 'n', 'name': 'N', 'op': 'derive', 'expr': expr}]}), catalog)
    assert evaluate_expression(Expression.model_validate(expr), {}, 300).value == expected
    for bad in [[], [{'value': 1}] * (4 if op == 'clamp' else 0)]:
        with pytest.raises(RuleError):
            compile_rule(AggregateRule.model_validate({'features': [
                {'key': 'n', 'name': 'N', 'op': 'derive', 'expr': {'op': op, 'args': bad}}]}), catalog)


@pytest.mark.parametrize('op,values,reason', [
    ('ratio', [1, 0], 'division_by_zero'), ('clamp', [1, 4, 2], 'invalid_clamp_bounds'),
    ('percent', [1e308], 'non_finite_result'),
])
def test_derived_failures(op, values, reason):
    assert evaluate_expression(Expression.model_validate({'op': op, 'args': [
        {'value': v} for v in values]}), {}).reason == reason


@pytest.mark.parametrize('op,field,value,events,expected', [
    ('neq', 'user.id', 'a', [{'user': {'id': 'b'}}, {}, {'user': {'id': None}}], 1),
    ('not_in', 'user.id', ['a'], [{'user': {'id': 'b'}}, {}], 1),
    ('gt', 'network.bytes', 2, [{'network': {'bytes': 2}}, {'network': {'bytes': 3}}], 1),
    ('gte', 'network.bytes', 2, [{'network': {'bytes': 2}}], 1),
    ('lt', 'network.bytes', 2, [{'network': {'bytes': 1}}], 1),
    ('lte', 'network.bytes', 2, [{'network': {'bytes': 2}}], 1),
    ('between', 'network.bytes', [2, 4], [{'network': {'bytes': v}} for v in [1, 2, 3, 4, 5]], 3),
    ('contains', 'user.id', '%_\\Ж', [{'user': {'id': 'a%_\\Жb'}}, {'user': {'id': 'anything'}}], 1),
    ('starts_with', 'user.id', "'Ж_", [{'user': {'id': "'Ж_abc"}}, {'user': {'id': 'Жabc'}}], 1),
    ('in_cidr', 'source.ip', '192.0.2.1/32', [{'source': {'ip': '192.0.2.1'}}, {'source': {'ip': '::1'}}], 1),
    ('in_cidr', 'source.ip', '2001:db8::/32', [{'source': {'ip': '2001:db8::1'}}, {'source': {'ip': '192.0.2.1'}}], 1),
    ('in_cidr', 'source.ip', '2001:db8::1/128', [{'source': {'ip': '2001:db8::1'}}, {'source': {'ip': '2001:db8::2'}}], 1),
    ('has_all', 'event.category', ['a', 'b'], [{'event': {'category': ['a', 'b']}}, {'event': {'category': []}}, {}], 1),
    ('has_none', 'event.category', ['a'], [{'event': {'category': ['b']}}, {'event': {'category': []}}, {}], 2),
    ('eq', 'event.created', '2026-01-01T01:00:00+01:00', [{'event': {'created': '2026-01-01T00:00:00Z'}}], 1),
    ('between', 'event.created', ['2026-01-01T00:00:00Z', '2026-01-01T02:00:00+01:00'], [{'event': {'created': '2026-01-01T00:30:00Z'}}], 1),
    ('eq', 'source.ip', '2001:db8::1', [{'source': {'ip': '2001:0db8:0:0::1'}}], 1),
])
def test_extended_conditions(database, catalog, op, field, value, events, expected):
    result = calculate(database, catalog, count_where({'op': op, 'field': field, 'value': value}), events)
    assert result['event_count'] == expected
    assert result['features'][0]['value'] == expected


@pytest.mark.parametrize('field,value,literal', [
    ('source.ip', 'broken', '127.0.0.1'), ('source.ip', '127.0.0.1/32', '127.0.0.1'),
    ('event.created', '2026-02-30T00:00:00Z', '2026-01-01T00:00:00Z'),
    ('event.created', 'yesterday', '2026-01-01T00:00:00Z'),
    ('network.bytes', 'oops', 1), ('user.id', ['a'], 'a'),
])
def test_not_never_selects_invalid_child(database, catalog, field, value, literal):
    data = {}; current = data
    parts = field.split('.')
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value
    condition = {'op': 'not', 'args': [{'op': 'eq', 'field': field, 'value': literal}]}
    result = calculate(database, catalog, count_where(condition), [data])
    assert result['event_count'] == 0
    assert result['features'][0]['reason'] == 'invalid_input_type'


def test_not_absence_and_logic_diagnostics(database, catalog):
    result = calculate(database, catalog, count_where({'op': 'not', 'args': [
        {'op': 'eq', 'field': 'user.id', 'value': 'a'}]}), [{}, {'user': {'id': None}}])
    assert result['event_count'] == 2
    for op in ['all', 'any']:
        result = calculate(database, catalog, count_where({'op': op, 'args': [
            {'op': 'not_exists', 'field': 'user.id'},
            {'op': 'eq', 'field': 'network.bytes', 'value': 1}]}), [{'network': {'bytes': 'bad'}}])
        assert result['features'][0]['invalid_count'] == 1


def test_exact_numeric_statistics(database, catalog):
    ops = ['range', 'variance', 'stddev', 'median', 'percentile', 'mode_count', 'mode_share', 'present_count', 'missing_count']
    rule = {'features': [feature(op, **({'percentile': .25} if op == 'percentile' else {})) for op in ops]}
    result = calculate(database, catalog, rule, [{'network': {'bytes': n}} for n in [2, 4, 4, 6]] + [{}, {'network': {'bytes': None}}])
    assert [f['value'] for f in result['features']] == pytest.approx([4, 2, 2**.5, 4, 3.5, 2, .5, 4, 2], rel=1e-14)
    assert [f['valid_count'] for f in result['features']] == [4]*8 + [2]
    empty = calculate(database, catalog, rule, [])
    assert [f['value'] for f in empty['features']] == [None]*7 + [0, 0]
    bad = calculate(database, catalog, rule, [{'network': {'bytes': 2}}, {'network': {'bytes': '2'}}])
    assert all(f['reason'] == 'invalid_input_type' and f['invalid_count'] == 1 for f in bad['features'])


@pytest.mark.parametrize('p,expected', [(0, 2**63-3), (.5, 2**63-2), (1, 2**63-1)])
def test_quantile_int64_does_not_round_input(database, catalog, p, expected):
    result = calculate(database, catalog, {'features': [feature('percentile', percentile=p)]},
                       [{'network': {'bytes': n}} for n in [2**63-3, 2**63-1]])
    assert type(result['features'][0]['value']) is int
    assert result['features'][0]['value'] == expected


def test_boolean_and_category_counters(database, catalog):
    rule = {'features': [feature(op, 'host.boot.id' if op.startswith('mode') else 'event.dataset')
                         for op in ['mode_count', 'mode_share']]}
    result = calculate(database, catalog, rule, [{'host': {'boot': {'id': v}}} for v in ['a', 'b', 'a', 'b']])
    assert [f['value'] for f in result['features']] == [2, .5]
    field = next(f for f in catalog.list_fields(None, None, None, None, 20000, 0)[0] if f.type == 'boolean' and not f.is_array)
    def event(v):
        result = {}; current = result
        for part in field.name.split('.')[:-1]:
            current = current.setdefault(part, {})
        current[field.name.split('.')[-1]] = v
        return result
    result = calculate(database, catalog, {'features': [feature(op, field.name) for op in ['true_count', 'false_count', 'present_count', 'missing_count']]},
                       [event(True), event(False), event(True), event(None), {}])
    assert [f['value'] for f in result['features']] == [2, 1, 3, 2]


@pytest.mark.parametrize('condition', [
    {'op': 'not', 'args': []}, {'op': 'not', 'args': [{'op': 'exists', 'field': 'user.id'}]*2},
    {'op': 'not_in', 'field': 'user.id', 'value': []},
    {'op': 'between', 'field': 'network.bytes', 'value': [3, 2]},
    {'op': 'between', 'field': 'network.bytes', 'value': [1]},
    {'op': 'contains', 'field': 'user.id', 'value': ''},
    {'op': 'starts_with', 'field': 'network.bytes', 'value': 'a'},
    {'op': 'in_cidr', 'field': 'source.ip', 'value': '127.0.0.1'},
    {'op': 'eq', 'field': 'event.created', 'value': '2026-01-01'},
    {'op': 'has_all', 'field': 'event.category', 'value': ['a', 1]},
])
def test_invalid_extended_forms(catalog, condition):
    with pytest.raises((RuleError, ValidationError)):
        compile_rule(AggregateRule.model_validate(count_where(condition)), catalog)


@pytest.mark.parametrize('p', [-1, 1.1, None])
def test_invalid_percentile(catalog, p):
    with pytest.raises(RuleError):
        compile_rule(AggregateRule.model_validate({'features': [feature('percentile', percentile=p)]}), catalog)


def test_catalog_exposes_precise_compatibility(catalog):
    entries = {op['name']: op for op in operator_catalog()}
    assert entries['not']['arity'] == {'min': 1, 'max': 1}
    assert entries['percentile']['parameter_types']['percentile'] == 'number[0,1]'
    assert {'type': 'ip', 'is_array': False} in entries['in_cidr']['compatible_fields']
    assert {'type': 'date', 'is_array': True} in entries['has_all']['compatible_fields']
    assert entries['count']['weight'] == 1 and entries['distinct_count']['weight'] == 8


def test_large_fractional_quantile_rounds_only_api_result(catalog):
    compiled = compile_rule(AggregateRule.model_validate({'features': [feature('median')]}), catalog)
    result = interpret_result(AggregateStatistics(2, 2, 256, None, None,
        {'median': FeatureStatistics([Decimal(2**63-2), Decimal(2**63-1)], 2, 0)}), compiled)
    # Documented binary64 rounding of the fractional result, after exact interpolation.
    assert result['features'][0]['value'] == float(Decimal('9223372036854775806.5'))


def test_numeric_statistics_remain_stable_near_int64_max(database, catalog):
    result = calculate(database, catalog, {'features': [feature(op) for op in ['range', 'variance', 'stddev']]},
                       [{'network': {'bytes': n}} for n in [2**63-3, 2**63-1]])
    assert [f['value'] for f in result['features']] == [2, 1, 1]
    result = calculate(database, catalog, {'features': [feature('range')]},
                       [{'network': {'bytes': n}} for n in [-2**63, 2**63-1]])
    assert result['features'][0]['value'] == 2**64-1


@pytest.mark.parametrize('bad', ['true', 1, [], {}])
def test_boolean_counter_rejects_coercion(database, catalog, bad):
    result = calculate(database, catalog, {'features': [feature(op, 'cloud.entity.attributes.managed')
        for op in ['true_count', 'false_count']]},
        [{'cloud': {'entity': {'attributes': {'managed': bad}}}}])
    assert all(f['reason'] == 'invalid_input_type' and f['invalid_count'] == 1 for f in result['features'])


def test_numeric_overflow_becomes_reason_not_postgres_error(database, catalog):
    result = calculate(database, catalog, {'features': [feature('sum', 'event.risk_score')]},
                       [{'event': {'risk_score': 1e308}}] * 2)
    assert result['features'][0]['reason'] == 'non_finite_result'

@pytest.mark.parametrize('ecs_type,values,literals', [
    ('keyword', ['a', 'b'], ['a']), ('boolean', [True, False], [True]),
    ('long', [1, 2], [1]), ('double', [1.5, 2.5], [1.5]),
    ('date', ['2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'], ['2026-01-01T01:00:00+01:00']),
    ('ip', ['2001:db8::1', '127.0.0.1'], ['2001:0db8:0:0::1']),
])
def test_array_type_engine(database, catalog, ecs_type, values, literals):
    # Exercise generic handlers for ECS array types even where 9.4.0 currently
    # has no field of that shape. Production still uses only packaged fields.
    class ArrayCatalog:
        def get(self, name):
            found = catalog.get(name)
            return replace(found, type=ecs_type) if name == 'event.category' else found
    for op, expected in [('has_any', 1), ('has_all', 1), ('has_none', 1)]:
        result = calculate(database, ArrayCatalog(), count_where({
            'op': op, 'field': 'event.category', 'value': literals}),
            [{'event': {'category': values}}, {'event': {'category': []}}, {}])
        assert result['features'][0]['value'] == expected
        invalid = calculate(database, ArrayCatalog(), count_where({
            'op': op, 'field': 'event.category', 'value': literals}),
            [{'event': {'category': [values[0], {}]}}])
        assert invalid['features'][0]['reason'] == 'invalid_input_type'


def test_array_limit_is_addressed_without_truncation(database, catalog):
    with pytest.raises(RuleError, match='array_elements'):
        calculate(database, catalog, count_where({'op': 'has_all', 'field': 'event.category', 'value': ['a']}),
                  [{'event': {'category': ['a'] * 4097}}])
    result = calculate(database, catalog, count_where({'op': 'has_all', 'field': 'event.category', 'value': ['a']}),
                       [{'event': {'category': ['a'] * 4096}}])
    assert result['features'][0]['value'] == 1


def test_array_presence_includes_empty(database, catalog):
    result = calculate(database, catalog, {'features': [feature(op, 'host.ip') for op in ['present_count', 'missing_count']]},
        [{'host': {'ip': []}}, {'host': {'ip': ['::1']}}, {}, {'host': {'ip': None}}])
    assert [f['value'] for f in result['features']] == [2, 2]


@pytest.mark.parametrize('op', ['range', 'variance', 'stddev', 'median', 'percentile'])
def test_numeric_singleton_and_invalid(database, catalog, op):
    rule = {'features': [feature(op, **({'percentile': .75} if op == 'percentile' else {}))]}
    result = calculate(database, catalog, rule, [{'network': {'bytes': 5}}])
    assert result['features'][0]['value'] == (0 if op in ['range', 'variance', 'stddev'] else 5)
    result = calculate(database, catalog, rule, [{'network': {'bytes': 1.0}}])
    assert result['features'][0]['reason'] == 'invalid_input_type'


def test_exact_statistics_memory_budget(catalog):
    from app.aggregation.budget import estimate
    for op in ['median', 'percentile', 'mode_count', 'mode_share']:
        compiled = compile_rule(AggregateRule.model_validate({'features': [
            feature(op, **({'percentile': .5} if op == 'percentile' else {}))]}), catalog)
        with pytest.raises(RuleError, match='total_memory'):
            estimate(compiled, 10000, 10000, 300, Limits(), False, 10000)


def test_date_and_per_second_ignore_database_timezone(database, catalog):
    from sqlalchemy import text
    rule = count_where({'op': 'gte', 'field': 'event.created', 'value': '2026-01-01T00:00:00Z'})
    rule['features'].append({'key': 'rate', 'name': 'Rate', 'op': 'derive', 'expr': {'op': 'per_second', 'args': [{'ref': 'n'}]}})
    events = [{'event': {'created': '2026-01-01T05:00:00+05:00'}}]
    with database.connect() as connection:
        for zone in ['Pacific/Honolulu', 'Asia/Tokyo', 'UTC']:
            connection.execute(text("SELECT set_config('TimeZone', :zone, true)"), {'zone': zone})
            # Same physical connection ensures the setting reaches the query.
            from sqlalchemy import literal, select
            from sqlalchemy.dialects.postgresql import JSONB

            from app.repositories.sqlalchemy.aggregate_compiler import (
                aggregate_statement,
                read_statistics,
            )
            base = select(literal({'@timestamp': '2026-01-01T00:00:00Z', **events[0]}, type_=JSONB).label('ecs_data')).subquery()
            compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
            row = connection.execute(aggregate_statement(base, compiled)).mappings().one()
            result = interpret_result(read_statistics(row, compiled), compiled, 300)
            assert [f['value'] for f in result['features']] == [1, 1/300]

@pytest.mark.parametrize('op', ['present_count', 'missing_count', 'true_count', 'false_count',
                                'range', 'variance', 'stddev', 'median', 'percentile', 'mode_count', 'mode_share'])
def test_new_features_reject_wrong_shape_and_type(catalog, op):
    params = {'percentile': .5} if op == 'percentile' else {}
    invalid_field = 'user.id' if op in ['true_count', 'false_count', 'range', 'variance', 'stddev', 'median', 'percentile'] else 'http.request.body.bytes.bogus'
    with pytest.raises(RuleError):
        compile_rule(AggregateRule.model_validate({'features': [feature(op, invalid_field, **params)]}), catalog)
    with pytest.raises(RuleError, match='invalid_arguments'):
        compile_rule(AggregateRule.model_validate({'features': [feature(op, expr={'value': 1}, **params)]}), catalog)


def test_text_category_is_now_supported(database, catalog):
    result = calculate(database, catalog, {'features': [feature('distinct_count', 'http.request.body.content')]},
        [{'http': {'request': {'body': {'content': v}}}} for v in ['a', 'a', 'b']])
    assert result['features'][0]['value'] == 2

@pytest.mark.parametrize('address', ['001.2.3.4', '127.1', '::ffff:001.2.3.4', '::1%lo', '1.2.3.4/24'])
def test_malformed_ip_is_invalid_not_a_cast_error(database, catalog, address):
    result = calculate(database, catalog, count_where({'op': 'in_cidr', 'field': 'source.ip', 'value': '0.0.0.0/0'}),
                       [{'source': {'ip': address}}])
    assert result['features'][0]['reason'] == 'invalid_input_type'


def test_extended_input_width_cost_is_explicit(catalog):
    from app.aggregation.budget import estimate
    compiled = compile_rule(AggregateRule.model_validate(count_where({
        'op': 'contains', 'field': 'user.id', 'value': 'a'})), catalog)
    narrow = estimate(compiled, 10, 10, 300, Limits(), False, 256)
    wide = estimate(compiled, 10, 10, 300, Limits(), False, 2560)
    assert wide['cost'] == narrow['cost'] * 10
    assert wide['estimated_memory_bytes'] > narrow['estimated_memory_bytes']


def test_representative_extended_plan(database, catalog, capsys):
    from sqlalchemy import Column, MetaData, Table, event, select
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.orm import Session

    from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement
    from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
    document = {'where': {'op': 'all', 'args': [
        {'op': 'has_any', 'field': 'event.category', 'value': ['web']},
        {'op': 'has_none', 'field': 'event.category', 'value': ['ignored']},
        {'op': 'in_cidr', 'field': 'source.ip', 'value': '192.0.2.0/24'},
        {'op': 'between', 'field': 'event.created', 'value': ['1999-01-01T00:00:00Z', '2001-01-01T00:00:00Z']}]},
        'features': [feature(op, **({'percentile': .75} if op == 'percentile' else {}))
                     for op in ['sum', 'avg', 'range', 'variance', 'stddev', 'median', 'percentile', 'mode_count', 'mode_share']]}
    events = Table('aggregate_operator_plan', MetaData(), Column('ecs_data', JSONB),
                   prefixes=['TEMPORARY'], postgresql_on_commit='DROP')
    base = select(events.c.ecs_data).subquery()
    compiled = compile_rule(AggregateRule.model_validate(document), catalog)
    statement = aggregate_statement(base, compiled)
    with Session(database) as session:
        SqlAlchemyAggregateRepository(session).limits(Limits(), False)
        connection = session.connection()
        events.create(connection)
        connection.execute(events.insert().values(ecs_data={
            '@timestamp': '2000-01-01T00:00:00Z', 'source': {'ip': '192.0.2.1'},
            'event': {'category': ['web'], 'created': '2000-01-01T00:00:00Z'},
            'network': {'bytes': 100}}))
        values = connection.execute(statement).mappings().one()
        assert values['event_count'] == 1
        assert [values['v'+str(i)] for i in range(5)] == [100, 100, 0, 0, 0]
        def explain(conn, cursor, sql, params, context, many):
            return 'EXPLAIN (ANALYZE, FORMAT JSON) ' + sql, params
        event.listen(connection, 'before_cursor_execute', explain, retval=True)
        result = connection.execute(statement)
        # EXPLAIN changes result columns; bypass the original aggregate row's
        # result processors while retaining bound-parameter adaptation.
        plan = result.cursor.fetchone()[0][0]
        result.close()
        assert plan.get('JIT') is None
        # Reproducible diagnosis for the timing smoke without a brittle local
        # wall-clock assertion; the real worker enforces the one-second gate.
        with capsys.disabled():
            print('Extended SQL plan:', {k: plan.get(k) for k in ['Planning Time', 'Execution Time', 'JIT']})


def test_legacy_array_rule_keeps_maximum_sql_width(catalog):
    from sqlalchemy import column, select
    from sqlalchemy.dialects.postgresql import JSONB

    from app.aggregation.budget import estimate
    from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement
    rule = count_where({'op': 'has_any', 'field': 'event.category', 'value': ['web']})
    rule['features'] = [{'key': 'n'+str(i), 'name': 'N', 'op': 'count'} for i in range(553)]
    compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
    assert estimate(compiled, 1, 1, 300, Limits(), False)['statement_columns'] == 1664
    assert len(aggregate_statement(select(column('ecs_data', JSONB)).subquery(), compiled).selected_columns) == 1664
