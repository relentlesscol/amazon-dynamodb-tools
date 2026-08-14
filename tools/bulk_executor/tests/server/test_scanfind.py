"""Unit tests for the `scanfind` server-side verb.

Covers `python_modules/scanfind/__init__.py`:
- ListAccumulator: zero / addInPlace contract for error accumulation
- Module constants: TOP_N preview cap, DEFAULT_SEGMENTS
- print_dynamodb_table_info: boto3 session region + shared helper calls
- _wire_json_default: binary (bytes/bytearray/memoryview) re-encoded to base64,
  every other type rejected rather than silently stringified
- _to_wire_json: one item -> one compact line of DynamoDB JSON, no encoder in
  the path (numbers stay {"N": "..."} strings, sets keep SS/NS/BS, NULL kept)
- _parse_limit: None passthrough, int coercion, non-integer and non-positive
  rejection
- _write_json_lines: saveAsTextFile (verbatim), never the DataFrame JSON writer
- _print_preview: bounded top-N preview with an explicit "...and N more"
- _build_scan_kwargs: TableName/Segment/TotalSegments, Select=ALL_ATTRIBUTES on
  the base table but omitted for an index, optional filter/expression params
  parsed as plain JSON, and the scan Limit only when nothing is filtered
- _scan_segment: low-level client (NOT the resource), boto3 Config (timeouts,
  retries), pagination loop, per-segment limit early stop, per-worker error
  accumulation, rate-limiter shutdown in finally
- run(): argument wiring, rate-limiter shared config, monitor options, spark
  parallelize/flatMap fan-out, the cached no-limit path vs the take() limit
  path, S3 output location, error propagation, rate-limiter shutdown
- Wire-type fidelity end to end: an item produced by botocore's *real* DynamoDB
  Scan response parser is re-serialized byte-for-byte back to what came off the
  wire (the point of issues #94 / #184)
"""

import base64
import json
from unittest.mock import MagicMock, call, patch

import pytest

from python_modules import scanfind as sf_module

# The source uses `from python_modules.shared.errors import *` which, under
# our Mock-based conftest, binds nothing (star-import from Mock is empty).
# Inject get_error_message so it's available when tested code paths call it.
sf_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


def _make_rl_worker(session):
    """A RateLimiterWorker stand-in handing out `session` and recording shutdown."""
    worker = MagicMock()
    worker.get_session = MagicMock(return_value=session)
    return worker


def _client_session(scan_returns):
    """A session whose .client('dynamodb') scans return `scan_returns` in order.

    `scan_returns` may be a single response dict or a list of pages.
    """
    session = MagicMock()
    client = MagicMock()
    if isinstance(scan_returns, list):
        client.scan = MagicMock(side_effect=scan_returns)
    else:
        client.scan = MagicMock(return_value=scan_returns)
    session.client = MagicMock(return_value=client)
    return session, client


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by scanfind with fresh mocks."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sf_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sf_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sf_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sf_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sf_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def lines_rdd():
    """The RDD of JSON lines that flatMap produces.

    persist() returns itself so the verb can rebind it; count()/take() default
    to an empty result and are overridden by tests that care.
    """
    rdd = MagicMock()
    rdd.persist = MagicMock(return_value=rdd)
    rdd.count = MagicMock(return_value=0)
    rdd.take = MagicMock(return_value=[])
    return rdd


@pytest.fixture
def segment_rdd(lines_rdd):
    """The parallelized segment-index RDD; flatMap yields the lines RDD."""
    rdd = MagicMock()
    rdd.flatMap = MagicMock(return_value=lines_rdd)
    return rdd


@pytest.fixture
def spark_context(segment_rdd):
    """Mock SparkContext recording accumulator() and parallelize() calls."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    sc.parallelize = MagicMock(return_value=segment_rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'index': None,
        'filter_expression': None,
        'expression_values': None,
        'expression_names': None,
        's3-bucket-name': 'out-bucket',
        'JOB_RUN_ID': 'jr-001',
    }


@pytest.fixture(autouse=True)
def stub_boto3_session(monkeypatch):
    """print_dynamodb_table_info asks boto3 for the region; keep it offline."""
    monkeypatch.setattr(sf_module.boto3, 'Session',
                        MagicMock(return_value=MagicMock(region_name='us-east-1')))


# --- ListAccumulator --------------------------------------------------------


class TestListAccumulator:
    """Custom AccumulatorParam for collecting per-worker errors into a list."""

    def test_zero_returns_empty_list(self):
        acc = sf_module.ListAccumulator()
        assert acc.zero(['anything']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_first_list(self):
        acc = sf_module.ListAccumulator()
        a = ['err1']
        result = acc.addInPlace(a, ['err2', 'err3'])
        assert a == ['err1', 'err2', 'err3'], "first arg mutated in place"
        assert result is a, "returns same list object"

    def test_addInPlace_empty_right(self):
        acc = sf_module.ListAccumulator()
        assert acc.addInPlace(['x'], []) == ['x']

    def test_addInPlace_empty_left(self):
        acc = sf_module.ListAccumulator()
        assert acc.addInPlace([], ['y']) == ['y']


# --- Module constants -------------------------------------------------------


class TestModuleConstants:
    def test_top_n_matches_sibling_verbs(self):
        """The preview cap is 10, the value find / sql / diff use."""
        assert sf_module.TOP_N == 10

    def test_default_segments_matches_scancount(self):
        assert sf_module.DEFAULT_SEGMENTS == 200


# --- print_dynamodb_table_info ----------------------------------------------


class TestPrintDynamodbTableInfo:
    def test_calls_helpers_with_table_and_index(self, shared_table_info_mocks):
        sf_module.print_dynamodb_table_info('tbl', 'idx')

        shared_table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('tbl', 'idx')
        shared_table_info_mocks.get_and_print_table_scan_cost.assert_called_once()

    def test_passes_region_from_session_to_scan_cost(self, shared_table_info_mocks, monkeypatch):
        monkeypatch.setattr(sf_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='ap-southeast-2')))

        sf_module.print_dynamodb_table_info('t', None)

        cost_args = shared_table_info_mocks.get_and_print_table_scan_cost.call_args
        assert cost_args.args[1] == 'ap-southeast-2', "region from session passed as second arg"

    def test_index_defaults_to_none(self, shared_table_info_mocks):
        sf_module.print_dynamodb_table_info('tbl')

        call_args = shared_table_info_mocks.get_and_print_dynamodb_table_info.call_args
        assert call_args.args == ('tbl',) or call_args == call('tbl', None)


# --- _wire_json_default -----------------------------------------------------


class TestWireJsonDefault:
    """Binary attribute values arrive from botocore as decoded bytes; base64 is
    their wire form, so they must be re-encoded, not stringified."""

    def test_bytes_become_base64_string(self):
        assert sf_module._wire_json_default(b'\x00\x01') == 'AAE='

    def test_bytearray_becomes_base64_string(self):
        assert sf_module._wire_json_default(bytearray(b'\x00\x01')) == 'AAE='

    def test_memoryview_becomes_base64_string(self):
        assert sf_module._wire_json_default(memoryview(b'\x00\x01')) == 'AAE='

    def test_base64_round_trips_to_original_bytes(self):
        """The encoding is lossless: a loader can recover the exact bytes."""
        raw = bytes(range(256))
        encoded = sf_module._wire_json_default(raw)
        assert base64.b64decode(encoded) == raw

    def test_returns_str_not_bytes(self):
        """b64encode gives bytes; a str is required or json.dumps recurses."""
        assert isinstance(sf_module._wire_json_default(b'x'), str)

    def test_unexpected_type_raises_type_error(self):
        """Silently coercing an unknown type would corrupt byte-faithful output."""
        with pytest.raises(TypeError, match="Cannot serialize"):
            sf_module._wire_json_default(object())

    def test_decimal_is_not_quietly_accepted(self):
        """A Decimal here means something deserialized the item — a bug, not a
        value to paper over (the whole verb exists to avoid that path)."""
        from decimal import Decimal
        with pytest.raises(TypeError, match="Cannot serialize"):
            sf_module._wire_json_default(Decimal('5'))


# --- _to_wire_json ----------------------------------------------------------


class TestToWireJson:
    """Serialization must not reinterpret anything the client returned."""

    def test_compact_separators_no_whitespace(self):
        assert sf_module._to_wire_json({'pk': {'S': 'a'}}) == '{"pk":{"S":"a"}}'

    def test_number_stays_a_string_typed_n(self):
        """A DynamoDB number is {"N": "<string>"} — not a JSON number, and not
        a Decimal. Losing this is exactly the fidelity bug scanfind fixes."""
        assert sf_module._to_wire_json({'n': {'N': '5'}}) == '{"n":{"N":"5"}}'

    def test_high_precision_number_is_not_rounded(self):
        """38 digits of precision survive because the value is never parsed as
        a float."""
        big = '1.2345678901234567890123456789012345678'
        assert sf_module._to_wire_json({'n': {'N': big}}) == f'{{"n":{{"N":"{big}"}}}}'

    def test_string_set_keeps_ss_key_and_list_payload(self):
        assert sf_module._to_wire_json({'ss': {'SS': ['a', 'b']}}) == '{"ss":{"SS":["a","b"]}}'

    def test_number_set_keeps_ns_key(self):
        assert sf_module._to_wire_json({'ns': {'NS': ['1', '2']}}) == '{"ns":{"NS":["1","2"]}}'

    def test_binary_set_re_encodes_each_member(self):
        out = sf_module._to_wire_json({'bs': {'BS': [b'\x01', b'\x02']}})
        assert out == '{"bs":{"BS":["AQ==","Ag=="]}}'

    def test_top_level_null_is_emitted_not_dropped(self):
        """A NULL attribute is a real attribute; dropping it changes the item."""
        assert sf_module._to_wire_json({'nul': {'NULL': True}}) == '{"nul":{"NULL":true}}'

    def test_bool_false_survives(self):
        assert sf_module._to_wire_json({'b': {'BOOL': False}}) == '{"b":{"BOOL":false}}'

    def test_empty_string_survives(self):
        assert sf_module._to_wire_json({'s': {'S': ''}}) == '{"s":{"S":""}}'

    def test_nested_list_and_map_preserved(self):
        item = {'l': {'L': [{'N': '1'}, {'S': 'x'}]}, 'm': {'M': {'in': {'NULL': True}}}}
        assert sf_module._to_wire_json(item) == \
            '{"l":{"L":[{"N":"1"},{"S":"x"}]},"m":{"M":{"in":{"NULL":true}}}}'

    def test_attribute_order_is_preserved(self):
        """Keys are not sorted, so the line matches the item as returned."""
        assert sf_module._to_wire_json({'z': {'S': '1'}, 'a': {'S': '2'}}) == \
            '{"z":{"S":"1"},"a":{"S":"2"}}'

    def test_non_ascii_string_is_escaped_but_lossless(self):
        out = sf_module._to_wire_json({'s': {'S': 'café'}})
        assert json.loads(out) == {'s': {'S': 'café'}}

    def test_output_is_a_single_line(self):
        """One item per line is the contract; an embedded newline would split
        one item into two records for any downstream reader."""
        item = {'a': {'S': 'x'}, 'b': {'M': {'c': {'L': [{'S': 'y'}]}}}}
        assert '\n' not in sf_module._to_wire_json(item)

    def test_embedded_newline_in_a_value_is_escaped(self):
        out = sf_module._to_wire_json({'s': {'S': 'a\nb'}})
        assert '\n' not in out, "the newline is escaped, not literal"
        assert json.loads(out) == {'s': {'S': 'a\nb'}}


# --- Wire-type fidelity through botocore's real parser ----------------------


class TestWireFidelityThroughRealParser:
    """The strongest fidelity check available offline: canned DynamoDB JSON is
    run through botocore's *real* Scan response parser (the same code path the
    low-level client uses on a Glue worker), and the item it produces is
    re-serialized by scanfind. Output must be byte-for-byte identical to the
    bytes that went in.

    Feeding hand-written "already wire-shaped" dicts to the serializer would
    pass even if the client returned something else entirely; this test cannot,
    because the input is the wire and the parser in between is real.
    """

    WIRE_ITEM = {
        "pk": {"S": "item-1"},
        "num": {"N": "5"},
        "big_num": {"N": "1.2345678901234567890123456789012345678"},
        "neg": {"N": "-0.5"},
        "bin": {"B": "AAE="},
        "str_set": {"SS": ["a", "b"]},
        "num_set": {"NS": ["1", "2.5"]},
        "bin_set": {"BS": ["AQ==", "Ag=="]},
        "flag": {"BOOL": False},
        "nothing": {"NULL": True},
        "list": {"L": [{"N": "1"}, {"S": "x"}, {"NULL": True}, {"B": "AAE="}]},
        "map": {"M": {"inner_num": {"N": "7"}, "inner_set": {"SS": ["z"]}}},
        "empty_str": {"S": ""},
    }

    @staticmethod
    def _parse_scan_response(items):
        """Run a canned Scan response body through botocore's DynamoDB parser.

        The service model comes straight from botocore rather than from a boto3
        client, so no credentials, region, or network are involved — and it is
        unaffected by the mocked boto3.Session the other tests install.
        """
        import botocore.session
        from botocore.parsers import create_parser

        service_model = botocore.session.get_session().get_service_model('dynamodb')
        operation_model = service_model.operation_model('Scan')
        parser = create_parser(service_model.metadata['protocol'])
        body = json.dumps({"Items": items, "Count": len(items),
                           "ScannedCount": len(items)}).encode('utf-8')
        return parser.parse(
            {"status_code": 200, "headers": {}, "body": body},
            operation_model.output_shape)

    def test_parser_really_decodes_binary_to_bytes(self):
        """Guards the premise of _wire_json_default: botocore hands back decoded
        bytes for B / BS, which plain json.dumps cannot serialize at all."""
        item = self._parse_scan_response([self.WIRE_ITEM])['Items'][0]

        assert item['bin']['B'] == b'\x00\x01'
        assert item['bin_set']['BS'] == [b'\x01', b'\x02']
        with pytest.raises(TypeError):
            json.dumps(item)

    def test_every_wire_type_round_trips_byte_for_byte(self):
        item = self._parse_scan_response([self.WIRE_ITEM])['Items'][0]

        line = sf_module._to_wire_json(item)

        assert line == json.dumps(self.WIRE_ITEM, separators=(',', ':')), \
            "serialized item must equal the DynamoDB JSON that came off the wire"

    def test_numbers_are_never_deserialized_to_python_numerics(self):
        """The resource layer would give Decimals here; the low-level client
        gives strings, and they must stay strings."""
        item = self._parse_scan_response([self.WIRE_ITEM])['Items'][0]

        assert item['num']['N'] == '5' and isinstance(item['num']['N'], str)
        reparsed = json.loads(sf_module._to_wire_json(item))
        assert reparsed['big_num']['N'] == self.WIRE_ITEM['big_num']['N']
        assert reparsed['neg']['N'] == '-0.5'

    def test_sets_stay_typed_sets_not_plain_lists(self):
        item = self._parse_scan_response([self.WIRE_ITEM])['Items'][0]

        reparsed = json.loads(sf_module._to_wire_json(item))
        assert reparsed['str_set'] == {'SS': ['a', 'b']}
        assert reparsed['num_set'] == {'NS': ['1', '2.5']}
        assert reparsed['bin_set'] == {'BS': ['AQ==', 'Ag==']}

    def test_null_attribute_survives_the_round_trip(self):
        item = self._parse_scan_response([self.WIRE_ITEM])['Items'][0]

        reparsed = json.loads(sf_module._to_wire_json(item))
        assert reparsed['nothing'] == {'NULL': True}, "top-level NULL kept, not dropped"
        assert reparsed['list']['L'][2] == {'NULL': True}, "NULL inside a list kept too"

    def test_scan_segment_emits_wire_faithful_lines(self, monkeypatch):
        """Same assertion, but through the worker: what _scan_segment yields for
        a real parsed response is the wire form."""
        parsed = self._parse_scan_response([self.WIRE_ITEM])
        session, _ = _client_session({'Items': parsed['Items']})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment(
            {}, 'tbl', None, None, None, None, 0, 1, None,
            MagicMock(), MagicMock()))

        assert lines == [json.dumps(self.WIRE_ITEM, separators=(',', ':'))]


# --- _parse_limit -----------------------------------------------------------


class TestParseLimit:
    def test_none_passes_through(self):
        assert sf_module._parse_limit(None) is None

    def test_string_digits_become_int(self):
        """Glue job arguments arrive as strings."""
        assert sf_module._parse_limit('25') == 25

    def test_int_passes_through(self):
        assert sf_module._parse_limit(7) == 7

    def test_non_integer_raises_bulk_executor_error(self):
        with pytest.raises(sf_module.BulkExecutorError, match="not an integer"):
            sf_module._parse_limit('abc')

    def test_zero_raises_bulk_executor_error(self):
        with pytest.raises(sf_module.BulkExecutorError, match="positive integer"):
            sf_module._parse_limit('0')

    def test_negative_raises_bulk_executor_error(self):
        with pytest.raises(sf_module.BulkExecutorError, match="positive integer"):
            sf_module._parse_limit('-3')


# --- _write_json_lines ------------------------------------------------------


class TestWriteJsonLines:
    def test_writes_with_save_as_text_file(self):
        rdd = MagicMock()
        sf_module._write_json_lines(rdd, 's3://b/output/jr-1')

        rdd.saveAsTextFile.assert_called_once_with('s3://b/output/jr-1')

    def test_does_not_route_items_through_a_dataframe(self):
        """Regression guard: find's spark.read.json(rdd) + df.write.json() route
        infers one schema across all items and would mangle the wire types, so
        no DataFrame may appear on this path."""
        rdd = MagicMock()
        sf_module._write_json_lines(rdd, 's3://b/output/jr-1')

        rdd.toJSON.assert_not_called()
        rdd.toDF.assert_not_called()
        assert not rdd.write.method_calls, "no DataFrame writer used"


# --- _print_preview --------------------------------------------------------


class TestPrintPreview:
    def test_prints_all_when_at_or_below_cap(self, capsys):
        sf_module._print_preview(['{"a":1}', '{"b":2}'], 2)

        out = capsys.readouterr().out
        assert '2 matching items:' in out
        assert '{"a":1}' in out and '{"b":2}' in out
        assert 'more not printed' not in out

    def test_truncation_is_announced_when_over_cap(self, capsys):
        preview = [f'{{"i":{i}}}' for i in range(sf_module.TOP_N)]
        sf_module._print_preview(preview, 250)

        out = capsys.readouterr().out
        assert f'First {sf_module.TOP_N} matching items:' in out
        assert f'...and {250 - sf_module.TOP_N} more not printed' in out

    def test_zero_items_says_zero(self, capsys):
        sf_module._print_preview([], 0)

        assert '0 matching items:' in capsys.readouterr().out

    def test_console_output_does_not_grow_with_result_size(self, capsys):
        """The preview is bounded whatever the total: only what it was handed is
        printed, so console volume never scales with the table."""
        sf_module._print_preview(['{"a":1}'], 1_000_000)

        printed_items = [l for l in capsys.readouterr().out.splitlines() if l.startswith('{')]
        assert len(printed_items) == 1


# --- _build_scan_kwargs -----------------------------------------------------


class TestBuildScanKwargs:
    def _kwargs(self, **overrides):
        params = dict(table_name='tbl', index_name=None, filter_expression=None,
                      expression_values=None, expression_names=None, segment=3,
                      total_segments=10, limit=None)
        params.update(overrides)
        return sf_module._build_scan_kwargs(**params)

    def test_table_and_segment_always_present(self):
        """The low-level client needs TableName explicitly (unlike the Table
        resource scancount uses)."""
        kwargs = self._kwargs(table_name='my-tbl')
        assert kwargs['TableName'] == 'my-tbl'
        assert kwargs['Segment'] == 3
        assert kwargs['TotalSegments'] == 10

    def test_select_all_attributes_on_base_table(self):
        assert self._kwargs()['Select'] == 'ALL_ATTRIBUTES'

    def test_index_scan_omits_select(self):
        """Select=ALL_ATTRIBUTES is a ValidationException on an index that does
        not project ALL; omitting it defaults to ALL_PROJECTED_ATTRIBUTES, which
        is valid for every projection type."""
        kwargs = self._kwargs(index_name='gsi-1')
        assert kwargs['IndexName'] == 'gsi-1'
        assert 'Select' not in kwargs

    def test_index_name_excluded_when_none(self):
        assert 'IndexName' not in self._kwargs(index_name=None)

    def test_filter_expression_included_when_truthy(self):
        assert self._kwargs(filter_expression='#s = :v')['FilterExpression'] == '#s = :v'

    def test_filter_expression_excluded_when_none(self):
        assert 'FilterExpression' not in self._kwargs()

    def test_expression_names_parsed_as_plain_json(self):
        kwargs = self._kwargs(expression_names='{"#s": "status"}')
        assert kwargs['ExpressionAttributeNames'] == {'#s': 'status'}

    def test_expression_names_excluded_when_none(self):
        assert 'ExpressionAttributeNames' not in self._kwargs()

    def test_expression_values_parsed_as_wire_json_without_decimal_coercion(self):
        """The low-level client wants wire-format values, so the JSON is used as
        given — no Decimal conversion (which scancount needs and scanfind must
        not do, since it would rewrite the caller's typed value)."""
        kwargs = self._kwargs(expression_values='{":v": {"N": "3.14"}}')

        assert kwargs['ExpressionAttributeValues'] == {':v': {'N': '3.14'}}
        assert isinstance(kwargs['ExpressionAttributeValues'][':v']['N'], str)

    def test_expression_values_excluded_when_none(self):
        assert 'ExpressionAttributeValues' not in self._kwargs()

    def test_limit_passed_down_when_unfiltered(self):
        """Without a filter, DynamoDB's Limit matches our item cap exactly, so
        pushing it down avoids reading a full 1MB page for a handful of items."""
        assert self._kwargs(limit=5)['Limit'] == 5

    def test_limit_not_passed_down_when_filtering(self):
        """Limit caps items *evaluated*, not returned: combined with a filter it
        would cut the scan short and return fewer matches than exist."""
        kwargs = self._kwargs(limit=5, filter_expression='#s = :v')
        assert 'Limit' not in kwargs

    def test_no_limit_kwarg_when_no_limit(self):
        assert 'Limit' not in self._kwargs(limit=None)


# --- _scan_segment ----------------------------------------------------------


class TestScanSegmentClient:
    """The verb's central requirement: read through the low-level client."""

    def test_uses_low_level_client_not_resource(self, monkeypatch):
        session, _ = _client_session({'Items': []})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1,
                                     None, MagicMock(), MagicMock()))

        assert session.client.call_args.args[0] == 'dynamodb'
        # resource() would auto-deserialize items and destroy the wire types.
        session.resource.assert_not_called()

    def test_config_has_4s_timeouts_and_50_retries(self, monkeypatch):
        session, _ = _client_session({'Items': []})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1,
                                     None, MagicMock(), MagicMock()))

        cfg = session.client.call_args.kwargs['config']
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50

    def test_monitor_options_forwarded_to_rate_limiter_worker(self, monkeypatch):
        session, _ = _client_session({'Items': []})
        worker_cls = MagicMock(return_value=_make_rl_worker(session))
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', worker_cls)
        shared_config = MagicMock()

        list(sf_module._scan_segment({'target_rate': 500}, 'tbl', None, None, None,
                                     None, 0, 1, None, MagicMock(), shared_config))

        assert worker_cls.call_args.kwargs['target_rate'] == 500
        assert worker_cls.call_args.kwargs['shared_config'] is shared_config


class TestScanSegmentYield:
    def test_yields_one_json_line_per_item(self, monkeypatch):
        session, _ = _client_session({'Items': [{'pk': {'S': 'a'}}, {'pk': {'S': 'b'}}]})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, None, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}']

    def test_no_items_yields_nothing(self, monkeypatch):
        session, _ = _client_session({})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        assert list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                            0, 1, None, MagicMock(), MagicMock())) == []

    def test_is_lazy_so_items_are_not_all_held_at_once(self, monkeypatch):
        """A generator, not a list: Spark writes each item as it is produced, so
        a segment's whole result never sits in executor memory."""
        session, client = _client_session({'Items': [{'pk': {'S': 'a'}}]})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        gen = sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1,
                                      None, MagicMock(), MagicMock())

        client.scan.assert_not_called()  # nothing scanned until the first item is pulled
        next(gen)
        client.scan.assert_called_once()


class TestScanSegmentPagination:
    def test_single_page_stops_without_exclusive_start_key(self, monkeypatch):
        session, client = _client_session({'Items': [{'pk': {'S': 'a'}}]})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1,
                                     None, MagicMock(), MagicMock()))

        assert client.scan.call_count == 1
        assert 'ExclusiveStartKey' not in client.scan.call_args.kwargs

    def test_last_evaluated_key_threads_into_next_request(self, monkeypatch):
        pages = [
            {'Items': [{'pk': {'S': 'a'}}], 'LastEvaluatedKey': {'pk': {'S': 'a'}}},
            {'Items': [{'pk': {'S': 'b'}}], 'LastEvaluatedKey': {'pk': {'S': 'b'}}},
            {'Items': [{'pk': {'S': 'c'}}]},
        ]
        session, client = _client_session(pages)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, None, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}', '{"pk":{"S":"c"}}']
        assert client.scan.call_count == 3
        second, third = client.scan.call_args_list[1], client.scan.call_args_list[2]
        assert second.kwargs['ExclusiveStartKey'] == {'pk': {'S': 'a'}}
        assert third.kwargs['ExclusiveStartKey'] == {'pk': {'S': 'b'}}

    def test_empty_page_with_last_evaluated_key_keeps_going(self, monkeypatch):
        """A filtered scan can return a page with no matches but more to scan."""
        pages = [
            {'Items': [], 'LastEvaluatedKey': {'pk': {'S': 'a'}}},
            {'Items': [{'pk': {'S': 'z'}}]},
        ]
        session, client = _client_session(pages)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', 'idx', '#s = :v', None,
                                             None, 0, 1, None, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"z"}}']
        assert client.scan.call_count == 2


class TestScanSegmentLimit:
    def test_stops_after_limit_items_within_a_page(self, monkeypatch):
        session, client = _client_session({
            'Items': [{'pk': {'S': str(i)}} for i in range(10)],
            'LastEvaluatedKey': {'pk': {'S': '9'}},
        })
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, 3, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"0"}}', '{"pk":{"S":"1"}}', '{"pk":{"S":"2"}}']
        assert client.scan.call_count == 1, "does not page on once the cap is met"

    def test_stops_paginating_once_limit_reached_across_pages(self, monkeypatch):
        pages = [
            {'Items': [{'pk': {'S': 'a'}}], 'LastEvaluatedKey': {'pk': {'S': 'a'}}},
            {'Items': [{'pk': {'S': 'b'}}], 'LastEvaluatedKey': {'pk': {'S': 'b'}}},
            {'Items': [{'pk': {'S': 'c'}}]},
        ]
        session, client = _client_session(pages)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, 2, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}']
        assert client.scan.call_count == 2

    def test_fewer_matches_than_limit_returns_what_exists(self, monkeypatch):
        session, _ = _client_session({'Items': [{'pk': {'S': 'a'}}]})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, 50, MagicMock(), MagicMock()))

        assert lines == ['{"pk":{"S":"a"}}']

    def test_shuts_down_rate_limiter_on_the_early_return(self, monkeypatch):
        session, _ = _client_session({'Items': [{'pk': {'S': 'a'}}, {'pk': {'S': 'b'}}],
                                      'LastEvaluatedKey': {'pk': {'S': 'b'}}})
        worker = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=worker))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1, 1,
                                     MagicMock(), MagicMock()))

        worker.shutdown.assert_called_once()


class TestScanSegmentErrors:
    def test_scan_failure_recorded_on_accumulator(self, monkeypatch):
        session, client = _client_session({'Items': []})
        client.scan = MagicMock(side_effect=Exception('boom'))
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))
        error_accumulator = MagicMock()

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             7, 20, None, error_accumulator, MagicMock()))

        assert lines == []
        recorded = error_accumulator.add.call_args.args[0]
        assert recorded == ['Error in worker 7: boom']

    def test_items_found_before_the_failure_are_still_yielded(self, monkeypatch):
        pages = [
            {'Items': [{'pk': {'S': 'a'}}], 'LastEvaluatedKey': {'pk': {'S': 'a'}}},
            Exception('page 2 exploded'),
        ]
        session, client = _client_session(None)
        client.scan = MagicMock(side_effect=pages)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))
        error_accumulator = MagicMock()

        lines = list(sf_module._scan_segment({}, 'tbl', None, None, None, None,
                                             0, 1, None, error_accumulator, MagicMock()))

        assert lines == ['{"pk":{"S":"a"}}'], "partial results are not thrown away"
        error_accumulator.add.assert_called_once()

    def test_rate_limiter_shut_down_even_on_failure(self, monkeypatch):
        session, client = _client_session(None)
        client.scan = MagicMock(side_effect=Exception('boom'))
        worker = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=worker))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 0, 1,
                                     None, MagicMock(), MagicMock()))

        worker.shutdown.assert_called_once()

    def test_worker_progress_printed_with_emitted_count(self, monkeypatch, capsys):
        session, _ = _client_session({'Items': [{'pk': {'S': 'a'}}, {'pk': {'S': 'b'}}]})
        monkeypatch.setattr(sf_module, 'RateLimiterWorker',
                            MagicMock(return_value=_make_rl_worker(session)))

        list(sf_module._scan_segment({}, 'tbl', None, None, None, None, 4, 50,
                                     None, MagicMock(), MagicMock()))

        assert 'Worker 4/50 emitted 2 items.' in capsys.readouterr().out


# --- run() ------------------------------------------------------------------


class TestRunArgumentWiring:
    def test_shared_config_uses_bucket_and_job_run_id(self, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        config_call = rate_limiter_mocks.config.call_args
        assert config_call.kwargs['bucket'] == 'out-bucket'
        assert config_call.kwargs['job_run_id'] == 'jr-001'

    def test_monitor_options_requested_for_read_mode(self, shared_table_info_mocks,
                                                    rate_limiter_mocks, spark_context, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        kwargs = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args.kwargs
        assert kwargs['modes'] == ['read']
        assert kwargs['format'] == 'monitor'

    def test_table_info_printed_with_index(self, shared_table_info_mocks,
                                           rate_limiter_mocks, spark_context, base_args):
        base_args['index'] = 'gsi-1'
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        shared_table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with(
            'my-table', 'gsi-1')

    def test_default_segment_count_is_200(self, shared_table_info_mocks,
                                          rate_limiter_mocks, spark_context, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        segments_arg, partitions = spark_context.parallelize.call_args.args
        assert segments_arg == list(range(200))
        assert partitions == 200, "one partition per segment"

    def test_segments_argument_overrides_default(self, shared_table_info_mocks,
                                                 rate_limiter_mocks, spark_context, base_args):
        base_args['segments'] = '12'
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        segments_arg, partitions = spark_context.parallelize.call_args.args
        assert segments_arg == list(range(12))
        assert partitions == 12

    def test_error_accumulator_created(self, shared_table_info_mocks, rate_limiter_mocks,
                                       spark_context, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        spark_context.accumulator.assert_called_once()
        assert spark_context.accumulator.call_args.args[0] == []

    def test_rate_limiter_aggregator_shut_down(self, shared_table_info_mocks,
                                               rate_limiter_mocks, spark_context, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        rate_limiter_mocks.aggregator.return_value.shutdown.assert_called_once()

    def test_bad_limit_rejected_before_any_scan(self, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        base_args['limit'] = 'nope'

        with pytest.raises(sf_module.BulkExecutorError):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        spark_context.parallelize.assert_not_called()


class TestRunFanOut:
    """The scan fans out through flatMap so items stream to S3 per partition."""

    def test_flat_map_used_not_collect(self, shared_table_info_mocks, rate_limiter_mocks,
                                      spark_context, segment_rdd, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        segment_rdd.flatMap.assert_called_once()
        segment_rdd.map.assert_not_called()
        # Items must never be gathered on the driver.
        segment_rdd.collect.assert_not_called()

    def test_flat_map_function_scans_the_given_segment(self, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context,
                                                      segment_rdd, base_args, monkeypatch):
        """Invoke the lambda Spark would run, and confirm the segment index and
        table arguments reach _scan_segment."""
        base_args['segments'] = '8'
        base_args['index'] = 'gsi-1'
        base_args['filter_expression'] = '#s = :v'
        base_args['expression_names'] = '{"#s": "status"}'
        base_args['expression_values'] = '{":v": {"S": "new"}}'
        seen = {}

        def fake_scan_segment(monitor_options, table_name, index_name, filter_expression,
                              expression_values, expression_names, segment, total_segments,
                              limit, error_accumulator, shared_config):
            seen.update(locals())
            return iter([])

        monkeypatch.setattr(sf_module, '_scan_segment', fake_scan_segment)
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        list(segment_rdd.flatMap.call_args.args[0](5))

        assert seen['segment'] == 5
        assert seen['total_segments'] == 8
        assert seen['table_name'] == 'my-table'
        assert seen['index_name'] == 'gsi-1'
        assert seen['filter_expression'] == '#s = :v'
        assert seen['expression_names'] == '{"#s": "status"}'
        assert seen['expression_values'] == '{":v": {"S": "new"}}'
        assert seen['monitor_options'] == {'monitor': 'opts'}
        assert seen['limit'] is None


class TestRunWithoutLimit:
    """No --limit: the full result set is cached, counted, and streamed to S3."""

    def test_result_cached_before_multiple_actions(self, shared_table_info_mocks,
                                                  rate_limiter_mocks, spark_context,
                                                  lines_rdd, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Without a cache, the count and the preview would each re-scan the table.
        lines_rdd.persist.assert_called_once()

    def test_writes_all_lines_to_the_standard_output_location(self, shared_table_info_mocks,
                                                             rate_limiter_mocks, spark_context,
                                                             lines_rdd, base_args):
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        lines_rdd.saveAsTextFile.assert_called_once_with('s3://out-bucket/output/jr-001')

    def test_write_is_unconditional_even_with_no_matches(self, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context,
                                                        lines_rdd, base_args):
        lines_rdd.count.return_value = 0
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        lines_rdd.saveAsTextFile.assert_called_once()

    def test_preview_limited_to_top_n(self, shared_table_info_mocks, rate_limiter_mocks,
                                     spark_context, lines_rdd, base_args):
        lines_rdd.count.return_value = 5000
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}']

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        lines_rdd.take.assert_called_once_with(sf_module.TOP_N)

    def test_prints_count_and_s3_pointer(self, shared_table_info_mocks, rate_limiter_mocks,
                                         spark_context, lines_rdd, base_args, capsys):
        lines_rdd.count.return_value = 1234
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}']

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'Wrote 1,234 items in DynamoDB JSON format to s3://out-bucket/output/jr-001/' in out
        assert f'...and {1234 - sf_module.TOP_N} more not printed' in out


class TestRunWithLimit:
    """--limit N is a global cap: each segment stops at N and take() trims
    across segments to exactly min(N, matches)."""

    def test_take_used_for_the_global_trim(self, shared_table_info_mocks, rate_limiter_mocks,
                                           spark_context, lines_rdd, base_args):
        base_args['limit'] = '3'
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}'] * 3

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        lines_rdd.take.assert_called_once_with(3)

    def test_limit_forwarded_to_each_segment_as_its_own_cap(self, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context,
                                                            segment_rdd, base_args, monkeypatch):
        base_args['limit'] = '4'
        seen = {}

        def fake_scan_segment(*args):
            seen['limit'] = args[8]
            return iter([])

        monkeypatch.setattr(sf_module, '_scan_segment', fake_scan_segment)
        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        list(segment_rdd.flatMap.call_args.args[0](0))

        assert seen['limit'] == 4

    def test_trimmed_lines_written_to_s3(self, shared_table_info_mocks, rate_limiter_mocks,
                                         spark_context, lines_rdd, segment_rdd, base_args):
        base_args['limit'] = '2'
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}']

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # The trimmed list is re-parallelized, then written.
        assert spark_context.parallelize.call_args.args[0] == \
            ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}']
        segment_rdd.saveAsTextFile.assert_called_once_with('s3://out-bucket/output/jr-001')
        lines_rdd.saveAsTextFile.assert_not_called()

    def test_uncached_because_a_single_action_produces_everything(self, shared_table_info_mocks,
                                                                  rate_limiter_mocks, spark_context,
                                                                  lines_rdd, base_args):
        base_args['limit'] = '2'
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}', '{"pk":{"S":"b"}}']

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        lines_rdd.persist.assert_not_called()
        lines_rdd.count.assert_not_called()

    def test_count_reported_is_the_trimmed_count(self, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context,
                                                lines_rdd, base_args, capsys):
        base_args['limit'] = '25'
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}'] * 25

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'Wrote 25 items in DynamoDB JSON format' in out
        assert f'...and {25 - sf_module.TOP_N} more not printed' in out

    def test_fewer_matches_than_limit_reports_what_was_found(self, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context,
                                                            lines_rdd, base_args, capsys):
        base_args['limit'] = '100'
        lines_rdd.take.return_value = ['{"pk":{"S":"a"}}']

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '1 matching items:' in out
        assert 'Wrote 1 items in DynamoDB JSON format' in out


class TestRunErrorHandling:
    def test_worker_error_raised_after_the_run(self, shared_table_info_mocks,
                                               rate_limiter_mocks, spark_context, base_args):
        spark_context.accumulator = MagicMock(
            return_value=MagicMock(value=['Error in worker 3: throttled']))

        with pytest.raises(Exception, match='Error in worker 3: throttled'):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_worker_error_message_names_the_partial_output_location(self, shared_table_info_mocks,
                                                                    rate_limiter_mocks,
                                                                    spark_context, base_args):
        """Items scanned before a failure are already in S3, so the error says
        where they went rather than leaving a half-populated prefix unexplained."""
        spark_context.accumulator = MagicMock(value=MagicMock(), return_value=MagicMock(
            value=['Error in worker 3: throttled']))

        with pytest.raises(Exception, match='s3://out-bucket/output/jr-001/'):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_spark_failure_wrapped_with_context(self, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize = MagicMock(side_effect=Exception('cluster gone'))

        with pytest.raises(Exception, match='Error in parallel execution: cluster gone'):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_aggregator_shut_down_even_when_spark_fails(self, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize = MagicMock(side_effect=Exception('cluster gone'))

        with pytest.raises(Exception):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        rate_limiter_mocks.aggregator.return_value.shutdown.assert_called_once()

    def test_nothing_printed_as_success_when_a_worker_failed(self, shared_table_info_mocks,
                                                             rate_limiter_mocks, spark_context,
                                                             base_args, capsys):
        spark_context.accumulator = MagicMock(
            return_value=MagicMock(value=['Error in worker 0: boom']))

        with pytest.raises(Exception):
            sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert 'Wrote' not in capsys.readouterr().out
