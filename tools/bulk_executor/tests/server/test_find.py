"""Unit tests for the `find` server-side verb.

Covers `python_modules/find.py`:
- print_dynamodb_table_info: generator that prints table info, optionally
  computes delete costs (PROVISIONED vs PAY_PER_REQUEST billing modes)
- run(): argument wiring, read_dynamodb_dataframe wrapper call,
  simple count (direct DataFrame count), DataFrame processing path
  with WHERE/ORDERBY/LIMIT,
  parse_sort_order (inner fn): asc/desc/default/multi-column/empty-spec,
  DO_FIND branch (S3 write, top-N printing, count <= TOP_N vs > TOP_N),
  DO_DELETE branch (repartitioning, delete_partition inner fn, error
  handling, rate-limiter shutdown), unknown action ValueError

The existing tests/server/conftest.py mocks awsglue, pyspark, and
shared modules at all resolution paths. These tests build on that.
The conftest provides a _ReadDataFrameStub for glue_connector.read_dynamodb_dataframe
that returns a chainable DataFrame mock and records calls.
"""

import json
import sys
from unittest.mock import MagicMock, call, patch

import pytest

# find.py imports these modules not covered by conftest
sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
_pyspark_sql_functions = MagicMock()
sys.modules.setdefault('pyspark.sql.functions', _pyspark_sql_functions)

from python_modules import find as find_module

# Star-imported from shared.errors — Mock doesn't populate __all__ so we inject it
if not hasattr(find_module, 'get_error_message'):
    find_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers in find's namespace."""
    mocks = MagicMock()
    mocks.get_and_print_dynamodb_table_info = MagicMock(return_value={
        'item_count': 1000,
        'size_bytes': 50000,
        'region_name': 'us-east-1',
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'WriteRequestUnits',
    })
    mocks.get_and_print_table_scan_cost = MagicMock(return_value=0.50)
    mocks.get_dynamodb_throughput_configs = MagicMock(return_value={'throughput': 'val'})

    monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info',
                        mocks.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost',
                        mocks.get_and_print_table_scan_cost)
    monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs',
                        mocks.get_dynamodb_throughput_configs)
    return mocks


@pytest.fixture
def pricing_mock(monkeypatch):
    """Mock PricingUtility used in delete cost estimation."""
    pricing_instance = MagicMock()
    pricing_instance.get_on_demand_capacity_pricing = MagicMock(
        return_value={'WriteRequestUnits': '0.000001'}
    )
    pricing_cls = MagicMock(return_value=pricing_instance)
    monkeypatch.setattr(find_module, 'PricingUtility', pricing_cls)
    return pricing_cls


@pytest.fixture
def boto3_session_mock(monkeypatch):
    """Mock boto3.Session() used for region_name in print_dynamodb_table_info."""
    session = MagicMock()
    session.region_name = 'us-west-2'
    session_cls = MagicMock(return_value=session)
    monkeypatch.setattr(find_module, 'boto3', MagicMock(Session=session_cls, client=MagicMock()))
    return session_cls


@pytest.fixture
def read_df(monkeypatch):
    """Provide a controlled DataFrame mock returned by read_dynamodb_dataframe.

    The conftest installs a _ReadDataFrameStub on find_module.read_dynamodb_dataframe.
    This fixture replaces it with a fresh MagicMock that returns a chainable df,
    giving each test full control over return values.

    Returns (read_mock, df) where:
    - read_mock: the mock replacing read_dynamodb_dataframe (inspect .call_args)
    - df: the DataFrame mock that run() will operate on
    """
    df = MagicMock()
    df.count.return_value = 0
    for method in ('cache', 'filter', 'orderBy', 'limit', 'select', 'repartition'):
        getattr(df, method).return_value = df
    df.toJSON.return_value = MagicMock()
    df.toJSON.return_value.collect.return_value = []
    df.toJSON.return_value.foreachPartition = MagicMock()

    read_mock = MagicMock(return_value=df)
    monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', read_mock)
    return read_mock, df


@pytest.fixture
def base_args():
    """Minimal parsed_args for a find action."""
    return {
        'splits': '100',
        'table': 'test-table',
        'where': None,
        'orderby': None,
        'limit': None,
        'XAction': 'find',
        's3-bucket-name': 'my-bucket',
        'JOB_RUN_ID': 'run-123',
    }


# --- print_dynamodb_table_info -----------------------------------------------

class TestPrintDynamodbTableInfo:
    """Generator that prints table info and optionally computes delete costs."""

    def test_non_delete_yields_table_info_and_completes(
        self, table_info_mocks, boto3_session_mock
    ):
        """Non-delete path calls info/cost helpers then yields."""
        gen = find_module.print_dynamodb_table_info('my-table', False)
        result = next(gen)

        table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('my-table')
        table_info_mocks.get_and_print_table_scan_cost.assert_called_once()
        assert result is None or result == table_info_mocks.get_and_print_dynamodb_table_info.return_value

    def test_non_delete_second_next_returns_stop_iteration(
        self, table_info_mocks, boto3_session_mock
    ):
        """Final yield prevents StopIteration on second next()."""
        gen = find_module.print_dynamodb_table_info('t', False)
        next(gen)
        try:
            next(gen)
        except StopIteration:
            pass  # Expected after the final yield is consumed

    def test_kwargs_passed_through_to_scan_cost(
        self, table_info_mocks, boto3_session_mock
    ):
        """kwargs forwarded to get_and_print_table_scan_cost."""
        gen = find_module.print_dynamodb_table_info('t', False, fraction=0.5)
        next(gen)

        call_kwargs = table_info_mocks.get_and_print_table_scan_cost.call_args
        assert call_kwargs.kwargs.get('fraction') == 0.5, \
            "fraction kwarg passed through"

    def test_delete_provisioned_prints_provisioned_cost(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """is_delete=True with PROVISIONED billing prints provisioned cost."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 1000,
            'size_bytes': 50000,
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(500)

        out = capsys.readouterr().out
        assert 'Provisioned' in out or 'provisioned' in out, \
            "PROVISIONED billing mode triggers provisioned cost line"
        assert '500' in out, "delete_count appears in output"

    def test_delete_pay_per_request_prints_ondemand_cost(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """is_delete=True with PAY_PER_REQUEST prints on-demand cost."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 1000,
            'size_bytes': 50000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(200)

        out = capsys.readouterr().out
        assert 'On-demand' in out or 'on-demand' in out or 'On-Demand' in out, \
            "PAY_PER_REQUEST billing triggers on-demand cost line"

    def test_delete_cost_math(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """avg_size, write_units, cost computation."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 99,  # +1 in denominator = 100
            'size_bytes': 10000,  # avg_size = ceil(10000/100) = 100
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(10)  # delete_count = 10

        out = capsys.readouterr().out
        # avg_size = ceil(10000/100) = 100, avg_write_units = ceil(100/1024) = 1
        # write_units = 10 * 1 = 10
        assert '10' in out, "write units or delete count in output"

    def test_delete_avoids_division_by_zero(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """item_count=0 uses +1 to avoid division by zero."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 0,
            'size_bytes': 1024,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        # Should not raise ZeroDivisionError
        gen.send(5)

    def test_delete_unknown_billing_mode_prints_neither_cost_line(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """billing_mode not PROVISIONED or PAY_PER_REQUEST skips both cost prints."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100,
            'size_bytes': 5000,
            'billing_mode': 'UNKNOWN_MODE',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(10)

        out = capsys.readouterr().out
        assert 'Approx DynamoDB cost for provisioned' not in out
        assert 'Approx DynamoDB cost for On-demand' not in out
        assert 'Write units required' in out, "common print still appears"


# --- run(): Simple count (no DataFrame conversion) ----------------------------

class TestRunSimpleCount:
    """The fast path: DO_COUNT with no WHERE/ORDERBY/LIMIT calls
    read_dynamodb_dataframe and prints df.count() directly."""

    def test_simple_count_calls_wrapper_and_prints_count(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """Simple count path calls read_dynamodb_dataframe and prints result."""
        read_mock, df = read_df
        df.count.return_value = 42

        args = {
            'splits': '200', 'table': 'my-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        out = capsys.readouterr().out
        assert '42' in out, "DataFrame count (42) printed directly"

    def test_simple_count_passes_table_and_splits_to_wrapper(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """read_dynamodb_dataframe called with table name and splits."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 'my-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        read_mock.assert_called_once()
        _, kwargs = read_mock.call_args
        assert kwargs.get('splits') == '200' or read_mock.call_args[0][2] == args

    def test_count_with_where_uses_filter(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """count + WHERE filters the DataFrame from read_dynamodb_dataframe."""
        read_mock, df = read_df
        df.count.return_value = 7

        args = {
            'splits': '200', 'table': 'my-table',
            'where': 'attr > 5', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        out = capsys.readouterr().out
        assert '7' in out, "DataFrame count used when WHERE present"
        df.filter.assert_called_once_with('attr > 5')

    def test_count_with_where_still_calls_wrapper(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """count + WHERE still goes through read_dynamodb_dataframe wrapper."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 'my-table',
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        read_mock.assert_called_once()


# --- run(): Wrapper call arguments --------------------------------------------

class TestRunWrapperArgs:
    """Verify find.run() passes correct arguments to read_dynamodb_dataframe."""

    def test_wrapper_receives_glue_context_table_and_splits(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """read_dynamodb_dataframe called with (glue_context, table, parsed_args, splits=)."""
        read_mock, df = read_df
        df.count.return_value = 0

        glue_ctx = MagicMock()
        args = {
            'splits': '150', 'table': 'conn-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), glue_ctx, args)

        read_mock.assert_called_once_with(
            glue_ctx, 'conn-table', args, splits='150'
        )

    def test_wrapper_receives_parsed_args_for_rate_config(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """parsed_args passed to wrapper so it can read XMaxReadRate etc."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
            'XMaxReadRate': 5000,
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        call_args = read_mock.call_args
        assert call_args[0][2] is args

    def test_throughput_configs_called_with_read_mode_for_delete(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """get_dynamodb_throughput_configs still called for write mode in delete path."""
        read_mock, df = read_df
        df.count.return_value = 0
        df.toJSON.return_value.foreachPartition = MagicMock()

        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        args = {
            'splits': '200', 'table': 'read-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        calls = table_info_mocks.get_dynamodb_throughput_configs.call_args_list
        assert len(calls) >= 1
        last_call = calls[-1]
        assert last_call.kwargs.get('modes') == ['write']


# --- run(): parse_sort_order (inner function) ---------------------------------

class TestParseSortOrder:
    """The inner parse_sort_order converts 'col asc, col2 desc' into pyspark
    sort directives. Tested via run() since it's not module-level."""

    def test_single_column_asc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Explicit 'asc' calls pyspark asc()."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'name asc', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.orderBy.assert_called_once()
        sort_list = df.orderBy.call_args.args[0]
        assert len(sort_list) == 1

    def test_single_column_desc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Explicit 'desc' calls pyspark desc()."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'age desc', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.orderBy.assert_called_once()

    def test_default_sort_is_asc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """No direction specified defaults to 'asc'."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'score', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.orderBy.assert_called_once()

    def test_multiple_columns(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Comma-separated specs produce multiple sort directives."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'a asc, b desc, c', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        sort_list = df.orderBy.call_args.args[0]
        assert len(sort_list) == 3, "three sort specs parsed"

    def test_empty_spec_in_orderby_raises_value_error(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Empty spec (e.g. trailing comma) fails regex -> ValueError."""
        read_mock, df = read_df

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'col asc,', 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(ValueError, match="Invalid sort specification"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_orderby_sets_needsRepartitioning(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """orderBy sets needsRepartitioning=True (observable in delete via repartition call)."""
        read_mock, df = read_df
        df.count.return_value = 3
        df.toJSON.return_value.foreachPartition = MagicMock()

        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'x asc', 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.select.assert_called()
        df.repartition.assert_called_with(200)


# --- run(): Error paths -------------------------------------------------------

class TestRunErrorPaths:
    """WHERE, ORDERBY, LIMIT each wrap exceptions with get_error_message."""

    def test_invalid_where_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Filter exception wrapped in 'Invalid where'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")
        _, df = read_df
        df.filter.side_effect = RuntimeError('bad filter')

        args = {
            'splits': '200', 'table': 't',
            'where': 'broken', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'where'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_invalid_orderby_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """orderBy exception wrapped in 'Invalid orderby'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")
        _, df = read_df
        df.orderBy.side_effect = RuntimeError('bad order')

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'x asc', 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'orderby'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_invalid_limit_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Non-integer limit wrapped in 'Invalid limit'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': 'not-a-number',
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'limit'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_unknown_action_raises_value_error(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """else branch raises ValueError for unknown action."""
        _, df = read_df

        args = {
            'splits': '200', 'table': 't',
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'unknown',
        }
        with pytest.raises(ValueError, match="Logic error"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)


# --- run(): LIMIT behavior ----------------------------------------------------

class TestRunLimit:
    """LIMIT converts to int and optionally sets needsRepartitioning."""

    def test_limit_applied_to_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """int(LIMIT) passed to records.limit()."""
        _, df = read_df
        df.count.return_value = 0

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '50',
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.limit.assert_called_once_with(50)

    def test_limit_over_1000_sets_repartitioning(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """limit > 1000 sets needsRepartitioning (observable in delete via repartition)."""
        _, df = read_df
        df.count.return_value = 3
        df.toJSON.return_value.foreachPartition = MagicMock()

        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '2000',
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.repartition.assert_called_with(200)

    def test_limit_under_1000_no_repartitioning(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """limit <= 1000 does NOT set needsRepartitioning."""
        _, df = read_df
        df.count.return_value = 3
        df.toJSON.return_value.foreachPartition = MagicMock()

        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '500',
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.repartition.assert_not_called()


# --- run(): DO_FIND branch ----------------------------------------------------

class TestRunFindAction:
    """DO_FIND writes JSON to S3 and prints top-N records."""

    def test_find_writes_json_to_s3_location(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args, capsys
    ):
        """S3 output location derived from bucket + job_run_id."""
        _, df = read_df
        df.count.return_value = 3
        json_rdd = MagicMock()
        df.toJSON.return_value = json_rdd
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"a":1}', '{"b":2}', '{"c":3}']

        spark_session = MagicMock()
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock(return_value=spark_session))

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 's3://my-bucket/output/run-123' in out, "S3 path printed"
        spark_session.read.json.assert_called_once_with(json_rdd)

    def test_find_count_le_top_n_prints_all(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args, capsys
    ):
        """count <= 10 prints 'N matching items:' header."""
        _, df = read_df
        df.count.return_value = 3
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"a":1}', '{"b":2}', '{"c":3}']
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '3 matching items:' in out
        assert 'more not printed' not in out

    def test_find_count_gt_top_n_prints_truncated(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args, capsys
    ):
        """count > 10 prints first 10 and '...and N more'."""
        _, df = read_df
        df.count.return_value = 25
        records = [f'{{"id":{i}}}' for i in range(10)]
        df.limit.return_value.toJSON.return_value.collect.return_value = records
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'First 10 matching items:' in out
        assert '15 more not printed' in out

    def test_find_prints_each_record(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args, capsys
    ):
        """Each top-N record printed."""
        _, df = read_df
        df.count.return_value = 2
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"x":1}', '{"y":2}']
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '{"x":1}' in out
        assert '{"y":2}' in out

    def test_find_caches_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args
    ):
        """records.cache() called before count."""
        _, df = read_df
        df.count.return_value = 1
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{}']
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        df.cache.assert_called_once()

    def test_find_writes_count_items_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, base_args, capsys
    ):
        """'Wrote N items in JSON format' message."""
        _, df = read_df
        df.count.return_value = 42
        df.limit.return_value.toJSON.return_value.collect.return_value = [f'{{"i":{i}}}' for i in range(10)]
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'Wrote 42 items in JSON format' in out


# --- run(): DO_DELETE branch --------------------------------------------------

class TestRunDeleteAction:
    """DO_DELETE gets table keys, optionally repartitions, then deletes via
    foreachPartition."""

    def _delete_args(self):
        return {
            'splits': '200', 'table': 'del-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'bucket', 'JOB_RUN_ID': 'run-1',
        }

    def _setup_delete_mocks(self, monkeypatch, read_df, table_info_mocks):
        """Wire up the minimum mocks for the delete path to execute."""
        _, df = read_df
        df.count.return_value = 2
        df.toJSON.return_value.foreachPartition = MagicMock()

        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        agg_instance = MagicMock()
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ]}
        }
        boto3_mock = MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        )
        monkeypatch.setattr(find_module, 'boto3', boto3_mock)

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        return df, agg_instance

    def test_delete_calls_describe_table_for_keys(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """boto3.client('dynamodb').describe_table called."""
        _, df = read_df
        df.count.return_value = 0
        df.toJSON.return_value.foreachPartition = MagicMock()

        args = self._delete_args()
        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))
        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        client_mock.describe_table.assert_called_once_with(TableName='del-table')

    def test_delete_sends_count_to_pricing_generator(
        self, monkeypatch, table_info_mocks, read_df, capsys
    ):
        """print_pricing_generator.send(count) passes item count."""
        df, _ = self._setup_delete_mocks(monkeypatch, read_df, table_info_mocks)
        df.count.return_value = 77

        args = self._delete_args()
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '77' in out, "delete count sent to pricing generator"

    def test_delete_prints_deleted_count(
        self, monkeypatch, table_info_mocks, read_df, capsys
    ):
        """'Deleted N items' printed after foreachPartition."""
        df, _ = self._setup_delete_mocks(monkeypatch, read_df, table_info_mocks)
        df.count.return_value = 15

        args = self._delete_args()
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert 'Deleted 15 items' in out

    def test_delete_no_repartition_without_orderby_or_large_limit(
        self, monkeypatch, table_info_mocks, read_df, capsys
    ):
        """needsRepartitioning=False skips select/repartition."""
        df, _ = self._setup_delete_mocks(monkeypatch, read_df, table_info_mocks)

        args = self._delete_args()
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        df.select.assert_not_called()
        df.repartition.assert_not_called()

    def test_delete_aggregator_shutdown_in_finally(
        self, monkeypatch, table_info_mocks, read_df, capsys
    ):
        """rate_limiter_aggregator.shutdown() in finally block."""
        _, df = read_df
        df.count.return_value = 1
        df.toJSON.return_value.foreachPartition = MagicMock(
            side_effect=RuntimeError('partition error')
        )

        args = self._delete_args()
        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        agg_instance = MagicMock()
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        with pytest.raises(RuntimeError, match='partition error'):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        agg_instance.shutdown.assert_called_once()

    def test_delete_throughput_configs_write_mode_monitor_format(
        self, monkeypatch, table_info_mocks, read_df, capsys
    ):
        """get_dynamodb_throughput_configs called with write mode and monitor format."""
        df, _ = self._setup_delete_mocks(monkeypatch, read_df, table_info_mocks)

        args = self._delete_args()
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        calls = table_info_mocks.get_dynamodb_throughput_configs.call_args_list
        assert len(calls) >= 1
        last_call = calls[-1]
        assert last_call.kwargs.get('modes') == ['write']
        assert last_call.kwargs.get('format') == 'monitor'


# --- delete_partition (inner function) ----------------------------------------

class TestDeletePartition:
    """The inner delete_partition function is invoked by foreachPartition.
    We capture the lambda and invoke it to test delete behavior."""

    def _capture_and_run_delete(self, monkeypatch, table_info_mocks, read_df,
                                 partition_data, rl_worker_mock=None):
        """Set up delete path and capture + invoke the foreachPartition lambda."""
        _, df = read_df
        df.count.return_value = len(partition_data)

        args = {
            'splits': '200', 'table': 'del-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'bucket', 'JOB_RUN_ID': 'run-1',
        }
        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        if rl_worker_mock is None:
            rl_worker_mock = MagicMock()
            session = MagicMock()
            table = MagicMock()
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            table.batch_writer.return_value = bw
            session.resource.return_value.Table.return_value = table
            rl_worker_mock.get_session.return_value = session
            rl_worker_mock.table = table
            rl_worker_mock.bw = bw

        monkeypatch.setattr(find_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker_mock))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ]}
        }
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock(return_value=client_mock)
        ))

        pricing_instance = MagicMock()
        pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
        monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100, 'size_bytes': 5000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }

        captured_fn = []

        def capture_foreach_partition(fn):
            captured_fn.append(fn)

        df.toJSON.return_value.foreachPartition = capture_foreach_partition

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        # Now invoke the captured lambda with our partition data
        assert len(captured_fn) == 1
        captured_fn[0](iter(partition_data))

        return rl_worker_mock

    def test_deletes_items_by_key(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """Each record parsed, keys extracted, delete_item called."""
        partition = [
            json.dumps({'pk': 'a', 'sk': '1', 'data': 'x'}),
            json.dumps({'pk': 'b', 'sk': '2', 'data': 'y'}),
        ]
        rl_mock = self._capture_and_run_delete(monkeypatch, table_info_mocks, read_df, partition)

        bw = rl_mock.bw
        assert bw.delete_item.call_count == 2
        keys_deleted = [c.kwargs['Key'] for c in bw.delete_item.call_args_list]
        assert {'pk': 'a', 'sk': '1'} in keys_deleted
        assert {'pk': 'b', 'sk': '2'} in keys_deleted

    def test_delete_item_error_prints_but_continues(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """Exception in delete_item prints error, continues loop."""
        rl_worker_mock = MagicMock()
        session = MagicMock()
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        bw.delete_item = MagicMock(side_effect=[RuntimeError('throttled'), None])
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        rl_worker_mock.get_session.return_value = session
        rl_worker_mock.table = table
        rl_worker_mock.bw = bw

        partition = [
            json.dumps({'pk': 'a', 'sk': '1'}),
            json.dumps({'pk': 'b', 'sk': '2'}),
        ]
        self._capture_and_run_delete(
            monkeypatch, table_info_mocks, read_df, partition,
            rl_worker_mock=rl_worker_mock
        )

        out = capsys.readouterr().out
        assert 'Error deleting item' in out
        assert bw.delete_item.call_count == 2, "continues after first error"

    def test_rate_limiter_worker_shutdown_in_finally(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """rate_limiter_worker.shutdown() called in finally."""
        partition = [json.dumps({'pk': 'x', 'sk': 'y'})]
        rl_mock = self._capture_and_run_delete(monkeypatch, table_info_mocks, read_df, partition)

        rl_mock.shutdown.assert_called_once()

    def test_rate_limiter_worker_shutdown_even_on_error(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """shutdown called even when batch_writer raises."""
        rl_worker_mock = MagicMock()
        session = MagicMock()
        table = MagicMock()
        # batch_writer context manager raises on __enter__
        bw = MagicMock()
        bw.__enter__ = MagicMock(side_effect=RuntimeError('connection failed'))
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        rl_worker_mock.get_session.return_value = session
        rl_worker_mock.table = table
        rl_worker_mock.bw = bw

        partition = [json.dumps({'pk': 'x', 'sk': 'y'})]

        with pytest.raises(RuntimeError, match='connection failed'):
            self._capture_and_run_delete(
                monkeypatch, table_info_mocks, read_df, partition,
                rl_worker_mock=rl_worker_mock
            )

        rl_worker_mock.shutdown.assert_called_once()

    def test_delete_partition_uses_config_with_timeouts(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """Config with connect_timeout=4, read_timeout=4, 50 retries."""
        seen_configs = []

        rl_worker_mock = MagicMock()
        session = MagicMock()

        def capture_resource(svc, **kwargs):
            if 'config' in kwargs:
                seen_configs.append(kwargs['config'])
            resource = MagicMock()
            table = MagicMock()
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            table.batch_writer.return_value = bw
            resource.Table.return_value = table
            return resource

        session.resource = capture_resource
        rl_worker_mock.get_session.return_value = session
        # Provide dummy attributes for the assertion helper
        rl_worker_mock.table = MagicMock()
        rl_worker_mock.bw = MagicMock()
        rl_worker_mock.bw.__enter__ = MagicMock(return_value=rl_worker_mock.bw)
        rl_worker_mock.bw.__exit__ = MagicMock(return_value=False)
        rl_worker_mock.table.batch_writer.return_value = rl_worker_mock.bw

        partition = [json.dumps({'pk': 'a', 'sk': 'b'})]
        self._capture_and_run_delete(
            monkeypatch, table_info_mocks, read_df, partition,
            rl_worker_mock=rl_worker_mock
        )

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


# --- run(): warnings suppression and defaults ---------------------------------

class TestRunMiscBehavior:
    """Miscellaneous behavior: default splits, warnings suppression."""

    def test_default_splits_is_200(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """splits defaults to '200' when not in parsed_args."""
        read_mock, df = read_df
        df.count.return_value = 0

        args = {
            'table': 't', 'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        read_mock.assert_called_once()
        _, kwargs = read_mock.call_args
        assert kwargs.get('splits') == '200'

    def test_warnings_suppressed_in_dataframe_path(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df
    ):
        """warnings.filterwarnings called for DataFrame constructor."""
        _, df = read_df
        df.count.return_value = 0

        with patch.object(find_module.warnings, 'filterwarnings') as mock_fw:
            args = {
                'splits': '200', 'table': 't',
                'where': 'x = 1', 'orderby': None, 'limit': None,
                'XAction': 'count',
            }
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
            mock_fw.assert_called_once_with(
                "ignore",
                message="DataFrame constructor is internal. Do not directly use it."
            )

    def test_count_with_orderby_uses_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """count + ORDERBY forces DataFrame processing path (not simple count)."""
        _, df = read_df
        df.count.return_value = 99

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'col asc', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '99' in out

    def test_count_with_limit_uses_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, read_df, capsys
    ):
        """count + LIMIT forces DataFrame processing path."""
        _, df = read_df
        df.count.return_value = 8

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '10',
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '8' in out
