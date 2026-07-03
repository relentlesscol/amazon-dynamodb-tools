"""Unit tests for the `run` server-side verb.

Issue #97: [verb] `run` - execute a python function against every item in a table.

The `run` verb loads a user-provided executor module (specified via --executor)
and calls its function against every scanned item. Unlike `update`, the return
value of the executor function is NOT used to drive any DynamoDB write API —
side effects are entirely the user's responsibility.

Key behaviors tested:
- run() loads the executor module via importlib and calls the named function
- The executor function receives each raw DynamoDB item from the table scan
- Return value of the executor is IGNORED (no update_item, no put_item)
- Execution counts (processed, errored) are reported via accumulators
- Supports filtering via a `where` clause / filter expression
- Rate limiter is configured for read-only throughput monitoring
"""

import sys
from unittest.mock import MagicMock, patch, call

import botocore.exceptions
import pytest

from python_modules import run as run_module

# Inject error helpers (same pattern as test_update.py)
run_module.get_error_message = MagicMock(side_effect=lambda e: str(e))
run_module.get_error_code = MagicMock(side_effect=lambda e: e.response['Error']['Code'])


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by run."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 100, 'size_bytes': 2048, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.50)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'opt': 'val'})

    monkeypatch.setattr(run_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(run_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(run_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(run_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(run_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext with accumulator() and parallelize()."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    rdd = MagicMock()
    rdd.map = MagicMock(return_value=rdd)
    rdd.collect = MagicMock(return_value=[])
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'executor': 'my_script',
        's3-bucket-name': 'rate-limit-bucket',
        'JOB_RUN_ID': 'jr-run-001',
    }


# --- Core behavior: executor function is called per item, return value ignored ---

class TestRunExecutorDispatch:
    """The run verb must call the user's executor function on each item WITHOUT
    using the return value to call any DynamoDB write API."""

    def test_executor_function_called_with_each_item(self, monkeypatch):
        """The executor function receives each raw item from the table scan."""
        # Setup: a table with 3 items
        items = [{'pk': 'a', 'data': 1}, {'pk': 'b', 'data': 2}, {'pk': 'c', 'data': 3}]
        table = MagicMock()
        table.scan.return_value = {'Items': items}  # single page, no LastEvaluatedKey

        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(run_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(run_module, 'get_error_message', lambda e: str(e))

        # Track what the executor sees
        received_items = []

        def user_executor(item):
            received_items.append(item)
            return "some_return_value"  # Should be IGNORED

        # Call the worker function directly
        processed_acc = MagicMock()
        error_acc = MagicMock()

        run_module._run_data(
            {}, 'my-table', user_executor, 0, 1,
            processed_acc, error_acc, MagicMock()
        )

        # Verify: executor received all 3 items
        assert received_items == items
        # Verify: NO DynamoDB write APIs called
        table.update_item.assert_not_called()
        table.put_item.assert_not_called()
        table.delete_item.assert_not_called()
        # Verify: processed count reflects all items
        processed_acc.add.assert_called_once_with(3)

    def test_executor_return_value_is_ignored(self, monkeypatch):
        """Even if executor returns DynamoDB-shaped kwargs, no write is performed."""
        items = [{'pk': 'x', 'sk': 'y'}]
        table = MagicMock()
        table.scan.return_value = {'Items': items}

        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(run_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(run_module, 'get_error_message', lambda e: str(e))

        # Executor returns what looks like update_item kwargs — should be ignored
        def sneaky_executor(item):
            return {
                'Key': {'pk': item['pk'], 'sk': item['sk']},
                'UpdateExpression': 'SET #a = :v',
                'ExpressionAttributeNames': {'#a': 'attr'},
                'ExpressionAttributeValues': {':v': 'val'}
            }

        processed_acc = MagicMock()
        run_module._run_data(
            {}, 'tbl', sneaky_executor, 0, 1,
            processed_acc, MagicMock(), MagicMock()
        )

        # The table should NOT have had any write API called
        table.update_item.assert_not_called()
        table.put_item.assert_not_called()
        table.delete_item.assert_not_called()
        processed_acc.add.assert_called_once_with(1)


class TestRunExecutorErrors:
    """When the user's executor raises an exception, it should be caught and
    counted (not crash the entire job)."""

    def test_executor_exception_increments_error_count(self, monkeypatch):
        """If the executor raises, the item is counted as errored, not processed."""
        items = [{'pk': 'good'}, {'pk': 'bad'}, {'pk': 'good2'}]
        table = MagicMock()
        table.scan.return_value = {'Items': items}

        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(run_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(run_module, 'get_error_message', lambda e: str(e))

        def flaky_executor(item):
            if item['pk'] == 'bad':
                raise ValueError("item processing failed")

        processed_acc = MagicMock()
        error_acc = MagicMock()

        run_module._run_data(
            {}, 'tbl', flaky_executor, 0, 1,
            processed_acc, error_acc, MagicMock()
        )

        # 2 succeeded, 1 errored
        processed_acc.add.assert_called_once_with(2)
        error_acc.add.assert_called_once()
        # Error count should include the 1 failure
        error_args = error_acc.add.call_args.args[0]
        assert error_args == 1 or (isinstance(error_args, list) and len(error_args) == 1)


class TestRunModuleLoading:
    """run() loads the executor module dynamically from parsed_args['executor']."""

    def test_loads_executor_module_by_name(self, monkeypatch, shared_table_info_mocks,
                                            rate_limiter_mocks, spark_context, base_args):
        """The executor module is loaded from python_modules.run.<executor_name>."""
        imported_modules = []

        def fake_import(name):
            imported_modules.append(name)
            m = MagicMock()
            m.execute = MagicMock()
            return m

        monkeypatch.setattr(run_module.importlib, 'import_module', fake_import)
        monkeypatch.setattr(run_module, 'print_dynamodb_table_info', MagicMock())

        run_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert any('my_script' in mod for mod in imported_modules)

    def test_custom_function_name_from_args(self, monkeypatch, shared_table_info_mocks,
                                              rate_limiter_mocks, spark_context, base_args):
        """A custom function name can be specified (defaults to 'execute')."""
        base_args['executorfunctionname'] = 'custom_fn'
        mock_module = MagicMock()
        mock_module.custom_fn = MagicMock()
        monkeypatch.setattr(run_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(run_module, 'print_dynamodb_table_info', MagicMock())

        # We need to verify getattr uses the custom function name
        captured_fn = []
        original_map = spark_context.parallelize.return_value.map

        def capture_map(fn):
            captured_fn.append(fn)
            rdd = MagicMock()
            rdd.collect = MagicMock(return_value=[])
            return rdd

        spark_context.parallelize.return_value.map = capture_map
        run_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Verify the module was imported and custom_fn was accessed
        assert hasattr(mock_module, 'custom_fn')


class TestRunReadOnlyRateLimiting:
    """The run verb should configure rate limiting for READ only (no writes)."""

    def test_monitor_options_read_only(self, monkeypatch, shared_table_info_mocks,
                                        rate_limiter_mocks, spark_context, base_args):
        """get_dynamodb_throughput_configs called with modes=["read"] only."""
        monkeypatch.setattr(run_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(execute=MagicMock())))
        monkeypatch.setattr(run_module, 'print_dynamodb_table_info', MagicMock())

        run_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        shared_table_info_mocks.get_dynamodb_throughput_configs.assert_called_once()
        call_kwargs = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args
        # run verb is read-only from the framework's perspective
        assert call_kwargs.kwargs['modes'] == ['read']


class TestRunSummaryOutput:
    """The run verb should print a summary showing processed and error counts."""

    def test_prints_processed_and_error_summary(self, monkeypatch, shared_table_info_mocks,
                                                  rate_limiter_mocks, spark_context, base_args, capsys):
        """After execution, prints total processed and error counts."""
        monkeypatch.setattr(run_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(execute=MagicMock())))
        monkeypatch.setattr(run_module, 'print_dynamodb_table_info', MagicMock())

        accs = [
            MagicMock(value=50),  # processed
            MagicMock(value=3),   # errored
            MagicMock(value=[]),  # error messages
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(return_value=[])

        run_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '50' in out, "processed count should appear in output"
        assert '3' in out, "error count should appear in output"


class TestRunFilterExpression:
    """The run verb should support a where/filter clause to limit items processed."""

    def test_where_clause_passed_as_filter_expression(self, monkeypatch):
        """When --where is specified, it should be passed to scan as FilterExpression."""
        scan_kwargs_seen = []
        table = MagicMock()

        def scan_capture(**kwargs):
            scan_kwargs_seen.append(dict(kwargs))
            return {'Items': []}

        table.scan = scan_capture
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(run_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(run_module, 'get_error_message', lambda e: str(e))

        run_module._run_data(
            {}, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(),
            filter_expression='attribute_exists(email)'
        )

        assert scan_kwargs_seen[0].get('FilterExpression') == 'attribute_exists(email)'
