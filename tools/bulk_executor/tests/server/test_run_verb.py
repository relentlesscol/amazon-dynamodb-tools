"""Test for issue #97: run verb - execute a python function against every item.

The run verb executes a user-provided Python function against every item
in a table (optionally filtered by a where clause). Unlike update which
modifies items in-place, run is a generic executor with no constraints
on what the function does.
"""

from unittest.mock import MagicMock, call

import pytest


class TestRunVerb:
    """The run verb executes a user function per item."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import run as run_module
            self.module = run_module
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.run does not exist (the 'run' verb module)")

    def test_executor_function_called_per_item(self, monkeypatch):
        """The user's executor function should be called once per item."""
        rl_worker = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(self.module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        table = MagicMock()
        items = [{'pk': 'a', 'data': '1'}, {'pk': 'b', 'data': '2'}]
        table.scan.return_value = {'Items': items, 'LastEvaluatedKey': None}
        session.resource.return_value.Table.return_value = table

        # Mock executor function
        executor = MagicMock()

        self.module._execute_segment(
            table_name='test-table',
            executor_fn=executor,
            segment=0,
            total_segments=1,
            monitor_options={},
            rate_limiter_shared_config=MagicMock(),
            count_accumulator=MagicMock(),
            error_accumulator=MagicMock(),
        )

        assert executor.call_count == 2
        executor.assert_any_call({'pk': 'a', 'data': '1'})
        executor.assert_any_call({'pk': 'b', 'data': '2'})

    def test_counts_processed_items(self, monkeypatch):
        """The verb should report how many items were processed."""
        rl_worker = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(self.module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        table = MagicMock()
        table.scan.return_value = {'Items': [{'pk': str(i)} for i in range(5)], 'LastEvaluatedKey': None}
        session.resource.return_value.Table.return_value = table

        count_acc = MagicMock()

        self.module._execute_segment(
            table_name='test-table',
            executor_fn=lambda item: None,
            segment=0,
            total_segments=1,
            monitor_options={},
            rate_limiter_shared_config=MagicMock(),
            count_accumulator=count_acc,
            error_accumulator=MagicMock(),
        )

        count_acc.add.assert_called_once_with(5)

    def test_run_prints_total_processed_count(self, monkeypatch, capsys):
        """run() output should include the total count of items processed."""
        spark_context = MagicMock()
        count_acc = MagicMock(value=42)
        err_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[count_acc, err_acc])
        spark_context.parallelize.return_value.foreach = MagicMock()

        monkeypatch.setattr(self.module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(self.module, 'get_and_print_table_scan_cost', MagicMock(return_value=0))
        monkeypatch.setattr(self.module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(self.module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(self.module, 'RateLimiterAggregator', MagicMock())

        args = {
            'table': 'test-table',
            'executor': 'my_script.py',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        self.module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert '42' in out
