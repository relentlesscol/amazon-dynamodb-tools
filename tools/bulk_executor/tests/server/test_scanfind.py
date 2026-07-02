"""Test for issue #94: scanfind verb - parallel scans with push-down predicate.

Instead of pulling the whole table into memory via the Glue connector,
scanfind uses direct boto3 scan calls with FilterExpression for memory-
efficient filtering. It supports limit and outputs matching items.

This tests the server-side scanfind module's run() function behavior.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestScanFindRun:
    """The scanfind verb performs parallel scans with FilterExpression."""

    @pytest.fixture(autouse=True)
    def import_module(self, monkeypatch):
        """Import the scanfind module (it must exist for the verb to work)."""
        try:
            from python_modules import scanfind as scanfind_module
            self.scanfind = scanfind_module
        except (ImportError, ModuleNotFoundError):
            pytest.fail(
                "python_modules.scanfind does not exist yet — "
                "this is the module that implements the scanfind verb"
            )

    def test_uses_filter_expression_in_scan(self, monkeypatch):
        """scanfind must pass a FilterExpression to DynamoDB scan."""
        rl_worker = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(self.scanfind, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        table = MagicMock()
        table.scan.return_value = {'Items': [{'pk': '1', 'status': 'active'}], 'LastEvaluatedKey': None}
        session.resource.return_value.Table.return_value = table

        # Simulating a worker scanning segment 0
        self.scanfind._scan_segment(
            table_name='my-table',
            filter_expression='#s = :val',
            expression_names={'#s': 'status'},
            expression_values={':val': 'active'},
            segment=0,
            total_segments=10,
            monitor_options={},
            rate_limiter_shared_config=MagicMock(),
            results_accumulator=MagicMock(),
            error_accumulator=MagicMock(),
            limit=None,
        )

        # The scan call must include FilterExpression
        scan_kwargs = table.scan.call_args.kwargs
        assert scan_kwargs.get('FilterExpression') == '#s = :val'
        assert scan_kwargs.get('ExpressionAttributeNames') == {'#s': 'status'}
        assert scan_kwargs.get('ExpressionAttributeValues') == {':val': 'active'}

    def test_respects_limit_parameter(self, monkeypatch):
        """When --limit is specified, scanfind stops after that many matches."""
        rl_worker = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(self.scanfind, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        # Return more items than the limit
        table = MagicMock()
        table.scan.return_value = {
            'Items': [{'pk': str(i)} for i in range(100)],
            'LastEvaluatedKey': None,
        }
        session.resource.return_value.Table.return_value = table

        results_acc = MagicMock()
        self.scanfind._scan_segment(
            table_name='my-table',
            filter_expression=None,
            expression_names=None,
            expression_values=None,
            segment=0,
            total_segments=1,
            monitor_options={},
            rate_limiter_shared_config=MagicMock(),
            results_accumulator=results_acc,
            error_accumulator=MagicMock(),
            limit=5,
        )

        # The accumulator should have received at most 5 items
        added = results_acc.add.call_args.args[0]
        assert len(added) <= 5

    def test_run_prints_matching_items(self, monkeypatch, capsys):
        """run() should print matching items found across all segments."""
        spark_context = MagicMock()
        # Accumulator for results returns some items
        items_acc = MagicMock(value=[{'pk': 'found1'}, {'pk': 'found2'}])
        err_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[items_acc, err_acc])
        spark_context.parallelize.return_value.foreach = MagicMock()

        monkeypatch.setattr(self.scanfind, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(self.scanfind, 'get_and_print_table_scan_cost', MagicMock(return_value=0))
        monkeypatch.setattr(self.scanfind, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(self.scanfind, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(self.scanfind, 'RateLimiterAggregator', MagicMock())

        args = {
            'table': 'test-table',
            'filter_expression': 'begins_with(pk, :prefix)',
            'expression_values': '{":prefix": "abc"}',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        self.scanfind.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        # Should report how many items matched
        assert '2' in out  # count of matching items
