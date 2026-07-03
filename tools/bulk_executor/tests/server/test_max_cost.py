"""Failing tests for issue #175: Add a max cost parameter.

The system should support an --XMaxEstimatedCostAllowed parameter that halts
execution if the estimated cost exceeds the specified dollar value.

This test validates the server-side behavior: after cost estimation, if the
total estimated cost > XMaxEstimatedCostAllowed, the job should abort with
a clear message rather than proceeding.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import find as find_module
from python_modules.shared.table_info import get_and_print_table_scan_cost


class TestMaxCostParameter:
    """When XMaxEstimatedCostAllowed is set, the job should halt if costs exceed it."""

    def test_find_aborts_when_cost_exceeds_max(self, monkeypatch, capsys):
        """find command should abort when estimated cost > XMaxEstimatedCostAllowed."""
        # Mock table info to return a high cost estimate
        mock_table_info = MagicMock(return_value={
            'table_name': 'expensive-table',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 10_000_000_000,
            'size_bytes': 1_000_000_000_000,  # 1 TB
            'key_schema': {'pk': {'name': 'id', 'type': 'S'}}
        })
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', mock_table_info)

        # Mock the scan cost to return $50
        mock_scan_cost = MagicMock(return_value=50.0)
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', mock_scan_cost)
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        # Mock Glue connector
        mock_read = MagicMock()
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)

        parsed_args = {
            'table': 'expensive-table',
            'splits': '200',
            'XAction': 'find',
            'where': None,
            'orderby': None,
            'limit': None,
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'job-123',
            'XMaxEstimatedCostAllowed': '10.00',  # $10 max allowed
        }

        # The job should abort because estimated cost ($50) > max allowed ($10)
        with pytest.raises((SystemExit, Exception)) as exc_info:
            find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out
        # Should mention cost or max or abort
        assert any(word in (str(exc_info.value) + output).lower()
                   for word in ['cost', 'exceed', 'max', 'abort', 'halt']), \
            f"Expected cost-abort message, got exc={exc_info.value}, output={output}"
        # The actual data read should NOT have been called
        mock_read.assert_not_called()

    def test_find_proceeds_when_cost_within_max(self, monkeypatch, capsys):
        """find command should proceed AND log a cost-check-passed message
        when estimated cost <= XMaxEstimatedCostAllowed."""
        mock_table_info = MagicMock(return_value={
            'table_name': 'small-table',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 1000,
            'size_bytes': 100_000,
            'key_schema': {'pk': {'name': 'id', 'type': 'S'}}
        })
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', mock_table_info)

        # Cost estimate: $0.01
        mock_scan_cost = MagicMock(return_value=0.01)
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', mock_scan_cost)
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        # Mock Glue connector
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        mock_read = MagicMock(return_value=mock_df)
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)

        parsed_args = {
            'table': 'small-table',
            'splits': '200',
            'XAction': 'count',
            'where': None,
            'orderby': None,
            'limit': None,
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'job-123',
            'XMaxEstimatedCostAllowed': '10.00',  # $10 max, cost is $0.01
        }

        # Should NOT raise — cost is within bounds
        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)
        # Data read should have been called
        mock_read.assert_called()

        # The output should confirm the cost check was performed and passed
        output = capsys.readouterr().out
        assert 'cost' in output.lower() and ('within' in output.lower() or 'below' in output.lower() or 'proceed' in output.lower()), \
            f"Expected cost-check-passed message when within budget, got: {output}"

    def test_no_max_cost_param_means_no_check(self, monkeypatch, capsys):
        """When XMaxEstimatedCostAllowed is not set, no cost check message appears."""
        mock_table_info = MagicMock(return_value={
            'table_name': 'expensive-table',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 10_000_000_000,
            'size_bytes': 1_000_000_000_000,
            'key_schema': {'pk': {'name': 'id', 'type': 'S'}}
        })
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', mock_table_info)
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=50.0))
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_read = MagicMock(return_value=mock_df)
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)

        parsed_args = {
            'table': 'expensive-table',
            'splits': '200',
            'XAction': 'count',
            'where': None,
            'orderby': None,
            'limit': None,
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'job-123',
            # No XMaxEstimatedCostAllowed — should proceed regardless of cost
        }

        # Should NOT raise even though cost is high
        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)
        mock_read.assert_called()

        # And there should be NO cost-check message in output since the param isn't set
        output = capsys.readouterr().out
        assert 'maxestimatedcost' not in output.lower() and 'cost limit' not in output.lower(), \
            f"Should not mention cost limit when XMaxEstimatedCostAllowed is not set, got: {output}"
