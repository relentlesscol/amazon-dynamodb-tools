"""Failing tests for issue #175: Add a max cost parameter (XMaxEstimatedCostAllowed).

When --XMaxEstimatedCostAllowed is specified, the job should:
1. Estimate the cost as normal
2. Compare against the threshold
3. HALT (raise/exit) if estimated cost exceeds the threshold
4. Proceed normally if cost is within budget

This tests the server-side diff.run() behavior — after computing the cost
estimate, it should check the XMaxEstimatedCostAllowed arg and abort if
the estimate is too high.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import diff as diff_module


class TestMaxCostParameter:
    """Jobs should halt if the estimated cost exceeds XMaxEstimatedCostAllowed."""

    def _base_args(self):
        return {
            'splits': '4',
            'sample_fraction': '1.0',
            'table': 'table1',
            'table2': 'table2',
            'format': 'keys',
            's3': None,
            'JOB_RUN_ID': 'job-1',
            's3-bucket-name': 'bucket',
        }

    def _setup_mocks(self, monkeypatch, cost_per_table=5.00):
        """Set up mocks where each table costs `cost_per_table` to scan."""
        monkeypatch.setattr(diff_module, 'print_dynamodb_table_info',
                            MagicMock(return_value=cost_per_table))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(diff_module, 'boto3', MagicMock(
            client=MagicMock(return_value=client_mock)
        ))

        monkeypatch.setattr(diff_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock())
        monkeypatch.setattr(diff_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

    def test_halts_when_cost_exceeds_max_allowed(self, monkeypatch, capsys):
        """If total estimated cost ($10) exceeds XMaxEstimatedCostAllowed ($5),
        the job should halt with an appropriate message instead of proceeding."""
        self._setup_mocks(monkeypatch, cost_per_table=5.00)  # total = $10
        args = self._base_args()
        args['XMaxEstimatedCostAllowed'] = 5.0  # budget is $5

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(4)]

        # The job should halt — either by raising an exception or by
        # NOT calling parallelize/map (short-circuit before execution)
        with pytest.raises((SystemExit, Exception)) as exc_info:
            diff_module.run(MagicMock(), spark_context, MagicMock(), args)

        # Verify the error mentions cost
        error_text = str(exc_info.value).lower()
        assert any(word in error_text for word in ['cost', 'budget', 'exceed', 'max']), \
            f"Expected cost-related error, got: {exc_info.value}"

    def test_proceeds_when_cost_within_budget(self, monkeypatch, capsys):
        """If total estimated cost ($2) is within XMaxEstimatedCostAllowed ($10),
        the job should proceed normally."""
        self._setup_mocks(monkeypatch, cost_per_table=1.00)  # total = $2
        args = self._base_args()
        args['XMaxEstimatedCostAllowed'] = 10.0  # budget is $10

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(4)]

        # Should proceed without raising
        diff_module.run(MagicMock(), spark_context, MagicMock(), args)

        # Verify it actually ran the job (called parallelize)
        spark_context.parallelize.assert_called_once()

    def test_prints_cost_comparison_when_max_set(self, monkeypatch, capsys):
        """When XMaxEstimatedCostAllowed is set, the output should show the
        comparison between estimated cost and the allowed maximum."""
        self._setup_mocks(monkeypatch, cost_per_table=1.00)  # total = $2
        args = self._base_args()
        args['XMaxEstimatedCostAllowed'] = 10.0

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(4)]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out

        # Should mention the max cost allowance
        assert any(word in out.lower() for word in ['max', 'budget', 'allowed', 'limit']), \
            f"Expected cost comparison info in output, got:\n{out}"
