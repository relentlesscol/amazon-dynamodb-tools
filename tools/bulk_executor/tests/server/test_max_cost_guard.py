"""Failing tests for issue #175: Add a max cost parameter.

When --XMaxEstimatedCostAllowed is set, the server-side verb should
estimate cost BEFORE performing the operation and halt with an error
if the estimated cost exceeds the threshold.

The behavior tested here:
- copy.run() estimates cost, compares to XMaxEstimatedCostAllowed, and
  raises BulkExecutorError if estimated cost > threshold.
- The guard applies to find (delete path), copy, and load_export verbs.
- When the cost is within budget, execution proceeds normally.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# copy.py uses AccumulatorParam as base class, and needs pyspark
sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
sys.modules.setdefault('pyspark.sql.functions', MagicMock())


from python_modules import copy as copy_module


@pytest.fixture
def mock_table_info(monkeypatch):
    """Mock table_info helpers to return predictable costs."""
    monkeypatch.setattr(copy_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
        'item_count': 1000,
        'size_bytes': 50000,
        'region_name': 'us-east-1',
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'std_wcu_pricing',
        'read_pricing_category': 'std_rcu_pricing',
    }))
    # Scan cost = $5.00
    monkeypatch.setattr(copy_module, 'get_and_print_table_scan_cost', MagicMock(return_value=5.00))
    # Write cost = $10.00, total = $15.00
    monkeypatch.setattr(copy_module, 'get_and_print_table_copy_write_cost', MagicMock(return_value=10.00))


@pytest.fixture
def mock_rate_limiter(monkeypatch):
    """Mock rate limiter modules so run() doesn't hit real S3."""
    monkeypatch.setattr(copy_module, 'RateLimiterSharedConfig', MagicMock())
    monkeypatch.setattr(copy_module, 'RateLimiterAggregator', MagicMock())
    monkeypatch.setattr(copy_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))


class TestMaxCostGuardCopy:
    """copy.run() should halt when estimated cost exceeds XMaxEstimatedCostAllowed."""

    def test_halts_when_cost_exceeds_threshold(self, mock_table_info, mock_rate_limiter):
        """When estimated cost ($15) > max allowed ($10), run() raises."""
        parsed_args = {
            'source': 'src-table',
            'target': 'dst-table',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'XMaxEstimatedCostAllowed': '10',  # $10 max, but estimated is $15
        }
        job = MagicMock()
        spark_context = MagicMock()
        # Accumulator mock
        spark_context.accumulator = MagicMock(return_value=MagicMock(value=0))
        glue_context = MagicMock()

        with pytest.raises(Exception, match="[Cc]ost|[Ee]xceed|[Bb]udget|[Hh]alt"):
            copy_module.run(job, spark_context, glue_context, parsed_args)

    def test_proceeds_when_cost_within_threshold(self, mock_table_info, mock_rate_limiter):
        """When estimated cost ($15) <= max allowed ($20), run() continues normally."""
        parsed_args = {
            'source': 'src-table',
            'target': 'dst-table',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'XMaxEstimatedCostAllowed': '20',  # $20 max, estimated is $15
        }
        job = MagicMock()
        spark_context = MagicMock()
        spark_context.accumulator = MagicMock(return_value=MagicMock(value=0))
        spark_context.parallelize = MagicMock(return_value=MagicMock())
        glue_context = MagicMock()

        # Should NOT raise — execution proceeds to the parallelize step
        copy_module.run(job, spark_context, glue_context, parsed_args)

        # Verify that the actual copy operation was attempted
        spark_context.parallelize.assert_called()

    def test_no_threshold_set_proceeds_normally(self, mock_table_info, mock_rate_limiter):
        """When XMaxEstimatedCostAllowed is not set, run() proceeds without guard."""
        parsed_args = {
            'source': 'src-table',
            'target': 'dst-table',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            # No XMaxEstimatedCostAllowed
        }
        job = MagicMock()
        spark_context = MagicMock()
        spark_context.accumulator = MagicMock(return_value=MagicMock(value=0))
        spark_context.parallelize = MagicMock(return_value=MagicMock())
        glue_context = MagicMock()

        # Should NOT raise
        copy_module.run(job, spark_context, glue_context, parsed_args)
        spark_context.parallelize.assert_called()
