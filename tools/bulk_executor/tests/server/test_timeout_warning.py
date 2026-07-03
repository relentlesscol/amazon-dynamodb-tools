"""Failing tests for issue #144: Dynamically report if XTimeout seems insufficient.

When a job's estimated completion time exceeds the configured XTimeout (default 60
minutes), the system should warn the user early as part of the cost/time estimate
before the job starts running.

The warning should appear when: item_count / max_read_rate > timeout_minutes * 60
(i.e., it would take longer to scan all items than the timeout allows).

This tests the server-side diff.run() and similar verb modules that print cost
estimates — they should also report a timeout insufficiency warning.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import diff as diff_module


class TestTimeoutWarning:
    """The system should warn when XTimeout seems insufficient for the data volume."""

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
            'XTimeout': 60,         # 60 minutes
            'XMaxReadRate': 10000,  # 10,000 items/sec
        }

    def _setup_mocks(self, monkeypatch, item_count=2_000_000_000):
        """Set up mocks for a table with `item_count` items."""
        table_info = {
            'table_name': 'table1',
            'item_count': item_count,
            'size_bytes': item_count * 100,
            'billing_mode': 'PAY_PER_REQUEST',
            'read_pricing_category': 'std_rcu_pricing',
            'write_pricing_category': 'std_wcu_pricing',
            'region_name': 'us-east-1',
        }
        monkeypatch.setattr(diff_module, 'print_dynamodb_table_info', MagicMock(return_value=0.10))
        monkeypatch.setattr(diff_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value=table_info))

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

    def test_warns_when_timeout_insufficient_for_item_count(self, monkeypatch, capsys):
        """With 2 billion items at 10,000/sec rate, it takes ~55 hours.
        A 60-minute timeout is clearly insufficient — the system should warn."""
        self._setup_mocks(monkeypatch, item_count=2_000_000_000)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(4)]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out

        # Should contain a timeout warning
        assert any(word in out.lower() for word in ['timeout', 'insufficient', 'exceed', 'warning']), \
            f"Expected timeout warning in output, got:\n{out}"

    def test_no_warning_when_timeout_sufficient(self, monkeypatch, capsys):
        """With 1000 items at 10,000/sec, it takes <1 second.
        A 60-minute timeout is plenty — no warning needed."""
        self._setup_mocks(monkeypatch, item_count=1000)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(4)]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out

        # Should NOT contain a timeout warning
        timeout_words = ['timeout insufficient', 'timeout exceeded', 'timeout warning']
        assert not any(word in out.lower() for word in timeout_words), \
            f"Unexpected timeout warning in output for small table:\n{out}"
