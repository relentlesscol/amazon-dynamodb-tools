"""Test for issue #96: size-histogram verb to learn item size distribution.

The verb scans a table (with optional filter) and computes a histogram
of item sizes in KB buckets (0-1KB, 1-2KB, etc.). Output is printed
showing the distribution.
"""

from unittest.mock import MagicMock

import pytest


class TestSizeHistogram:
    """size_histogram computes and outputs an item size distribution."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import size_histogram
            self.module = size_histogram
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.size_histogram does not exist")

    def test_computes_histogram_from_items(self):
        """Given item sizes, produces correct bucket counts."""
        # Items of various sizes (in bytes, simulating what DynamoDB returns)
        item_sizes = [100, 500, 900, 1100, 1500, 2500, 3500, 4000]
        # 0-1KB: 100, 500, 900 -> 3 items
        # 1-2KB: 1100, 1500 -> 2 items
        # 2-3KB: 2500 -> 1 item
        # 3-4KB: 3500 -> 1 item
        # 4-5KB: 4000 -> 1 item

        histogram = self.module.compute_histogram(item_sizes)

        assert histogram['0-1KB'] == 3
        assert histogram['1-2KB'] == 2
        assert histogram['2-3KB'] == 1
        assert histogram['3-4KB'] == 1
        assert histogram['4-5KB'] == 1

    def test_empty_table_produces_empty_histogram(self):
        """An empty table should produce an empty or all-zero histogram."""
        histogram = self.module.compute_histogram([])
        # Either empty dict or all values are 0
        total = sum(histogram.values()) if histogram else 0
        assert total == 0

    def test_run_outputs_histogram(self, monkeypatch, capsys):
        """run() should print the histogram to stdout."""
        spark_context = MagicMock()
        # Accumulator returns a histogram-like result
        histogram_acc = MagicMock(value={'0-1KB': 50, '1-2KB': 30, '2-3KB': 20})
        err_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[histogram_acc, err_acc])
        spark_context.parallelize.return_value.foreach = MagicMock()

        monkeypatch.setattr(self.module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(self.module, 'get_and_print_table_scan_cost', MagicMock(return_value=0))
        monkeypatch.setattr(self.module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(self.module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(self.module, 'RateLimiterAggregator', MagicMock())

        args = {
            'table': 'test-table',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        self.module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        # Output should contain bucket labels
        assert '0-1KB' in out or '0-1' in out
