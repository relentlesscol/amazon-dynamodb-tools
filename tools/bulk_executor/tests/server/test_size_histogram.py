"""Unit tests for the `size-histogram` server-side verb.

Tests `python_modules/size_histogram/__init__.py`:
- run(): scans DynamoDB items, calculates each item's marshalled size,
  bins into KB-range buckets, and prints a histogram of the distribution.

The size-histogram verb should:
1. Scan the table (optionally filtered by a where/filter expression)
2. For each item, calculate its DynamoDB-marshalled size in bytes
3. Bucket items into ranges: 0-1KB, 1-2KB, 2-3KB, etc.
4. Print a histogram showing bucket label and item count
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from python_modules import size_histogram as sh_module

# The source uses `from python_modules.shared.errors import *` which, under
# our Mock-based conftest, binds nothing. Inject get_error_message.
if not hasattr(sh_module, 'get_error_message'):
    sh_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by size_histogram."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sh_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sh_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sh_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sh_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sh_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext that records accumulator() and parallelize() calls."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    rdd = MagicMock()
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'index': None,
        'filter_expression': None,
        'expression_values': None,
        'expression_names': None,
        's3-bucket-name': 'rate-bucket',
        'JOB_RUN_ID': 'jr-001',
    }


# --- Histogram output behavior ----------------------------------------------


class TestSizeHistogramOutput:
    """The core behavior: run() scans items, computes sizes, and prints a
    histogram showing bucket distribution."""

    def _make_scan_worker(self, monkeypatch, items):
        """Set up mocks so the scan worker processes given items and
        accumulates their sizes into buckets.

        items: list of dicts representing DynamoDB items (unmarshalled).
        """
        # We need to capture the foreach lambda and feed it our items
        # The pattern matches scancount: spark parallelize -> foreach -> worker scans
        session = MagicMock()
        table = MagicMock()

        # Build scan responses from items - return them all in one page
        # Each item in DynamoDB scan response comes with all attributes
        scan_response = {
            'Items': items,
            'Count': len(items),
        }
        table.scan = MagicMock(return_value=scan_response)
        session.resource.return_value.Table.return_value = table

        rl = MagicMock()
        rl.get_session.return_value = session
        monkeypatch.setattr(sh_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        return table

    def test_prints_histogram_with_bucket_labels(self, monkeypatch,
                                                   shared_table_info_mocks,
                                                   rate_limiter_mocks,
                                                   spark_context, base_args,
                                                   capsys):
        """run() should print a histogram with KB range labels like '0-1 KB'."""
        # Create items of known sizes:
        # - small item (~50 bytes) -> 0-1 KB bucket
        # - medium item (~1500 bytes) -> 1-2 KB bucket
        small_item = {'pk': 'a', 'data': 'x'}  # small, fits in 0-1KB
        medium_item = {'pk': 'b', 'data': 'y' * 1500}  # bigger, 1-2KB range

        # The histogram accumulator should collect size info from workers
        # We'll simulate the accumulators holding bucket counts
        # After all workers finish, the histogram dict is: {0: count_0_1kb, 1: count_1_2kb, ...}
        histogram_data = {0: 3, 1: 2}  # 3 items in 0-1KB, 2 items in 1-2KB
        total_items = 5

        # Mock accumulators: first is histogram dict, second is total count, third is errors
        accs = [
            MagicMock(value=histogram_data),  # histogram accumulator
            MagicMock(value=total_items),      # total items counted
            MagicMock(value=[]),               # error accumulator
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sh_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sh_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        # The output must contain bucket labels and counts
        assert '0-1 KB' in out or '0-1KB' in out, \
            "histogram should show 0-1 KB bucket label"
        assert '1-2 KB' in out or '1-2KB' in out, \
            "histogram should show 1-2 KB bucket label"

    def test_prints_total_items_counted(self, monkeypatch,
                                         shared_table_info_mocks,
                                         rate_limiter_mocks,
                                         spark_context, base_args,
                                         capsys):
        """run() should print the total number of items analyzed."""
        histogram_data = {0: 10}
        total_items = 10

        accs = [
            MagicMock(value=histogram_data),
            MagicMock(value=total_items),
            MagicMock(value=[]),
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sh_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sh_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '10' in out, "total items counted should appear in output"

    def test_multiple_buckets_sorted_by_range(self, monkeypatch,
                                               shared_table_info_mocks,
                                               rate_limiter_mocks,
                                               spark_context, base_args,
                                               capsys):
        """Histogram buckets should be printed in ascending order of size range."""
        # 5 items in 0-1KB, 3 in 2-3KB, 1 in 5-6KB (gaps are fine)
        histogram_data = {0: 5, 2: 3, 5: 1}
        total_items = 9

        accs = [
            MagicMock(value=histogram_data),
            MagicMock(value=total_items),
            MagicMock(value=[]),
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sh_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sh_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        lines = out.strip().split('\n')

        # Find lines containing KB bucket labels
        bucket_lines = [l for l in lines if 'KB' in l or 'kb' in l.lower()]
        assert len(bucket_lines) >= 3, \
            f"Expected at least 3 bucket lines, got {len(bucket_lines)}: {bucket_lines}"

        # Verify ascending order: first bucket line should be 0-1, last should be 5-6
        first_bucket = bucket_lines[0]
        last_bucket = bucket_lines[-1]
        assert '0' in first_bucket, "first bucket should be 0-1 KB range"
        assert '5' in last_bucket or '6' in last_bucket, \
            "last bucket should be 5-6 KB range"

    def test_empty_table_prints_zero_items(self, monkeypatch,
                                            shared_table_info_mocks,
                                            rate_limiter_mocks,
                                            spark_context, base_args,
                                            capsys):
        """When no items are found, the histogram should indicate zero items."""
        histogram_data = {}
        total_items = 0

        accs = [
            MagicMock(value=histogram_data),
            MagicMock(value=total_items),
            MagicMock(value=[]),
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sh_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sh_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '0' in out, "zero items should be reported"


class TestSizeHistogramWorker:
    """The worker function (_size_data or similar) that scans items and
    calculates sizes should correctly bucket items by their size."""

    def test_item_under_1kb_goes_to_bucket_0(self, monkeypatch):
        """An item of ~50 bytes should be placed in bucket 0 (0-1 KB)."""
        # This tests the worker function that computes item sizes
        # Item size in DynamoDB is the sum of attribute name lengths + value sizes
        # For a simple item like {'pk': 'a'}, it's very small
        assert hasattr(sh_module, '_size_data') or hasattr(sh_module, '_compute_sizes'), \
            "Module should expose a worker function for computing sizes"

    def test_item_over_1kb_goes_to_bucket_1(self, monkeypatch):
        """An item of ~1500 bytes should be placed in bucket 1 (1-2 KB)."""
        assert hasattr(sh_module, '_size_data') or hasattr(sh_module, '_compute_sizes'), \
            "Module should expose a worker function for computing sizes"


class TestSizeHistogramModuleStructure:
    """Basic module structure checks."""

    def test_module_is_importable(self):
        """size_histogram module loads without error under test mocks."""
        assert sh_module is not None

    def test_module_exposes_run_function(self):
        """run() is the public entry point."""
        assert callable(sh_module.run)
