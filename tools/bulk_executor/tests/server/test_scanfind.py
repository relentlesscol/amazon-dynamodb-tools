"""Unit tests for the `scanfind` server-side verb.

Covers `python_modules/scanfind/__init__.py`:
- run(): dispatches parallel boto3 scans with FilterExpression and returns
  actual matching items (ALL_ATTRIBUTES) instead of just counts.
- _find_data: worker function that scans a segment using boto3, collects
  matching items, respects a limit parameter, and accumulates results.
- limit support: stops scanning early once enough items are found globally.
- output: prints matching items as JSON lines to stdout.
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

# The scanfind module doesn't exist yet — importing it should fail,
# which makes this test fail (RED phase).
from python_modules import scanfind as sf_module

# Star-imported from shared.errors — Mock doesn't populate __all__
sf_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by scanfind with fresh mocks."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sf_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sf_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sf_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sf_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sf_module, 'RateLimiterAggregator', aggregator_cls)
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
        'filter_expression': '#status = :val',
        'expression_values': '{":val": "active"}',
        'expression_names': '{"#status": "status"}',
        'limit': None,
        's3-bucket-name': 'rate-bucket',
        'JOB_RUN_ID': 'jr-001',
    }


# --- Helper to run _find_data -----------------------------------------------


def _make_rl_worker(session=None):
    """Build a mock RateLimiterWorker with a controllable session."""
    rl = MagicMock()
    if session is None:
        session = MagicMock()
    rl.get_session.return_value = session
    return rl


# --- _find_data: returns items, not counts ----------------------------------


class TestFindDataReturnsItems:
    """_find_data should return actual items from the scan, not just counts."""

    def test_single_page_returns_items(self, monkeypatch):
        """When scan returns Items, _find_data collects and returns them."""
        session = MagicMock()
        table = MagicMock()
        items = [
            {'pk': 'id1', 'status': 'active', 'name': 'Alice'},
            {'pk': 'id2', 'status': 'active', 'name': 'Bob'},
        ]
        table.scan = MagicMock(return_value={'Items': items, 'Count': 2})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        items_acc = MagicMock()
        items_acc.value = []
        error_acc = MagicMock()

        result = sf_module._find_data(
            {}, 'tbl', None, '#status = :val', '{":val": "active"}',
            '{"#status": "status"}', None,
            0, 1, items_acc, error_acc, MagicMock()
        )

        # The worker should accumulate items (add them to the accumulator)
        items_acc.add.assert_called()
        added_items = items_acc.add.call_args.args[0]
        assert len(added_items) == 2
        assert added_items[0]['pk'] == 'id1'
        assert added_items[1]['pk'] == 'id2'

    def test_scan_uses_all_attributes_not_count(self, monkeypatch):
        """scanfind must NOT use Select=COUNT; it needs actual item data."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Items': [], 'Count': 0})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sf_module._find_data(
            {}, 'tbl', None, None, None, None, None,
            0, 1, MagicMock(), MagicMock(), MagicMock()
        )

        scan_kwargs = table.scan.call_args.kwargs
        # Must NOT have Select=COUNT — we want actual items
        assert scan_kwargs.get('Select') != 'COUNT', \
            "scanfind must retrieve items, not just count"

    def test_multi_page_collects_all_items(self, monkeypatch):
        """Pagination should collect items from all pages."""
        session = MagicMock()
        table = MagicMock()
        scan_responses = iter([
            {'Items': [{'pk': 'a'}], 'Count': 1, 'LastEvaluatedKey': {'pk': 'a'}},
            {'Items': [{'pk': 'b'}], 'Count': 1, 'LastEvaluatedKey': {'pk': 'b'}},
            {'Items': [{'pk': 'c'}], 'Count': 1},
        ])
        table.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        items_acc = MagicMock()
        items_acc.value = []
        sf_module._find_data(
            {}, 'tbl', None, None, None, None, None,
            0, 1, items_acc, MagicMock(), MagicMock()
        )

        # Should have accumulated all 3 items across pages
        all_added = []
        for c in items_acc.add.call_args_list:
            all_added.extend(c.args[0])
        assert len(all_added) == 3
        assert [i['pk'] for i in all_added] == ['a', 'b', 'c']


# --- Limit support ----------------------------------------------------------


class TestFindDataLimitSupport:
    """When a limit is specified, workers should stop scanning once enough
    items have been found globally."""

    def test_limit_stops_scanning_early(self, monkeypatch):
        """With limit=2, worker should stop after finding 2 items even if
        there are more pages available."""
        session = MagicMock()
        table = MagicMock()
        # First page has 2 items and claims there's more data
        scan_responses = iter([
            {'Items': [{'pk': 'a'}, {'pk': 'b'}], 'Count': 2,
             'LastEvaluatedKey': {'pk': 'b'}},
            # This page should NOT be reached
            {'Items': [{'pk': 'c'}], 'Count': 1},
        ])
        table.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        items_acc = MagicMock()
        items_acc.value = []
        sf_module._find_data(
            {}, 'tbl', None, None, None, None, 2,  # limit=2
            0, 1, items_acc, MagicMock(), MagicMock()
        )

        # Should have only scanned once (got 2 items, which meets limit)
        assert table.scan.call_count == 1

    def test_no_limit_scans_all_pages(self, monkeypatch):
        """Without limit (None), worker should scan all pages."""
        session = MagicMock()
        table = MagicMock()
        scan_responses = iter([
            {'Items': [{'pk': 'a'}], 'Count': 1, 'LastEvaluatedKey': {'pk': 'a'}},
            {'Items': [{'pk': 'b'}], 'Count': 1},
        ])
        table.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        items_acc = MagicMock()
        items_acc.value = []
        sf_module._find_data(
            {}, 'tbl', None, None, None, None, None,  # no limit
            0, 1, items_acc, MagicMock(), MagicMock()
        )

        assert table.scan.call_count == 2


# --- run() output behavior --------------------------------------------------


class TestRunOutputBehavior:
    """run() should print matching items as JSON lines to stdout."""

    def test_run_prints_items_as_json_lines(self, monkeypatch, shared_table_info_mocks,
                                             rate_limiter_mocks, spark_context, base_args, capsys):
        """After parallel scan completes, run() should print each found item
        as a JSON line to stdout."""
        items_found = [
            {'pk': 'id1', 'status': 'active', 'name': 'Alice'},
            {'pk': 'id2', 'status': 'active', 'name': 'Bob'},
        ]

        # Mock the accumulators so items_acc.value returns found items
        items_acc = MagicMock(value=items_found)
        error_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[items_acc, error_acc])
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sf_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        lines = [l for l in out.strip().split('\n') if l.strip()]

        # Should contain JSON representations of both items
        json_lines = []
        for line in lines:
            try:
                json_lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue

        assert len(json_lines) >= 2, f"Expected at least 2 JSON items in output, got: {lines}"
        pks = [item['pk'] for item in json_lines]
        assert 'id1' in pks
        assert 'id2' in pks

    def test_run_prints_total_found_count(self, monkeypatch, shared_table_info_mocks,
                                           rate_limiter_mocks, spark_context, base_args, capsys):
        """run() should print a summary of how many items were found."""
        items_found = [{'pk': 'x'}, {'pk': 'y'}, {'pk': 'z'}]
        items_acc = MagicMock(value=items_found)
        error_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[items_acc, error_acc])
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sf_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sf_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        # Should mention the count of found items
        assert '3' in out, "Output should include the count of found items"


# --- Error handling ---------------------------------------------------------


class TestFindDataErrorHandling:
    """Errors in _find_data should be accumulated, not raised."""

    def test_scan_error_accumulated_not_raised(self, monkeypatch):
        """Errors during scan are caught and added to error_accumulator."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttled'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sf_module, 'get_error_message', lambda e: f'msg:{e}')

        error_acc = MagicMock()
        # Should NOT raise
        sf_module._find_data(
            {}, 'tbl', None, None, None, None, None,
            7, 10, MagicMock(), error_acc, MagicMock()
        )

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1
        assert 'worker 7' in appended[0].lower() or '7' in appended[0]

    def test_rate_limiter_shutdown_after_error(self, monkeypatch):
        """rate_limiter_worker.shutdown() called in finally block."""
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('boom'))
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sf_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sf_module, 'get_error_message', lambda e: str(e))

        sf_module._find_data(
            {}, 'tbl', None, None, None, None, None,
            0, 1, MagicMock(), MagicMock(), MagicMock()
        )

        rl.shutdown.assert_called_once()
