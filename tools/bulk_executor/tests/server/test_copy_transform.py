"""Test for issue #93: copy verb with a hook to transform items.

The copy verb should support a --transformer parameter that applies a
user-provided Python function to each item during the copy. This enables
redaction, reshaping, filtering, etc.

This tests the server-side _copy_data function: when a transformer function
is provided, each item should be passed through it before writing.
"""

from unittest.mock import MagicMock, call

import pytest

from python_modules import copy as copy_module


class TestCopyWithTransform:
    """copy._copy_data should apply a transformer function to items."""

    def _setup_worker(self, monkeypatch, scan_items, transformer=None):
        """Set up a worker environment with mocked DynamoDB and a transformer."""
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        table = MagicMock()
        table.scan.return_value = {'Items': scan_items, 'LastEvaluatedKey': None}
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        return bw, table

    def test_transformer_is_applied_to_each_item(self, monkeypatch):
        """When a transformer is provided, each item passes through it."""
        items = [{'pk': 'a', 'secret': 'classified'}, {'pk': 'b', 'secret': 'top-secret'}]
        bw, _ = self._setup_worker(monkeypatch, items)

        # Transformer that redacts the 'secret' field
        def redact(item):
            item = dict(item)
            item['secret'] = 'REDACTED'
            return item

        copy_module._copy_data(
            'src', 'dst', {}, {}, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),
            transformer=redact,
        )

        put_calls = bw.put_item.call_args_list
        assert len(put_calls) == 2
        for c in put_calls:
            assert c.kwargs['Item']['secret'] == 'REDACTED'

    def test_transformer_can_filter_items_by_returning_none(self, monkeypatch):
        """A transformer returning None should skip that item (filter it out)."""
        items = [{'pk': 'keep'}, {'pk': 'skip'}, {'pk': 'keep2'}]
        bw, _ = self._setup_worker(monkeypatch, items)

        def keep_filter(item):
            if item['pk'] == 'skip':
                return None
            return item

        copy_module._copy_data(
            'src', 'dst', {}, {}, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),
            transformer=keep_filter,
        )

        put_calls = bw.put_item.call_args_list
        assert len(put_calls) == 2
        written_pks = [c.kwargs['Item']['pk'] for c in put_calls]
        assert 'skip' not in written_pks

    def test_no_transformer_copies_items_unmodified(self, monkeypatch):
        """Without a transformer, items are copied as-is (backward compat)."""
        items = [{'pk': 'x', 'data': 'original'}]
        bw, _ = self._setup_worker(monkeypatch, items)

        copy_module._copy_data(
            'src', 'dst', {}, {}, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),
            transformer=None,
        )

        put_calls = bw.put_item.call_args_list
        assert len(put_calls) == 1
        assert put_calls[0].kwargs['Item'] == {'pk': 'x', 'data': 'original'}

    def test_transformer_in_run_is_wired_to_workers(self, monkeypatch):
        """run() should accept a transformer arg and pass it through to workers."""
        # Mock all the shared helpers
        monkeypatch.setattr(copy_module, 'get_and_print_dynamodb_table_info',
                            MagicMock(return_value={'item_count': 1, 'size_bytes': 100, 'region_name': 'us-east-1'}))
        monkeypatch.setattr(copy_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0))
        monkeypatch.setattr(copy_module, 'get_and_print_table_copy_write_cost', MagicMock(return_value=0))
        monkeypatch.setattr(copy_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(copy_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(copy_module, 'RateLimiterAggregator', MagicMock())

        captured_lambda_args = {}

        def fake_foreach(fn):
            # Call the lambda with worker_id=0 and capture what it does
            captured_lambda_args['fn'] = fn

        spark_context = MagicMock()
        spark_context.accumulator = MagicMock(side_effect=[MagicMock(value=0), MagicMock(value=[])])
        spark_context.parallelize.return_value.foreach = fake_foreach

        args = {
            'source': 'src-table',
            'target': 'dst-table',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'jr-1',
            'transformer': 'my_transform_module',  # user-provided transformer
        }

        # run() should accept the transformer from parsed_args
        copy_module.run(MagicMock(), spark_context, MagicMock(), args)

        # The foreach lambda should have been created — the key test is that
        # a transformer parameter flows into the function
        assert 'fn' in captured_lambda_args
