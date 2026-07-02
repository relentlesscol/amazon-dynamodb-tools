"""Unit tests for the `copy` verb's --transformer feature (issue #93).

The feature: when the user passes --transformer, each item scanned from the
source table is passed through the user-provided transform function before
being written to the target table.  This enables reshaping, filtering, and
redacting data during a cross-table copy.

These tests exercise the SERVER-SIDE behavior: run() must wire the transformer
through to _copy_data, and _copy_data must apply it to every item.  We mock
Spark/boto3 but call the real module logic.
"""

from unittest.mock import MagicMock, patch, call

import pytest

from python_modules import copy as copy_module


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers with predictable mocks."""
    monkeypatch.setattr(copy_module, 'get_and_print_dynamodb_table_info',
                        MagicMock(return_value={'item_count': 10, 'size_bytes': 512, 'region_name': 'us-east-1'}))
    monkeypatch.setattr(copy_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
    monkeypatch.setattr(copy_module, 'get_and_print_table_copy_write_cost', MagicMock(return_value=0.50))
    monkeypatch.setattr(copy_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiter classes so run() doesn't hit real infra."""
    monkeypatch.setattr(copy_module, 'RateLimiterSharedConfig', MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(copy_module, 'RateLimiterAggregator', MagicMock())


@pytest.fixture
def spark_context():
    """Mock SparkContext with working accumulators."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    sc.parallelize = MagicMock(return_value=MagicMock())
    return sc


@pytest.fixture
def base_args_with_transformer():
    """Parsed args that include a transformer — the feature under test."""
    return {
        'source': 'src-table',
        'target': 'dst-table',
        's3-bucket-name': 'bucket',
        'JOB_RUN_ID': 'jr-001',
        'transformer': 'my_transform',
    }


# ---------------------------------------------------------------------------
# Tests: run() passes the transformer to _copy_data
# ---------------------------------------------------------------------------

class TestRunPassesTransformerToCopyData:
    """Issue #93: run() must read 'transformer' from parsed_args and pass it
    to _copy_data so the transform is applied on each worker partition.

    Without this wiring, the --transformer flag is dead code — specifying it
    on the CLI has no observable effect on the copied data."""

    def test_run_passes_transformer_arg_to_copy_data(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks,
        spark_context, base_args_with_transformer
    ):
        """When parsed_args['transformer'] is set, run() must pass it through
        to _copy_data so the worker can load and apply the transform module."""
        captured_calls = []

        def fake_copy_data(*args, **kwargs):
            captured_calls.append((args, kwargs))

        monkeypatch.setattr(copy_module, '_copy_data', fake_copy_data)

        # Have foreach invoke our fake for a single worker
        def fake_foreach(fn):
            fn(0)
        spark_context.parallelize.return_value.foreach = fake_foreach

        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args_with_transformer)

        # _copy_data must receive the transformer name somewhere in its args
        assert len(captured_calls) == 1, "expected _copy_data to be called once (worker 0)"
        all_args = captured_calls[0][0]  # positional args
        all_kwargs = captured_calls[0][1]  # keyword args

        # The transformer must appear either as a positional or keyword argument
        transformer_found = (
            'my_transform' in all_args or
            all_kwargs.get('transformer') == 'my_transform'
        )
        assert transformer_found, (
            f"run() did not pass transformer='my_transform' to _copy_data.\n"
            f"Positional args: {all_args}\n"
            f"Keyword args: {all_kwargs}\n"
            "The --transformer flag has no effect — it's dead code."
        )

    def test_run_without_transformer_still_works(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks,
        spark_context
    ):
        """When no transformer is specified, run() should still function
        (backward compat: copy without transformation)."""
        args_no_transformer = {
            'source': 'src-table',
            'target': 'dst-table',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'jr-002',
        }

        spark_context.parallelize.return_value.foreach = MagicMock()
        # Should not raise
        copy_module.run(MagicMock(), spark_context, MagicMock(), args_no_transformer)


# ---------------------------------------------------------------------------
# Tests: _copy_data applies the transformer to items before writing
# ---------------------------------------------------------------------------

class TestCopyDataAppliesTransformer:
    """When _copy_data receives a transformer, it must load the transform
    module and apply its function to each scanned item BEFORE batch_writer
    puts it to the target table.

    This is the core user-facing behavior: items land in the target table
    transformed, not as raw copies of the source."""

    def _make_copy_data_env(self, monkeypatch, scan_items, transformer_fn=None):
        """Set up mocks for a _copy_data call and return the batch_writer mock
        so the caller can inspect what was written."""
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        # Source table scan returns the provided items in one page
        src_table = MagicMock()
        src_table.scan = MagicMock(return_value={
            'Items': scan_items,
            'LastEvaluatedKey': None,
        })

        # Target table with inspectable batch_writer
        dst_table = MagicMock()
        batch_writer = MagicMock()
        batch_writer.__enter__ = MagicMock(return_value=batch_writer)
        batch_writer.__exit__ = MagicMock(return_value=False)
        dst_table.batch_writer.return_value = batch_writer

        # Route Table() calls: first is source, second is target
        tables = iter([src_table, dst_table])
        session.resource.return_value.Table.side_effect = lambda name: next(tables)

        return batch_writer

    def test_transformer_modifies_items_before_write(self, monkeypatch):
        """A transformer that adds a field should produce items with that field
        in the target table."""
        source_items = [
            {'pk': '1', 'name': 'Alice'},
            {'pk': '2', 'name': 'Bob'},
        ]

        # Transformer adds a 'transformed' flag to each item
        def my_transform(item):
            item = dict(item)  # don't mutate original
            item['transformed'] = True
            return item

        batch_writer = self._make_copy_data_env(monkeypatch, source_items)

        # Call _copy_data with the transformer
        # The implementation should accept a transformer callable (or module name)
        # and apply it to each item before writing
        copy_module._copy_data(
            'src-table', 'dst-table', {}, {},
            0, 1,  # segment, total_segments
            MagicMock(), MagicMock(),  # accumulators
            MagicMock(), MagicMock(),  # rate_limiter configs
            transformer=my_transform,
        )

        # Verify the WRITTEN items have the transformation applied
        put_calls = batch_writer.put_item.call_args_list
        assert len(put_calls) == 2, "both items should be written"

        written_items = [c.kwargs['Item'] for c in put_calls]
        for item in written_items:
            assert item.get('transformed') is True, (
                f"Item was written WITHOUT transformation: {item}\n"
                "The transformer function was not applied before put_item."
            )

    def test_transformer_can_filter_items_by_returning_none(self, monkeypatch):
        """A transformer that returns None should cause the item to be skipped
        (not written to the target). This supports the 'limit what gets sent'
        use case from the issue."""
        source_items = [
            {'pk': '1', 'name': 'Alice', 'active': True},
            {'pk': '2', 'name': 'Bob', 'active': False},
            {'pk': '3', 'name': 'Carol', 'active': True},
        ]

        # Transformer filters out inactive users
        def filter_active(item):
            if not item.get('active'):
                return None
            return item

        batch_writer = self._make_copy_data_env(monkeypatch, source_items)

        copy_module._copy_data(
            'src-table', 'dst-table', {}, {},
            0, 1,
            MagicMock(), MagicMock(),
            MagicMock(), MagicMock(),
            transformer=filter_active,
        )

        put_calls = batch_writer.put_item.call_args_list
        assert len(put_calls) == 2, (
            f"Expected 2 items written (active only), got {len(put_calls)}.\n"
            "Transformer returning None should skip the item."
        )
        written_names = {c.kwargs['Item']['name'] for c in put_calls}
        assert written_names == {'Alice', 'Carol'}, (
            "Only active items should be written to target"
        )

    def test_transformer_can_reshape_items(self, monkeypatch):
        """A transformer that removes/renames attributes (redaction use case)."""
        source_items = [
            {'pk': '1', 'name': 'Alice', 'ssn': '123-45-6789', 'email': 'alice@example.com'},
        ]

        # Transformer redacts PII
        def redact_pii(item):
            item = dict(item)
            item.pop('ssn', None)
            item['email'] = '***@***.***'
            return item

        batch_writer = self._make_copy_data_env(monkeypatch, source_items)

        copy_module._copy_data(
            'src-table', 'dst-table', {}, {},
            0, 1,
            MagicMock(), MagicMock(),
            MagicMock(), MagicMock(),
            transformer=redact_pii,
        )

        put_calls = batch_writer.put_item.call_args_list
        assert len(put_calls) == 1
        written_item = put_calls[0].kwargs['Item']
        assert 'ssn' not in written_item, "PII field 'ssn' should be redacted"
        assert written_item['email'] == '***@***.***', "email should be masked"
        assert written_item['pk'] == '1', "non-PII fields preserved"

    def test_no_transformer_writes_items_unmodified(self, monkeypatch):
        """Without a transformer, items are written as-is (backward compat)."""
        source_items = [
            {'pk': '1', 'data': 'original'},
        ]

        batch_writer = self._make_copy_data_env(monkeypatch, source_items)

        # Call without transformer kwarg — current behavior
        copy_module._copy_data(
            'src-table', 'dst-table', {}, {},
            0, 1,
            MagicMock(), MagicMock(),
            MagicMock(), MagicMock(),
        )

        put_calls = batch_writer.put_item.call_args_list
        assert len(put_calls) == 1
        written_item = put_calls[0].kwargs['Item']
        assert written_item == {'pk': '1', 'data': 'original'}, \
            "without transformer, items are written verbatim"
