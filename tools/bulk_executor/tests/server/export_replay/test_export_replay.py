"""Unit tests for the export_replay verb (issue #95).

The `export-replay` verb plays an incremental export FORWARD on a target table:
- Items with new_image → PUT (write the new state)
- Items with no new_image (deletions) → DELETE (remove the item)
- Only incremental exports are supported (full exports should be rejected)
- Only NEW_AND_OLD_IMAGES view type is supported (need full context)

This tests the server-side module behavior, not just argument parsing.
"""

import pytest
from unittest.mock import Mock, patch

from python_modules.export_replay import run, _post_validate
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.export.pipeline import _apply_transform_and_resolve
from python_modules.shared.export.utils.enums import ExportLoadType, Operation
from python_modules.shared.export.parsers.records import IncrementalExportRecord


@pytest.fixture
def mock_spark_context():
    sc = Mock()
    sc.defaultParallelism = 4
    sc.accumulator = Mock(side_effect=lambda init, *args: Mock(value=init))
    return sc


@pytest.fixture
def mock_glue_context():
    return Mock()


@pytest.fixture
def mock_job():
    return Mock()


@pytest.fixture
def parsed_args():
    return {
        'table': 'target-table',
        's3_path': 's3://my-bucket/prefix/AWSDynamoDB/export-001',
        'transform': None,
        's3-bucket-name': 'config-bucket',
        'JOB_RUN_ID': 'jr_456',
        'XDebug': 'false',
    }


@pytest.fixture
def key_schema():
    return {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}}


@pytest.fixture
def manifest_data_incremental_new_and_old():
    return {
        'total_item_count': 50,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 50},
        ],
        'export_type': 'INCREMENTAL_EXPORT',
        'output_view': 'NEW_AND_OLD_IMAGES',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def manifest_data_full():
    return {
        'total_item_count': 50,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 50},
        ],
        'export_type': 'FULL_EXPORT',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def table_info():
    return {
        'key_schema': {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}},
        'table_name': 'target-table',
        'billing_mode': 'PAY_PER_REQUEST',
    }


class TestExportReplayValidation:
    """export-replay must reject non-incremental exports."""

    @patch('python_modules.shared.export.pipeline.validate')
    def test_rejects_full_export(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema, manifest_data_full):
        """export-replay only works with incremental exports; full export raises BulkExecutorError."""
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_data_full,
            'key_schema_result': {'avg_item_size': 200},
        }

        with pytest.raises(BulkExecutorError, match="export-replay requires an incremental export"):
            run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

    @patch('python_modules.shared.export.pipeline.validate')
    def test_rejects_new_image_only_view(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema):
        """export-replay requires NEW_AND_OLD_IMAGES view to have full replay context."""
        manifest_new_image_only = {
            'total_item_count': 50,
            'data_files': [{'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 50}],
            'export_type': 'INCREMENTAL_EXPORT',
            'output_view': 'NEW_IMAGE',
            'output_format': 'DYNAMODB_JSON',
        }
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_new_image_only,
            'key_schema_result': {'avg_item_size': 200},
        }

        with pytest.raises(BulkExecutorError, match="export-replay requires output view NEW_AND_OLD_IMAGES"):
            run(mock_job, mock_spark_context, mock_glue_context, parsed_args)


class TestExportReplayForwardBehavior:
    """export-replay plays changes forward — new_image stays as-is (no post_transform swap)."""

    def test_replay_preserves_new_image_for_update(self, mock_spark_context, key_schema):
        """An update record (both old and new image) should apply new_image as a PUT.

        Unlike revert-export which swaps new_image=old_image, export-replay
        keeps new_image unchanged so the incremental parser resolves it as PUT(new_image).
        """
        old_item = {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'before'}
        new_item = {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'after'}
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image=new_item,
            old_image=old_item,
            table_key_schema=key_schema,
            write_timestamp_micros=100000
        )

        mock_rdd = Mock()
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        # export-replay should NOT have a post_transform that swaps images.
        # It passes records through to the parser which resolves:
        #   new_image present → PUT(new_image)
        #   new_image absent → DELETE(keys)
        # This is the key behavioral difference from revert-export.
        _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, None,
            'python_modules.export_replay.transform', key_schema, error_accumulator,
            post_transform=None  # export-replay does NOT swap images
        )

        # Since no post_transform, the resolve function (second .map call)
        # should receive the record unchanged — new_image stays 'after'
        resolve_fn = mock_rdd.map.call_args_list[0][0][0]
        resolved = resolve_fn(record)
        # Parser.resolve is called; for incremental records with new_image present
        # the standard parser returns PUT with new_image data
        parser.resolve.assert_called_once_with(record)

    def test_replay_insert_applies_new_item(self, key_schema):
        """An insert (new_image but no old_image) should PUT the new item.

        The incremental parser resolves: new_image present → PUT(new_image).
        """
        from python_modules.shared.export.parsers.incremental_export_parser import IncrementalExportParser

        parser = IncrementalExportParser(key_schema)
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image={'pk_attr': 'pk1', 'sk_attr': 'sk1', 'value': 'inserted'},
            old_image=None,
            table_key_schema=key_schema,
            write_timestamp_micros=200000
        )

        # Without any post_transform (export-replay behavior), resolve uses new_image
        result = parser.resolve(record)
        assert result['operation'] == Operation.PUT
        assert result['data'] == {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'value': 'inserted'}

    def test_replay_deletion_removes_item(self, key_schema):
        """A deletion (old_image but no new_image) should DELETE the item.

        The incremental parser resolves: no new_image → DELETE(keys).
        """
        from python_modules.shared.export.parsers.incremental_export_parser import IncrementalExportParser

        parser = IncrementalExportParser(key_schema)
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image=None,
            old_image={'pk_attr': 'pk1', 'sk_attr': 'sk1', 'value': 'was_here'},
            table_key_schema=key_schema,
            write_timestamp_micros=300000
        )

        result = parser.resolve(record)
        assert result['operation'] == Operation.DELETE
        assert result['data'] == {'pk_attr': 'pk1', 'sk_attr': 'sk1'}


class TestExportReplayPipeline:
    """Full pipeline integration for export-replay."""

    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_full_pipeline_passes_no_post_transform(
        self, mock_validate, mock_cost, mock_read, mock_transform, mock_write, mock_report,
        mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema,
        manifest_data_incremental_new_and_old
    ):
        """export-replay calls run_export_pipeline WITHOUT a post_transform.

        This is the key difference: revert-export passes post_transform=_revert
        which swaps new→old, while export-replay passes no post_transform so
        records flow through unchanged (new_image → PUT, no new_image → DELETE).
        """
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_data_incremental_new_and_old,
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_cost.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.INCREMENTAL, Mock(), 50)
        mock_transform.return_value = (Mock(), False, None, None)
        mock_write.return_value = Mock(value=50)

        run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

        # Verify the pipeline was called correctly
        mock_validate.assert_called_once()
        mock_cost.assert_called_once()
        mock_read.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once()
        mock_report.assert_called_once()

        # The crucial assertion: _apply_transform_and_resolve should have been
        # called with post_transform=None (no image swapping for replay)
        transform_call_kwargs = mock_transform.call_args
        # post_transform is the last positional arg or keyword arg
        call_args = transform_call_kwargs[0] if transform_call_kwargs[0] else ()
        call_kwargs = transform_call_kwargs[1] if transform_call_kwargs[1] else {}
        # In the pipeline, post_transform is passed as keyword argument
        assert call_kwargs.get('post_transform') is None, \
            "export-replay must NOT pass a post_transform (unlike revert-export which swaps images)"

    @patch('python_modules.shared.export.pipeline.validate')
    def test_zero_items_exits_early(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args):
        """export-replay exits early when validate returns None (zero items)."""
        mock_validate.return_value = None
        # Should not raise
        run(mock_job, mock_spark_context, mock_glue_context, parsed_args)
