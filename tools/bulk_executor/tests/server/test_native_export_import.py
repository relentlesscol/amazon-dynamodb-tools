"""Test for issue #176: Wrap native import from S3 and export to S3 as commands.

The bulk tool should wrap DynamoDB's native export-to-S3 and import-from-S3
functionality as commands, adding cost estimation and nice error handling.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestNativeExport:
    """The native_export verb wraps DynamoDB ExportTableToPointInTime."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import native_export
            self.module = native_export
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.native_export does not exist")

    def test_calls_export_table_api(self, monkeypatch):
        """Should call the DynamoDB ExportTableToPointInTime API."""
        mock_client = MagicMock()
        mock_client.export_table_to_point_in_time.return_value = {
            'ExportDescription': {
                'ExportArn': 'arn:aws:dynamodb:us-east-1:123:export/abc',
                'ExportStatus': 'IN_PROGRESS',
            }
        }
        monkeypatch.setattr('boto3.client', MagicMock(return_value=mock_client))

        result = self.module.start_export(
            table_arn='arn:aws:dynamodb:us-east-1:123:table/MyTable',
            s3_bucket='my-bucket',
            s3_prefix='exports/',
        )

        mock_client.export_table_to_point_in_time.assert_called_once()
        call_kwargs = mock_client.export_table_to_point_in_time.call_args.kwargs
        assert call_kwargs['TableArn'] == 'arn:aws:dynamodb:us-east-1:123:table/MyTable'
        assert call_kwargs['S3Bucket'] == 'my-bucket'

    def test_reports_cost_estimate(self, monkeypatch, capsys):
        """Should report estimated export cost before starting."""
        monkeypatch.setattr(self.module, 'get_and_print_dynamodb_table_info',
                            MagicMock(return_value={'size_bytes': 1_000_000_000, 'item_count': 1000000}))

        self.module.print_export_cost_estimate(
            table_info={'size_bytes': 1_000_000_000, 'item_count': 1000000}
        )
        out = capsys.readouterr().out
        assert '$' in out  # Should include a dollar amount


class TestNativeImport:
    """The native_import verb wraps DynamoDB ImportTable."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import native_import
            self.module = native_import
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.native_import does not exist")

    def test_calls_import_table_api(self, monkeypatch):
        """Should call the DynamoDB ImportTable API."""
        mock_client = MagicMock()
        mock_client.import_table.return_value = {
            'ImportTableDescription': {
                'ImportArn': 'arn:aws:dynamodb:us-east-1:123:import/xyz',
                'ImportStatus': 'IN_PROGRESS',
            }
        }
        monkeypatch.setattr('boto3.client', MagicMock(return_value=mock_client))

        result = self.module.start_import(
            table_name='TargetTable',
            s3_bucket='my-bucket',
            s3_prefix='exports/abc/',
            input_format='DYNAMODB_JSON',
        )

        mock_client.import_table.assert_called_once()
        call_kwargs = mock_client.import_table.call_args.kwargs
        assert call_kwargs['TableCreationParameters']['TableName'] == 'TargetTable'
