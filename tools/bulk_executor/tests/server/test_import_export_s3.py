"""Unit tests for native import-from-S3 and export-to-S3 commands (issue #176).

DynamoDB has native import/export functionality. The bulk executor should
wrap these as commands (`import_s3` and `export_s3`) that call the
DynamoDB APIs and report results.

This tests the OBSERVABLE BEHAVIOR: that server-side modules for import_s3
and export_s3 exist and their run() functions call the appropriate DynamoDB
APIs (ImportTable / ExportTableToPointInTime) and produce output about the
operation status.
"""

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest


class TestExportToS3Command:
    """The export_s3 server-side module should invoke DynamoDB's native
    ExportTableToPointInTime API and print the export ARN/status."""

    def test_export_s3_module_exists_and_has_run(self):
        """A module `python_modules.export_s3` must exist with a run() function."""
        try:
            from python_modules import export_s3
        except (ImportError, ModuleNotFoundError):
            pytest.fail(
                "python_modules.export_s3 module does not exist. "
                "Issue #176 requires wrapping native export-to-S3 as a command."
            )
        assert hasattr(export_s3, 'run'), \
            "export_s3 module must have a run() function"

    def test_export_s3_run_calls_export_table_api(self):
        """export_s3.run() must call dynamodb.export_table_to_point_in_time."""
        from python_modules import export_s3

        job = MagicMock()
        spark_context = MagicMock()
        glue_context = MagicMock()
        parsed_args = {
            'table': 'my-table',
            's3-bucket-name': 'my-export-bucket',
            's3-prefix': 'exports/',
        }

        mock_client = MagicMock()
        mock_client.export_table_to_point_in_time.return_value = {
            'ExportDescription': {
                'ExportArn': 'arn:aws:dynamodb:us-east-1:123:table/my-table/export/abc',
                'ExportStatus': 'IN_PROGRESS',
            }
        }

        with patch('python_modules.export_s3.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_client
            export_s3.run(job, spark_context, glue_context, parsed_args)

        mock_client.export_table_to_point_in_time.assert_called_once()

    def test_export_s3_run_prints_export_location(self, capsys):
        """export_s3.run() should print the S3 export location."""
        from python_modules import export_s3

        parsed_args = {
            'table': 'test-table',
            's3-bucket-name': 'export-bucket',
            's3-prefix': 'output/',
        }

        mock_client = MagicMock()
        mock_client.export_table_to_point_in_time.return_value = {
            'ExportDescription': {
                'ExportArn': 'arn:aws:dynamodb:us-east-1:123:table/test-table/export/xyz',
                'ExportStatus': 'IN_PROGRESS',
                'S3Bucket': 'export-bucket',
                'S3Prefix': 'output/',
            }
        }

        with patch('python_modules.export_s3.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_client
            export_s3.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out
        assert 'export-bucket' in output or 'export' in output.lower()


class TestImportFromS3Command:
    """The import_s3 server-side module should invoke DynamoDB's native
    ImportTable API and print the import ARN/status."""

    def test_import_s3_module_exists_and_has_run(self):
        """A module `python_modules.import_s3` must exist with a run() function."""
        try:
            from python_modules import import_s3
        except (ImportError, ModuleNotFoundError):
            pytest.fail(
                "python_modules.import_s3 module does not exist. "
                "Issue #176 requires wrapping native import-from-S3 as a command."
            )
        assert hasattr(import_s3, 'run'), \
            "import_s3 module must have a run() function"

    def test_import_s3_run_calls_import_table_api(self):
        """import_s3.run() must call dynamodb.import_table."""
        from python_modules import import_s3

        parsed_args = {
            'table': 'target-table',
            's3-bucket-name': 'import-bucket',
            's3-prefix': 'data/',
            'input-format': 'DYNAMODB_JSON',
        }

        mock_client = MagicMock()
        mock_client.import_table.return_value = {
            'ImportTableDescription': {
                'ImportArn': 'arn:aws:dynamodb:us-east-1:123:table/target-table/import/def',
                'ImportStatus': 'IN_PROGRESS',
            }
        }

        with patch('python_modules.import_s3.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_client
            import_s3.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        mock_client.import_table.assert_called_once()
