"""Failing tests for issue #180: Create a convention for structured output across commands.

Commands should support a --output flag (like the AWS CLI) that controls the
format of their output. When --output json is specified, the command should
emit machine-parseable JSON instead of human-readable text.

This enables piping: `bulk find ... --output json | jq '.s3_location'`
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import find as find_module


class TestStructuredJsonOutput:
    """When --output json is specified, commands emit structured JSON."""

    def test_find_json_output_contains_s3_location(self, monkeypatch, capsys):
        """In JSON output mode, find should output the S3 location as structured data."""
        mock_table_info = MagicMock(return_value={
            'table_name': 'my-table',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 100,
            'size_bytes': 10000,
            'key_schema': {'pk': {'name': 'id', 'type': 'S'}}
        })
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', mock_table_info)
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.01))
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        # Mock the DataFrame chain
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        mock_df.cache.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.toJSON.return_value = MagicMock()
        mock_df.toJSON.return_value.collect.return_value = [
            '{"id": "1", "name": "Alice"}',
            '{"id": "2", "name": "Bob"}',
        ]
        mock_read = MagicMock(return_value=mock_df)
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)

        # Mock SparkSession for json_df.write
        mock_spark_session = MagicMock()
        monkeypatch.setattr('python_modules.find.SparkSession', mock_spark_session)

        parsed_args = {
            'table': 'my-table',
            'splits': '200',
            'XAction': 'find',
            'where': None,
            'orderby': None,
            'limit': None,
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'job-123',
            'output': 'json',  # NEW: structured output mode
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out
        # The output should contain valid JSON with the S3 location
        # Find the JSON portion of the output (might be mixed with other prints)
        lines = output.strip().split('\n')
        json_lines = [l for l in lines if l.strip().startswith('{')]
        assert json_lines, f"Expected JSON output, got:\n{output}"

        # Parse the structured output
        result = json.loads(json_lines[-1])  # Last JSON line should be the summary
        assert 's3_location' in result or 's3_output' in result or 'location' in result, \
            f"JSON output should contain S3 location, got: {result}"

    def test_count_json_output_is_parseable(self, monkeypatch, capsys):
        """In JSON output mode, count should emit count as structured JSON."""
        mock_table_info = MagicMock(return_value={
            'table_name': 'my-table',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 1000,
            'size_bytes': 100000,
            'key_schema': {'pk': {'name': 'id', 'type': 'S'}}
        })
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', mock_table_info)
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.01))
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        mock_df = MagicMock()
        mock_df.count.return_value = 42
        mock_read = MagicMock(return_value=mock_df)
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)

        parsed_args = {
            'table': 'my-table',
            'splits': '200',
            'XAction': 'count',
            'where': None,
            'orderby': None,
            'limit': None,
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'job-123',
            'output': 'json',  # NEW: structured output mode
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out
        lines = output.strip().split('\n')
        json_lines = [l for l in lines if l.strip().startswith('{')]
        assert json_lines, f"Expected JSON output for count, got:\n{output}"

        result = json.loads(json_lines[-1])
        assert 'count' in result, f"JSON output should contain count, got: {result}"
        assert result['count'] == 42
