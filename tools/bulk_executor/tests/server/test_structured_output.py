"""Failing tests for issue #180: Create a convention for structured output across commands.

When --output json is passed, commands should emit machine-parseable JSON
to stdout instead of human-readable decorated text. This enables piping
output between commands (e.g., `bulk find --output json | jq '.s3_location'`).

The behavior tested here:
- find.run() with output='json' should emit results as a JSON object (not
  decorated text with color codes and "First N matching items:" headers)
- The JSON output should include structured fields like item_count, s3_location
- count output as JSON should be a simple {"count": N} object
"""

import json
import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import find as find_module
from python_modules.shared.glue_connector import read_dynamodb_dataframe


class TestStructuredOutputCount:
    """When output='json', the count verb should emit a JSON object."""

    def test_count_with_json_output_emits_valid_json(self, monkeypatch, capsys):
        """count action with output=json should print a JSON object with a 'count' field."""
        # Set up the dataframe mock from conftest
        df_stub = read_dynamodb_dataframe
        # Reset it for our test
        df_stub.last_df = None

        parsed_args = {
            'splits': '200',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'count',
            'output': 'json',  # <-- the new parameter
        }

        # Mock table info helpers
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'item_count': 1000,
            'size_bytes': 50000,
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
        }))
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock()
        ))

        job = MagicMock()
        spark_context = MagicMock()
        glue_context = MagicMock()

        # Make the dataframe stub return a known count
        find_module.run(job, spark_context, glue_context, parsed_args)

        captured = capsys.readouterr()
        stdout = captured.out.strip()

        # The output should be valid JSON
        result = json.loads(stdout)
        assert 'count' in result, f"Expected 'count' key in JSON output, got: {result}"
        assert isinstance(result['count'], int), "count should be an integer"


class TestStructuredOutputFind:
    """When output='json', the find verb should emit a JSON object with results metadata."""

    def test_find_with_json_output_includes_s3_location(self, monkeypatch, capsys):
        """find action with output=json should include s3_location in JSON output."""
        parsed_args = {
            'splits': '200',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-456',
            'output': 'json',  # <-- the new parameter
        }

        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'item_count': 1000,
            'size_bytes': 50000,
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
        }))
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock()
        ))
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        job = MagicMock()
        spark_context = MagicMock()
        glue_context = MagicMock()

        find_module.run(job, spark_context, glue_context, parsed_args)

        captured = capsys.readouterr()
        stdout = captured.out.strip()

        # Should be valid JSON with s3_location
        result = json.loads(stdout)
        assert 's3_location' in result or 's3_output' in result or 'location' in result, (
            f"Expected structured output with S3 location field. Got: {result}"
        )
        # Should contain the expected S3 path
        json_str = json.dumps(result)
        assert 'my-bucket' in json_str, "S3 bucket should appear in output"
        assert 'run-456' in json_str, "Job run ID should appear in S3 path"


class TestStructuredOutputNotSet:
    """When output is not set (default), commands should produce human-readable output as today."""

    def test_count_without_output_flag_produces_text(self, monkeypatch, capsys):
        """Default output (no --output flag) should produce the existing text format."""
        parsed_args = {
            'splits': '200',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'count',
            # No 'output' key — defaults to text
        }

        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'item_count': 1000,
            'size_bytes': 50000,
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
        }))
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
        monkeypatch.setattr(find_module, 'boto3', MagicMock(
            Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
            client=MagicMock()
        ))

        job = MagicMock()
        spark_context = MagicMock()
        glue_context = MagicMock()

        find_module.run(job, spark_context, glue_context, parsed_args)

        captured = capsys.readouterr()
        stdout = captured.out

        # Default output should contain the human-readable "Count of matching items:" text
        assert 'Count of matching items:' in stdout or 'count' in stdout.lower(), (
            f"Default text output should contain readable count line. Got: {stdout}"
        )

        # Should NOT be valid JSON (it's decorated text)
        try:
            json.loads(stdout.strip())
            # If we get here, the output is valid JSON even without the flag —
            # that means structured output is always on, which defeats the purpose
            pytest.fail("Default output should NOT be valid JSON — it should be human-readable text")
        except json.JSONDecodeError:
            pass  # Expected: text output is not JSON
