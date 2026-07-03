"""Unit tests for structured output convention across commands (issue #180).

Commands should support an `--output` argument (like AWS CLI: json, text, table)
that controls how results are formatted. When `--output json` is specified,
the command produces machine-parseable JSON output instead of human-readable text.

This tests the OBSERVABLE BEHAVIOR: that the `find` command's run() function,
when given output='json' in parsed_args, produces valid JSON output containing
structured result data (items, count, s3_location), rather than the default
human-readable print lines.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# find.py imports these modules not covered by conftest
sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
_pyspark_sql_functions = MagicMock()
sys.modules.setdefault('pyspark.sql.functions', _pyspark_sql_functions)

from python_modules import find as find_module

# Inject get_error_message if star-import didn't populate it
if not hasattr(find_module, 'get_error_message'):
    find_module.get_error_message = lambda e: str(e)


class TestStructuredJsonOutput:
    """When output='json' is in parsed_args, commands must produce
    structured JSON output to stdout instead of human-readable text."""

    @pytest.fixture
    def mock_glue_connector(self, monkeypatch):
        """Mock the glue_connector.read_dynamodb_dataframe."""
        df = MagicMock()
        df.count.return_value = 3
        df.cache.return_value = df
        df.filter.return_value = df
        df.orderBy.return_value = df
        df.limit.return_value = df
        df.toJSON.return_value = MagicMock()
        df.toJSON.return_value.collect.return_value = [
            '{"pk": "a", "data": "hello"}',
            '{"pk": "b", "data": "world"}',
            '{"pk": "c", "data": "foo"}',
        ]
        df.select.return_value = df
        df.repartition.return_value = df

        mock_read = MagicMock(return_value=df)
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', mock_read)
        return df

    @pytest.fixture
    def mock_dependencies(self, monkeypatch):
        """Mock table_info and other shared dependencies."""
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'item_count': 1000,
            'size_bytes': 50000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
            'read_pricing_category': 'ReadRequestUnits',
            'region_name': 'us-east-1',
        }))
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

    def test_find_with_output_json_produces_valid_json(
        self, monkeypatch, mock_glue_connector, mock_dependencies, capsys
    ):
        """find command with output='json' must produce valid JSON to stdout."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        parsed_args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-001',
            'output': 'json',  # <-- structured output request
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out.strip()

        # The output must be valid JSON
        try:
            result = json.loads(output)
        except json.JSONDecodeError:
            pytest.fail(
                f"With output='json', find should produce valid JSON. "
                f"Got:\n{output[:500]}"
            )

        # The JSON must contain structured result information
        assert isinstance(result, dict), "JSON output should be a dict"
        # Should have count or items or s3_location
        assert any(key in result for key in ('count', 'items', 's3_location', 'item_count')), \
            f"JSON output should contain structured result data, got keys: {list(result.keys())}"

    def test_find_with_output_json_does_not_contain_human_text(
        self, monkeypatch, mock_glue_connector, mock_dependencies, capsys
    ):
        """With output='json', find must NOT produce human-readable lines like
        'matching items:' or 'Wrote N items'."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        mock_glue_connector.limit.return_value.toJSON.return_value.collect.return_value = [
            '{"pk": "a"}'
        ]

        parsed_args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-003',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out

        # With output='json', there should be NO human-readable decorative text
        assert 'matching items:' not in output, \
            "With output='json', human-readable 'matching items:' should not appear"
        assert 'Wrote' not in output, \
            "With output='json', human-readable 'Wrote N items' should not appear"

    def test_count_with_output_json_produces_json_count(
        self, monkeypatch, mock_glue_connector, mock_dependencies, capsys
    ):
        """count command with output='json' must produce JSON with a count field."""
        mock_glue_connector.count.return_value = 42

        parsed_args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'count',
            'output': 'json',  # <-- structured output request
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)

        output = capsys.readouterr().out.strip()

        try:
            result = json.loads(output)
        except json.JSONDecodeError:
            pytest.fail(
                f"With output='json', count should produce valid JSON. "
                f"Got:\n{output[:500]}"
            )

        assert 'count' in result, "JSON output for count should have a 'count' key"
        assert result['count'] == 42
