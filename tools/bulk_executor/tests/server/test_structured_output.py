"""Test for issue #180: Structured output convention across commands (JSON).

Commands should support an --output flag (like AWS CLI) that produces
machine-readable JSON output instead of human-readable text. This enables
piping between commands (e.g., find output -> load input).
"""

import json
from unittest.mock import MagicMock

import pytest

from python_modules import find as find_module


class TestStructuredOutput:
    """Commands should support --output json for structured results."""

    def test_find_with_json_output_produces_parseable_json(self, monkeypatch, capsys):
        """When --output json is specified, find should output a JSON object
        with machine-readable fields (s3_location, count, etc.)."""
        # Set up find's environment
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock())
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock())
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        df = MagicMock()
        df.count.return_value = 3
        df.cache.return_value = df
        df.limit.return_value = df
        df.toJSON.return_value = MagicMock(collect=MagicMock(return_value=['{"pk":"a"}']))
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        spark_context = MagicMock()
        spark_session = MagicMock()
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock(return_value=spark_session))

        args = {
            'table': 'my-table',
            'XAction': 'find',
            'splits': '10',
            'output': 'json',  # <-- the new flag
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        find_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out

        # Output should be valid JSON
        result = json.loads(out.strip().split('\n')[-1])  # last line should be JSON
        assert 's3_location' in result or 'count' in result

    def test_find_without_output_flag_uses_text(self, monkeypatch, capsys):
        """Without --output flag, existing text output is preserved."""
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock())
        monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock())
        monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(find_module, 'RateLimiterAggregator', MagicMock())

        df = MagicMock()
        df.count.return_value = 0
        df.cache.return_value = df
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'table': 'my-table',
            'XAction': 'find',
            'splits': '10',
            # No 'output' key
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        out = capsys.readouterr().out

        # Output should NOT be JSON (should be human-readable text)
        with pytest.raises(json.JSONDecodeError):
            json.loads(out.strip())
