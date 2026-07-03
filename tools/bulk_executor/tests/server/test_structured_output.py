"""Failing tests for issue #180: Structured output convention across commands.

Commands should support an --output parameter (like AWS CLI) that controls the
format of the output. When --output json is specified, the command should emit
structured JSON instead of human-readable text. This enables piping between
commands (e.g., `find --output json | load`).

Tests the server-side diff.run() function: when output='json' is in parsed_args,
the diff output should be valid JSON containing structured data about the
differences found.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import diff as diff_module


class TestStructuredOutput:
    """Commands should support --output json for machine-readable output."""

    def _base_args(self):
        return {
            'splits': '4',
            'sample_fraction': '1.0',
            'table': 'table1',
            'table2': 'table2',
            'format': 'keys',
            's3': None,
            'JOB_RUN_ID': 'job-1',
            's3-bucket-name': 'bucket',
        }

    def _setup_mocks(self, monkeypatch):
        monkeypatch.setattr(diff_module, 'print_dynamodb_table_info', MagicMock(return_value=0.10))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(diff_module, 'boto3', MagicMock(
            client=MagicMock(return_value=client_mock)
        ))

        monkeypatch.setattr(diff_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock())
        monkeypatch.setattr(diff_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

    def test_json_output_produces_valid_json(self, monkeypatch, capsys):
        """When output='json', the printed output should be valid JSON."""
        self._setup_mocks(monkeypatch)
        args = self._base_args()
        args['output'] = 'json'

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        # Simulate finding some diffs
        diffs = ['- {"pk": {"S": "a"}}', '+ {"pk": {"S": "b"}}']
        rdd.map.return_value.collect.return_value = [diffs]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out.strip()

        # The output should be parseable as JSON
        try:
            parsed = json.loads(out)
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON when --output json is set:\n{out}")

        # The JSON should contain difference information
        assert isinstance(parsed, (dict, list)), f"Expected dict/list, got {type(parsed)}"

    def test_json_output_contains_diff_count(self, monkeypatch, capsys):
        """JSON output should include the total count of differences."""
        self._setup_mocks(monkeypatch)
        args = self._base_args()
        args['output'] = 'json'

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        diffs = ['- {"pk": {"S": "a"}}', '+ {"pk": {"S": "b"}}']
        rdd.map.return_value.collect.return_value = [diffs]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out.strip()

        parsed = json.loads(out)
        # Should have a count or total field
        if isinstance(parsed, dict):
            assert 'count' in parsed or 'total' in parsed or 'differences' in parsed, \
                f"JSON output missing count/total/differences field: {parsed}"

    def test_no_output_flag_defaults_to_text(self, monkeypatch, capsys):
        """Without --output, behavior is unchanged (human-readable text)."""
        self._setup_mocks(monkeypatch)
        args = self._base_args()
        # No 'output' key in args — default behavior

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[]]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out

        # Default text output should NOT be JSON
        assert 'No differences found' in out
