"""Failing tests for issue #180: Structured JSON output convention.

When parsed_args['output'] == 'json', the find verb's run() should emit
a single JSON object to stdout containing:
  - "count": the number of matching items
  - "items": list of the top-N items (as dicts)
  - "s3_location": the S3 path where all results were written

This tests the SERVER-SIDE behavior — the actual output format produced
by python_modules/find.py run() — not client-side argument parsing.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# find.py imports not covered by conftest
sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
_pyspark_sql_functions = MagicMock()
sys.modules.setdefault('pyspark.sql.functions', _pyspark_sql_functions)

from python_modules import find as find_module

# Star-imported from shared.errors
if not hasattr(find_module, 'get_error_message'):
    find_module.get_error_message = lambda e: str(e)


@pytest.fixture
def glue_connector_mock(monkeypatch):
    """Mock the glue_connector.read_dynamodb_dataframe used by find."""
    df = MagicMock()
    df.cache.return_value = df
    df.filter.return_value = df
    df.orderBy.return_value = df
    df.limit.return_value = df
    df.count.return_value = 3
    # toJSON().collect() returns JSON strings
    df.toJSON.return_value = MagicMock()
    df.toJSON.return_value.collect.return_value = [
        '{"pk": "a", "name": "Alice"}',
        '{"pk": "b", "name": "Bob"}',
        '{"pk": "c", "name": "Charlie"}',
    ]

    read_fn = MagicMock(return_value=df)
    monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', read_fn)
    return df


@pytest.fixture
def table_info_mocks(monkeypatch):
    """Mock table info helpers."""
    monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
        'item_count': 1000,
        'size_bytes': 50000,
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'WriteRequestUnits',
    }))
    monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.50))
    monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))


@pytest.fixture
def spark_session_mock(monkeypatch):
    """Mock SparkSession for find's S3 write path."""
    ss = MagicMock()
    monkeypatch.setattr(find_module, 'SparkSession', MagicMock(return_value=ss))
    return ss


class TestFindStructuredJsonOutput:
    """When output='json' is passed in parsed_args, find's run() emits
    a machine-parseable JSON object instead of human-readable text."""

    def test_json_output_emits_valid_json_object(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """With output='json', stdout should contain exactly one JSON object."""
        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        # Should parse as valid JSON
        result = json.loads(stdout)
        assert isinstance(result, dict), "Output should be a JSON object"

    def test_json_output_contains_count(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """The JSON output must include a 'count' field with the item count."""
        glue_connector_mock.count.return_value = 3

        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        result = json.loads(stdout)
        assert 'count' in result, "JSON output must include 'count'"
        assert result['count'] == 3

    def test_json_output_contains_items_list(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """The JSON output must include an 'items' list with parsed dicts."""
        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        result = json.loads(stdout)
        assert 'items' in result, "JSON output must include 'items'"
        assert isinstance(result['items'], list)
        assert len(result['items']) == 3
        assert result['items'][0]['pk'] == 'a'

    def test_json_output_contains_s3_location(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """The JSON output must include 's3_location' with the output path."""
        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        result = json.loads(stdout)
        assert 's3_location' in result, "JSON output must include 's3_location'"
        assert result['s3_location'] == 's3://my-bucket/output/run-123'

    def test_json_output_suppresses_human_readable_text(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """With output='json', no human-readable decorations like
        'matching items:' or '...and N more not printed' should appear."""
        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        assert 'matching items:' not in stdout
        assert 'more not printed' not in stdout
        assert 'Wrote' not in stdout

    def test_default_output_still_prints_human_readable(
        self, monkeypatch, table_info_mocks, glue_connector_mock,
        spark_session_mock, capsys
    ):
        """Without output='json', find should still emit the old-style text."""
        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-123',
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        stdout = capsys.readouterr().out
        # Human-readable output includes text like "matching items:"
        assert 'matching items:' in stdout or 'Wrote' in stdout
