"""Unit tests for structured output format support (issue #180).

Issue #180 asks for an --output flag (json, text, table) across commands so
that output is machine-parsable and commands can be piped together.

These tests verify that when parsed_args includes output='json', the server-
side verb modules produce valid JSON output instead of human-readable text.
The key behavior change: with output='json', the find verb should emit a
JSON object containing the results metadata (count, s3_location, items)
rather than scattered print() lines.
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

# Star-imported from shared.errors — Mock doesn't populate __all__ so we inject it
if not hasattr(find_module, 'get_error_message'):
    find_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers in find's namespace."""
    mocks = MagicMock()
    mocks.get_and_print_dynamodb_table_info = MagicMock(return_value={
        'item_count': 1000,
        'size_bytes': 50000,
        'region_name': 'us-east-1',
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'WriteRequestUnits',
    })
    mocks.get_and_print_table_scan_cost = MagicMock(return_value=0.50)
    mocks.get_dynamodb_throughput_configs = MagicMock(return_value={'throughput': 'val'})

    monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info',
                        mocks.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost',
                        mocks.get_and_print_table_scan_cost)
    monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs',
                        mocks.get_dynamodb_throughput_configs)
    return mocks


@pytest.fixture
def boto3_session_mock(monkeypatch):
    """Mock boto3.Session() used for region_name."""
    session = MagicMock()
    session.region_name = 'us-west-2'
    session_cls = MagicMock(return_value=session)
    monkeypatch.setattr(find_module, 'boto3', MagicMock(Session=session_cls, client=MagicMock()))
    return session_cls


@pytest.fixture
def glue_context_with_records():
    """Mock GlueContext whose DataFrame returns JSON records for the find path."""
    ctx = MagicMock()
    df = MagicMock()
    df.filter.return_value = df
    df.orderBy.return_value = df
    df.limit.return_value = df
    df.count.return_value = 3
    df.cache.return_value = df
    df.select.return_value = df
    df.repartition.return_value = df

    # These are the items the "find" would return
    sample_records = [
        '{"pk": "user1", "name": "Alice", "age": 30}',
        '{"pk": "user2", "name": "Bob", "age": 25}',
        '{"pk": "user3", "name": "Carol", "age": 35}',
    ]
    df.toJSON.return_value = MagicMock()
    df.limit.return_value.toJSON.return_value.collect.return_value = sample_records

    ctx.create_dynamic_frame.from_options.return_value = df
    return ctx, df, sample_records


# --- Tests: Structured JSON output for find -----------------------------------


class TestStructuredOutputJsonFind:
    """When parsed_args['output'] == 'json', find.run() should produce a single
    valid JSON object on stdout containing the result metadata and items,
    instead of human-readable text lines."""

    def test_find_with_output_json_produces_valid_json(
        self, monkeypatch, table_info_mocks, boto3_session_mock,
        glue_context_with_records, capsys
    ):
        """The core behavior: output='json' makes find emit parsable JSON."""
        ctx, df, sample_records = glue_context_with_records
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())

        # read_dynamodb_dataframe returns our mock df
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-abc',
            'output': 'json',  # <-- THE NEW FLAG
        }

        find_module.run(MagicMock(), MagicMock(), ctx, args)

        captured = capsys.readouterr()
        stdout = captured.out.strip()

        # The output MUST be valid JSON
        result = json.loads(stdout)

        # The JSON result must contain the S3 output location
        assert 's3_location' in result or 'output_location' in result or 's3' in str(result).lower(), \
            "JSON output must include the S3 output location for piping to other commands"

        # The JSON result must contain the item count
        assert result.get('count') == 3 or result.get('item_count') == 3, \
            "JSON output must include the count of matched items"

    def test_find_with_output_json_includes_items(
        self, monkeypatch, table_info_mocks, boto3_session_mock,
        glue_context_with_records, capsys
    ):
        """JSON output includes the found items so downstream commands can use them."""
        ctx, df, sample_records = glue_context_with_records
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-abc',
            'output': 'json',
        }

        find_module.run(MagicMock(), MagicMock(), ctx, args)

        captured = capsys.readouterr()
        result = json.loads(captured.out.strip())

        # Items should be included as a list of dicts
        items = result.get('items', result.get('records', []))
        assert len(items) == 3, "All found items should be in the JSON output"
        assert items[0]['pk'] == 'user1'
        assert items[1]['name'] == 'Bob'

    def test_find_without_output_flag_produces_human_readable_text(
        self, monkeypatch, table_info_mocks, boto3_session_mock,
        glue_context_with_records, capsys
    ):
        """Without output flag (default), find produces human-readable text (not JSON)."""
        ctx, df, sample_records = glue_context_with_records
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'splits': '100',
            'table': 'test-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'find',
            's3-bucket-name': 'my-bucket',
            'JOB_RUN_ID': 'run-abc',
            # No 'output' key — default behavior
        }

        find_module.run(MagicMock(), MagicMock(), ctx, args)

        captured = capsys.readouterr()
        stdout = captured.out

        # Default output should NOT be a single JSON object (it's human-readable text)
        # It should contain the familiar "matching items:" text
        assert 'matching items' in stdout.lower(), \
            "Default output (no --output flag) should be human-readable text"


class TestStructuredOutputJsonCount:
    """When output='json' and XAction='count', the count result should be
    emitted as structured JSON rather than a human-readable string."""

    def test_count_with_output_json_produces_json_with_count(
        self, monkeypatch, table_info_mocks, boto3_session_mock, capsys
    ):
        """count verb with output='json' emits {"count": N} as valid JSON."""
        df = MagicMock()
        df.count.return_value = 42
        df.filter.return_value = df
        df.cache.return_value = df

        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'splits': '200',
            'table': 'my-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'count',
            'output': 'json',  # <-- THE NEW FLAG
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        captured = capsys.readouterr()
        stdout = captured.out.strip()

        # Must be valid JSON
        result = json.loads(stdout)

        # Must contain the count
        assert result.get('count') == 42, \
            "JSON output for count must include the numeric count value"

    def test_count_without_output_flag_prints_human_text(
        self, monkeypatch, table_info_mocks, boto3_session_mock, capsys
    ):
        """count verb without output flag prints 'Count of matching items: N'."""
        df = MagicMock()
        df.count.return_value = 42
        df.filter.return_value = df
        df.cache.return_value = df

        monkeypatch.setattr(find_module, 'read_dynamodb_dataframe', MagicMock(return_value=df))

        args = {
            'splits': '200',
            'table': 'my-table',
            'where': None,
            'orderby': None,
            'limit': None,
            'XAction': 'count',
            # No 'output' key
        }

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        captured = capsys.readouterr()
        assert 'Count of matching items' in captured.out or '42' in captured.out, \
            "Default count output should be human-readable text"
