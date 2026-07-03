"""Failing tests for issue #139: Consider breaking `load` command into `load-*` versions.

The proposal is to have `load-csv`, `load-json`, `load-parquet` as distinct
command verbs that match the existing `load-export` pattern. The verbs should
route to the same server-side load module but with the format pre-determined.

This test validates:
1. The server-side routing recognizes `load_csv`, `load_json`, `load_parquet` modules
2. Each subcommand auto-sets the --format parameter
3. Each has a run() function conforming to the standard verb interface
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# awsglue.transforms is imported by load/__init__.py at collection time.
# The conftest mocks 'awsglue' as a Mock() but doesn't register the
# submodule path 'awsglue.transforms'. We must do so before importing load.
if 'awsglue.transforms' not in sys.modules:
    sys.modules['awsglue.transforms'] = MagicMock()


class TestLoadSubcommandRouting:
    """load-csv, load-json, load-parquet should exist as server-side verb modules."""

    def test_load_csv_module_exists(self):
        """A load_csv module should exist for the 'load-csv' command.

        When the user runs `bulk load-csv`, root.py maps 'load-csv' to
        python_modules.load_csv (replacing - with _). The module must exist.
        """
        try:
            from python_modules import load_csv
        except ImportError:
            pytest.fail("python_modules.load_csv module does not exist. "
                       "Issue #139 requires load-csv, load-json, load-parquet subcommands.")

    def test_load_json_module_exists(self):
        """A load_json module should exist for the 'load-json' command."""
        try:
            from python_modules import load_json
        except ImportError:
            pytest.fail("python_modules.load_json module does not exist. "
                       "Issue #139 requires load-csv, load-json, load-parquet subcommands.")

    def test_load_parquet_module_exists(self):
        """A load_parquet module should exist for the 'load-parquet' command."""
        try:
            from python_modules import load_parquet
        except ImportError:
            pytest.fail("python_modules.load_parquet module does not exist. "
                       "Issue #139 requires load-csv, load-json, load-parquet subcommands.")

    def test_load_csv_has_run_function(self):
        """load_csv module must have a run() function."""
        from python_modules import load_csv
        assert hasattr(load_csv, 'run'), "load_csv module must have a run() function"

    def test_load_json_has_run_function(self):
        """load_json module must have a run() function."""
        from python_modules import load_json
        assert hasattr(load_json, 'run'), "load_json module must have a run() function"

    def test_load_parquet_has_run_function(self):
        """load_parquet module must have a run() function."""
        from python_modules import load_parquet
        assert hasattr(load_parquet, 'run'), "load_parquet module must have a run() function"


class TestLoadSubcommandBehavior:
    """The load-* subcommands should inject the correct format into parsed_args."""

    def test_load_csv_injects_format(self, monkeypatch):
        """load_csv.run() should ensure format='csv' is set in parsed_args."""
        from python_modules import load_csv
        from python_modules import load as load_module

        # Track calls to the real load module's run function
        captured_args = {}
        original_run = load_module.run

        def spy_run(job, spark_context, glue_context, parsed_args):
            captured_args.update(parsed_args)

        monkeypatch.setattr(load_module, 'run', spy_run)

        parsed_args = {
            'table': 'my-table',
            's3_path': 's3://bucket/data.csv',
            # No 'format' key - load_csv should inject it
        }

        load_csv.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)
        assert captured_args.get('format') == 'csv', \
            f"load_csv should inject format='csv', got: {captured_args.get('format')}"

    def test_load_json_injects_format(self, monkeypatch):
        """load_json.run() should ensure format='json' is set in parsed_args."""
        from python_modules import load_json
        from python_modules import load as load_module

        captured_args = {}

        def spy_run(job, spark_context, glue_context, parsed_args):
            captured_args.update(parsed_args)

        monkeypatch.setattr(load_module, 'run', spy_run)

        parsed_args = {
            'table': 'my-table',
            's3_path': 's3://bucket/data.json',
        }

        load_json.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)
        assert captured_args.get('format') == 'json', \
            f"load_json should inject format='json', got: {captured_args.get('format')}"

    def test_load_parquet_injects_format(self, monkeypatch):
        """load_parquet.run() should ensure format='parquet' is set in parsed_args."""
        from python_modules import load_parquet
        from python_modules import load as load_module

        captured_args = {}

        def spy_run(job, spark_context, glue_context, parsed_args):
            captured_args.update(parsed_args)

        monkeypatch.setattr(load_module, 'run', spy_run)

        parsed_args = {
            'table': 'my-table',
            's3_path': 's3://bucket/data.parquet',
        }

        load_parquet.run(MagicMock(), MagicMock(), MagicMock(), parsed_args)
        assert captured_args.get('format') == 'parquet', \
            f"load_parquet should inject format='parquet', got: {captured_args.get('format')}"
