"""Failing tests for issues #138/#139: Command-oriented folder structure and load-* variants.

Issue #138: Commands should be discoverable from a commands/CMDNAME/ folder structure.
The root.py module resolution should support loading verbs from both the current
flat `python_modules/CMDNAME` layout AND a future `commands/CMDNAME/server/` layout.

Issue #139: The `load` command should support variant naming like `load-csv`,
`load-json`, `load-parquet`. The verb dispatcher (root.py) already does
`replace('-', '_')` which handles `load-csv` → `load_csv`, but there's no
actual `load_csv` module — the verb resolution should map load variants to
the `load` module with appropriate format parameters.

These tests exercise the server-side root module's verb routing logic.
"""

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('awsglue.context', MagicMock())
sys.modules.setdefault('awsglue.job', MagicMock())
sys.modules.setdefault('pyspark.context', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())


class TestCommandDiscovery:
    """The verb dispatcher should discover commands from a structured layout."""

    def test_load_csv_verb_resolves_to_module(self):
        """The verb 'load-csv' should resolve to a loadable module.

        root.py does: action_module = parsed_args.get('XAction').replace('-', '_')
        So 'load-csv' becomes 'load_csv'. There should be a python_modules.load_csv
        module OR the dispatcher should map it to 'load' with format='csv'.
        """
        action = 'load-csv'
        module_name = action.replace('-', '_')  # load_csv

        # Try to import it — this should work after issue #139 is implemented
        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run'), \
                f"Module python_modules.{module_name} exists but has no run() function"
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Issue #139 requires 'load-csv' to resolve to a loadable module."
            )

    def test_load_json_verb_resolves_to_module(self):
        """The verb 'load-json' should resolve to a loadable module."""
        action = 'load-json'
        module_name = action.replace('-', '_')

        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run')
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Issue #139 requires 'load-json' to resolve to a loadable module."
            )

    def test_load_parquet_verb_resolves_to_module(self):
        """The verb 'load-parquet' should resolve to a loadable module."""
        action = 'load-parquet'
        module_name = action.replace('-', '_')

        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run')
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Issue #139 requires 'load-parquet' to resolve to a loadable module."
            )

    def test_load_export_verb_still_works(self):
        """Existing 'load-export' verb (load_export module) should still work."""
        action = 'load-export'
        module_name = action.replace('-', '_')  # load_export

        # This should already work — it's the existing behavior
        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run')
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Existing 'load-export' verb should still resolve."
            )


class TestNativeImportExportCommands:
    """Issue #176: Wrap native DynamoDB import/export S3 as commands.

    There should be an 'import-s3' and 'export-s3' command that wraps
    DynamoDB's native ImportTable/ExportTableToPointInTime APIs.
    """

    def test_export_s3_verb_resolves_to_module(self):
        """The verb 'export-s3' should resolve to a loadable server module."""
        action = 'export-s3'
        module_name = action.replace('-', '_')  # export_s3

        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run'), \
                f"Module python_modules.{module_name} exists but has no run() function"
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Issue #176 requires 'export-s3' command to wrap native DynamoDB export."
            )

    def test_import_s3_verb_resolves_to_module(self):
        """The verb 'import-s3' should resolve to a loadable server module."""
        action = 'import-s3'
        module_name = action.replace('-', '_')  # import_s3

        try:
            module = importlib.import_module(f'python_modules.{module_name}')
            assert hasattr(module, 'run'), \
                f"Module python_modules.{module_name} exists but has no run() function"
        except ImportError:
            pytest.fail(
                f"Cannot import python_modules.{module_name}. "
                f"Issue #176 requires 'import-s3' command to wrap native DynamoDB import."
            )
