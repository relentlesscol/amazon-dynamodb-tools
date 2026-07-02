"""Test for issue #138: Command-oriented folder structure.

The system should be able to discover available commands dynamically
from the folder structure, rather than requiring manual registration.
This enables "install a command by dropping a folder in place."

Also covers issue #139: breaking load into load-* versions. The verb
resolution in root.py must support hyphenated verb names that map to
module paths (e.g., 'load-csv' -> load_csv or load/csv).
"""

from unittest.mock import MagicMock, patch

import pytest

from python_modules.shared import bulk_executor_error  # available per conftest


class TestVerbDiscovery:
    """The server root should dynamically discover available verbs."""

    def test_hyphenated_verb_resolves_to_module(self):
        """Verb names like 'load-csv' should resolve despite hyphens."""
        # root.py already does action_module.replace('-', '_')
        # This test ensures that works for multi-part verb names
        import importlib
        import sys

        # The root.py logic:
        verb = 'load-export'
        module_name = f"python_modules.{verb.replace('-', '_')}"

        # This should import successfully (load_export already exists)
        module = importlib.import_module(module_name)
        assert hasattr(module, 'run'), \
            f"Module {module_name} must have a run() function"

    def test_nonexistent_verb_raises_clear_error(self):
        """An unknown verb should raise a clear error message."""
        import importlib

        verb = 'nonexistent-verb-xyz'
        module_name = f"python_modules.{verb.replace('-', '_')}"

        with pytest.raises(ImportError):
            importlib.import_module(module_name)

    def test_list_available_commands(self):
        """There should be a way to discover all available commands."""
        # This is the new behavior: a function that lists available verbs
        try:
            from python_modules import list_commands
            commands = list_commands()
        except (ImportError, AttributeError):
            # Try alternative location
            try:
                from python_modules.shared import command_registry
                commands = command_registry.list_commands()
            except (ImportError, AttributeError):
                pytest.fail(
                    "No command discovery mechanism exists. "
                    "Need python_modules.list_commands() or "
                    "python_modules.shared.command_registry.list_commands()"
                )

        # Should include known verbs
        assert 'copy' in commands
        assert 'diff' in commands
        assert 'find' in commands or 'count' in commands


class TestLoadVerbVariants:
    """Issue #139: load should be accessible as load-csv, load-json, etc."""

    def test_load_csv_verb_exists(self):
        """'load-csv' should resolve to a loadable module with run()."""
        import importlib
        try:
            module = importlib.import_module("python_modules.load_csv")
        except ImportError:
            # Alternative: load module accepts a format parameter
            module = importlib.import_module("python_modules.load")
            # At minimum, the load module must support format='csv'
            # We test this indirectly — if load_csv doesn't exist as
            # a separate module, the load module should handle format routing
            assert hasattr(module, 'run')
            # The test fails because load_csv module doesn't exist yet
            pytest.fail(
                "python_modules.load_csv does not exist — "
                "load command should be broken into load-csv, load-json, load-parquet"
            )

    def test_load_json_verb_exists(self):
        """'load-json' should resolve to a loadable module with run()."""
        import importlib
        try:
            module = importlib.import_module("python_modules.load_json")
            assert hasattr(module, 'run')
        except ImportError:
            pytest.fail("python_modules.load_json does not exist")

    def test_load_parquet_verb_exists(self):
        """'load-parquet' should resolve to a loadable module with run()."""
        import importlib
        try:
            module = importlib.import_module("python_modules.load_parquet")
            assert hasattr(module, 'run')
        except ImportError:
            pytest.fail("python_modules.load_parquet does not exist")
