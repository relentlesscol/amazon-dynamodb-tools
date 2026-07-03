"""Unit tests for per-verb requirements.txt resolution (issue #228).

Instead of hard-coding _THIRD_PARTY_PYTHON_MODULES = ['faker'] in constants.py,
each verb should be able to define its own `requirements.txt` in its folder.
The bootstrap process should discover these files and assemble the combined
`--additional-python-modules` value from them.

This tests the OBSERVABLE BEHAVIOR: that the THIRD_PARTY_PYTHON_MODULES
constant (or its replacement function) reads per-verb requirements.txt files
from the server/src/python_modules/<verb>/ directories and produces the
correct comma-separated module list.
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, 'client/src')


class TestPerVerbRequirements:
    """The system should discover requirements.txt files in each verb's
    directory and aggregate them into the Glue --additional-python-modules value."""

    def test_fill_verb_requirements_discovered(self, tmp_path):
        """A requirements.txt in the fill verb folder should be discovered
        and its contents included in the modules list."""
        # Set up a fake python_modules tree
        fill_dir = tmp_path / "server" / "src" / "python_modules" / "fill"
        fill_dir.mkdir(parents=True)
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker\n")

        # Other verbs without requirements
        count_dir = tmp_path / "server" / "src" / "python_modules" / "count.py"
        count_dir.write_text("def run(): pass")

        from infrastructure import constants

        # The function/mechanism that resolves per-verb requirements
        # should exist and return the right thing
        assert hasattr(constants, 'get_third_party_modules') or \
               hasattr(constants, 'discover_verb_requirements'), \
            "constants module must have a function to discover per-verb requirements " \
            "(get_third_party_modules or discover_verb_requirements)"

        # Call whichever function exists
        if hasattr(constants, 'get_third_party_modules'):
            result = constants.get_third_party_modules(str(tmp_path / "server" / "src" / "python_modules"))
        else:
            result = constants.discover_verb_requirements(str(tmp_path / "server" / "src" / "python_modules"))

        assert 'faker' in result, \
            f"fill/requirements.txt contains 'faker' but got: {result}"

    def test_multiple_verbs_requirements_aggregated(self, tmp_path):
        """Requirements from multiple verbs should be combined (deduplicated)."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"

        # fill needs faker
        fill_dir = modules_dir / "fill"
        fill_dir.mkdir(parents=True)
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker\n")

        # hypothetical verb needing requests
        custom_dir = modules_dir / "custom_verb"
        custom_dir.mkdir(parents=True)
        (custom_dir / "__init__.py").write_text("")
        (custom_dir / "requirements.txt").write_text("requests>=2.28\n")

        from infrastructure import constants

        if hasattr(constants, 'get_third_party_modules'):
            result = constants.get_third_party_modules(str(modules_dir))
        else:
            result = constants.discover_verb_requirements(str(modules_dir))

        assert 'faker' in result
        assert 'requests' in result

    def test_verbs_without_requirements_contribute_nothing(self, tmp_path):
        """Verb directories without requirements.txt are silently skipped."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"

        # count has no requirements
        count_dir = modules_dir / "count"
        count_dir.mkdir(parents=True)
        (count_dir / "__init__.py").write_text("def run(): pass")

        # fill has requirements
        fill_dir = modules_dir / "fill"
        fill_dir.mkdir(parents=True)
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker\n")

        from infrastructure import constants

        if hasattr(constants, 'get_third_party_modules'):
            result = constants.get_third_party_modules(str(modules_dir))
        else:
            result = constants.discover_verb_requirements(str(modules_dir))

        # Should have faker but not crash
        assert 'faker' in result

    def test_empty_requirements_file_contributes_nothing(self, tmp_path):
        """An empty requirements.txt should not add empty strings."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"

        empty_verb_dir = modules_dir / "empty_verb"
        empty_verb_dir.mkdir(parents=True)
        (empty_verb_dir / "__init__.py").write_text("")
        (empty_verb_dir / "requirements.txt").write_text("")

        from infrastructure import constants

        if hasattr(constants, 'get_third_party_modules'):
            result = constants.get_third_party_modules(str(modules_dir))
        else:
            result = constants.discover_verb_requirements(str(modules_dir))

        # Should not contain empty strings or be malformed
        if isinstance(result, str):
            assert ',,' not in result, "Empty requirements should not create double commas"
            assert not result.startswith(','), "Should not start with comma"
            assert not result.endswith(','), "Should not end with comma"
        elif isinstance(result, list):
            assert '' not in result, "Empty strings should not be in the list"
