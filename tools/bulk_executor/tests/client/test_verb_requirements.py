"""Failing tests for issue #228: Let each command define its own Glue requirements.txt.

Currently, third-party Python modules for Glue are hard-coded in
constants.py (_THIRD_PARTY_PYTHON_MODULES = ['faker']). This means
ALL Glue jobs install faker even when the command doesn't need it.

The desired behavior: each verb folder (e.g., server/src/python_modules/fill/)
can contain a `requirements.txt` that lists its dependencies. The bootstrap
discovers these files and aggregates them into the --additional-python-modules
argument. If a verb has no requirements.txt, it adds no extra deps.

This tests the SERVER-SIDE discovery mechanism — a function that scans
verb directories for requirements.txt files and returns the union of all
dependencies.
"""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure client/src is on sys.path
_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'client', 'src')
)
if _CLIENT_SRC not in sys.path:
    sys.path.insert(0, _CLIENT_SRC)


class TestDiscoverVerbRequirements:
    """Test the discovery of per-verb requirements.txt files.

    The infrastructure module should provide a function that:
    1. Scans server/src/python_modules/ for verb directories
    2. Looks for a requirements.txt in each verb directory
    3. Returns a deduplicated, sorted list of all pip dependencies
    """

    def test_discover_function_exists(self):
        """The infrastructure.constants module should expose a function
        to discover verb requirements dynamically."""
        from infrastructure.constants import discover_verb_requirements
        assert callable(discover_verb_requirements)

    def test_discovers_requirements_from_verb_directory(self, tmp_path):
        """A verb directory with requirements.txt should contribute its deps."""
        from infrastructure.constants import discover_verb_requirements

        # Create a fake verb directory with a requirements.txt
        verb_dir = tmp_path / "fill"
        verb_dir.mkdir()
        (verb_dir / "__init__.py").write_text("")
        (verb_dir / "requirements.txt").write_text("faker==19.0.0\n")

        result = discover_verb_requirements(str(tmp_path))
        assert 'faker==19.0.0' in result

    def test_ignores_directories_without_requirements(self, tmp_path):
        """A verb directory without requirements.txt adds nothing."""
        from infrastructure.constants import discover_verb_requirements

        # Create a verb directory WITHOUT requirements.txt
        verb_dir = tmp_path / "find"
        verb_dir.mkdir()
        (verb_dir / "__init__.py").write_text("")

        result = discover_verb_requirements(str(tmp_path))
        assert result == []

    def test_aggregates_from_multiple_verbs(self, tmp_path):
        """Multiple verbs with requirements.txt get their deps merged."""
        from infrastructure.constants import discover_verb_requirements

        # Verb "fill" needs faker
        fill_dir = tmp_path / "fill"
        fill_dir.mkdir()
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker\n")

        # Verb "export" needs pandas
        export_dir = tmp_path / "export"
        export_dir.mkdir()
        (export_dir / "__init__.py").write_text("")
        (export_dir / "requirements.txt").write_text("pandas>=2.0\n")

        result = discover_verb_requirements(str(tmp_path))
        assert 'faker' in result
        assert 'pandas>=2.0' in result

    def test_deduplicates_across_verbs(self, tmp_path):
        """Same dep from multiple verbs only appears once."""
        from infrastructure.constants import discover_verb_requirements

        fill_dir = tmp_path / "fill"
        fill_dir.mkdir()
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker\n")

        users_dir = tmp_path / "users"
        users_dir.mkdir()
        (users_dir / "__init__.py").write_text("")
        (users_dir / "requirements.txt").write_text("faker\n")

        result = discover_verb_requirements(str(tmp_path))
        assert result.count('faker') == 1

    def test_skips_blank_lines_and_comments(self, tmp_path):
        """requirements.txt comments (#) and blank lines are ignored."""
        from infrastructure.constants import discover_verb_requirements

        verb_dir = tmp_path / "fill"
        verb_dir.mkdir()
        (verb_dir / "__init__.py").write_text("")
        (verb_dir / "requirements.txt").write_text(
            "# This is a comment\n"
            "\n"
            "faker\n"
            "  \n"
            "# Another comment\n"
        )

        result = discover_verb_requirements(str(tmp_path))
        assert result == ['faker']

    def test_skips_shared_directory(self, tmp_path):
        """The 'shared' directory should not be treated as a verb."""
        from infrastructure.constants import discover_verb_requirements

        # Shared has requirements.txt but isn't a verb
        shared_dir = tmp_path / "shared"
        shared_dir.mkdir()
        (shared_dir / "__init__.py").write_text("")
        (shared_dir / "requirements.txt").write_text("boto3\n")

        result = discover_verb_requirements(str(tmp_path))
        assert 'boto3' not in result

    def test_returns_sorted_list(self, tmp_path):
        """Dependencies should be returned in sorted order."""
        from infrastructure.constants import discover_verb_requirements

        verb_dir = tmp_path / "fill"
        verb_dir.mkdir()
        (verb_dir / "__init__.py").write_text("")
        (verb_dir / "requirements.txt").write_text("zebra\nalpha\nmango\n")

        result = discover_verb_requirements(str(tmp_path))
        assert result == sorted(result)


class TestThirdPartyModulesUsesDiscovery:
    """The THIRD_PARTY_PYTHON_MODULES constant should use discover_verb_requirements
    rather than hard-coding the list."""

    def test_third_party_modules_includes_fill_faker(self):
        """fill's requirements.txt declares faker, so THIRD_PARTY_PYTHON_MODULES
        should still include faker (backwards compat with current behavior)."""
        from infrastructure.constants import THIRD_PARTY_PYTHON_MODULES
        assert 'faker' in THIRD_PARTY_PYTHON_MODULES

    def test_fill_verb_has_requirements_txt(self):
        """The fill verb directory should contain a requirements.txt file."""
        fill_dir = Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules" / "fill"
        requirements_file = fill_dir / "requirements.txt"
        assert requirements_file.exists(), (
            f"Expected requirements.txt at {requirements_file}. "
            "Each verb that needs pip packages should define its own requirements.txt."
        )

    def test_fill_requirements_txt_contains_faker(self):
        """fill/requirements.txt should list faker as a dependency."""
        fill_dir = Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules" / "fill"
        requirements_file = fill_dir / "requirements.txt"
        if requirements_file.exists():
            content = requirements_file.read_text()
            assert 'faker' in content.lower(), (
                "fill/requirements.txt should declare faker as a dependency"
            )
        else:
            pytest.fail("fill/requirements.txt does not exist yet")
