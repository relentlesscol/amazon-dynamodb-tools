"""Failing tests for issue #228: Let each command define its own Glue `requirements.txt`.

Currently, third-party dependencies like `faker` are hard-coded in
infrastructure/constants.py as THIRD_PARTY_PYTHON_MODULES. Instead,
each command verb should be able to define its own requirements.txt
in its module directory, and the system should discover and merge these.

For example, `server/src/python_modules/fill/requirements.txt` would contain
`faker` and only be included when the fill command is being run.
"""

import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _load_real_module_zipper():
    """Load the REAL module_zipper module (not through conftest mocks)."""
    module_zipper_path = Path(__file__).resolve().parents[2] / "client" / "src" / "utils" / "module_zipper.py"
    spec = importlib.util.spec_from_file_location("module_zipper_real", str(module_zipper_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestPerCommandRequirements:
    """Each command verb should declare its own requirements.txt."""

    def test_discover_verb_requirements_function_exists(self):
        """module_zipper should have a discover_verb_requirements function."""
        module_zipper = _load_real_module_zipper()
        assert hasattr(module_zipper, 'discover_verb_requirements'), \
            "module_zipper must have a discover_verb_requirements function"

    def test_discovers_requirements_from_verb_directory(self, tmp_path):
        """A requirements.txt in a verb's folder should be discovered."""
        module_zipper = _load_real_module_zipper()

        # Create a mock python_modules structure
        modules_dir = tmp_path / "python_modules"
        modules_dir.mkdir()

        # fill/ has a requirements.txt
        fill_dir = modules_dir / "fill"
        fill_dir.mkdir()
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker==24.0.0\n")

        # load/ has no requirements.txt
        load_dir = modules_dir / "load"
        load_dir.mkdir()
        (load_dir / "__init__.py").write_text("")

        requirements = module_zipper.discover_verb_requirements(str(modules_dir), verb='fill')
        assert 'faker' in requirements or 'faker==24.0.0' in requirements

    def test_returns_empty_for_verb_without_requirements(self, tmp_path):
        """Verbs without requirements.txt should return empty list."""
        module_zipper = _load_real_module_zipper()

        modules_dir = tmp_path / "python_modules"
        modules_dir.mkdir()
        load_dir = modules_dir / "load"
        load_dir.mkdir()
        (load_dir / "__init__.py").write_text("")

        requirements = module_zipper.discover_verb_requirements(str(modules_dir), verb='load')
        assert requirements == [] or requirements == ''

    def test_discovers_requirements_for_single_file_verb(self, tmp_path):
        """Verbs that are single .py files (not directories) should also
        support requirements via a sibling file or other convention."""
        module_zipper = _load_real_module_zipper()

        modules_dir = tmp_path / "python_modules"
        modules_dir.mkdir()
        # diff.py is a single-file verb
        (modules_dir / "diff.py").write_text("# diff command")
        # requirements could be diff.requirements.txt or similar convention
        (modules_dir / "diff.requirements.txt").write_text("some-package\n")

        requirements = module_zipper.discover_verb_requirements(str(modules_dir), verb='diff')
        assert 'some-package' in requirements

    def test_all_verb_requirements_merged_when_no_specific_verb(self, tmp_path):
        """When building for all commands, all requirements.txt files are merged."""
        module_zipper = _load_real_module_zipper()

        modules_dir = tmp_path / "python_modules"
        modules_dir.mkdir()

        fill_dir = modules_dir / "fill"
        fill_dir.mkdir()
        (fill_dir / "__init__.py").write_text("")
        (fill_dir / "requirements.txt").write_text("faker==24.0.0\n")

        update_dir = modules_dir / "update"
        update_dir.mkdir()
        (update_dir / "__init__.py").write_text("")
        (update_dir / "requirements.txt").write_text("jmespath\n")

        # When verb=None, merge all
        requirements = module_zipper.discover_verb_requirements(str(modules_dir), verb=None)
        assert 'faker' in str(requirements)
        assert 'jmespath' in str(requirements)
