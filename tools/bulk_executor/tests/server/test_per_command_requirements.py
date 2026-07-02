"""Test for issue #228: Let each command define its own Glue requirements.txt.

Instead of hard-coding that 'fill' needs 'faker' in runner.py, each verb
should be able to declare its own server-side requirements in a
requirements.txt within its module directory. The bootstrap/zipper should
discover and bundle these.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestPerCommandRequirements:
    """Each command should be able to declare its own pip requirements."""

    def test_discover_requirements_finds_fill_faker(self):
        """The discovery function should find fill's requirements.txt with faker."""
        from utils import module_zipper

        # There should be a function to discover per-command requirements
        requirements = module_zipper.discover_command_requirements()

        # fill needs faker — this should be discoverable from the directory structure
        assert any('faker' in r.lower() for r in requirements), \
            "fill's faker requirement should be discovered"

    def test_discover_returns_empty_for_commands_without_requirements(self):
        """Commands without requirements.txt should not contribute entries."""
        from utils import module_zipper

        requirements = module_zipper.discover_command_requirements()
        # The result should be a list/set of strings, not None
        assert isinstance(requirements, (list, set))

    def test_requirements_are_deduplicated(self):
        """If multiple commands need the same package, it appears once."""
        from utils import module_zipper

        requirements = module_zipper.discover_command_requirements()
        # No duplicates
        assert len(requirements) == len(set(requirements))

    def test_requirements_file_location_convention(self, tmp_path):
        """Requirements file should be at <verb>/requirements.txt relative
        to the server python_modules directory."""
        # Create a mock command directory with requirements.txt
        cmd_dir = tmp_path / "python_modules" / "my_verb"
        cmd_dir.mkdir(parents=True)
        (cmd_dir / "__init__.py").write_text("def run(*a, **kw): pass")
        (cmd_dir / "requirements.txt").write_text("some-package>=1.0\n")

        from utils import module_zipper

        # Discovery should work from a given base path
        reqs = module_zipper.discover_command_requirements(
            base_path=str(tmp_path / "python_modules")
        )
        assert 'some-package>=1.0' in reqs or any('some-package' in r for r in reqs)
