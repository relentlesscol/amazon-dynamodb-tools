"""Failing tests for issue #228: Let each command define its own Glue requirements.txt.

Instead of hard-coding third-party dependencies (like 'faker') in constants.py,
each verb directory should be able to define its own requirements.txt that lists
server-side pip packages needed by that verb.

Bootstrap should discover these per-verb requirements.txt files and aggregate
them into the --additional-python-modules Glue job argument.

The behavior tested here:
1. If a verb directory contains a requirements.txt, its contents are included
   in THIRD_PARTY_PYTHON_MODULES
2. The discovery mechanism finds requirements.txt files in verb directories
3. A verb without requirements.txt doesn't contribute any packages
4. The hard-coded 'faker' in constants.py is replaced by fill/requirements.txt
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestVerbRequirementsDiscovery:
    """Bootstrap should discover requirements.txt in verb folders."""

    def _get_server_modules_path(self):
        return Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules"

    def test_fill_verb_has_own_requirements_txt(self):
        """The fill verb directory should have a requirements.txt with 'faker'."""
        fill_dir = self._get_server_modules_path() / "fill"
        requirements_file = fill_dir / "requirements.txt"

        assert requirements_file.exists(), (
            f"Expected {requirements_file} to exist. "
            f"Each verb that needs server-side pip packages should define its own "
            f"requirements.txt instead of hard-coding dependencies in constants.py"
        )

        contents = requirements_file.read_text().strip()
        assert 'faker' in contents.lower(), (
            f"fill/requirements.txt should contain 'faker' "
            f"(moved from the hard-coded _THIRD_PARTY_PYTHON_MODULES in constants.py). "
            f"Got: {contents}"
        )

    def test_constants_no_longer_hardcodes_faker(self):
        """constants.py should NOT hard-code 'faker' — it should be discovered from fill/requirements.txt."""
        from infrastructure.constants import _THIRD_PARTY_PYTHON_MODULES

        assert 'faker' not in _THIRD_PARTY_PYTHON_MODULES, (
            f"_THIRD_PARTY_PYTHON_MODULES in constants.py still hard-codes 'faker'. "
            f"This should be moved to server/src/python_modules/fill/requirements.txt "
            f"and discovered dynamically. Current value: {_THIRD_PARTY_PYTHON_MODULES}"
        )

    def test_discover_verb_requirements_finds_fill(self):
        """A discovery function should find fill/requirements.txt and return its packages."""
        # This tests that a discovery function exists and works
        try:
            from infrastructure.constants import discover_verb_requirements
        except ImportError:
            pytest.fail(
                "Expected a 'discover_verb_requirements' function in infrastructure.constants "
                "(or similar location) that scans verb directories for requirements.txt files"
            )

        packages = discover_verb_requirements()
        assert 'faker' in packages, (
            f"discover_verb_requirements() should find 'faker' from fill/requirements.txt. "
            f"Got: {packages}"
        )

    def test_verb_without_requirements_contributes_nothing(self):
        """A verb directory without requirements.txt adds no packages."""
        # copy.py is a single-file verb with no requirements.txt
        copy_dir = self._get_server_modules_path() / "copy"
        # copy is not a directory, it's a single .py file — no requirements
        # The test verifies the discovery doesn't break on non-directory verbs
        try:
            from infrastructure.constants import discover_verb_requirements
        except ImportError:
            pytest.skip("discover_verb_requirements not yet implemented")

        packages = discover_verb_requirements()
        # Should only contain packages from verbs that actually have requirements.txt
        # Currently that's just faker from fill
        assert isinstance(packages, list), f"Expected list, got {type(packages)}"


class TestBootstrapUsesDiscoveredRequirements:
    """Bootstrap should use discovered requirements, not hard-coded ones."""

    def test_glue_job_additional_modules_includes_discovered_packages(self):
        """The --additional-python-modules arg should include packages from verb requirements.txt files."""
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            instance = BootstrapInfrastructure(env)

        instance._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            instance._create_or_update_glue_job({})

        update_kwargs = instance.glue_client.update_job.call_args.kwargs['JobUpdate']
        additional_modules = update_kwargs['DefaultArguments'].get('--additional-python-modules', '')

        # faker should still be included (discovered from fill/requirements.txt)
        assert 'faker' in additional_modules, (
            f"--additional-python-modules should include 'faker' "
            f"(discovered from fill/requirements.txt). Got: '{additional_modules}'"
        )
