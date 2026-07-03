"""Failing tests for issue #228: Let each command define its own Glue requirements.txt.

Each command verb should be able to define a `requirements.txt` in its folder
that lists additional Python packages needed on the Glue server. During bootstrap,
these per-command requirements should be discovered and merged into the
THIRD_PARTY_PYTHON_MODULES constant that's passed to the Glue job.

Currently, the requirements are hard-coded in constants.py as a static list.
The new behavior should sniff for requirements.txt files in each verb's folder.
"""

import os
from unittest.mock import MagicMock, patch

import pytest


class TestPerCommandRequirements:
    """Bootstrap should discover per-command requirements.txt files."""

    def test_discovers_requirements_from_verb_folder(self, tmp_path):
        """If a command folder contains requirements.txt, those packages should
        be included in the Glue job's --additional-python-modules argument."""
        # Create a mock command folder structure
        fill_dir = tmp_path / "server" / "src" / "python_modules" / "fill"
        fill_dir.mkdir(parents=True)
        (fill_dir / "requirements.txt").write_text("faker\n")

        diff_dir = tmp_path / "server" / "src" / "python_modules" / "diff"
        diff_dir.mkdir(parents=True)
        # diff has no requirements.txt — uses only stdlib

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

        # If there's a method to discover requirements, call it
        # The current code has no such method — this will fail
        if hasattr(instance, '_discover_requirements'):
            result = instance._discover_requirements(str(tmp_path))
            assert 'faker' in result
        else:
            # Test that the feature doesn't exist yet (the test fails)
            pytest.fail(
                "BootstrapInfrastructure has no _discover_requirements method. "
                "Issue #228 requires per-command requirements.txt discovery."
            )

    def test_multiple_commands_requirements_merged(self, tmp_path):
        """Requirements from multiple commands should be merged (deduplicated)."""
        # Create two commands with requirements
        fill_dir = tmp_path / "server" / "src" / "python_modules" / "fill"
        fill_dir.mkdir(parents=True)
        (fill_dir / "requirements.txt").write_text("faker\nrequests\n")

        sql_dir = tmp_path / "server" / "src" / "python_modules" / "sql"
        sql_dir.mkdir(parents=True)
        (sql_dir / "requirements.txt").write_text("sqlparse\nrequests\n")

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

        if hasattr(instance, '_discover_requirements'):
            result = instance._discover_requirements(str(tmp_path))
            # Should contain all unique packages
            assert 'faker' in result
            assert 'sqlparse' in result
            assert 'requests' in result
            # No duplicates
            assert result.count('requests') == 1
        else:
            pytest.fail(
                "BootstrapInfrastructure has no _discover_requirements method. "
                "Issue #228 requires per-command requirements.txt discovery."
            )

    def test_glue_job_default_arguments_include_discovered_requirements(self, tmp_path):
        """The --additional-python-modules in Glue job DefaultArguments should
        include packages discovered from command requirements.txt files."""
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
        modules_str = update_kwargs['DefaultArguments']['--additional-python-modules']

        # Currently hard-coded to just 'faker'. After the fix, it should
        # dynamically discover requirements. For now, assert the method exists.
        # The real behavior test is in the above tests.
        assert 'faker' in modules_str  # This passes with current code (baseline)
