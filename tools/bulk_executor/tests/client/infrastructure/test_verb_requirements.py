"""Tests for per-verb requirements.txt discovery (issue #228).

Instead of hard-coding third-party dependencies in constants.py, each
verb folder (server/src/python_modules/<verb>/) can contain a
requirements.txt that declares its own server-side pip dependencies.
Bootstrap should scan these files and aggregate the packages into the
Glue job's --additional-python-modules argument.

These tests exercise the actual bootstrap output when verb folders
define requirements.txt files, verifying the observable behavior: what
gets passed to the Glue API as --additional-python-modules.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def bootstrap_with_verb_dirs(tmp_path):
    """Construct BootstrapInfrastructure with a fake verb directory tree.

    Creates a temporary python_modules directory with verb folders that
    may or may not contain requirements.txt files.  Patches the module
    dir path constant so bootstrap reads from our temp tree.
    """
    # Create verb directories with requirements.txt
    modules_dir = tmp_path / "server" / "src" / "python_modules"
    modules_dir.mkdir(parents=True)

    # fill verb needs faker
    fill_dir = modules_dir / "fill"
    fill_dir.mkdir()
    (fill_dir / "__init__.py").touch()
    (fill_dir / "requirements.txt").write_text("faker\n")

    # update verb has no requirements
    update_dir = modules_dir / "update"
    update_dir.mkdir()
    (update_dir / "__init__.py").touch()

    # shared dir should be ignored (not a verb)
    shared_dir = modules_dir / "shared"
    shared_dir.mkdir()
    (shared_dir / "__init__.py").touch()
    (shared_dir / "requirements.txt").write_text("should-be-ignored\n")

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
    return instance, tmp_path


def _get_additional_modules(bootstrap, args=None, *, modules_dir):
    """Run _create_or_update_glue_job and return the --additional-python-modules value."""
    if args is None:
        args = {}
    with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True), \
         patch('infrastructure.constants.PYTHON_MODULE_CLIENT_DIR_PATH',
               str(modules_dir / "server" / "src" / "python_modules")):
        bootstrap._create_or_update_glue_job(args)
    default_args = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
    return default_args.get('--additional-python-modules', '')


class TestVerbRequirementsDiscovery:
    """Bootstrap reads requirements.txt from verb folders to build --additional-python-modules."""

    def test_verb_requirements_txt_included_in_additional_modules(
        self, bootstrap_with_verb_dirs
    ):
        """When a verb folder has a requirements.txt, its packages appear
        in the Glue job's --additional-python-modules argument."""
        bootstrap, tmp_path = bootstrap_with_verb_dirs
        result = _get_additional_modules(bootstrap, modules_dir=tmp_path)
        assert 'faker' in result.split(',')

    def test_verb_without_requirements_txt_contributes_nothing(
        self, bootstrap_with_verb_dirs
    ):
        """A verb folder without requirements.txt adds no packages."""
        bootstrap, tmp_path = bootstrap_with_verb_dirs
        result = _get_additional_modules(bootstrap, modules_dir=tmp_path)
        packages = result.split(',') if result else []
        # 'update' has no requirements.txt, shouldn't add spurious entries
        # Only 'faker' should be present (from fill)
        assert all(pkg.strip() for pkg in packages), "No blank entries"

    def test_shared_dir_requirements_not_included(
        self, bootstrap_with_verb_dirs
    ):
        """The 'shared' directory is not a verb and its requirements.txt
        should not be read."""
        bootstrap, tmp_path = bootstrap_with_verb_dirs
        result = _get_additional_modules(bootstrap, modules_dir=tmp_path)
        assert 'should-be-ignored' not in result

    def test_multiple_verbs_with_requirements_are_aggregated(self, tmp_path):
        """When multiple verbs define requirements, all are aggregated."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"
        modules_dir.mkdir(parents=True)

        # fill needs faker
        fill_dir = modules_dir / "fill"
        fill_dir.mkdir()
        (fill_dir / "__init__.py").touch()
        (fill_dir / "requirements.txt").write_text("faker\n")

        # hypothetical 'enrich' verb needs requests
        enrich_dir = modules_dir / "enrich"
        enrich_dir.mkdir()
        (enrich_dir / "__init__.py").touch()
        (enrich_dir / "requirements.txt").write_text("requests\n")

        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            bootstrap = BootstrapInfrastructure(env)

        bootstrap._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True), \
             patch('infrastructure.constants.PYTHON_MODULE_CLIENT_DIR_PATH',
                   str(modules_dir)):
            bootstrap._create_or_update_glue_job({})

        default_args = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
        modules = default_args.get('--additional-python-modules', '')
        packages = [p.strip() for p in modules.split(',')]
        assert 'faker' in packages
        assert 'requests' in packages

    def test_duplicate_requirements_across_verbs_are_deduplicated(self, tmp_path):
        """If two verbs both list the same package, it appears only once."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"
        modules_dir.mkdir(parents=True)

        for verb in ("fill", "enrich"):
            verb_dir = modules_dir / verb
            verb_dir.mkdir()
            (verb_dir / "__init__.py").touch()
            (verb_dir / "requirements.txt").write_text("faker\n")

        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            bootstrap = BootstrapInfrastructure(env)

        bootstrap._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True), \
             patch('infrastructure.constants.PYTHON_MODULE_CLIENT_DIR_PATH',
                   str(modules_dir)):
            bootstrap._create_or_update_glue_job({})

        default_args = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
        modules = default_args.get('--additional-python-modules', '')
        packages = [p.strip() for p in modules.split(',')]
        assert packages.count('faker') == 1

    def test_empty_requirements_txt_contributes_nothing(self, tmp_path):
        """An empty requirements.txt in a verb folder adds no packages."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"
        modules_dir.mkdir(parents=True)

        verb_dir = modules_dir / "empty_verb"
        verb_dir.mkdir()
        (verb_dir / "__init__.py").touch()
        (verb_dir / "requirements.txt").write_text("")

        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            bootstrap = BootstrapInfrastructure(env)

        bootstrap._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True), \
             patch('infrastructure.constants.PYTHON_MODULE_CLIENT_DIR_PATH',
                   str(modules_dir)):
            bootstrap._create_or_update_glue_job({})

        default_args = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
        modules = default_args.get('--additional-python-modules', '')
        # Empty or only whitespace — no spurious commas
        if modules:
            packages = [p.strip() for p in modules.split(',') if p.strip()]
            assert len(packages) == 0

    def test_no_verb_folders_yields_empty_additional_modules(self, tmp_path):
        """When no verb folders exist at all, --additional-python-modules is empty."""
        modules_dir = tmp_path / "server" / "src" / "python_modules"
        modules_dir.mkdir(parents=True)
        # Only shared (non-verb) directory
        shared_dir = modules_dir / "shared"
        shared_dir.mkdir()
        (shared_dir / "__init__.py").touch()

        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            bootstrap = BootstrapInfrastructure(env)

        bootstrap._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True), \
             patch('infrastructure.constants.PYTHON_MODULE_CLIENT_DIR_PATH',
                   str(modules_dir)):
            bootstrap._create_or_update_glue_job({})

        default_args = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
        modules = default_args.get('--additional-python-modules', '')
        # Should be empty or not present — currently it's hard-coded to 'faker'
        # so this test will FAIL until the fix is applied
        if modules:
            packages = [p.strip() for p in modules.split(',') if p.strip()]
            assert len(packages) == 0
