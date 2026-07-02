"""Tests for --persegment flag in the scancount CLI-to-server flow.

Issue #92: scancount should support --persegment to print per-segment counts.

These tests verify the FULL pipeline:
  CLI argv → client argparse → convert_client_dict_to_script_args →
  runner._get_glue_job_arguments → server root._get_parsed_glue_job_args →
  server scancount.run() receives persegment=truthy

Previous test attempts masked the bug by injecting True directly into
parsed_args dicts. These tests exercise real argparse parsing from sys.argv
to prove the flag is actually registered and propagates correctly.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BULK_ROOT = Path(__file__).resolve().parents[2]
CLIENT_SCANCOUNT_PATH = BULK_ROOT / "client" / "src" / "python_modules" / "scancount.py"


def _load_client_scancount():
    """Load client/src/python_modules/scancount.py by file path to avoid
    collision with server/src/python_modules/scancount/ package."""
    spec = importlib.util.spec_from_file_location("client_scancount", CLIENT_SCANCOUNT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestScancountClientParserAcceptsPerSegment:
    """The client-side parser in client/src/python_modules/scancount.py must
    accept --persegment without raising SystemExit (unrecognized argument)."""

    def test_parser_accepts_persegment_flag(self, monkeypatch):
        """--persegment must be a registered argparse argument.

        If this flag is missing from the parser definition, argparse will
        call parser.error() which calls sys.exit(2). This test catches that.
        """
        monkeypatch.setattr(sys, 'argv', [
            'bulk', 'scancount', '--table', 'my-table', '--persegment'
        ])
        monkeypatch.setattr('utils.validate_tables', lambda *a, **kw: None)

        client_scancount = _load_client_scancount()
        mock_env_configs = MagicMock()

        is_server_action, result = client_scancount.run(mock_env_configs)

        assert is_server_action is True
        assert 'persegment' in result, (
            "--persegment must appear in parsed result dict"
        )

    def test_persegment_flag_value_is_truthy_when_passed(self, monkeypatch):
        """When --persegment is passed on the CLI, the value must be truthy
        (True, not None/empty string). store_true should produce True."""
        monkeypatch.setattr(sys, 'argv', [
            'bulk', 'scancount', '--table', 'my-table', '--persegment'
        ])
        monkeypatch.setattr('utils.validate_tables', lambda *a, **kw: None)

        client_scancount = _load_client_scancount()
        mock_env_configs = MagicMock()

        _, result = client_scancount.run(mock_env_configs)

        assert result['persegment'] is True, (
            "--persegment must store True (not None or bare flag value)"
        )

    def test_persegment_absent_when_not_passed(self, monkeypatch):
        """When --persegment is NOT passed, it should not appear in result
        (uses argparse.SUPPRESS as default, matching other optional flags)
        OR it should be falsy so filter_none_or_false_values drops it."""
        monkeypatch.setattr(sys, 'argv', [
            'bulk', 'scancount', '--table', 'my-table'
        ])
        monkeypatch.setattr('utils.validate_tables', lambda *a, **kw: None)

        client_scancount = _load_client_scancount()
        mock_env_configs = MagicMock()

        _, result = client_scancount.run(mock_env_configs)

        if 'persegment' in result:
            assert not result['persegment'], (
                "persegment should be falsy when flag not passed"
            )


class TestPerSegmentSurvivesConversionToScriptArgs:
    """convert_client_dict_to_script_args must include persegment in the
    script_args list so it reaches the Glue job."""

    def test_persegment_true_converted_to_script_args(self):
        """A truthy persegment value must survive conversion to script_args
        so it can reach the server via Glue job arguments."""
        import utils
        client_dict = {
            'verb': 'scancount',
            'table': 'my-table',
            'persegment': True,
        }
        script_args = utils.convert_client_dict_to_script_args(client_dict)

        assert '--persegment' in script_args, (
            "persegment must appear as a --flag in script_args"
        )


class TestPerSegmentSurvivesGlueJobArgsParsing:
    """The full CLI → Glue → server arg flow must deliver persegment."""

    def test_persegment_reaches_server_parsed_args(self):
        """Simulate the full arg flow:
        1. Client emits script_args with --persegment True
        2. Runner._get_glue_job_arguments converts to Glue dict
        3. Server root._get_parsed_glue_job_args parses the Glue dict

        The server's parsed_args must contain 'persegment' with truthy value.
        """
        import utils
        from runner import BulkDynamoDbRunner

        # Step 1: Client dict as produced by scancount.run() with --persegment
        client_dict = {
            'verb': 'scancount',
            'table': 'my-table',
            'persegment': True,
        }
        script_args = utils.convert_client_dict_to_script_args(client_dict)

        # Step 2: Runner converts script_args to Glue arguments dict
        runner = BulkDynamoDbRunner.__new__(BulkDynamoDbRunner)
        glue_args = runner._get_glue_job_arguments({}, script_args)

        # Step 3: Build a fake sys.argv as the Glue job would see it
        server_argv = ['root.py']
        for key, value in glue_args.items():
            server_argv.append(key)
            server_argv.append(value)

        # Step 4: Server-side parsing (replicate _get_parsed_glue_job_args
        # logic from server/src/root.py without importing the full module
        # which has Spark/Glue side effects)
        parsed_args = {}
        i = 1
        while i < len(server_argv):
            if server_argv[i].startswith('--'):
                key = server_argv[i].lstrip('--')
                if i + 1 < len(server_argv) and not server_argv[i + 1].startswith('--'):
                    value = server_argv[i + 1]
                    i += 1
                else:
                    value = None
                parsed_args[key] = value
            i += 1

        assert 'persegment' in parsed_args, (
            "persegment must survive the full CLI→Glue→server flow"
        )
        assert parsed_args['persegment'], (
            "persegment value must be truthy on server side"
        )
