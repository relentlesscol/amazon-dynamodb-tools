"""Failing tests for issue #138: per-command folder layout discovery.

Issue #138 requests restructuring from the flat layout:
  client/src/python_modules/CMDNAME.py
  server/src/python_modules/CMDNAME.py

to a per-command folder layout:
  commands/CMDNAME/client/...
  commands/CMDNAME/server/...
  commands/CMDNAME/test/
  commands/CMDNAME/README.md

The key observable behaviors that must change:
1. The module zipper must discover and package server modules from
   commands/*/server/ into a zip (instead of a flat server/src/python_modules).
2. The CLI command discovery (the `bulk` entrypoint) must find and load
   command client modules from commands/*/client/ (instead of
   client/src/python_modules/).

These tests verify that when a commands/ directory structure exists with
per-command folders, the packaging and discovery logic correctly assembles
the server zip and locates client command modules from the new layout.
"""

import os
import zipfile
from unittest.mock import patch

import pytest

from utils import module_zipper


class TestCommandFolderZipDiscovery:
    """The module zipper should discover server modules from commands/*/server/
    and produce a zip containing all command server code, preserving module
    structure so that the Glue job can import them at runtime.
    """

    def test_zips_server_modules_from_per_command_folders(self, tmp_path):
        """Given a commands/ layout with multiple commands, zip_commands()
        discovers each command's server/ subtree and produces a zip containing
        all server modules under a python_modules/ prefix.

        Expected zip layout:
          python_modules/
          python_modules/copy.py
          python_modules/count.py
          python_modules/shared/
          python_modules/shared/helper.py
        """
        # Arrange: create per-command folder structure
        commands_dir = tmp_path / "commands"

        # Command: copy (simple single-file server)
        copy_server = commands_dir / "copy" / "server"
        copy_server.mkdir(parents=True)
        (copy_server / "copy.py").write_text("def run(): pass")

        # Command: count (simple single-file server)
        count_server = commands_dir / "count" / "server"
        count_server.mkdir(parents=True)
        (count_server / "count.py").write_text("def run(): pass")

        # Shared modules (per issue: shared/server/rate-limiter/)
        shared_server = commands_dir / "shared" / "server"
        shared_server.mkdir(parents=True)
        (shared_server / "helper.py").write_text("def help(): pass")

        # Also create client-side and test dirs to prove they're excluded
        (commands_dir / "copy" / "client").mkdir(parents=True)
        (commands_dir / "copy" / "client" / "copy.py").write_text("client code")
        (commands_dir / "copy" / "test").mkdir(parents=True)
        (commands_dir / "copy" / "test" / "test_copy.py").write_text("test code")
        (commands_dir / "copy" / "README.md").write_text("# Copy command")

        zip_path = tmp_path / "python_modules.zip"

        # Act: call the new function that discovers from commands/ layout
        with patch.object(module_zipper, 'log'):
            result = module_zipper.zip_commands(str(commands_dir), str(zip_path))

        # Assert: zip was created with correct structure
        assert result is True, "zip_commands() should return True on success"
        assert zip_path.exists(), "zip file should be created"

        with zipfile.ZipFile(zip_path) as zf:
            names = set(zf.namelist())

        # Server modules present under python_modules/ prefix
        assert "python_modules/" in names, "zip should have python_modules/ root entry"
        assert "python_modules/copy.py" in names, "copy server module should be in zip"
        assert "python_modules/count.py" in names, "count server module should be in zip"
        assert "python_modules/shared/" in names or \
            "python_modules/shared/helper.py" in names, \
            "shared server module should be in zip"

        # Client/test code must NOT be in the zip
        client_entries = [n for n in names if "client" in n.lower()]
        test_entries = [n for n in names if "test" in n.lower()]
        readme_entries = [n for n in names if "readme" in n.lower()]

        assert client_entries == [], f"Client code leaked into server zip: {client_entries}"
        assert test_entries == [], f"Test code leaked into server zip: {test_entries}"
        assert readme_entries == [], f"README leaked into server zip: {readme_entries}"

    def test_zip_commands_includes_multi_file_server_packages(self, tmp_path):
        """Commands with multi-file server packages (directories) should have
        their entire server subtree included in the zip.

        This mirrors the current layout where some server commands are
        directories (e.g., server/src/python_modules/load/ with __init__.py).
        """
        commands_dir = tmp_path / "commands"

        # Command: load (multi-file server package)
        load_server = commands_dir / "load" / "server" / "load"
        load_server.mkdir(parents=True)
        (load_server / "__init__.py").write_text("")
        (load_server / "loader.py").write_text("def load(): pass")
        (load_server / "parser.py").write_text("def parse(): pass")

        zip_path = tmp_path / "python_modules.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper.zip_commands(str(commands_dir), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = set(zf.namelist())

        # Multi-file package preserved with structure
        assert "python_modules/load/__init__.py" in names
        assert "python_modules/load/loader.py" in names
        assert "python_modules/load/parser.py" in names

    def test_zip_commands_skips_commands_with_no_server_dir(self, tmp_path):
        """A command folder that has no server/ subdirectory is gracefully
        skipped — it might be client-only or documentation-only.
        """
        commands_dir = tmp_path / "commands"

        # Command with server
        (commands_dir / "copy" / "server").mkdir(parents=True)
        (commands_dir / "copy" / "server" / "copy.py").write_text("def run(): pass")

        # Command without server (client-only)
        (commands_dir / "info" / "client").mkdir(parents=True)
        (commands_dir / "info" / "client" / "info.py").write_text("def run(): pass")

        zip_path = tmp_path / "python_modules.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper.zip_commands(str(commands_dir), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = set(zf.namelist())

        assert "python_modules/copy.py" in names
        # No info module since it has no server dir
        info_entries = [n for n in names if "info" in n]
        assert info_entries == [], f"Client-only command leaked into server zip: {info_entries}"
