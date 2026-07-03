"""Failing tests for issue #174: Limit what can end up in python_modules.zip.

The module_zipper currently includes ALL files in the source directory.
It should exclude common cruft like __pycache__/, .DS_Store, .pyc files,
and other non-essential development artifacts.
"""

import os
import zipfile
from unittest.mock import patch

import pytest

from utils import module_zipper


class TestZipIgnoresCruft:
    """module_zipper._zip_module must exclude __pycache__, .DS_Store, etc."""

    def test_excludes_pycache_directories(self, tmp_path):
        """__pycache__ dirs and their contents should not appear in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "load.py").write_text("# load command")

        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "load.cpython-314.pyc").write_text("bytecode")
        (pycache / "__init__.cpython-314.pyc").write_text("bytecode")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # __pycache__ directory entry should not be present
        assert not any('__pycache__' in n for n in names), \
            f"__pycache__ found in zip: {[n for n in names if '__pycache__' in n]}"

    def test_excludes_nested_pycache(self, tmp_path):
        """__pycache__ in nested subdirs should also be excluded."""
        source = tmp_path / "python_modules"
        source.mkdir()
        sub = source / "fill"
        sub.mkdir()
        (sub / "__init__.py").write_text("")
        pycache = sub / "__pycache__"
        pycache.mkdir()
        (pycache / "nosk.cpython-314.pyc").write_text("bytecode")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        assert not any('__pycache__' in n for n in names), \
            f"Nested __pycache__ found in zip: {[n for n in names if '__pycache__' in n]}"

    def test_excludes_ds_store_files(self, tmp_path):
        """.DS_Store files should not be included in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "load.py").write_text("# load command")
        (source / ".DS_Store").write_text("binary mac garbage")
        sub = source / "load_export"
        sub.mkdir()
        (sub / "__init__.py").write_text("")
        (sub / ".DS_Store").write_text("more garbage")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        assert not any('.DS_Store' in n for n in names), \
            f".DS_Store found in zip: {[n for n in names if '.DS_Store' in n]}"

    def test_excludes_pyc_files_outside_pycache(self, tmp_path):
        """Stray .pyc files should be excluded even if not in __pycache__."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "load.py").write_text("# load command")
        (source / "load.pyc").write_text("bytecode")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        assert not any(n.endswith('.pyc') for n in names), \
            f".pyc files found in zip: {[n for n in names if n.endswith('.pyc')]}"

    def test_still_includes_legitimate_python_files(self, tmp_path):
        """Real .py files should still be included."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "load.py").write_text("# load")
        (source / "__init__.py").write_text("")
        sub = source / "shared"
        sub.mkdir()
        (sub / "pricing.py").write_text("# pricing")

        # Also add cruft that should be excluded
        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "load.cpython-314.pyc").write_text("bytecode")
        (source / ".DS_Store").write_text("garbage")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # Legitimate files present
        assert 'python_modules/load.py' in names
        assert 'python_modules/__init__.py' in names
        assert 'python_modules/shared/pricing.py' in names
        # Cruft excluded
        assert not any('__pycache__' in n for n in names)
        assert not any('.DS_Store' in n for n in names)

    def test_excludes_data_artifacts(self, tmp_path):
        """Non-Python data artifacts left by developers should be excluded.

        Issue #174 shows test data dirs like `data/jzhunter/` with
        manifest files that shouldn't be zipped.
        """
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "load_export.py").write_text("# load_export")
        data = source / "data"
        data.mkdir()
        user_data = data / "jzhunter"
        user_data.mkdir()
        export = user_data / "01777434077719-58efe0e3"
        export.mkdir()
        (export / "manifest-summary.md5").write_text("hash")
        (export / "manifest-files.md5").write_text("hash")

        zip_path = tmp_path / "out.zip"
        with patch.object(module_zipper, 'log'):
            module_zipper._zip_module(str(source), str(zip_path))

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # The load_export.py should be there but not the data/ artifacts
        assert 'python_modules/load_export.py' in names
        # data/ directory with test artifacts should be excluded
        assert not any('manifest-summary.md5' in n for n in names), \
            f"Data artifacts found in zip: {[n for n in names if 'data/' in n]}"
