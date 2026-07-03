"""Unit tests for zip-ignore behavior in module_zipper (issue #174).

When zipping python_modules, the zipper should EXCLUDE common cruft:
- __pycache__/ directories and their contents
- .DS_Store files
- .pyc files

This tests the OBSERVABLE BEHAVIOR: that the archive produced by _zip_module
does NOT contain these entries, even when they exist in the source tree.
"""

import os
import zipfile
from unittest.mock import patch

import pytest

from utils import module_zipper


class TestZipModuleExcludesCruft:
    """_zip_module must filter out __pycache__, .DS_Store, .pyc from the archive."""

    def test_pycache_directories_excluded_from_archive(self, tmp_path):
        """__pycache__/ dirs and their contents must not appear in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "main.py").write_text("print('main')")

        # Create __pycache__ with .pyc files
        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "main.cpython-314.pyc").write_bytes(b"\x00" * 100)

        # Nested __pycache__
        subdir = source / "fill"
        subdir.mkdir()
        (subdir / "default.py").write_text("print('fill')")
        nested_pycache = subdir / "__pycache__"
        nested_pycache.mkdir()
        (nested_pycache / "default.cpython-314.pyc").write_bytes(b"\x00" * 50)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # Real module files MUST be present
        assert 'python_modules/main.py' in names
        assert 'python_modules/fill/default.py' in names

        # __pycache__ entries must NOT be present
        pycache_entries = [n for n in names if '__pycache__' in n]
        assert pycache_entries == [], \
            f"__pycache__ entries should be excluded but found: {pycache_entries}"

    def test_ds_store_files_excluded_from_archive(self, tmp_path):
        """.DS_Store files must not appear in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("x = 1")
        (source / ".DS_Store").write_bytes(b"\x00\x00\x00\x01Bud1")

        subdir = source / "load_export"
        subdir.mkdir()
        (subdir / "__init__.py").write_text("")
        (subdir / ".DS_Store").write_bytes(b"\x00\x00\x00\x01Bud1")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # Real files present
        assert 'python_modules/module.py' in names
        assert 'python_modules/load_export/__init__.py' in names

        # .DS_Store must NOT be present
        ds_store_entries = [n for n in names if '.DS_Store' in n]
        assert ds_store_entries == [], \
            f".DS_Store entries should be excluded but found: {ds_store_entries}"

    def test_standalone_pyc_files_excluded_from_archive(self, tmp_path):
        """.pyc files outside __pycache__ must also be excluded."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "real.py").write_text("y = 2")
        (source / "stale.pyc").write_bytes(b"\x00" * 20)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        assert 'python_modules/real.py' in names
        pyc_entries = [n for n in names if n.endswith('.pyc')]
        assert pyc_entries == [], \
            f".pyc files should be excluded but found: {pyc_entries}"

    def test_legitimate_files_still_included_alongside_cruft(self, tmp_path):
        """Excluding cruft must not accidentally exclude real modules."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "app.py").write_text("app")
        (source / "utils.py").write_text("utils")
        # cruft
        (source / ".DS_Store").write_bytes(b"x")
        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "app.cpython-311.pyc").write_bytes(b"x")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # All real files present
        assert 'python_modules/app.py' in names
        assert 'python_modules/utils.py' in names
        # No cruft
        cruft = [n for n in names if '__pycache__' in n or '.DS_Store' in n or n.endswith('.pyc')]
        assert cruft == []
