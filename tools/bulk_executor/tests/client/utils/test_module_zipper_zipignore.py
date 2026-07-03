"""Failing tests for issue #174: Limit what can end up in python_modules.zip.

The module zipper should exclude common cruft files like __pycache__/,
.DS_Store, and other unwanted artifacts from the zip archive. The behavior
should be similar to a "zipignore" — files matching exclusion patterns are
silently skipped.

Tests exercise the real _zip_module function against a temp directory
containing cruft files, verifying they do NOT appear in the resulting archive.
"""

import os
import zipfile
from unittest.mock import patch

import pytest

from utils import module_zipper


class TestModuleZipperExcludesCruft:
    """_zip_module should skip __pycache__, .DS_Store, and similar cruft."""

    def test_pycache_directories_excluded(self, tmp_path):
        """__pycache__ directories and their contents must not appear in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("# real module")

        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "module.cpython-310.pyc").write_bytes(b'\x00' * 100)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # Real module should be there
        assert 'python_modules/module.py' in names
        # Pycache should NOT be there
        pycache_entries = [n for n in names if '__pycache__' in n]
        assert pycache_entries == [], f"__pycache__ entries found: {pycache_entries}"

    def test_nested_pycache_excluded(self, tmp_path):
        """__pycache__ in nested subdirectories must also be excluded."""
        source = tmp_path / "python_modules"
        source.mkdir()
        sub = source / "fill"
        sub.mkdir()
        (sub / "__init__.py").write_text("")
        pycache = sub / "__pycache__"
        pycache.mkdir()
        (pycache / "nosk.cpython-314.pyc").write_bytes(b'\x00' * 50)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        pycache_entries = [n for n in names if '__pycache__' in n]
        assert pycache_entries == [], f"Nested __pycache__ entries found: {pycache_entries}"

    def test_ds_store_files_excluded(self, tmp_path):
        """.DS_Store files must not appear in the zip."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("# real module")
        (source / ".DS_Store").write_bytes(b'\x00' * 50)

        sub = source / "load_export"
        sub.mkdir()
        (sub / "__init__.py").write_text("")
        (sub / ".DS_Store").write_bytes(b'\x00' * 50)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        ds_store_entries = [n for n in names if '.DS_Store' in n]
        assert ds_store_entries == [], f".DS_Store entries found: {ds_store_entries}"

    def test_data_directories_excluded(self, tmp_path):
        """Leftover data/ directories from local testing should be excluded."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("# real module")

        data_dir = source / "load_export" / "data" / "jzhunter"
        data_dir.mkdir(parents=True)
        (data_dir / "manifest-summary.md5").write_text("hash")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # If there's a "data" directory exclusion pattern, test artifacts
        # shouldn't be included. The current code DOES include them (the bug).
        data_entries = [n for n in names if '/data/' in n]
        assert data_entries == [], f"data/ directory entries found: {data_entries}"

    def test_pyc_files_excluded_even_outside_pycache(self, tmp_path):
        """.pyc files should be excluded even if not in __pycache__."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("# real module")
        (source / "module.pyc").write_bytes(b'\x00' * 100)

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        pyc_entries = [n for n in names if n.endswith('.pyc')]
        assert pyc_entries == [], f".pyc entries found: {pyc_entries}"

    def test_real_python_files_still_included(self, tmp_path):
        """Legitimate .py files should still be included in the archive."""
        source = tmp_path / "python_modules"
        source.mkdir()
        (source / "module.py").write_text("# real module")
        (source / "__init__.py").write_text("")
        sub = source / "shared"
        sub.mkdir()
        (sub / "utils.py").write_text("# utilities")

        # Add cruft alongside
        pycache = source / "__pycache__"
        pycache.mkdir()
        (pycache / "module.cpython-310.pyc").write_bytes(b'\x00')
        (source / ".DS_Store").write_bytes(b'\x00')

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # Real files present
        assert 'python_modules/module.py' in names
        assert 'python_modules/__init__.py' in names
        assert 'python_modules/shared/utils.py' in names
