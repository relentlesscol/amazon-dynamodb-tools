"""Test for issue #174: Limit what can end up in python_modules.zip.

The module zipper should exclude common cruft files:
- __pycache__/ directories
- .DS_Store files
- .pyc files
- Any other patterns that shouldn't be deployed to Glue

This acts like a "zipignore" to keep the deployment artifact clean.
"""

import os
import tempfile
import zipfile

import pytest

from utils.module_zipper import _zip_module


class TestModuleZipperExclusions:
    """Module zipper should exclude cruft files from the zip."""

    def _create_source_tree(self, tmp_path):
        """Create a source directory with both valid and cruft files."""
        src = tmp_path / "python_modules"
        src.mkdir()

        # Valid files that SHOULD be included
        (src / "__init__.py").write_text("# init")
        (src / "copy.py").write_text("# copy module")

        sub = src / "fill"
        sub.mkdir()
        (sub / "__init__.py").write_text("# fill init")
        (sub / "default.py").write_text("# fill default")

        # Cruft that should be EXCLUDED
        cache = sub / "__pycache__"
        cache.mkdir()
        (cache / "default.cpython-314.pyc").write_text("bytecode")
        (cache / "__init__.cpython-314.pyc").write_text("bytecode")

        (src / ".DS_Store").write_text("apple cruft")
        (sub / ".DS_Store").write_text("nested apple cruft")

        # A data directory that shouldn't be there
        data = src / "load_export" / "data" / "jzhunter"
        data.mkdir(parents=True)
        (data / "manifest-summary.md5").write_text("stale data")

        return src

    def test_excludes_pycache_directories(self, tmp_path):
        """__pycache__ directories and their contents must be excluded."""
        src = self._create_source_tree(tmp_path)
        zip_path = str(tmp_path / "output.zip")

        _zip_module(str(src), zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            pycache_entries = [n for n in names if '__pycache__' in n]
            assert pycache_entries == [], \
                f"__pycache__ files found in zip: {pycache_entries}"

    def test_excludes_ds_store_files(self, tmp_path):
        """macOS .DS_Store files must be excluded."""
        src = self._create_source_tree(tmp_path)
        zip_path = str(tmp_path / "output.zip")

        _zip_module(str(src), zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            ds_store = [n for n in names if '.DS_Store' in n]
            assert ds_store == [], f".DS_Store files found in zip: {ds_store}"

    def test_excludes_pyc_files(self, tmp_path):
        """Compiled .pyc files must be excluded even if outside __pycache__."""
        src = self._create_source_tree(tmp_path)
        # Add a stray .pyc not in __pycache__
        (src / "stray.pyc").write_text("stray bytecode")
        zip_path = str(tmp_path / "output.zip")

        _zip_module(str(src), zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            pyc_files = [n for n in names if n.endswith('.pyc')]
            assert pyc_files == [], f".pyc files found in zip: {pyc_files}"

    def test_includes_valid_python_files(self, tmp_path):
        """Valid .py files must still be included."""
        src = self._create_source_tree(tmp_path)
        zip_path = str(tmp_path / "output.zip")

        _zip_module(str(src), zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            py_files = [n for n in names if n.endswith('.py')]
            assert len(py_files) >= 4, f"Expected at least 4 .py files, got: {py_files}"
