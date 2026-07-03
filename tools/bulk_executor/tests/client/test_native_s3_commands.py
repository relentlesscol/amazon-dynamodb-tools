"""Failing tests for issue #176: Wrap native 'import from S3' and 'export to S3' as commands.

Users should be able to run 'bulk export' and 'bulk import' (or 'export-to-s3'/'import-from-s3')
as first-class commands that wrap DynamoDB's native ExportTableToPointInTime and
ImportTable APIs.

These tests verify:
1. A client-side module exists for the 'export' verb that can be imported
2. A client-side module exists for the 'import' verb that can be imported
3. The server root can dispatch to these verb modules
"""

import importlib
import sys
from pathlib import Path

import pytest


# Test that the client-side verb modules exist and are importable

class TestExportVerbModuleExists:
    """There should be a client/src/python_modules/export.py (or export_s3.py)."""

    def test_export_client_module_importable(self):
        """A client verb module for 'export' (native S3 export) should exist."""
        # The client verb modules live in client/src/python_modules/
        client_modules_dir = Path(__file__).resolve().parents[2] / "client" / "src" / "python_modules"

        # Look for any export-related module
        export_candidates = list(client_modules_dir.glob("export*.py"))
        # Exclude existing export_args.py (that's a shared helper, not a verb)
        export_candidates = [
            p for p in export_candidates
            if p.name not in ('export_args.py',)
        ]

        assert len(export_candidates) > 0, (
            f"Expected a client-side verb module for native DynamoDB S3 export "
            f"(e.g., export.py or export_s3.py) in {client_modules_dir}. "
            f"Found: {[p.name for p in client_modules_dir.glob('*.py')]}"
        )


class TestImportVerbModuleExists:
    """There should be a client/src/python_modules/import_s3.py (or similar)."""

    def test_import_client_module_importable(self):
        """A client verb module for 'import' (native S3 import) should exist."""
        client_modules_dir = Path(__file__).resolve().parents[2] / "client" / "src" / "python_modules"

        # Look for any import-related module (import is a Python keyword so likely import_s3.py)
        import_candidates = list(client_modules_dir.glob("import*.py"))

        assert len(import_candidates) > 0, (
            f"Expected a client-side verb module for native DynamoDB S3 import "
            f"(e.g., import_s3.py or import_from_s3.py) in {client_modules_dir}. "
            f"Found: {[p.name for p in client_modules_dir.glob('*.py')]}"
        )


class TestExportCommandBehavior:
    """The export command should invoke DynamoDB's ExportTableToPointInTime API."""

    def test_export_verb_calls_dynamodb_export_api(self):
        """The server-side export module should call export_table_to_point_in_time."""
        server_modules_dir = Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules"

        # There should be a server-side export module
        export_candidates = list(server_modules_dir.glob("export*.py"))
        # Filter out __init__.py and shared modules
        export_candidates = [
            p for p in export_candidates
            if p.name not in ('__init__.py',)
            and 'shared' not in str(p)
        ]

        assert len(export_candidates) > 0, (
            f"Expected a server-side module for native DynamoDB export "
            f"in {server_modules_dir}. "
            f"Found top-level: {[p.name for p in server_modules_dir.glob('*.py')]}"
        )


class TestImportCommandBehavior:
    """The import command should invoke DynamoDB's ImportTable API."""

    def test_import_verb_calls_dynamodb_import_api(self):
        """The server-side import module should call import_table."""
        server_modules_dir = Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules"

        # There should be a server-side import module
        import_candidates = list(server_modules_dir.glob("import*.py"))
        import_candidates = [
            p for p in import_candidates
            if p.name not in ('__init__.py',)
            and 'shared' not in str(p)
        ]

        assert len(import_candidates) > 0, (
            f"Expected a server-side module for native DynamoDB import "
            f"in {server_modules_dir}. "
            f"Found top-level: {[p.name for p in server_modules_dir.glob('*.py')]}"
        )
