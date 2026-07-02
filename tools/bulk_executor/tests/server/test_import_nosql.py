"""Test for issue #108: import (migrate) from NoSQL sources into DynamoDB.

The migrate verb for NoSQL sources reads from MongoDB (and eventually
other NoSQL DBs) and writes to DynamoDB, leveraging Glue's native
MongoDB connector.
"""

from unittest.mock import MagicMock

import pytest


class TestImportNoSQL:
    """The migrate verb reads from MongoDB and writes to DynamoDB."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import migrate as migrate_module
            self.module = migrate_module
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.migrate does not exist")

    def test_reads_from_mongodb_connection(self, monkeypatch):
        """migrate should support mongodb source type."""
        args = {
            'source_type': 'mongodb',
            'source_collection': 'mydb.orders',
            'target': 'OrdersTable',
            'mongodb_connection': 'my-mongo-connection',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        conn_opts = self.module.get_nosql_connection_options(args)
        assert conn_opts['connectionName'] == 'my-mongo-connection'
        assert 'orders' in str(conn_opts.get('collection', ''))

    def test_supports_mongodb_filter(self, monkeypatch):
        """MongoDB imports should support a query filter."""
        args = {
            'source_type': 'mongodb',
            'source_collection': 'mydb.orders',
            'target': 'OrdersTable',
            'mongodb_connection': 'conn',
            'filter': '{"status": "completed"}',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        conn_opts = self.module.get_nosql_connection_options(args)
        # The filter should be passed through to the connection options
        assert 'filter' in conn_opts or 'query' in conn_opts

    def test_supports_mongodb_projection(self, monkeypatch):
        """MongoDB imports should support field projection."""
        args = {
            'source_type': 'mongodb',
            'source_collection': 'mydb.orders',
            'target': 'OrdersTable',
            'mongodb_connection': 'conn',
            'projection': '{"order_id": 1, "total": 1}',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        conn_opts = self.module.get_nosql_connection_options(args)
        assert 'projection' in conn_opts or 'fields' in conn_opts
