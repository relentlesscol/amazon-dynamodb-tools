"""Test for issue #107: import (migrate) from JDBC sources into DynamoDB.

The migrate verb imports data from JDBC-compatible sources (starting with
Redshift) into DynamoDB tables, leveraging Glue's native JDBC connectivity.
"""

from unittest.mock import MagicMock

import pytest


class TestImportJDBC:
    """The migrate verb reads from a JDBC source and writes to DynamoDB."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import migrate as migrate_module
            self.module = migrate_module
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.migrate does not exist")

    def test_reads_from_redshift_connection(self, monkeypatch):
        """migrate should use the Glue JDBC connection to read from Redshift."""
        glue_context = MagicMock()
        # Simulate a DataFrame from the JDBC read
        df = MagicMock()
        df.count.return_value = 10
        glue_context.create_dynamic_frame.from_options.return_value = df

        args = {
            'source_type': 'redshift',
            'source_table': 'public.orders',
            'target': 'OrdersTable',
            'redshift_connection': 'my-glue-connection',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        # The module must have a function that creates the JDBC connection options
        conn_opts = self.module.get_jdbc_connection_options(args)
        assert conn_opts['connectionName'] == 'my-glue-connection'
        assert 'public.orders' in str(conn_opts.get('dbtable', ''))

    def test_writes_to_dynamodb_target(self, monkeypatch, capsys):
        """After reading from JDBC, data is written to the DynamoDB target table."""
        spark_context = MagicMock()
        glue_context = MagicMock()

        # Mock the source read to return a DataFrame
        source_df = MagicMock()
        source_df.count.return_value = 5
        glue_context.create_dynamic_frame.from_options.return_value = source_df

        monkeypatch.setattr(self.module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(self.module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(self.module, 'RateLimiterAggregator', MagicMock())

        args = {
            'source_type': 'redshift',
            'source_table': 'public.orders',
            'target': 'OrdersTable',
            'redshift_connection': 'conn',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        self.module.run(MagicMock(), spark_context, glue_context, args)
        out = capsys.readouterr().out
        # Should report items imported
        assert '5' in out or 'import' in out.lower()
