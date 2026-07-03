"""Unit tests for the `migrate` server-side verb.

Covers the core behavior of `python_modules/migrate/__init__.py`:
- run(): reads from a JDBC source (Redshift) via Spark's JDBC reader using
  a Glue Connection, then writes the resulting DataFrame to DynamoDB via
  write_dynamodb_dataframe.

The migrate verb should:
1. Use spark.read.format("jdbc") with the connection URL from the named
   Glue Connection to read from the source.
2. Support --source-table (reads the full table) and --source-query (runs
   custom SQL) as mutually exclusive source specifications.
3. Support an optional --where clause when using --source-table.
4. Write the resulting DataFrame to DynamoDB via write_dynamodb_dataframe.
5. Log the count of migrated items.
"""

import sys
from unittest.mock import MagicMock, patch, call

import pytest

# Ensure awsglue.transforms mock is present (see conftest pattern)
if 'awsglue.transforms' not in sys.modules:
    sys.modules['awsglue.transforms'] = MagicMock()


class TestMigrateRunReadsFromJdbcAndWritesToDynamoDB:
    """migrate.run() reads from JDBC source and writes to DynamoDB."""

    def _import_migrate(self):
        """Import the migrate module (deferred to allow conftest mocking)."""
        from python_modules import migrate as migrate_module
        return migrate_module

    def test_reads_redshift_table_via_jdbc_and_writes_to_dynamodb(self, monkeypatch):
        """When source_type=redshift and source_table is provided, run() reads
        from the Redshift table via Spark JDBC and writes all rows to DynamoDB
        using write_dynamodb_dataframe."""
        migrate_module = self._import_migrate()

        # Mock Spark session and JDBC reader chain
        mock_df = MagicMock()
        mock_df.count.return_value = 42

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        mock_spark = MagicMock()
        mock_spark.read = mock_reader

        mock_glue_context = MagicMock()
        mock_glue_context.spark_session = mock_spark

        # Mock the Glue connection lookup to return JDBC URL
        mock_get_connection = MagicMock(return_value={
            'Connection': {
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:redshift://cluster.abc.us-east-1.redshift.amazonaws.com:5439/mydb',
                    'USERNAME': 'admin',
                    'PASSWORD': 'secret',
                }
            }
        })
        mock_glue_client = MagicMock()
        mock_glue_client.get_connection = mock_get_connection
        monkeypatch.setattr(migrate_module, 'boto3', MagicMock())
        migrate_module.boto3.client.return_value = mock_glue_client

        # Mock write_dynamodb_dataframe
        write_mock = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', write_mock)

        parsed_args = {
            'source_type': 'redshift',
            'source_table': 'public.orders',
            'table': 'Orders',
            'redshift_connection': 'my-glue-connection',
        }

        migrate_module.run(MagicMock(), MagicMock(), mock_glue_context, parsed_args)

        # Assert: JDBC reader was used with format("jdbc")
        mock_reader.format.assert_called_with('jdbc')
        # Assert: data was written to DynamoDB
        write_mock.assert_called_once_with(
            mock_glue_context, mock_df, 'Orders', parsed_args)

    def test_source_query_uses_custom_sql_instead_of_table(self, monkeypatch):
        """When source_query is provided instead of source_table, run() uses
        the custom SQL query as the dbtable option (Spark JDBC query syntax)."""
        migrate_module = self._import_migrate()

        mock_df = MagicMock()
        mock_df.count.return_value = 10

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        mock_spark = MagicMock()
        mock_spark.read = mock_reader

        mock_glue_context = MagicMock()
        mock_glue_context.spark_session = mock_spark

        mock_glue_client = MagicMock()
        mock_glue_client.get_connection.return_value = {
            'Connection': {
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:redshift://cluster:5439/db',
                    'USERNAME': 'user',
                    'PASSWORD': 'pass',
                }
            }
        }
        monkeypatch.setattr(migrate_module, 'boto3', MagicMock())
        migrate_module.boto3.client.return_value = mock_glue_client

        write_mock = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', write_mock)

        custom_query = "SELECT order_id, total FROM orders WHERE status = 'completed'"
        parsed_args = {
            'source_type': 'redshift',
            'source_query': custom_query,
            'table': 'Orders',
            'redshift_connection': 'my-conn',
        }

        migrate_module.run(MagicMock(), MagicMock(), mock_glue_context, parsed_args)

        # The query should be wrapped as a subquery for Spark JDBC
        # Spark JDBC expects: (SELECT ...) AS subquery
        all_option_calls = mock_reader.option.call_args_list
        dbtable_calls = [c for c in all_option_calls if c[0][0] == 'dbtable']
        assert len(dbtable_calls) == 1
        dbtable_value = dbtable_calls[0][0][1]
        assert custom_query in dbtable_value

        write_mock.assert_called_once()

    def test_where_clause_filters_source_table(self, monkeypatch):
        """When --where is provided with --source-table, the table read is
        filtered by wrapping it as a subquery with WHERE clause."""
        migrate_module = self._import_migrate()

        mock_df = MagicMock()
        mock_df.count.return_value = 5

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        mock_spark = MagicMock()
        mock_spark.read = mock_reader

        mock_glue_context = MagicMock()
        mock_glue_context.spark_session = mock_spark

        mock_glue_client = MagicMock()
        mock_glue_client.get_connection.return_value = {
            'Connection': {
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:redshift://cluster:5439/db',
                    'USERNAME': 'user',
                    'PASSWORD': 'pass',
                }
            }
        }
        monkeypatch.setattr(migrate_module, 'boto3', MagicMock())
        migrate_module.boto3.client.return_value = mock_glue_client

        write_mock = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', write_mock)

        parsed_args = {
            'source_type': 'redshift',
            'source_table': 'orders',
            'table': 'Orders',
            'redshift_connection': 'my-conn',
            'where': "order_date > '2024-01-01'",
        }

        migrate_module.run(MagicMock(), MagicMock(), mock_glue_context, parsed_args)

        # The dbtable option should contain the WHERE clause
        all_option_calls = mock_reader.option.call_args_list
        dbtable_calls = [c for c in all_option_calls if c[0][0] == 'dbtable']
        assert len(dbtable_calls) == 1
        dbtable_value = dbtable_calls[0][0][1]
        assert "order_date > '2024-01-01'" in dbtable_value
        assert 'orders' in dbtable_value

        write_mock.assert_called_once()

    def test_zero_rows_returns_early_without_writing(self, monkeypatch):
        """When the JDBC source returns zero rows, run() should not attempt
        to write to DynamoDB."""
        migrate_module = self._import_migrate()

        mock_df = MagicMock()
        mock_df.count.return_value = 0

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        mock_spark = MagicMock()
        mock_spark.read = mock_reader

        mock_glue_context = MagicMock()
        mock_glue_context.spark_session = mock_spark

        mock_glue_client = MagicMock()
        mock_glue_client.get_connection.return_value = {
            'Connection': {
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:redshift://cluster:5439/db',
                    'USERNAME': 'user',
                    'PASSWORD': 'pass',
                }
            }
        }
        monkeypatch.setattr(migrate_module, 'boto3', MagicMock())
        migrate_module.boto3.client.return_value = mock_glue_client

        write_mock = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', write_mock)

        parsed_args = {
            'source_type': 'redshift',
            'source_table': 'empty_table',
            'table': 'Target',
            'redshift_connection': 'conn',
        }

        migrate_module.run(MagicMock(), MagicMock(), mock_glue_context, parsed_args)

        # Should NOT write to DynamoDB when source has no data
        write_mock.assert_not_called()
