"""Unit tests for the `migrate` server-side verb.

Covers `python_modules/migrate.py`:
- run(): reads from a MongoDB source via Spark's MongoDB connector,
  writes the resulting DataFrame to DynamoDB using write_dynamodb_dataframe,
  applies optional query filter and projection, reports row count.

Issue #108: import data from NoSQL sources into DynamoDB (starting with MongoDB).
"""

import sys
from unittest.mock import MagicMock, Mock, patch, call

import pytest

# Ensure pyspark.sql is available (sql module needs SparkSession)
if 'pyspark.sql' not in sys.modules:
    sys.modules['pyspark.sql'] = Mock()


class TestMigrateModuleExists:
    """The migrate server-side module must exist and expose a run() function."""

    def test_migrate_module_importable(self):
        """python_modules.migrate must be importable as a server-side verb."""
        from python_modules import migrate
        assert hasattr(migrate, 'run'), (
            "migrate module must expose a run(job, spark_context, glue_context, parsed_args) function"
        )


class TestMigrateRunReadsMongoDB:
    """run() reads from MongoDB using the Spark MongoDB connector."""

    @pytest.fixture
    def migrate_module(self, monkeypatch):
        """Import the migrate module with mocked externals."""
        from python_modules import migrate as mod

        # Inject get_error_message if not present (star-import from mocked errors)
        if not hasattr(mod, 'get_error_message'):
            mod.get_error_message = lambda e: str(e)
        return mod

    @pytest.fixture
    def mock_spark_session(self):
        """Build a mock SparkSession with a chainable read builder."""
        spark = MagicMock()
        reader = MagicMock()
        spark.read.format.return_value = reader
        reader.option.return_value = reader

        # The DataFrame returned by reader.load()
        df = MagicMock()
        df.count.return_value = 42
        reader.load.return_value = df

        return spark, reader, df

    @pytest.fixture
    def glue_context(self, mock_spark_session):
        """Mock GlueContext with spark_session attached."""
        spark, _, _ = mock_spark_session
        ctx = MagicMock()
        ctx.spark_session = spark
        return ctx

    @pytest.fixture
    def base_args(self):
        """Minimal parsed_args for a MongoDB migrate run."""
        return {
            'source-type': 'mongodb',
            'source-collection': 'orders',
            'table': 'Orders',
            'mongodb-connection': 'my-glue-connection',
            's3-bucket-name': 'bulk-bucket',
            'JOB_RUN_ID': 'jr_migrate_001',
        }

    def test_reads_from_mongodb_format(self, migrate_module, glue_context,
                                        mock_spark_session, base_args, monkeypatch):
        """run() must call spark.read.format('mongodb') to use the MongoDB connector."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        migrate_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        spark.read.format.assert_called_once_with('mongodb')

    def test_sets_mongodb_connection_option(self, migrate_module, glue_context,
                                             mock_spark_session, base_args, monkeypatch):
        """run() must configure the MongoDB connection name via connector options."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        migrate_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        # Verify that connection name was set as an option
        option_calls = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
        assert 'connectionName' in option_calls or 'connection' in option_calls, (
            "Must set MongoDB Glue connection name via reader options"
        )
        conn_value = option_calls.get('connectionName', option_calls.get('connection'))
        assert conn_value == 'my-glue-connection'

    def test_sets_mongodb_collection_option(self, migrate_module, glue_context,
                                             mock_spark_session, base_args, monkeypatch):
        """run() must configure the source collection in the reader options."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        migrate_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        # Check that collection was set (could be 'database' + 'collection' or 'database.collection')
        option_calls = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
        # The collection identifier should be present somewhere in the options
        all_values = list(option_calls.values())
        assert 'orders' in all_values or any('orders' in str(v) for v in all_values), (
            "Must configure the source collection 'orders' in reader options"
        )


class TestMigrateRunWritesToDynamoDB:
    """run() writes the MongoDB data to DynamoDB via write_dynamodb_dataframe."""

    @pytest.fixture
    def migrate_module(self, monkeypatch):
        from python_modules import migrate as mod
        if not hasattr(mod, 'get_error_message'):
            mod.get_error_message = lambda e: str(e)
        return mod

    @pytest.fixture
    def mock_spark_session(self):
        spark = MagicMock()
        reader = MagicMock()
        spark.read.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        df.count.return_value = 10
        reader.load.return_value = df
        return spark, reader, df

    @pytest.fixture
    def glue_context(self, mock_spark_session):
        spark, _, _ = mock_spark_session
        ctx = MagicMock()
        ctx.spark_session = spark
        return ctx

    @pytest.fixture
    def base_args(self):
        return {
            'source-type': 'mongodb',
            'source-collection': 'users',
            'table': 'Users',
            'mongodb-connection': 'prod-mongodb',
            's3-bucket-name': 'bulk-bucket',
            'JOB_RUN_ID': 'jr_migrate_002',
        }

    def test_writes_to_dynamodb_target_table(self, migrate_module, glue_context,
                                              mock_spark_session, base_args, monkeypatch):
        """run() must call write_dynamodb_dataframe with the target DynamoDB table."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        migrate_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        # write_dynamodb_dataframe(glue_context, df, table_name, parsed_args)
        assert call_args.args[0] is glue_context or call_args[0][0] is glue_context
        assert call_args.args[2] == 'Users' or call_args[0][2] == 'Users'

    def test_prints_row_count(self, migrate_module, glue_context,
                               mock_spark_session, base_args, monkeypatch, capsys):
        """run() must print the number of rows migrated."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        migrate_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        out = capsys.readouterr().out
        assert '10' in out, "Should print the count of migrated rows"


class TestMigrateRunWithFilter:
    """run() applies a MongoDB query filter when --filter is provided."""

    @pytest.fixture
    def migrate_module(self, monkeypatch):
        from python_modules import migrate as mod
        if not hasattr(mod, 'get_error_message'):
            mod.get_error_message = lambda e: str(e)
        return mod

    @pytest.fixture
    def mock_spark_session(self):
        spark = MagicMock()
        reader = MagicMock()
        spark.read.format.return_value = reader
        reader.option.return_value = reader
        df = MagicMock()
        df.count.return_value = 5
        reader.load.return_value = df
        return spark, reader, df

    @pytest.fixture
    def glue_context(self, mock_spark_session):
        spark, _, _ = mock_spark_session
        ctx = MagicMock()
        ctx.spark_session = spark
        return ctx

    def test_filter_passed_as_pipeline_option(self, migrate_module, glue_context,
                                               mock_spark_session, monkeypatch):
        """When --filter is provided, it should be set as a MongoDB aggregation pipeline option."""
        spark, reader, df = mock_spark_session
        mock_write = MagicMock()
        monkeypatch.setattr(migrate_module, 'write_dynamodb_dataframe', mock_write)

        args = {
            'source-type': 'mongodb',
            'source-collection': 'orders',
            'table': 'Orders',
            'mongodb-connection': 'my-conn',
            'filter': '{"status": "completed"}',
            's3-bucket-name': 'bulk-bucket',
            'JOB_RUN_ID': 'jr_003',
        }

        migrate_module.run(MagicMock(), MagicMock(), glue_context, args)

        # The filter should appear somewhere in the reader option calls
        option_calls = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
        filter_options = [v for k, v in option_calls.items()
                         if 'pipeline' in k.lower() or 'filter' in k.lower() or 'match' in k.lower()]
        assert len(filter_options) > 0, (
            "Must pass --filter value as a MongoDB connector option (pipeline/filter/match)"
        )
        assert 'completed' in str(filter_options[0]), (
            "Filter option must contain the user's query filter"
        )
