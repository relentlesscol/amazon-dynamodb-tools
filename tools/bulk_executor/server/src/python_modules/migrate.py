"""Migrate data from external NoSQL sources into DynamoDB.

Reads from a source (currently MongoDB via the Spark MongoDB connector)
and writes the resulting DataFrame to DynamoDB using write_dynamodb_dataframe.

Issue #108: import data from NoSQL sources into DynamoDB (starting with MongoDB).
"""

import sys

sys.path.append('/server/src')
from python_modules.shared.errors import get_error_message
from python_modules.shared.glue_connector import write_dynamodb_dataframe


def run(job, spark_context, glue_context, parsed_args):
    source_type = parsed_args.get('source-type')
    table_name = parsed_args.get('table')
    collection = parsed_args.get('source-collection')
    connection = parsed_args.get('mongodb-connection')

    spark = glue_context.spark_session

    # Build the MongoDB reader via Spark's MongoDB connector
    reader = (
        spark.read.format('mongodb')
        .option('connectionName', connection)
        .option('collection', collection)
    )

    # Apply optional query filter as an aggregation pipeline
    query_filter = parsed_args.get('filter')
    if query_filter:
        reader = reader.option('pipeline', f'[{{"$match": {query_filter}}}]')

    df = reader.load()
    row_count = df.count()

    write_dynamodb_dataframe(glue_context, df, table_name, parsed_args)

    print(f"Migrated {row_count} rows from {source_type}://{collection} to DynamoDB table '{table_name}'")
