"""Server-side verb: migrate data from MongoDB into DynamoDB.

Reads from a MongoDB source collection via Spark's MongoDB connector
and writes the resulting DataFrame to DynamoDB using the shared
write_dynamodb_dataframe helper.
"""

import sys

sys.path.append('/server/src')
from python_modules.shared.errors import get_error_message
from python_modules.shared.glue_connector import write_dynamodb_dataframe


def run(job, spark_context, glue_context, parsed_args):
    """Read from MongoDB and write to DynamoDB."""
    source_collection = parsed_args['source-collection']
    connection_name = parsed_args['mongodb-connection']
    target_table = parsed_args['table']

    spark = glue_context.spark_session

    reader = (
        spark.read.format('mongodb')
        .option('connectionName', connection_name)
        .option('collection', source_collection)
    )

    # Apply optional query filter as an aggregation pipeline
    filter_expr = parsed_args.get('filter')
    if filter_expr:
        reader = reader.option('pipeline', filter_expr)

    df = reader.load()

    row_count = df.count()

    write_dynamodb_dataframe(glue_context, df, target_table, parsed_args)

    print(f"Migrated {row_count} rows to {target_table}")
