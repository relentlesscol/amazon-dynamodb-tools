"""Migrate verb: reads from a JDBC source (e.g. Redshift) and writes to DynamoDB.

Uses a named AWS Glue Connection to resolve JDBC URL and credentials,
then reads via Spark's JDBC DataFrameReader and writes via
write_dynamodb_dataframe.
"""

import boto3

from python_modules.shared.glue_connector import write_dynamodb_dataframe
from python_modules.shared.logger import log


def run(job, spark_context, glue_context, parsed_args):
    """Read from a JDBC source and write to a DynamoDB table."""
    table_name = parsed_args.get('table')
    connection_name = parsed_args.get('redshift_connection')

    # Resolve JDBC connection details from Glue
    glue_client = boto3.client('glue')
    response = glue_client.get_connection(Name=connection_name)
    conn_props = response['Connection']['ConnectionProperties']
    jdbc_url = conn_props['JDBC_CONNECTION_URL']
    username = conn_props['USERNAME']
    password = conn_props['PASSWORD']

    # Build the dbtable option
    source_query = parsed_args.get('source_query')
    source_table = parsed_args.get('source_table')
    where_clause = parsed_args.get('where')

    if source_query:
        dbtable = f"({source_query}) AS subquery"
    elif where_clause:
        dbtable = f"(SELECT * FROM {source_table} WHERE {where_clause}) AS subquery"
    else:
        dbtable = source_table

    # Read from JDBC source
    spark = glue_context.spark_session
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", username)
        .option("password", password)
        .load()
    )

    row_count = df.count()
    if row_count == 0:
        log.info("Source returned 0 rows — nothing to migrate.")
        return

    log.info(f"Migrating {row_count} items to '{table_name}'")
    write_dynamodb_dataframe(glue_context, df, table_name, parsed_args)
    log.info(f"Wrote {row_count} items to '{table_name}'")
