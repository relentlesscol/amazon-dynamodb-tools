"""Verb: export-s3 — wraps DynamoDB's native ExportTableToPointInTime API."""

import boto3
from python_modules.shared.logger import log


def run(job, spark_context, glue_context, parsed_args):
    """Export a DynamoDB table to S3 using the native export API."""
    table_name = parsed_args.get('table')
    bucket = parsed_args.get('s3-bucket-name') or parsed_args.get('s3_bucket')
    prefix = parsed_args.get('s3_prefix', '')
    export_format = parsed_args.get('export_format', 'DYNAMODB_JSON')

    table_arn = _get_table_arn(table_name)

    s3_destination = f"s3://{bucket}/{prefix}".rstrip('/')

    client = boto3.client('dynamodb')
    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=bucket,
        S3Prefix=prefix,
        ExportFormat=export_format,
    )

    export_arn = response['ExportDescription']['ExportArn']
    print(f"Export started: {export_arn}")
    print(f"Destination: {s3_destination}")
    return response


def _get_table_arn(table_name):
    """Resolve a table name to its ARN."""
    client = boto3.client('dynamodb')
    response = client.describe_table(TableName=table_name)
    return response['Table']['TableArn']
