"""Server-side module for native DynamoDB S3 export (ExportTableToPointInTime).

Wraps the DynamoDB ExportTableToPointInTime API as a bulk executor verb.
"""

import sys

import boto3

sys.path.append('/server/src')


def run(job, spark_context, glue_context, parsed_args):
    """Execute a native DynamoDB export to S3."""
    table_arn = parsed_args.get('table')
    s3_bucket = parsed_args.get('s3-bucket')
    s3_prefix = parsed_args.get('s3-prefix', '')
    export_format = parsed_args.get('export-format', 'DYNAMODB_JSON')

    client = boto3.client('dynamodb')

    s3_destination = f's3://{s3_bucket}'
    if s3_prefix:
        s3_destination = f'{s3_destination}/{s3_prefix}'

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=s3_bucket,
        S3Prefix=s3_prefix,
        ExportFormat=export_format,
    )

    export_arn = response['ExportDescription']['ExportArn']
    print(f"Export started: {export_arn}")
    print(f"Destination: {s3_destination}")
    return response
