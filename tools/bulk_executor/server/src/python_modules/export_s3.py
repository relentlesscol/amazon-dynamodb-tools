"""Export a DynamoDB table to S3 using the native ExportTableToPointInTime API.

This command wraps the DynamoDB native export functionality, which creates
a point-in-time snapshot of the table in S3 without consuming read capacity.
"""

import boto3


def run(job, spark_context, glue_context, parsed_args):
    """Execute a native DynamoDB export-to-S3 operation.

    Args:
        job: Glue job object (unused for native API calls).
        spark_context: Spark context (unused for native API calls).
        glue_context: Glue context (unused for native API calls).
        parsed_args: Dict with 'table', 's3-bucket-name', and optional 's3-prefix'.
    """
    table_name = parsed_args.get('table')
    bucket_name = parsed_args.get('s3-bucket-name')
    s3_prefix = parsed_args.get('s3-prefix', '')

    client = boto3.client('dynamodb')

    # Build the S3 destination
    s3_bucket_owner = None
    export_kwargs = {
        'TableArn': _get_table_arn(client, table_name),
        'S3Bucket': bucket_name,
    }
    if s3_prefix:
        export_kwargs['S3Prefix'] = s3_prefix

    response = client.export_table_to_point_in_time(**export_kwargs)

    export_desc = response.get('ExportDescription', {})
    export_arn = export_desc.get('ExportArn', 'N/A')
    export_status = export_desc.get('ExportStatus', 'UNKNOWN')

    print(f"Export started for table '{table_name}'")
    print(f"  Export ARN: {export_arn}")
    print(f"  Status: {export_status}")
    print(f"  Destination: s3://{bucket_name}/{s3_prefix}")


def _get_table_arn(client, table_name):
    """Resolve a table name to its ARN."""
    response = client.describe_table(TableName=table_name)
    return response['Table']['TableArn']
