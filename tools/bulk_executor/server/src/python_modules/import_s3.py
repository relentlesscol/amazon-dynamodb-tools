"""Server-side module for native DynamoDB S3 import (ImportTable).

Wraps the DynamoDB ImportTable API as a bulk executor verb.
"""

import sys

import boto3

sys.path.append('/server/src')


def run(job, spark_context, glue_context, parsed_args):
    """Execute a native DynamoDB import from S3."""
    table_name = parsed_args.get('table')
    s3_bucket = parsed_args.get('s3-bucket')
    s3_prefix = parsed_args.get('s3-prefix', '')
    input_format = parsed_args.get('input-format', 'DYNAMODB_JSON')

    client = boto3.client('dynamodb')

    response = client.import_table(
        S3BucketSource={
            'S3Bucket': s3_bucket,
            'S3KeyPrefix': s3_prefix,
        },
        InputFormat=input_format,
        TableCreationParameters={
            'TableName': table_name,
            'KeySchema': parsed_args.get('key-schema', []),
            'AttributeDefinitions': parsed_args.get('attribute-definitions', []),
            'BillingMode': parsed_args.get('billing-mode', 'PAY_PER_REQUEST'),
        },
    )

    import_arn = response['ImportTableDescription']['ImportArn']
    print(f"Import started: {import_arn}")
    print(f"Source: s3://{s3_bucket}/{s3_prefix}")
    return response
