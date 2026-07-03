"""Verb: import-s3 — wraps DynamoDB's native ImportTable API."""

import boto3
from python_modules.shared.logger import log


def run(job, spark_context, glue_context, parsed_args):
    """Import data from S3 into a DynamoDB table using the native import API."""
    table_name = parsed_args.get('table')
    bucket = parsed_args.get('s3-bucket-name') or parsed_args.get('s3_bucket')
    prefix = parsed_args.get('s3_prefix', '')
    input_format = parsed_args.get('input_format', 'DYNAMODB_JSON')

    key_schema = parsed_args.get('key_schema', [{'AttributeName': 'pk', 'KeyType': 'HASH'}])
    attribute_definitions = parsed_args.get('attribute_definitions',
                                            [{'AttributeName': 'pk', 'AttributeType': 'S'}])

    client = boto3.client('dynamodb')
    response = client.import_table(
        S3BucketSource={
            'S3Bucket': bucket,
            'S3KeyPrefix': prefix,
        },
        InputFormat=input_format,
        TableCreationParameters={
            'TableName': table_name,
            'KeySchema': key_schema,
            'AttributeDefinitions': attribute_definitions,
            'BillingMode': parsed_args.get('billing_mode', 'PAY_PER_REQUEST'),
        },
    )

    import_arn = response['ImportTableDescription']['ImportArn']
    print(f"Import started: {import_arn}")
    print(f"Target table: {table_name}")
    return response
