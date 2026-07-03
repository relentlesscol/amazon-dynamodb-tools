"""Import a DynamoDB table from S3 using the native ImportTable API.

This command wraps the DynamoDB native import functionality, which creates
a new table from data stored in S3.
"""

import boto3


def run(job, spark_context, glue_context, parsed_args):
    """Execute a native DynamoDB import-from-S3 operation.

    Args:
        job: Glue job object (unused for native API calls).
        spark_context: Spark context (unused for native API calls).
        glue_context: Glue context (unused for native API calls).
        parsed_args: Dict with 'table', 's3-bucket-name', 's3-prefix',
                     and 'input-format'.
    """
    table_name = parsed_args.get('table')
    bucket_name = parsed_args.get('s3-bucket-name')
    s3_prefix = parsed_args.get('s3-prefix', '')
    input_format = parsed_args.get('input-format', 'DYNAMODB_JSON')

    client = boto3.client('dynamodb')

    import_kwargs = {
        'S3BucketSource': {
            'S3Bucket': bucket_name,
        },
        'InputFormat': input_format,
        'TableCreationParameters': {
            'TableName': table_name,
            'AttributeDefinitions': [
                {'AttributeName': 'pk', 'AttributeType': 'S'},
            ],
            'KeySchema': [
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
            ],
            'BillingMode': 'PAY_PER_REQUEST',
        },
    }
    if s3_prefix:
        import_kwargs['S3BucketSource']['S3KeyPrefix'] = s3_prefix

    response = client.import_table(**import_kwargs)

    import_desc = response.get('ImportTableDescription', {})
    import_arn = import_desc.get('ImportArn', 'N/A')
    import_status = import_desc.get('ImportStatus', 'UNKNOWN')

    print(f"Import started for table '{table_name}'")
    print(f"  Import ARN: {import_arn}")
    print(f"  Status: {import_status}")
    print(f"  Source: s3://{bucket_name}/{s3_prefix}")
