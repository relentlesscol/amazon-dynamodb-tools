import boto3

from python_modules.shared.logger import log


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    bucket = parsed_args.get('s3-bucket-name')
    prefix = parsed_args.get('s3-prefix', f'imports/{table_name}')
    input_format = parsed_args.get('format', 'DYNAMODB_JSON')

    client = boto3.client('dynamodb')

    log.info(f"Starting native import into '{table_name}' from s3://{bucket}/{prefix}")

    response = client.import_table(
        S3BucketSource={
            'S3Bucket': bucket,
            'S3KeyPrefix': prefix,
        },
        InputFormat=input_format,
        TableCreationParameters={
            'TableName': table_name,
            'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            'AttributeDefinitions': [{'AttributeName': 'pk', 'AttributeType': 'S'}],
            'BillingMode': 'PAY_PER_REQUEST',
        },
    )

    import_arn = response['ImportTableDescription']['ImportArn']
    log.info(f"Import started: {import_arn}")
    log.info(f"Status: {response['ImportTableDescription']['ImportStatus']}")
