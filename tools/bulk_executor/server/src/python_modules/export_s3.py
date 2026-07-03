import boto3

from python_modules.shared.logger import log


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    bucket = parsed_args.get('s3-bucket-name')
    prefix = parsed_args.get('s3-prefix', f'exports/{table_name}')

    client = boto3.client('dynamodb')

    log.info(f"Starting native export of '{table_name}' to s3://{bucket}/{prefix}")

    # Get the table ARN
    table_desc = client.describe_table(TableName=table_name)
    table_arn = table_desc['Table']['TableArn']

    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=bucket,
        S3Prefix=prefix,
        ExportFormat='DYNAMODB_JSON',
    )

    export_arn = response['ExportDescription']['ExportArn']
    log.info(f"Export started: {export_arn}")
    log.info(f"Status: {response['ExportDescription']['ExportStatus']}")
