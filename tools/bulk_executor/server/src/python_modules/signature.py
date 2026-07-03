import hashlib
import json
import sys

import boto3
from botocore.config import Config

sys.path.append('/server/src')
from python_modules.shared.errors import get_error_message
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info,
    get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs
)

from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)


def hash_segment(table_name, monitor_options, segment, total_segments, rate_limiter_shared_config):
    """Scan one segment of a DynamoDB table and return a SHA-256 hex digest of its items."""
    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    try:
        session = rate_limiter_worker.get_session()
        client = session.client('dynamodb', config=Config(
            connect_timeout=4.0,
            read_timeout=4.0,
            retries={'mode': 'standard', 'total_max_attempts': 50}
        ))

        hasher = hashlib.sha256()
        last_evaluated_key = None

        while True:
            kwargs = {
                'TableName': table_name,
                'Segment': segment,
                'TotalSegments': total_segments,
            }
            if last_evaluated_key:
                kwargs['ExclusiveStartKey'] = last_evaluated_key

            response = client.scan(**kwargs)

            for item in response.get('Items', []):
                hasher.update(json.dumps(item, sort_keys=True, separators=(',', ':')).encode())

            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break
    finally:
        rate_limiter_worker.shutdown()

    return hasher.hexdigest()


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    splits = int(parsed_args.get('splits', '400'))
    use_s3 = parsed_args.get('s3')
    bucket = parsed_args.get('s3-bucket-name')
    job_id = parsed_args.get('JOB_RUN_ID')

    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    get_and_print_table_scan_cost(table_info, region_name)

    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=("read"), format="monitor")

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket,
        job_run_id=job_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    try:
        rdd = spark_context.parallelize(range(splits), splits)
        segment_hashes = rdd.map(
            lambda seg: hash_segment(table_name, monitor_options, seg, splits, rate_limiter_shared_config)
        ).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    # Compute overall hash from concatenated segment hashes
    overall_hash = hashlib.sha256(''.join(segment_hashes).encode()).hexdigest()

    if use_s3:
        s3_client = boto3.client('s3')
        # Write per-segment hashes
        for i, h in enumerate(segment_hashes):
            s3_client.put_object(
                Body=h,
                Bucket=bucket,
                Key=f"{job_id}/segment_{i}.txt"
            )
        # Write overall signature
        s3_client.put_object(
            Body=overall_hash,
            Bucket=bucket,
            Key=f"{job_id}/signature.txt"
        )
    else:
        print()
        print("Segment hashes:")
        for i, h in enumerate(segment_hashes):
            print(f"  segment {i}: {h}")
        print()
        print(f"Overall signature: {overall_hash}")
        print()
