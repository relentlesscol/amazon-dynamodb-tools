import importlib
import json
import math
import sys
from decimal import Decimal

import boto3
import botocore
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.config import Config
from pyspark import AccumulatorParam
from pyspark.context import SparkContext


class DecimalEncoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        return {k: Decimal(str(v)) if isinstance(v, float) else v
                for k, v in result.items()}


# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.errors import *
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info, get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs)


class DictAccumulator(AccumulatorParam):
    """Accumulates dict values by merging keys with addition."""
    def zero(self, initialValue):
        return {}

    def addInPlace(self, v1, v2):
        for k, v in v2.items():
            v1[k] = v1.get(k, 0) + v
        return v1


class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1


DYNAMO_DB_THROTTLE_EXCEPTION = 'ProvisionedThroughputExceededException'
DYNAMO_DB_VALIDATION_EXCEPTION = 'ValidationException'


def _compute_item_size(item):
    """Estimate the DynamoDB-marshalled size of an item in bytes.

    This approximates the wire size: attribute name lengths + value sizes.
    """
    size = 0
    for attr_name, attr_value in item.items():
        # Attribute name size
        size += len(attr_name.encode('utf-8'))
        # Attribute value size (simplified estimation)
        if isinstance(attr_value, str):
            size += len(attr_value.encode('utf-8'))
        elif isinstance(attr_value, (int, float, Decimal)):
            # Numbers are stored as variable-length; approximate
            size += len(str(attr_value))
        elif isinstance(attr_value, bytes):
            size += len(attr_value)
        elif isinstance(attr_value, bool):
            size += 1
        elif isinstance(attr_value, dict):
            size += _compute_item_size(attr_value)
        elif isinstance(attr_value, (list, set)):
            for elem in attr_value:
                if isinstance(elem, str):
                    size += len(elem.encode('utf-8'))
                elif isinstance(elem, (int, float, Decimal)):
                    size += len(str(elem))
                elif isinstance(elem, bytes):
                    size += len(elem)
                else:
                    size += len(str(elem))
        elif attr_value is None:
            size += 1
        else:
            size += len(str(attr_value))
    return size


def _get_bucket(size_bytes):
    """Return the KB bucket index for a given size in bytes.

    Bucket 0 = 0-1 KB, bucket 1 = 1-2 KB, etc.
    """
    return size_bytes // 1024


def print_dynamodb_table_info(table_name, index_name=None):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name, index_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    index_name = parsed_args.get('index')
    filter_expression = parsed_args.get('filter_expression')
    expression_values = parsed_args.get('expression_values')
    expression_names = parsed_args.get('expression_names')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    print_dynamodb_table_info(table_name, index_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    # Accumulators: histogram dict, total items, errors
    histogram_accumulator = spark_context.accumulator({}, DictAccumulator())
    total_items_accumulator = spark_context.accumulator(0)
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions
    try:
        parallelize_count = 200
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.foreach(lambda worker_id: _size_data(
            monitor_options, table_name, index_name,
            filter_expression, expression_values, expression_names,
            worker_id, parallelize_count,
            histogram_accumulator, total_items_accumulator,
            error_accumulator, rate_limiter_shared_config
        ))
        rdd.count()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    # Print histogram results
    histogram = histogram_accumulator.value
    total_items = total_items_accumulator.value

    print(f"\nSize Distribution Histogram")
    print(f"Total items analyzed: {total_items:,}")
    print(f"{'Bucket':<12} {'Count':<10} {'Bar'}")
    print("-" * 50)

    if histogram:
        max_count = max(histogram.values())
        for bucket_idx in sorted(histogram.keys()):
            count = histogram[bucket_idx]
            lo = bucket_idx
            hi = bucket_idx + 1
            label = f"{lo}-{hi} KB"
            bar_len = int((count / max_count) * 30) if max_count > 0 else 0
            bar = '#' * bar_len
            print(f"{label:<12} {count:<10} {bar}")
    else:
        print("No items found.")

    print(f"\nTotal: {total_items:,} items")


def _size_data(monitor_options, table_name, index_name,
               filter_expression, expression_values, expression_names,
               segment, total_segments,
               histogram_accumulator, total_items_accumulator,
               error_accumulator, rate_limiter_shared_config):
    """Worker function that scans a segment and computes item size buckets."""

    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    session = rate_limiter_worker.get_session()
    dynamodb_resource = session.resource('dynamodb', config=Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={
            'mode': 'standard',
            'total_max_attempts': 50
        }
    ))

    local_histogram = {}
    local_count = 0

    try:
        table = dynamodb_resource.Table(table_name)

        scan_kwargs = {
            "TableName": table_name,
            "Segment": segment,
            "TotalSegments": total_segments
        }
        if index_name:
            scan_kwargs["IndexName"] = index_name
        if filter_expression:
            scan_kwargs["FilterExpression"] = filter_expression
        if expression_names:
            scan_kwargs["ExpressionAttributeNames"] = json.loads(expression_names, cls=DecimalEncoder)
        if expression_values:
            scan_kwargs["ExpressionAttributeValues"] = json.loads(expression_values, cls=DecimalEncoder)

        while True:
            response = table.scan(**scan_kwargs)
            items = response.get("Items", [])
            for item in items:
                size_bytes = _compute_item_size(item)
                bucket = _get_bucket(size_bytes)
                local_histogram[bucket] = local_histogram.get(bucket, 0) + 1
                local_count += 1

            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
    finally:
        rate_limiter_worker.shutdown()

    histogram_accumulator.add(local_histogram)
    total_items_accumulator.add(local_count)
    return local_count
