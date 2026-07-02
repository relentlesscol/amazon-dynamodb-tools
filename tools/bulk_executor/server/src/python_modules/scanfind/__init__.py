import json
import sys
from decimal import Decimal

import boto3
from botocore.config import Config
from pyspark import AccumulatorParam

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.errors import *
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info, get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Return int if possible, else float
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


class DecimalDecoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        if isinstance(result, dict):
            return {k: Decimal(str(v)) if isinstance(v, float) else v
                    for k, v in result.items()}
        return result


class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    index_name = parsed_args.get('index')
    filter_expression = parsed_args.get('filter_expression')
    expression_values = parsed_args.get('expression_values')
    expression_names = parsed_args.get('expression_names')
    limit = parsed_args.get('limit')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name, index_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    items_accumulator = spark_context.accumulator([], ListAccumulator())
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions
    try:
        parallelize_count = 200
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.foreach(lambda worker_id: _find_data(
            monitor_options, table_name, index_name,
            filter_expression, expression_values, expression_names, limit,
            worker_id, parallelize_count,
            items_accumulator, error_accumulator, rate_limiter_shared_config
        ))
        rdd.count()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    # Print matching items as JSON lines
    found_items = items_accumulator.value
    for item in found_items:
        print(json.dumps(item, cls=DecimalEncoder))

    print(f"Total items found: {len(found_items)}")


def _find_data(monitor_options, table_name, index_name, filter_expression,
               expression_values, expression_names, limit,
               segment, total_segments,
               items_accumulator, error_accumulator, rate_limiter_shared_config):

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
            scan_kwargs["ExpressionAttributeNames"] = json.loads(expression_names, cls=DecimalDecoder)
        if expression_values:
            scan_kwargs["ExpressionAttributeValues"] = json.loads(expression_values, cls=DecimalDecoder)

        while True:
            response = table.scan(**scan_kwargs)
            items = response.get("Items", [])

            if items:
                items_accumulator.add(items)

            # Check if limit reached
            if limit is not None:
                collected = len(items_accumulator.value) if hasattr(items_accumulator.value, '__len__') else 0
                if collected >= limit or len(items) >= limit:
                    break

            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
    finally:
        rate_limiter_worker.shutdown()
