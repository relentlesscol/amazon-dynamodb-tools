import importlib
import sys

import boto3
import botocore
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


class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1


def print_dynamodb_table_info(table_name):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')

    executor_name = parsed_args.get('executor')
    executor_function_name = parsed_args.get('executorfunctionname', 'execute')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    module = importlib.import_module(f"python_modules.run.{executor_name}")
    executor_fn = getattr(module, executor_function_name)

    print_dynamodb_table_info(table_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Read-only: run verb does not write to DynamoDB
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    processed_accumulator = spark_context.accumulator(0)
    error_accumulator = spark_context.accumulator(0)
    error_messages_accumulator = spark_context.accumulator([], ListAccumulator())

    try:
        parallelize_count = 800
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.map(lambda worker_id: _run_data(
            monitor_options, table_name, executor_fn, worker_id, parallelize_count,
            processed_accumulator, error_accumulator, error_messages_accumulator
        )).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    print(f"Processed {processed_accumulator.value:,} items, {error_accumulator.value:,} errors")


def _run_data(monitor_options, table_name, executor_fn, segment, total_segments,
              processed_accumulator, error_accumulator, error_messages_accumulator,
              filter_expression=None):
    rate_limiter_worker = RateLimiterWorker(
        shared_config=monitor_options,
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

    table = dynamodb_resource.Table(table_name)

    processed_count = 0
    error_count = 0
    scan_kwargs = {
        "TableName": table_name,
        "Segment": segment,
        "TotalSegments": total_segments
    }

    if filter_expression:
        scan_kwargs["FilterExpression"] = filter_expression

    try:
        while True:
            response = table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                try:
                    executor_fn(item)
                    processed_count += 1
                except Exception as e:
                    error_count += 1

            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    except Exception as e:
        error_messages_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
    finally:
        rate_limiter_worker.shutdown()

    processed_accumulator.add(processed_count)
    error_accumulator.add(error_count)
    return 0
