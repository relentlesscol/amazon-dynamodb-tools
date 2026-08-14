import base64
import json
import sys

import boto3
from botocore.config import Config
from pyspark import AccumulatorParam, StorageLevel

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.bulk_executor_error import BulkExecutorError
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


# Console preview cap, matching find / sql / diff. Console delivery goes through
# CloudWatch Live Tail, which is bandwidth-limited and lossy, so the full result
# always goes to S3 and only a bounded preview is printed. See
# ai_lint/rules/large_output_to_s3.md.
TOP_N = 10

DEFAULT_SEGMENTS = 200


def print_dynamodb_table_info(table_name, index_name=None):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name, index_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)


def run(job, spark_context, glue_context, parsed_args):
    """Parallel segmented scan that emits matching items as DynamoDB JSON.

    This is `scancount` with items instead of a count: the same fan-out of
    `segments` parallel scan segments across the Glue cluster, the same rate
    limiter, the same 50-retry client config. The difference is the output
    contract — every item is written to S3 in *wire format* (the
    ``{"attr": {"S": "..."}}`` shape a DynamoDB `scan` actually returns), one
    JSON object per line, with full type fidelity. That is the whole point of
    the verb (issues #94 / #184): `find` reads through the Glue DataFrame
    connector, which flattens DynamoDB's type system (sets become lists,
    numbers become doubles, binary is lost), so its output cannot be loaded
    back into a table faithfully. `scanfind` output can.
    """
    table_name = parsed_args.get('table')
    index_name = parsed_args.get('index')
    filter_expression = parsed_args.get('filter_expression')
    expression_values = parsed_args.get('expression_values')
    expression_names = parsed_args.get('expression_names')
    segments = int(parsed_args.get('segments', DEFAULT_SEGMENTS))
    limit = _parse_limit(parsed_args.get('limit'))

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    # Same output layout as find / sql / diff.
    s3_output_location = f"s3://{bucket_name}/output/{job_run_id}"

    print_dynamodb_table_info(table_name, index_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    try:
        # Distribute work among partitions, each knowing what segment it's to
        # handle. flatMap (not map/collect) is deliberate: each worker *yields*
        # its items, so a partition's items stream straight into the S3 write
        # and are never gathered on the driver. Collecting items would put the
        # whole result set in driver memory and OOM on any real table.
        rdd = spark_context.parallelize(list(range(segments)), segments)
        lines_rdd = rdd.flatMap(lambda segment: _scan_segment(
            monitor_options, table_name, index_name, filter_expression,
            expression_values, expression_names, segment, segments, limit,
            error_accumulator, rate_limiter_shared_config))

        if limit is None:
            # Cache so the count and the preview don't re-run the scan (and so
            # re-read the table and pay for it twice), same as find does.
            # MEMORY_AND_DISK rather than cache()/MEMORY_ONLY: an RDD of items
            # is far bigger than a count, and spilling to executor disk is much
            # cheaper than recomputing a full table scan.
            lines_rdd = lines_rdd.persist(StorageLevel.MEMORY_AND_DISK)
            count = lines_rdd.count()
            _write_json_lines(lines_rdd, s3_output_location)
            preview = lines_rdd.take(TOP_N)
        else:
            # take() is what makes --limit a *global* cap: each scanned segment
            # already stops at `limit` items (see _scan_segment), and take()
            # trims across segments to exactly min(limit, matches). It also
            # stops launching segments once it has enough, so a small --limit
            # scans only a few segments instead of all of them.
            #
            # This is the one path where results pass through the driver, and
            # it is bounded by --limit on purpose. find has the same shape (its
            # comment notes a --limit moves all the data to a single worker).
            lines = lines_rdd.take(limit)
            count = len(lines)
            _write_json_lines(spark_context.parallelize(lines, 1), s3_output_location)
            preview = lines[:TOP_N]
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        # Unlike scancount, a failed worker here may already have written items,
        # so say so rather than leaving a half-populated prefix unexplained.
        raise Exception(
            f"{first_error} (items scanned before the failure may already have "
            f"been written to {s3_output_location}/)"
        ) from None

    _print_preview(preview, count)
    print(f"Wrote {count:,} items in DynamoDB JSON format to {s3_output_location}/")
    print()


def _wire_json_default(value):
    """Re-encode a binary attribute value as base64, which is its wire form.

    The one place the low-level client does not hand back exactly what came off
    the wire: botocore parses DynamoDB's `B` / `BS` blob shapes by base64-
    *decoding* them, so a binary attribute arrives as Python `bytes`, which
    `json.dumps` cannot serialize at all. Base64 is what DynamoDB JSON uses for
    binary, so encoding it back is a faithful round-trip, not a lossy
    conversion — `b"\\x00\\x01"` becomes `"AAE="`, exactly the string the API
    sent and exactly what a loader must send back to write the same bytes.

    Anything else is left to raise TypeError: an unexpected type in an item
    means an assumption about the client's output broke, and silently
    stringifying it would put a corrupt value in output that is supposed to be
    byte-faithful.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(value).decode('ascii')
    raise TypeError(
        f"Cannot serialize {type(value).__name__} to DynamoDB JSON; "
        f"expected the low-level DynamoDB client's wire-format types"
    )


def _to_wire_json(item):
    """Serialize one low-level-client item to a single line of DynamoDB JSON.

    No encoder rewrites values here (contrast scancount's DecimalEncoder): the
    item is already wire format, so numbers stay as `{"N": "5"}` strings, sets
    keep their SS/NS/BS keys, `{"NULL": true}` survives instead of collapsing to
    a dropped attribute, and key order is preserved. `separators` only removes
    the whitespace json.dumps would otherwise add.
    """
    return json.dumps(item, separators=(',', ':'), default=_wire_json_default)


def _parse_limit(raw_limit):
    """Validate the optional --limit. The client already rejects a bad value;
    this is defense in depth for a job launched with raw Glue arguments."""
    if raw_limit is None:
        return None
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        raise BulkExecutorError(f"Invalid 'limit': {raw_limit!r} is not an integer") from None
    if limit < 1:
        raise BulkExecutorError(f"Invalid 'limit': must be a positive integer, got {limit}")
    return limit


def _write_json_lines(lines_rdd, s3_output_location):
    """Write the already-serialized JSON lines to S3, one item per line.

    ``saveAsTextFile`` writes each string verbatim, which is the only way to
    keep wire-format fidelity. find's ``spark.read.json(rdd)`` +
    ``df.write.json()`` route must NOT be copied here: it infers one Spark
    schema across every item, so items with differing attributes get null-padded
    into a union schema, a number that appears as both ``{"N":"1"}`` and a
    string somewhere else collapses to one type, key order is rewritten, and a
    heterogeneous table can fail schema resolution outright. Passing the bytes
    through untouched is the requirement, so no DataFrame ever sees the items.
    """
    lines_rdd.saveAsTextFile(s3_output_location)


def _print_preview(preview_lines, count):
    """Print at most TOP_N items plus an explicit truncation notice, so the
    console never grows with the result set and truncation is never silent."""
    if count <= TOP_N:
        print(f"{count} matching items:")
    else:
        print(f"First {TOP_N} matching items:")
    for line in preview_lines:
        print(line)
    if count > TOP_N:
        print(f"...and {count - TOP_N} more not printed")
    print()


def _build_scan_kwargs(table_name, index_name, filter_expression,
                       expression_values, expression_names, segment,
                       total_segments, limit):
    """Build the kwargs for one segment's low-level `scan` call."""
    scan_kwargs = {
        "TableName": table_name,
        "Segment": segment,
        "TotalSegments": total_segments
    }

    if index_name:
        scan_kwargs["IndexName"] = index_name
        # Select is deliberately omitted for an index: an index scan defaults to
        # ALL_PROJECTED_ATTRIBUTES, which is valid for every projection type,
        # whereas Select=ALL_ATTRIBUTES is a ValidationException unless the
        # index projects ALL (and on a KEYS_ONLY / INCLUDE index it would be
        # asking DynamoDB to fetch from the base table anyway).
    else:
        scan_kwargs["Select"] = "ALL_ATTRIBUTES"

    if filter_expression:
        scan_kwargs["FilterExpression"] = filter_expression

    # Plain json.loads, no Decimal coercion: the low-level client takes these
    # substitutions in wire format already ({":v": {"N": "5"}}), so whatever
    # valid JSON the caller passed is exactly what the API wants. Coercing
    # floats to Decimal here (as scancount must, because the resource layer
    # re-serializes them) would corrupt them.
    if expression_names:
        scan_kwargs["ExpressionAttributeNames"] = json.loads(expression_names)
    if expression_values:
        scan_kwargs["ExpressionAttributeValues"] = json.loads(expression_values)

    if limit is not None and not filter_expression:
        # Limit caps items *evaluated*, not items returned, so it only lines up
        # with our item cap when nothing is filtered out. With a
        # FilterExpression it would cut the scan short and silently return fewer
        # matches than exist, so the pagination loop does the capping instead.
        scan_kwargs["Limit"] = limit

    return scan_kwargs


def _scan_segment(monitor_options, table_name, index_name, filter_expression,
                  expression_values, expression_names, segment, total_segments,
                  limit, error_accumulator, rate_limiter_shared_config):
    """Yield one line of wire-format DynamoDB JSON per matching item in `segment`.

    A generator, not a list: Spark consumes it lazily as it writes, so a
    segment's items are never all held in executor memory at once.

    Reads through ``session.client('dynamodb')`` — the low-level client — and
    NOT ``session.resource('dynamodb').Table(...)``. The resource layer runs
    every item through a TypeDeserializer, turning ``{"N": "5"}`` into
    ``Decimal('5')``, ``{"SS": [...]}`` into a Python ``set``, and ``{"NULL":
    true}`` into ``None``; the wire types are then unrecoverable. The low-level
    client hands back ``response["Items"]`` already in wire form, so each item
    is serialized straight to JSON with no encoder in the path.

    ``limit`` here is a per-segment cap. It bounds the work a single segment
    does; ``run`` trims across segments for the global total.

    Any exception raised while scanning is recorded on ``error_accumulator`` and
    then swallowed, so the RateLimiterWorker is always shut down and the items
    found before the failure are still yielded. ``run`` reports the first error
    after the write completes.
    """
    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    session = rate_limiter_worker.get_session()
    dynamodb_client = session.client('dynamodb', config=Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={
            'mode': 'standard',
            'total_max_attempts': 50
        }
    ))

    emitted = 0

    try:
        scan_kwargs = _build_scan_kwargs(
            table_name, index_name, filter_expression, expression_values,
            expression_names, segment, total_segments, limit)

        while True:
            response = dynamodb_client.scan(**scan_kwargs) # We do 50 retries within the SDK so shouldn't see a throttle response
            for item in response.get("Items", []):
                yield _to_wire_json(item)
                emitted += 1
                if limit is not None and emitted >= limit:
                    return
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
        # Let control drop down to exit
    finally:
        # In a finally so it also runs on the early return above, and if Spark
        # abandons the generator once a --limit is satisfied.
        rate_limiter_worker.shutdown()
        print(f"Worker {segment}/{total_segments} emitted {emitted} items.")
