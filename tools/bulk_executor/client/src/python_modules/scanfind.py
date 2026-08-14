import argparse
import json
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

# Every shape a DynamoDB AttributeValue can take on the wire. scanfind talks to
# the low-level DynamoDB API, so the caller's --expression-values must already
# be in this form ({":v": {"N": "5"}}), not the plain JSON scancount takes.
ATTRIBUTE_VALUE_TYPE_KEYS = {'S', 'N', 'B', 'SS', 'NS', 'BS', 'M', 'L', 'NULL', 'BOOL'}

help_text = f"""
    Purpose of "scanfind":
        Find items in a DynamoDB table using a parallel scan, and write them to S3 as
        DynamoDB JSON (the wire format, one item per line) with full type fidelity
        Required --table parameter
        Optional --index parameter if scanning a secondary index
        Optional --filter-expression parameter to specify a push-down FilterExpression predicate
        Optional --expression-names parameter to specify the expression names used in the filter-expression
        Optional --expression-values parameter to specify the expression values used in the filter-expression,
            in DynamoDB JSON, e.g. '{{":ts": {{"S": "2025-01-01"}}}}' (see the note below)
        Optional --limit parameter to cap how many items are returned in total
        Optional --segments parameter to control how many parallel scan segments to use (default 200)
        Saves full output to S3 and prints the top few items to console

        Unlike "find", scanfind preserves DynamoDB's type system exactly: sets stay sets,
        numbers stay numbers, binary stays binary, and null attributes are kept. It scans
        directly rather than reading through the Glue DataFrame connector, so its output is
        a faithful representation of what is in the table.

        Note on --expression-values: because scanfind reads through the low-level DynamoDB
        API, values are given in DynamoDB JSON — '{{":n": {{"N": "5"}}}}', not '{{":n": 5}}'.
        This differs from scancount, which takes plain JSON. --expression-names are plain
        strings in both.

    Examples:
        # Find all items in a table
        bulk scanfind --table orders

        # Find items matching a filter expression (uses DynamoDB FilterExpression syntax)
        bulk scanfind --table audit --filter-expression "#ts > :ts" --expression-names '{{"#ts": "timestamp"}}' --expression-values '{{":ts": {{"S": "2025-01-01"}}}}'

        # Find items with a numeric comparison
        bulk scanfind --table orders --filter-expression "total > :min" --expression-values '{{":min": {{"N": "100"}}}}'

        # Scan an index instead of the base table
        bulk scanfind --table orders --index status-index

        # Sample a handful of items to inspect their exact shape
        bulk scanfind --table orders --limit 25

        # Use fewer segments for a smaller table
        bulk scanfind --table orders --segments 10
    """


def json_type(s):
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"JSON type parameter held invalid JSON: {e} | Parsed string: {s}")


def positive_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(f"must be an integer, got: {value}")
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got: {value}")
    return parsed


def validate_expression_values(parser, raw_expression_values):
    """Reject --expression-values that aren't DynamoDB JSON.

    Catching this client-side matters because the failure it prevents is
    expensive and confusing: plain JSON like '{":n": 5}' is accepted by argparse,
    launches a Glue job, and only then comes back as a DynamoDB
    ValidationException from a worker. The wire shape is unambiguous (every
    AttributeValue is a single-key object), so we can check it for free here and
    say exactly what to write instead.
    """
    values = json.loads(raw_expression_values)

    if not isinstance(values, dict):
        parser.error("--expression-values must be a JSON object, e.g. '{\":n\": {\"N\": \"5\"}}'")

    for name, value in values.items():
        if isinstance(value, dict) and len(value) == 1 and next(iter(value)) in ATTRIBUTE_VALUE_TYPE_KEYS:
            continue
        parser.error(
            f"--expression-values entry '{name}' is not DynamoDB JSON: {json.dumps(value)}. "
            f"scanfind reads through the low-level DynamoDB API, so each value needs its "
            f"type, e.g. {{\"S\": \"text\"}}, {{\"N\": \"5\"}}, or {{\"BOOL\": true}} — "
            f"unlike scancount, which takes plain JSON."
        )


def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk scanfind", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--filter-expression', type=str, default=argparse.SUPPRESS, help='Filter expression to push down')
    parser.add_argument('--expression-names', type=json_type, default=argparse.SUPPRESS, help='Expression names to use')
    parser.add_argument('--expression-values', type=json_type, default=argparse.SUPPRESS, help='Expression values to use, in DynamoDB JSON (e.g. \'{":n": {"N": "5"}}\')')
    parser.add_argument('--index', type=str, default=argparse.SUPPRESS, help='Index to use')
    parser.add_argument('--limit', type=positive_int, default=argparse.SUPPRESS, help='Maximum number of items to return in total')
    parser.add_argument('--segments', type=positive_int, default=200, help='Number of parallel scan segments (default 200)')
    args = parser.parse_args()

    if hasattr(args, "filter_expression"):
        if "#" in args.filter_expression and not hasattr(args, "expression_names"):
            parser.error("--filter-expression having name substitution requires --expression-names")
        if ":" in args.filter_expression and not hasattr(args, "expression_values"):
            parser.error("--filter-expression having value substitution requires --expression-values")

    if hasattr(args, "expression_values"):
        validate_expression_values(parser, args.expression_values)

    result = args.__dict__

    if 'index' in result:
        utils.validate_tables(env_configs, parser, result['table'], index=result['index'])
    else:
        utils.validate_tables(env_configs, parser, result['table'])

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
