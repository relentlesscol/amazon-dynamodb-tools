import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

ALLOWED_ARGUMENTS = {
    'verb', 'table', 'source_type', 'source_table', 'source_query', 'where',
    'redshift_connection', 'transformer', 'partitionColumn', 'lowerBound', 
    'upperBound', 'numPartitions'
}

ALLOWED_SOURCE_TYPES = {'redshift'}

help_text = f"""
    Purpose of "import":
        Import data from JDBC-compatible relational databases into DynamoDB tables.
        Currently supports Amazon Redshift with extensible design for other sources.
        
    Parameters:
        Required --table parameter: Target DynamoDB table name
        Required --source-type parameter: Type of source database (currently: 'redshift')
        Required --redshift-connection parameter: AWS Glue Connection name for Redshift
        Required ONE of:
            --source-table: Name of the source table to import
            --source-query: Custom SQL query to execute (mutually exclusive with --source-table)
        Optional --where: WHERE clause for filtering (only valid with --source-table)
        Optional --transformer: Python script for schema mapping (SQL to NoSQL)
        Optional --partitionColumn: Column to use for parallel reads (improves performance)
        Optional --lowerBound: Lower bound for partitionColumn
        Optional --upperBound: Upper bound for partitionColumn
        Optional --numPartitions: Number of partitions for parallel reads
        
    Examples:
        # Import from a Redshift table
        bulk import --table orders --source-type redshift --redshift-connection my-redshift-conn --source-table public.orders
        
        # Import with a WHERE clause for filtering
        bulk import --table orders --source-type redshift --redshift-connection my-redshift-conn --source-table public.orders --where "order_date > '2024-01-01'"
        
        # Import using a custom SQL query
        bulk import --table orders --source-type redshift --redshift-connection my-redshift-conn --source-query "SELECT order_id, customer_id, total FROM public.orders WHERE status = 'completed'"
        
        # Import with parallel partitioning for better performance
        bulk import --table orders --source-type redshift --redshift-connection my-redshift-conn --source-table public.orders --partitionColumn order_id --lowerBound 1 --upperBound 1000000 --numPartitions 10
        
    Notes:
        - The --redshift-connection must be an AWS Glue Connection configured with Redshift access
        - Data type conversion from SQL types to DynamoDB types happens automatically
        - Use --transformer for custom schema mappings if needed
        - Partitioning parameters can significantly improve performance for large tables
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk import", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Target DynamoDB table name')
    parser.add_argument('--source-type', required=True, type=str, choices=list(ALLOWED_SOURCE_TYPES), 
                        help='Type of source database (currently supports: redshift)')
    parser.add_argument('--source-table', type=str, default=argparse.SUPPRESS, 
                        help='Source table name (mutually exclusive with --source-query)')
    parser.add_argument('--source-query', type=str, default=argparse.SUPPRESS, 
                        help='Custom SQL query (mutually exclusive with --source-table)')
    parser.add_argument('--where', type=str, default=argparse.SUPPRESS, 
                        help='WHERE clause for filtering (only valid with --source-table)')
    parser.add_argument('--redshift-connection', type=str, default=argparse.SUPPRESS, 
                        help='AWS Glue Connection name for Redshift')
    parser.add_argument('--transformer', type=str, default=argparse.SUPPRESS, 
                        help='Python script for custom schema mapping')
    parser.add_argument('--partitionColumn', type=str, default=argparse.SUPPRESS, 
                        help='Column to use for parallel reads')
    parser.add_argument('--lowerBound', type=str, default=argparse.SUPPRESS, 
                        help='Lower bound for partitionColumn')
    parser.add_argument('--upperBound', type=str, default=argparse.SUPPRESS, 
                        help='Upper bound for partitionColumn')
    parser.add_argument('--numPartitions', type=int, default=argparse.SUPPRESS, 
                        help='Number of partitions for parallel reads')
    
    args = parser.parse_args()
    result = args.__dict__

    # Validate required parameters based on source type
    source_type = getattr(args, 'source_type')
    
    if source_type == 'redshift':
        if 'redshift_connection' not in result:
            parser.error('--redshift-connection is required when --source-type is redshift')
    
    # Validate mutually exclusive source-table and source-query
    has_source_table = 'source_table' in result
    has_source_query = 'source_query' in result
    
    if not has_source_table and not has_source_query:
        parser.error('Either --source-table or --source-query must be provided')
    
    if has_source_table and has_source_query:
        parser.error('--source-table and --source-query are mutually exclusive')
    
    # Validate WHERE clause only works with source-table
    if 'where' in result and not has_source_table:
        parser.error('--where clause can only be used with --source-table, not with --source-query')
    
    # Validate partition parameters
    partition_params = ['partitionColumn', 'lowerBound', 'upperBound', 'numPartitions']
    provided_partition_params = [p for p in partition_params if p in result]
    
    if provided_partition_params:
        if len(provided_partition_params) != len(partition_params):
            missing = set(partition_params) - set(provided_partition_params)
            parser.error(f'When using partitioning, all partition parameters must be provided. Missing: {", ".join(missing)}')
    
    # Check only allowed arguments
    for arg in result:
        if arg not in ALLOWED_ARGUMENTS and not arg.startswith("X"):
            parser.error(f'argument [{arg}] is not allowed for import commands')
    
    # Validate target table exists
    utils.validate_tables(env_configs, parser, result['table'])

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
