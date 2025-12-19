import boto3
import importlib
import json
import math
import sys
from awsglue.transforms import Map
from botocore.exceptions import ClientError
from python_modules.shared.errors import *
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_dynamodb_throughput_configs
from python_modules.shared.table_info import get_and_print_dynamodb_table_info


# SQL to DynamoDB type conversion mapping
SQL_TO_DYNAMODB_TYPE_MAP = {
    # Numeric types
    'SMALLINT': 'N',
    'INTEGER': 'N',
    'BIGINT': 'N',
    'DECIMAL': 'N',
    'NUMERIC': 'N',
    'REAL': 'N',
    'DOUBLE': 'N',
    'DOUBLE PRECISION': 'N',
    'FLOAT': 'N',
    
    # String types
    'VARCHAR': 'S',
    'CHAR': 'S',
    'TEXT': 'S',
    'BPCHAR': 'S',  # Redshift specific
    
    # Boolean
    'BOOLEAN': 'BOOL',
    'BOOL': 'BOOL',
    
    # Date/Time - stored as strings in DynamoDB
    'DATE': 'S',
    'TIMESTAMP': 'S',
    'TIMESTAMPTZ': 'S',
    'TIME': 'S',
    'TIMETZ': 'S',
    
    # Binary
    'BYTEA': 'B',
    'VARBYTE': 'B',  # Redshift specific
}


def convert_sql_type_to_dynamodb(sql_type):
    """
    Convert SQL data type to DynamoDB type.
    
    Args:
        sql_type: SQL data type string
        
    Returns:
        DynamoDB type code (S, N, B, BOOL, etc.)
    """
    sql_type_upper = sql_type.upper()
    
    # Handle types with parameters like VARCHAR(255)
    base_type = sql_type_upper.split('(')[0].strip()
    
    return SQL_TO_DYNAMODB_TYPE_MAP.get(base_type, 'S')  # Default to String if unknown


def convert_sql_value_to_dynamodb(value, sql_type):
    """
    Convert SQL value to DynamoDB-compatible format.
    
    Args:
        value: The value from SQL source
        sql_type: SQL data type
        
    Returns:
        Converted value suitable for DynamoDB
    """
    if value is None:
        return None
        
    ddb_type = convert_sql_type_to_dynamodb(sql_type)
    
    if ddb_type == 'N':
        # Convert numeric types to string representation
        return str(value)
    elif ddb_type == 'BOOL':
        # Ensure boolean
        return bool(value)
    elif ddb_type == 'S':
        # Convert to string
        return str(value)
    elif ddb_type == 'B':
        # Binary data - keep as is or encode
        return value
    else:
        # Default to string conversion
        return str(value)


def transform_record_for_dynamodb(rec):
    """
    Transform a record from SQL format to DynamoDB format.
    Handles type conversions and removes null values.
    
    Args:
        rec: Record dict from SQL source
        
    Returns:
        Transformed record suitable for DynamoDB
    """
    transformed = {}
    for key, value in rec.items():
        # Skip null values - DynamoDB doesn't store nulls well
        if value is None:
            continue
            
        # For now, convert all values to appropriate types
        # Numbers and booleans stay as is, everything else becomes string
        if isinstance(value, bool):
            transformed[key] = value
        elif isinstance(value, (int, float)):
            # DynamoDB stores numbers as strings internally but boto3 handles this
            transformed[key] = value
        else:
            # Convert to string for safety
            transformed[key] = str(value)
    
    return transformed


def run(job, spark_context, glue_context, parsed_args):
    """
    Main entry point for the import verb.
    Reads data from JDBC source and writes to DynamoDB.
    
    Args:
        job: Glue job object
        spark_context: Spark context
        glue_context: Glue context
        parsed_args: Parsed command-line arguments
    """
    log.debug(f"parsed_args {parsed_args}")
    
    table_name = parsed_args.get('table')
    source_type = parsed_args.get('source_type')
    source_table = parsed_args.get('source_table')
    source_query = parsed_args.get('source_query')
    where_clause = parsed_args.get('where')
    transformer_name = parsed_args.get('transformer')
    
    # Partition parameters for parallel reads
    partition_column = parsed_args.get('partitionColumn')
    lower_bound = parsed_args.get('lowerBound')
    upper_bound = parsed_args.get('upperBound')
    num_partitions = parsed_args.get('numPartitions')
    
    # Get connection name based on source type
    connection_name = None
    if source_type == 'redshift':
        connection_name = parsed_args.get('redshift_connection')
    
    if not connection_name:
        raise Exception(f"Connection name not provided for source type '{source_type}'")
    
    log.info(f"Starting import from {source_type} to DynamoDB table '{table_name}'")
    log.info(f"Using AWS Glue Connection: {connection_name}")
    
    # Build the JDBC read options
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": connection_name,
    }
    
    # Determine the data source - either table or query
    if source_query:
        log.info(f"Using custom SQL query: {source_query}")
        connection_options["query"] = source_query
    elif source_table:
        log.info(f"Reading from source table: {source_table}")
        
        # Build the query with optional WHERE clause
        if where_clause:
            log.info(f"Applying WHERE clause: {where_clause}")
            query = f"SELECT * FROM {source_table} WHERE {where_clause}"
            connection_options["query"] = query
        else:
            connection_options["dbtable"] = source_table
    
    # Add partitioning parameters if provided
    if partition_column and lower_bound and upper_bound and num_partitions:
        log.info(f"Using parallel partitioning on column '{partition_column}'")
        log.info(f"Partition range: {lower_bound} to {upper_bound} ({num_partitions} partitions)")
        connection_options["partitionColumn"] = partition_column
        connection_options["lowerBound"] = lower_bound
        connection_options["upperBound"] = upper_bound
        connection_options["numPartitions"] = str(num_partitions)
    
    # Read data from JDBC source
    log.info("Reading data from JDBC source...")
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="custom.jdbc",
            connection_options=connection_options
        )
    except Exception as e:
        raise Exception(f"Failed to read from JDBC source: {get_error_message(e)}") from None
    
    # Count records
    count = 0
    try:
        count = dynamic_frame.count()
        if count == 0:
            log.error("No data found in source, please check your source configuration")
            return
        log.info(f"\nPreparing to import {count:,} items")
        log.info("Source schema:")
        dynamic_frame.printSchema()
    except Exception as e:
        raise Exception(f"Failed to count records from source: {get_error_message(e)}") from None
    
    # Apply custom transformer if provided
    if transformer_name:
        log.info(f"Applying custom transformer: {transformer_name}")
        try:
            transformer_module = importlib.import_module(f"python_modules.import.{transformer_name}")
            transform_function = getattr(transformer_module, 'transform')
            dynamic_frame = Map.apply(frame=dynamic_frame, f=transform_function)
        except Exception as e:
            raise Exception(f"Failed to apply transformer '{transformer_name}': {get_error_message(e)}") from None
    else:
        # Apply default transformation for SQL to DynamoDB conversion
        log.info("Applying default SQL to DynamoDB type conversion")
        dynamic_frame = Map.apply(frame=dynamic_frame, f=transform_record_for_dynamodb)
    
    # Get throughput configuration for DynamoDB write
    dynamodb_connection_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"])
    dynamodb_connection_options["dynamodb.output.tableName"] = table_name
    
    # Print cost estimation
    try:
        session = boto3.Session()
        print_import_cost_info(session, table_name, count, dynamic_frame)
    except Exception as e:
        log.warning(f"Could not generate cost estimate: {str(e)}")
    
    # Write to DynamoDB
    log.info(f"Writing data to DynamoDB table '{table_name}'...")
    try:
        # Repartition for optimal write performance
        dynamic_frame = dynamic_frame.repartition(30)
        
        glue_context.write_dynamic_frame_from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options=dynamodb_connection_options
        )
        log.info(f"Successfully imported {count:,} items to '{table_name}'")
    except Exception as e:
        raise Exception(f"Error writing to DynamoDB table: {get_error_message(e)}") from None


def check_dynamic_frame_avg_size(dynamic_frame):
    """
    Calculate average item size from a DynamicFrame sample.
    
    Args:
        dynamic_frame: The DynamicFrame to sample
        
    Returns:
        Average item size in bytes
    """
    # Sample up to 100 items
    sample_frame = dynamic_frame.toDF().limit(100)
    items = sample_frame.collect()
    
    total_size = 0
    item_count = 0
    
    for item in items:
        # Convert to dict then to JSON to simulate DynamoDB storage
        item_dict = item.asDict()
        # Calculate size in bytes
        item_size = sys.getsizeof(json.dumps(item_dict))
        total_size += item_size
        item_count += 1
    
    if item_count > 0:
        return total_size / item_count
    else:
        raise Exception("Cannot determine average size without any items")


def print_import_cost_info(session, table_name, num_items, dynamic_frame):
    """
    Print cost estimation for the import operation.
    
    Args:
        session: Boto3 session
        table_name: Target DynamoDB table name
        num_items: Number of items to import
        dynamic_frame: The data frame being imported
    """
    region_name = session.region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    
    # Estimate average item size
    avg_size = check_dynamic_frame_avg_size(dynamic_frame)
    
    # Calculate write units needed
    avg_write_units_per_item = math.ceil(avg_size / 1024)
    write_units = num_items * avg_write_units_per_item
    
    # Get pricing
    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    wru_cost = float(ondemand_pricing.get(table_info['write_pricing_category']))
    od_cost = write_units * wru_cost
    prov_cost = od_cost / 1.5  # Rough estimate
    
    # Print cost information
    log.info("\n" + "="*80)
    log.info("COST ESTIMATION FOR IMPORT OPERATION")
    log.info("="*80)
    log.info(f"Source: JDBC database")
    log.info(f"Target: DynamoDB table '{table_name}'")
    log.info(f"Items to import: {num_items:,}")
    log.info(f"Average item size: {int(avg_size):,} bytes")
    log.info(f"Write units per item: {avg_write_units_per_item}")
    log.info(f"Total write units required: {write_units:,}")
    log.info("")
    log.info("DynamoDB Costs (estimated):")
    if table_info['billing_mode'] == "PROVISIONED":
        log.info(f"  Provisioned mode: ${prov_cost:,.2f} (using {region_name} prices)")
    elif table_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"  On-demand mode: ${od_cost:,.2f} (using {region_name} prices)")
    log.info("")
    log.info("Additional Costs:")
    log.info("  - AWS Glue DPU hours (varies based on data volume and worker configuration)")
    log.info("  - Data transfer costs (if source and target are in different regions/VPCs)")
    log.info("  - JDBC source database query costs (if applicable)")
    log.info("")
    log.info("Note: Secondary indexes will incur additional write costs")
    log.info("="*80 + "\n")
