"""
Example transformer for SQL to DynamoDB import.

This demonstrates how to create custom schema transformations
when importing data from relational databases.

Usage:
    bulk import --table mytable \
        --source-type redshift \
        --redshift-connection my-conn \
        --source-table public.orders \
        --transformer example_transformer
"""


def transform(record):
    """
    Transform a SQL record to DynamoDB format.
    
    This example shows common patterns:
    - Creating composite keys (pk/sk pattern)
    - Converting data types
    - Renaming fields
    - Combining multiple columns
    - Handling null values
    
    Args:
        record: Dict containing SQL column names and values
        
    Returns:
        Dict formatted for DynamoDB
    """
    # Skip records with missing required fields
    if not record.get('customer_id') or not record.get('order_id'):
        return None
    
    # Create composite primary key
    pk = f"CUSTOMER#{record['customer_id']}"
    sk = f"ORDER#{record['order_date']}#{record['order_id']}"
    
    # Build the DynamoDB item
    ddb_item = {
        'pk': pk,
        'sk': sk,
        'order_id': str(record['order_id']),
        'customer_id': str(record['customer_id']),
    }
    
    # Optional fields - only add if present
    if record.get('order_date'):
        ddb_item['order_date'] = str(record['order_date'])
    
    if record.get('total'):
        # Convert to float, DynamoDB will handle as Number
        ddb_item['total'] = float(record['total'])
    
    if record.get('status'):
        ddb_item['status'] = str(record['status'])
    
    if record.get('items_count'):
        ddb_item['items_count'] = int(record['items_count'])
    
    # Combine multiple fields into one
    if record.get('first_name') and record.get('last_name'):
        ddb_item['customer_name'] = f"{record['first_name']} {record['last_name']}"
    
    # Add computed fields
    if record.get('total'):
        total = float(record['total'])
        if total > 1000:
            ddb_item['order_tier'] = 'premium'
        elif total > 100:
            ddb_item['order_tier'] = 'standard'
        else:
            ddb_item['order_tier'] = 'basic'
    
    return ddb_item
