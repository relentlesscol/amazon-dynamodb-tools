"""Client-side verb module for native DynamoDB S3 import (ImportTable)."""


def get_args():
    """Return the argument definitions for the import command."""
    return {
        'table': {'required': True, 'help': 'Destination table name'},
        's3-bucket': {'required': True, 'help': 'Source S3 bucket containing data to import'},
        's3-prefix': {'required': False, 'help': 'S3 key prefix for import files'},
        'input-format': {
            'required': False,
            'default': 'DYNAMODB_JSON',
            'choices': ['DYNAMODB_JSON', 'ION', 'CSV'],
            'help': 'Input file format',
        },
    }
