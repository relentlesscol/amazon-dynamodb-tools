"""Client-side verb module for native DynamoDB S3 export (ExportTableToPointInTime)."""


def get_args():
    """Return the argument definitions for the export command."""
    return {
        'table': {'required': True, 'help': 'Source table name or ARN'},
        's3-bucket': {'required': True, 'help': 'Destination S3 bucket for export'},
        's3-prefix': {'required': False, 'help': 'S3 key prefix for export files'},
        'export-format': {
            'required': False,
            'default': 'DYNAMODB_JSON',
            'choices': ['DYNAMODB_JSON', 'ION'],
            'help': 'Export file format',
        },
    }
