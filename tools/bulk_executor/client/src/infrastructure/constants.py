import os
from enum import Enum

GLUE_VERSION = '5.1'
PYTHON_VERSION = '3'
LOG4J_PROPERTIES_FILE = 'server/src/log4j2.properties'

GLUE_JOB_NAME = 'bulk_dynamodb'
GLUE_JOB_ROOT_ROLE_NAME = 'AWSGlueServiceRoleBulkDynamoDB' # AWSGlueServiceRole prefix intentional.
GLUE_JOB_SERVER_ROOT_PATH = "server/src/root.py"

# Glue 5.x DataFrame-based DynamoDB source requires an attached Glue
# connection of type DYNAMODB to register the data source on the Spark
# classpath. ConnectionProperties is intentionally empty; the connection
# is purely a marker that tells Glue to load the connector library.
GLUE_DYNAMODB_CONNECTION_NAME = 'bulk-dynamodb-connection'

# CloudWatch Log Groups for Glue Jobs
GLUE_LOG_GROUP_ERROR = '/aws-glue/jobs/error'
GLUE_LOG_GROUP_OUTPUT = '/aws-glue/jobs/output'
GLUE_LOG_GROUP_NAMES = [GLUE_LOG_GROUP_ERROR, GLUE_LOG_GROUP_OUTPUT]
GLUE_LOG_GROUP_RETENTION_IN_DAYS = 365 # One year

READ_ONLY_ROLE_ID = "DdbReadOnly"
READ_WRITE_ROLE_ID = "DdbReadWrite"

# Role type constants
ROLE_TYPE_READ_ONLY = 'READ-ONLY'
ROLE_TYPE_READ_WRITE = 'READ-WRITE'
ROLE_TYPE_CUSTOM = 'custom'
READ_WRITE_ROLE_TYPES = [ROLE_TYPE_READ_ONLY, ROLE_TYPE_READ_WRITE]  # Standard role types, excluding custom

PYTHON_MODULE_CLIENT_DIR_PATH = 'server/src/python_modules'
PYTHON_MODULE_CLIENT_ZIP_PATH = 'client/src/infrastructure/tmp/python_modules.zip'
PYTHON_MODULE_SERVER_ZIP_PATH = 'server/src/python_modules.zip'

class GlueJobDefaults(Enum):
    ExecutionClass='STANDARD'
    MaxConcurrentRuns=20
    Retries=0
    Timeout=60
    NumberOfWorkers=220
    WorkerType='G.1X'

# Directories under python_modules that are not verbs (shared utilities, caches)
_NON_VERB_DIRS = {'shared', '__pycache__'}


def discover_verb_requirements(modules_dir=None):
    """Scan verb folders for requirements.txt and return a deduplicated, sorted package list.

    Each verb directory under modules_dir may contain a requirements.txt
    declaring its server-side pip dependencies.  This function reads all such
    files, deduplicates the package names, and returns them as a sorted list.

    Args:
        modules_dir: Path to the python_modules directory.  Defaults to
            PYTHON_MODULE_CLIENT_DIR_PATH.
    """
    if modules_dir is None:
        modules_dir = PYTHON_MODULE_CLIENT_DIR_PATH

    packages = set()

    if not os.path.isdir(modules_dir):
        return []

    for entry in os.listdir(modules_dir):
        if entry in _NON_VERB_DIRS:
            continue
        verb_path = os.path.join(modules_dir, entry)
        if not os.path.isdir(verb_path):
            continue
        req_file = os.path.join(verb_path, 'requirements.txt')
        if os.path.isfile(req_file):
            with open(req_file) as f:
                for line in f:
                    pkg = line.strip()
                    if pkg and not pkg.startswith('#'):
                        packages.add(pkg)

    return sorted(packages)


# Third-party Python modules discovered from per-verb requirements.txt files.
# Kept as a comma-separated string for backwards compatibility with bootstrap.
THIRD_PARTY_PYTHON_MODULES = ','.join(discover_verb_requirements())
