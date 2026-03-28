"""
Stub module for apps.source_repository.functions.

These functions are expected to be provided by the parent project.
Replace these stubs with actual implementations or import from the main project.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def get_source_credential_dict(user, app_source_name):
    """
    Return credentials dictionary and source object for a given source name.

    This stub reads from environment variables. Update with your actual
    credential retrieval logic.
    """
    if app_source_name == 'METADATA_SYSTEM_INFORMATION':
        credentials = {
            'host': os.environ.get('PGHOST', ''),
            'port': int(os.environ.get('PGPORT', 5432)),
            'user': os.environ.get('PGUSER', ''),
            'password': os.environ.get('PGPASSWORD', ''),
            'database': os.environ.get('PGDATABASE', 'postgres'),
            'sslmode': 'require',
        }
    elif app_source_name == 'DATA_OBSERVABILITY_CONSUMPTION_DB':
        credentials = {
            'host': os.environ.get('PGHOST', ''),
            'port': int(os.environ.get('PGPORT', 5432)),
            'user': os.environ.get('PGUSER', ''),
            'password': os.environ.get('PGPASSWORD', ''),
            'database': 'data_observability',
            'sslmode': 'require',
        }
    else:
        credentials = {}

    return credentials, None
