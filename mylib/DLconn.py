import os
import sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError
from dotenv import load_dotenv
from logging import getLogger
#from ..log_config import setup_logging

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from logging_config.log_config import setup_logging

# Initialize logging
setup_logging()
logger_error = getLogger('error_logger')
logger_info = getLogger('info_logger')

def datalake_connect():

    '''
    Accesses the data lake: 'ncaab-data-lake' within storage account: mjh140dukestorage
    Returns file system client for retrieving data
    Environment variables loaded from .env file
    '''

    load_dotenv()

    account_url = os.getenv("account_url")
    account_key = os.getenv("account_key")
    DL_name = os.getenv("DL_name")

    try:
        # Establish service client
        service_client = DataLakeServiceClient(account_url=account_url,
                                               credential=account_key)
        # Trigger network request
        _ = service_client.list_file_systems()
        logger_info.info("Successfully authenticated service client.")
        
        # Establish file system client
        file_system_client = service_client.get_file_system_client(file_system=DL_name)

        # Trigger network request
        _ = file_system_client.get_paths()
        logger_info.info("Successfully authenticated ncaab_data_lake file system client.")


    except ClientAuthenticationError as e:
        logger_error.error("Error message: %s", e)
        return None, "Error"

    except ServiceRequestError as e:
        logger_error.error("Error message: %s", e)
        return None, "Error"

    except Exception as e:
        logger_error.error("An error occurred: %s", e)
        return None, "Error"

    return file_system_client, "Success"

if __name__ == "__main__":
    file_system_client, status = datalake_connect()
    