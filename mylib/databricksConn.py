import os
from databricks import sql
from urllib3.exceptions import MaxRetryError
from databricks.sql.exc import RequestError
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

def databricks_connect():
    'Establish Databricks Connection'
    load_dotenv()

    account_url = os.getenv("account_url")
    account_key = os.getenv("account_key")
    DL_name = os.getenv("DL_name")


if __init__ == "__main__":
    databricks_connect()