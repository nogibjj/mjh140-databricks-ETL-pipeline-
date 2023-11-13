import os
import logging
from databricks import sql
from dotenv import load_dotenv
from urllib3.exceptions import MaxRetryError
from databricks.sql.exc import RequestError

logging.basicConfig(filename='error_log.txt', 
                    level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def sqlConnect():
    # Define your SQL Data Warehouse connection details
    # Load environment variables from .env file
    load_dotenv()

    SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
    TOKEN = os.getenv("ACCESS_TOKEN")
    PATH = os.getenv("HTTP_PATH")
    try:
        conn = sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=PATH,
            access_token=TOKEN,
        )
    except MaxRetryError:
        logging.error("MaxRetryError - Unable to connect to server;", exc_info=False)
        return None, "Error"
    except RequestError:
        logging.error("RequestError - Invalid Token;", exc_info=False)
        return None, "Error"

    c = conn.cursor()

    return c, "Success"

def sqlClose(conn):
    conn.close()

if __name__ == "__main__":
    sqlConnect()