"""
Databricks query script
"""
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from delta import *
from logging import getLogger

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from mylib.DLconn import datalake_connect
from logging_config.log_config import setup_logging

# Initialize logging
setup_logging()
logger_error = getLogger('error_logger')
logger_info = getLogger('info_logger')

account_key = os.getenv("account_key")
account_name = os.getenv("account_name")

def initialize_spark():
     '''Initialize Spark Session'''

     # Create Spark session
     spark = SparkSession.builder.appName("NCAABDataLakeApp")\
     .config("spark.hadoop.fs.azure.account.key.mjh140dukestorage.dfs.core.windows.net", "tQVQmoOjBTUzRubHETDwJTPZym4Gz+ao7ACWBmGeP22xvXZfrLF8blBUR9M7+Uuh1aBZ9i7FH+jv+ASt6MHXjA==")\
     .getOrCreate()
     
     logger_info.info("Successfully Spark Session")
     return spark

def off_v_def_retrieve(file_system_client):
      pass
def off_v_def_viz(data_df):
      pass


if __name__ == "__main__":
     print("Connecting to NCAAB Data Lake")
     file_system_client, status = datalake_connect()
     if status != "Success":
          print('Error Occurred. Check error_log.txt.')
          sys.exit()

     spark = initialize_spark()

     data_df = off_v_def_retrieve(file_system_client)

     off_v_def_viz(data_df)