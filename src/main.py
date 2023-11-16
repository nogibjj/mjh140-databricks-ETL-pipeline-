"""
Databricks query script
"""
import sys
import os
import pandas as pd
import pyarrow.parquet as pq
from delta import *
from logging import getLogger
from io import BytesIO, StringIO
import matplotlib.pyplot as plt
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession


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

connection_string=os.getenv("connection_string")

def initialize_spark():
     # Create Spark session
     spark = SparkSession.builder.appName("NCAABDataLakeApp")\
          .config("spark.hadoop.fs.azure.account.key.mjh140dukestorage.dfs.core.windows.net", "tQVQmoOjBTUzRubHETDwJTPZym4Gz+ao7ACWBmGeP22xvXZfrLF8blBUR9M7+Uuh1aBZ9i7FH+jv+ASt6MHXjA==")\
          .getOrCreate()
     return spark

def off_v_def_retrieve(file_system_client):
     # Access the file (replace 'your-directory' and 'your-file.csv' with your directory and file name)
     file_system_client = file_system_client.get_file_client("/Gold/OffenseVsDefense_Conf_Parq/data.parquet/")

     # Download the file
     download = file_system_client.download_file()
     print(download)
     downloaded_bytes = download.readall()
     print(len(downloaded_bytes))

     # Convert to Pandas DataFrame
     data = BytesIO(downloaded_bytes)
     table = pq.read_table(data)
     df = table.to_pandas()

     # Convert to Pandas DataFrame
     #s = str(downloaded_bytes, 'utf-8')
     #data = StringIO(s)
     #df = pd.read_csv(data)
     return(df)


def off_v_def_viz(df):
     # Plotting
     plt.figure(figsize=(10, 6))
     plt.scatter(df['AdjOE'], df['AdjDE'], alpha=0.7)

     # Adding labels with a slight offset
     offset = 0.1  # Adjust this value as needed
     for i in range(len(df)):
          plt.text(df['AdjOE'][i] + offset, df['AdjDE'][i] + offset, df['Conference'][i], fontsize=8)

     plt.xlabel('AdjOE')
     plt.ylabel('AdjDE')
     plt.title('AdjOE vs AdjDE for Different Conferences')
     plt.grid(True)
     plt.show()

def blob_retrieval():

     #blob_service_client = BlobServiceClient.from_connection_string(connection_string)
     #container_client = blob_service_client.get_container_client(container="ncaab_data_lake")

     #downloaded_blob = container_client.download_blob(upload_name)
     #bytes_io = BytesIO(downloaded_blob.readall())
     #df = pd.read_parquet(bytes_io)

     # Azure Storage Account details
     account_name = 'mjh140dukestorage'
     file_system = 'ncaab-data-lake'
     file_path = 'Gold/OffenseVsDefense_Conf_Parq/data.parquet/'

     # Connection string for your Azure Storage Account
     connection_string = connection_string

     # Initialize the BlobServiceClient
     blob_service_client = BlobServiceClient.from_connection_string(connection_string)

     # Get the blob client for the parquet file
     blob_client = blob_service_client.get_blob_client(container=file_system, blob=file_path)

     # Download the blob
     downloaded_blob = blob_client.download_blob()
     stream = BytesIO(downloaded_blob.readall())

     # Ensure the stream position is set to the beginning
     stream.seek(0)

     # Read the Parquet file into a pandas DataFrame
     df = pd.read_parquet(stream)

     return df

def saskey():
     source ='https://mjh140dukestorage.blob.core.windows.net/ncaab-data-lake/Gold/OffenseVsDefense_Conf_Parq/data.parquet?sp=r&st=2023-11-16T06:33:05Z&se=2023-11-30T06:33:05Z&spr=https&sv=2022-11-02&sr=d&sig=jYdQoncpX4Uq6bOPdPjatHK4ULAfLKEjxhXZeHJ6bwE%3D&sdd=3'
     df = pd.read_parquet(source)
     print(df)

if __name__ == "__main__":
     print("Connecting to NCAAB Data Lake")
     file_system_client, status = datalake_connect()
     if status != "Success":
          print('Error Occurred. Check error_log.txt.')
          sys.exit()

     #data_df = off_v_def_retrieve(file_system_client)
     #data_df = blob_retrieval
     #data_df = saskey()
     #print(data_df)
     #off_v_def_viz(data_df)
     #initialize_spark()