import os
import tarfile
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


from config import LISTINGS_DOWNLOAD_PATH_URL, LOCAL_RAW_DATA_DIR 
#from s3_download import download_file_from_s3


def download_tar_file(**kwargs):
    """Download the tar file from a URL."""
    local_tar_path = os.path.join(LOCAL_RAW_DATA_DIR,"abo-listings.tar")
    response = requests.get(LISTINGS_DOWNLOAD_PATH_URL, stream=True)
    print(f'Check URL: {LISTINGS_DOWNLOAD_PATH_URL}')
    print(response)
    print('Check-end')
    response.raise_for_status()  # Raise an exception for HTTP errors
    #files = [ f for f in os.listdir(local_tar_path) if os.path.isfile(os.path.join(local_tar_path, f))]
    
    with open(local_tar_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    files =os.listdir(LOCAL_RAW_DATA_DIR)
    print(f'Files at {LOCAL_RAW_DATA_DIR} : {files}')            
    print(f"Downloaded tar file to {LOCAL_RAW_DATA_DIR}")
    
    

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="shoptalk_ingestion_pipeline",
    default_args=default_args,
    description="Pipeline to download, extract, and process product listings",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_tar_file",
        python_callable=download_tar_file,
    )

    # extract_task = PythonOperator(
    #     task_id="extract_tar_file",
    #     python_callable=extract_tar_file,
    # )

    # process_task = PythonOperator(
    #     task_id="process_csv_files",
    #     python_callable=process_csv_files,
    # )
    
    download_task