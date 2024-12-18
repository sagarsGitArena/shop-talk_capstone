import json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base_hook import BaseHook



import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import sys
logging.info(f'data-pipeline-dag PATH: {sys.path}')

#from preprocessing import preprocess_data
import os
from config import BUCKET_NAME, S3_OBJECT_KEY, FAISS_API_ENDPOINT, DB_LOAD_TASK_TIMEOUT
from airflow_utils.s3_utils import download_file_from_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id='shoptalk-data-pipeline',
    default_args=default_args,
    description="Pipeline to download product listings and load to vector DB",
    start_date=datetime(2024, 12, 15),  # Specify the start_date here
    schedule_interval=timedelta(minutes=10),  # Every 10 minutes
    max_active_runs=1,
    catchup=False
) as dag:

    # Task 1: S3 Sensor
    check_if_data_file_arrived = S3KeySensor(
        task_id='check_if_data_file_arrived',
        poke_interval=10,  # Check for file every 60 seconds
        timeout=6000,  # Timeout if file not found after 600 seconds
        bucket_key=S3_OBJECT_KEY,  # Update with your S3 path
        bucket_name=BUCKET_NAME,
        aws_conn_id="aws_default",
        mode='poke',
        dag=dag,
    )

    # Task 2: Create embeddings and load to vector database
    #   call faiss api for faiss service to do the following
    #   1> load to DF 
    #   2> create embeddings from description
    #   3> load embeddings into DB
    load_faiss_vector_db = SimpleHttpOperator(
        task_id='trigger_load_to_fiass_vector_db',
        method='POST',
        http_conn_id=None,  # Define this connection in Airflow's Connection UI
        endpoint=FAISS_API_ENDPOINT,
        data=json.dumps({
            's3_bucket_name': BUCKET_NAME,
            'aws_access_key': os.environ["AWS_ACCESS_KEY_ID"],
            'aws_secret_key': os.environ["AWS_SECRET_ACCESS_KEY"],
            's3_object_key': S3_OBJECT_KEY
        }),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,  # Check success status
        extra_options={"timeout": DB_LOAD_TASK_TIMEOUT},  # Timeout of 5 minutes -- Shouldn't take more than 3 mins
    )
check_if_data_file_arrived >> load_faiss_vector_db