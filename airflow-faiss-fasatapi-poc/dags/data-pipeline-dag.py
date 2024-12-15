from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

#from preprocessing import preprocess_data
import os
from config import BUCKET_NAME, S3_DATA_FILE_PATH, FAISS_API_ENDPOINT
from utils.s3_utils import download_file_from_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# with DAG(
#     dag_id="shoptalk_ingestion_pipeline",
#     default_args=default_args,
#     description="Pipeline to download, extract, and process product listings",
#     start_date=datetime(2024, 1, 1),
#     #schedule_interval="@daily",
#     #schedule_interval="*/10 * * * *",  # Every 10 minutes
#     schedule_interval=timedelta(minutes=10),  # Every 10 minutes
#     max_active_runs=1,
#     catchup=False,
# ) as dag:

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
        bucket_key=S3_DATA_FILE_PATH,  # Update with your S3 path
        bucket_name=BUCKET_NAME,
        aws_conn_id="aws_default",
        mode='poke',
        dag=dag,
    )

    # Task 2: 
    #   call faiss api for faiss service to do the following
    #   1> load to DF 
    #   2> create embeddings from description
    #   3> load embeddings into DB

    # Step 2: Trigger FAISS Service via API (passing S3 bucket and file info)

    load_faiss_vector_db = SimpleHttpOperator(
        task_id='trigger_load_to_fiass_vector_db',
        method='POST',
        http_conn_id='faiss_api_connection',  # Define this connection in Airflow's Connection UI
        endpoint=FAISS_API_ENDPOINT,
        data={
            's3_bucket_name': BUCKET_NAME,
            'aws_access_key':  os.environ["AWS_ACCESS_KEY_ID"],
            'aws_secret_key': os.environ["AWS_SECRET_ACCESS_KEY"],
            'file_name' : S3_DATA_FILE_PATH

        },
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.status_code == 200,  # Ensure the request succeeds
        extra_options={"timeout": 600},  # This is valid
    )

    check_if_data_file_arrived >> load_faiss_vector_db