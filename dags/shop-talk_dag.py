import os
import tarfile
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import shutil
import gzip
import glob
import json
from utilities import flatten_json

from config import LISTINGS_DOWNLOAD_PATH_URL, LOCAL_RAW_DATA_DIR 
#from s3_download import download_file_from_s3


def download_tar_file(**kwargs):
    """Download the tar file from a URL."""
    local_tar_path = os.path.join(LOCAL_RAW_DATA_DIR,"abo-listings.tar")
    
    
    if os.path.exists(local_tar_path):    
        print(f"File already downloaded and exits: {local_tar_path}.")
        return
    
    response = requests.get(LISTINGS_DOWNLOAD_PATH_URL, stream=True)
    print(f'Check URL: {LISTINGS_DOWNLOAD_PATH_URL}')
    print(response)
    print('Check-end')
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Save the file to disk in chunks
    with open(local_tar_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    files = os.listdir(LOCAL_RAW_DATA_DIR)
    print(f'Files at {LOCAL_RAW_DATA_DIR} : {files}')            
    print(f"Downloaded tar file to {LOCAL_RAW_DATA_DIR}")
    

def extract_tar_file(extract_dir_path, tar_file, local_extracted_json_dir, extracted_file_pattern, decompressed_json_file_pattern):        
    
    local_tar_path = os.path.join(extract_dir_path, tar_file)
    compressed_json_dir = os.path.join(extract_dir_path, local_extracted_json_dir)
    
    # Check if directory exists [/home/sagar/Work/IK/CapStone-ShopTalk/PROJECT/ShopTalk/data/raw/listings/metadata]
    if os.path.exists(compressed_json_dir) and os.path.isdir(compressed_json_dir):
        print(f"The directory '{compressed_json_dir}' exists.")
    
    # extracted_file_pattern_path[/home/sagar/Work/IK/CapStone-ShopTalk/PROJECT/ShopTalk/data/raw/listings/metadata/listings_?.json.gz]
    extracted_file_pattern_path = os.path.join(compressed_json_dir, extracted_file_pattern)
    file_pattern = os.path.expanduser(extracted_file_pattern_path)
    print(f'file_pattern: {file_pattern}')
    print(f'extracted_file_pattern: {extracted_file_pattern}')
    matching_files = glob.glob(extracted_file_pattern_path)
    print(f'matching_files: {matching_files}')
    if matching_files:
        print(f"Files matching the pattern '{file_pattern}' exist:")
        for file in matching_files:
            print(f" - {file}")
        print(f"Tar file {tar_file} already extracted. Moving to next Task")
        
    
    decompressed_file_pattern_path = os.path.join(compressed_json_dir, decompressed_json_file_pattern)
    decompressed_file_pattern = os.path.expanduser(decompressed_file_pattern_path)
    matching_files = glob.glob(decompressed_file_pattern_path)
    print(f'matching_files: {matching_files}')
    if matching_files:
        print(f"Files matching the pattern '{file_pattern}' exist:")
        for file in matching_files:
            print(f" - {file}")
        print(f"Tar file {tar_file} already extracted. Moving to next Task")
        return
    
    print(f'extract-1 :{compressed_json_dir}')
    if not os.path.exists(local_tar_path):
        raise FileNotFoundError(f"Tar file not found: {local_tar_path}")
    # Ensure the extraction directory exists
    print('extract-2')
    try:
        # Open the tar file in read mode
        with tarfile.open(local_tar_path, "r:*") as tar:
            tar.extractall(extract_dir_path)
        print('extract-3')
        print(f"Successfully extracted tar file to: {extract_dir_path}")        
    except tarfile.TarError as e:
        print(f"Error extracting tar file: {e}")
        raise
    print('extract-4')
    
    
    files = os.listdir(compressed_json_dir)
    print(f'files {files}')
    for file_i in files:            
        print(file_i)
        compressed_json_path = os.path.join(compressed_json_dir, file_i)
        print(f'compressed_json_path :{compressed_json_path}')
        base_file = os.path.splitext(compressed_json_path)[0]
        with gzip.open(compressed_json_path, 'rb') as gz_file:
            type(compressed_json_path)
            print(compressed_json_path)
            print(compressed_json_path.split('.'))
            # Remove the .gz extension
            
            print(f'Base File : {base_file}')
            with open(base_file, 'wb') as decompressed_file:
                shutil.copyfileobj(gz_file, decompressed_file)
                print(f'Extracted file {decompressed_file}')
        
    
    files = os.listdir(compressed_json_dir)
    print(f'Files at {compressed_json_dir} : {files}')       


def flatten_each_json_and_save_as_csv(local_extracted_json_dir):
    print("ENTERED flatten_json_and_load_to_dataframe ***************")
    directory_path= os.path.join(LOCAL_RAW_DATA_DIR, local_extracted_json_dir)
    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]    
    print(f"JSON FILES LIST :{json_files}")

    for listing_file in json_files:
        print(f'Processing : {listing_file}')
        listing_file_path = os.path.join(directory_path, listing_file)
        print(listing_file_path)
        flattened_data = []
        with open(listing_file_path, 'r') as f:
            for line in f:
                json_obj = json.loads(line.strip())  # Load JSON from each line
                flattened_data.append(flatten_json(json_obj))

        flattened_json_as_df = pd.json_normalize(flattened_data)
        base_file_name = listing_file.split('.')[0]
        csv_file = directory_path +'/'+ base_file_name + '.csv'
        flattened_json_as_df.to_csv(csv_file)      
        print(f'saved : {listing_file} as {csv_file}')     

    
    





def flatten_all_json_and_save_as_csv(local_extracted_json_dir):
    print("ENTERED flatten_json_and_load_to_dataframe ***************")
    directory_path= os.path.join(LOCAL_RAW_DATA_DIR, local_extracted_json_dir)
    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]
    raw_data_df=pd.DataFrame()
    print(f"JSON FILES LIST :{json_files}")

    for listing_file in json_files:
        print(f'Processing : {listing_file}')
        listing_file_path = os.path.join(directory_path, listing_file)
        print(listing_file_path)
        flattened_data = []
        with open(listing_file_path, 'r') as f:
            for line in f:
                json_obj = json.loads(line.strip())  # Load JSON from each line
                flattened_data.append(flatten_json(json_obj))

        flattened_json_as_df = pd.json_normalize(flattened_data)
        base_file_name = listing_file.split('.')[0]
        csv_file = directory_path +'/'+ base_file_name + '.csv'
        flattened_json_as_df.to_csv(csv_file)
        raw_data_df = pd.concat([raw_data_df, flattened_json_as_df], ignore_index=True)
        print(f'saved : {listing_file} as {csv_file}')
        

    print(raw_data_df.info())
    




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

    extract_task = PythonOperator(
        task_id="extract_tar_file",
        python_callable=extract_tar_file,
        op_kwargs={"extract_dir_path": LOCAL_RAW_DATA_DIR, "tar_file": "abo-listings.tar", 
                                        "local_extracted_json_dir": "listings/metadata/", 
                                        "extracted_file_pattern": "listings_?.json.gz",
                                        "decompressed_json_file_pattern": "listings_?.json"},
        #provide_context=True,        
        trigger_rule='all_done',
#        dag=dag

    )
    
    flatten_each_json_and_save_as_csv = PythonOperator(
        task_id="flatten_each_json_and_save_as_csv",
        python_callable=flatten_each_json_and_save_as_csv,
        op_kwargs= {"local_extracted_json_dir": "listings/metadata/"                   
        },
        #provide_context=True,        
        trigger_rule='all_done',
#        dag=dag

    )

    # process_task = PythonOperator(
    #     task_id="process_csv_files",
    #     python_callable=process_csv_files,
    # )
    
    #Intended for local machine run
    download_task >> extract_task >> flatten_each_json_and_save_as_csv
    #Intended to run in a machine with high RAM (eg: AWS EC2)
    #download_task >> extract_task >> flatten_all_json_and_save_as_csv    
    