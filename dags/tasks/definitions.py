
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
from utils.json_utils import flatten_json
from utils.s3_utils import upload_file_to_s3
import re
import numpy as np
#import mlflow
#from sentence_transformers import SentenceTransformer

from datetime import datetime, timedelta
from config import LISTINGS_DOWNLOAD_PATH_URL, LOCAL_RAW_DATA_DIR, ALL_LISTINGS_DATA_CSV, US_ONLY_LISTINGS_CSV, US_PRODUCT_IMAGE_MERGE_CSV, AWS_S3_BUCKET, LISTINGS_CSV_FILE_LOCATION, IMAGES_DOWNLOAD_PATH_URL,LOCAL_RAW_IMGS_DIR, IMAGES_CSV_FILE_LOCATION, IMAGES_CSV_FILE, TMP_LISTINGS_SOURCE, TAR_FILE_NAME

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



def copy_listings_tar_file(source_path, destination_path, tar_file_name):
    
    source = os.path.join(source_path, tar_file_name)
    # Ensure the source file exists
    if not os.path.isfile(source):
        print(f"Source file does not exist: {source_path}")
        return
    
    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    try:
        # Copy the .tar file
        shutil.copy(source, destination_path)
        print(f"File copied successfully to {destination_path}")
    except Exception as e:
        print(f"Error while copying file: {e}")

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
    #raw_data_df=pd.DataFrame()
    print(f"JSON FILES LIST :{json_files}")

    us_listings_raw_df = pd.DataFrame()
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
        
        us_listing_df = flattened_json_as_df[flattened_json_as_df['country'] == 'US']
        us_listings_raw_df  = pd.concat([ us_listings_raw_df ,  us_listing_df ], ignore_index=True)
        print(f'saved : {listing_file} as {csv_file}')
        

    print(us_listings_raw_df.info())
    all_listings_csv_file = directory_path +'/'+ US_ONLY_LISTINGS_CSV
    us_listings_raw_df.to_csv(all_listings_csv_file)
    print(f"US_listings raw data is saved to :{all_listings_csv_file}")



def perform_eda_on_us_listings_data(local_extracted_json_dir):
    
    directory_path = os.path.join(LOCAL_RAW_DATA_DIR)
    all_listings_csv_file = directory_path +'/'+ US_ONLY_LISTINGS_CSV
    US_DF = pd.read_csv(all_listings_csv_file)
    
    ## Replacing 
    language_tag_columns = [col for col in US_DF.columns if 'language_tag' in col]
    value_columns =  [col.replace('language_tag', 'value') for col in language_tag_columns]

    language_df=US_DF[language_tag_columns ].copy()
    value_df=US_DF[value_columns].copy()
    distinct_columns_set = {re.sub(r'_\d+_language_tag', '', col) for col in language_tag_columns}
    
    lang_val_merge_df = pd.DataFrame()

    for a_col in distinct_columns_set:
        the_column_list = []
        prefix = a_col + '_'
        print(prefix)
        the_column_list = [col for col in US_DF.columns if prefix in col]
        the_column_list = [col for col in the_column_list if 'standardized' not in col]
        the_column_list = [col for col in the_column_list if 'alternate_representations' not in col]

        num_brand_keypairs = len(the_column_list)//2
        print(the_column_list)
        a_col_value= prefix + 'value'
        lang_val_merge_df[a_col_value] = US_DF.apply(
            lambda row: ' '.join(
                str(row[f'{prefix}{i}_value'])
                for i in range(num_brand_keypairs)
                if row[f'{prefix}{i}_language_tag'] == 'en_US'
            ).strip(),
            axis=1
        )
    
    US_DF.drop(columns=language_tag_columns, inplace=True)
    US_DF.drop(columns=value_columns, inplace=True)
    
    df_new = US_DF.drop([col for col in US_DF.columns if 'standardized' in col], axis=1)
    df_new2 = df_new.drop([col for col in df_new.columns if 'alternate_representations' in col], axis=1)
    
    US_DF_filtered = pd.concat([df_new2, lang_val_merge_df], axis=1)
    
    drop_column_list1=['spin_id', '3dmodel_id', 'node_0_node_id', 'node_0_node_name', 'node_1_node_id', 'node_1_node_name', 'node_2_node_id', 'node_2_node_name', 'node_3_node_id', 'node_3_node_name', 'node_4_node_id', 'node_4_node_name', 'node_5_node_id', 'node_5_node_name', 'node_6_node_id', 'node_6_node_name', 'node_7_node_id', 'node_7_node_name', 'node_8_node_id', 'node_8_node_name', 'node_9_node_id', 'node_9_node_name', 'node_10_node_id', 'node_10_node_name']
    US_DF_filtered3= US_DF_filtered.drop(columns=drop_column_list1)
    US_DF_filtered4 = US_DF_filtered3.dropna(subset=['main_image_id'])
    US_DF_filtered4['Price'] = np.random.uniform(20, 100, size=len(US_DF_filtered4)).round(2)
    
    US_DF_filtered4.to_csv(directory_path+'/'+'All_US_DF_filtered_v1.csv', index=False)
    



def flatten_to_csv_images(**kwargs):
    """Decompress and flatten the image metadata to a CSV file."""
    metadata_gz_path = os.path.join(LOCAL_RAW_IMGS_DIR, "images/metadata/images.csv.gz")
    output_csv_path = os.path.join(LOCAL_RAW_IMGS_DIR, "images_metadata.csv")

    # Check if the compressed metadata file exists
    if not os.path.exists(metadata_gz_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_gz_path}")

    print(f"Decompressing and processing metadata file: {metadata_gz_path}")
    decompressed_file_path = metadata_gz_path.rstrip(".gz")

    # Decompress the .gz file
    with gzip.open(metadata_gz_path, "rb") as f_in:
        with open(decompressed_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Read the decompressed CSV file
    metadata_df = pd.read_csv(decompressed_file_path)
    print(f"Metadata columns: {metadata_df.columns}")
    print(f"Number of records in metadata: {len(metadata_df)}")

    # Save the decompressed metadata to a flat CSV file
    metadata_df.to_csv(output_csv_path, index=False)
    print(f"Flattened metadata saved to: {output_csv_path}")




def download_tar_file_images(**kwargs):
    """Download the image tar file from the specified URL."""
    # Define the local path to save the tar file
    local_tar_path = os.path.join(LOCAL_RAW_IMGS_DIR, "abo-images-small.tar")

    # Check if the file already exists to avoid re-downloading
    if os.path.exists(local_tar_path):
        print(f"File already downloaded and exists: {local_tar_path}.")
        return

    # Make the local directory if it doesn't exist
    os.makedirs(LOCAL_RAW_IMGS_DIR, exist_ok=True)

    # Download the tar file from the specified URL
    response = requests.get(IMAGES_DOWNLOAD_PATH_URL, stream=True)
    print(f"Downloading from URL: {IMAGES_DOWNLOAD_PATH_URL}")
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Save the file to disk in chunks
    with open(local_tar_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # List files in the directory for confirmation
    files = os.listdir(LOCAL_RAW_IMGS_DIR)
    print(f"Files at {LOCAL_RAW_IMGS_DIR}: {files}")
    print(f"Downloaded tar file to {local_tar_path}")




def extract_tar_file_images(**kwargs):
    """Extract the image tar file and process metadata and images."""
    tar_file_path = os.path.join(LOCAL_RAW_IMGS_DIR, "abo-images-small.tar")
    extract_dir = LOCAL_RAW_IMGS_DIR

    # Check if the tar file exists
    if not os.path.exists(tar_file_path):
        raise FileNotFoundError(f"Tar file not found: {tar_file_path}")

    # Check if already extracted
    if os.path.exists(os.path.join(extract_dir, "images")):
        print(f"Tar file already extracted to: {extract_dir}. Skipping extraction.")
        return

    print(f"Extracting tar file: {tar_file_path}")
    try:
        # Open the tar file and extract its contents
        with tarfile.open(tar_file_path, "r:*") as tar:
            tar.extractall(extract_dir)
        print(f"Successfully extracted tar file to: {extract_dir}")
    except tarfile.TarError as e:
        print(f"Error extracting tar file: {e}")
        raise    

def up_load_us_listings_to_s3():
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    print(f'aws_access_key: {aws_access_key_id} - aws_secret_access_key: {aws_secret_access_key}')
    print(f' AWS_S3_BUCKET: {AWS_S3_BUCKET}')    
    
    local_file_path2 = LISTINGS_CSV_FILE_LOCATION + 'us_listings.csv'
    print(f'local_file_path2: {os.path.exists(local_file_path2)}')
    upload_file_to_s3(aws_access_key_id, aws_secret_access_key, AWS_S3_BUCKET, "listings/us_listings.csv", local_file_path2 )

def merge_listings_images():
    # Read from local and merge
    print(f'MERGED listings and images dataframes')
    images_csv_path=IMAGES_CSV_FILE_LOCATION+"/"+IMAGES_CSV_FILE
    images_csv_df = pd.read_csv(images_csv_df)
    listings_csv_path=LISTINGS_CSV_FILE_LOCATION+"/"+US_ONLY_LISTINGS_CSV
    us_listings_csv_df = pd.read_csv(listings_csv_path)    
    merged_df = pd.merge(us_listings_csv_df, images_csv_df, left_on='main_image_id', right_on='image_id', how=left)
    
