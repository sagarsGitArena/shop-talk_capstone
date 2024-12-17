import boto3
import logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# from botocore.exceptions import NoCredentialsError

# def download_file_from_s3(access_key, secret_key, bucket_name, file_name, local_file_path):
#     s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

#     try:
#         s3.download_file(bucket_name, file_name, local_file_path)
#         logging.info(f"File '{file_name}' downloaded successfully to '{local_file_path}'")
#     except FileNotFoundError:
#         logging.error(f"The file '{file_name}' does not exist in the bucket '{bucket_name}'")
#     except NoCredentialsError:
#         logging.error("Credentials not available or incorrect")


import boto3
import tempfile

from config import LOCAL_TMP_DIR

def download_file_from_s3(aws_access_key, aws_secret_key, bucket_name, file_name, local_data_dir):
    # Custom temporary directory on the same filesystem as the target file path
    temp_dir = LOCAL_TMP_DIR  # This must exist beforehand

    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)
    
    # Use tempdir for boto3 temporary files
    with tempfile.TemporaryDirectory(dir=temp_dir) as temp_path:
        s3.download_file(bucket_name, file_name, local_data_dir)
    print(f"File downloaded to {local_data_dir}")