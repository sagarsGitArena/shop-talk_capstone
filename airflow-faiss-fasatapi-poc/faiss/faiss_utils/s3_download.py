import boto3
import logging
import os

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



def download_file_from_s3(aws_access_key, aws_secret_key, bucket_name, s3_object_key, local_directory):
    """
    Downloads a file from an S3 bucket to a local directory.

    Parameters:
        aws_access_key (str): AWS access key ID.
        aws_secret_key (str): AWS secret access key.
        bucket_name (str): Name of the S3 bucket.
        s3_object_key (str): The full path to the object in the S3 bucket (key).
        local_directory (str): The local directory where the file will be downloaded.
    
    Returns:
        None
    """
    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # Extract the file name from the S3 object key
    file_name = os.path.basename(s3_object_key)

    # Construct the local file path
    local_file_path = os.path.join(local_directory, file_name)

    # Create an S3 client
    s3 = boto3.client('s3',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key)

    print(f"Downloading '{s3_object_key}' from bucket '{bucket_name}' to '{local_file_path}'...")

    # Download the file
    try:
        s3.download_file(bucket_name, s3_object_key, local_file_path)
        print(f"File successfully downloaded to: {local_file_path}")
        return True
    except Exception as e:
        print(f"Error occurred while downloading: {e}")
    
    return False


def delete_file_from_s3(aws_access_key, aws_secret_key, bucket_name, s3_object_key):
    """
    Downloads a file from an S3 bucket to a local directory and deletes the object from the S3 bucket.

    Parameters:
        aws_access_key (str): AWS access key ID.
        aws_secret_key (str): AWS secret access key.
        bucket_name (str): Name of the S3 bucket.
        s3_object_key (str): The full path to the object in the S3 bucket (key).
    Returns:
        None
    """

    # Create an S3 client
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

    try:
    
        # Step 1: Delete the file from S3
        print(f"Deleting '{s3_object_key}' from bucket '{bucket_name}'...")
        s3.delete_object(Bucket=bucket_name, Key=s3_object_key)
        print(f"File '{s3_object_key}' successfully deleted from bucket '{bucket_name}'.")
        return True
    except Exception as e:
        print(f"Error: {e}")
    
    return False
