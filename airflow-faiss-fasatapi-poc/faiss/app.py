from flask import Flask, request, jsonify
import faiss
import numpy as np
import cupy  # CuPy to support GPU operations
import os
import sys
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), 'faiss_utils'))
print(f'faiss: app.py - PATH: {sys.path}')

from faiss_utils.s3_download import download_file_from_s3 
from config import BUCKET_NAME, S3_DATA_FILE_PATH, LOCAL_TMP_DOWNLOAD_PATH



app = Flask(__name__)

# Initialize FAISS index to use GPU
dimension = 128  # Example dimensionality

# Move FAISS index to GPU
res = faiss.StandardGpuResources()  # Initialize GPU resources
index_cpu = faiss.IndexFlatL2(dimension)  # Create index on CPU
index_gpu = faiss.index_cpu_to_gpu(res, 0, index_cpu)  # Move index to GPU

@app.route("/")
def home():
    return jsonify({"message": "FAISS GPU Service is running!"})

@app.route("/add", methods=["POST"])
def add_vectors():
    vectors = np.array(request.json["vectors"]).astype("float32")
    # Add vectors to the GPU index
    index_gpu.add(vectors)
    return jsonify({"message": "Vectors added successfully!"})

@app.route("/search", methods=["POST"])
def search_vectors():
    query = np.array(request.json["query"]).astype("float32")
    k = request.json.get("k", 5)  # Number of neighbors
    distances, indices = index_gpu.search(query, k)  # Perform search on GPU index
    return jsonify({"distances": distances.tolist(), "indices": indices.tolist()})

@app.route("/load_data_file_from_s3", methods=["POST"])
def embed_description_and_load_vectors():

   # local_file_path = os.path.join(LOCAL_TMP_DOWNLOAD_PATH, S3_DATA_FILE_PATH)

    data = request.json
    s3_bucket_name = data['s3_bucket_name']
    aws_access_key = data['aws_access_key']
    aws_secret_key = data['aws_secret_key']
    file_name = data['file_name']

    print(f'bucket name:{s3_bucket_name}, aws_access_key:{aws_access_key}, aws_secret_key:{aws_secret_key}, fine_name:{file_name}')

    # Download the CSV from S3
    download_file_from_s3(aws_access_key, aws_secret_key, s3_bucket_name, file_name, local_file_path)
    # file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    US_df = pd.read_csv(os.path.join(local_file_path, file_name))
    print('====================================')
    print(US_df.info())
    print('====================================')
    # Get the 'descriptions' column and process
    #descriptions = df['descriptions'].tolist(
    #embeddings = compute_embeddings(descriptions)

    # Add embeddings to FAISS index
    #index.add(embeddings)

    return jsonify({"status": "success", "message": "Embeddings processed and loaded to FAISS!"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)