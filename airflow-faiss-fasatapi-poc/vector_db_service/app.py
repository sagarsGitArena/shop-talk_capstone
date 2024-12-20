from flask import Flask, request, jsonify
import faiss
import numpy as np
import cupy  # CuPy to support GPU operations
import os
import sys
import pandas as pd
import json
import logging
from sentence_transformers import SentenceTransformer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
print(f'faiss: app.py - PATH: {sys.path}')

from utils.s3_utils import download_file_from_s3, delete_file_from_s3, upload_file_to_s3
from config import BUCKET_NAME, S3_DATA_FILE_PATH, LOCAL_DATA_DIR
from config import FAISS_LOCAL_DB_STORE, FAISS_METADATA_JSON_FILE, FAISS_INDEX_BIN_FILE, FAISS_METADATA_JSON_FILE_S3_OBJECT_KEY, FAISS_INDEX_FILE_S3_OBJECT_KEY



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

    local_directory = LOCAL_DATA_DIR
    #os.path.join(LOCAL_TMP_DOWNLOAD_PATH, S3_DATA_FILE_PATH)
    
    data = request.json
    s3_bucket_name = data['s3_bucket_name']
    aws_access_key = data['aws_access_key']
    aws_secret_key = data['aws_secret_key']
    s3_object_key = data['s3_object_key']

    # Extract file name
    file_name = os.path.basename(s3_object_key)
    #file_name = os.path.basename(s3_object_key)


    print(f"File name: {file_name}")
    print(f'bucket name:{s3_bucket_name}, aws_access_key:{aws_access_key}, aws_secret_key:{aws_secret_key}, fine_name:{file_name}')
    # Log the masked information
    logging.info(
        f'bucket name: {s3_bucket_name}, '
        f'aws_access_key: {aws_access_key}, '
        f'aws_secret_key: {aws_secret_key}, '
        f'file_name: {file_name}',
        f's3_object_key: {s3_object_key}'
    )

    # Download the CSV from S3

    downloaded = download_file_from_s3(aws_access_key, aws_secret_key, s3_bucket_name, s3_object_key, local_directory)

    if (downloaded):
        delete_file_from_s3(aws_access_key, aws_secret_key, s3_bucket_name, s3_object_key)
    # file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    products_captioned_df = pd.read_csv(os.path.join(local_directory, file_name))
    logging.info('==================d==================')
    logging.info(products_captioned_df.info())
    logging.info('====================================')

    products_captioned_df['concatenated_desc'] = products_captioned_df['caption'].astype(str)+" "+ products_captioned_df['bullet_point_value'].astype(str)

    # Initialize the model for embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Generate embeddings and add them as a column
    products_captioned_df['embeddings'] = products_captioned_df['concatenated_desc'].apply(lambda desc: model.encode(desc).astype('float32'))

    # Get the dimension of embeddings (depends on the model used)
    embedding_dim = len(products_captioned_df['embeddings'][0])

    # Parameters for IVF-PQ index
    nlist = 100  # Number of clusters (inverted lists), tune based on dataset size
    m = 8        # Number of sub-quantizers
    nbits = 8    # Bits per sub-quantizer (for 256 centroids per sub-vector)

    
    # Create an IVF-PQ index with Euclidean distance metric
    quantizer = faiss.IndexFlatL2(embedding_dim)  # Underlying index for quantization
    index = faiss.IndexIVFPQ(quantizer, embedding_dim, nlist, m, nbits)


    # Train the index on embeddings (required for IVF-PQ)
    embeddings_array = np.stack(products_captioned_df['embeddings'].values)
    index.train(embeddings_array)
    
    # Dictionary to map FAISS index to item_id
    faiss_to_item_id = {}

    # Populate FAISS and store metadata by item_id
    metadata = {}
    for i, row in products_captioned_df.iterrows():
        embedding = np.array(row['embeddings'], dtype='float32').reshape(1, -1)
        index.add(embedding)  # Add embedding to FAISS

        # Store metadata using item_id as key
        item_id = row['item_id']
        metadata[item_id] =  {
            "item_id": row['item_id'],
            "item_weight_0_normalized_value_unit": row['item_weight_0_normalized_value_unit'],
            "item_weight_0_normalized_value_value": row['item_weight_0_normalized_value_value'],
            "item_weight_0_unit": row['item_weight_0_unit'],
            "item_weight_0_value": row['item_weight_0_value'],
            "model_number_0_value": row['model_number_0_value'],
            "product_type_0_value": row['product_type_0_value'],
            "main_image_id": row['main_image_id'],
            "color_code_0": row['color_code_0'],
            "country": row['country'],
            "marketplace": row['marketplace'],
            "domain_name": row['domain_name'],
            "item_dimensions_height_normalized_value_unit": row['item_dimensions_height_normalized_value_unit'],
            "item_dimensions_height_normalized_value_value": row['item_dimensions_height_normalized_value_value'],
            "item_dimensions_height_unit": row['item_dimensions_height_unit'],
            "item_dimensions_height_value": row['item_dimensions_height_value'],
            "item_dimensions_length_normalized_value_unit": row['item_dimensions_length_normalized_value_unit'],
            "item_dimensions_length_normalized_value_value": row['item_dimensions_length_normalized_value_value'],
            "item_dimensions_length_unit": row['item_dimensions_length_unit'],
            "item_dimensions_length_value": row['item_dimensions_length_value'],
            "item_dimensions_width_normalized_value_unit": row['item_dimensions_width_normalized_value_unit'],
            "item_dimensions_width_normalized_value_value": row['item_dimensions_width_normalized_value_value'],
            "item_dimensions_width_unit": row['item_dimensions_width_unit'],
            "item_dimensions_width_value": row['item_dimensions_width_value'],
            "model_year_0_value": row['model_year_0_value'],
            "style_value": row['style_value'],
            "item_shape_value": row['item_shape_value'],
            "pattern_value": row['pattern_value'],
            "fabric_type_value": row['fabric_type_value'],
            "item_name_value": row['item_name_value'],
            "material_value": row['material_value'],
            "item_keywords_value": row['item_keywords_value'],
            "finish_type_value": row['finish_type_value'],
            "model_name_value": row['model_name_value'],
            "bullet_point_value": row['bullet_point_value'],
            "color_value": row['color_value'],
            "brand_value": row['brand_value'],
            "Price": row['Price'],
            "caption": row['caption'],
            "concatenated_desc": row['concatenated_desc']
        }

            # Map FAISS index to item_id
        faiss_to_item_id[index.ntotal - 1] = item_id  # Current FAISS index
    
    # Save FAISS index locally
    local_index_bin_file_path=  FAISS_LOCAL_DB_STORE + '/' + FAISS_INDEX_BIN_FILE
    faiss.write_index(index, local_index_bin_file_path)

    # Save metadata locally
    local_json_metadata_file_path = FAISS_LOCAL_DB_STORE + '/' + FAISS_METADATA_JSON_FILE
    with open(local_json_metadata_file_path, 'w') as f:
        json.dump(metadata, f)
    
    upload_file_to_s3(aws_access_key, aws_secret_key, s3_bucket_name, FAISS_INDEX_FILE_S3_OBJECT_KEY, local_index_bin_file_path)
    upload_file_to_s3(aws_access_key, aws_secret_key, s3_bucket_name, FAISS_METADATA_JSON_FILE_S3_OBJECT_KEY, local_json_metadata_file_path)
    
    return jsonify({"status": "success", "message": "Embeddings processed and loaded to FAISS!"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)