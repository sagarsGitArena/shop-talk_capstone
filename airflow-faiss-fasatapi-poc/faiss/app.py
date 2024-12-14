from flask import Flask, request, jsonify
import faiss
import numpy as np
import cupy  # CuPy to support GPU operations

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)