from flask import Flask, request, jsonify
import faiss
import numpy as np

app = Flask(__name__)

# Initialize FAISS index
dimension = 128  # Example dimensionality
index = faiss.IndexFlatL2(dimension)

@app.route("/")
def home():
    return jsonify({"message": "FAISS Service is running!"})

@app.route("/add", methods=["POST"])
def add_vectors():
    vectors = np.array(request.json["vectors"]).astype("float32")
    index.add(vectors)
    return jsonify({"message": "Vectors added successfully!"})

@app.route("/search", methods=["POST"])
def search_vectors():
    query = np.array(request.json["query"]).astype("float32")
    k = request.json.get("k", 5)  # Number of neighbors
    distances, indices = index.search(query, k)
    return jsonify({"distances": distances.tolist(), "indices": indices.tolist()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
