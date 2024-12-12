from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Configuration for connecting to FAISS service
FAISS_URL = "http://faiss-service:8000"

@app.route("/")
def home():
    return jsonify({"message": "Welcome to the FastTrack API!"})

@app.route("/add-vectors", methods=["POST"])
def add_vectors():
    # Example to forward vector addition request to FAISS
    data = request.json
    try:
        response = requests.post(f"{FAISS_URL}/add", json=data)
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route("/search-vectors", methods=["POST"])
def search_vectors():
    # Example to forward search query to FAISS
    data = request.json
    try:
        response = requests.post(f"{FAISS_URL}/search", json=data)
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
