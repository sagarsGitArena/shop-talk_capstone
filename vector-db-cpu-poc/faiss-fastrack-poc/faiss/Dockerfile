FROM python:3.9-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    libopenblas-dev && \
    pip install --no-cache-dir faiss-cpu flask

# Copy application files
WORKDIR /app
COPY app.py .

# Expose FAISS service
EXPOSE 8000

# Run the FAISS server
CMD ["python", "app.py"]
