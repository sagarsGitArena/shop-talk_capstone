FROM python:3.9-slim

# Install dependencies
RUN pip install --no-cache-dir flask requests

# Copy application files
WORKDIR /app
COPY main.py .

# Expose FastTrack API
EXPOSE 5000

# Run the FastTrack API
CMD ["python", "main.py"]
