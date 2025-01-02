# Use a lightweight Python base image
FROM python:3.10

# Set working directory
WORKDIR /app

# Copy requirements.txt to the container
COPY requirements.txt /app/requirements.txt

# Copy recommend.py from recommendation_service folder to the container
COPY main/recommendation_service/recommend.py /app/recommend.py

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port that your service will run on (e.g., 5000)
EXPOSE 5000

# Run the inference service
CMD ["python", "recommend.py"]
