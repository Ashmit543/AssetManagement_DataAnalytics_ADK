# Use the official Python image as a base image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the common directory and its contents
# This makes common/constants.py available inside the container
COPY ../common/ ./common/

# Copy the root-level requirements.txt file
COPY ../requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the agent's main application file
COPY gemini_llm_agent/main.py .

# Command to run the application using Gunicorn
# Ensure gunicorn is in your requirements.txt
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]