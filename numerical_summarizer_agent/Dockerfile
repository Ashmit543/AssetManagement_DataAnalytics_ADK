# STEP 1: Choose a base image
FROM python:3.10-slim-buster

# STEP 2: Set environment variables
ENV PYTHONUNBUFFERED True

# STEP 3: Create a working directory inside the container
WORKDIR /app

# STEP 4: Copy dependency file
COPY requirements.txt .

# STEP 5: Install Python dependencies
RUN pip install -r requirements.txt

# STEP 6: Copy your application code
COPY main.py .

# STEP 7: Define the command to run when the container starts
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app