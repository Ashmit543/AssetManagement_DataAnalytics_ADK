# Use a slim Python base image
FROM python:3.10-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the common directory
COPY common/ ./common/

# Copy the config directory
COPY config/ ./config/

# Copy the main requirements.txt to install dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the agent-specific requirements.txt (if any)
COPY agents/coordinator_agent/requirements.txt .
RUN pip install --no-cache-dir -r agents/coordinator_agent/requirements.txt || echo "No agent-specific requirements"

# Copy the agent's code
COPY agents/coordinator_agent/agent.py .

# Expose the port that the Flask app will listen on
EXPOSE 8080

# Run the Gunicorn server. 'agent:app' refers to the Flask app instance 'app'
# within the 'agent.py' file.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "agent:app"]