# Use a slim Python base image
FROM python:3.10-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy common utilities and configuration
COPY common/ ./common/
COPY config/ ./config/
COPY tools/ ./tools/
COPY schemas/ ./schemas/ # Schemas are not strictly needed at runtime but can be useful for reference

# Copy the main requirements.txt
# If yfinance is removed from main requirements.txt, ensure it's truly not needed by any other agent.
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the agent-specific requirements.txt (if any)
# Since we removed yfinance, no specific dependencies here
COPY agents/financial_metrics_agent/requirements.txt .
RUN pip install --no-cache-dir -r agents/financial_metrics_agent/requirements.txt || echo "No agent-specific requirements"

# Copy the agent's code
COPY agents/financial_metrics_agent/agent.py .
COPY agents/financial_metrics_agent/alpha_vantage_processor.py . # Changed from yfinance_processor.py

# Expose the port that the Flask app will listen on
EXPOSE 8080

# Run the Gunicorn server
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "agent:app"]