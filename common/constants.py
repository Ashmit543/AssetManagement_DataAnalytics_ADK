import os

# GCP Project and Region
PROJECT_ID = os.environ.get("PROJECT_ID", "agenticassetmange-dataanalysis")
REGION = os.environ.get("REGION", "asia-south1") # Ensure this matches your gcloud config

# Service Account Email (for reference, actual use is typically via ENV var or default credentials)
SERVICE_ACCOUNT_EMAIL = f"assetmgmt-adk-service-account@{PROJECT_ID}.iam.gserviceaccount.com"

# BigQuery
BIGQUERY_DATASET_ID = "asset_management_dataset"
BIGQUERY_TABLE_FINANCIAL_METRICS = "financial_metrics"
BIGQUERY_TABLE_NUMERICAL_INSIGHTS = "numerical_insights"
BIGQUERY_TABLE_SENTIMENT_SCORES = "sentiment_scores"
BIGQUERY_TABLE_REPORT_METADATA = "report_metadata"

# Pub/Sub Topics
PUBSUB_TOPIC_COORDINATOR_REQUESTS = "coordinator-requests-topic"
PUBSUB_TOPIC_FINANCIAL_DATA_REQUESTS = "financial-data-requests-topic"
PUBSUB_TOPIC_FINANCIAL_DATA_AVAILABLE = "financial-data-available-topic"

# ADDED/MODIFIED TOPICS FOR NUMERICAL SUMMARIZER AGENT
PUBSUB_TOPIC_FINANCIAL_METRICS_PROCESSED = "financial-metrics-processed-topic" # New: Trigger for NumericalSummarizer
PUBSUB_TOPIC_NUMERICAL_INSIGHTS_PROCESSED = "numerical-insights-processed-topic" # Renamed/New: Output of NumericalSummarizer

PUBSUB_TOPIC_NUMERICAL_SUMMARIES_REQUESTS = "numerical-summaries-requests-topic" # Keep if external requests to numerical summarizer
# PUBSUB_TOPIC_NUMERICAL_SUMMARIES_AVAILABLE = "numerical-summaries-available-topic" # Removed/Replaced by PUBSUB_TOPIC_NUMERICAL_INSIGHTS_PROCESSED

PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS = "report-generation-requests-topic"
PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED = "report-generation-completed-topic"
PUBSUB_TOPIC_DASHBOARD_UPDATES = "dashboard-updates-topic"

# Cloud Storage
GCS_REPORTS_BUCKET = f"{PROJECT_ID}-generated-reports"

# Nifty 50 Tickers (A representative list; you might want to fetch a live list or expand this)
NIFTY_50_TICKERS = [
    "ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS",
    "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "BHARTIARTL.NS", "BPCL.NS", "BRITANNIA.NS", "CIPLA.NS", "COALINDIA.NS",
    "DIVISLAB.NS", "DRREDDY.NS", "EICHERMOT.NS", "GRASIM.NS", "HCLTECH.NS",
    "HDFCBANK.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HINDUNILVR.NS",
    "ICICIBANK.NS", "INDUSINDBK.NS", "INFY.NS", "ITC.NS", "JSWSTEEL.NS",
    "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS", "NESTLEIND.NS",
    "NTPC.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS", "SBIN.NS",
    "SBILIFE.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SUNPHARMA.NS", "TCS.NS",
    "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "TECHM.NS", "TITAN.NS",
    "ULTRACEMCO.NS", "UPL.NS", "WIPRO.NS"
]

# Vertex AI Model Names
GEMINI_MODEL_NAME = "gemini-1.5-flash" # Or "gemini-1.5-pro" for more complex tasks
TEXT_EMBEDDING_MODEL_NAME = "text-embedding-004" # For vector embeddings

# Secret Manager Secrets (names of secrets you'll store in Secret Manager)
SECRET_ALPHA_VANTAGE_API_KEY = "alpha-vantage-api-key"

PUBSUB_TOPIC_REPORT_GENERATION_REQUEST = "report-generation-requests-topic" # Added 's'