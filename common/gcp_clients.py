from google.cloud import bigquery, storage, aiplatform, pubsub_v1, secretmanager_v1beta1
import google.generativeai as genai
from common.constants import PROJECT_ID, REGION

# Initialize BigQuery Client
def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

# Initialize Cloud Storage Client
def get_storage_client():
    return storage.Client(project=PROJECT_ID)

# Initialize Vertex AI (AI Platform) Client for LLMs and Embeddings
def get_vertex_ai_client():
    # aiplatform.init needs to be called once per process if you're using it this way
    aiplatform.init(project=PROJECT_ID, location=REGION)
    return aiplatform # Return the module itself, as models are accessed via aiplatform.preview.generative_models.GenerativeModel etc.

# Initialize Pub/Sub Publisher Client
def get_pubsub_publisher_client():
    return pubsub_v1.PublisherClient()

# Initialize Pub/Sub Subscriber Client (if agents need to pull messages)
def get_pubsub_subscriber_client():
    return pubsub_v1.SubscriberClient()

# Initialize Secret Manager Client
def get_secret_manager_client():
    return secretmanager_v1beta1.SecretManagerServiceClient()

# Initialize Google Generative AI Client (for Gemini)
def get_generative_model(model_name: str):
    """
    Initializes and returns a generative model from Google AI.
    Make sure genai.configure is called with your API key if not using default credentials.
    For production on GCP, prefer default application credentials.
    """
    # For local development, you might set genai.configure(api_key="YOUR_API_KEY")
    # For Cloud Run, default credentials will be used, so direct model loading is fine.
    return genai.GenerativeModel(model_name)