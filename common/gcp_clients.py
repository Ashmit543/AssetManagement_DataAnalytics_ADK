# common/gcp_clients.py

from google.cloud import bigquery, storage, aiplatform, pubsub_v1, secretmanager_v1beta1
# Removed import google.generativeai as genai
from common.constants import PROJECT_ID, REGION

# Initialize BigQuery Client
def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

# Initialize Cloud Storage Client
def get_storage_client():
    return storage.Client(project=PROJECT_ID)

# Initialize Vertex AI (AI Platform) Client (renamed from get_vertex_ai_client)
def get_aiplatform_client(): # Renamed this function
    # aiplatform.init needs to be called once per process.
    # It's usually better to call aiplatform.init directly in the tool/agent that uses it,
    # or ensure it's called globally once, to avoid re-initialization issues.
    # For now, we'll keep the init call here, but be aware.
    aiplatform.init(project=PROJECT_ID, location=REGION)
    return aiplatform # Return the module itself for accessing sub-modules like .generative_models

# Initialize Pub/Sub Publisher Client
def get_pubsub_publisher_client():
    return pubsub_v1.PublisherClient()

# Initialize Pub/Sub Subscriber Client (if agents need to pull messages)
def get_pubsub_subscriber_client():
    return pubsub_v1.SubscriberClient()

# Initialize Secret Manager Client
def get_secret_manager_client():
    return secretmanager_v1beta1.SecretManagerServiceClient()

# Removed get_generative_model function