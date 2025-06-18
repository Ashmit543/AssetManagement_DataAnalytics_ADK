import os
from google.cloud import bigquery, pubsub_v1
import google.cloud.aiplatform as aiplatform
from datetime import datetime, date
from flask import Flask, request, jsonify
import json
import base64

# --- Flask App Initialization (MUST BE AT GLOBAL SCOPE) ---
app = Flask(__name__)

# --- Configuration ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'finopsanalystagent')
BIGQUERY_DATASET = 'AssetMgt_data'
BIGQUERY_FINANCIAL_METRICS_TABLE = 'financial_metrics'

# Pub/Sub topics
NUMERICAL_DATA_PROCESSED_TOPIC = 'numerical-data-processed-topic'  # Input topic
INSIGHTS_GENERATED_TOPIC = 'insights-generated-topic'  # Output topic

# Vertex AI Model Configuration
REGION = 'asia-south1'  # Your Vertex AI region (Mumbai)
GENERATIVE_MODEL_NAME = 'gemini-1.5-flash-001'  # Using stable model name
EMBEDDING_MODEL_NAME = 'text-embedding-004'  # Vertex AI embedding model
# IMPORTANT: Replace with your actual endpoint ID from the Cloud Console
VECTOR_SEARCH_INDEX_ENDPOINT_ID = '8168368639771148288'

# Initialize Clients
bigquery_client = bigquery.Client(project=PROJECT_ID)
pubsub_publisher = pubsub_v1.PublisherClient()

# Initialize Vertex AI
aiplatform.init(project=PROJECT_ID, location=REGION)

# Initialize models as None - will be loaded when needed
generative_model = None
embedding_model = None


def get_generative_model():
    """Lazy load the generative model."""
    global generative_model
    if generative_model is None:
        from vertexai.generative_models import GenerativeModel
        generative_model = GenerativeModel(GENERATIVE_MODEL_NAME)
    return generative_model


def get_embedding_model():
    """Lazy load the embedding model."""
    global embedding_model
    if embedding_model is None:
        from vertexai.language_models import TextEmbeddingModel
        embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL_NAME)
    return embedding_model


# --- Helper Functions ---

def fetch_numerical_data_from_bq(ticker):
    """Fetches the latest numerical metrics for a given ticker from BigQuery."""
    query = f"""
    SELECT
        metric_name,
        metric_value,
        unit,
        metric_category,
        report_date
    FROM
        `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_FINANCIAL_METRICS_TABLE}`
    WHERE
        company_ticker = @ticker
    ORDER BY
        report_date DESC, extraction_timestamp DESC
    LIMIT 200 -- Limit to a reasonable number of rows to avoid overwhelming the LLM
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
        ]
    )

    print(f"Fetching numerical data for {ticker} from BigQuery...")
    query_job = bigquery_client.query(query, job_config=job_config)
    rows = list(query_job.result())
    print(f"Fetched {len(rows)} numerical data points for {ticker}.")
    return rows


def format_numerical_data_for_llm(numerical_data, ticker):
    """Formats the numerical data into a human-readable string for the LLM."""
    if not numerical_data:
        return f"No numerical data found for {ticker} to summarize."

    formatted_string = f"Here is the latest financial and historical data for {ticker}:\n\n"
    categories = {}
    for row in numerical_data:
        category = row.metric_category
        if category not in categories:
            categories[category] = []
        categories[category].append(
            f"- {row.metric_name}: {row.metric_value} {row.unit or ''} (Report Date: {row.report_date})")

    for category, metrics in categories.items():
        formatted_string += f"**{category}**:\n"
        formatted_string += "\n".join(metrics) + "\n\n"

    formatted_string += "Please provide a concise summary of this data, highlighting key performance indicators, recent trends, and any significant financial strengths or weaknesses."
    return formatted_string


def generate_summary_with_gemini(formatted_data):
    """Generates a summary using the Vertex AI Gemini generative model."""
    print("Generating summary with Vertex AI Gemini...")
    try:
        model = get_generative_model()
        response = model.generate_content(formatted_data)
        summary_text = response.text
        print("Summary generated successfully.")
        return summary_text
    except Exception as e:
        print(f"Error generating summary with Vertex AI Gemini: {e}")
        return None


def generate_embedding(text):
    """Generates a vector embedding for a given text using Vertex AI."""
    print("Generating embedding with Vertex AI...")
    try:
        model = get_embedding_model()
        # Use Vertex AI TextEmbeddingModel
        embeddings = model.get_embeddings([text])
        # Extract the embedding vector from the first (and only) result
        embedding_vector = embeddings[0].values
        print("Embedding generated successfully.")
        return embedding_vector
    except Exception as e:
        print(f"Error generating embedding with Vertex AI: {e}")
        return None


def store_insight_in_vector_search(ticker, summary, embedding):
    """Stores the generated insight and its embedding in Vertex AI Vector Search."""
    print(f"Storing insight for {ticker} in Vector Search (placeholder for now)...")
    # Placeholder implementation
    return True


def publish_pubsub_message(topic_name, message_data, attributes=None):
    """Publish a message to a given Pub/Sub topic."""
    topic_path = pubsub_publisher.topic_path(PROJECT_ID, topic_name)
    message_json = json.dumps(message_data)
    message_bytes = message_json.encode('utf-8')

    try:
        future = pubsub_publisher.publish(topic_path, message_bytes, **(attributes or {}))
        message_id = future.result()
        print(f"Message published to {topic_name}. Message ID: {message_id}")
        return message_id
    except Exception as e:
        print(f"Failed to publish message to {topic_name}: {e}")
        return None


# --- Cloud Run HTTP Endpoint ---

@app.route('/', methods=['POST'])
def index():
    """
    Cloud Run services receive HTTP requests. When a Pub/Sub push subscription
    triggers this service, the Pub/Sub message data is included in the HTTP POST body.
    """
    try:
        envelope = request.get_json()
        if not envelope:
            print("No Pub/Sub message received")
            return 'No Pub/Sub message received', 400

        if not isinstance(envelope, dict) or 'message' not in envelope:
            print("Invalid Pub/Sub message format")
            return 'Invalid Pub/Sub message format', 400

        pubsub_message = envelope['message']

        ticker = None
        if 'data' in pubsub_message:
            try:
                message_data_str = base64.b64decode(pubsub_message['data']).decode('utf-8')
                message_data = json.loads(message_data_str)
                ticker = message_data.get('ticker')
            except Exception as e:
                print(f"Error decoding Pub/Sub message data: {e}")
                return 'Error decoding message', 500

        if not ticker:
            print("No ticker provided in Pub/Sub message")
            return 'No ticker provided in Pub/Sub message', 400

        print(f"Received request for ticker: {ticker}")

        # 1. Fetch numerical data from BigQuery
        numerical_data = fetch_numerical_data_from_bq(ticker)

        # 2. Format data for LLM
        formatted_data = format_numerical_data_for_llm(numerical_data, ticker)

        # 3. Generate summary using Vertex AI Gemini
        summary = generate_summary_with_gemini(formatted_data)
        if not summary:
            return jsonify({'status': 'error', 'message': f'Failed to generate summary for {ticker}.'}), 500

        # 4. Generate embedding for the summary using Vertex AI
        embedding = generate_embedding(summary)
        if not embedding:
            return jsonify({'status': 'error', 'message': f'Failed to generate embedding for {ticker}.'}), 500

        # 5. Store insight (summary + embedding) in Vertex AI Vector Search
        vector_search_success = store_insight_in_vector_search(ticker, summary, embedding)

        if vector_search_success:
            # 6. Publish to insights-generated-topic
            publish_pubsub_message(
                INSIGHTS_GENERATED_TOPIC,
                {'ticker': ticker, 'summary': summary, 'timestamp': datetime.now().isoformat()},
                {'event_type': 'insight_generated'}
            )
            print(f"Successfully processed and summarized numerical data for {ticker}")
            return jsonify({'status': 'success', 'message': f'Numerical data for {ticker} summarized and stored.'}), 200
        else:
            print(f"Failed to store insight for {ticker} in Vector Search.")
            return jsonify(
                {'status': 'error', 'message': f'Failed to store insight for {ticker} in Vector Search.'}), 500

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))