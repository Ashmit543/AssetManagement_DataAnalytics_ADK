import os
from google.cloud import bigquery, pubsub_v1
# We still keep aiplatform imported because you have `aiplatform.init`
# and it might be used for other Vertex AI functionalities later,
# even if not directly for model loading in this specific way.
import google.cloud.aiplatform as aiplatform
from datetime import datetime, date
from flask import Flask, request, jsonify
import google.generativeai as genai # This is what you should use for text-embedding-004
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
GENERATIVE_MODEL_NAME = 'gemini-2.0-flash-lite'  # Or 'gemini-1.0-pro-001' etc.
EMBEDDING_MODEL_NAME = 'text-embedding-004'  # THIS IS A GEMINI EMBEDDING MODEL
# IMPORTANT: Replace with your actual endpoint ID from the Cloud Console
VECTOR_SEARCH_INDEX_ENDPOINT_ID = '8168368639771148288'

# Initialize Clients
bigquery_client = bigquery.Client(project=PROJECT_ID)
pubsub_publisher = pubsub_v1.PublisherClient()

# Initialize Vertex AI - Keep this if you need other Vertex AI features,
# but it's not strictly required for `genai.GenerativeModel` or `genai.embed_content`
# as they handle auth via ADC when on GCP.
aiplatform.init(project=PROJECT_ID, location=REGION)

# Generative Model from google.generativeai (already corrected)
generative_model = genai.GenerativeModel(GENERATIVE_MODEL_NAME)

# REMOVE THIS LINE:
# embedding_model = aiplatform.get_embedding_model(EMBEDDING_MODEL_NAME)
# Instead, we will use `genai.embed_content` directly in the function.


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
    """Generates a summary using the Gemini generative model."""
    print("Generating summary with Gemini...")
    try:
        response = generative_model.generate_content(formatted_data)
        summary_text = response.text
        print("Summary generated.")
        return summary_text
    except Exception as e:
        print(f"Error generating summary with Gemini: {e}")
        return None


def generate_embedding(text):
    """Generates a vector embedding for a given text using google.generativeai."""
    print("Generating embedding...")
    try:
        # Use genai.embed_content for 'text-embedding-004' (Gemini embedding model)
        response = genai.embed_content(model=EMBEDDING_MODEL_NAME, content=text)
        embeddings = response['embedding'] # Access the embedding from the response
        print("Embedding generated.")
        return embeddings
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None


def store_insight_in_vector_search(ticker, summary, embedding):
    """Stores the generated insight and its embedding in Vertex AI Vector Search."""
    print(f"Storing insight for {ticker} in Vector Search (placeholder for now)...")

    # This is a placeholder for the actual upsert operation.
    # For a real implementation, you would use Vertex AI SDK to upsert datapoints
    # into your deployed Vector Search index.
    # Example (conceptual, requires more setup):
    # from google.cloud import aiplatform_v1beta1
    # client_options = {"api_endpoint": f"{REGION}-aiplatform.googleapis.com"}
    # index_endpoint_client = aiplatform_v1beta1.IndexEndpointServiceClient(client_options=client_options)
    # deployed_index_resource_name = f"projects/{PROJECT_ID}/locations/{REGION}/indexEndpoints/{VECTOR_SEARCH_INDEX_ENDPOINT_ID}/deployedIndexes/your_deployed_index_id_from_endpoint_details"
    #
    # data_points = [
    #     aiplatform_v1beta1.IndexDatapoint(
    #         datapoint_id=f"{ticker}_{datetime.now().isoformat()}",
    #         feature_vector=embedding,
    #         restricts=[aiplatform_v1beta1.IndexDatapoint.Restriction(namespace="ticker", allow_list=[ticker])],
    #         metadata={"summary": summary, "timestamp": datetime.now().isoformat()}
    #     )
    # ]
    #
    # try:
    #     index_endpoint_client.upsert_datapoints(
    #         index_endpoint=deployed_index_resource_name,
    #         datapoints=data_points
    #     )
    #     print(f"Successfully stored insight for {ticker} in Vector Search.")
    #     return True
    # except Exception as e:
    #     print(f"Error storing insight in Vector Search: {e}")
    #     return False
    return True  # Always return True for now due to placeholder implementation


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
    envelope = request.get_json()
    if not envelope:
        return 'No Pub/Sub message received', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
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
        return 'No ticker provided in Pub/Sub message', 400

    print(f"Received request for ticker: {ticker}")
    try:
        # 1. Fetch numerical data from BigQuery
        numerical_data = fetch_numerical_data_from_bq(ticker)

        # 2. Format data for LLM
        formatted_data = format_numerical_data_for_llm(numerical_data, ticker)

        # 3. Generate summary using Gemini
        summary = generate_summary_with_gemini(formatted_data)
        if not summary:
            return jsonify({'status': 'error', 'message': f'Failed to generate summary for {ticker}.'}), 500

        # 4. Generate embedding for the summary
        embedding = generate_embedding(summary) # This now uses genai.embed_content
        if not embedding:
            return jsonify({'status': 'error', 'message': f'Failed to generate embedding for {ticker}.'}), 500

        # 5. Store insight (summary + embedding) in Vertex AI Vector Search
        # NOTE: The store_insight_in_vector_search function in this example is a placeholder.
        # It currently just logs the intention. You will need to implement the actual upsert logic later.
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
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))