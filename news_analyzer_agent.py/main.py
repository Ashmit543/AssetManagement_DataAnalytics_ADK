# gemini_llm_agent/main.py

import os
import base64
import json
from datetime import datetime

from flask import Flask, request, jsonify
import google.generativeai as genai
from google.cloud import bigquery, pubsub_v1, aiplatform
import requests # For Alpha Vantage API calls

# Import constants from common module
# This assumes 'common' directory is accessible in the Python path when deployed
try:
    from common.constants import (
        PROJECT_ID, REGION,
        QUALITATIVE_DATA_REQUEST_TOPIC, QUALITATIVE_INSIGHTS_GENERATED_TOPIC
    )
except ImportError:
    # Fallback if common.constants cannot be imported (e.g., local testing without proper path setup)
    print("Warning: Could not import common.constants. Falling back to environment variables or defaults.")
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'your-gcp-project-id') # <<< IMPORTANT: REPLACE IF NOT IN ENV
    REGION = os.environ.get('GCP_REGION', 'asia-south1')
    QUALITATIVE_DATA_REQUEST_TOPIC = os.environ.get('QUALITATIVE_REQUEST_TOPIC', 'qualitative-data-request-topic')
    QUALITATIVE_INSIGHTS_GENERATED_TOPIC = os.environ.get('QUALITATIVE_INSIGHTS_TOPIC', 'qualitative-insights-generated-topic')


# Initialize Flask app
app = Flask(__name__)

# Initialize Google Cloud Clients
pubsub_publisher = pubsub_v1.PublisherClient()

# Configure Vertex AI and Gemini Models
aiplatform.init(project=PROJECT_ID, location=REGION)
GENERATIVE_MODEL_NAME = 'gemini-1.5-flash' # Your specified model
EMBEDDING_MODEL_NAME = 'text-embedding-004' # Recommended newer embedding model
generative_model = genai.GenerativeModel(GENERATIVE_MODEL_NAME)

# Alpha Vantage Configuration (API Key will be set as an environment variable in Cloud Run)
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# --- Generic Helper Functions ---

def publish_pubsub_message(topic_name, message_data, attributes=None):
    """Publishes a message to a given Pub/Sub topic."""
    topic_path = pubsub_publisher.topic_path(PROJECT_ID, topic_name)
    data = json.dumps(message_data).encode('utf-8')
    future = pubsub_publisher.publish(topic_path, data, **(attributes or {}))
    message_id = future.result()
    print(f"Message published to {topic_name}. Message ID: {message_id}")
    return message_id

def make_alpha_vantage_request(function_name, ticker=None, **kwargs):
    """
    Generic function to make Alpha Vantage API requests.
    Handles API key, base URL, common parameters, and error checking.
    """
    if not ALPHA_VANTAGE_API_KEY:
        print("ERROR: ALPHA_VANTAGE_API_KEY environment variable not set. Cannot make AV request.")
        return None

    params = {
        "function": function_name,
        "apikey": ALPHA_VANTAGE_API_KEY,
        **kwargs # Include any other function-specific parameters
    }
    if ticker:
        params["symbol"] = ticker # Most AV functions use 'symbol' for ticker

    print(f"Attempting AV request: Function='{function_name}', Ticker='{ticker}', Params: {kwargs}")
    try:
        response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        # Alpha Vantage often returns an error key for issues (e.g., invalid ticker, API limit)
        if "Error Message" in data:
            print(f"Alpha Vantage API Error for {function_name}: {data['Error Message']}")
            return None
        if "Note" in data and "API call frequency" in data["Note"]:
            print(f"Alpha Vantage API Call Frequency Limit Hit for {function_name}: {data['Note']}")
            # Consider adding retry logic or backoff here for production
            return None
        if not data:
            print(f"Alpha Vantage returned empty data for {function_name}.")
            return None

        print(f"Successfully fetched data for AV function {function_name}.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Network/HTTP error fetching data from Alpha Vantage for {function_name}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decoding error from Alpha Vantage response for {function_name}: {e}. Response content: {response.text}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred in make_alpha_vantage_request for {function_name}: {e}")
        return None

# --- Specific Alpha Vantage Data Fetchers & LLM Formatting ---

def get_news_sentiment_data(ticker=None, topics=None):
    """Fetches news and sentiment data and formats it for LLM."""
    params = {
        "sort": "RELEVANCE",
        "limit": 50 # Get a reasonable number of recent articles
    }
    if topics:
        params["topics"] = topics # Alpha Vantage uses 'topics' for keywords/themes

    data = make_alpha_vantage_request("NEWS_SENTIMENT", ticker=ticker, **params)
    if data and "feed" in data:
        news_text = "--- Recent News Articles ---\n\n"
        for article in data["feed"]:
            title = article.get("title", "No Title")
            summary = article.get("summary", "No Summary")
            url = article.get("url", "#")
            sentiment = article.get("overall_sentiment_label", "Neutral") # Example sentiment
            news_text += f"Title: {title}\nSummary: {summary}\nURL: {url}\nSentiment: {sentiment}\n\n"
        print(f"Prepared {len(data['feed'])} news articles for LLM.")
        return news_text
    return None

def get_company_overview_data(ticker):
    """Fetches company overview data and formats it for LLM."""
    data = make_alpha_vantage_request("OVERVIEW", ticker=ticker)
    if data:
        overview_text = f"--- Company Overview for {data.get('Name', ticker)} ---\n"
        overview_text += f"Symbol: {data.get('Symbol', 'N/A')}\n"
        overview_text += f"Exchange: {data.get('Exchange', 'N/A')}\n"
        overview_text += f"Currency: {data.get('Currency', 'N/A')}\n"
        overview_text += f"Sector: {data.get('Sector', 'N/A')}\n"
        overview_text += f"Industry: {data.get('Industry', 'N/A')}\n"
        overview_text += f"Description: {data.get('Description', 'No description available.')}\n"
        overview_text += f"Market Capitalization: {data.get('MarketCapitalization', 'N/A')}\n"
        overview_text += f"P/E Ratio: {data.get('PERatio', 'N/A')}\n"
        overview_text += f"Dividend Yield: {data.get('DividendYield', 'N/A')}\n"
        overview_text += f"Analyst Target Price: {data.get('AnalystTargetPrice', 'N/A')}\n"
        return overview_text
    return None

def get_earnings_call_data(ticker):
    """
    Fetches earnings calendar data and formats it for LLM.
    NOTE: Alpha Vantage standard API provides earnings DATES and EPS figures,
    NOT full earnings call transcripts.
    """
    data = make_alpha_vantage_request("EARNINGS", ticker=ticker) # 'EARNINGS' function provides reported/estimated EPS
    if data and "annualReports" in data: # Also 'quarterlyReports'
        earnings_text = f"--- Earnings Data for {ticker} ---\n"
        earnings_text += "Latest Annual Earnings:\n"
        latest_annual = data["annualReports"][0] if data["annualReports"] else {}
        if latest_annual:
            earnings_text += (f"  Fiscal Date: {latest_annual.get('fiscalDateEnding', 'N/A')}, "
                              f"Reported EPS: {latest_annual.get('reportedEPS', 'N/A')}\n")

        earnings_text += "\nLatest Quarterly Earnings:\n"
        latest_quarterly = data["quarterlyReports"][0] if data["quarterlyReports"] else {}
        if latest_quarterly:
            earnings_text += (f"  Fiscal Date: {latest_quarterly.get('fiscalDateEnding', 'N/A')}, "
                              f"Report Date: {latest_quarterly.get('reportedDate', 'N/A')}, "
                              f"Reported EPS: {latest_quarterly.get('reportedEPS', 'N/A')}, "
                              f"Estimated EPS: {latest_quarterly.get('estimatedEPS', 'N/A')}, "
                              f"Surprise: {latest_quarterly.get('surprise', 'N/A')} ({latest_quarterly.get('surprisePercentage', 'N/A')}%)\n")
        return earnings_text
    return "No earnings data available or Alpha Vantage does not provide full transcripts via this API."


def get_annual_report_key_points_data(ticker):
    """Fetches key financial statements (Income, Balance, Cash Flow) and formats for LLM."""
    income_data = make_alpha_vantage_request("INCOME_STATEMENT", ticker=ticker)
    balance_data = make_alpha_vantage_request("BALANCE_SHEET", ticker=ticker)
    cashflow_data = make_alpha_vantage_request("CASH_FLOW", ticker=ticker)

    report_text = f"--- Key Financial Report Highlights for {ticker} ---\n\n"

    if income_data and income_data.get("annualReports"):
        latest_income = income_data["annualReports"][0]
        report_text += f"**Latest Annual Income Statement ({latest_income.get('fiscalDateEnding', 'N/A')}):\n"
        report_text += f"- Total Revenue: {latest_income.get('totalRevenue', 'N/A')}\n"
        report_text += f"- Gross Profit: {latest_income.get('grossProfit', 'N/A')}\n"
        report_text += f"- Net Income: {latest_income.get('netIncome', 'N/A')}\n"
        report_text += f"- EPS: {latest_income.get('eps', 'N/A')}\n\n"

    if balance_data and balance_data.get("annualReports"):
        latest_balance = balance_data["annualReports"][0]
        report_text += f"**Latest Annual Balance Sheet ({latest_balance.get('fiscalDateEnding', 'N/A')}):\n"
        report_text += f"- Total Assets: {latest_balance.get('totalAssets', 'N/A')}\n"
        report_text += f"- Total Liabilities: {latest_balance.get('totalLiabilities', 'N/A')}\n"
        report_text += f"- Shareholder Equity: {latest_balance.get('totalShareholderEquity', 'N/A')}\n\n"

    if cashflow_data and cashflow_data.get("annualReports"):
        latest_cashflow = cashflow_data["annualReports"][0]
        report_text += f"**Latest Annual Cash Flow Statement ({latest_cashflow.get('fiscalDateEnding', 'N/A')}):\n"
        report_text += f"- Operating Cash Flow: {latest_cashflow.get('cashflowFromOperatingActivities', 'N/A')}\n"
        report_text += f"- Investing Cash Flow: {latest_cashflow.get('cashflowFromInvestingActivities', 'N/A')}\n"
        report_text += f"- Financing Cash Flow: {latest_cashflow.get('cashflowFromFinancingActivities', 'N/A')}\n\n"

    if report_text == f"--- Key Financial Report Highlights for {ticker} ---\n\n": # Only header if no data
        return "No financial statement data available from Alpha Vantage to summarize."
    return report_text

def get_insider_ownership_trends_data(ticker):
    """Fetches insider transaction data and formats it for LLM."""
    data = make_alpha_vantage_request("STOCK_INSIDER_TRANSACTIONS", ticker=ticker)
    if data and "data" in data:
        transactions_text = f"--- Insider Transactions for {ticker} ---\n"
        for tx in data["data"][:10]: # Limit to 10 most recent for brevity
            transactions_text += (
                f"- Owner: {tx.get('ownerName', 'N/A')} ({tx.get('relationship', 'N/A')})\n"
                f"  Type: {tx.get('transactionType', 'N/A')}, Value: {tx.get('value', 'N/A')}\n"
                f"  Date: {tx.get('reportDate', 'N/A')}, Price: {tx.get('transactionPrice', 'N/A')}\n\n"
            )
        return transactions_text
    return None

def get_macro_factors_data(indicator_type):
    """Fetches specific macro economic indicator data and formats it for LLM."""
    data = make_alpha_vantage_request(indicator_type) # e.g., "CPI", "TREASURY_YIELD"
    if data and "data" in data:
        macro_text = f"--- Latest {indicator_type.replace('_', ' ').title()} Data ---\n"
        for entry in data["data"][:5]: # Get last 5 data points
            macro_text += f"- Date: {entry.get('date', 'N/A')}, Value: {entry.get('value', 'N/A')}\n"
        return macro_text
    return None

# --- LLM and Embedding Functions ---

def analyze_data_with_gemini(data_content, insight_type, request_query=None):
    """Analyzes various types of data using Gemini."""
    if not data_content or len(data_content.strip()) < 50:
        print("Insufficient content for analysis.")
        return "No meaningful content provided for analysis."

    base_prompt = f"You are an AI financial analyst. Analyze the following data to provide concise qualitative insights related to {insight_type.replace('_', ' ').lower()}."

    if request_query: # Add specific query if provided
        base_prompt += f" Focus on answering the specific request: '{request_query}'."

    prompt = f"""{base_prompt}

Data for Analysis:
{data_content[:30000]} # Limit input size to prevent token limits. Adjust as needed.

Please provide a summary or report highlighting key themes, sentiment (if applicable), significant events, trends, and potential impacts. Structure your response with clear headings or bullet points.
"""
    print(f"Analyzing {insight_type} data with Gemini...")
    try:
        response = generative_model.generate_content(prompt)
        analysis_text = response.text
        print(f"Qualitative analysis for {insight_type} generated.")
        return analysis_text
    except Exception as e:
        print(f"Error analyzing {insight_type} data with Gemini: {e}")
        return f"Failed to generate analysis for {insight_type}: {str(e)}"

def generate_embedding(text):
    """Generates embeddings for the given text using Vertex AI Embeddings API."""
    if not text or len(text.strip()) == 0:
        print("No text provided for embedding generation.")
        return None
    print("Generating embedding...")
    try:
        embedding_model = aiplatform.get_embedding_model(EMBEDDING_MODEL_NAME)
        # Embedding models often have max input size; handle larger texts by chunking if necessary
        embeddings = embedding_model.embed(texts=[text]).predictions[0].values
        print("Embedding generated.")
        return embeddings
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

def store_qualitative_insight_in_vector_search(ticker, insight_type, query, summary, embedding):
    """
    Placeholder for storing the generated qualitative insight and its embedding in Vertex AI Vector Search.
    You would implement actual Vertex AI Vector Search upsert logic here.
    """
    print(f"Storing qualitative insight: Type='{insight_type}', Ticker='{ticker}', Query='{query}' (placeholder)...")
    return True

# --- Flask Route ---

@app.route('/', methods=['POST'])
def index():
    """
    Main entry point for the Cloud Run service.
    Expects a Pub/Sub message in the request body, including 'insight_type'.
    """
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return 'No Pub/Sub message received', 400

    pubsub_message = envelope['message']
    request_data = {}
    try:
        if 'data' in pubsub_message:
            message_data_str = base64.b64decode(pubsub_message['data']).decode('utf-8')
            request_data = json.loads(message_data_str)
        else:
            # For non-Pub/Sub test calls, try to parse JSON directly
            print("Pub/Sub message 'data' field missing, attempting to parse directly from body.")
            request_data = request.get_json()

    except Exception as e:
        print(f"Error decoding Pub/Sub message data: {e}")
        return 'Error decoding message', 500

    # Extract required parameters: insight_type, and optional ticker/query
    insight_type = request_data.get('insight_type')
    company_ticker = request_data.get('company_ticker')
    query = request_data.get('query') # General query for LLM or specific topic for news

    if not insight_type:
        return jsonify({'status': 'error', 'message': 'Missing "insight_type" in request.'}), 400

    print(f"Processing request for insight type: '{insight_type}'. Ticker: '{company_ticker}', Query: '{query}'")

    data_content_for_llm = None
    # --- Route to appropriate Alpha Vantage data fetcher based on insight_type ---
    if insight_type == "NEWS_SENTIMENT_SUMMARY":
        data_content_for_llm = get_news_sentiment_data(ticker=company_ticker, topics=query)
    elif insight_type == "COMPANY_SUMMARY":
        if not company_ticker: return jsonify({'status': 'error', 'message': 'Company ticker required for COMPANY_SUMMARY.'}), 400
        data_content_for_llm = get_company_overview_data(company_ticker)
    elif insight_type == "EARNINGS_CALL_SUMMARY":
        if not company_ticker: return jsonify({'status': 'error', 'message': 'Company ticker required for EARNINGS_CALL_SUMMARY.'}), 400
        data_content_for_llm = get_earnings_call_data(company_ticker)
    elif insight_type == "ANNUAL_REPORT_KEY_POINTS":
        if not company_ticker: return jsonify({'status': 'error', 'message': 'Company ticker required for ANNUAL_REPORT_KEY_POINTS.'}), 400
        data_content_for_llm = get_annual_report_key_points_data(company_ticker)
    elif insight_type == "INSIDER_OWNERSHIP_TRENDS":
        if not company_ticker: return jsonify({'status': 'error', 'message': 'Company ticker required for INSIDER_OWNERSHIP_TRENDS.'}), 400
        data_content_for_llm = get_insider_ownership_trends_data(company_ticker)
    elif insight_type == "MACRO_FACTORS_CPI": # Example for a specific macro factor
        data_content_for_llm = get_macro_factors_data("CPI")
    elif insight_type == "MACRO_FACTORS_TREASURY_YIELD": # Another example
        data_content_for_llm = get_macro_factors_data("TREASURY_YIELD")
    # You can add more 'elif' conditions for other Alpha Vantage macro functions or technicals
    # elif insight_type == "TECHNICAL_RSI":
    #    data_content_for_llm = get_technical_indicator_data(company_ticker, "RSI")
    else:
        return jsonify({'status': 'error', 'message': f"Unsupported insight_type: {insight_type}"}), 400

    if not data_content_for_llm or "No meaningful content" in data_content_for_llm:
        print(f"WARN: No data fetched or meaningful content for {insight_type}. Skipping LLM analysis.")
        return jsonify({'status': 'warning', 'message': f'Failed to fetch or process data for {insight_type}.'}), 200

    try:
        # 2. Summarize/Analyze data with Gemini
        summary_result = analyze_data_with_gemini(data_content_for_llm, insight_type, query)
        if "Failed to generate analysis" in summary_result or not summary_result:
            print(f"ERROR: Failed to generate analysis for {insight_type} with Gemini: {summary_result}")
            return jsonify({'status': 'error', 'message': summary_result}), 500

        # 3. Generate embedding for the summary
        embedding = generate_embedding(summary_result)
        if not embedding:
            print(f"ERROR: Failed to generate embedding for {insight_type} summary.")
            return jsonify({'status': 'error', 'message': f'Failed to generate embedding for {insight_type} summary.'}), 500

        # 4. Store insight in Vector Search (placeholder)
        store_qualitative_insight_in_vector_search(company_ticker, insight_type, query, summary_result, embedding)

        # 5. Publish to qualitative-insights-generated-topic
        publish_pubsub_message(
            QUALITATIVE_INSIGHTS_GENERATED_TOPIC,
            {'type': insight_type, 'query': query, 'ticker': company_ticker,
             'summary': summary_result, 'embedding': embedding,
             'timestamp': datetime.now().isoformat()},
            {'event_type': f'{insight_type.lower().replace("_", "")}_generated'}
        )

        print(f"Successfully processed {insight_type} request for Ticker: '{company_ticker}'.")
        return jsonify({'status': 'success', 'message': f'{insight_type} analyzed and stored.'}), 200

    except Exception as e:
        print(f"An unexpected error occurred during {insight_type} analysis: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    # For local testing: Set ALPHA_VANTAGE_API_KEY, GCP_PROJECT_ID, GCP_REGION as environment variables
    # Example: set ALPHA_VANTAGE_API_KEY=YOUR_KEY
    # Example: set GCP_PROJECT_ID=your-project-id
    # Example: set GCP_REGION=asia-south1
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))