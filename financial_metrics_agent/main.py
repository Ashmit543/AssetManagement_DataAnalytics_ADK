import os
import yfinance as yf
from google.cloud import bigquery, pubsub_v1
from datetime import datetime, date
from flask import Flask, request, jsonify
import json
import base64

app = Flask(__name__)

# --- Configuration ---
# Your GCP Project ID (replace 'finopsanalystagent' with your actual project ID)
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'finopsanalystagent')
BIGQUERY_DATASET = 'AssetMgt_data'  # Your dataset ID
BIGQUERY_TABLE = 'financial_metrics'  # Your BigQuery table name

# Pub/Sub topics (ensure these topics exist or are created in GCP)
NUMERICAL_DATA_PROCESSED_TOPIC = 'numerical-data-processed-topic'
FINANCIAL_DATA_UPDATED_TOPIC = 'financial-data-updated-topic'

# Initialize BigQuery and Pub/Sub clients
bigquery_client = bigquery.Client(project=PROJECT_ID)
pubsub_publisher = pubsub_v1.PublisherClient()


# --- Helper Functions (as explained in previous responses) ---

def fetch_financial_data(ticker):
    """Fetches comprehensive financial data for a given ticker using yfinance."""
    print(f"Fetching data for {ticker}...")
    stock = yf.Ticker(ticker)

    info = stock.info
    hist = stock.history(period="1y")

    data = {
        'ticker': ticker,
        'current_price': info.get('currentPrice'),
        'day_change_percent': info.get('regularMarketChangePercent'),
        'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
        'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
        'market_cap': info.get('marketCap'),
        'volume': info.get('volume'),
        'average_volume': info.get('averageVolume'),
        'dividend_yield': info.get('dividendYield'),
        'sector': info.get('sector'),
        'industry': info.get('industry'),
        'info': info  # Store full info for later parsing if needed
    }

    # Fetch and format financials (income statement, balance sheet, cash flow)
    # This is a simplified example. You might need to iterate through periods or specific metrics.
    financials_data = {}
    if stock.financials is not None:
        if not stock.financials.empty:
            for col_name, col_data in stock.financials.iloc[:, :1].items():  # Take only the latest column
                financials_data[col_name.strftime('%Y-%m-%d')] = col_data.to_dict()

    balance_sheet_data = {}
    if stock.balance_sheet is not None:
        if not stock.balance_sheet.empty:
            for col_name, col_data in stock.balance_sheet.iloc[:, :1].items():
                balance_sheet_data[col_name.strftime('%Y-%m-%d')] = col_data.to_dict()

    cash_flow_data = {}
    if stock.cashflow is not None:
        if not stock.cashflow.empty:
            for col_name, col_data in stock.cashflow.iloc[:, :1].items():
                cash_flow_data[col_name.strftime('%Y-%m-%d')] = col_data.to_dict()

    data['financials'] = financials_data
    data['balance_sheet'] = balance_sheet_data
    data['cash_flow'] = cash_flow_data
    data['history'] = hist.to_dict('records')  # Convert historical DataFrame to list of dicts

    print(f"Finished fetching data for {ticker}.")
    return data


def transform_data_for_bq(raw_data):
    """
    Transforms raw yfinance data into rows for the financial_metrics BigQuery table.
    """
    rows = []
    ticker = raw_data['ticker']
    current_timestamp = datetime.now().isoformat()

    # --- Basic Stock Performance Metrics ---
    metrics_to_include = {
        'current_price': ('Valuation', 'Current Price', 'INR'),
        'day_change_percent': ('Valuation', 'Day Change %', '%'),
        'fifty_two_week_high': ('Valuation', '52-Week High', 'INR'),
        'fifty_two_week_low': ('Valuation', '52-Week Low', 'INR'),
        'market_cap': ('Valuation', 'Market Cap', 'INR'),
        'volume': ('Trading', 'Volume', 'shares'),
        'average_volume': ('Trading', 'Average Volume', 'shares'),
        'dividend_yield': ('Valuation', 'Dividend Yield', '%')
    }

    for key, (category, name, unit) in metrics_to_include.items():
        value = raw_data.get(key)
        if value is not None:
            rows.append({
                'company_ticker': ticker,
                'report_date': date.today().isoformat(),
                'metric_category': category,
                'metric_name': name,
                'metric_value': float(value),
                'unit': unit,
                'fiscal_period': 'N/A',
                'source': 'yfinance',
                'extraction_timestamp': current_timestamp
            })

    # --- Financial Statement Metrics (Income Statement, Balance Sheet, Cash Flow) ---
    # This section is simplified. You would need to add more robust parsing
    # and error handling for real-world financial statements.

    financial_metrics_map = {
        'financials': {
            'Total Revenue': ('Income Statement', 'Revenue', 'INR'),
            'Net Income': ('Income Statement', 'Net Income', 'INR'),
            'Basic Average Shares': ('Income Statement', 'Basic Average Shares', 'shares')
        },
        'balance_sheet': {
            'Total Assets': ('Balance Sheet', 'Total Assets', 'INR'),
            'Total Liabilities Net Minority Interest': ('Balance Sheet', 'Total Liabilities', 'INR'),
            'Total Stockholder Equity': ('Balance Sheet', 'Total Equity', 'INR')
        },
        'cash_flow': {
            'Operating Cash Flow': ('Cash Flow', 'Operating Cash Flow', 'INR'),
            'Capital Expenditures': ('Cash Flow', 'Capital Expenditures', 'INR')
        }
    }

    for report_type_key, metrics_map in financial_metrics_map.items():
        data_dict = raw_data.get(report_type_key)
        if data_dict:
            # Try to get the latest available report date
            latest_report_date_str = None
            if data_dict:
                try:
                    # Assuming keys are string dates, sort them to get the latest
                    latest_report_date_str = sorted(data_dict.keys(), reverse=True)[0]
                except (KeyError, IndexError):
                    pass  # No data or empty dict

            if latest_report_date_str:
                latest_report_data = data_dict[latest_report_date_str]

                for yf_metric_name, (category, bq_metric_name, unit) in metrics_map.items():
                    value = latest_report_data.get(yf_metric_name)
                    if value is not None:
                        rows.append({
                            'company_ticker': ticker,
                            'report_date': latest_report_date_str,
                            'metric_category': category,
                            'metric_name': bq_metric_name,
                            'metric_value': float(value),
                            'unit': unit,
                            'fiscal_period': 'Latest Reported',  # Can improve this by parsing yfinance index dates
                            'source': 'yfinance',
                            'extraction_timestamp': current_timestamp
                        })

    # --- Historical Data (Close Price) ---
    for record in raw_data.get('history', []):
        if 'Close' in record and 'Date' in record:
            rows.append({
                'company_ticker': ticker,
                'report_date': record.get('Date', '').split('T')[0],  # Extract YYYY-MM-DD
                'metric_category': 'Historical',
                'metric_name': 'Close Price',
                'metric_value': float(record.get('Close')),
                'unit': 'INR',
                'fiscal_period': 'Daily',
                'source': 'yfinance',
                'extraction_timestamp': current_timestamp
            })

    # --- Technical Indicators (Example: Beta) ---
    # Beta is often in the main info object
    beta = raw_data['info'].get('beta')
    if beta is not None:
        rows.append({
            'company_ticker': ticker,
            'report_date': date.today().isoformat(),
            'metric_category': 'Technical',
            'metric_name': 'Beta',
            'metric_value': float(beta),
            'unit': 'ratio',
            'fiscal_period': 'N/A',
            'source': 'yfinance',
            'extraction_timestamp': current_timestamp
        })

    return rows


def load_to_bigquery(rows):
    """Loads a list of rows into the BigQuery financial_metrics table."""
    if not rows:
        print("No rows to insert into BigQuery.")
        return True  # Consider it a success if no rows to insert

    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    try:
        errors = bigquery_client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
            return False
        else:
            print(f"Successfully inserted {len(rows)} rows into {table_id}.")
            return True
    except Exception as e:
        print(f"Error loading to BigQuery: {e}")
        return False


def publish_pubsub_message(topic_name, message_data, attributes=None):
    """Publishes a message to a given Pub/Sub topic."""
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
            # Pub/Sub message data is base64 encoded
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
        # 1. Fetch data from yfinance
        raw_data = fetch_financial_data(ticker)

        # 2. Transform data for BigQuery
        bq_rows = transform_data_for_bq(raw_data)

        # 3. Load to BigQuery
        bq_success = load_to_bigquery(bq_rows)

        if bq_success:
            # 4. Publish to numerical-data-processed-topic (for Numerical Summarizer Agent)
            publish_pubsub_message(
                NUMERICAL_DATA_PROCESSED_TOPIC,
                {'ticker': ticker, 'data_source': 'financial_metrics', 'timestamp': datetime.now().isoformat()},
                {'event_type': 'numerical_data_processed'}
            )
            # 5. Publish to financial-data-updated-topic (for Anomaly Detector Agent/Dashboard notification)
            publish_pubsub_message(
                FINANCIAL_DATA_UPDATED_TOPIC,
                {'ticker': ticker, 'status': 'updated', 'timestamp': datetime.now().isoformat()},
                {'event_type': 'financial_data_updated'}
            )
            print(f"Successfully processed financial data for {ticker}")
            return jsonify({'status': 'success', 'message': f'Financial data for {ticker} processed.'}), 200
        else:
            print(f"Failed to load data for {ticker} to BigQuery.")
            return jsonify({'status': 'error', 'message': f'Failed to load data for {ticker} to BigQuery.'}), 500

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    # This is for local testing only.
    # When deployed on Cloud Run, Gunicorn will manage the application.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))