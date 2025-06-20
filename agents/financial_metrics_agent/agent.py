from common.adk_base import ADKBaseAgent
from tools.bigquery_tool import BigQueryTool
# FROM THIS LINE:
from agents.financial_metrics_agent.alpha_vantage_processor import AlphaVantageProcessor # Changed import
from common.constants import BIGQUERY_TABLE_FINANCIAL_METRICS, PUBSUB_TOPIC_FINANCIAL_DATA_AVAILABLE, PUBSUB_TOPIC_DASHBOARD_UPDATES
import json

class FinancialMetricsAgent(ADKBaseAgent):
    """
    The Financial Metrics Agent fetches financial data using Alpha Vantage,
    formats it, and stores it in BigQuery.
    It responds to requests from the Coordinator Agent.
    """
    def __init__(self):
        super().__init__("FinancialMetricsAgent")
        self.av_processor = AlphaVantageProcessor() # Changed processor instance
        self.bigquery_tool = BigQueryTool()
        print("FinancialMetricsAgent initialized.")

    def process_message(self, message_data: dict):
        """
        Processes a request to fetch financial metrics for a given ticker.
        Expected `message_data` format:
        {
            "ticker": "AAPL",
            "request_id": "unique-id-123" (optional, for tracking)
        }
        """
        ticker = message_data.get("ticker")
        request_id = message_data.get("request_id")

        if not ticker:
            error_msg = "FinancialMetricsAgent: Missing 'ticker' in request payload."
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })
            return

        print(f"FinancialMetricsAgent: Processing request for ticker: {ticker}")

        try:
            financial_data = self.av_processor.fetch_and_format_financial_metrics(ticker) # Changed processor call

            if financial_data:
                # Insert data into BigQuery
                success = self.bigquery_tool.insert_rows(BIGQUERY_TABLE_FINANCIAL_METRICS, [financial_data])

                if success:
                    print(f"FinancialMetricsAgent: Successfully stored financial data for {ticker} in BigQuery.")
                    # Publish a message indicating data availability
                    self.publish_message(PUBSUB_TOPIC_FINANCIAL_DATA_AVAILABLE, {
                        "ticker": ticker,
                        "data_type": "financial_metrics",
                        "date": financial_data.get("date"),
                        "request_id": request_id
                    })
                    self.publish_dashboard_update({
                        "request_id": request_id,
                        "status": "COMPLETED",
                        "agent": self.agent_name,
                        "message": f"Financial metrics for {ticker} fetched and stored.",
                        "data_snapshot": {
                            "current_price": financial_data.get("current_price"),
                            "day_change_percent": financial_data.get("day_change_percent")
                        }
                    })
                else:
                    error_msg = f"FinancialMetricsAgent: Failed to store financial data for {ticker} in BigQuery."
                    print(error_msg)
                    self.publish_dashboard_update({
                        "request_id": request_id,
                        "status": "FAILED",
                        "agent": self.agent_name,
                        "message": error_msg
                    })
            else:
                error_msg = f"FinancialMetricsAgent: No financial data retrieved for {ticker}."
                print(error_msg)
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "FAILED",
                    "agent": self.agent_name,
                    "message": error_msg
                })

        except Exception as e:
            error_msg = f"FinancialMetricsAgent: An unexpected error occurred while processing {ticker}: {e}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })

# Entry point for the Cloud Run service
app = FinancialMetricsAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running FinancialMetricsAgent direct test ---")
    agent_instance = FinancialMetricsAgent()

    # Simulate a message for a valid ticker
    test_message_valid = {
        "ticker": "RELIANCE.NS", # Use a ticker you know works, like RELIANCE.BSE
        "request_id": "test-req-123"
    }
    print(f"Simulating request for {test_message_valid['ticker']}...")
    agent_instance.process_message(test_message_valid)

    print("\n--- Simulating request for a non-existent ticker ---")
    test_message_invalid = {
        "ticker": "NONEXISTENT.BSE", # This should trigger an error path
        "request_id": "test-req-456"
    }
    print(f"Simulating request for {test_message_invalid['ticker']}...")
    agent_instance.process_message(test_message_invalid)

    print("\n--- FinancialMetricsAgent direct test completed ---")