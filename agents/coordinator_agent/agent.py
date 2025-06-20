# agents/coordinator_agent/agent.py

import json
import base64
from datetime import datetime, timezone
from common.adk_base import ADKBaseAgent
from common.constants import (
    # Make sure these constants are defined in common/constants.py
    PUBSUB_TOPIC_FINANCIAL_METRICS_PROCESSED,
    PUBSUB_TOPIC_NUMERICAL_INSIGHTS_PROCESSED,
    PUBSUB_TOPIC_REPORT_GENERATION_REQUEST,
    PUBSUB_TOPIC_DASHBOARD_UPDATES,
)
from common.utils import get_current_ist_timestamp
from flask import request  # Import request from flask for test context


class CoordinatorAgent(ADKBaseAgent):
    """
    The Coordinator Agent orchestrates the workflow of generating reports.
    It listens for numerical insights and triggers the report generation process.
    """

    def __init__(self):
        super().__init__("CoordinatorAgent")
        print("CoordinatorAgent initialized.")

    def process_message(self, message_data: dict):
        """
        Processes messages from numerical-insights-processed-topic.
        Expected `message_data` format:
        {
            "ticker": "IBM",
            "date": "2025-06-19",
            "insights_count": 2,
            "request_id": "num-summ-req-1" # The request ID from the original trigger
        }
        """
        ticker = message_data.get("ticker")
        data_date = message_data.get("date")
        request_id = message_data.get("request_id")

        if not ticker or not data_date:
            print("CoordinatorAgent: Missing 'ticker' or 'date' in numerical insights processed message. Skipping.")
            return

        print(f"CoordinatorAgent: Received numerical insights processed for {ticker} on {data_date}.")

        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "INFO",
            "agent": self.agent_name,
            "message": f"Numerical insights ready for {ticker}. Triggering report generation."
        })

        # Trigger ReportGeneratorAgent
        report_request_message = {
            "report_type": "Executive Summary",
            "company_ticker": ticker,
            "request_id": request_id,
            "report_date": data_date
        }

        self.publish_message(PUBSUB_TOPIC_REPORT_GENERATION_REQUEST, report_request_message)
        print(f"CoordinatorAgent: Triggered ReportGeneratorAgent for {ticker} report on {data_date}.")

        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "COMPLETED",
            "agent": self.agent_name,
            "message": f"Orchestration completed for {ticker}. Report generation initiated."
        })


# Entry point for the Cloud Run service.
app = CoordinatorAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running CoordinatorAgent direct test ---")

    # We create an instance just for the test block
    test_agent_instance = CoordinatorAgent()

    test_ticker_for_coord = "TESTDATA"
    # Use current date for testing
    current_date_ist = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    test_data_date_for_coord = current_date_ist

    simulated_numerical_insights_payload = {
        "ticker": test_ticker_for_coord,
        "date": test_data_date_for_coord,
        "insights_count": 2,
        "request_id": "coord-test-req-1"
    }

    # Encode the message data to base64
    encoded_data = base64.b64encode(json.dumps(simulated_numerical_insights_payload).encode("utf-8")).decode("utf-8")

    # Construct the full Pub/Sub message format (as it would be in the HTTP request body)
    simulated_pubsub_message_body = {
        "message": {
            "data": encoded_data,
            "messageId": "test-message-id-123",
            "publishTime": datetime.now(timezone.utc).isoformat(),
            "attributes": {
                "eventType": "numerical_insights_processed"
            }
        },
        # The subscription field is optional for local testing but good to include for realism
        "subscription": "projects/your-project-id/subscriptions/test-subscription"
    }

    print(
        f"\nSimulating Pub/Sub message for numerical insights processed for {test_ticker_for_coord} on {test_data_date_for_coord}...")

    # Use app.test_request_context to simulate an incoming HTTP POST request
    with app.test_request_context(method='POST', json=simulated_pubsub_message_body):
        # Now, `request.get_json()` will work correctly inside handle_pubsub_message
        test_agent_instance.handle_pubsub_message(request)  # Pass the Flask request object

    print("\n--- CoordinatorAgent direct test completed ---")