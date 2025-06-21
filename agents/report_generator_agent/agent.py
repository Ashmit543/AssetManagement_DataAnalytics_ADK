# agents/report_generator_agent/agent.py

import json
import os
import time
import base64
from datetime import datetime, timezone
from common.adk_base import ADKBaseAgent
from common.utils import get_current_ist_timestamp
from common.constants import (
    PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS,
    PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED,
    PUBSUB_TOPIC_DASHBOARD_UPDATES,
    REGION, # Use REGION here, as defined in common/constants.py
    PROJECT_ID
)
from tools.gemini_tool import GeminiTool   # Assuming this path is correct
from flask import request  # Import request for test context


class ReportGeneratorAgent(ADKBaseAgent):
    """
    The Report Generator Agent listens for requests to generate reports,
    uses Gemini to create the report content, and publishes the completed report.
    """

    def __init__(self):
        super().__init__("ReportGeneratorAgent")
        print("ReportGeneratorAgent initialized.")
        # Use REGION from common.constants.py as the location for GeminiTool
        self.gemini_tool = GeminiTool() # Call without arguments

    def process_message(self, message_data: dict):
        """
        Processes a report generation request.
        Expected `message_data` format:
        {
            "report_type": "Executive Summary",
            "company_ticker": "IBM",
            "request_id": "unique-request-id-123",
            "report_date": "2025-06-19" # Date for which report is requested
        }
        """
        report_type = message_data.get("report_type")
        company_ticker = message_data.get("company_ticker")
        request_id = message_data.get("request_id")
        report_date = message_data.get("report_date")

        if not report_type or not company_ticker or not request_id or not report_date:
            error_msg = f"ReportGeneratorAgent: Missing essential data in request (report_type, company_ticker, request_id, report_date). Received: {message_data}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })
            return

        print(
            f"ReportGeneratorAgent: Starting generation of '{report_type}' report for {company_ticker} on {report_date} (Request ID: {request_id}).")

        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "IN_PROGRESS",
            "agent": self.agent_name,
            "message": f"Generating '{report_type}' report for {company_ticker}."
        })

        try:
            # For demonstration, let's assume we have some dummy financial data or fetched it.
            # In a real scenario, this would come from NumericalSummarizer or FinancialMetrics agents.
            dummy_financial_data = {
                "ticker": company_ticker,
                "date": report_date,
                "revenue": "50B",
                "profit": "10B",
                "growth_rate": "15%",
                "market_sentiment": "positive",
                "key_insights": "Strong quarter with robust growth in cloud services.",
                "analyst_consensus": "Buy"
            }

            # Construct the prompt for Gemini
            prompt = f"""
            Generate an "Executive Summary" report for {company_ticker} based on the following financial data and insights for the date {report_date}:

            Financial Data:
            - Revenue: {dummy_financial_data['revenue']}
            - Profit: {dummy_financial_data['profit']}
            - Growth Rate: {dummy_financial_data['growth_rate']}
            - Market Sentiment: {dummy_financial_data['market_sentiment']}
            - Key Insights: {dummy_financial_data['key_insights']}
            - Analyst Consensus: {dummy_financial_data['analyst_consensus']}

            The report should be concise, professional, and highlight key performance indicators and future outlook.
            Format the output as a JSON object with the following structure:
            {{
                "report_title": "Executive Summary for {company_ticker}",
                "summary": "...",
                "key_highlights": [ "...", "..." ],
                "future_outlook": "...",
                "disclaimer": "This report is for informational purposes only and should not be considered financial advice."
            }}
            """

            print(f"ReportGeneratorAgent: Sending prompt to Gemini for {company_ticker}...")
            gemini_response = self.gemini_tool.send_prompt(prompt, response_format="json")
            print(f"ReportGeneratorAgent: Received response from Gemini.")

            # ADD THESE LINES TO PRINT THE REPORT CONTENT
            if gemini_response:
                print("\n--- GENERATED REPORT CONTENT START ---")
                print(gemini_response)  # Or gemini_response.text if it's a model response object
                print("--- GENERATED REPORT CONTENT END ---\n")
            else:
                print("Gemini response was empty or not handled.")

            # Parse the Gemini response (it should already be a dict if response_format="json" worked)
            generated_report_content = gemini_response  # Assuming GeminiTool already returns parsed JSON/dict

            # Construct the final message for publication
            completed_report_message = {
                "request_id": request_id,
                "ticker": company_ticker,
                "report_type": report_type,
                "report_date": report_date,
                "generated_at": get_current_ist_timestamp(),
                "report_content": generated_report_content
            }

            self.publish_message(PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED, completed_report_message)
            print(
                f"ReportGeneratorAgent: Published completed '{report_type}' report for {company_ticker} (ID: {request_id}).")

            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "COMPLETED",
                "agent": self.agent_name,
                "message": f"'{report_type}' report generated for {company_ticker}."
            })

        except Exception as e:
            error_msg = f"ReportGeneratorAgent: Error generating report for {company_ticker} (Request ID: {request_id}): {e}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })


# Entry point for the Cloud Run service.
# This is the standard way to expose the Flask app for deployment.
app = ReportGeneratorAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running ReportGeneratorAgent direct test ---")

    # We create an instance just for the test block
    test_agent_instance = ReportGeneratorAgent()

    test_ticker_for_report = "GOOGL"
    test_report_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")

    # Simulate a message from CoordinatorAgent or other source
    # This message needs to be wrapped in the Pub/Sub push message format
    # The 'data' field must be base64 encoded.
    simulated_report_request_payload = {
        "report_type": "Executive Summary",
        "company_ticker": test_ticker_for_report,
        "request_id": "report-test-req-1",
        "report_date": test_report_date
    }

    # Encode the message data to base64
    encoded_data = base64.b64encode(json.dumps(simulated_report_request_payload).encode("utf-8")).decode("utf-8")

    # Construct the full Pub/Sub message format
    simulated_pubsub_message = {
        "message": {
            "data": encoded_data,
            "messageId": "test-report-message-id-456",
            "publishTime": datetime.now(timezone.utc).isoformat(),
            "attributes": {
                "eventType": "report_generation_request"
            }
        },
        "subscription": "projects/your-project-id/subscriptions/test-report-subscription"  # Placeholder
    }

    print(
        f"\nSimulating Pub/Sub message for report generation request for {test_ticker_for_report} on {test_report_date}...")

    # Use app.test_request_context to simulate an incoming HTTP POST request
    with app.test_request_context(method='POST', json=simulated_pubsub_message):
        # Now, `request.get_json()` will work correctly inside handle_pubsub_message
        test_agent_instance.handle_pubsub_message(request)  # Pass the Flask request object

    print("\n--- ReportGeneratorAgent direct test completed ---")