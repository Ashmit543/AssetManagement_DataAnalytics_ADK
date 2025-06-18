import json
from common.adk_base import ADKBaseAgent
from common.constants import (
    PUBSUB_TOPIC_FINANCIAL_DATA_REQUESTS,
    PUBSUB_TOPIC_NUMERICAL_SUMMARIES_REQUESTS,
    PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS,
    PUBSUB_TOPIC_DASHBOARD_UPDATES
)
from flask import jsonify # Ensure jsonify is imported if used in index or handle_pubsub_message

class CoordinatorAgent(ADKBaseAgent):
    """
    The Coordinator Agent orchestrates requests, routing them to appropriate
    downstream agents based on the received message content.
    It primarily listens for requests from the Dashboard Agent or direct triggers.
    """
    def __init__(self):
        super().__init__("CoordinatorAgent")
        print("CoordinatorAgent initialized.")

    def process_message(self, message_data: dict):
        """
        Processes an incoming message, determines the target agent, and routes the request.
        Expected `message_data` format:
        {
            "request_type": "financial_metrics" | "numerical_summary" | "generate_report",
            "payload": { ... } # specific data for the request
        }
        """
        request_type = message_data.get("request_type")
        payload = message_data.get("payload", {})
        request_id = message_data.get("request_id") # Unique ID to track requests

        print(f"Coordinator: Received request_type: {request_type} with payload: {payload}")

        if not request_type:
            print("Coordinator: Missing 'request_type' in message. Cannot route.")
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "message": "Missing request_type in coordinator request."
            })
            return

        response_topic = None # Future: Can use reply-to topic for direct response

        try:
            if request_type == "financial_metrics":
                if not payload.get("ticker"):
                    raise ValueError("Financial metrics request requires 'ticker'.")
                self.publish_message(PUBSUB_TOPIC_FINANCIAL_DATA_REQUESTS, payload)
                print(f"Coordinator: Routed financial metrics request for {payload.get('ticker')} to Financial Metrics Agent.")
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "ROUTED",
                    "agent": "FinancialMetricsAgent",
                    "message": f"Request for financial metrics for {payload.get('ticker')} routed."
                })

            elif request_type == "numerical_summary":
                if not payload.get("ticker") and not payload.get("insight_type"):
                    raise ValueError("Numerical summary request requires 'ticker' or 'insight_type'.")
                self.publish_message(PUBSUB_TOPIC_NUMERICAL_SUMMARIES_REQUESTS, payload)
                print(f"Coordinator: Routed numerical summary request for {payload.get('ticker')} to Numerical Summarizer Agent.")
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "ROUTED",
                    "agent": "NumericalSummarizerAgent",
                    "message": f"Request for numerical summary for {payload.get('ticker')} routed."
                })

            elif request_type == "generate_report":
                if not payload.get("report_type") or not (payload.get("ticker") or payload.get("sector")):
                    raise ValueError("Report generation request requires 'report_type' and 'ticker' or 'sector'.")
                self.publish_message(PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS, payload)
                print(f"Coordinator: Routed report generation request for {payload.get('report_type')} to Report Generator Agent.")
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "ROUTED",
                    "agent": "ReportGeneratorAgent",
                    "message": f"Request for report '{payload.get('report_type')}' routed."
                })

            else:
                error_msg = f"Coordinator: Unknown request_type: {request_type}"
                print(error_msg)
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "FAILED",
                    "message": error_msg
                })

        except ValueError as ve:
            error_msg = f"Coordinator: Invalid request payload: {ve}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "message": error_msg
            })
        except Exception as e:
            error_msg = f"Coordinator: An unexpected error occurred: {e}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "message": error_msg
            })


# Entry point for the Cloud Run service
app = CoordinatorAgent().app # Expose the Flask app instance