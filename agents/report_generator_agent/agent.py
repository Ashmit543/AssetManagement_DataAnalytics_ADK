# agents/report_generator_agent/agent.py

import json
import uuid
from datetime import datetime, timezone
from google.cloud import storage # For GCS operations

from common.adk_base import ADKBaseAgent
from tools.bigquery_tool import BigQueryTool
from tools.gemini_tool import GeminiTool
from common.constants import (
    PROJECT_ID, # ADDED THIS IMPORT
    GEMINI_MODEL_NAME,
    BIGQUERY_TABLE_FINANCIAL_METRICS,
    BIGQUERY_TABLE_NUMERICAL_INSIGHTS,
    BIGQUERY_TABLE_REPORT_METADATA,
    PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED,
    PUBSUB_TOPIC_DASHBOARD_UPDATES,
    GCS_REPORTS_BUCKET,
    BIGQUERY_DATASET_ID, # ADDED THIS IMPORT
)
from agents.report_generator_agent.report_templates import get_report_template
from common.utils import get_current_ist_timestamp

class ReportGeneratorAgent(ADKBaseAgent):
    """
    The Report Generator Agent fetches relevant data, uses Gemini to generate
    investment reports based on templates, stores them in GCS, and logs metadata in BigQuery.
    """
    def __init__(self):
        super().__init__("ReportGeneratorAgent")
        # Ensure project_id and bigquery_dataset_id are explicitly set or inherited properly
        self.project_id = PROJECT_ID # ADDED: Ensure project_id is available
        self.bigquery_dataset_id = BIGQUERY_DATASET_ID # ADDED: Ensure dataset_id is available

        self.bigquery_tool = BigQueryTool()
        self.gemini_tool = GeminiTool()
        self.gcs_client = storage.Client(project=PROJECT_ID) # Initialize GCS client using constant
        self.reports_bucket = self.gcs_client.bucket(GCS_REPORTS_BUCKET)
        print("ReportGeneratorAgent initialized.")

    def process_message(self, message_data: dict):
        """
        Processes a request to generate a report.
        Expected `message_data` format:
        {
            "report_type": "Executive Summary",
            "company_ticker": "IBM", # Optional
            "request_id": "unique-id-123"
            "parameters": {"start_date": "2024-01-01", "end_date": "2024-06-30"} # Optional, for custom reports
        }
        """
        report_type = message_data.get("report_type")
        company_ticker = message_data.get("company_ticker")
        request_id = message_data.get("request_id")
        parameters = message_data.get("parameters", {})

        report_id = str(uuid.uuid4()) # Generate a unique ID for this report
        generation_timestamp = datetime.now(timezone.utc).isoformat()

        print(f"ReportGeneratorAgent: Starting generation of '{report_type}' report (ID: {report_id}) for {company_ticker or 'N/A'}.")

        # 1. Log initial report metadata as IN_PROGRESS
        report_metadata = {
            "report_id": report_id,
            "report_type": report_type,
            "company_ticker": company_ticker,
            "generation_timestamp": generation_timestamp,
            "gcs_uri": "", # FIXED: Changed None to empty string as gcs_uri is REQUIRED
            "llm_model_used": GEMINI_MODEL_NAME,
            "parameters_used": json.dumps(parameters),
            "status": "IN_PROGRESS",
            "embedding_id": None,
            "ingestion_timestamp": get_current_ist_timestamp()
        }
        # Insert the initial status. If the process fails later, we'll insert another row with FAILED status.
        # BigQuery does not support direct UPDATE based on row content for stream inserts in this context.
        self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata])
        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "IN_PROGRESS",
            "agent": self.agent_name,
            "message": f"Generating '{report_type}' report for {company_ticker or 'N/A'} (ID: {report_id})."
        })

        try:
            # 2. Fetch data from BigQuery (example: latest financial metrics)
            # In a real scenario, you'd fetch more complex data based on report_type
            financial_data_rows = None
            if company_ticker:
                # Construct the query string directly, as query_data doesn't support query_params
                # Make sure to properly escape the ticker if it could contain problematic characters,
                # but for simple tickers like 'IBM', direct insertion is fine.
                query = f"""
                    SELECT * FROM `{self.project_id}.{self.bigquery_dataset_id}.{BIGQUERY_TABLE_FINANCIAL_METRICS}`
                    WHERE ticker = '{company_ticker}'
                    ORDER BY date DESC, ingestion_timestamp DESC
                    LIMIT 1
                """
                # FIXED: Changed query_table to query_data and pass only the query string
                financial_data_rows = self.bigquery_tool.query_data(query)

            # TODO: Fetch numerical insights here too once NumericalSummarizerAgent is ready

            # 3. Prepare data for the LLM
            # For this MVP, we'll just format basic financial data
            formatted_data_for_llm = ""
            if financial_data_rows and financial_data_rows[0]:
                latest_data = financial_data_rows[0]
                formatted_data_for_llm += f"Latest Financial Metrics for {company_ticker} ({latest_data.get('date')}):\n"
                formatted_data_for_llm += f"- Current Price: {latest_data.get('current_price')}\n"
                formatted_data_for_llm += f"- Day Change: {latest_data.get('day_change_percent'):.2f}%\n"
                formatted_data_for_llm += f"- Market Cap: {latest_data.get('market_cap'):,.0f}\n"
                formatted_data_for_llm += f"- PE Ratio: {latest_data.get('pe_ratio')}\n"
                # Add more relevant fields as needed
            else:
                formatted_data_for_llm = f"No recent financial data found for {company_ticker or 'the requested entity'}."
                if not company_ticker:
                    formatted_data_for_llm = "No specific company data requested."

            # 4. Get the appropriate report template
            prompt_template = get_report_template(report_type)
            if not prompt_template:
                raise ValueError(f"No template found for report type: {report_type}")

            # 5. Generate report content using Gemini
            full_prompt = prompt_template.format(
                ticker=company_ticker or "the entity", # Pass ticker for use in template
                financial_data=formatted_data_for_llm,
                # Add other data like numerical_insights, sentiment_analysis here later
                user_parameters=json.dumps(parameters) # Pass any additional user parameters
            )

            print(f"ReportGeneratorAgent: Sending prompt to Gemini for {report_id}...")
            generated_report_content = self.gemini_tool.generate_text(full_prompt, temperature=0.7, max_tokens=2048)

            if not generated_report_content:
                raise Exception("Gemini failed to generate report content.")

            print(f"ReportGeneratorAgent: Report content generated for {report_id}.")

            # 6. Save report to GCS
            gcs_object_name = f"reports/{report_id}.txt" # You can change format (e.g., .pdf if rendering)
            blob = self.reports_bucket.blob(gcs_object_name)
            blob.upload_from_string(generated_report_content, content_type="text/plain")
            gcs_uri = f"gs://{GCS_REPORTS_BUCKET}/{gcs_object_name}"
            print(f"ReportGeneratorAgent: Report saved to GCS: {gcs_uri}")

            # 7. Update report metadata in BigQuery (status, gcs_uri)
            # Fetch the existing row or create a new one to insert with updated status
            # For simplicity, we are inserting a new row with the COMPLETED status and the gcs_uri.
            # In a full production system, you might implement a more robust update/upsert mechanism.
            report_metadata["gcs_uri"] = gcs_uri
            report_metadata["status"] = "COMPLETED"
            # We don't need to update embedding_id here unless we're generating embeddings for the report summary
            self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata])
            print(f"ReportGeneratorAgent: Report metadata updated for {report_id}.")

            # 8. Publish completion message
            self.publish_message(PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED, {
                "report_id": report_id,
                "report_type": report_type,
                "company_ticker": company_ticker,
                "gcs_uri": gcs_uri,
                "request_id": request_id
            })
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "COMPLETED",
                "agent": self.agent_name,
                "message": f"'{report_type}' report for {company_ticker or 'N/A'} generated and available at {gcs_uri}.",
                "report_id": report_id,
                "gcs_uri": gcs_uri
            })
            print(f"ReportGeneratorAgent: Report generation completed for {report_id}.")

        except Exception as e:
            error_msg = f"ReportGeneratorAgent: Error generating '{report_type}' report for {company_ticker or 'N/A'} (ID: {report_id}): {e}"
            print(error_msg)
            # Log failure in BigQuery and publish to dashboard
            report_metadata["status"] = "FAILED"
            # report_metadata["error_message"] = str(e) # REMOVED: Schema does not have this field
            self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata]) # Insert failed status
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })

# Entry point for the Cloud Run service (assuming this agent will be deployed via Cloud Run)
# This line creates the Flask app instance for ADK to deploy.
app = ReportGeneratorAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running ReportGeneratorAgent direct test ---")
    # For local testing, ensure your GCP credentials are set up
    # (e.g., gcloud auth application-default login)

    agent_instance = ReportGeneratorAgent()

    # --- Test 1: Generate an Executive Summary for a known ticker (e.g., RELIANCE.BSE or IBM) ---
    # Use a ticker that successfully inserted financial data into BigQuery from previous tests.
    # IMPORTANT: If 'IBM' is not present in your financial_metrics BigQuery table, this will
    # report "No recent financial data found for IBM." but should still generate a report based
    # on the template.
    test_ticker = "IBM" # Or "RELIANCE.NS" if you fix Alpha Vantage data fetching for it later
    test_message_executive = {
        "report_type": "Executive Summary",
        "company_ticker": test_ticker,
        "request_id": "rpt-req-exec-1"
    }
    print(f"\nSimulating request for '{test_message_executive['report_type']}' report for {test_ticker}...")
    agent_instance.process_message(test_message_executive)

    # --- Test 2: Generate a generic market overview report (no specific ticker) ---
    test_message_market = {
        "report_type": "Market Overview", # This will need a corresponding template
        "request_id": "rpt-req-market-2",
        "parameters": {"market_focus": "Indian Equities", "time_frame": "Q2 2025"}
    }
    print(f"\nSimulating request for '{test_message_market['report_type']}' report...")
    agent_instance.process_message(test_message_market)

    print("\n--- ReportGeneratorAgent direct test completed ---")