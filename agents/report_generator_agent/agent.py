# agents/report_generator_agent/agent.py

import json
import uuid
from datetime import datetime, timezone, timedelta
from google.cloud import storage  # For GCS operations

from common.adk_base import ADKBaseAgent
from tools.bigquery_tool import BigQueryTool
from tools.gemini_tool import GeminiTool
from common.constants import (
    PROJECT_ID,
    GEMINI_MODEL_NAME,
    BIGQUERY_TABLE_FINANCIAL_METRICS,
    BIGQUERY_TABLE_NUMERICAL_INSIGHTS,  # NEW: Import numerical insights table constant
    BIGQUERY_TABLE_REPORT_METADATA,
    PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED,
    PUBSUB_TOPIC_DASHBOARD_UPDATES,
    GCS_REPORTS_BUCKET,
    BIGQUERY_DATASET_ID,
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
        self.project_id = PROJECT_ID
        self.bigquery_dataset_id = BIGQUERY_DATASET_ID

        self.bigquery_tool = BigQueryTool()
        self.gemini_tool = GeminiTool()
        self.gcs_client = storage.Client(project=PROJECT_ID)
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
            "report_date": "2025-06-19" # NEW: Date for which report should be generated (defaults to today)
        }
        """
        report_type = message_data.get("report_type")
        company_ticker = message_data.get("company_ticker")
        request_id = message_data.get("request_id")
        parameters = message_data.get("parameters", {})

        # NEW: Determine the report generation date
        report_date_str = message_data.get("report_date")
        if not report_date_str:
            report_date_obj = datetime.now(timezone.utc).date()
            report_date_str = report_date_obj.strftime("%Y-%m-%d")
        else:
            report_date_obj = datetime.strptime(report_date_str, "%Y-%m-%d").date()

        report_id = str(uuid.uuid4())
        generation_timestamp = get_current_ist_timestamp()  # Use IST timestamp

        print(
            f"ReportGeneratorAgent: Starting generation of '{report_type}' report (ID: {report_id}) for {company_ticker or 'N/A'} on {report_date_str}.")

        report_metadata = {
            "report_id": report_id,
            "report_type": report_type,
            "company_ticker": company_ticker,
            "generation_timestamp": generation_timestamp,
            "gcs_uri": "",
            "llm_model_used": GEMINI_MODEL_NAME,
            "parameters_used": json.dumps(parameters),
            "status": "IN_PROGRESS",
            "embedding_id": None,
            "ingestion_timestamp": get_current_ist_timestamp()
        }
        self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata])
        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "IN_PROGRESS",
            "agent": self.agent_name,
            "message": f"Generating '{report_type}' report for {company_ticker or 'N/A'} (ID: {report_id})."
        })

        try:
            # 2. Fetch data from BigQuery
            financial_data_for_llm = ""
            numerical_insights_for_llm = ""

            if company_ticker:
                # Fetch latest financial metrics (as before)
                financial_query = f"""
                    SELECT * FROM `{self.project_id}.{self.bigquery_dataset_id}.{BIGQUERY_TABLE_FINANCIAL_METRICS}`
                    WHERE ticker = '{company_ticker}'
                    ORDER BY date DESC, ingestion_timestamp DESC
                    LIMIT 1
                """
                financial_data_rows = self.bigquery_tool.query_data(financial_query)

                if financial_data_rows and financial_data_rows[0]:
                    latest_data = financial_data_rows[0]
                    financial_data_for_llm += f"Latest Financial Metrics for {company_ticker} ({latest_data.get('date')}):\n"
                    financial_data_for_llm += f"- Current Price: {latest_data.get('current_price')}\n"
                    financial_data_for_llm += f"- Day Change: {latest_data.get('day_change_percent'):.2f}%\n"
                    financial_data_for_llm += f"- Market Cap: {latest_data.get('market_cap'):,.0f}\n"
                    financial_data_for_llm += f"- PE Ratio: {latest_data.get('pe_ratio')}\n"
                else:
                    financial_data_for_llm = f"No recent financial data found for {company_ticker}."

                # NEW: Fetch numerical insights
                # Fetch insights generated on or around the report_date, looking back a few days
                insights_start_date_obj = report_date_obj - timedelta(days=7)  # Look back 7 days for insights
                insights_start_date_str = insights_start_date_obj.strftime("%Y-%m-%d")

                numerical_insights_query = f"""
                    SELECT insight_type, summary_text, generation_date
                    FROM `{self.project_id}.{self.bigquery_dataset_id}.{BIGQUERY_TABLE_NUMERICAL_INSIGHTS}`
                    WHERE ticker = '{company_ticker}'
                    AND generation_date BETWEEN '{insights_start_date_str}' AND '{report_date_str}'
                    ORDER BY generation_date DESC, ingestion_timestamp DESC
                    LIMIT 5 -- Fetch up to 5 latest insights
                """
                numerical_insights_rows = self.bigquery_tool.query_data(numerical_insights_query)

                if numerical_insights_rows:
                    numerical_insights_for_llm += f"\nRecent Numerical Insights for {company_ticker}:\n"
                    for insight in numerical_insights_rows:
                        numerical_insights_for_llm += (
                            f"- Type: {insight.get('insight_type')}, Date: {insight.get('generation_date')}\n"
                            f"  Summary: {insight.get('summary_text')}\n"
                        )
                else:
                    numerical_insights_for_llm = f"\nNo recent numerical insights found for {company_ticker}."

            else:  # No company_ticker specified for report
                financial_data_for_llm = "No specific company financial data requested for this report."
                numerical_insights_for_llm = "No specific company numerical insights requested for this report."

            # 3. Get the appropriate report template
            prompt_template = get_report_template(report_type)
            if not prompt_template:
                raise ValueError(f"No template found for report type: {report_type}")

            # 4. Generate report content using Gemini
            full_prompt = prompt_template.format(
                ticker=company_ticker or "the entity",
                report_date=report_date_str,  # NEW: Pass report date to template
                financial_data=financial_data_for_llm,
                numerical_insights=numerical_insights_for_llm,  # NEW: Pass numerical insights
                user_parameters=json.dumps(parameters)
            )

            print(f"ReportGeneratorAgent: Sending prompt to Gemini for {report_id}...")
            generated_report_content = self.gemini_tool.generate_text(full_prompt, temperature=0.7, max_tokens=2048)

            if not generated_report_content:
                raise Exception("Gemini failed to generate report content.")

            print(f"ReportGeneratorAgent: Report content generated for {report_id}.")

            # 5. Save report to GCS
            gcs_object_name = f"reports/{report_id}.txt"
            blob = self.reports_bucket.blob(gcs_object_name)
            blob.upload_from_string(generated_report_content, content_type="text/plain")
            gcs_uri = f"gs://{GCS_REPORTS_BUCKET}/{gcs_object_name}"
            print(f"ReportGeneratorAgent: Report saved to GCS: {gcs_uri}")

            # 6. Update report metadata in BigQuery
            report_metadata["gcs_uri"] = gcs_uri
            report_metadata["status"] = "COMPLETED"
            self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata])
            print(f"ReportGeneratorAgent: Report metadata updated for {report_id}.")

            # 7. Publish completion message
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
            report_metadata["status"] = "FAILED"
            self.bigquery_tool.insert_rows(BIGQUERY_TABLE_REPORT_METADATA, [report_metadata])
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })


# Entry point for the Cloud Run service
app = ReportGeneratorAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running ReportGeneratorAgent direct test ---")

    agent_instance = ReportGeneratorAgent()

    # --- Ensure `report_templates.py` exists in the same directory ---
    # Create a dummy report_templates.py if you haven't already, e.g.:
    # # agents/report_generator_agent/report_templates.py
    # def get_report_template(report_type: str) -> str:
    #     if report_type == "Executive Summary":
    #         return """
    #         Generate an Executive Summary for {ticker} for the report date {report_date}.
    #         Include the following latest financial metrics:
    #         {financial_data}
    #         Also, consider these recent numerical insights:
    #         {numerical_insights}
    #         Highlight key performance and trends based on the provided data.
    #         """
    #     elif report_type == "Market Overview":
    #         return """
    #         Generate a Market Overview report for {report_date} with focus on {user_parameters}.
    #         """
    #     return None

    # Use today's date for current test, or a date for which you have recent TESTDATA and TEST.NS insights
    test_report_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")

    # --- Test 1: Generate an Executive Summary for TESTDATA ---
    # This assumes 'TESTDATA' has recent financial metrics AND numerical insights in BQ.
    test_message_executive = {
        "report_type": "Executive Summary",
        "company_ticker": "TESTDATA",
        "request_id": "rpt-req-exec-1",
        "report_date": test_report_date  # Pass the date explicitly
    }
    print(
        f"\nSimulating request for '{test_message_executive['report_type']}' report for {test_message_executive['company_ticker']} on {test_report_date}...")
    agent_instance.process_message(test_message_executive)

    # --- Test 2: Generate an Executive Summary for TEST.NS ---
    # This assumes 'TEST.NS' also has recent data. Note: TEST.NS only had 1 day of data in previous tests,
    # so its summary from Gemini might still be very basic.
    test_message_executive_ns = {
        "report_type": "Executive Summary",
        "company_ticker": "TEST.NS",
        "request_id": "rpt-req-exec-ns-1",
        "report_date": test_report_date  # Pass the date explicitly
    }
    print(
        f"\nSimulating request for '{test_message_executive_ns['report_type']}' report for {test_message_executive_ns['company_ticker']} on {test_report_date}...")
    agent_instance.process_message(test_message_executive_ns)

    # --- Test 3: Generate a generic market overview report (no specific ticker) ---
    test_message_market = {
        "report_type": "Market Overview",
        "request_id": "rpt-req-market-2",
        "parameters": {"market_focus": "Indian Equities", "time_frame": "Q2 2025"},
        "report_date": test_report_date
    }
    print(f"\nSimulating request for '{test_message_market['report_type']}' report...")
    agent_instance.process_message(test_message_market)

    # --- Test 4: Generate a report for a non-existent ticker ---
    test_message_no_data = {
        "report_type": "Executive Summary",
        "company_ticker": "NONEXISTENT",
        "request_id": "rpt-req-no-data",
        "report_date": test_report_date
    }
    print(
        f"\nSimulating request for '{test_message_no_data['report_type']}' report for {test_message_no_data['company_ticker']} (expecting no data)...")
    agent_instance.process_message(test_message_no_data)

    print("\n--- ReportGeneratorAgent direct test completed ---")