# agents/numerical_summarizer_agent/agent.py

import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from common.adk_base import ADKBaseAgent
from tools.bigquery_tool import BigQueryTool
from tools.gemini_tool import GeminiTool  # NEW: Import GeminiTool
from common.constants import (
    PROJECT_ID,
    BIGQUERY_DATASET_ID,
    BIGQUERY_TABLE_FINANCIAL_METRICS,
    BIGQUERY_TABLE_NUMERICAL_INSIGHTS,
    PUBSUB_TOPIC_FINANCIAL_METRICS_PROCESSED,
    PUBSUB_TOPIC_NUMERICAL_INSIGHTS_PROCESSED,
    PUBSUB_TOPIC_DASHBOARD_UPDATES
)
from common.utils import get_current_ist_timestamp


class NumericalSummarizerAgent(ADKBaseAgent):
    """
    The Numerical Summarizer Agent analyzes financial metrics to generate
    key numerical insights and stores them in BigQuery.
    """

    def __init__(self):
        super().__init__("NumericalSummarizerAgent")
        self.project_id = PROJECT_ID
        self.bigquery_dataset_id = BIGQUERY_DATASET_ID
        self.bigquery_tool = BigQueryTool()
        self.gemini_tool = GeminiTool()  # NEW: Initialize GeminiTool
        print("NumericalSummarizerAgent initialized.")

    def _generate_summary_with_gemini(self, ticker: str, data: List[Dict[str, Any]], insight_type: str) -> str:
        """
        Uses Gemini to generate a more insightful summary based on raw financial data.
        """
        if not data:
            return "No data available for summary generation."

        # Prepare a concise string of recent data points for Gemini
        # We'll take the last 7 days of data for a quick summary
        recent_data_points = data[-7:] if len(data) >= 7 else data

        # Format data for Gemini. Focus on key metrics.
        formatted_data = []
        for dp in recent_data_points:
            formatted_data.append(
                f"Date: {dp.get('date')}, Close: {dp.get('close')}, Volume: {dp.get('volume')}, "
                f"Day Change: {dp.get('day_change_percent')}%"
            )
        data_string = "\n".join(formatted_data)

        prompt = f"""
        Analyze the following recent daily financial data for {ticker} and provide a concise, insightful summary for an asset manager. Focus on trends, significant movements, or stability observed in the closing prices, volumes, and daily percentage changes.

        Financial Data for {ticker} (Last 7 days or available):
        {data_string}

        Based on this data, summarize the key observations regarding {ticker}'s recent performance ({insight_type}). Your summary should be 1-2 sentences.

        Example Output format: "The stock [Ticker] showed [brief description of trend/movement] over the past week, with [mention a key metric or observation like volume/volatility]."
        """
        print(f"NumericalSummarizerAgent: Sending prompt to Gemini for {ticker} ({insight_type})...")
        try:
            summary = self.gemini_tool.generate_text(prompt)
            # NEW: Add a check here
            if summary:
                print(f"NumericalSummarizerAgent: Gemini generated summary for {ticker}: {summary[:100]}...")
            else:
                print(f"NumericalSummarizerAgent: Gemini returned empty or None summary for {ticker}.")
                summary = "Failed to generate summary via Gemini."  # Default message if empty
            return summary
        except Exception as e:
            print(f"NumericalSummarizerAgent: Error calling Gemini for summary: {e}")
            return f"Error generating summary: {e}. Raw data: {data_string}"

    def process_message(self, message_data: dict):
        """
        Processes a trigger message to summarize financial metrics.
        Expected `message_data` format (example, could come from FinancialMetricsAgent):
        {
            "ticker": "IBM",
            "date": "2024-06-18", # Date for which new data was added
            "request_id": "unique-req-id"
        }
        """
        ticker = message_data.get("ticker")
        data_date = message_data.get("date")  # The date of the newly ingested financial data
        request_id = message_data.get("request_id")  # To link back to original request

        if not ticker or not data_date:
            print("NumericalSummarizerAgent: Missing 'ticker' or 'date' in message. Skipping.")
            return

        print(f"NumericalSummarizerAgent: Processing financial metrics for {ticker} on {data_date}.")

        end_date_obj = datetime.strptime(data_date, "%Y-%m-%d").date()
        start_date_obj = end_date_obj - timedelta(days=30)  # Look back 30 days for data fetching
        start_date_str = start_date_obj.strftime("%Y-%m-%d")
        end_date_str = data_date

        self.publish_dashboard_update({
            "request_id": request_id,
            "status": "IN_PROGRESS",
            "agent": self.agent_name,
            "message": f"Analyzing financial metrics for {ticker} from {start_date_str} to {end_date_str}."
        })

        try:
            # 1. Fetch relevant financial metrics from BigQuery
            financial_metrics = self.bigquery_tool.get_financial_metrics(
                ticker=ticker,
                start_date=start_date_str,
                end_date=end_date_str
            )

            if not financial_metrics:
                print(
                    f"NumericalSummarizerAgent: No financial metrics found for {ticker} in the last 30 days. Skipping summarization.")
                self.publish_dashboard_update({
                    "request_id": request_id,
                    "status": "SKIPPED",
                    "agent": self.agent_name,
                    "message": f"No data for {ticker} to summarize."
                })
                return

            print(f"NumericalSummarizerAgent: Fetched {len(financial_metrics)} financial data points for {ticker}.")

            # 2. Perform Numerical Summarization (now with Gemini for insight_summary)
            numerical_insights_to_insert = []

            # Generate Insight 1: Overall Price Trend Summary (using Gemini)
            # Pass all fetched data to Gemini to get a broader view
            price_trend_summary = self._generate_summary_with_gemini(ticker, financial_metrics, "Overall Price Trend")

            numerical_insights_to_insert.append({
                "ticker": ticker,
                "insight_type": "overall_price_trend",
                "generation_date": end_date_obj.isoformat(),
                "summary_text": price_trend_summary,
                "embedding_id": None,  # Placeholder for future embedding generation
                "source_metrics": [f"financial_metrics:{ticker}:{start_date_str}_to_{end_date_str}"],
                "ingestion_timestamp": get_current_ist_timestamp()
            })

            # Generate Insight 2: Volatility / Volume Action Summary (using Gemini)
            # You might add more specific calculations here later, e.g., standard deviation for volatility.
            # For now, we'll ask Gemini to infer from daily changes and volumes.
            volume_volatility_summary = self._generate_summary_with_gemini(ticker, financial_metrics,
                                                                           "Volume and Volatility")

            numerical_insights_to_insert.append({
                "ticker": ticker,
                "insight_type": "volume_volatility_analysis",
                "generation_date": end_date_obj.isoformat(),
                "summary_text": volume_volatility_summary,
                "embedding_id": None,  # Placeholder for future embedding generation
                "source_metrics": [f"financial_metrics:{ticker}:{start_date_str}_to_{end_date_str}"],
                "ingestion_timestamp": get_current_ist_timestamp()
            })

            # --- Keep the simple calculations for display, but main insights come from Gemini ---
            total_close_price = sum(row.get('close', 0.0) for row in financial_metrics if row.get('close') is not None)
            average_close_price = total_close_price / len(financial_metrics) if financial_metrics else 0
            print(f"Calculated Average closing price for {ticker} over the last 30 days: {average_close_price:.2f}")

            first_close_price = financial_metrics[0].get('close') if financial_metrics else None
            last_close_price = financial_metrics[-1].get('close') if financial_metrics else None
            price_change_percent = 0.0
            if first_close_price is not None and last_close_price is not None and first_close_price != 0:
                price_change_percent = ((last_close_price - first_close_price) / first_close_price) * 100
            print(f"Calculated Price change for {ticker} over the last 30 days: {price_change_percent:.2f}%")
            # The above simple calculations are just printed, the actual insights inserted come from Gemini now.

            # 4. Insert insights into BigQuery
            if numerical_insights_to_insert:
                self.bigquery_tool.insert_rows(BIGQUERY_TABLE_NUMERICAL_INSIGHTS, numerical_insights_to_insert)
                print(
                    f"NumericalSummarizerAgent: Successfully inserted {len(numerical_insights_to_insert)} numerical insights for {ticker}.")

            # 5. Publish completion message
            self.publish_message(PUBSUB_TOPIC_NUMERICAL_INSIGHTS_PROCESSED, {
                "ticker": ticker,
                "date": data_date,
                "insights_count": len(numerical_insights_to_insert),
                "request_id": request_id
            })
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "COMPLETED",
                "agent": self.agent_name,
                "message": f"Numerical insights generated for {ticker} on {data_date}."
            })
            print(f"NumericalSummarizerAgent: Summarization completed for {ticker}.")

        except Exception as e:
            error_msg = f"NumericalSummarizerAgent: Error processing {ticker} for {data_date}: {e}"
            print(error_msg)
            self.publish_dashboard_update({
                "request_id": request_id,
                "status": "FAILED",
                "agent": self.agent_name,
                "message": error_msg
            })


# Entry point for the Cloud Run service
app = NumericalSummarizerAgent().app

# --- Test Block for direct execution ---
if __name__ == "__main__":
    print("\n--- Running NumericalSummarizerAgent direct test ---")
    agent_instance = NumericalSummarizerAgent()

    # --- Test 1: Simulate a trigger for a known ticker (e.g., TESTDATA) ---
    test_ticker = "TESTDATA"
    today = datetime.now(timezone.utc).date()
    test_data_date = today.strftime("%Y-%m-%d")

    test_message = {
        "ticker": test_ticker,
        "date": test_data_date,
        "request_id": "num-summ-req-1"
    }

    print(f"\nSimulating financial metrics processed message for {test_ticker} on {test_data_date}...")
    agent_instance.process_message(test_message)

    # --- Test 2: Simulate a trigger for an older, existing ticker (e.g., TEST.NS from BigQueryTool example) ---
    print(f"\nSimulating financial metrics processed message for TEST.NS on 2023-01-01...")
    agent_instance.process_message({
        "ticker": "TEST.NS",
        "date": "2023-01-01",
        "request_id": "num-summ-req-old"
    })

    # --- Test 3: Simulate a trigger for a ticker with no data (to test error handling) ---
    test_no_data_ticker = "NODATAHERE"
    test_no_data_message = {
        "ticker": test_no_data_ticker,
        "date": test_data_date,
        "request_id": "num-summ-req-2"
    }
    print(
        f"\nSimulating financial metrics processed message for {test_no_data_ticker} on {test_data_date} (expecting no data found)...")
    agent_instance.process_message(test_no_data_message)

    print("\n--- NumericalSummarizerAgent direct test completed ---")