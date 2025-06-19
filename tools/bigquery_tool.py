from google.cloud import bigquery
from typing import List, Dict, Any, Optional
from common.gcp_clients import get_bigquery_client
from common.constants import PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_FINANCIAL_METRICS, BIGQUERY_TABLE_NUMERICAL_INSIGHTS, BIGQUERY_TABLE_SENTIMENT_SCORES, BIGQUERY_TABLE_REPORT_METADATA
from datetime import datetime, timezone

class BigQueryTool:
    """
    A wrapper for BigQuery operations (insert, query).
    """

    def __init__(self):
        self.client = get_bigquery_client()
        self.dataset_ref = self.client.dataset(BIGQUERY_DATASET_ID, project=PROJECT_ID)

    def insert_rows(self, table_id: str, rows: List[Dict[str, Any]]) -> bool:
        """
        Inserts a list of rows into a specified BigQuery table.
        Args:
            table_id: The ID of the table (e.g., 'financial_metrics').
            rows: A list of dictionaries, where each dictionary represents a row.
        Returns:
            True if insertion is successful, False otherwise.
        """
        table_ref = self.dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)

        errors = self.client.insert_rows(table, rows)  # API request
        if errors:
            print(f"Errors occurred while inserting rows into {table_id}:")
            for error in errors:
                print(error)
            return False
        else:
            print(f"Successfully inserted {len(rows)} rows into {table_id}.")
            return True

    def query_data(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Executes a SQL query against BigQuery and returns the results as a list of dictionaries.
        Args:
            query: The SQL query string.
        Returns:
            A list of dictionaries representing the query results, or None if an error occurs.
        """
        try:
            query_job = self.client.query(query)  # API request
            results = query_job.result()  # Waits for job to complete

            rows = []
            for row in results:
                rows.append(dict(row))  # Convert Row object to dictionary
            return rows
        except Exception as e:
            print(f"Error executing BigQuery query: {e}")
            return None

    def get_financial_metrics(self, ticker: str, start_date: str, end_date: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches financial metrics for a given ticker within a date range.
        """
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_FINANCIAL_METRICS}`
            WHERE ticker = '{ticker}' AND date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date ASC
        """
        return self.query_data(query)

    def get_numerical_insights(self, ticker: Optional[str] = None, insight_type: Optional[str] = None,
                               limit: int = 1) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches numerical insights based on ticker and/or insight type.
        """
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_NUMERICAL_INSIGHTS}`
            WHERE 1=1
        """
        if ticker:
            query += f" AND ticker = '{ticker}'"
        if insight_type:
            query += f" AND insight_type = '{insight_type}'"
        query += f" ORDER BY ingestion_timestamp DESC LIMIT {limit}"
        return self.query_data(query)

    def get_sentiment_scores(self, company_ticker: str, start_date: str, end_date: str) -> Optional[
        List[Dict[str, Any]]]:
        """
        Fetches sentiment scores for a given company within a date range.
        """
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_SENTIMENT_SCORES}`
            WHERE company_ticker = '{company_ticker}' AND date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date DESC
        """
        return self.query_data(query)


# Example Usage (for testing purposes)
if __name__ == "__main__":
    bq_tool = BigQueryTool()

    # Example 1: Insert a row (you'd normally get data from an agent)
    print("\n--- Example: Inserting a dummy financial metric ---")
    dummy_financial_row = [{
        "ticker": "TEST.NS",
        "date": "2023-01-01",
        "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 100000,
        "market_cap": 10000000, "pe_ratio": 20.0, "eps": 5.0, "revenue": 500000,
        "net_income": 100000, "debt_to_equity": 0.5, "roe": 0.15, "current_price": 103.0,
        "day_change_percent": 3.0, "fifty_two_week_high": 120.0, "fifty_two_week_low": 80.0,
        "moving_average_50": 101.0, "moving_average_200": 95.0, "rsi": 60.0, "beta": 1.2,
        "cagr": 0.1, "geographical_exposure": "India", "risk_signals": "Low liquidity",
        "sector_performance_index": 1.0,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
    }]
    # import datetime and timezone for the timestamp if running locally
    from datetime import datetime, timezone

    bq_tool.insert_rows(BIGQUERY_TABLE_FINANCIAL_METRICS, dummy_financial_row)

    # Example 2: Query data
    print("\n--- Example: Querying recent financial metrics for TEST.NS ---")
    queried_data = bq_tool.get_financial_metrics("TEST.NS", "2023-01-01", "2023-01-01")
    if queried_data:
        for row in queried_data:
            print(row)

    # Example 3: Query numerical insights
    print("\n--- Example: Querying latest numerical insights for TEST.NS ---")
    queried_insights = bq_tool.get_numerical_insights(ticker="TEST.NS", limit=1)
    if queried_insights:
        for row in queried_insights:
            print(row)