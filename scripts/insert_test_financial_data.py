# scripts/insert_test_financial_data.py

import sys
import os
from datetime import datetime, timedelta, timezone
import json  # For pretty printing
from dotenv import load_dotenv

# Add the project root to the sys.path to allow imports from common, tools, etc.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tools.bigquery_tool import BigQueryTool
from common.constants import BIGQUERY_TABLE_FINANCIAL_METRICS
from common.utils import get_current_ist_timestamp  # Assuming this is available


def insert_dummy_financial_data(ticker: str, num_days: int = 35):
    """
    Inserts dummy financial data for a given ticker into the financial_metrics table.
    Data is generated for the last `num_days`.
    """
    bq_tool = BigQueryTool()
    rows_to_insert = []

    current_date = datetime.now(timezone.utc).date()
    base_price = 100.0

    print(f"Generating {num_days} days of dummy data for ticker: {ticker}...")

    for i in range(num_days, 0, -1):  # From 35 days ago up to yesterday
        date_obj = current_date - timedelta(days=i)

        # Simulate some price fluctuation
        open_price = base_price + (i % 5) - 2.5
        close_price = open_price + (i % 3) - 1.5
        high_price = max(open_price, close_price) + 1.0
        low_price = min(open_price, close_price) - 1.0
        volume = 1000000 + (i * 10000)

        # Basic dummy metrics
        market_cap = 10_000_000_000 + (i * 100_000_000)
        pe_ratio = 20.0 + (i % 10) / 10.0
        eps = 5.0 + (i % 4) / 4.0
        revenue = 500_000_000 + (i * 5_000_000)
        net_income = 50_000_000 + (i * 500_000)
        debt_to_equity = 0.5 + (i % 2) / 10.0
        roe = 0.15 + (i % 3) / 100.0
        beta = 1.0 + (i % 4) / 10.0
        cagr = 0.12 + (i % 5) / 100.0

        # Current price and day change will be calculated by financial_metrics_agent from this raw data
        # For this dummy, we just set current_price to close and simulate day_change_percent
        day_change_percent = ((close_price - open_price) / open_price) * 100 if open_price else 0.0

        # These would ideally be calculated from a longer history or by an agent,
        # but for dummy data, we'll make them approximate.
        fifty_two_week_high = high_price + 5
        fifty_two_week_low = low_price - 5
        moving_average_50 = close_price - 2
        moving_average_200 = close_price - 5
        rsi = 50 + (i % 20) - 10  # Simulate some RSI value

        row = {
            "ticker": ticker,
            "date": date_obj.isoformat(),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": int(volume),
            "market_cap": int(market_cap),
            "pe_ratio": round(pe_ratio, 2),
            "eps": round(eps, 2),
            "revenue": int(revenue),
            "net_income": int(net_income),
            "debt_to_equity": round(debt_to_equity, 2),
            "roe": round(roe, 2),
            "current_price": round(close_price, 2),
            "day_change_percent": round(day_change_percent, 2),
            "fifty_two_week_high": round(fifty_two_week_high, 2),
            "fifty_two_week_low": round(fifty_two_week_low, 2),
            "moving_average_50": round(moving_average_50, 2),
            "moving_average_200": round(moving_average_200, 2),
            "rsi": round(rsi, 2),
            "beta": round(beta, 2),
            "cagr": round(cagr, 2),
            "geographical_exposure": "Global",
            "risk_signals": "Moderate volatility",
            "sector_performance_index": 1.0,
            "ingestion_timestamp": get_current_ist_timestamp()
        }
        rows_to_insert.append(row)

    # Insert in batches for efficiency (BigQuery insert_rows has a limit per request)
    # For a few rows, one batch is fine. For many, loop and send smaller batches.
    print(f"Inserting {len(rows_to_insert)} rows into {BIGQUERY_TABLE_FINANCIAL_METRICS}...")
    success = bq_tool.insert_rows(BIGQUERY_TABLE_FINANCIAL_METRICS, rows_to_insert)

    if success:
        print("Dummy financial data insertion completed successfully.")
        # Print the latest inserted data point for verification
        print("\nLatest inserted data point (for verification):")
        print(json.dumps(rows_to_insert[-1], indent=2))
    else:
        print("Failed to insert dummy financial data.")


if __name__ == "__main__":
    load_dotenv()  # Load .env for local testing if API key is there
    test_ticker = "TESTDATA"  # Using a unique ticker for test data
    insert_dummy_financial_data(test_ticker)