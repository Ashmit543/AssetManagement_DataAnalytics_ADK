from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from tools.yfinance_tool import YFinanceTool
from common.utils import sanitize_ticker, get_current_ist_timestamp


class YFinanceProcessor:
    """
    Processes financial data fetched from YFinanceTool and formats it
    for the BigQuery financial_metrics table.
    """

    def __init__(self):
        self.yfinance_tool = YFinanceTool()

    def fetch_and_format_financial_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the latest key financial metrics for a given ticker and
        formats them according to the financial_metrics BigQuery schema.
        """
        sanitized_ticker = sanitize_ticker(ticker)
        print(f"YFinanceProcessor: Fetching key metrics for {sanitized_ticker}")

        # Get key metrics (includes valuation, basic info)
        metrics = self.yfinance_tool.get_key_metrics(sanitized_ticker)

        if not metrics or metrics.get("current_price") is None:
            print(f"YFinanceProcessor: Could not retrieve sufficient key metrics for {sanitized_ticker}.")
            return None

        # Fetch historical data to calculate certain metrics if not directly available
        # For simplicity, let's fetch 1 year of daily data to potentially calculate MAs, RSI later
        hist_data = self.yfinance_tool.get_historical_data(sanitized_ticker, period="1y", interval="1d")

        if hist_data is not None and not hist_data.empty:
            # Example: Calculate 50-day and 200-day Moving Averages
            if len(hist_data) >= 200:
                hist_data['MA_50'] = hist_data['close'].rolling(window=50).mean()
                hist_data['MA_200'] = hist_data['close'].rolling(window=200).mean()
                latest_ma_50 = hist_data['MA_50'].iloc[-1]
                latest_ma_200 = hist_data['MA_200'].iloc[-1]
                metrics["moving_average_50"] = latest_ma_50
                metrics["moving_average_200"] = latest_ma_200
            elif len(hist_data) >= 50:
                hist_data['MA_50'] = hist_data['close'].rolling(window=50).mean()
                latest_ma_50 = hist_data['MA_50'].iloc[-1]
                metrics["moving_average_50"] = latest_ma_50

            # RSI calculation requires more complex logic, often external libraries like `ta-lib` or `pandas_ta`.
            # For MVP, we might leave it as None or add a simpler placeholder.
            # metrics["rsi"] = calculate_rsi(hist_data['close']) # Placeholder

        # Ensure all fields from schema are present, even if None
        # This helps in consistent BigQuery insertions
        full_metrics = {
            "ticker": sanitized_ticker,
            "date": metrics.get("date", datetime.now().date().isoformat()),
            "open": metrics.get("open"),
            "high": metrics.get("high"),
            "low": metrics.get("low"),
            "close": metrics.get("close"),
            "volume": metrics.get("volume"),
            "market_cap": metrics.get("market_cap"),
            "pe_ratio": metrics.get("pe_ratio"),
            "eps": metrics.get("eps"),
            "revenue": metrics.get("revenue"),
            "net_income": metrics.get("net_income"),
            "debt_to_equity": metrics.get("debt_to_equity"),
            "roe": metrics.get("roe"),
            "current_price": metrics.get("current_price"),
            "day_change_percent": metrics.get("day_change_percent"),
            "fifty_two_week_high": metrics.get("fifty_two_week_high"),
            "fifty_two_week_low": metrics.get("fifty_two_week_low"),
            "moving_average_50": metrics.get("moving_average_50"),
            "moving_average_200": metrics.get("moving_average_200"),
            "rsi": metrics.get("rsi"),
            "beta": metrics.get("beta"),
            "cagr": metrics.get("cagr"),
            "geographical_exposure": metrics.get("geographical_exposure"),
            "risk_signals": metrics.get("risk_signals"),
            "sector_performance_index": metrics.get("sector_performance_index"),
            "ingestion_timestamp": get_current_ist_timestamp()  # Use the utility for consistency
        }
        return full_metrics


# Example Usage (for testing purposes)
if __name__ == "__main__":
    processor = YFinanceProcessor()

    # Test with a known ticker
    sample_ticker = "INFY.NS"
    print(f"Processing financial metrics for {sample_ticker}...")
    formatted_data = processor.fetch_and_format_financial_metrics(sample_ticker)

    if formatted_data:
        print("\nFormatted Data for BigQuery:")
        for key, value in formatted_data.items():
            print(f"  {key}: {value}")
    else:
        print(f"Failed to fetch or format data for {sample_ticker}.")

    # Test with a ticker that might have limited data
    sample_ticker_no_data = "XYZ.NS"  # Non-existent or very illiquid
    print(f"\nProcessing financial metrics for {sample_ticker_no_data} (expecting failure/partial)...")
    formatted_data_no_data = processor.fetch_and_format_financial_metrics(sample_ticker_no_data)
    if not formatted_data_no_data:
        print(f"Correctly failed to fetch data for {sample_ticker_no_data}.")