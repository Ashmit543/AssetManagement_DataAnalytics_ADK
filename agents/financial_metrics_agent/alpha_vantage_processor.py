from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from tools.alpha_vantage_tool import AlphaVantageTool  # Changed import
from common.utils import sanitize_ticker, get_current_ist_timestamp


class AlphaVantageProcessor:  # Renamed class
    """
    Processes financial data fetched from AlphaVantageTool and formats it
    for the BigQuery financial_metrics table.
    """

    def __init__(self):
        self.alpha_vantage_tool = AlphaVantageTool()  # Changed tool instance

    def fetch_and_format_financial_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the latest key financial metrics for a given ticker from Alpha Vantage
        and formats them according to the financial_metrics BigQuery schema.
        """
        sanitized_ticker = sanitize_ticker(
            ticker)  # Keeps original symbol if it's already in AV format (e.g., RELIANCE.BSE)
        print(f"AlphaVantageProcessor: Fetching key metrics for {sanitized_ticker}")

        # Use the get_key_metrics from AlphaVantageTool which consolidates data
        formatted_data = self.alpha_vantage_tool.get_key_metrics(sanitized_ticker)

        if not formatted_data or formatted_data.get("current_price") is None:
            print(f"AlphaVantageProcessor: Could not retrieve sufficient key metrics for {sanitized_ticker}.")
            return None

        # Add the ingestion timestamp right before returning
        formatted_data["ingestion_timestamp"] = get_current_ist_timestamp()

        return formatted_data


# Example Usage (for testing purposes)
if __name__ == "__main__":
    # Ensure ALPHA_VANTAGE_API_KEY is set in your .env file for local testing
    from dotenv import load_dotenv

    load_dotenv()

    processor = AlphaVantageProcessor()

    # Test with a known ticker that Alpha Vantage supports for Indian market (e.g., BSE symbol)
    sample_ticker = "RELIANCE.BSE"
    print(f"Processing financial metrics for {sample_ticker} using Alpha Vantage...")
    formatted_data = processor.fetch_and_format_financial_metrics(sample_ticker)

    if formatted_data:
        print("\nFormatted Data for BigQuery:")
        for key, value in formatted_data.items():
            if value is not None:  # Only print non-None values for brevity
                print(f"  {key}: {value}")
    else:
        print(f"Failed to fetch or format data for {sample_ticker}.")

    # Test with a ticker that might have limited/no data
    sample_ticker_no_data = "NONEXISTENT.BSE"
    print(f"\nProcessing financial metrics for {sample_ticker_no_data} (expecting failure/partial)...")
    formatted_data_no_data = processor.fetch_and_format_financial_metrics(sample_ticker_no_data)
    if not formatted_data_no_data:
        print(f"Correctly failed to fetch data for {sample_ticker_no_data}.")