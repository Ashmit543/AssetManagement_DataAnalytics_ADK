import os
import requests
import json
from typing import Dict, Any, Optional, List
from common.gcp_clients import get_secret_manager_client
from common.constants import PROJECT_ID, SECRET_ALPHA_VANTAGE_API_KEY

class AlphaVantageTool:
    """
    A wrapper for the Alpha Vantage API to fetch various financial and news data.
    API Key is loaded from Google Secret Manager.
    """
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self):
        self._api_key = None
        self._load_api_key()

    def _load_api_key(self):
        """
        Loads the Alpha Vantage API key from Google Secret Manager.
        For local development, fall back to environment variable for convenience.
        """
        if os.getenv("ALPHA_VANTAGE_API_KEY"): # For local testing via .env
            self._api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            print("Alpha Vantage API key loaded from environment variable.")
            return

        try:
            client = get_secret_manager_client()
            name = f"projects/{PROJECT_ID}/secrets/{SECRET_ALPHA_VANTAGE_API_KEY}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            self._api_key = response.payload.data.decode("UTF-8")
            print("Alpha Vantage API key loaded from Secret Manager.")
        except Exception as e:
            print(f"Error loading Alpha Vantage API key from Secret Manager: {e}")
            print("Please ensure the secret exists and the service account has 'Secret Manager Secret Accessor' role.")
            self._api_key = None # Ensure it's None if loading fails

    def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Helper method to make a request to the Alpha Vantage API.
        """
        if not self._api_key:
            print("Alpha Vantage API key is not available. Cannot make request.")
            return None

        params["apikey"] = self._api_key
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status() # Raise an exception for HTTP errors
            data = response.json()
            if "Error Message" in data:
                print(f"Alpha Vantage API error: {data['Error Message']}")
                return None
            if "Note" in data:
                print(f"Alpha Vantage API note: {data['Note']}") # Rate limit message
            return data
        except requests.exceptions.RequestException as e:
            print(f"Network error or invalid request to Alpha Vantage API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response from Alpha Vantage: {e}. Response text: {response.text}")
            return None

    def get_company_overview(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches company overview and fundamental data."""
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_income_statement(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches income statement data."""
        params = {"function": "INCOME_STATEMENT", "symbol": symbol}
        return self._make_request(params)

    def get_balance_sheet(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches balance sheet data."""
        params = {"function": "BALANCE_SHEET", "symbol": symbol}
        return self._make_request(params)

    def get_cash_flow(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches cash flow data."""
        params = {"function": "CASH_FLOW", "symbol": symbol}
        return self._make_request(params)

    def get_earnings(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches quarterly and annual earnings data."""
        params = {"function": "EARNINGS", "symbol": symbol}
        return self._make_request(params)

    def get_news_sentiment(self, tickers: Optional[List[str]] = None, topics: Optional[List[str]] = None,
                          sort_by: str = "relevance", limit: int = 50) -> Optional[Dict[str, Any]]:
        """
        Fetches news and sentiment data.
        Args:
            tickers: List of stock tickers (e.g., ["IBM", "MSFT"]). Max 5.
            topics: List of topics (e.g., ["economy", "financial_markets"]). Max 5.
            sort_by: How to sort (e.g., "relevance", "earliest", "latest").
            limit: Number of articles to return (max 200).
        """
        params = {"function": "NEWS_SENTIMENT", "sort": sort_by, "limit": limit}
        if tickers:
            params["tickers"] = ",".join(tickers)
        if topics:
            params["topics"] = ",".join(topics)
        return self._make_request(params)

    def get_technical_indicator(self, function: str, symbol: str, interval: str = "daily",
                                time_period: int = 60, series_type: str = "close") -> Optional[Dict[str, Any]]:
        """
        Fetches various technical indicators (e.g., SMA, EMA, RSI, MACD).
        Args:
            function: e.g., "SMA", "EMA", "RSI", "MACD", "BBANDS".
            symbol: Stock ticker.
            interval: Data interval (e.g., "1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly").
            time_period: Number of data points used to calculate each indicator value.
            series_type: The price type (e.g., "open", "high", "low", "close").
        """
        params = {
            "function": function,
            "symbol": symbol,
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type
        }
        return self._make_request(params)

    # You can add more Alpha Vantage API wrappers as needed for other functionalities,
    # e.g., get_treasury_yield, get_cpi, get_unemployment_rate for macro data.

# Example Usage (for testing purposes, not part of the tool itself for deployment)
if __name__ == "__main__":
    # For local testing, ensure ALPHA_VANTAGE_API_KEY is set in your .env file
    # or as an environment variable before running this script directly.
    # Otherwise, it will try to fetch from Secret Manager.
    from dotenv import load_dotenv
    load_dotenv() # Load .env for local testing

    av_tool = AlphaVantageTool()
    if av_tool._api_key:
        print("\n--- Fetching Company Overview for IBM ---")
        overview = av_tool.get_company_overview("IBM")
        if overview:
            print(f"Company Name: {overview.get('Name')}")
            print(f"Description: {overview.get('Description')[:100]}...")

        print("\n--- Fetching Latest News Sentiment for AAPL, MSFT ---")
        news_sentiment = av_tool.get_news_sentiment(tickers=["AAPL", "MSFT"], limit=5)
        if news_sentiment and "feed" in news_sentiment:
            for article in news_sentiment["feed"]:
                print(f"Title: {article.get('title')}")
                print(f"Overall Sentiment: {article.get('overall_sentiment_score')}, Label: {article.get('overall_sentiment_label')}")
                print("-" * 20)
        elif news_sentiment:
            print("No news articles found or unexpected response structure.")
    else:
        print("Alpha Vantage API key not configured. Skipping examples.")