# tools/alpha_vantage_tool.py
import os
import requests
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, List
from common.gcp_clients import get_secret_manager_client
from common.constants import PROJECT_ID, SECRET_ALPHA_VANTAGE_API_KEY
from common.utils import get_current_ist_timestamp


class AlphaVantageAPIError(Exception):
    """Custom exception for Alpha Vantage API errors"""
    pass


class AlphaVantageTool:
    """
    Enhanced wrapper for the Alpha Vantage API with proper error handling
    for Indian stock market limitations and comprehensive logging.

    Key Issues Addressed:
    1. Alpha Vantage stopped supporting NSE (.NS) symbols
    2. Only BSE (.BSE) symbols work for Indian stocks
    3. Proper error handling for empty responses
    4. Rate limiting detection and handling
    5. Comprehensive logging for debugging
    """
    BASE_URL = "https://www.alphavantage.co/query"

    # Known supported Indian exchanges
    SUPPORTED_INDIAN_EXCHANGES = ['.BSE']
    UNSUPPORTED_INDIAN_EXCHANGES = ['.NS', '.NSE']

    def __init__(self):
        self._api_key = None
        self._load_api_key()

    def _load_api_key(self):
        """
        Loads the Alpha Vantage API key from Google Secret Manager.
        For local development, fall back to environment variable for convenience.
        """
        if os.getenv("ALPHA_VANTAGE_API_KEY"):  # For local testing via .env
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
            self._api_key = None

    def _validate_symbol(self, symbol: str) -> tuple[str, bool]:
        """
        Validates and potentially converts symbol format for Alpha Vantage compatibility.

        Returns:
            tuple: (validated_symbol, is_supported)
        """
        symbol = symbol.upper()

        # Check if it's an unsupported Indian exchange
        for unsupported in self.UNSUPPORTED_INDIAN_EXCHANGES:
            if symbol.endswith(unsupported):
                # Try to convert NSE to BSE equivalent
                if symbol.endswith('.NS'):
                    bse_symbol = symbol.replace('.NS', '.BSE')
                    print(f"WARNING: {symbol} uses NSE format which is not supported by Alpha Vantage.")
                    print(f"Attempting to use BSE equivalent: {bse_symbol}")
                    return bse_symbol, True
                else:
                    print(f"ERROR: {symbol} uses unsupported exchange format for Alpha Vantage.")
                    return symbol, False

        # Check if it's a supported Indian exchange
        for supported in self.SUPPORTED_INDIAN_EXCHANGES:
            if symbol.endswith(supported):
                return symbol, True

        # For international symbols (no exchange suffix), assume supported
        if '.' not in symbol:
            return symbol, True

        # Unknown exchange format
        print(f"WARNING: Unknown exchange format for {symbol}. Attempting anyway.")
        return symbol, True

    def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Enhanced helper method to make a request to the Alpha Vantage API
        with comprehensive error handling and logging.
        """
        if not self._api_key:
            raise AlphaVantageAPIError("Alpha Vantage API key is not available. Cannot make request.")

        params["apikey"] = self._api_key

        # Log the request being made (without API key)
        log_params = {k: v for k, v in params.items() if k != "apikey"}
        print(f"Alpha Vantage API Request: {log_params}")

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            print(f"Alpha Vantage API Response Status: {response.status_code}")

            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            # Try to decode JSON
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"JSON Decode Error: {e}")
                print(f"Raw response text: {response.text[:500]}...")
                raise AlphaVantageAPIError(f"Invalid JSON response from Alpha Vantage API")

            # Check for API-specific errors
            if "Error Message" in data:
                error_msg = data["Error Message"]
                print(f"Alpha Vantage API Error: {error_msg}")
                raise AlphaVantageAPIError(f"API Error: {error_msg}")

            if "Note" in data:
                note_msg = data["Note"]
                print(f"Alpha Vantage API Note: {note_msg}")
                # Check for rate limiting
                if "5 calls per minute" in note_msg or "500 calls per day" in note_msg:
                    raise AlphaVantageAPIError(f"Rate limit hit: {note_msg}")

            # Check if response is empty or missing expected data
            if not data or len(data) == 0:
                raise AlphaVantageAPIError("Empty response from Alpha Vantage API")

            print(f"Alpha Vantage API Response Keys: {list(data.keys())}")
            return data

        except requests.exceptions.Timeout:
            raise AlphaVantageAPIError("Request timeout - Alpha Vantage API took too long to respond")
        except requests.exceptions.ConnectionError:
            raise AlphaVantageAPIError("Connection error - Unable to connect to Alpha Vantage API")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                print(f"Response text: {e.response.text[:500]}...")
            raise AlphaVantageAPIError(f"Request failed: {str(e)}")

    def get_company_overview(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetches company overview and fundamental data with symbol validation.
        """
        validated_symbol, is_supported = self._validate_symbol(symbol)
        if not is_supported:
            print(f"Symbol {symbol} is not supported by Alpha Vantage.")
            return None

        params = {"function": "OVERVIEW", "symbol": validated_symbol}

        try:
            data = self._make_request(params)

            # Check if overview data is actually present
            if not data or not data.get("Symbol"):
                print(f"No company overview data available for {validated_symbol}")
                return None

            return data
        except AlphaVantageAPIError as e:
            print(f"Failed to get company overview for {validated_symbol}: {e}")
            return None

    def get_daily_time_series(self, symbol: str, outputsize: str = "compact") -> Optional[pd.DataFrame]:
        """
        Enhanced method to fetch daily time series data with better error handling.
        """
        validated_symbol, is_supported = self._validate_symbol(symbol)
        if not is_supported:
            print(f"Symbol {symbol} is not supported by Alpha Vantage.")
            return None

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": validated_symbol,
            "outputsize": outputsize
        }

        try:
            data = self._make_request(params)

            # Check for the expected time series key
            time_series_key = "Time Series (Daily)"
            if not data or time_series_key not in data:
                available_keys = list(data.keys()) if data else []
                print(f"No daily time series data found for {validated_symbol}")
                print(f"Available keys in response: {available_keys}")
                return None

            time_series_data = data[time_series_key]
            if not time_series_data:
                print(f"Empty time series data for {validated_symbol}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series_data, orient="index")

            # Rename columns to standard format
            df = df.rename(columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume"
            })

            # Convert index to datetime and then to date
            df.index = pd.to_datetime(df.index).date

            # Convert data types
            numeric_columns = ['open', 'high', 'low', 'close']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Handle volume separately as it might be very large
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')

            # Sort chronologically and reset index
            df.sort_index(inplace=True)
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            print(f"Successfully retrieved {len(df)} days of data for {validated_symbol}")
            return df[['date', 'open', 'high', 'low', 'close', 'volume']]

        except AlphaVantageAPIError as e:
            print(f"Failed to get daily time series for {validated_symbol}: {e}")
            return None

    def get_technical_indicator(self, function: str, symbol: str, interval: str = "daily",
                                time_period: int = 14, series_type: str = "close") -> Optional[Dict[str, Any]]:
        """
        Enhanced method to fetch technical indicators with proper error handling.
        """
        validated_symbol, is_supported = self._validate_symbol(symbol)
        if not is_supported:
            print(f"Symbol {symbol} is not supported by Alpha Vantage.")
            return None

        params = {
            "function": function,
            "symbol": validated_symbol,
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type
        }

        try:
            return self._make_request(params)
        except AlphaVantageAPIError as e:
            print(f"Failed to get {function} indicator for {validated_symbol}: {e}")
            return None

    def get_key_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        Enhanced method to fetch key financial metrics with comprehensive error handling
        and better data validation.
        """
        validated_symbol, is_supported = self._validate_symbol(symbol)

        metrics = {
            "ticker": symbol,  # Keep original ticker as requested
            "date": datetime.now().date().isoformat(),
            "ingestion_timestamp": get_current_ist_timestamp(),
            "data_source": "alpha_vantage",
            "symbol_used": validated_symbol,  # Track what symbol was actually used
            "data_availability": "none"  # Track what data was available
        }

        if not is_supported:
            print(f"Symbol {symbol} is not supported by Alpha Vantage.")
            metrics["error_message"] = f"Unsupported exchange format: {symbol}"
            return metrics

        data_found = []

        try:
            # Fetch Company Overview for fundamental data
            print(f"Fetching company overview for {validated_symbol}...")
            overview = self.get_company_overview(validated_symbol)

            if overview and overview.get("Symbol"):
                data_found.append("overview")
                print(f"Company overview found for {validated_symbol}")

                # Safely extract numeric values with proper error handling
                def safe_float(value, default=None):
                    if value and value != "None" and value != "-":
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return default
                    return default

                def safe_int(value, default=None):
                    if value and value != "None" and value != "-":
                        try:
                            return int(float(value))  # Convert to float first to handle decimal strings
                        except (ValueError, TypeError):
                            return default
                    return default

                # Map Alpha Vantage overview fields to BigQuery schema
                metrics["market_cap"] = safe_int(overview.get("MarketCapitalization"))
                metrics["pe_ratio"] = safe_float(overview.get("PERatio"))
                metrics["eps"] = safe_float(overview.get("EPS"))
                metrics["roe"] = safe_float(overview.get("ReturnOnEquityTTM"))
                metrics["debt_to_equity"] = safe_float(overview.get("DebtToEquityRatio"))
                metrics["revenue"] = safe_int(overview.get("RevenueTTM"))
                metrics["net_income"] = safe_int(overview.get("NetIncomeTTM"))
                metrics["beta"] = safe_float(overview.get("Beta"))
                metrics["geographical_exposure"] = overview.get("Description", "")[:500] if overview.get(
                    "Description") else None

            else:
                print(f"No company overview data available for {validated_symbol}")

            # Fetch Daily Time Series for price data
            print(f"Fetching daily time series for {validated_symbol}...")
            daily_data = self.get_daily_time_series(validated_symbol, outputsize="compact")

            if daily_data is not None and not daily_data.empty:
                data_found.append("daily_time_series")
                print(f"Daily time series found for {validated_symbol} ({len(daily_data)} days)")

                latest = daily_data.iloc[-1]
                metrics["current_price"] = latest.get("close")
                metrics["open"] = latest.get("open")
                metrics["high"] = latest.get("high")
                metrics["low"] = latest.get("low")
                metrics["close"] = latest.get("close")
                metrics["volume"] = latest.get("volume")

                # Calculate Day Change Percent
                if len(daily_data) >= 2:
                    prev_close = daily_data.iloc[-2].get("close")
                    if prev_close and prev_close != 0 and metrics["close"]:
                        metrics["day_change_percent"] = ((metrics["close"] - prev_close) / prev_close) * 100

                # Calculate 52-week high/low from available data
                metrics["fifty_two_week_high"] = float(daily_data['high'].max())
                metrics["fifty_two_week_low"] = float(daily_data['low'].min())

                # Calculate Moving Averages if enough data
                if len(daily_data) >= 200:
                    metrics["moving_average_50"] = float(daily_data['close'].rolling(window=50).mean().iloc[-1])
                    metrics["moving_average_200"] = float(daily_data['close'].rolling(window=200).mean().iloc[-1])
                elif len(daily_data) >= 50:
                    metrics["moving_average_50"] = float(daily_data['close'].rolling(window=50).mean().iloc[-1])

            else:
                print(f"No daily time series data available for {validated_symbol}")

            # Fetch RSI if we have price data
            if metrics.get("current_price"):
                print(f"Fetching RSI for {validated_symbol}...")
                rsi_data = self.get_technical_indicator("RSI", validated_symbol, interval="daily", time_period=14)

                if rsi_data and "Technical Analysis: RSI" in rsi_data:
                    rsi_series = rsi_data["Technical Analysis: RSI"]
                    if rsi_series:
                        data_found.append("rsi")
                        latest_date_rsi = sorted(rsi_series.keys())[-1]
                        metrics["rsi"] = float(rsi_series[latest_date_rsi]["RSI"])
                        print(f"RSI found for {validated_symbol}: {metrics['rsi']}")

        except Exception as e:
            print(f"Error fetching key metrics for {validated_symbol}: {e}")
            metrics["error_message"] = str(e)

        # Set data availability status
        if data_found:
            metrics["data_availability"] = ",".join(data_found)
            print(f"Data available for {validated_symbol}: {metrics['data_availability']}")
        else:
            print(f"No data available for {validated_symbol}")

        # Ensure all required fields are present
        required_fields = [
            "open", "high", "low", "close", "volume", "market_cap", "pe_ratio", "eps",
            "revenue", "net_income", "debt_to_equity", "roe", "current_price",
            "day_change_percent", "fifty_two_week_high", "fifty_two_week_low",
            "moving_average_50", "moving_average_200", "rsi", "beta", "cagr",
            "geographical_exposure", "risk_signals", "sector_performance_index"
        ]

        for field in required_fields:
            if field not in metrics:
                metrics[field] = None

        return metrics


# Example usage and testing
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    av_tool = AlphaVantageTool()

    if not av_tool._api_key:
        print("ERROR: No API key available. Please set ALPHA_VANTAGE_API_KEY in .env file.")
        exit(1)

    # Test symbols
    test_symbols = [
        "RELIANCE.NS",  # Should convert to BSE
        "RELIANCE.BSE",  # Should work directly
        "NONEXISTENT.BSE",  # Should fail gracefully
        "IBM",  # International symbol
    ]

    for symbol in test_symbols:
        print(f"\n{'=' * 60}")
        print(f"Testing symbol: {symbol}")
        print(f"{'=' * 60}")

        try:
            metrics = av_tool.get_key_metrics(symbol)
            print(f"Result for {symbol}:")
            print(f"  Data availability: {metrics.get('data_availability', 'none')}")
            print(f"  Symbol used: {metrics.get('symbol_used', 'N/A')}")
            print(f"  Current price: {metrics.get('current_price', 'N/A')}")
            print(f"  Market cap: {metrics.get('market_cap', 'N/A')}")
            if metrics.get('error_message'):
                print(f"  Error: {metrics['error_message']}")
        except Exception as e:
            print(f"Exception for {symbol}: {e}")