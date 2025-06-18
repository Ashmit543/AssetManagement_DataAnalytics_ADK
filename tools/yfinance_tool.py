import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from common.utils import get_current_ist_timestamp  # Assuming this is available


class YFinanceTool:
    """
    A wrapper for yfinance API to fetch various financial metrics.
    """

    def __init__(self):
        pass

    def get_historical_data(self, ticker: str, period: str = "1y", interval: str = "1d") -> Optional[pd.DataFrame]:
        """
        Fetches historical stock data.
        Args:
            ticker: Stock ticker symbol (e.g., 'RELIANCE.NS').
            period: Data period (e.g., "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max").
            interval: Data interval (e.g., "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo").
        Returns:
            A pandas DataFrame with historical data or None if an error occurs.
        """
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period, interval=interval)
            if hist.empty:
                print(f"No historical data found for {ticker} with period={period}, interval={interval}")
                return None
            hist.index = hist.index.date  # Convert datetime index to date
            hist.reset_index(inplace=True)
            hist.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
                                 'Volume': 'volume'}, inplace=True)
            return hist[['date', 'open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            print(f"Error fetching historical data for {ticker}: {e}")
            return None

    def get_company_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetches basic company information.
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if not info:
                print(f"No info found for {ticker}")
                return None
            return info
        except Exception as e:
            print(f"Error fetching company info for {ticker}: {e}")
            return None

    def get_key_metrics(self, ticker: str) -> Dict[str, Any]:
        """
        Fetches key financial metrics and transforms them into the BigQuery schema format.
        """
        metrics = {
            "ticker": ticker,
            "date": datetime.now().date().isoformat(),  # Use current date for "snapshot" metrics
            "ingestion_timestamp": get_current_ist_timestamp()
        }
        info = self.get_company_info(ticker)
        if not info:
            print(f"Could not retrieve company info for {ticker}. Returning partial metrics.")
            return metrics

        # Basic stock performance
        metrics["current_price"] = info.get("currentPrice")
        metrics["day_change_percent"] = info.get("regularMarketChangePercent") * 100 if info.get(
            "regularMarketChangePercent") else None
        metrics["fifty_two_week_high"] = info.get("fiftyTwoWeekHigh")
        metrics["fifty_two_week_low"] = info.get("fiftyTwoWeekLow")
        metrics["market_cap"] = info.get("marketCap")
        metrics["volume"] = info.get("regularMarketVolume")

        # Valuation and profitability
        metrics["pe_ratio"] = info.get("trailingPE")
        metrics["eps"] = info.get("trailingEps")
        metrics["roe"] = info.get("returnOnEquity")  # This is often an annualized figure
        metrics["debt_to_equity"] = info.get("debtToEquity")

        # Moving Averages (simplified - yfinance info doesn't directly expose these. Usually calculated from history.)
        # For a full implementation, you'd calculate these from historical data if needed for the BigQuery schema.
        # For now, we'll leave them as None or provide placeholders.
        metrics["moving_average_50"] = None  # To be calculated from historical data
        metrics["moving_average_200"] = None  # To be calculated from historical data
        metrics["rsi"] = None  # To be calculated from historical data
        metrics["beta"] = info.get("beta")

        # Revenue and Net Income (often from income statement; info provides summary)
        # yfinance `info` object has 'revenue' and 'netIncome' but their accuracy/availability can vary.
        # For more precise historical financials, you'd parse `stock.financials` and `stock.balancesheet`.
        # This is a simplified fetch for the latest available summary.
        metrics["revenue"] = info.get("totalRevenue")  # Or similar, check yfinance 'info' keys
        metrics["net_income"] = info.get("netIncome")  # Or similar, check yfinance 'info' keys

        # Placeholder for more complex metrics not directly from info
        metrics["cagr"] = None  # Requires historical data analysis
        metrics["geographical_exposure"] = None  # Requires parsing reports or detailed data
        metrics["risk_signals"] = None  # Derived from analysis
        metrics["sector_performance_index"] = None  # Derived from external data

        return metrics


# Example Usage (for testing purposes, not part of the tool itself for deployment)
if __name__ == "__main__":
    tool = YFinanceTool()
    ticker = "AAPL.NS"  # Example Indian stock

    print(f"--- Fetching historical data for {ticker} ---")
    hist_data = tool.get_historical_data(ticker, period="1mo", interval="1d")
    if hist_data is not None:
        print(hist_data.head())

    print(f"\n--- Fetching company info for {ticker} ---")
    company_info = tool.get_company_info(ticker)
    if company_info:
        print(f"Sector: {company_info.get('sector')}")
        print(f"Industry: {company_info.get('industry')}")
        print(f"Market Cap: {company_info.get('marketCap')}")
        print(f"Trailing PE: {company_info.get('trailingPE')}")

    print(f"\n--- Fetching key metrics for {ticker} (for BigQuery schema) ---")
    key_metrics = tool.get_key_metrics(ticker)
    print(key_metrics)