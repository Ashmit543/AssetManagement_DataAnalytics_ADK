import datetime

def get_current_ist_timestamp():
    """
    Returns the current timestamp in IST (Indian Standard Time) as an ISO formatted string.
    """
    # Using a fixed offset for IST (+5:30) for simplicity.
    # For more robust timezone handling, consider `pytz` or `zoneinfo` (Python 3.9+).
    ist_offset = datetime.timedelta(hours=5, minutes=30)
    now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    now_ist = now_utc + ist_offset
    return now_ist.isoformat()

def sanitize_ticker(ticker: str) -> str:
    """
    Sanitizes a stock ticker to ensure it's in a consistent format (e.g., uppercase).
    """
    return ticker.strip().upper()

def is_valid_date_format(date_str: str, format_str: str = "%Y-%m-%d") -> bool:
    """
    Checks if a string matches a given date format.
    """
    try:
        datetime.datetime.strptime(date_str, format_str)
        return True
    except ValueError:
        return False

# Add other utility functions as needed (e.g., data validation, formatting helpers)