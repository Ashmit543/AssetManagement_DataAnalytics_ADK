import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    """Centralized configuration settings for the application."""

    PROJECT_ID: str = os.getenv("PROJECT_ID")
    REGION: str = os.getenv("REGION")

    # API Keys
    ALPHA_VANTAGE_API_KEY: str = os.getenv("ALPHA_VANTAGE_API_KEY")

    # Add other configurations here if needed, e.g.,
    # BQ_DATASET: str = os.getenv("BQ_DATASET", "asset_management_dataset")

    def __post_init__(self):
        # Basic validation
        if not self.PROJECT_ID:
            raise ValueError("PROJECT_ID environment variable not set.")
        if not self.REGION:
            raise ValueError("REGION environment variable not set.")
        # ALPHA_VANTAGE_API_KEY can be None if it's retrieved from Secret Manager directly by tools

settings = Settings()