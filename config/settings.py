"""
Centralized configuration management.
Loads settings from environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://username:password@localhost:5432/crypto_analytics"
    )

    # CoinGecko API
    API_BASE_URL: str = os.getenv(
        "API_BASE_URL",
        "https://api.coingecko.com/api/v3"
    )

    # Data Ingestion
    FETCH_INTERVAL: int = int(os.getenv("FETCH_INTERVAL", "300"))
    COINS: list = os.getenv(
        "COINS",
        "bitcoin,ethereum,solana,cardano,ripple"
    ).split(",")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


# Singleton settings instance
settings = Settings()
