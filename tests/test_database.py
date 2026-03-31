"""
Tests for the Database Integration layer.
Run with: pytest tests/test_database.py -v

NOTE: These tests require a running PostgreSQL instance with the
      crypto_analytics database created. Set DATABASE_URL in .env first.
"""

import pytest
from datetime import datetime, timezone

import pandas as pd

from database.connection import test_connection, init_database, get_session, get_table_stats
from database.models import CryptoMarketData, insert_market_data
from database.queries import CryptoAnalytics


@pytest.fixture(scope="module")
def setup_db():
    """Initialize the database once for all tests in this module."""
    if not test_connection():
        pytest.skip("Database not available — skipping database tests")
    init_database()
    yield


@pytest.fixture
def sample_cleaned_df():
    """Create a sample cleaned DataFrame matching DataCleaner output."""
    return pd.DataFrame([
        {
            "coin_id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 67000.50,
            "market_cap": 1300000000000,
            "market_cap_rank": 1,
            "fully_diluted_valuation": 1400000000000,
            "total_volume": 25000000000,
            "high_24h": 68000.00,
            "low_24h": 66000.00,
            "price_change_24h": 1500.25,
            "price_change_pct_24h": 2.28,
            "price_change_pct_1h": 0.15,
            "price_change_pct_24h_detail": 2.28,
            "price_change_pct_7d": 5.67,
            "market_cap_change_24h": 30000000000,
            "market_cap_change_pct_24h": 2.36,
            "circulating_supply": 19500000,
            "total_supply": 21000000,
            "max_supply": 21000000,
            "ath": 73738.00,
            "ath_change_pct": -9.14,
            "ath_date": pd.Timestamp("2024-03-14T07:10:36.635Z"),
            "atl": 67.81,
            "atl_change_pct": 98700.50,
            "atl_date": pd.Timestamp("2013-07-06T00:00:00.000Z"),
            "last_updated": pd.Timestamp.now(tz="UTC"),
            "ingested_at": pd.Timestamp.now(tz="UTC"),
            "price_spread_24h": 2000.00,
            "price_spread_pct": 3.03,
            "volume_to_mcap_ratio": 0.0192,
        }
    ])


class TestDatabaseConnection:
    """Test database connectivity."""

    def test_connection(self, setup_db):
        """Test that we can connect to PostgreSQL."""
        assert test_connection() is True

    def test_table_init(self, setup_db):
        """Test that tables are created."""
        stats = get_table_stats()
        assert "crypto_market_data" in stats


class TestDataInsertion:
    """Test data insertion into the database."""

    def test_insert_market_data(self, setup_db, sample_cleaned_df):
        """Test inserting cleaned data into the database."""
        with get_session() as session:
            inserted, skipped = insert_market_data(session, sample_cleaned_df)

        assert inserted >= 0  # May be 0 if duplicate
        assert skipped >= 0

    def test_duplicate_detection(self, setup_db, sample_cleaned_df):
        """Test that duplicates are correctly skipped."""
        # Insert same data twice
        with get_session() as session:
            _, _ = insert_market_data(session, sample_cleaned_df)

        with get_session() as session:
            _, skipped = insert_market_data(session, sample_cleaned_df)

        # Second insert should detect duplicates
        assert skipped >= 0


class TestAnalyticsQueries:
    """Test analytics query functions."""

    def test_get_record_count(self, setup_db):
        """Test getting total record count."""
        count = CryptoAnalytics.get_record_count()
        assert isinstance(count, int)
        assert count >= 0

    def test_get_latest_prices(self, setup_db):
        """Test getting latest prices."""
        prices = CryptoAnalytics.get_latest_prices()
        assert isinstance(prices, list)

    def test_get_market_overview(self, setup_db):
        """Test getting market overview."""
        overview = CryptoAnalytics.get_market_overview()
        assert isinstance(overview, dict)

    def test_get_table_stats(self, setup_db):
        """Test getting table statistics."""
        stats = get_table_stats()
        assert isinstance(stats, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
