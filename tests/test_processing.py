"""
Tests for the Data Cleaning & Transformation module.
Run with: pytest tests/test_processing.py -v
"""

import pytest
import pandas as pd
import numpy as np
from processing.data_cleaner import DataCleaner


@pytest.fixture
def cleaner():
    """Create a DataCleaner instance."""
    return DataCleaner()


@pytest.fixture
def sample_raw_data():
    """Sample raw API response data for testing."""
    return [
        {
            "id": "bitcoin",
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
            "price_change_percentage_24h": 2.28,
            "market_cap_change_24h": 30000000000,
            "market_cap_change_percentage_24h": 2.36,
            "circulating_supply": 19500000,
            "total_supply": 21000000,
            "max_supply": 21000000,
            "ath": 73738.00,
            "ath_change_percentage": -9.14,
            "ath_date": "2024-03-14T07:10:36.635Z",
            "atl": 67.81,
            "atl_change_percentage": 98700.50,
            "atl_date": "2013-07-06T00:00:00.000Z",
            "last_updated": "2024-12-01T10:30:00.000Z",
            "price_change_percentage_1h_in_currency": 0.15,
            "price_change_percentage_24h_in_currency": 2.28,
            "price_change_percentage_7d_in_currency": 5.67,
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3500.75,
            "market_cap": 420000000000,
            "market_cap_rank": 2,
            "fully_diluted_valuation": 420000000000,
            "total_volume": 15000000000,
            "high_24h": 3600.00,
            "low_24h": 3400.00,
            "price_change_24h": -50.25,
            "price_change_percentage_24h": -1.41,
            "market_cap_change_24h": -6000000000,
            "market_cap_change_percentage_24h": -1.41,
            "circulating_supply": 120000000,
            "total_supply": None,
            "max_supply": None,
            "ath": 4878.26,
            "ath_change_percentage": -28.22,
            "ath_date": "2021-11-10T14:24:19.604Z",
            "atl": 0.432979,
            "atl_change_percentage": 808500.00,
            "atl_date": "2015-10-20T00:00:00.000Z",
            "last_updated": "2024-12-01T10:30:00.000Z",
            "price_change_percentage_1h_in_currency": -0.05,
            "price_change_percentage_24h_in_currency": -1.41,
            "price_change_percentage_7d_in_currency": 3.20,
        },
    ]


class TestDataCleaner:
    """Test suite for the DataCleaner class."""

    def test_clean_market_data_returns_dataframe(self, cleaner, sample_raw_data):
        """Test that cleaning returns a DataFrame."""
        df = cleaner.clean_market_data(sample_raw_data)

        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_clean_market_data_empty_input(self, cleaner):
        """Test handling of empty input."""
        result = cleaner.clean_market_data([])
        assert result is None

    def test_clean_market_data_none_input(self, cleaner):
        """Test handling of None input."""
        result = cleaner.clean_market_data(None)
        assert result is None

    def test_column_renaming(self, cleaner, sample_raw_data):
        """Test that columns are renamed correctly."""
        df = cleaner.clean_market_data(sample_raw_data)

        assert "coin_id" in df.columns
        assert "current_price" in df.columns
        assert "price_change_pct_24h" in df.columns
        # Original names should NOT be present
        assert "id" not in df.columns
        assert "price_change_percentage_24h" not in df.columns

    def test_data_type_conversion(self, cleaner, sample_raw_data):
        """Test that data types are converted correctly."""
        df = cleaner.clean_market_data(sample_raw_data)

        # Numeric columns
        assert pd.api.types.is_float_dtype(df["current_price"])
        assert pd.api.types.is_numeric_dtype(df["market_cap"])
        assert pd.api.types.is_numeric_dtype(df["total_volume"])

        # String columns
        assert pd.api.types.is_string_dtype(df["coin_id"])
        assert pd.api.types.is_string_dtype(df["symbol"])

        # Datetime columns
        assert pd.api.types.is_datetime64_any_dtype(df["last_updated"])

    def test_metadata_columns_added(self, cleaner, sample_raw_data):
        """Test that metadata columns are added."""
        df = cleaner.clean_market_data(sample_raw_data)

        assert "ingested_at" in df.columns
        assert "price_spread_24h" in df.columns
        assert "price_spread_pct" in df.columns
        assert "volume_to_mcap_ratio" in df.columns

    def test_price_spread_calculation(self, cleaner, sample_raw_data):
        """Test price spread calculation."""
        df = cleaner.clean_market_data(sample_raw_data)

        btc = df[df["coin_id"] == "bitcoin"].iloc[0]
        # high_24h (68000) - low_24h (66000) = 2000
        assert btc["price_spread_24h"] == pytest.approx(2000.0)

    def test_volume_to_mcap_ratio(self, cleaner, sample_raw_data):
        """Test volume to market cap ratio calculation."""
        df = cleaner.clean_market_data(sample_raw_data)

        btc = df[df["coin_id"] == "bitcoin"].iloc[0]
        expected_ratio = 25000000000 / 1300000000000
        assert btc["volume_to_mcap_ratio"] == pytest.approx(expected_ratio, rel=1e-4)

    def test_missing_values_handling(self, cleaner, sample_raw_data):
        """Test that missing values are handled correctly."""
        # Ethereum has None for total_supply and max_supply
        df = cleaner.clean_market_data(sample_raw_data)

        # The row should still exist (not dropped — only critical columns cause drops)
        assert len(df) == 2

        # Percentage change columns should be filled with 0
        assert df["price_change_pct_24h"].isna().sum() == 0

    def test_negative_price_removal(self, cleaner):
        """Test that records with negative prices are removed."""
        bad_data = [
            {
                "id": "fakecoin",
                "symbol": "fake",
                "name": "Fake Coin",
                "current_price": -100,
                "market_cap": 1000,
                "total_volume": 100,
                "high_24h": 10, "low_24h": 5,
                "last_updated": "2024-12-01T10:30:00.000Z",
            }
        ]
        df = cleaner.clean_market_data(bad_data)

        assert df is not None
        assert len(df) == 0  # Negative price should be removed

    def test_string_normalization(self, cleaner, sample_raw_data):
        """Test that string columns are normalized."""
        df = cleaner.clean_market_data(sample_raw_data)

        # coin_id and symbol should be lowercase
        assert df["coin_id"].iloc[0] == "bitcoin"
        assert df["symbol"].iloc[0] == "btc"

        # name should be Title Case
        assert df["name"].iloc[0] == "Bitcoin"

    def test_get_summary(self, cleaner, sample_raw_data):
        """Test the data summary generation."""
        df = cleaner.clean_market_data(sample_raw_data)
        summary = cleaner.get_summary(df)

        assert summary["total_records"] == 2
        assert "bitcoin" in summary["coins"]
        assert "ethereum" in summary["coins"]
        assert summary["price_range"]["min"] > 0
        assert summary["price_range"]["max"] > summary["price_range"]["min"]

    def test_get_summary_empty_data(self, cleaner):
        """Test summary with no data."""
        summary = cleaner.get_summary(None)
        assert summary == {"status": "no_data"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
