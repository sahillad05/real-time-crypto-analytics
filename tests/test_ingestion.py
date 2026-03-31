"""
Tests for the CoinGecko data ingestion client.
Run with: pytest tests/test_ingestion.py -v
"""

import pytest
from ingestion.coingecko_client import CoinGeckoClient


@pytest.fixture
def client():
    """Create a CoinGecko client instance."""
    return CoinGeckoClient()


class TestCoinGeckoClient:
    """Test suite for the CoinGecko API client."""

    def test_ping(self, client):
        """Test API connectivity."""
        result = client.ping()
        assert result is True, "API ping should return True"

    def test_fetch_market_data(self, client):
        """Test fetching general market data."""
        data = client.fetch_market_data(per_page=5)

        assert data is not None, "Market data should not be None"
        assert isinstance(data, list), "Market data should be a list"
        assert len(data) > 0, "Market data should contain at least one coin"

        # Verify expected fields exist in the response
        first_coin = data[0]
        expected_fields = [
            "id", "symbol", "name", "current_price",
            "market_cap", "total_volume", "high_24h", "low_24h"
        ]
        for field in expected_fields:
            assert field in first_coin, f"Missing expected field: {field}"

    def test_fetch_tracked_coins(self, client):
        """Test fetching data for tracked coins from settings."""
        data = client.fetch_tracked_coins()

        assert data is not None, "Tracked coins data should not be None"
        assert isinstance(data, list), "Tracked coins data should be a list"
        assert len(data) > 0, "Should fetch at least one tracked coin"

        # Verify we got coins matching our config
        coin_ids = [coin["id"] for coin in data]
        assert "bitcoin" in coin_ids, "Bitcoin should be in tracked coins"

    def test_fetch_coin_details(self, client):
        """Test fetching details for a specific coin."""
        data = client.fetch_coin_details("bitcoin")

        assert data is not None, "Coin details should not be None"
        assert isinstance(data, dict), "Coin details should be a dict"
        assert data["id"] == "bitcoin", "Should return Bitcoin details"
        assert "market_data" in data, "Should contain market_data"

    def test_fetch_market_data_with_specific_coins(self, client):
        """Test fetching market data for specific coin IDs."""
        coins = ["bitcoin", "ethereum"]
        data = client.fetch_market_data(coin_ids=coins)

        assert data is not None
        assert len(data) == 2, "Should return exactly 2 coins"

        returned_ids = {coin["id"] for coin in data}
        assert returned_ids == set(coins), "Should return the requested coins"

    def test_market_data_fields_types(self, client):
        """Test that market data fields have correct types."""
        data = client.fetch_market_data(coin_ids=["bitcoin"])

        assert data is not None and len(data) > 0
        coin = data[0]

        assert isinstance(coin["current_price"], (int, float))
        assert isinstance(coin["market_cap"], (int, float))
        assert isinstance(coin["total_volume"], (int, float))
        assert isinstance(coin["name"], str)
        assert isinstance(coin["symbol"], str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
