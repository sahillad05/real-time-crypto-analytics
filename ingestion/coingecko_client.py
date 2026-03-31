"""
CoinGecko API Client
====================
Handles all interactions with the CoinGecko free API.
Includes rate-limit handling, retries, and structured data extraction.
"""

import time
import logging
import requests
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


class CoinGeckoClient:
    """Client for fetching cryptocurrency data from CoinGecko API."""

    def __init__(self):
        self.base_url = settings.API_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "CryptoAnalytics/1.0"
        })
        # Rate limiting: track last request time
        self._last_request_time = 0
        self._min_request_interval = 2  # seconds between requests (safe for free tier)
        self._max_retries = 3

    def _rate_limit_wait(self):
        """Ensure we respect the API rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            wait_time = self._min_request_interval - elapsed
            logger.debug(f"Rate limit: waiting {wait_time:.1f}s before next request")
            time.sleep(wait_time)

    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict | list]:
        """
        Make a GET request with retry logic and rate-limit handling.

        Args:
            endpoint: API endpoint path (e.g., '/coins/markets')
            params: Query parameters

        Returns:
            Parsed JSON response or None on failure
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(1, self._max_retries + 1):
            self._rate_limit_wait()

            try:
                logger.debug(f"Request [attempt {attempt}]: GET {url}")
                response = self.session.get(url, params=params, timeout=30)
                self._last_request_time = time.time()

                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited (429). Retrying after {retry_after}s "
                        f"(attempt {attempt}/{self._max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                # Handle server errors (5xx)
                if response.status_code >= 500:
                    logger.warning(
                        f"Server error ({response.status_code}). "
                        f"Retrying in 10s (attempt {attempt}/{self._max_retries})"
                    )
                    time.sleep(10)
                    continue

                # Raise for other HTTP errors
                response.raise_for_status()

                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Request timed out (attempt {attempt}/{self._max_retries})"
                )
                time.sleep(5)

            except requests.exceptions.ConnectionError:
                logger.error(
                    f"Connection error (attempt {attempt}/{self._max_retries})"
                )
                time.sleep(10)

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                return None

        logger.error(f"All {self._max_retries} attempts failed for {url}")
        return None

    def fetch_market_data(
        self,
        vs_currency: str = "usd",
        per_page: int = 10,
        page: int = 1,
        coin_ids: list = None
    ) -> Optional[list]:
        """
        Fetch market data for cryptocurrencies.

        Args:
            vs_currency: Target currency (default: 'usd')
            per_page: Number of results per page (max 250)
            page: Page number
            coin_ids: Optional list of specific coin IDs to fetch

        Returns:
            List of coin market data dictionaries, or None on failure
        """
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d"
        }

        # If specific coins are requested, filter by IDs
        if coin_ids:
            params["ids"] = ",".join(coin_ids)

        logger.info(
            f"Fetching market data: {len(coin_ids) if coin_ids else per_page} coins "
            f"(vs {vs_currency})"
        )

        data = self._make_request("/coins/markets", params=params)

        if data:
            logger.info(f"Successfully fetched data for {len(data)} coins")
        else:
            logger.error("Failed to fetch market data")

        return data

    def fetch_coin_details(self, coin_id: str) -> Optional[dict]:
        """
        Fetch detailed data for a specific coin.

        Args:
            coin_id: CoinGecko coin ID (e.g., 'bitcoin')

        Returns:
            Coin detail dictionary, or None on failure
        """
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false"
        }

        logger.info(f"Fetching details for coin: {coin_id}")
        return self._make_request(f"/coins/{coin_id}", params=params)

    def ping(self) -> bool:
        """
        Test API connectivity.

        Returns:
            True if the API is reachable, False otherwise
        """
        result = self._make_request("/ping")
        if result and "gecko_says" in result:
            logger.info(f"API ping successful: {result['gecko_says']}")
            return True
        logger.error("API ping failed")
        return False

    def fetch_tracked_coins(self) -> Optional[list]:
        """
        Fetch market data specifically for the coins configured in settings.

        Returns:
            List of market data for tracked coins
        """
        return self.fetch_market_data(coin_ids=settings.COINS)
