"""
Analytics SQL Queries
======================
Pre-built analytics queries for extracting insights from the stored data.
All queries return results as lists of dictionaries for easy consumption.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from database.connection import get_session

logger = logging.getLogger(__name__)


class CryptoAnalytics:
    """Collection of analytics queries for cryptocurrency market data."""

    @staticmethod
    def get_latest_prices() -> list:
        """
        Get the most recent price for each tracked coin.

        Returns:
            List of dicts with coin_id, name, price, change_24h, etc.
        """
        query = text("""
            SELECT DISTINCT ON (coin_id)
                coin_id, name, symbol, current_price,
                price_change_pct_24h, market_cap, total_volume,
                market_cap_rank, ingested_at
            FROM crypto_market_data
            ORDER BY coin_id, ingested_at DESC
        """)

        with get_session() as session:
            result = session.execute(query)
            return [dict(row._mapping) for row in result]

    @staticmethod
    def get_price_history(coin_id: str, hours: int = 24) -> list:
        """
        Get price history for a specific coin over the last N hours.

        Args:
            coin_id: CoinGecko coin ID
            hours: Number of hours to look back

        Returns:
            List of dicts with timestamp and price data
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        query = text("""
            SELECT 
                coin_id, current_price, high_24h, low_24h,
                total_volume, market_cap, ingested_at
            FROM crypto_market_data
            WHERE coin_id = :coin_id AND ingested_at >= :since
            ORDER BY ingested_at ASC
        """)

        with get_session() as session:
            result = session.execute(query, {"coin_id": coin_id, "since": since})
            return [dict(row._mapping) for row in result]

    @staticmethod
    def get_market_overview() -> dict:
        """
        Get an overview of the entire tracked market.

        Returns:
            Dict with total market cap, volume, top gainers, top losers
        """
        overview = {}

        # Total market cap and volume from latest data
        query = text("""
            WITH latest AS (
                SELECT DISTINCT ON (coin_id)
                    coin_id, name, current_price, market_cap,
                    total_volume, price_change_pct_24h, market_cap_rank
                FROM crypto_market_data
                ORDER BY coin_id, ingested_at DESC
            )
            SELECT 
                COUNT(*) as total_coins,
                SUM(market_cap) as total_market_cap,
                SUM(total_volume) as total_volume,
                AVG(price_change_pct_24h) as avg_change_24h
            FROM latest
        """)

        with get_session() as session:
            result = session.execute(query)
            row = result.fetchone()
            if row:
                overview["total_coins"] = row[0]
                overview["total_market_cap"] = row[1]
                overview["total_volume"] = row[2]
                overview["avg_change_24h"] = row[3]

        # Top gainers and losers
        query = text("""
            WITH latest AS (
                SELECT DISTINCT ON (coin_id)
                    coin_id, name, current_price, price_change_pct_24h
                FROM crypto_market_data
                ORDER BY coin_id, ingested_at DESC
            )
            SELECT coin_id, name, current_price, price_change_pct_24h
            FROM latest
            ORDER BY price_change_pct_24h DESC
        """)

        with get_session() as session:
            result = session.execute(query)
            rows = [dict(row._mapping) for row in result]
            if rows:
                overview["top_gainer"] = rows[0]
                overview["top_loser"] = rows[-1]

        return overview

    @staticmethod
    def get_volatility_analysis() -> list:
        """
        Analyze price volatility for each tracked coin.

        Returns:
            List of dicts with coin_id, avg spread, max spread, etc.
        """
        query = text("""
            WITH latest_day AS (
                SELECT *
                FROM crypto_market_data
                WHERE ingested_at >= NOW() - INTERVAL '24 hours'
            )
            SELECT 
                coin_id,
                name,
                COUNT(*) as data_points,
                AVG(current_price) as avg_price,
                MIN(current_price) as min_price,
                MAX(current_price) as max_price,
                AVG(price_spread_pct) as avg_spread_pct,
                MAX(price_spread_pct) as max_spread_pct,
                AVG(volume_to_mcap_ratio) as avg_liquidity
            FROM latest_day
            GROUP BY coin_id, name
            ORDER BY avg_spread_pct DESC
        """)

        with get_session() as session:
            result = session.execute(query)
            return [dict(row._mapping) for row in result]

    @staticmethod
    def get_volume_trends(hours: int = 24) -> list:
        """
        Analyze volume trends over time.

        Args:
            hours: Number of hours to analyze

        Returns:
            List of dicts with hourly volume aggregations
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        query = text("""
            SELECT 
                coin_id,
                name,
                DATE_TRUNC('hour', ingested_at) as hour,
                AVG(total_volume) as avg_volume,
                AVG(current_price) as avg_price,
                COUNT(*) as data_points
            FROM crypto_market_data
            WHERE ingested_at >= :since
            GROUP BY coin_id, name, DATE_TRUNC('hour', ingested_at)
            ORDER BY coin_id, hour
        """)

        with get_session() as session:
            result = session.execute(query, {"since": since})
            return [dict(row._mapping) for row in result]

    @staticmethod
    def get_record_count() -> int:
        """Get total number of records in the database."""
        query = text("SELECT COUNT(*) FROM crypto_market_data")

        with get_session() as session:
            result = session.execute(query)
            return result.scalar() or 0

    @staticmethod
    def get_coin_summary(coin_id: str) -> dict:
        """
        Get comprehensive summary for a specific coin.

        Args:
            coin_id: CoinGecko coin ID

        Returns:
            Dict with latest data, stats, and trends
        """
        summary = {}

        # Latest data
        query = text("""
            SELECT *
            FROM crypto_market_data
            WHERE coin_id = :coin_id
            ORDER BY ingested_at DESC
            LIMIT 1
        """)

        with get_session() as session:
            result = session.execute(query, {"coin_id": coin_id})
            row = result.fetchone()
            if row:
                summary["latest"] = dict(row._mapping)

        # Historical stats (last 24h)
        query = text("""
            SELECT 
                COUNT(*) as data_points,
                AVG(current_price) as avg_price,
                MIN(current_price) as min_price,
                MAX(current_price) as max_price,
                AVG(total_volume) as avg_volume,
                MIN(ingested_at) as first_seen,
                MAX(ingested_at) as last_seen
            FROM crypto_market_data
            WHERE coin_id = :coin_id
              AND ingested_at >= NOW() - INTERVAL '24 hours'
        """)

        with get_session() as session:
            result = session.execute(query, {"coin_id": coin_id})
            row = result.fetchone()
            if row:
                summary["stats_24h"] = dict(row._mapping)

        return summary
