"""
SQLAlchemy ORM Models
======================
Defines the database schema for cryptocurrency market data.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column, Integer, BigInteger, Float, String, DateTime,
    Index, UniqueConstraint
)

from database.connection import Base

logger = logging.getLogger(__name__)


class CryptoMarketData(Base):
    """
    Stores cryptocurrency market snapshots.
    Each row represents a single coin's market data at a point in time.
    """

    __tablename__ = "crypto_market_data"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Coin identification
    coin_id = Column(String(50), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)
    name = Column(String(100), nullable=False)

    # Price data
    current_price = Column(Float, nullable=False)
    high_24h = Column(Float)
    low_24h = Column(Float)
    price_change_24h = Column(Float)
    price_change_pct_24h = Column(Float)
    price_change_pct_1h = Column(Float)
    price_change_pct_24h_detail = Column(Float)
    price_change_pct_7d = Column(Float)

    # Market data
    market_cap = Column(BigInteger)
    market_cap_rank = Column(Integer)
    fully_diluted_valuation = Column(BigInteger)
    total_volume = Column(BigInteger)
    market_cap_change_24h = Column(Float)
    market_cap_change_pct_24h = Column(Float)

    # Supply data
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)

    # All-time records
    ath = Column(Float)
    ath_change_pct = Column(Float)
    ath_date = Column(DateTime(timezone=True))
    atl = Column(Float)
    atl_change_pct = Column(Float)
    atl_date = Column(DateTime(timezone=True))

    # Computed metrics (from data cleaner)
    price_spread_24h = Column(Float)
    price_spread_pct = Column(Float)
    volume_to_mcap_ratio = Column(Float)

    # Timestamps
    last_updated = Column(DateTime(timezone=True))
    ingested_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )

    # Table-level indexes for query performance
    __table_args__ = (
        # Composite index for time-series queries per coin
        Index("ix_coin_ingested", "coin_id", "ingested_at"),
        # Index for market cap ranking queries
        Index("ix_market_cap_rank", "market_cap_rank"),
        # Unique constraint to prevent duplicate snapshots
        UniqueConstraint(
            "coin_id", "last_updated",
            name="uq_coin_snapshot"
        ),
    )

    def __repr__(self):
        return (
            f"<CryptoMarketData("
            f"coin={self.coin_id}, "
            f"price=${self.current_price:,.2f}, "
            f"at={self.ingested_at}"
            f")>"
        )


def insert_market_data(session, df):
    """
    Insert cleaned DataFrame into the crypto_market_data table.
    Handles duplicate detection via the unique constraint.

    Args:
        session: SQLAlchemy session
        df: Cleaned pandas DataFrame from DataCleaner

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            record = CryptoMarketData(
                coin_id=row.get("coin_id"),
                symbol=row.get("symbol"),
                name=row.get("name"),
                current_price=row.get("current_price"),
                high_24h=row.get("high_24h"),
                low_24h=row.get("low_24h"),
                price_change_24h=row.get("price_change_24h"),
                price_change_pct_24h=row.get("price_change_pct_24h"),
                price_change_pct_1h=row.get("price_change_pct_1h"),
                price_change_pct_24h_detail=row.get("price_change_pct_24h_detail"),
                price_change_pct_7d=row.get("price_change_pct_7d"),
                market_cap=_safe_int(row.get("market_cap")),
                market_cap_rank=_safe_int(row.get("market_cap_rank")),
                fully_diluted_valuation=_safe_int(row.get("fully_diluted_valuation")),
                total_volume=_safe_int(row.get("total_volume")),
                market_cap_change_24h=row.get("market_cap_change_24h"),
                market_cap_change_pct_24h=row.get("market_cap_change_pct_24h"),
                circulating_supply=row.get("circulating_supply"),
                total_supply=row.get("total_supply"),
                max_supply=row.get("max_supply"),
                ath=row.get("ath"),
                ath_change_pct=row.get("ath_change_pct"),
                ath_date=_safe_datetime(row.get("ath_date")),
                atl=row.get("atl"),
                atl_change_pct=row.get("atl_change_pct"),
                atl_date=_safe_datetime(row.get("atl_date")),
                price_spread_24h=row.get("price_spread_24h"),
                price_spread_pct=row.get("price_spread_pct"),
                volume_to_mcap_ratio=row.get("volume_to_mcap_ratio"),
                last_updated=_safe_datetime(row.get("last_updated")),
                ingested_at=_safe_datetime(row.get("ingested_at")),
            )
            session.add(record)
            session.flush()  # Flush to detect unique constraint violations early
            inserted += 1

        except Exception as e:
            session.rollback()
            if "uq_coin_snapshot" in str(e):
                skipped += 1
                logger.debug(f"Skipped duplicate: {row.get('coin_id')} at {row.get('last_updated')}")
            else:
                logger.error(f"Error inserting {row.get('coin_id')}: {e}")
                skipped += 1

    logger.info(f"Database insert complete: {inserted} inserted, {skipped} skipped")
    return inserted, skipped


def _safe_int(value):
    """Safely convert a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        import math
        if math.isnan(float(value)):
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_datetime(value):
    """Safely handle datetime values, returning None for NaT."""
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
        return value
    except (ValueError, TypeError):
        return None
