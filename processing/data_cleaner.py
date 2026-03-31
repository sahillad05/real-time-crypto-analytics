"""
Data Cleaner & Transformer
===========================
Cleans raw API responses and transforms them into structured,
analysis-ready DataFrames for database storage.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and transforms raw CoinGecko API data into structured DataFrames."""

    # Columns we extract from the raw API response
    RAW_COLUMNS = [
        "id", "symbol", "name", "current_price", "market_cap",
        "market_cap_rank", "fully_diluted_valuation", "total_volume",
        "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "market_cap_change_24h",
        "market_cap_change_percentage_24h", "circulating_supply",
        "total_supply", "max_supply", "ath", "ath_change_percentage",
        "ath_date", "atl", "atl_change_percentage", "atl_date",
        "last_updated",
        "price_change_percentage_1h_in_currency",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
    ]

    # Final clean column names for the database
    CLEAN_COLUMNS = {
        "id": "coin_id",
        "symbol": "symbol",
        "name": "name",
        "current_price": "current_price",
        "market_cap": "market_cap",
        "market_cap_rank": "market_cap_rank",
        "fully_diluted_valuation": "fully_diluted_valuation",
        "total_volume": "total_volume",
        "high_24h": "high_24h",
        "low_24h": "low_24h",
        "price_change_24h": "price_change_24h",
        "price_change_percentage_24h": "price_change_pct_24h",
        "market_cap_change_24h": "market_cap_change_24h",
        "market_cap_change_percentage_24h": "market_cap_change_pct_24h",
        "circulating_supply": "circulating_supply",
        "total_supply": "total_supply",
        "max_supply": "max_supply",
        "ath": "ath",
        "ath_change_percentage": "ath_change_pct",
        "ath_date": "ath_date",
        "atl": "atl",
        "atl_change_percentage": "atl_change_pct",
        "atl_date": "atl_date",
        "last_updated": "last_updated",
        "price_change_percentage_1h_in_currency": "price_change_pct_1h",
        "price_change_percentage_24h_in_currency": "price_change_pct_24h_detail",
        "price_change_percentage_7d_in_currency": "price_change_pct_7d",
    }

    def clean_market_data(self, raw_data: list) -> Optional[pd.DataFrame]:
        """
        Clean and transform raw market data from the CoinGecko API.

        Args:
            raw_data: List of coin dictionaries from the API response

        Returns:
            Cleaned pandas DataFrame ready for database insertion, or None on failure
        """
        if not raw_data:
            logger.warning("No raw data to clean")
            return None

        try:
            logger.info(f"Cleaning market data for {len(raw_data)} coins...")

            # Step 1: Create DataFrame from raw data
            df = pd.DataFrame(raw_data)
            logger.debug(f"Raw DataFrame shape: {df.shape}")

            # Step 2: Select only the columns we need (handle missing columns gracefully)
            available_columns = [col for col in self.RAW_COLUMNS if col in df.columns]
            missing_columns = [col for col in self.RAW_COLUMNS if col not in df.columns]

            if missing_columns:
                logger.debug(f"Missing columns (will be filled with NaN): {missing_columns}")

            df = df[available_columns].copy()

            # Add missing columns with NaN
            for col in missing_columns:
                df[col] = np.nan

            # Step 3: Rename columns to clean names
            df = df.rename(columns=self.CLEAN_COLUMNS)

            # Step 4: Handle data types
            df = self._convert_data_types(df)

            # Step 5: Handle missing values
            df = self._handle_missing_values(df)

            # Step 6: Add metadata columns
            df = self._add_metadata(df)

            # Step 7: Validate data quality
            df = self._validate_data(df)

            logger.info(
                f"Data cleaning complete: {len(df)} records, "
                f"{len(df.columns)} columns"
            )

            return df

        except Exception as e:
            logger.error(f"Error cleaning market data: {e}", exc_info=True)
            return None

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        logger.debug("Converting data types...")

        # Numeric columns
        numeric_columns = [
            "current_price", "market_cap", "market_cap_rank",
            "fully_diluted_valuation", "total_volume", "high_24h", "low_24h",
            "price_change_24h", "price_change_pct_24h",
            "market_cap_change_24h", "market_cap_change_pct_24h",
            "circulating_supply", "total_supply", "max_supply",
            "ath", "ath_change_pct", "atl", "atl_change_pct",
            "price_change_pct_1h", "price_change_pct_24h_detail",
            "price_change_pct_7d",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # String columns
        string_columns = ["coin_id", "symbol", "name"]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()

        # Keep 'name' with title case for display
        if "name" in df.columns:
            df["name"] = df["name"].str.title()

        # Datetime columns
        datetime_columns = ["last_updated", "ath_date", "atl_date"]
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values with appropriate strategies."""
        logger.debug("Handling missing values...")

        # Critical columns — drop rows where these are missing
        critical_columns = ["coin_id", "current_price"]
        before_count = len(df)
        df = df.dropna(subset=critical_columns)
        dropped = before_count - len(df)
        if dropped > 0:
            logger.warning(f"Dropped {dropped} rows with missing critical data")

        # Fill numeric columns with 0 where it makes sense
        fill_zero_columns = [
            "price_change_24h", "price_change_pct_24h",
            "market_cap_change_24h", "market_cap_change_pct_24h",
            "price_change_pct_1h", "price_change_pct_24h_detail",
            "price_change_pct_7d",
        ]
        for col in fill_zero_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns for tracking."""
        logger.debug("Adding metadata columns...")

        # Timestamp when the data was ingested (our system time)
        df["ingested_at"] = datetime.now(timezone.utc)

        # Price spread (high - low) as a volatility indicator
        if "high_24h" in df.columns and "low_24h" in df.columns:
            df["price_spread_24h"] = df["high_24h"] - df["low_24h"]
            df["price_spread_pct"] = np.where(
                df["low_24h"] > 0,
                (df["price_spread_24h"] / df["low_24h"]) * 100,
                0
            )

        # Volume-to-Market Cap ratio (liquidity indicator)
        if "total_volume" in df.columns and "market_cap" in df.columns:
            df["volume_to_mcap_ratio"] = np.where(
                df["market_cap"] > 0,
                df["total_volume"] / df["market_cap"],
                0
            )

        return df

    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and log any issues."""
        logger.debug("Validating data quality...")

        # Check for negative prices
        if "current_price" in df.columns:
            negative_prices = df[df["current_price"] < 0]
            if len(negative_prices) > 0:
                logger.warning(
                    f"Found {len(negative_prices)} records with negative prices — removing"
                )
                df = df[df["current_price"] >= 0]

        # Check for unreasonable percentage changes (> 1000%)
        pct_columns = [
            "price_change_pct_24h", "price_change_pct_1h", "price_change_pct_7d"
        ]
        for col in pct_columns:
            if col in df.columns:
                extreme = df[df[col].abs() > 1000]
                if len(extreme) > 0:
                    logger.warning(
                        f"Found {len(extreme)} records with extreme {col} values "
                        f"(>1000%) — flagging but keeping"
                    )

        # Log data quality summary
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0]
        if len(cols_with_nulls) > 0:
            logger.debug(f"Columns with remaining NaN values:\n{cols_with_nulls}")

        return df

    def get_summary(self, df: pd.DataFrame) -> dict:
        """
        Generate a summary of the cleaned data for logging/display.

        Args:
            df: Cleaned DataFrame

        Returns:
            Dictionary with summary statistics
        """
        if df is None or df.empty:
            return {"status": "no_data"}

        summary = {
            "total_records": len(df),
            "columns": len(df.columns),
            "coins": df["coin_id"].tolist() if "coin_id" in df.columns else [],
            "price_range": {
                "min": float(df["current_price"].min()),
                "max": float(df["current_price"].max()),
                "mean": float(df["current_price"].mean()),
            },
            "null_percentage": float(df.isnull().mean().mean() * 100),
            "ingested_at": str(df["ingested_at"].iloc[0]) if "ingested_at" in df.columns else None,
        }

        return summary
