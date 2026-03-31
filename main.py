"""
Real-Time Crypto Analytics System
==================================
Main entry point for the application.
Supports CLI commands for database initialization, data ingestion, and dashboard launch.
"""

import argparse
import logging
import sys

from config.settings import settings


def setup_logging():
    """Configure application-wide logging."""
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def main():
    """Main entry point with CLI argument parsing."""
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Real-Time Crypto Analytics System"
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the database tables"
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Run a single data ingestion cycle"
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Start the scheduled ingestion pipeline"
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the Streamlit dashboard"
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Real-Time Crypto Analytics System")
    logger.info("=" * 60)
    logger.info(f"  Tracking coins : {', '.join(settings.COINS)}")
    logger.info(f"  Fetch interval : {settings.FETCH_INTERVAL}s")
    logger.info(f"  Log level      : {settings.LOG_LEVEL}")
    logger.info("=" * 60)

    if args.init_db:
        logger.info("Database initialization will be available in Chunk 4.")
    elif args.ingest:
        from ingestion.coingecko_client import CoinGeckoClient
        import json

        client = CoinGeckoClient()

        # Test connectivity first
        logger.info("Testing API connectivity...")
        if not client.ping():
            logger.error("Cannot reach CoinGecko API. Check your internet connection.")
            sys.exit(1)

        # Fetch market data for tracked coins
        logger.info("Fetching market data for tracked coins...")
        data = client.fetch_tracked_coins()

        if data:
            logger.info(f"\n{'='*80}")
            logger.info(f"  {'Coin':<15} {'Price (USD)':>12} {'24h Change':>12} {'Market Cap':>18} {'Volume':>18}")
            logger.info(f"  {'-'*75}")
            for coin in data:
                price = coin.get("current_price", 0)
                change_24h = coin.get("price_change_percentage_24h", 0) or 0
                market_cap = coin.get("market_cap", 0)
                volume = coin.get("total_volume", 0)
                arrow = "▲" if change_24h >= 0 else "▼"

                logger.info(
                    f"  {coin['name']:<15} ${price:>11,.2f} "
                    f"{arrow} {change_24h:>+.2f}% "
                    f"${market_cap:>16,.0f} ${volume:>16,.0f}"
                )
            logger.info(f"{'='*80}")
            logger.info(f"Total coins fetched: {len(data)}")
        else:
            logger.error("Failed to fetch market data.")
    elif args.schedule:
        logger.info("Scheduled pipeline will be available in Chunk 5.")
    elif args.dashboard:
        logger.info("Dashboard will be available in Chunk 6.")
    else:
        parser.print_help()
        logger.info("\nSystem initialized successfully. Use flags above to run components.")


if __name__ == "__main__":
    main()
