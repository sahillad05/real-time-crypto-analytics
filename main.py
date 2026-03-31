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
        from database.connection import init_database, test_connection

        logger.info("Testing database connection...")
        if not test_connection():
            logger.error(
                "Cannot connect to PostgreSQL. Check your DATABASE_URL in .env\n"
                "  Expected format: postgresql://user:password@host:port/dbname"
            )
            sys.exit(1)

        init_database()
        logger.info("Database initialized successfully! Tables are ready.")
    elif args.ingest:
        from ingestion.coingecko_client import CoinGeckoClient
        from processing.data_cleaner import DataCleaner

        client = CoinGeckoClient()
        cleaner = DataCleaner()

        # Test connectivity first
        logger.info("Testing API connectivity...")
        if not client.ping():
            logger.error("Cannot reach CoinGecko API. Check your internet connection.")
            sys.exit(1)

        # Step 1: Fetch raw market data
        logger.info("Step 1: Fetching market data for tracked coins...")
        raw_data = client.fetch_tracked_coins()

        if not raw_data:
            logger.error("Failed to fetch market data.")
            sys.exit(1)

        logger.info(f"Fetched {len(raw_data)} raw records")

        # Step 2: Clean and transform data
        logger.info("Step 2: Cleaning and transforming data...")
        df = cleaner.clean_market_data(raw_data)

        if df is None or df.empty:
            logger.error("Data cleaning failed — no valid records produced.")
            sys.exit(1)

        # Display cleaned data summary
        summary = cleaner.get_summary(df)
        logger.info(f"\n{'='*80}")
        logger.info("  CLEANED DATA SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"  Records    : {summary['total_records']}")
        logger.info(f"  Columns    : {summary['columns']}")
        logger.info(f"  Null %     : {summary['null_percentage']:.2f}%")
        logger.info(f"  Price range: ${summary['price_range']['min']:,.2f} — ${summary['price_range']['max']:,.2f}")
        logger.info(f"{'='*80}")

        # Display formatted table
        logger.info(f"\n  {'Coin':<15} {'Price (USD)':>12} {'1h %':>8} {'24h %':>8} {'7d %':>8} {'Vol/MCap':>10}")
        logger.info(f"  {'-'*71}")
        for _, row in df.iterrows():
            logger.info(
                f"  {row['name']:<15} ${row['current_price']:>11,.2f} "
                f"{row.get('price_change_pct_1h', 0):>+7.2f}% "
                f"{row.get('price_change_pct_24h', 0):>+7.2f}% "
                f"{row.get('price_change_pct_7d', 0):>+7.2f}% "
                f"{row.get('volume_to_mcap_ratio', 0):>9.4f}"
            )
        logger.info(f"{'='*80}")

        # Step 3: Store in PostgreSQL
        logger.info("Step 3: Storing data in PostgreSQL...")
        from database.connection import test_connection, get_session
        from database.models import insert_market_data

        if not test_connection():
            logger.error("Cannot connect to database. Run 'python main.py --init-db' first.")
            sys.exit(1)

        with get_session() as session:
            inserted, skipped = insert_market_data(session, df)

        logger.info(f"{'='*80}")
        logger.info(f"  PIPELINE COMPLETE")
        logger.info(f"  API → {len(raw_data)} raw → {len(df)} cleaned → {inserted} stored ({skipped} duplicates)")
        logger.info(f"{'='*80}")
    elif args.schedule:
        from scheduler.jobs import start_scheduler
        start_scheduler()
    elif args.dashboard:
        logger.info("Dashboard will be available in Chunk 6.")
    else:
        parser.print_help()
        logger.info("\nSystem initialized successfully. Use flags above to run components.")


if __name__ == "__main__":
    main()
