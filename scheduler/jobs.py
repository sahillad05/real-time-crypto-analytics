"""
Scheduled Ingestion Jobs
=========================
Manages automated data ingestion using APScheduler.
Runs the full pipeline (fetch → clean → store) at configurable intervals.
"""

import logging
import signal
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from config.settings import settings
from ingestion.coingecko_client import CoinGeckoClient
from processing.data_cleaner import DataCleaner
from database.connection import get_session, test_connection, init_database
from database.models import insert_market_data
from database.queries import CryptoAnalytics

logger = logging.getLogger(__name__)

# Pipeline components (initialized once, reused across runs)
client = CoinGeckoClient()
cleaner = DataCleaner()

# Track pipeline stats
pipeline_stats = {
    "total_runs": 0,
    "successful_runs": 0,
    "failed_runs": 0,
    "total_records_inserted": 0,
    "started_at": None,
}


def run_ingestion_pipeline():
    """
    Execute a single ingestion cycle: Fetch → Clean → Store.
    This function is called by the scheduler at each interval.
    """
    pipeline_stats["total_runs"] += 1
    run_number = pipeline_stats["total_runs"]
    start_time = datetime.now(timezone.utc)

    logger.info(f"{'='*60}")
    logger.info(f"  Pipeline Run #{run_number} — {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"{'='*60}")

    try:
        # Step 1: Fetch data from CoinGecko
        logger.info("Step 1: Fetching market data...")
        raw_data = client.fetch_tracked_coins()

        if not raw_data:
            logger.error("Failed to fetch data — skipping this cycle")
            pipeline_stats["failed_runs"] += 1
            return

        logger.info(f"  Fetched {len(raw_data)} coins")

        # Step 2: Clean and transform
        logger.info("Step 2: Cleaning data...")
        df = cleaner.clean_market_data(raw_data)

        if df is None or df.empty:
            logger.error("Data cleaning produced no valid records — skipping")
            pipeline_stats["failed_runs"] += 1
            return

        logger.info(f"  Cleaned {len(df)} records")

        # Step 3: Store in database
        logger.info("Step 3: Storing in PostgreSQL...")
        with get_session() as session:
            inserted, skipped = insert_market_data(session, df)

        pipeline_stats["successful_runs"] += 1
        pipeline_stats["total_records_inserted"] += inserted

        # Calculate elapsed time
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Log summary
        logger.info(f"  ✓ Inserted: {inserted} | Skipped: {skipped} | Time: {elapsed:.1f}s")

        # Show quick price snapshot
        for _, row in df.iterrows():
            change = row.get("price_change_pct_24h", 0)
            arrow = "▲" if change >= 0 else "▼"
            logger.info(
                f"    {row['name']:<12} ${row['current_price']:>10,.2f}  {arrow} {change:>+.2f}%"
            )

        # Periodic stats report (every 10 runs)
        if run_number % 10 == 0:
            _log_pipeline_stats()

        # Log total DB records
        total_records = CryptoAnalytics.get_record_count()
        logger.info(f"  Total records in database: {total_records:,}")

    except Exception as e:
        pipeline_stats["failed_runs"] += 1
        logger.error(f"Pipeline run #{run_number} failed: {e}", exc_info=True)


def _log_pipeline_stats():
    """Log cumulative pipeline statistics."""
    stats = pipeline_stats
    success_rate = (
        (stats["successful_runs"] / stats["total_runs"] * 100)
        if stats["total_runs"] > 0 else 0
    )

    logger.info(f"\n{'='*60}")
    logger.info(f"  PIPELINE STATS (since {stats['started_at']})")
    logger.info(f"{'='*60}")
    logger.info(f"  Total runs       : {stats['total_runs']}")
    logger.info(f"  Successful       : {stats['successful_runs']}")
    logger.info(f"  Failed           : {stats['failed_runs']}")
    logger.info(f"  Success rate     : {success_rate:.1f}%")
    logger.info(f"  Records inserted : {stats['total_records_inserted']:,}")
    logger.info(f"{'='*60}\n")


def _job_listener(event):
    """Listener for scheduler job events."""
    if event.exception:
        logger.error(f"Scheduled job failed with exception: {event.exception}")
    else:
        next_run = datetime.now(timezone.utc)
        logger.debug(f"Job completed. Next run in {settings.FETCH_INTERVAL}s")


def start_scheduler():
    """
    Start the automated ingestion scheduler.
    Runs the pipeline at the configured interval (FETCH_INTERVAL in .env).
    Gracefully handles shutdown via Ctrl+C.
    """
    logger.info("=" * 60)
    logger.info("  Starting Automated Ingestion Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Coins     : {', '.join(settings.COINS)}")
    logger.info(f"  Interval  : {settings.FETCH_INTERVAL} seconds ({settings.FETCH_INTERVAL/60:.1f} min)")
    logger.info(f"  Database  : Connected")
    logger.info("=" * 60)

    # Verify prerequisites
    logger.info("Verifying database connection...")
    if not test_connection():
        logger.error("Database connection failed. Run 'python main.py --init-db' first.")
        sys.exit(1)

    # Ensure tables exist
    init_database()

    # Record start time
    pipeline_stats["started_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Create scheduler
    scheduler = BlockingScheduler()

    # Add the ingestion job
    scheduler.add_job(
        run_ingestion_pipeline,
        trigger=IntervalTrigger(seconds=settings.FETCH_INTERVAL),
        id="crypto_ingestion",
        name="Crypto Market Data Ingestion",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,    # Combine missed runs into one
    )

    # Add event listener
    scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # Run immediately on start, then on schedule
    logger.info("Running initial ingestion cycle...")
    run_ingestion_pipeline()

    logger.info(f"\nScheduler started. Next run in {settings.FETCH_INTERVAL}s.")
    logger.info("Press Ctrl+C to stop.\n")

    # Graceful shutdown handler
    def shutdown(signum, frame):
        logger.info("\nShutdown signal received...")
        _log_pipeline_stats()
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped. Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nScheduler stopped by user.")
        _log_pipeline_stats()
