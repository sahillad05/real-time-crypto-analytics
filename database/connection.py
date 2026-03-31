"""
Database Connection Manager
============================
Manages PostgreSQL connections using SQLAlchemy.
Provides session management and engine creation.
"""

import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase

from config.settings import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


# Create the database engine
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before using them
    echo=False,  # Set to True for SQL debugging
)

# Session factory
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_session() -> Session:
    """
    Context manager for database sessions.
    Automatically commits on success and rolls back on failure.

    Usage:
        with get_session() as session:
            session.add(record)
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}", exc_info=True)
        raise
    finally:
        session.close()


def init_database():
    """
    Create all tables defined in ORM models.
    Safe to call multiple times — only creates tables that don't exist.
    """
    from database.models import CryptoMarketData  # noqa: F401

    logger.info("Initializing database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully.")


def test_connection() -> bool:
    """
    Test the database connection.

    Returns:
        True if connection is successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("Database connection successful.")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def get_table_stats() -> dict:
    """
    Get basic statistics about the database tables.

    Returns:
        Dictionary with table stats (row counts, etc.)
    """
    stats = {}
    try:
        with get_session() as session:
            result = session.execute(
                text("SELECT COUNT(*) FROM crypto_market_data")
            )
            stats["crypto_market_data"] = {
                "row_count": result.scalar()
            }

            # Get date range
            result = session.execute(
                text("""
                    SELECT 
                        MIN(ingested_at) as first_record,
                        MAX(ingested_at) as last_record
                    FROM crypto_market_data
                """)
            )
            row = result.fetchone()
            if row:
                stats["crypto_market_data"]["first_record"] = str(row[0])
                stats["crypto_market_data"]["last_record"] = str(row[1])

    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        stats["error"] = str(e)

    return stats
