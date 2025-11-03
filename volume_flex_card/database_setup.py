"""Database setup script for trading volume aggregator.

Creates a SQLite database `trading_volume.db` with tables:
- trades: stores individual trade records with absolute notional_value (always positive)
- wallets: stores wallet addresses and metadata
"""

import sqlite3
from pathlib import Path
from . import config


DB_PATH = Path(config.DB_PATH)


def init_db(db_path: Path = DB_PATH) -> None:
    """Initialize the SQLite database and create required tables."""

    conn = sqlite3.connect(str(db_path))
    try:
        # Create trades table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT,
                exchange TEXT,
                market TEXT,
                side TEXT,
                price REAL,
                size REAL,
                -- Absolute notional value (always positive)
                notional_value REAL,
                timestamp INTEGER,
                trade_id TEXT UNIQUE
            )
            """
        )

        # Create wallets table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT UNIQUE,
                chain TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(
        "Database setup complete: created tables 'trades' and 'wallets' in trading_volume.db"
    )