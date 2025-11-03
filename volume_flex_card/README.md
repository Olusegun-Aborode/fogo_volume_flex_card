# Fogo: volume_flex_card

Trading volume aggregator for crypto wallets.

This project scaffolds a minimal Python setup to:
- fetch wallet trading volume data from external sources (e.g., Hyperliquid),
- store volumes locally in a simple SQLite database, and
- query aggregated volume metrics.

## Getting Started

1. Create and activate a virtual environment.
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Initialize the database in Python:
   ```python
   from database_setup import init_db
   init_db()
   ```

## Project Structure

```
volume_flex_card/
├── fetch_hyperliquid.py    # Placeholder for fetching external volume data
├── database_setup.py       # SQLite initialization and basic schema
├── query_volume.py         # Simple aggregation queries on stored data
├── requirements.txt        # Project dependencies
└── README.md               # Project overview
```

Note: API integrations and production database choices are left for future implementation.

## Database Migration: Absolute Notional Values

As of this update, the `trades.notional_value` column is now a regular `REAL` field that stores the absolute notional value (always non-negative). Previously, the column could be computed from `price * size`. This change ensures consistent handling across different exchanges and side conventions.

If you have an existing database, migrate with the following steps in SQLite:

1. Backup your database file.
2. In a SQLite shell, run:
   ```sql
   ALTER TABLE trades RENAME TO trades_old;
   ```
3. Recreate the schema from the project by running `init_db()` in Python to create the new `trades` table.
4. Copy data into the new table while computing absolute notionals:
   ```sql
   INSERT INTO trades (wallet_address, exchange, market, side, price, size, notional_value, timestamp, trade_id)
   SELECT wallet_address, exchange, market, side, price, size, ABS(price * size) AS notional_value, timestamp, trade_id
   FROM trades_old;
   ```
5. Drop the old table when satisfied:
   ```sql
   DROP TABLE trades_old;
   ```

All fetchers now insert `notional_value` explicitly as an absolute amount. Aggregation queries (`query_volume.py`) use this column and verify that no negative values exist.