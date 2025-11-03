"""Simple tests for insert_trades feature using a temporary SQLite DB.

This script:
- Sets a temporary DB path via environment variable
- Initializes the DB schema
- Inserts sample trades using fetch_hyperliquid.insert_trades
- Verifies the database record counts
- Prints PASS/FAIL for each test case

Run with: python3 volume_flex_card/test_insert_trades.py
"""

import os
import sqlite3
from pathlib import Path


def print_result(name: str, passed: bool) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"{name}: {status}")


def main() -> None:
    # Use a temporary database for testing
    test_db = Path("test_trading_volume.db")
    if test_db.exists():
        try:
            test_db.unlink()
        except Exception:
            pass

    # Ensure config picks up the test DB before importing modules
    os.environ["DB_PATH"] = str(test_db)

    from database_setup import init_db
    from fetch_hyperliquid import insert_trades as hl_insert_trades

    # Initialize schema
    init_db(test_db)

    # Sample trades (two unique records)
    sample_trades = [
        {
            "wallet_address": "0xabc",
            "exchange": "Hyperliquid",
            "market": "ETH",
            "side": "buy",
            "price": 2500.0,
            "size": 0.2,
            "timestamp": 1700000000,
            "trade_id": "test_hl_1",
        },
        {
            "wallet_address": "0xabc",
            "exchange": "Hyperliquid",
            "market": "BTC",
            "side": "sell",
            "price": 30000.0,
            "size": 0.01,
            "timestamp": 1700000100,
            "trade_id": "test_hl_2",
        },
    ]

    # Test case 1: insert new trades
    try:
        inserted = hl_insert_trades(sample_trades)
        assert inserted == len(sample_trades), "Inserted count should equal sample length"

        # Verify DB row count
        conn = sqlite3.connect(str(test_db))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM trades")
            count = cur.fetchone()[0]
            assert count == len(sample_trades), "DB should contain two inserted trades"
        finally:
            conn.close()
        print_result("Insert new trades", True)
    except AssertionError as e:
        print_result("Insert new trades", False)
        print(f"AssertionError: {e}")

    # Test case 2: duplicate insert should be ignored
    try:
        inserted_again = hl_insert_trades(sample_trades)
        assert inserted_again == 0, "Duplicate inserts should be ignored (0 new rows)"

        conn = sqlite3.connect(str(test_db))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM trades")
            count_after = cur.fetchone()[0]
            assert count_after == len(sample_trades), "DB count should remain unchanged after duplicate insert"
        finally:
            conn.close()
        print_result("Ignore duplicates", True)
    except AssertionError as e:
        print_result("Ignore duplicates", False)
        print(f"AssertionError: {e}")


if __name__ == "__main__":
    main()