"""End-to-end tests for all fetchers (Hyperliquid, dYdX, GMX, Drift).

Behavior:
- Uses a temporary SQLite DB `test_trading_volume.db`
- Calls each fetcher with a known address from environment variables
- Validates that trades are inserted, no duplicate trade_ids, positives notionals
- Ensures required fields are non-null
- Prints PASS/FAIL for each fetcher
- Cleans up the test DB after each test

Addresses:
- Set these environment variables before running to ensure non-empty results:
  - HYPERLIQUID_ADDRESS
  - DYDX_ADDRESS
  - GMX_ACCOUNT
  - DRIFT_ACCOUNT

Run:
- python3 volume_flex_card/test_all_fetchers.py
"""

import os
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Tuple


TEST_DB = Path("test_trading_volume.db")


def print_result(name: str, passed: bool, reason: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    msg = f"{name}: {status}"
    if not passed and reason:
        msg += f" - {reason}"
    print(msg)


def setup_test_db() -> None:
    # Ensure config picks up the test DB before importing modules
    os.environ["DB_PATH"] = str(TEST_DB)

    # Remove leftover DB if present
    if TEST_DB.exists():
        try:
            TEST_DB.unlink()
        except Exception:
            pass

    # Initialize schema
    from database_setup import init_db

    init_db(TEST_DB)


def cleanup_test_db() -> None:
    try:
        if TEST_DB.exists():
            TEST_DB.unlink()
    except Exception:
        pass


def validate_db_rows(conn: sqlite3.Connection, wallet: str, exchange: str) -> Tuple[bool, str]:
    cur = conn.cursor()

    # Count inserted rows
    cur.execute(
        "SELECT COUNT(*) FROM trades WHERE wallet_address = ? AND exchange = ?",
        (wallet, exchange),
    )
    total = int(cur.fetchone()[0])
    if total <= 0:
        return False, "No trades inserted"

    # No duplicate trade_ids
    cur.execute(
        "SELECT COUNT(DISTINCT trade_id) FROM trades WHERE wallet_address = ? AND exchange = ?",
        (wallet, exchange),
    )
    distinct_ids = int(cur.fetchone()[0])
    if distinct_ids != total:
        return False, "Duplicate trade_id detected"

    # Validate absolute notional logic and signed values handling
    cur.execute(
        "SELECT price, size, notional_value, side FROM trades WHERE wallet_address = ? AND exchange = ?",
        (wallet, exchange),
    )
    rows = cur.fetchall()
    # Track specific violations to provide a clear reason
    for (price, size, notional_value, side) in rows:
        # Ensure price and size are non-zero
        if float(price) == 0.0 or float(size) == 0.0:
            return False, "Zero price or size present"
        # Ensure notional_value exists and is non-negative
        if not isinstance(notional_value, (int, float)):
            return False, "notional_value is not numeric"
        if float(notional_value) < 0.0:
            return False, "Negative notional_value present"
        # For non-Drift, validate notional_value ~= abs(price * size)
        if exchange != "Drift":
            computed = abs(float(price) * float(size))
            if abs(float(notional_value) - computed) >= 0.01:
                return False, f"Notional mismatch: {notional_value} vs {computed}"
        # Verify sells have positive (non-negative) notional values
        try:
            if isinstance(side, str) and side.lower() == "sell" and float(notional_value) < 0.0:
                return False, "Sell trade has negative notional_value"
        except Exception:
            # If side is malformed, ignore this specific check
            pass

    # Required fields are non-null
    cur.execute(
        (
            "SELECT COUNT(*) FROM trades WHERE wallet_address = ? AND exchange = ? "
            "AND (wallet_address IS NULL OR exchange IS NULL OR market IS NULL OR side IS NULL "
            "OR price IS NULL OR size IS NULL OR timestamp IS NULL OR trade_id IS NULL)"
        ),
        (wallet, exchange),
    )
    nulls = int(cur.fetchone()[0])
    if nulls != 0:
        return False, "Null fields detected"

    return True, ""


def run_hyperliquid_test() -> None:
    setup_test_db()
    try:
        from fetch_hyperliquid import ensure_db, fetch_user_fills, normalize_trade, insert_trades

        wallet = os.environ.get("HYPERLIQUID_ADDRESS", "").strip()
        if not wallet:
            print_result("Hyperliquid", False, "HYPERLIQUID_ADDRESS not set")
            return

        ensure_db()
        raw = fetch_user_fills(wallet)
        normalized = [normalize_trade(r, wallet) for r in raw if isinstance(r, dict)]
        inserted = insert_trades(normalized)

        conn = sqlite3.connect(str(TEST_DB))
        try:
            passed, reason = validate_db_rows(conn, wallet, "Hyperliquid")
            if inserted <= 0:
                passed = False
                reason = reason or "No trades inserted"
            print_result("Hyperliquid", passed, reason)
        finally:
            conn.close()
    finally:
        cleanup_test_db()


def run_dydx_test() -> None:
    setup_test_db()
    try:
        from fetch_dydx import ensure_db, fetch_fills, normalize_fill, insert_trades

        address = os.environ.get("DYDX_ADDRESS", "").strip()
        if not address:
            print_result("dYdX", False, "DYDX_ADDRESS not set")
            return

        ensure_db()
        raw = fetch_fills(address, subaccount=0, limit=100)
        normalized = [normalize_fill(r, address) for r in raw if isinstance(r, dict)]
        inserted = insert_trades(normalized)

        conn = sqlite3.connect(str(TEST_DB))
        try:
            passed, reason = validate_db_rows(conn, address, "dYdX")
            if inserted <= 0:
                passed = False
                reason = reason or "No trades inserted"
            print_result("dYdX", passed, reason)
        finally:
            conn.close()
    finally:
        cleanup_test_db()


def run_gmx_test() -> None:
    setup_test_db()
    try:
        from fetch_gmx import ensure_db, fetch_trades, normalize_trade, insert_trades

        account = os.environ.get("GMX_ACCOUNT", "").strip()
        if not account:
            print_result("GMX_Arbitrum", False, "GMX_ACCOUNT not set")
            return

        ensure_db()
        raw = fetch_trades(account, limit=200)
        normalized = [normalize_trade(r, account) for r in raw if isinstance(r, dict)]
        inserted = insert_trades(normalized)

        conn = sqlite3.connect(str(TEST_DB))
        try:
            passed, reason = validate_db_rows(conn, account.lower(), "GMX_Arbitrum")
            if inserted <= 0:
                passed = False
                reason = reason or "No trades inserted"
            print_result("GMX_Arbitrum", passed, reason)
        finally:
            conn.close()
    finally:
        cleanup_test_db()


def run_drift_test() -> None:
    setup_test_db()
    try:
        from fetch_drift import ensure_db, fetch_trades, normalize_trade, insert_trades

        account = os.environ.get("DRIFT_ACCOUNT", "").strip()
        if not account:
            print_result("Drift", False, "DRIFT_ACCOUNT not set")
            return

        ensure_db()
        raw = fetch_trades(account)
        normalized = [normalize_trade(r, account) for r in raw if isinstance(r, dict)]
        inserted = insert_trades(normalized)

        conn = sqlite3.connect(str(TEST_DB))
        try:
            passed, reason = validate_db_rows(conn, account, "Drift")
            if inserted <= 0:
                passed = False
                reason = reason or "No trades inserted"
            print_result("Drift", passed, reason)
        finally:
            conn.close()
    finally:
        cleanup_test_db()


def main() -> None:
    # Execute each fetcher test independently, printing PASS/FAIL
    run_hyperliquid_test()
    run_dydx_test()
    run_gmx_test()
    run_drift_test()


if __name__ == "__main__":
    main()