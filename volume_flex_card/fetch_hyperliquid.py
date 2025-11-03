"""Fetch user fills from Hyperliquid and insert into SQLite.

Example usage:
    python fetch_hyperliquid.py --address 0x123...

Steps:
- POST to Hyperliquid API with wallet address
- Parse returned trades
- Normalize fields and compute notional
- Insert into `trades` table using INSERT OR IGNORE
- Print stats: fetched count, inserted count, total notional volume
"""

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from logging_utils import get_logger, request_with_retries
import config


DB_PATH = Path(config.DB_PATH)
HYPERLIQUID_URL = config.HYPERLIQUID_URL
logger = get_logger(__name__)


def ensure_db() -> None:
    """Ensure the database exists by attempting to initialize tables."""
    if not DB_PATH.exists():
        try:
            # Import local init_db if available
            from database_setup import init_db  # type: ignore

            init_db()
        except Exception as e:  # pragma: no cover
            logger.warning(f"Could not initialize database automatically: {e}")


def fetch_user_fills(wallet: str) -> List[Dict[str, Any]]:
    """Fetch trade fills from Hyperliquid for the given wallet with retries."""
    payload = {"type": "userFills", "user": wallet}
    headers = {"Content-Type": "application/json"}
    resp = request_with_retries(
        "POST",
        HYPERLIQUID_URL,
        json=payload,
        headers=headers,
        timeout=config.REQUEST_TIMEOUT,
        retries=config.MAX_RETRIES,
        backoff_base=config.RETRY_DELAY,
        logger=logger,
    )
    if resp is None:
        return []
    try:
        data = resp.json()
    except ValueError:
        logger.error("Failed to parse JSON response from Hyperliquid.")
        return []
    if not isinstance(data, list):
        logger.warning("Unexpected response format from Hyperliquid (expected list).")
        return []
    return data


def normalize_trade(raw: Dict[str, Any], wallet_address: Optional[str] = None) -> Dict[str, Any]:
    """Normalize a raw Hyperliquid trade into our schema fields.

    If `wallet_address` is provided, it will be set on the normalized trade.
    """
    coin = raw.get("coin")
    px = float(raw.get("px", 0) or 0)
    sz = float(raw.get("sz", 0) or 0)
    side_raw = raw.get("side")
    side = "buy" if side_raw == "B" else ("sell" if side_raw == "A" else str(side_raw))
    ts = int(raw.get("time", 0) or 0)
    tid = raw.get("tid")
    trade_id = f"hl_{tid}" if tid is not None else None
    # Absolute notional from local price*size
    notional_value = abs(px * sz)

    return {
        "wallet_address": wallet_address or "",
        "exchange": "Hyperliquid",
        "market": coin,
        "side": side,
        "price": px,
        "size": sz,
        "timestamp": ts,
        "trade_id": trade_id,
        "notional_value": notional_value,
    }


def insert_trades(trades: List[Dict[str, Any]]) -> int:
    """Insert trades into SQLite using INSERT OR IGNORE; return inserted count.

    Stores `notional_value` explicitly as an absolute amount (always positive).
    """
    inserted = 0
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        for tr in trades:
            if not tr.get("trade_id"):
                # Skip rows without a unique id
                logger.warning("Skipping trade without trade_id")
                continue
            try:
                cur.execute(
                    (
                        "INSERT OR IGNORE INTO trades "
                        "(wallet_address, exchange, market, side, price, size, notional_value, timestamp, trade_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    ),
                    (
                        tr["wallet_address"],
                        tr["exchange"],
                        tr["market"],
                        tr["side"],
                        tr["price"],
                        tr["size"],
                        tr.get("notional_value", abs(float(tr["price"]) * float(tr["size"]))),
                        tr["timestamp"],
                        tr["trade_id"],
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            except sqlite3.Error as e:
                logger.error(f"SQLite insert error for trade {tr.get('trade_id')}: {e}")
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"SQLite connection error: {e}")
    finally:
        try:
            conn.close()  # type: ignore
        except Exception:
            pass
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Hyperliquid user fills and insert into SQLite")
    parser.add_argument("--address", help="Wallet address to fetch", default=os.environ.get("HYPERLIQUID_ADDRESS", ""))
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        try:
            import logging

            logger.setLevel(logging.DEBUG)
        except Exception:
            pass

    if not args.address:
        logger.error("No address provided. Use --address or set HYPERLIQUID_ADDRESS.")
        return

    ensure_db()
    trades_raw = fetch_user_fills(args.address)
    fetched_count = len(trades_raw)
    normalized = [normalize_trade(t, args.address) for t in trades_raw if isinstance(t, dict)]
    total_volume = sum(t.get("notional_value", 0.0) for t in normalized)
    inserted_count = insert_trades(normalized)

    logger.info(f"Fetched trades: {fetched_count}")
    logger.info(f"Inserted new trades: {inserted_count}")
    logger.info(f"Total volume: {total_volume:.8f}")


if __name__ == "__main__":
    main()