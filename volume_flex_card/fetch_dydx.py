"""Fetch fills from dYdX Indexer API and insert into SQLite.

Features:
- GET `https://indexer.dydx.trade/v4/fills` with address, subaccountNumber, limit
- Parse `fills` array, normalize fields, compute notional
- Insert into `trades` table using INSERT OR IGNORE with exchange = "dYdX"
- Print summary: fetched, inserted, total volume
"""

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from logging_utils import get_logger, request_with_retries
import config


DB_PATH = Path(config.DB_PATH)
DYDX_INDEXER_URL = config.DYDX_URL
logger = get_logger(__name__)


def ensure_db() -> None:
    """Ensure the database exists with required tables."""
    if not DB_PATH.exists():
        try:
            from database_setup import init_db  # type: ignore

            init_db()
        except Exception as e:  # pragma: no cover
            logger.warning(f"Could not initialize database automatically: {e}")


def iso_to_unix(ts: str) -> int:
    """Convert ISO timestamp to Unix seconds.

    Handles trailing 'Z' and microseconds. Returns 0 on failure.
    """
    if not ts:
        return 0
    try:
        # Replace trailing Z with explicit UTC offset to satisfy fromisoformat
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        return int(dt.timestamp())
    except Exception:
        # Fallback patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt = datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                continue
    return 0


def fetch_fills(address: str, subaccount: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
    """Fetch fills from dYdX indexer for the given address with retries."""
    params = {"address": address, "subaccountNumber": subaccount, "limit": limit}
    resp = request_with_retries(
        "GET",
        DYDX_INDEXER_URL,
        params=params,
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
        logger.error("Failed to parse JSON response from dYdX Indexer.")
        return []
    if isinstance(data, dict):
        fills = data.get("fills", [])
        if isinstance(fills, list):
            return fills
    logger.warning("Unexpected response format from dYdX Indexer (expected dict with 'fills').")
    return []


def normalize_fill(raw: Dict[str, Any], wallet_address: str) -> Dict[str, Any]:
    """Normalize a raw dYdX fill into our schema fields."""
    fid = raw.get("id")
    market = raw.get("market")
    side_raw = (raw.get("side") or "").upper()
    side = "buy" if side_raw == "BUY" else ("sell" if side_raw == "SELL" else side_raw.lower())
    px = float(raw.get("price", 0) or 0)
    sz = float(raw.get("size", 0) or 0)
    created_at = raw.get("createdAt")
    ts = iso_to_unix(str(created_at) if created_at is not None else "")
    # Absolute notional from local price*size
    notional_value = abs(px * sz)

    return {
        "wallet_address": wallet_address,
        "exchange": "dYdX",
        "market": market,
        "side": side,
        "price": px,
        "size": sz,
        "timestamp": ts,
        "trade_id": f"dydx_{fid}" if fid is not None else None,
        "notional_value": notional_value,
    }


def insert_trades(trades: List[Dict[str, Any]]) -> int:
    """Insert trades into SQLite using INSERT OR IGNORE; return inserted count."""
    inserted = 0
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        for tr in trades:
            if not tr.get("trade_id"):
                # Skip rows without a unique id
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
    ensure_db()

    parser = argparse.ArgumentParser(description="Fetch dYdX fills and insert into SQLite")
    parser.add_argument("--address", help="dYdX address", default=os.environ.get("DYDX_ADDRESS", ""))
    parser.add_argument("--subaccount", type=int, default=0, help="Subaccount number (default: 0)")
    parser.add_argument("--limit", type=int, default=100, help="Max fills to fetch (default: 100)")
    args = parser.parse_args()

    if not args.address:
        logger.error("No dYdX address provided. Use --address or set DYDX_ADDRESS.")
        return

    raw_fills = fetch_fills(args.address, subaccount=args.subaccount, limit=args.limit)
    fetched_count = len(raw_fills)
    normalized = [normalize_fill(f, args.address) for f in raw_fills if isinstance(f, dict)]
    total_volume = sum(t.get("notional_value", 0.0) for t in normalized)
    inserted_count = insert_trades(normalized)

    logger.info(f"Fetched fills: {fetched_count}")
    logger.info(f"Inserted new trades: {inserted_count}")
    logger.info(f"Total volume: {total_volume:.8f}")


if __name__ == "__main__":
    main()