"""Fetch user trades from Drift Protocol (Solana) and insert into SQLite.

Endpoint:
- GET https://data.api.drift.trade/user/{accountId}/trades

Behavior:
- Takes a Solana public key (base58, no 0x) as input
- Parses JSON (expects { success: true, records: [...] })
- Extracts: ts, marketIndex, marketType, baseAssetAmount, quoteAssetAmount, price, side
- notional_value = abs(quoteAssetAmount) (divide by 1e6 if provided in micro units)
- trade_id = "drift_" + ts + "_" + marketIndex
- Inserts into SQLite `trades` with exchange = "Drift"
- Prints summary (fetched, inserted, total volume)
"""

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import requests
from logging_utils import get_logger, request_with_retries
import config


DB_PATH = Path(config.DB_PATH)
DRIFT_URL_TEMPLATE = config.DRIFT_URL
logger = get_logger(__name__)


def ensure_db() -> None:
    """Ensure the database exists with required tables."""
    if not DB_PATH.exists():
        try:
            from database_setup import init_db  # type: ignore

            init_db()
        except Exception as e:
            logger.warning(f"Could not initialize database automatically: {e}")


def _parse_float(value: Any) -> float:
    """Best-effort float parsing from drift response values."""
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
    except Exception:
        return 0.0
    return 0.0


def _normalize_notional(quote_amount: Any) -> float:
    """Return absolute notional value.

    Drift responses can return quote amounts in micro-units (1e6).
    Example: a `quoteAssetAmount` of 50,000,000 represents $50 USD.
    This function converts integer-like values to standard units by dividing
    by 1e6 and returns the absolute value.

    Heuristic details:
    - If the input is a string without a decimal point, treat as micro-units.
    - If the input is an integer, treat as micro-units.
    - Otherwise, parse as a float and return absolute value as-is.

    Source: Drift protocol data formats (confirm against latest docs).
    """
    # Raw to float
    if isinstance(quote_amount, str):
        s = quote_amount
        try:
            v = float(s)
        except Exception:
            return 0.0
        if "." not in s:
            # Likely micro units
            return abs(v) / 1_000_000.0
        return abs(v)
    elif isinstance(quote_amount, (int, float)):
        v = float(quote_amount)
        # Assume integers are micro units
        if isinstance(quote_amount, int):
            return abs(v) / 1_000_000.0
        return abs(v)
    return 0.0


def fetch_trades(account_id: str) -> List[Dict[str, Any]]:
    url = DRIFT_URL_TEMPLATE.format(accountId=account_id)
    resp = request_with_retries(
        "GET",
        url,
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
        logger.error("Failed to parse JSON response from Drift API.")
        return []

    # Handle success flag and records
    if not isinstance(data, dict):
        logger.warning("Unexpected response format from Drift API.")
        return []

    success = data.get("success")
    if success is not True:
        logger.warning(f"Drift API returned success=false or missing. Raw: {data}")
        return []

    records = data.get("records", [])
    if not isinstance(records, list):
        logger.warning("Drift API response missing 'records' list.")
        return []

    return records


def normalize_trade(raw: Dict[str, Any], wallet_address: str) -> Dict[str, Any]:
    ts = int(_parse_float(raw.get("ts")))
    market_index = str(raw.get("marketIndex", ""))
    market_type = str(raw.get("marketType", ""))
    price = _parse_float(raw.get("price"))
    size = _parse_float(raw.get("baseAssetAmount"))
    # Example conversion: quoteAssetAmount=50000000 -> 50.0 USD (50000000 / 1e6)
    notional = _normalize_notional(raw.get("quoteAssetAmount"))
    # Warn if notional appears unreasonably large or suspiciously small
    try:
        if notional > 1e12 or (0 < notional < 0.01):
            logger.warning(
                f"Drift notional outlier: {notional:.6f} (quoteAssetAmount={raw.get('quoteAssetAmount')}, ts={ts}, marketIndex={market_index})"
            )
    except Exception:
        pass
    side = str(raw.get("side", ""))

    market = f"{market_type}:{market_index}" if market_type else market_index
    trade_id = f"drift_{ts}_{market_index}" if ts and market_index else None

    return {
        "wallet_address": wallet_address,
        "exchange": "Drift",
        "market": market,
        "side": side,
        "price": price,
        "size": size,
        "timestamp": ts,
        "trade_id": trade_id,
        "notional_value": notional,
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
                        tr.get("notional_value", 0.0),
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

    parser = argparse.ArgumentParser(description="Fetch Drift trades and insert into SQLite")
    parser.add_argument(
        "--account",
        help="Solana public key (base58, no 0x)",
        default=os.environ.get("DRIFT_ACCOUNT", ""),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max trades to process locally from response (default: 1000)",
    )
    args = parser.parse_args()

    if not args.account:
        logger.error("No Drift account provided. Use --account or set DRIFT_ACCOUNT.")
        return

    raw_trades = fetch_trades(args.account)
    fetched_count = len(raw_trades)
    # Locally limit processed trades if desired
    if args.limit and fetched_count > args.limit:
        raw_trades = raw_trades[: args.limit]

    normalized = [normalize_trade(t, args.account) for t in raw_trades if isinstance(t, dict)]
    total_volume = sum(t.get("notional_value", 0.0) for t in normalized)
    inserted_count = insert_trades(normalized)

    logger.info(f"Fetched trades: {fetched_count}")
    logger.info(f"Processed trades: {len(normalized)}")
    logger.info(f"Inserted new trades: {inserted_count}")
    logger.info(f"Total volume: {total_volume:.8f}")


if __name__ == "__main__":
    main()