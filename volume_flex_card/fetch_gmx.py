"""Fetch trades from GMX Arbitrum GraphQL and insert into SQLite.

Endpoint:
- https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql

Flow:
- POST GraphQL query with variables (account lowercased, limit)
- Parse data.trades
- Map fields to local schema and compute notional
- Insert into `trades` using INSERT OR IGNORE with exchange = "GMX_Arbitrum"
- Print summary (fetched, inserted, total volume)
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
GMX_URL = config.GMX_ARBITRUM_URL
logger = get_logger(__name__)


GRAPHQL_QUERY = (
    """
    query UserTrades($account: String!, $limit: Int! ) {
      trades(
        where: { account: $account }
        limit: $limit
        orderBy: timestamp
        orderDirection: desc
      ) {
        id
        account
        marketAddress
        sizeInUsd
        sizeInTokens
        executionPrice
        timestamp
      }
    }
    """
).strip()


def ensure_db() -> None:
    if not DB_PATH.exists():
        try:
            from database_setup import init_db  # type: ignore

            init_db()
        except Exception as e:
            logger.warning(f"Could not initialize database automatically: {e}")


def fetch_trades(account: str, limit: int = 1000) -> List[Dict[str, Any]]:
    variables = {"account": account.lower(), "limit": int(limit)}
    headers = {"Content-Type": "application/json"}
    payload = {"query": GRAPHQL_QUERY, "variables": variables}
    resp = request_with_retries(
        "POST",
        GMX_URL,
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
        logger.error("Failed to parse JSON response from GMX GraphQL.")
        return []
    if isinstance(data, dict):
        node = data.get("data", {})
        trades = node.get("trades", [])
        if isinstance(trades, list):
            return trades
    logger.warning("Unexpected response format from GMX GraphQL (expected data.trades array).")
    return []


def normalize_trade(raw: Dict[str, Any], wallet_address: str) -> Dict[str, Any]:
    try:
        tid = raw.get("id")
        market = raw.get("marketAddress")
        price = float(raw.get("executionPrice", 0) or 0)
        size = float(raw.get("sizeInTokens", 0) or 0)
        # Absolute notional computed locally from price*size
        notional = abs(price * size)
        ts = int(raw.get("timestamp", 0) or 0)

        # Heuristic: infer side from the sign of sizeInTokens
        # - sizeInTokens > 0 => buy
        # - sizeInTokens < 0 => sell
        # - sizeInTokens == 0 => unknown (log a warning)
        side = "buy" if size > 0 else ("sell" if size < 0 else "unknown")
        if side == "unknown":
            logger.warning(f"GMX trade has sizeInTokens == 0; side unknown. id: {tid}")

        return {
            "wallet_address": wallet_address.lower(),
            "exchange": "GMX_Arbitrum",
            "market": market,
            "side": side,
            "price": price,
            "size": size,
            "timestamp": ts,
            "trade_id": f"gmx_arb_{tid}" if tid is not None else None,
            "notional_value": notional,
        }
    except Exception:
        size = float(raw.get("sizeInTokens", 0) or 0)
        side = "buy" if size > 0 else ("sell" if size < 0 else "unknown")
        if side == "unknown":
            logger.warning(f"GMX trade has sizeInTokens == 0; side unknown. id: {raw.get('id')}")
        return {
            "wallet_address": wallet_address.lower(),
            "exchange": "GMX_Arbitrum",
            "market": raw.get("marketAddress"),
            "side": side,
            "price": float(raw.get("executionPrice", 0) or 0),
            "size": float(raw.get("sizeInTokens", 0) or 0),
            "timestamp": int(raw.get("timestamp", 0) or 0),
            "trade_id": f"gmx_arb_{raw.get('id')}" if raw.get("id") is not None else None,
            # Absolute notional computed locally from price*size
            "notional_value": abs(float(raw.get("executionPrice", 0) or 0) * float(raw.get("sizeInTokens", 0) or 0)),
        }


def insert_trades(trades: List[Dict[str, Any]]) -> int:
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
            conn.close()
        except Exception:
            pass
    return inserted


def main() -> None:
    ensure_db()

    parser = argparse.ArgumentParser(description="Fetch GMX Arbitrum trades and insert into SQLite")
    parser.add_argument("--account", help="GMX account address (lowercased)", default=os.environ.get("GMX_ACCOUNT", ""))
    parser.add_argument("--limit", type=int, default=1000, help="Max trades to fetch (default: 1000)")
    args = parser.parse_args()

    if not args.account:
        logger.error("No GMX account provided. Use --account or set GMX_ACCOUNT.")
        return

    raw_trades = fetch_trades(args.account, limit=args.limit)
    fetched_count = len(raw_trades)
    normalized = [normalize_trade(t, args.account) for t in raw_trades if isinstance(t, dict)]
    total_volume = sum(t.get("notional_value", 0.0) for t in normalized)
    inserted_count = insert_trades(normalized)

    logger.info(f"Fetched trades: {fetched_count}")
    logger.info(f"Inserted new trades: {inserted_count}")
    logger.info(f"Total volume: {total_volume:.8f}")


if __name__ == "__main__":
    main()