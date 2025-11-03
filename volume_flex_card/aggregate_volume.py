"""Aggregate trading volume across multiple exchanges for a set of wallets.

Reads wallets from wallets.json, inserts wallets into DB, calls per-chain
fetchers (Hyperliquid/dYdX/GMX for EVM; Drift for Solana), then prints
aggregated results using the query_volume logic and writes JSON output
to volume_card_output.json.
"""

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple
import time
import config
from logging_utils import get_logger
from cache import get_cached_volume, cache_volume

# Local imports (modular usage of existing fetchers and query helpers)
from database_setup import init_db
from query_volume import connect, fetch_overall, fetch_by_exchange, fetch_by_wallet

# EVM fetchers
from fetch_hyperliquid import (
    fetch_user_fills as hl_fetch_user_fills,
    normalize_trade as hl_normalize_trade,
    insert_trades as hl_insert_trades,
)
from fetch_dydx import (
    fetch_fills as dydx_fetch_fills,
    normalize_fill as dydx_normalize_fill,
    insert_trades as dydx_insert_trades,
)
from fetch_gmx import (
    fetch_trades as gmx_fetch_trades,
    normalize_trade as gmx_normalize_trade,
    insert_trades as gmx_insert_trades,
)

# Solana fetcher
from fetch_drift import (
    fetch_trades as drift_fetch_trades,
    normalize_trade as drift_normalize_trade,
    insert_trades as drift_insert_trades,
)


DB_PATH = Path(config.DB_PATH)
WALLETS_JSON = Path("wallets.json")
OUTPUT_JSON = Path("volume_card_output.json")

logger = get_logger(__name__)


def ensure_db() -> None:
    """Ensure database exists with required tables."""
    if not DB_PATH.exists():
        init_db(DB_PATH)


def load_wallets(config_path: Path = WALLETS_JSON) -> List[Dict[str, Any]]:
    """Load wallets configuration from JSON file."""
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return []
    try:
        with config_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        wallets = data.get("wallets", [])
        if not isinstance(wallets, list):
            print("Invalid wallets.json format: 'wallets' must be a list")
            return []
        return wallets
    except Exception as e:
        print(f"Failed to read wallets.json: {e}")
        return []


def insert_wallet(address: str, chain: str) -> None:
    """Insert or update wallet metadata using UPSERT.

    Uses: INSERT ... ON CONFLICT(address) DO UPDATE SET chain=excluded.chain
    Logs whether the wallet was inserted, updated, or unchanged.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()

        # Check existing chain to determine log message
        cur.execute("SELECT chain FROM wallets WHERE address = ?", (address,))
        row = cur.fetchone()
        if row is None:
            logger.info(f"Inserted wallet {address} with chain {chain}")
        else:
            existing_chain = str(row[0])
            if existing_chain != chain:
                logger.info(
                    f"Updated wallet {address} chain {existing_chain} -> {chain}"
                )
            else:
                logger.info(f"Wallet {address} chain unchanged ({chain})")

        # UPSERT ensures metadata is stored/updated correctly
        cur.execute(
            (
                "INSERT INTO wallets (address, chain) "
                "VALUES (?, ?) "
                "ON CONFLICT(address) DO UPDATE SET chain=excluded.chain"
            ),
            (address, chain),
        )
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"SQLite error inserting/updating wallet {address}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_evm_wallet(address: str) -> Dict[str, Any]:
    """Process an EVM wallet across Hyperliquid, dYdX, and GMX."""
    # Check cache first
    cached = get_cached_volume(address)
    if cached and isinstance(cached, dict):
        return {
            "address": address,
            "chain": "EVM",
            "exchanges": cached.get("breakdown", {}),
            "cached": True,
            "cached_timestamp": cached.get("timestamp"),
            "cached_total_volume": cached.get("total_volume"),
        }

    summary = {
        "address": address,
        "chain": "EVM",
        "exchanges": {},
    }

    # Hyperliquid
    hl_raw = hl_fetch_user_fills(address)
    hl_norm = [hl_normalize_trade(t) for t in hl_raw if isinstance(t, dict)]
    # Override wallet_address from constant to actual wallet
    for tr in hl_norm:
        tr["wallet_address"] = address
    hl_vol = sum(tr.get("notional_value", 0.0) for tr in hl_norm)
    hl_inserted = hl_insert_trades(hl_norm)
    summary["exchanges"]["Hyperliquid"] = {
        "fetched": len(hl_raw),
        "inserted": hl_inserted,
        "volume": hl_vol,
    }

    # dYdX (v4 indexer)
    dydx_raw = dydx_fetch_fills(address, subaccount=0, limit=100)
    dydx_norm = [dydx_normalize_fill(f, address) for f in dydx_raw if isinstance(f, dict)]
    dydx_vol = sum(tr.get("notional_value", 0.0) for tr in dydx_norm)
    dydx_inserted = dydx_insert_trades(dydx_norm)
    summary["exchanges"]["dYdX"] = {
        "fetched": len(dydx_raw),
        "inserted": dydx_inserted,
        "volume": dydx_vol,
    }

    # GMX Arbitrum
    gmx_raw = gmx_fetch_trades(address, limit=1000)
    gmx_norm = [gmx_normalize_trade(t, address) for t in gmx_raw if isinstance(t, dict)]
    gmx_vol = sum(tr.get("notional_value", 0.0) for tr in gmx_norm)
    gmx_inserted = gmx_insert_trades(gmx_norm)
    summary["exchanges"]["GMX_Arbitrum"] = {
        "fetched": len(gmx_raw),
        "inserted": gmx_inserted,
        "volume": gmx_vol,
    }
    # Store aggregated result in cache (5m default TTL)
    try:
        total_vol = float(hl_vol) + float(dydx_vol) + float(gmx_vol)
        cache_volume(
            address,
            {
                "total_volume": total_vol,
                "breakdown": summary["exchanges"],
                "timestamp": int(time.time()),
            },
            ttl=300,
        )
    except Exception:
        pass

    return summary


def process_solana_wallet(address: str) -> Dict[str, Any]:
    """Process a Solana wallet via Drift Protocol."""
    # Check cache first
    cached = get_cached_volume(address)
    if cached and isinstance(cached, dict):
        return {
            "address": address,
            "chain": "Solana",
            "exchanges": cached.get("breakdown", {}),
            "cached": True,
            "cached_timestamp": cached.get("timestamp"),
            "cached_total_volume": cached.get("total_volume"),
        }

    summary = {
        "address": address,
        "chain": "Solana",
        "exchanges": {},
    }

    drift_raw = drift_fetch_trades(address)
    drift_norm = [drift_normalize_trade(t, address) for t in drift_raw if isinstance(t, dict)]
    drift_vol = sum(tr.get("notional_value", 0.0) for tr in drift_norm)
    drift_inserted = drift_insert_trades(drift_norm)
    summary["exchanges"]["Drift"] = {
        "fetched": len(drift_raw),
        "inserted": drift_inserted,
        "volume": drift_vol,
    }
    # Store aggregated result in cache (5m default TTL)
    try:
        cache_volume(
            address,
            {
                "total_volume": float(drift_vol),
                "breakdown": summary["exchanges"],
                "timestamp": int(time.time()),
            },
            ttl=300,
        )
    except Exception:
        pass

    return summary


def dataframe_to_exchange_breakdown(df) -> Dict[str, Dict[str, Any]]:
    """Convert query_volume by-exchange DataFrame to a dict structure."""
    result: Dict[str, Dict[str, Any]] = {}
    if df is None:
        return result
    try:
        for _, row in df.iterrows():
            exch = row["exchange"]
            trades = int(row["trades"])
            tv = row["total_volume"]
            # tv may come formatted as string from query_volume
            total_volume = float(tv) if not isinstance(tv, float) else tv
            result[exch] = {"trades": trades, "total_volume": total_volume}
    except Exception:
        pass
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate trading volume across wallets")
    parser.add_argument(
        "--config",
        default=os.environ.get("WALLETS_JSON", str(WALLETS_JSON)),
        help="Path to wallets.json configuration",
    )
    args = parser.parse_args()

    ensure_db()

    wallets = load_wallets(Path(args.config))
    if not wallets:
        print("No wallets to process.")
        return

    wallet_summaries: List[Dict[str, Any]] = []

    # Insert wallets first, then process per-chain
    for w in wallets:
        address = str(w.get("address", "")).strip()
        chain = str(w.get("chain", "")).strip()
        if not address or not chain:
            print(f"Skipping invalid wallet entry: {w}")
            continue

        insert_wallet(address, chain)

        if chain == "EVM":
            wallet_summaries.append(process_evm_wallet(address))
        elif chain == "Solana":
            wallet_summaries.append(process_solana_wallet(address))
        else:
            print(f"Unknown chain '{chain}' for wallet {address}, skipping.")

    # Query overall results and print using query_volume logic
    try:
        conn = connect(DB_PATH)
    except sqlite3.Error as e:
        print(f"Failed to connect to DB for summary: {e}")
        return

    try:
        total_volume, total_trades = fetch_overall(conn)
        by_exchange_df = fetch_by_exchange(conn)
        by_wallet_df = fetch_by_wallet(conn)

        # Print a concise summary
        print("=== Aggregated Summary ===")
        print(f"Total trades: {total_trades}")
        print(f"Total volume: {total_volume:.8f}")
        print("\nBreakdown by exchange:")
        if by_exchange_df is not None and not by_exchange_df.empty:
            print(by_exchange_df.to_string(index=False))
        else:
            print("(none)")

        print("\nBreakdown by wallet:")
        if by_wallet_df is not None and not by_wallet_df.empty:
            print(by_wallet_df.to_string(index=False))
        else:
            print("(none)")

        # Build JSON output structure
        output = {
            "total_volume": float(f"{total_volume:.8f}"),
            "total_trades": int(total_trades),
            "breakdown_by_exchange": dataframe_to_exchange_breakdown(by_exchange_df),
            "wallets": wallet_summaries,
        }

        with OUTPUT_JSON.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print(f"\nSaved output JSON to {OUTPUT_JSON}")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()