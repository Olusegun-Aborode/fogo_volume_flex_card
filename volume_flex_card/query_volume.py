"""Query and display trading volume summaries from SQLite using pandas.

Outputs:
- Overall summary (total volume, total trades)
- Breakdown by exchange (trades, total volume)
- Breakdown by wallet address (trades, total volume)
"""

import sqlite3
from pathlib import Path
from typing import Tuple
from . import config

import pandas as pd


DB_PATH = Path(config.DB_PATH)


# Simple ANSI color codes for styled terminal output
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
MAGENTA = "\033[35m"
YELLOW = "\033[33m"


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path))


def fetch_overall(conn: sqlite3.Connection) -> Tuple[float, int]:
    df = pd.read_sql_query(
        "SELECT COALESCE(SUM(notional_value), 0) AS total_volume, COUNT(*) AS total_trades FROM trades",
        conn,
    )
    total_volume = float(df.loc[0, "total_volume"]) if not df.empty else 0.0
    total_trades = int(df.loc[0, "total_trades"]) if not df.empty else 0
    return total_volume, total_trades


def fetch_by_exchange(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT exchange,
               COUNT(*) AS trades,
               COALESCE(SUM(notional_value), 0) AS total_volume
        FROM trades
        GROUP BY exchange
        ORDER BY total_volume DESC
        """,
        conn,
    )
    if not df.empty:
        df["total_volume"] = df["total_volume"].astype(float).map(lambda v: f"{v:.8f}")
    return df


def fetch_by_wallet(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT wallet_address,
               COUNT(*) AS trades,
               COALESCE(SUM(notional_value), 0) AS total_volume
        FROM trades
        GROUP BY wallet_address
        ORDER BY total_volume DESC
        """,
        conn,
    )
    if not df.empty:
        df["total_volume"] = df["total_volume"].astype(float).map(lambda v: f"{v:.8f}")
    return df


def verify_non_negative_notionals(conn: sqlite3.Connection) -> int:
    """Return count of trades with negative notional_value and print a warning if any.

    This enforces the invariant that `notional_value` is absolute (>= 0).
    """
    df = pd.read_sql_query(
        "SELECT COUNT(*) AS negatives FROM trades WHERE notional_value < 0",
        conn,
    )
    negatives = int(df.loc[0, "negatives"]) if not df.empty else 0
    if negatives > 0:
        print(f"{YELLOW}Warning: {negatives} trades have negative notional_value.{RESET}")
    return negatives


def print_header(text: str, color: str = CYAN) -> None:
    bar = "═" * (len(text) + 2)
    print(f"{color}{BOLD}╔{bar}╗{RESET}")
    print(f"{color}{BOLD}║ {text} ║{RESET}")
    print(f"{color}{BOLD}╚{bar}╝{RESET}")


def main() -> None:
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.width", 120)
    pd.set_option("display.max_colwidth", 40)

    if not DB_PATH.exists():
        print(f"{YELLOW}Database not found: {DB_PATH}{RESET}")
        return

    try:
        conn = connect()
    except sqlite3.Error as e:
        print(f"{YELLOW}Failed to connect to DB: {e}{RESET}")
        return

    try:
        # Verify invariant: notional_value should never be negative
        verify_non_negative_notionals(conn)

        total_volume, total_trades = fetch_overall(conn)
        print_header("Overall Summary", color=GREEN)
        print(f"Total trades: {BOLD}{total_trades}{RESET}")
        print(f"Total volume: {BOLD}{total_volume:.8f}{RESET}")

        print()  # spacer
        print_header("Breakdown by Exchange", color=CYAN)
        by_exch = fetch_by_exchange(conn)
        if by_exch.empty:
            print("No trades found.")
        else:
            print(by_exch.to_string(index=False))

        print()  # spacer
        print_header("Breakdown by Wallet", color=MAGENTA)
        by_wallet = fetch_by_wallet(conn)
        if by_wallet.empty:
            print("No trades found.")
        else:
            print(by_wallet.to_string(index=False))

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()