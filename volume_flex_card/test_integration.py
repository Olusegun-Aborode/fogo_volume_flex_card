"""Integration test for end-to-end volume aggregation workflow.

Steps:
- Create temporary DB (`test_trading_volume.db`) and wallets.json with 2 wallets
  (1 EVM, 1 Solana), sourced from environment variables.
- Run aggregate_volume.main() programmatically with the test wallets file.
- Verify:
  - Both wallets exist in `wallets` table
  - Trades exist for both wallets
  - `volume_card_output.json` is created
  - JSON output has expected structure
- Print a detailed report and clean up temporary files.

Required env variables for meaningful results:
- EVM: one of `GMX_ACCOUNT`, `HYPERLIQUID_ADDRESS`, or `DYDX_ADDRESS`
- Solana: `DRIFT_ACCOUNT`

Run:
- python3 volume_flex_card/test_integration.py
"""

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Tuple


TEST_DB = Path("test_trading_volume.db")
TEST_WALLETS = Path("test_wallets.json")
OUTPUT_JSON = Path("volume_card_output.json")


def print_result(name: str, passed: bool, reason: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    msg = f"{name}: {status}"
    if not passed and reason:
        msg += f" - {reason}"
    print(msg)


def setup_env_and_files() -> Tuple[str, str]:
    # Ensure config picks up the test DB
    os.environ["DB_PATH"] = str(TEST_DB)

    # Resolve test addresses from env
    evm = (
        os.environ.get("GMX_ACCOUNT")
        or os.environ.get("HYPERLIQUID_ADDRESS")
        or os.environ.get("DYDX_ADDRESS")
        or ""
    ).strip()
    sol = os.environ.get("DRIFT_ACCOUNT", "").strip()

    # Create test wallets.json
    wallets_payload = {
        "wallets": [
            {"address": evm, "chain": "EVM"},
            {"address": sol, "chain": "Solana"},
        ]
    }
    TEST_WALLETS.write_text(json.dumps(wallets_payload, indent=2), encoding="utf-8")

    # Initialize DB schema
    try:
        from database_setup import init_db

        init_db(TEST_DB)
    except Exception:
        # Best effort; aggregator.ensure_db will also try
        pass

    return evm, sol


def cleanup_files() -> None:
    for p in (TEST_DB, TEST_WALLETS, OUTPUT_JSON):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def verify_wallets_and_trades(evm: str, sol: str) -> Tuple[bool, str]:
    conn = sqlite3.connect(str(TEST_DB))
    try:
        cur = conn.cursor()

        # Wallets exist
        cur.execute("SELECT COUNT(*) FROM wallets WHERE address IN (?, ?)", (evm, sol))
        wallets_count = int(cur.fetchone()[0])
        if wallets_count != 2:
            return False, "Both wallets not present in wallets table"

        # Trades exist for both wallets
        cur.execute("SELECT COUNT(*) FROM trades WHERE wallet_address = ?", (evm,))
        evm_trades = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM trades WHERE wallet_address = ?", (sol,))
        sol_trades = int(cur.fetchone()[0])
        if evm_trades <= 0 or sol_trades <= 0:
            return False, "No trades found for one or both wallets"

        return True, ""
    finally:
        conn.close()


def verify_output_json_structure() -> Tuple[bool, str]:
    if not OUTPUT_JSON.exists():
        return False, "volume_card_output.json not created"
    try:
        data = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return False, "Failed to parse output JSON"

    # Basic structure checks
    required_top = ["total_volume", "total_trades", "breakdown_by_exchange", "wallets"]
    for k in required_top:
        if k not in data:
            return False, f"Missing key in output JSON: {k}"

    if not isinstance(data["wallets"], list) or len(data["wallets"]) != 2:
        return False, "Wallets list malformed or wrong length"

    be = data["breakdown_by_exchange"]
    if not isinstance(be, dict):
        return False, "breakdown_by_exchange must be a dict"

    # Sanity checks
    try:
        float(data["total_volume"])  # must be numeric
        int(data["total_trades"])    # must be integer
    except Exception:
        return False, "Invalid types for totals in output JSON"

    return True, ""


def run_aggregator_with_test_wallets() -> None:
    # Call aggregate_volume.main() with --config pointing to our test file
    import aggregate_volume

    argv_backup = sys.argv[:]
    try:
        sys.argv = ["aggregate_volume.py", "--config", str(TEST_WALLETS)]
        aggregate_volume.main()
    finally:
        sys.argv = argv_backup


def main() -> None:
    print("=== Integration Test: Volume Aggregation Pipeline ===")
    evm, sol = setup_env_and_files()

    # Early check for addresses to set expectations
    if not evm or not sol:
        print_result("Setup", False, "Missing EVM or Solana test address in env")
    else:
        print_result("Setup", True)

    # Run aggregator
    run_aggregator_with_test_wallets()

    # Verify DB and output
    wallets_trades_ok, wallets_trades_reason = verify_wallets_and_trades(evm, sol)
    print_result("Wallets and Trades", wallets_trades_ok, wallets_trades_reason)

    json_ok, json_reason = verify_output_json_structure()
    print_result("Output JSON", json_ok, json_reason)

    # Cleanup
    cleanup_files()
    print("=== Integration Test Complete ===")


if __name__ == "__main__":
    main()