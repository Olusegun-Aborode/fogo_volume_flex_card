"""Fetch Uniswap V3 Swap events via Alchemy RPC and insert into SQLite.

Features:
- Connects to Ethereum mainnet using Alchemy and web3.py
- Filters Swap logs for a specific wallet (sender or recipient)
- Decodes non-indexed params (amount0, amount1, sqrtPriceX96, liquidity, tick)
- Resolves pool token0/token1, token decimals, and current USD prices (CoinGecko)
- Computes absolute USD notional as max(|amount0_usd|, |amount1_usd|)
- Inserts into `trades` with exchange = "Uniswap_V3"
- Handles pagination by chunking block ranges and adds simple retry & logging

Note:
- Without providing pool addresses, we search by topics across the specified
  block range. This can be heavy; restrict the range with --from-block.
- Prices use CoinGecko's simple token price endpoint (current market price).

Run:
  python3 volume_flex_card/fetch_uniswap_rpc.py --wallet 0x... --from-block <start>
"""

import argparse
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from web3 import Web3
from hexbytes import HexBytes

from . import config
from .logging_utils import get_logger, request_with_retries
from .price_oracle import get_price_at_timestamp


DB_PATH = Path(config.DB_PATH)
logger = get_logger(__name__)


# Event: Swap(address indexed sender, address indexed recipient,
#              int256 amount0, int256 amount1, uint160 sqrtPriceX96,
#              uint128 liquidity, int24 tick)
SWAP_TOPIC0 = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)


# Minimal ABIs to resolve pool/token details
POOL_ABI = [
    {"name": "token0", "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "token1", "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_ABI = [
    {"name": "decimals", "inputs": [], "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
    {"name": "symbol", "inputs": [], "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
]


def ensure_db() -> None:
    if not DB_PATH.exists():
        try:
            from database_setup import init_db  # type: ignore

            init_db(DB_PATH)
        except Exception as e:
            logger.warning(f"Could not initialize database automatically: {e}")


def get_w3() -> Web3:
    api_key = config.ALCHEMY_API_KEY or os.environ.get("ALCHEMY_API_KEY", "")
    rpc_url = config.ALCHEMY_ETH_URL or (
        f"https://eth-mainnet.g.alchemy.com/v2/{api_key}" if api_key else ""
    )
    if not rpc_url:
        raise RuntimeError("Missing Alchemy RPC URL. Set ALCHEMY_API_KEY or ALCHEMY_ETH_URL.")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():  # web3>=7 uses is_connected
        # Fallback for older web3 versions
        try:
            if not w3.isConnected():  # type: ignore[attr-defined]
                raise RuntimeError("Failed to connect to Alchemy RPC.")
        except AttributeError:
            raise RuntimeError("Failed to connect to Alchemy RPC.")
    return w3


def to_topic_address(address: str) -> str:
    """Encode an EVM address for topics filtering (32-byte left-padded)."""
    addr = Web3.to_checksum_address(address)
    # topics for indexed address are encoded as 12 zero bytes + 20-byte address
    return "0x" + ("0" * 24) + addr.lower().replace("0x", "")


def get_block_ts(w3: Web3, block_number: int, cache: Dict[int, int]) -> int:
    if block_number in cache:
        return cache[block_number]
    retries = config.MAX_RETRIES
    delay = config.RETRY_DELAY
    for attempt in range(retries):
        try:
            blk = w3.eth.get_block(block_number)
            ts = int(blk["timestamp"]) if isinstance(blk.get("timestamp"), (int, float)) else int(blk.timestamp)
            cache[block_number] = ts
            return ts
        except Exception as e:
            logger.warning(f"get_block({block_number}) failed: {e}; retry {attempt+1}/{retries}")
            time.sleep(delay * (1 + 0.5 * attempt))
    return int(time.time())


def call_contract_fn(w3: Web3, address: str, abi: List[Dict[str, Any]], fn_name: str) -> Optional[Any]:
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
        return getattr(contract.functions, fn_name)().call()
    except Exception as e:
        logger.warning(f"Contract call {fn_name} on {address} failed: {e}")
        return None


def get_pool_tokens(w3: Web3, pool_address: str) -> Tuple[Optional[str], Optional[str]]:
    t0 = call_contract_fn(w3, pool_address, POOL_ABI, "token0")
    t1 = call_contract_fn(w3, pool_address, POOL_ABI, "token1")
    return (t0, t1)


def get_token_info(w3: Web3, token_address: str) -> Tuple[str, int]:
    symbol = call_contract_fn(w3, token_address, ERC20_ABI, "symbol")
    decimals = call_contract_fn(w3, token_address, ERC20_ABI, "decimals")
    sym = str(symbol) if symbol else token_address[:6]
    dec = int(decimals) if isinstance(decimals, (int, float)) else 18
    return sym, dec


def get_prices_at_timestamp(token0: str, token1: str, ts: int) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    p0 = get_price_at_timestamp(token0, ts)
    p1 = get_price_at_timestamp(token1, ts)
    if isinstance(p0, float):
        prices[token0.lower()] = p0
    if isinstance(p1, float):
        prices[token1.lower()] = p1
    return prices


def decode_swap_data(w3: Web3, data_hex: str) -> Optional[Tuple[int, int, int, int, int]]:
    try:
        data_bytes = HexBytes(data_hex)
        types = ["int256", "int256", "uint160", "uint128", "int24"]
        decoded = w3.codec.decode_abi(types, data_bytes)
        # amount0, amount1, sqrtPriceX96, liquidity, tick
        return (
            int(decoded[0]),
            int(decoded[1]),
            int(decoded[2]),
            int(decoded[3]),
            int(decoded[4]),
        )
    except Exception as e:
        logger.warning(f"Failed to decode swap data: {e}")
        return None


def normalize_swap(
    w3: Web3,
    log: Dict[str, Any],
    wallet_address: str,
    token0: Optional[str],
    token1: Optional[str],
    prices: Dict[str, float],
    decimals_map: Dict[str, int],
    block_ts_cache: Dict[int, int],
) -> Optional[Dict[str, Any]]:
    decoded = decode_swap_data(w3, log["data"]) if isinstance(log.get("data"), (str, HexBytes)) else None
    if decoded is None:
        return None
    amount0, amount1, _, _, _ = decoded
    pool_addr = log.get("address")
    if not pool_addr or not token0 or not token1:
        return None

    # decimals
    d0 = decimals_map.get(token0.lower(), 18)
    d1 = decimals_map.get(token1.lower(), 18)

    amt0_tokens = abs(amount0) / (10 ** d0)
    amt1_tokens = abs(amount1) / (10 ** d1)

    p0 = prices.get(token0.lower(), 0.0)
    p1 = prices.get(token1.lower(), 0.0)

    usd0 = amt0_tokens * p0
    usd1 = amt1_tokens * p1

    # choose dominant leg for price/size, and compute absolute notional
    if usd0 >= usd1:
        price = p0
        size = amt0_tokens
        notional = usd0
    else:
        price = p1
        size = amt1_tokens
        notional = usd1

    if notional <= 0:
        return None

    # timestamp from block
    blk_num = int(log.get("blockNumber", 0))
    timestamp = get_block_ts(w3, blk_num, block_ts_cache) if blk_num else int(time.time())
    # Resolve prices at the exact swap timestamp (Chainlink→CoinGecko fallback with cache)
    prices = get_prices_at_timestamp(token0, token1, timestamp)

    # token symbols (best-effort)
    sym0, _ = get_token_info(w3, token0)
    sym1, _ = get_token_info(w3, token1)
    market = f"{sym0}-{sym1}"

    tx_hash = log.get("transactionHash")
    if isinstance(tx_hash, (bytes, HexBytes)):
        tx_hash_hex = HexBytes(tx_hash).hex()
    elif isinstance(tx_hash, str):
        tx_hash_hex = tx_hash
    else:
        tx_hash_hex = ""
    log_index = int(log.get("logIndex", 0))

    trade = {
        "exchange": "Uniswap_V3",
        "wallet_address": wallet_address,
        "market": market,
        "side": "swap",
        "price": float(price),
        "size": float(size),
        "notional_value": float(abs(notional)),
        "timestamp": int(timestamp),
        "trade_id": f"uni_{tx_hash_hex}_{log_index}",
    }
    return trade


def insert_trades(trades: List[Dict[str, Any]]) -> int:
    if not trades:
        return 0
    inserted = 0
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        for t in trades:
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO trades
                    (exchange, wallet_address, market, side, price, size, notional_value, timestamp, trade_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        t.get("exchange"),
                        t.get("wallet_address"),
                        t.get("market"),
                        t.get("side"),
                        float(t.get("price", 0.0)),
                        float(t.get("size", 0.0)),
                        float(t.get("notional_value", 0.0)),
                        int(t.get("timestamp", 0)),
                        t.get("trade_id"),
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            except Exception as e:
                logger.warning(f"Failed to insert trade {t.get('trade_id')}: {e}")
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
    finally:
        try:
            conn.close()  # type: ignore
        except Exception:
            pass
    return inserted


def fetch_swaps(
    wallet: str,
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    chunk_size: int = 2000,
    broad_scan: bool = False,
) -> List[Dict[str, Any]]:
    """Fetch and normalize Uniswap V3 swaps for `wallet` across a block range.

    Returns a list of normalized trade dicts.
    """
    w3 = get_w3()
    wallet = Web3.to_checksum_address(wallet)
    wallet_topic = to_topic_address(wallet)

    current_block = int(w3.eth.block_number)
    start = int(from_block) if from_block is not None else max(current_block - 100_000, 0)
    end = int(to_block) if to_block is not None else current_block

    logger.info(f"Fetching Uniswap V3 swaps for {wallet} from block {start} to {end}...")

    trades: List[Dict[str, Any]] = []
    seen_keys: set = set()
    block_ts_cache: Dict[int, int] = {}
    # Cache transaction senders to reduce RPC calls
    tx_from_cache: Dict[str, str] = {}

    # Iterate over chunks to avoid log limits; query twice (sender, recipient)
    for chunk_start in range(start, end + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end)
        filter_sender = {
            "fromBlock": chunk_start,
            "toBlock": chunk_end,
            "topics": [SWAP_TOPIC0, wallet_topic, None],
        }
        filter_recipient = {
            "fromBlock": chunk_start,
            "toBlock": chunk_end,
            "topics": [SWAP_TOPIC0, None, wallet_topic],
        }
        # Broad filter: any Swap event; attribution via tx.from (optional)
        filter_any = {
            "fromBlock": chunk_start,
            "toBlock": chunk_end,
            "topics": [SWAP_TOPIC0],
        }

        def get_logs_with_retry(log_filter: Dict[str, Any]) -> List[Dict[str, Any]]:
            retries = config.MAX_RETRIES
            delay = config.RETRY_DELAY
            for attempt in range(retries):
                try:
                    logs = w3.eth.get_logs(log_filter)
                    return list(logs)
                except Exception as e:
                    logger.warning(f"eth_getLogs failed: {e}; retry {attempt+1}/{retries} (blocks {chunk_start}-{chunk_end})")
                    time.sleep(delay * (1 + 0.5 * attempt))
            return []

        logs_sender = get_logs_with_retry(filter_sender)
        logs_recipient = get_logs_with_retry(filter_recipient)
        logs_any = get_logs_with_retry(filter_any) if broad_scan else []
        logs = logs_sender + logs_recipient + logs_any

        # Process logs
        for lg in logs:
            # basic keys
            txh = lg.get("transactionHash")
            li = int(lg.get("logIndex", 0))
            key = (HexBytes(txh).hex() if isinstance(txh, (bytes, HexBytes)) else str(txh), li)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            # Attribute by transaction sender (wallet initiator)
            try:
                tx_key = HexBytes(txh).hex() if isinstance(txh, (bytes, HexBytes)) else str(txh)
            except Exception:
                tx_key = str(txh)
            tx_from = tx_from_cache.get(tx_key)
            if tx_from is None:
                try:
                    tx = w3.eth.get_transaction(txh)
                    tx_from = Web3.to_checksum_address(tx.get("from")) if tx and tx.get("from") else ""
                    tx_from_cache[tx_key] = tx_from
                except Exception as e:
                    logger.warning(f"get_transaction failed for {tx_key}: {e}")
                    tx_from = ""
            # Only keep swaps initiated by the wallet
            if not tx_from or tx_from != wallet:
                continue

            pool_addr = lg.get("address")
            if not pool_addr:
                continue
            token0, token1 = get_pool_tokens(w3, pool_addr)
            if not token0 or not token1:
                continue

            # prepare token decimals and prices
            sym0, dec0 = get_token_info(w3, token0)
            sym1, dec1 = get_token_info(w3, token1)
            decimals_map = {token0.lower(): dec0, token1.lower(): dec1}
            # prices fetched per swap timestamp using the price oracle
            prices = get_prices_at_timestamp(token0, token1, get_block_ts(w3, int(lg.get("blockNumber", 0)), block_ts_cache))

            tr = normalize_swap(
                w3,
                lg,
                wallet,
                token0,
                token1,
                prices,
                decimals_map,
                block_ts_cache,
            )
            if tr is not None:
                trades.append(tr)

    return trades


def main() -> None:
    ensure_db()

    parser = argparse.ArgumentParser(description="Fetch Uniswap V3 swaps via Alchemy RPC and insert into SQLite")
    parser.add_argument("--wallet", help="EVM wallet address", default=os.environ.get("UNISWAP_WALLET", ""))
    parser.add_argument("--from-block", type=int, default=None, help="Start block (default: current-100000)")
    parser.add_argument("--to-block", type=int, default=None, help="End block (default: current)")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Blocks per eth_getLogs chunk (default: 2000)")
    parser.add_argument("--broad-scan", action="store_true", help="Include all Swap logs (topic0 only) and attribute by tx.from")
    args = parser.parse_args()

    if not args.wallet:
        logger.error("No wallet provided. Use --wallet or set UNISWAP_WALLET.")
        return

    raw_trades = fetch_swaps(
        args.wallet,
        from_block=args.from_block,
        to_block=args.to_block,
        chunk_size=args.chunk_size,
        broad_scan=args.broad_scan,
    )
    fetched_count = len(raw_trades)
    inserted_count = insert_trades(raw_trades)
    total_volume = sum(t.get("notional_value", 0.0) for t in raw_trades)

    logger.info(f"Fetched swaps: {fetched_count}")
    logger.info(f"Inserted new trades: {inserted_count}")
    logger.info(f"Total volume: {total_volume:.8f}")


if __name__ == "__main__":
    main()