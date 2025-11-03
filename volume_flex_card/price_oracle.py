"""Price oracle providing historical USD prices via Chainlink with CoinGecko fallback.

Functions:
- get_price_at_timestamp(token_address: str, timestamp: int) -> float
  - Attempts Chainlink aggregator for the token (via mapping)
  - Falls back to CoinGecko historical price
  - Caches results in Redis (immutable historical)

Config/Env:
- Uses Alchemy RPC via config.ALCHEMY_* settings
- Redis via REDIS_URL (default: redis://localhost:6379/0)
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from web3 import Web3

from . import config
from .logging_utils import get_logger, request_with_retries

logger = get_logger(__name__)


AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint80", "name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


# Chainlink feed mappings (Ethereum mainnet)
# Keys MUST be lowercased token contract addresses
TOKEN_TO_FEED: Dict[str, str] = {
    # WETH (ETH / USD)
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",  # ETH/USD
    # USDC / USD
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
    # USDT / USD
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
    # DAI / USD
    "0x6b175474e89094c44da98b954eedeac495271d0f": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
    # WBTC → use BTC / USD feed
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
    # UNI / USD
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
    # LINK / USD
    "0x514910771af9ca656af840dff83e8264ecf986ca": "0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
}

# CoinGecko token IDs for fallback
TOKEN_TO_COINGECKO_ID: Dict[str, str] = {
    # WETH → use ETH
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "ethereum",
    # Stablecoins and majors
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "usd-coin",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "tether",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "dai",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "wrapped-bitcoin",
    # Popular DeFi tokens
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "uniswap",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "chainlink",
}


def get_w3() -> Web3:
    api_key = config.ALCHEMY_API_KEY or os.environ.get("ALCHEMY_API_KEY", "")
    rpc_url = config.ALCHEMY_ETH_URL or (
        f"https://eth-mainnet.g.alchemy.com/v2/{api_key}" if api_key else ""
    )
    if not rpc_url:
        raise RuntimeError("Missing Alchemy RPC URL. Set ALCHEMY_API_KEY or ALCHEMY_ETH_URL.")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not (getattr(w3, "is_connected", None) and w3.is_connected()):
        try:
            if not w3.isConnected():  # type: ignore[attr-defined]
                raise RuntimeError("Failed to connect to Alchemy RPC.")
        except AttributeError:
            raise RuntimeError("Failed to connect to Alchemy RPC.")
    return w3


def get_redis():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    try:
        import redis

        return redis.from_url(url)
    except Exception as e:
        logger.warning(f"Redis unavailable ({url}): {e}; caching disabled")
        return None


def cache_get(key: str) -> Optional[float]:
    r = get_redis()
    if not r:
        return None
    try:
        val = r.get(key)
        if val is None:
            return None
        return float(val.decode("utf-8"))
    except Exception:
        return None


def cache_set(key: str, value: float, ttl_seconds: int = 7 * 24 * 3600) -> None:
    r = get_redis()
    if not r:
        return
    try:
        r.setex(key, ttl_seconds, str(value))
    except Exception:
        pass


def get_chainlink_price_at_timestamp(token_address: str, timestamp: int) -> Optional[float]:
    feed_addr = TOKEN_TO_FEED.get(token_address.lower())
    if not feed_addr:
        return None
    w3 = get_w3()
    contract = w3.eth.contract(address=Web3.to_checksum_address(feed_addr), abi=AGGREGATOR_V3_ABI)
    try:
        decimals = int(contract.functions.decimals().call())
    except Exception as e:
        logger.warning(f"Chainlink decimals() failed for {feed_addr}: {e}")
        return None

    # Strategy: walk backwards from latest round until updatedAt <= timestamp
    try:
        latest = contract.functions.latestRoundData().call()
        latest_round = int(latest[0])
    except Exception as e:
        logger.warning(f"latestRoundData() failed: {e}")
        return None

    step = 1000
    round_id = latest_round
    best_price: Optional[float] = None
    # cap iterations to avoid long scans
    for _ in range(50):
        try:
            rd = contract.functions.getRoundData(round_id).call()
        except Exception:
            # invalid round; reduce id
            round_id = max(round_id - step, 0)
            continue
        updated_at = int(rd[3])
        answer = int(rd[1])
        price = float(answer) / (10 ** decimals)

        if updated_at <= timestamp:
            best_price = price
            break
        # move back in time
        round_id = max(round_id - step, 0)
        # adapt step once we get close
        if updated_at - timestamp < 3600 * 24:
            step = max(step // 2, 1)

    return best_price


def get_coingecko_price_at_timestamp(token_address: str, timestamp: int) -> Optional[float]:
    coin_id = TOKEN_TO_COINGECKO_ID.get(token_address.lower())
    if not coin_id:
        return None
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    date_str = dt.strftime("%d-%m-%Y")
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    params = {"date": date_str}
    resp = request_with_retries(
        "GET",
        url,
        params=params,
        timeout=config.REQUEST_TIMEOUT,
        retries=config.MAX_RETRIES,
        backoff_base=config.RETRY_DELAY,
        logger=logger,
    )
    if resp is None:
        return None
    try:
        data = resp.json()
        md = data.get("market_data", {})
        cp = md.get("current_price", {})
        usd = cp.get("usd")
        if isinstance(usd, (int, float)):
            return float(usd)
    except Exception as e:
        logger.warning(f"Failed to parse CoinGecko history response: {e}")
    return None


def get_price_at_timestamp(token_address: str, timestamp: int) -> Optional[float]:
    token = token_address.lower()
    # Normalize timestamp to day granularity for caching (UTC midnight)
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    day_key = dt.strftime("%Y-%m-%d")
    cache_key = f"price:{token}:{day_key}"

    cached = cache_get(cache_key)
    if isinstance(cached, float):
        return cached

    try:
        price = get_chainlink_price_at_timestamp(token, timestamp)
    except Exception as e:
        logger.warning(f"Chainlink oracle error, falling back to CoinGecko: {e}")
        price = None
    if price is None:
        price = get_coingecko_price_at_timestamp(token, timestamp)

    if isinstance(price, float) and price > 0:
        cache_set(cache_key, price)
        return price

    return None