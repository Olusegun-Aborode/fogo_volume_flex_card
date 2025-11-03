"""Central configuration for endpoints, database path, and request settings.

Loads environment variables via python-dotenv. All values can be overridden
using environment variables in a `.env` file or system environment.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env located next to this file
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# API endpoints
HYPERLIQUID_URL = os.getenv("HYPERLIQUID_URL", "https://api.hyperliquid.xyz/info")
DYDX_URL = os.getenv("DYDX_URL", "https://indexer.dydx.trade/v4/fills")
GMX_ARBITRUM_URL = os.getenv(
    "GMX_ARBITRUM_URL",
    "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql",
)
# Template string; format with accountId where needed
DRIFT_URL = os.getenv("DRIFT_URL", "https://data.api.drift.trade/user/{accountId}/trades")

# Database path (string); scripts should wrap with Path when needed
DB_PATH = os.getenv("DB_PATH", "trading_volume.db")

# Request timeouts and retry settings
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))  # seconds; GMX GraphQL can be slow
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "2"))  # seconds; base backoff delay
RETRY_JITTER = os.getenv("RETRY_JITTER", "true").lower() in {"1", "true", "yes", "on"}

# Alchemy / Ethereum RPC settings
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
ALCHEMY_ETH_URL = os.getenv(
    "ALCHEMY_ETH_URL",
    f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY else ""
)