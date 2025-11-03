# fogo_volume_flex_card
# Fogo Volume Flex Card

A modular Python toolkit to fetch, normalize, and aggregate trading volume across centralized and decentralized exchanges. Includes on-chain Uniswap V3 swap ingestion via RPC, robust historical price oracle with Chainlink→CoinGecko fallback, and Redis-backed caching for fast responses.

## Features

- EVM Perp volume: Hyperliquid, dYdX (v4 indexer), GMX (Arbitrum)
- DEX swaps: Uniswap V3 `Swap` events via Alchemy RPC (wallet attribution via `tx.from`)
- Historical price oracle:
  - Primary: Chainlink via `web3.py` and your `ALCHEMY_ETH_URL`
  - Fallback: CoinGecko
  - Redis caching for prices and volume summaries
- SQLite storage with normalized schema and simple query helpers
- CLI tools and test scripts for quick validation

## Requirements

- macOS or Linux
- Python 3.10+
- Redis server (local or remote)
- Alchemy Ethereum HTTP URL (`ALCHEMY_ETH_URL`) or `ALCHEMY_API_KEY`

Install Python deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r volume_flex_card/requirements.txt
```

## Environment

Create `volume_flex_card/.env` (or set OS env vars):

```
# Redis: choose one of the following
REDIS_URL=redis://localhost:6379/0
# or
REDIS_HOST=localhost
REDIS_PORT=6379

# Alchemy Ethereum RPC
ALCHEMY_ETH_URL=https://eth-mainnet.g.alchemy.com/v2/<YOUR_KEY>
# or
ALCHEMY_API_KEY=<YOUR_KEY>

# Database path (optional; defaults provided in config)
DB_PATH=volume_flex_card/trading_volume.db
```

Start Redis (Homebrew on macOS):

```bash
brew install redis
brew services start redis
redis-cli ping  # should print PONG
```

## Initialize the Database

The fetchers insert into `trades` automatically. If needed, initialize explicitly:

```bash
python3 -c "from volume_flex_card.database_setup import init_db; init_db()"
```

## Price Oracle Test

Validate Chainlink→CoinGecko fallback and Redis caching:

```bash
python3 test_price_oracle.py
```

Expected behavior:
- With `ALCHEMY_ETH_URL` set, Chainlink path is used
- If RPC is unavailable, falls back to CoinGecko
- Second lookup should be fast (Redis cache hit)

## Uniswap V3 Swaps

Fetch swaps attributed to a wallet by `tx.from` (correctly captures router-initiated swaps):

1) Pick a recent start block:

```bash
python3 - <<'PY'
from web3 import Web3
from volume_flex_card import config
w3 = Web3(Web3.HTTPProvider(config.ALCHEMY_ETH_URL))
print(w3.eth.block_number - 5000)
PY
```

2) Run the fetcher (topic-constrained mode; faster):

```bash
python3 -m volume_flex_card.fetch_uniswap_rpc \
  --wallet 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 \
  --from-block <START_BLOCK> \
  --chunk-size 1000
```

3) Comprehensive mode (include all `Swap` logs; heavier):

```bash
python3 -m volume_flex_card.fetch_uniswap_rpc \
  --wallet 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 \
  --from-block <START_BLOCK> \
  --chunk-size 1000 \
  --broad-scan
```

Verify inserts in SQLite:

```bash
sqlite3 volume_flex_card/trading_volume.db \
  'SELECT COUNT(*), SUM(notional_value) FROM trades WHERE exchange="Uniswap_V3" AND wallet_address="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";'
```

Notes:
- Vitalik’s recent swaps may be outside your scanned range or on Uniswap V2 (not yet implemented).
- Broad scans over large ranges are heavy; start narrow (2k–5k blocks).

## Perp Volume Aggregation

Run the end-to-end aggregator for EVM wallets (Hyperliquid, dYdX, GMX):

```bash
python3 -m volume_flex_card.aggregate_volume --config wallets.json
```

Outputs:
- Inserts normalized trades into SQLite
- Writes JSON summary (see code)
- Caches per-wallet volume summaries in Redis (default TTL 5 minutes)

## Configuration

Key options in `volume_flex_card/config.py`:
- `ALCHEMY_ETH_URL`, `ALCHEMY_API_KEY` for oracle and RPC fetches
- Redis settings (`REDIS_URL` or host/port)
- Retry counts and delays for RPC calls
- DB path

## Troubleshooting

- `ModuleNotFoundError` when running from project root:
  - Use module form: `python3 -m volume_flex_card.fetch_uniswap_rpc ...`
- `Missing Alchemy RPC URL`: set `ALCHEMY_ETH_URL` in `volume_flex_card/.env`
- `redis-cli: command not found`:
  - Install via Homebrew: `brew install redis`; start: `brew services start redis`
- RPC rate limits/timeouts:
  - Reduce `--chunk-size` or block range; avoid very broad scans unless necessary
- `python` vs `python3`:
  - Use `python3` on macOS where `python` may not be available

## Development

Run tests (live tests require network):

```bash
python3 -m pytest -q  # if pytest is installed
python3 volume_flex_card/test_fetchers_mocked.py
python3 volume_flex_card/test_fetchers_live.py
```

## Security & Secrets

- `.gitignore` excludes `.env` and database files by default
- Do not commit API keys or secrets

## Roadmap

- Uniswap V2 support
- CLI for oracle queries and DB inspection
- Configurable cache TTLs exposed via CLI