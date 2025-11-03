import json
import os
import time
from typing import Any, Dict, Optional

import redis


_redis_client: Optional[redis.Redis] = None


def _get_redis_client() -> Optional[redis.Redis]:
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    host = os.getenv("REDIS_HOST", "localhost")
    port_str = os.getenv("REDIS_PORT", "6379")
    db_str = os.getenv("REDIS_DB", "0")
    password = os.getenv("REDIS_PASSWORD")

    try:
        port = int(port_str)
    except ValueError:
        port = 6379
    try:
        db = int(db_str)
    except ValueError:
        db = 0

    try:
        _redis_client = redis.Redis(host=host, port=port, db=db, password=password, socket_timeout=2)
        # Lightweight connectivity check
        _redis_client.ping()
        return _redis_client
    except Exception:
        # If Redis is unavailable, degrade gracefully
        _redis_client = None
        return None


def _volume_key(wallet_address: str) -> str:
    return f"volume:{wallet_address.lower()}"


def cache_volume(wallet_address: str, volume_data: Dict[str, Any], ttl: int = 300) -> bool:
    """Cache volume summary for a wallet.

    volume_data MUST include keys: total_volume (float), breakdown (dict), timestamp (int)
    ttl is in seconds; default 5 minutes.
    """
    client = _get_redis_client()
    if client is None:
        return False
    try:
        # Ensure timestamp present
        if "timestamp" not in volume_data:
            volume_data["timestamp"] = int(time.time())
        key = _volume_key(wallet_address)
        payload = json.dumps(volume_data)
        client.setex(key, ttl, payload)
        return True
    except Exception:
        return False


def get_cached_volume(wallet_address: str) -> Optional[Dict[str, Any]]:
    """Get cached volume summary if present and not expired."""
    client = _get_redis_client()
    if client is None:
        return None
    try:
        key = _volume_key(wallet_address)
        data = client.get(key)
        if not data:
            return None
        return json.loads(data)
    except Exception:
        return None


def invalidate_cache(wallet_address: str) -> bool:
    """Invalidate cached volume for a wallet."""
    client = _get_redis_client()
    if client is None:
        return False
    try:
        key = _volume_key(wallet_address)
        client.delete(key)
        return True
    except Exception:
        return False