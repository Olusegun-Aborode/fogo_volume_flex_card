"""Shared logging and HTTP retry utilities for volume aggregator scripts.

Backoff strategy:
- Exponential backoff with optional jitter to avoid thundering herd and rate limiting.
- Default base delay is 2 seconds (from config) and timeout is 15 seconds to better handle slow APIs like GMX GraphQL.
"""

import logging
import time
import random
from pathlib import Path
from typing import Optional

import requests


LOG_FILE = Path("volume_aggregator.log")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Configure and return a logger that logs to console and file.

    - INFO for normal success messages
    - WARNING for skipped/invalid data
    - ERROR for failures
    """
    logger_name = name or "volume_aggregator"
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # File handler
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


def request_with_retries(
    method: str,
    url: str,
    *,
    retries: int = 3,
    backoff_base: float = 1.0,
    logger: Optional[logging.Logger] = None,
    **kwargs,
) -> Optional[requests.Response]:
    """Perform an HTTP request with retries and exponential backoff.

    Retries on requests.exceptions.RequestException and non-2xx responses.
    Returns the successful Response or None if all attempts fail.
    """
    log = logger or get_logger("http")
    # Import config lazily to avoid circular import during early setup
    try:
        from config import RETRY_JITTER, RETRY_DELAY
    except Exception:
        RETRY_JITTER = False  # fallback
        RETRY_DELAY = backoff_base

    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                # Exponential backoff with optional jitter
                base = RETRY_DELAY if RETRY_DELAY else backoff_base
                exp_delay = base * (2 ** (attempt - 1))
                jitter = random.uniform(0, 1) if RETRY_JITTER else 0.0
                delay = exp_delay + jitter
                log.warning(
                    f"HTTP {method} {url} failed (attempt {attempt}/{retries}): {e}. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
            else:
                log.error(f"HTTP {method} {url} failed after {retries} attempts: {e}")
                return None