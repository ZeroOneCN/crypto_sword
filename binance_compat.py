"""Shared native Binance compatibility helpers.

This module keeps old command-style calls on the native REST client while
centralizing throttling and retry behavior for strategy modules.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

try:
    from binance_api_client import get_native_binance_client
except Exception:  # pragma: no cover - import fallback for optional runtime deps
    get_native_binance_client = None

logger = logging.getLogger(__name__)

_throttle_lock = threading.Lock()
_last_api_call_time = 0.0


def _resolve_throttle(throttle_sec: float | None, env_name: str) -> float:
    if throttle_sec is not None:
        return max(0.0, float(throttle_sec))
    return max(0.0, float(os.getenv(env_name, os.getenv("HERMES_BINANCE_COMPAT_THROTTLE_SEC", "0.05"))))


def _throttle(throttle_sec: float):
    global _last_api_call_time
    if throttle_sec <= 0:
        return
    with _throttle_lock:
        now = time.time()
        elapsed = now - _last_api_call_time
        if elapsed < throttle_sec:
            time.sleep(throttle_sec - elapsed)
        _last_api_call_time = time.time()


def run_native_binance_compat(
    args: list[str],
    timeout: int = 60,
    max_retries: int = 5,
    throttle_sec: float | None = None,
    throttle_env: str = "HERMES_BINANCE_COMPAT_THROTTLE_SEC",
) -> Any | None:
    """Run a command-style request through the native Binance REST client."""
    del timeout  # Native urllib requests use client-level timeouts.
    if get_native_binance_client is None:
        logger.error("Native Binance API client is not available")
        return None

    wait_sec = _resolve_throttle(throttle_sec, throttle_env)
    for attempt in range(max_retries + 1):
        try:
            _throttle(wait_sec)
            return get_native_binance_client().command_compat(list(args))  # type: ignore[union-attr]
        except Exception as exc:
            if attempt < max_retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            logger.error(f"Native Binance API compatibility call failed: {exc}")
            return None

    return None
