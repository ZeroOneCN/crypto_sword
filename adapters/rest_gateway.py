"""REST-style adapters for exchange and market data dependencies."""

from __future__ import annotations

from typing import Any

from binance_breakout_scanner import (
    fetch_ticker_24hr,
    get_top_symbols_by_change,
    get_top_symbols_by_volume,
    scan_symbols,
)
from binance_trading_executor import get_account_balance, is_native_binance_configured
from surf_enhancer import get_market_overview


def get_top_symbols_by_change_rest(limit: int, min_change: float = 0.0) -> list[str]:
    return get_top_symbols_by_change(limit, min_change=min_change)


def get_top_symbols_by_volume_rest(limit: int) -> list[str]:
    return get_top_symbols_by_volume(limit)


def scan_symbols_rest(symbols: list[str], min_stage: str, max_workers: int):
    return scan_symbols(symbols, min_stage=min_stage, max_workers=max_workers)


def fetch_symbol_ticker_24h(symbol: str) -> dict[str, Any]:
    return fetch_ticker_24hr(symbol)


def load_market_overview() -> dict[str, Any]:
    overview = get_market_overview()
    return overview if isinstance(overview, dict) else {}


def load_account_balance() -> dict[str, Any]:
    account_info = get_account_balance()
    return account_info if isinstance(account_info, dict) else {}


def is_exchange_ready() -> bool:
    return bool(is_native_binance_configured())
