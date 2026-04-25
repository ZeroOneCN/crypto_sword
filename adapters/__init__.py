"""External dependency adapters for REST/WebSocket and service boundaries."""

from .rest_gateway import (
    fetch_symbol_ticker_24h,
    get_top_symbols_by_change_rest,
    get_top_symbols_by_volume_rest,
    is_exchange_ready,
    load_account_balance,
    load_market_overview,
    scan_symbols_rest,
)
from .ws_gateway import (
    get_all_market_ticker_client_class,
    get_market_price_client_class,
    get_user_data_client_class,
)

__all__ = [
    "fetch_symbol_ticker_24h",
    "get_top_symbols_by_change_rest",
    "get_top_symbols_by_volume_rest",
    "is_exchange_ready",
    "load_account_balance",
    "load_market_overview",
    "scan_symbols_rest",
    "get_all_market_ticker_client_class",
    "get_market_price_client_class",
    "get_user_data_client_class",
]
