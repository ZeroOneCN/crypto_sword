"""WebSocket dependency adapters with safe optional imports."""

from __future__ import annotations


def get_all_market_ticker_client_class():
    try:
        from binance_websocket import BinanceAllMarketTickerWebSocketClient

        return BinanceAllMarketTickerWebSocketClient
    except Exception:
        return None


def get_market_price_client_class():
    try:
        from binance_websocket import BinanceWebSocketClient

        return BinanceWebSocketClient
    except Exception:
        return None


def get_user_data_client_class():
    try:
        from binance_websocket import BinanceUserDataWebSocketClient

        return BinanceUserDataWebSocketClient
    except Exception:
        return None
