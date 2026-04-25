"""WebSocket dependency adapters with safe optional imports."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_all_market_ticker_client_class():
    try:
        from binance_websocket import BinanceAllMarketTickerWebSocketClient

        return BinanceAllMarketTickerWebSocketClient
    except Exception as e:
        logger.warning(f"All-market WebSocket adapter unavailable, fallback to REST: {e}")
        return None


def get_market_price_client_class():
    try:
        from binance_websocket import BinanceWebSocketClient

        return BinanceWebSocketClient
    except Exception as e:
        logger.warning(f"Price WebSocket adapter unavailable, fallback to REST: {e}")
        return None


def get_user_data_client_class():
    try:
        from binance_websocket import BinanceUserDataWebSocketClient

        return BinanceUserDataWebSocketClient
    except Exception as e:
        logger.warning(f"User-data WebSocket adapter unavailable, fallback to REST: {e}")
        return None
