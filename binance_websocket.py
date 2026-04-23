"""WebSocket client for Binance futures real-time data.

Provides low-latency price, orderbook, and trade stream access.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

try:
    from binance_api_client import get_native_binance_client, is_native_binance_configured
except Exception:
    get_native_binance_client = None

    def is_native_binance_configured() -> bool:
        return False

try:
    import websocket  # pip install websocket-client
except Exception:  # pragma: no cover - optional runtime dependency
    websocket = None

logger = logging.getLogger(__name__)


@dataclass
class TickerData:
    """Real-time ticker data."""
    symbol: str
    price: float = 0.0
    price_change_pct: float = 0.0
    high_24h: float = 0.0
    low_24h: float = 0.0
    volume_24h: float = 0.0
    quote_volume_24h: float = 0.0
    last_update: float = 0.0


@dataclass
class OrderBookData:
    """Real-time orderbook data."""
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, qty)
    asks: list[tuple[float, float]] = field(default_factory=list)
    last_update: float = 0.0


class BinanceWebSocketClient:
    """Binance futures WebSocket client."""

    def __init__(
        self,
        symbols: list[str],
        callbacks: Optional[dict[str, Callable]] = None,
        base_ws_url: str | None = None,
    ):
        """Initialize WebSocket client.

        Args:
            symbols: List of symbols to subscribe (e.g., ['btcusdt', 'ethusdt'])
            callbacks: Dict of callback functions:
                - 'on_ticker': called on ticker update
                - 'on_trade': called on trade
                - 'on_orderbook': called on orderbook update
        """
        self.symbols = [s.lower() for s in symbols]
        self.callbacks = callbacks or {}
        self.base_ws_url = (base_ws_url or _get_default_ws_base_url()).rstrip("/")

        self.tickers: dict[str, TickerData] = {}
        self.orderbooks: dict[str, OrderBookData] = {}
        self.trades: deque = deque(maxlen=1000)

        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self._thread: Optional[threading.Thread] = None

        # Initialize ticker data
        for symbol in self.symbols:
            self.tickers[symbol] = TickerData(symbol=symbol.upper())
            self.orderbooks[symbol] = OrderBookData(symbol=symbol.upper())

    def _get_streams(self) -> list[str]:
        """Build WebSocket stream URLs."""
        streams = []
        for symbol in self.symbols:
            # Ticker stream
            streams.append(f"{symbol}@ticker")
            # Orderbook depth (top 20)
            streams.append(f"{symbol}@depth20@100ms")
            # Trade stream
            streams.append(f"{symbol}@trade")
        return streams

    def _on_message(self, ws, message: str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            stream_name = data.get("stream", "")
            if "data" in data:
                data = data["data"]

            # Ticker update
            if "e" in data and data["e"] == "24hrTicker":
                symbol = data["s"].lower()
                if symbol in self.tickers:
                    ticker = self.tickers[symbol]
                    ticker.price = float(data["c"])
                    ticker.price_change_pct = float(data["P"])
                    ticker.high_24h = float(data["h"])
                    ticker.low_24h = float(data["l"])
                    ticker.volume_24h = float(data["v"])
                    ticker.quote_volume_24h = float(data["q"])
                    ticker.last_update = time.time()

                    if "on_ticker" in self.callbacks:
                        self.callbacks["on_ticker"](ticker)

            # Orderbook update
            elif "lastUpdateId" in data and "bids" in data:
                symbol = data.get("s", data.get("symbol", "")).lower()
                if not symbol and "@" in stream_name:
                    symbol = stream_name.split("@", 1)[0]
                if symbol in self.orderbooks:
                    ob = self.orderbooks[symbol]
                    ob.bids = [(float(b[0]), float(b[1])) for b in data["bids"]]
                    ob.asks = [(float(a[0]), float(a[1])) for a in data["asks"]]
                    ob.last_update = time.time()

                    if "on_orderbook" in self.callbacks:
                        self.callbacks["on_orderbook"](ob)

            # Trade update
            elif "e" in data and data["e"] == "trade":
                trade = {
                    "symbol": data["s"],
                    "price": float(data["p"]),
                    "qty": float(data["q"]),
                    "is_buyer_maker": data["m"],
                    "time": data["T"],
                }
                self.trades.append(trade)

                if "on_trade" in self.callbacks:
                    self.callbacks["on_trade"](trade)

        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")

    def _on_error(self, ws, error):
        """Handle WebSocket error."""
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        logger.info(f"WebSocket closed: {close_status_code} {close_msg}")
        self.running = False

    def _on_open(self, ws):
        """Handle WebSocket open."""
        logger.info("WebSocket connected")

    def _run_ws(self):
        """Run WebSocket loop in background thread."""
        streams = "/".join(self._get_streams())
        url = f"{self.base_ws_url}/stream?streams={streams}"

        self.ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.ws.run_forever(ping_interval=60, ping_timeout=10)

    def start(self):
        """Start WebSocket connection in background thread."""
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        if self.running:
            return

        self.running = True
        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()

        # Wait for connection
        time.sleep(2)

    def stop(self):
        """Stop WebSocket connection."""
        self.running = False
        if self.ws:
            self.ws.close()
        if self._thread:
            self._thread.join(timeout=5)

    def get_price(self, symbol: str) -> float:
        """Get latest price for a symbol."""
        symbol = symbol.lower()
        if symbol in self.tickers:
            return self.tickers[symbol].price
        return 0.0

    def get_spread(self, symbol: str) -> float:
        """Get bid-ask spread for a symbol."""
        symbol = symbol.lower()
        if symbol in self.orderbooks:
            ob = self.orderbooks[symbol]
            if ob.bids and ob.asks:
                return ob.asks[0][0] - ob.bids[0][0]
        return 0.0

    def get_mid_price(self, symbol: str) -> float:
        """Get mid price for a symbol."""
        symbol = symbol.lower()
        if symbol in self.orderbooks:
            ob = self.orderbooks[symbol]
            if ob.bids and ob.asks:
                return (ob.bids[0][0] + ob.asks[0][0]) / 2
        return self.get_price(symbol)


class BinanceUserDataWebSocketClient:
    """Binance futures user data stream for order and account updates."""

    def __init__(self, callbacks: Optional[dict[str, Callable]] = None):
        self.callbacks = callbacks or {}
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.listen_key = ""
        self._thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._reconnect_lock = threading.Lock()

    def _client(self):
        if not get_native_binance_client or not is_native_binance_configured():
            raise RuntimeError("Native Binance API is not configured")
        return get_native_binance_client()

    def _open_listen_key(self) -> str:
        self.listen_key = self._client().start_user_data_stream()
        return self.listen_key

    def _on_message(self, ws, message: str):
        try:
            data = json.loads(message)
            event_type = data.get("e", "")

            if event_type == "ORDER_TRADE_UPDATE" and "on_order_update" in self.callbacks:
                self.callbacks["on_order_update"](data)
            elif event_type == "ACCOUNT_UPDATE" and "on_account_update" in self.callbacks:
                self.callbacks["on_account_update"](data)
            elif event_type == "listenKeyExpired":
                logger.warning("Binance user data listenKey expired; reconnecting")
                self._reconnect_async()

            if "on_event" in self.callbacks:
                self.callbacks["on_event"](data)
        except Exception as e:
            logger.error(f"Error processing user data WebSocket message: {e}")

    def _on_error(self, ws, error):
        logger.error(f"User data WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"User data WebSocket closed: {close_status_code} {close_msg}")

    def _on_open(self, ws):
        logger.info("User data WebSocket connected")

    def _run_keepalive(self):
        while self.running:
            time.sleep(30 * 60)
            if not self.running or not self.listen_key:
                continue
            try:
                self._client().keepalive_user_data_stream(self.listen_key)
                logger.debug("Binance user data listenKey keepalive sent")
            except Exception as e:
                logger.warning(f"Binance user data listenKey keepalive failed: {e}")
                self._reconnect_async()

    def _run_ws(self):
        while self.running:
            try:
                listen_key = self.listen_key or self._open_listen_key()
                base_ws_url = self._client().websocket_base_url().rstrip("/")
                url = f"{base_ws_url}/ws/{listen_key}"
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                logger.error(f"User data WebSocket loop failed: {e}")

            if self.running:
                time.sleep(5)

    def _reconnect_async(self):
        if not self.running:
            return
        with self._reconnect_lock:
            self.listen_key = ""
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass

    def start(self):
        """Start user data stream in background threads."""
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        if self.running:
            return

        self.running = True
        self._open_listen_key()
        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()
        self._keepalive_thread = threading.Thread(target=self._run_keepalive, daemon=True)
        self._keepalive_thread.start()
        time.sleep(1)

    def stop(self):
        """Stop user data stream and close listenKey."""
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
        if self._keepalive_thread:
            self._keepalive_thread.join(timeout=2)
        if self.listen_key:
            try:
                self._client().close_user_data_stream(self.listen_key)
            except Exception:
                pass
            self.listen_key = ""


def _get_default_ws_base_url() -> str:
    try:
        if get_native_binance_client:
            return get_native_binance_client().websocket_base_url()
    except Exception:
        pass
    return "wss://fstream.binance.com"


def main():
    """Test WebSocket client."""
    import argparse
    import time

    parser = argparse.ArgumentParser(description="Test Binance WebSocket")
    parser.add_argument("--symbols", "-s", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    args = parser.parse_args()

    def on_ticker(ticker: TickerData):
        print(f"📊 {ticker.symbol}: ${ticker.price:,.2f} ({ticker.price_change_pct:+.2f}%)")

    client = BinanceWebSocketClient(
        symbols=args.symbols,
        callbacks={"on_ticker": on_ticker},
    )

    client.start()
    print(f"WebSocket started for {args.symbols}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.stop()
        print("WebSocket stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
