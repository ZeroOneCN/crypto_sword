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

import websocket  # pip install websocket-client

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

    def __init__(self, symbols: list[str], callbacks: Optional[dict[str, Callable]] = None):
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
                symbol = data.get("symbol", "").lower() or list(self.orderbooks.keys())[0]
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
        url = f"wss://fstream.binance.com/ws/{streams}"

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
