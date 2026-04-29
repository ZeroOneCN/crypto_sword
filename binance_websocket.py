"""WebSocket client for Binance futures real-time data.

Provides low-latency price, orderbook, and trade stream access.
"""

from __future__ import annotations

import json
import logging
import os
import socket
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


def _ws_sockopt() -> tuple[tuple[int, int, int], ...]:
    """Prefer low-latency TCP packets for realtime trading streams."""
    options: list[tuple[int, int, int]] = []
    try:
        options.append((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1))
    except Exception:
        pass
    try:
        options.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
    except Exception:
        pass
    return tuple(options)


def _coerce_int(value: Any, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(float(value)))
    except (TypeError, ValueError):
        return max(minimum, int(default))


def _network_error_text(error: Any) -> str:
    text = str(error or "").strip()
    return text or error.__class__.__name__


def _is_transient_ws_error(error: Any) -> bool:
    text = _network_error_text(error).lower()
    return any(
        token in text
        for token in (
            "timed out",
            "timeout",
            "ping/pong timed out",
            "connection to remote host was lost",
            "connection reset",
            "temporarily unavailable",
            "network is unreachable",
            "broken pipe",
            "goodbye",
        )
    )


def _ws_runtime_settings(kind: str) -> dict[str, int]:
    normalized = kind.strip().upper().replace("-", "_")
    ping_interval = _coerce_int(
        os.environ.get(f"BINANCE_{normalized}_WS_PING_INTERVAL_SEC")
        or os.environ.get("BINANCE_WS_PING_INTERVAL_SEC"),
        30 if normalized == "MARKET" else 50,
        5,
    )
    ping_timeout = _coerce_int(
        os.environ.get(f"BINANCE_{normalized}_WS_PING_TIMEOUT_SEC")
        or os.environ.get("BINANCE_WS_PING_TIMEOUT_SEC"),
        15 if normalized == "MARKET" else 20,
        3,
    )
    if ping_timeout >= ping_interval:
        ping_timeout = max(3, ping_interval - 1)
    reconnect_max_delay = _coerce_int(
        os.environ.get(f"BINANCE_{normalized}_WS_RECONNECT_MAX_DELAY_SEC")
        or os.environ.get("BINANCE_WS_RECONNECT_MAX_DELAY_SEC"),
        20,
        5,
    )
    return {
        "ping_interval": ping_interval,
        "ping_timeout": ping_timeout,
        "reconnect_max_delay": reconnect_max_delay,
    }


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
        stream_types: Optional[list[str]] = None,
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
        self.stream_types = stream_types or ["mark_price"]

        self.tickers: dict[str, TickerData] = {}
        self.orderbooks: dict[str, OrderBookData] = {}
        self.trades: deque = deque(maxlen=1000)

        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Initialize ticker data
        for symbol in self.symbols:
            self.tickers[symbol] = TickerData(symbol=symbol.upper())
            self.orderbooks[symbol] = OrderBookData(symbol=symbol.upper())

    def _get_streams(self) -> list[str]:
        """Build WebSocket stream URLs."""
        streams = []
        for symbol in self.symbols:
            if "mark_price" in self.stream_types:
                streams.append(f"{symbol}@markPrice@1s")
            if "ticker" in self.stream_types:
                streams.append(f"{symbol}@ticker")
            if "orderbook" in self.stream_types:
                streams.append(f"{symbol}@depth5@100ms")
            if "trade" in self.stream_types:
                streams.append(f"{symbol}@trade")
        return streams

    def _on_message(self, ws, message: str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            stream_name = data.get("stream", "")
            if "data" in data:
                data = data["data"]

            # Mark price update. This is the lightest stream for position monitoring.
            if "e" in data and data["e"] == "markPriceUpdate":
                symbol = data["s"].lower()
                if symbol in self.tickers:
                    with self._lock:
                        ticker = self.tickers[symbol]
                        ticker.price = float(data["p"])
                        ticker.last_update = time.time()

                    if "on_ticker" in self.callbacks:
                        self.callbacks["on_ticker"](ticker)

            # Ticker update
            elif "e" in data and data["e"] == "24hrTicker":
                symbol = data["s"].lower()
                if symbol in self.tickers:
                    with self._lock:
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
                    with self._lock:
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
        text = _network_error_text(error)
        if _is_transient_ws_error(error):
            logger.warning(f"WebSocket transient network error: {text}")
        else:
            logger.error(f"WebSocket error: {text}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        logger.info(f"WebSocket closed: {close_status_code} {close_msg}")

    def _on_open(self, ws):
        """Handle WebSocket open."""
        logger.info("WebSocket connected")

    def _run_ws(self):
        """Run WebSocket loop in background thread."""
        reconnect_delay = 1.0
        streams = "/".join(self._get_streams())
        url = f"{self.base_ws_url}/stream?streams={streams}"
        ws_settings = _ws_runtime_settings("price")

        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(
                    ping_interval=ws_settings["ping_interval"],
                    ping_timeout=ws_settings["ping_timeout"],
                    skip_utf8_validation=True,
                    sockopt=_ws_sockopt(),
                )
            except Exception as e:
                logger.warning(f"WebSocket loop failed: {e}")

            if self.running:
                logger.info(f"WebSocket reconnecting in {reconnect_delay:.1f}s")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, float(ws_settings["reconnect_max_delay"]))

    def start(self):
        """Start WebSocket connection in background thread."""
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        if self.running:
            return

        self.running = True
        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()

        # Do not block trading; first ticks will arrive asynchronously.
        time.sleep(0.1)

    def stop(self):
        """Stop WebSocket connection."""
        self.running = False
        if self.ws:
            self.ws.close()
        if self._thread:
            self._thread.join(timeout=5)

    def get_price(self, symbol: str, max_age_sec: float = 10.0) -> float:
        """Get latest price for a symbol."""
        symbol = symbol.lower()
        with self._lock:
            ticker = self.tickers.get(symbol)
            if ticker and ticker.price > 0:
                if max_age_sec <= 0 or time.time() - ticker.last_update <= max_age_sec:
                    return ticker.price
        return 0.0

    def get_spread(self, symbol: str) -> float:
        """Get bid-ask spread for a symbol."""
        symbol = symbol.lower()
        with self._lock:
            ob = self.orderbooks.get(symbol)
            if ob and ob.bids and ob.asks:
                return ob.asks[0][0] - ob.bids[0][0]
        return 0.0

    def get_mid_price(self, symbol: str) -> float:
        """Get mid price for a symbol."""
        symbol = symbol.lower()
        with self._lock:
            ob = self.orderbooks.get(symbol)
            if ob and ob.bids and ob.asks:
                return (ob.bids[0][0] + ob.asks[0][0]) / 2
        return self.get_price(symbol)


class BinanceAllMarketTickerWebSocketClient:
    """Lightweight all-market mini ticker stream for fast anomaly ranking."""

    def __init__(
        self,
        callbacks: Optional[dict[str, Callable]] = None,
        base_ws_url: str | None = None,
    ):
        self.callbacks = callbacks or {}
        self.base_ws_url = (base_ws_url or _get_default_ws_base_url()).rstrip("/")
        self.tickers: dict[str, TickerData] = {}
        self.price_history: dict[str, deque[tuple[float, float]]] = {}
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._last_event_time = 0.0

    def _on_message(self, ws, message: str):
        try:
            payload = json.loads(message)
            if isinstance(payload, dict) and "data" in payload:
                payload = payload["data"]
            if isinstance(payload, dict):
                payload = [payload]
            if not isinstance(payload, list):
                return

            now = time.time()
            updated = 0
            with self._lock:
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("s", "")).upper()
                    if not symbol.endswith("USDT"):
                        continue
                    last_price = float(item.get("c", 0) or 0)
                    open_price = float(item.get("o", 0) or 0)
                    change_pct = 0.0
                    if open_price > 0 and last_price > 0:
                        change_pct = (last_price / open_price - 1.0) * 100.0
                    self.tickers[symbol] = TickerData(
                        symbol=symbol,
                        price=last_price,
                        price_change_pct=change_pct,
                        high_24h=float(item.get("h", 0) or 0),
                        low_24h=float(item.get("l", 0) or 0),
                        volume_24h=float(item.get("v", 0) or 0),
                        quote_volume_24h=float(item.get("q", 0) or 0),
                        last_update=now,
                    )
                    if last_price > 0:
                        history = self.price_history.setdefault(symbol, deque(maxlen=900))
                        if not history or now - history[-1][0] >= 0.9:
                            history.append((now, last_price))
                    updated += 1
                if updated:
                    self._last_event_time = now

            if updated and "on_batch" in self.callbacks:
                self.callbacks["on_batch"](updated)
        except Exception as e:
            logger.error(f"Error processing all-market WebSocket message: {e}")

    def _on_error(self, ws, error):
        text = _network_error_text(error)
        if _is_transient_ws_error(error):
            logger.warning(f"All-market WebSocket transient network error: {text}")
        else:
            logger.error(f"All-market WebSocket error: {text}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"All-market WebSocket closed: {close_status_code} {close_msg}")

    def _on_open(self, ws):
        logger.info("All-market WebSocket connected")

    def _run_ws(self):
        reconnect_delay = 1.0
        url = f"{self.base_ws_url}/ws/!miniTicker@arr"
        ws_settings = _ws_runtime_settings("market")
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(
                    ping_interval=ws_settings["ping_interval"],
                    ping_timeout=ws_settings["ping_timeout"],
                    skip_utf8_validation=True,
                    sockopt=_ws_sockopt(),
                )
            except Exception as e:
                logger.warning(f"All-market WebSocket loop failed: {e}")

            if self.running:
                logger.info(f"All-market WebSocket reconnecting in {reconnect_delay:.1f}s")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, float(ws_settings["reconnect_max_delay"]))

    def start(self):
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()
        time.sleep(0.1)

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self._thread:
            self._thread.join(timeout=5)

    def get_top_symbols_by_change(
        self,
        limit: int,
        min_change: float = 3.0,
        max_age_sec: float = 180.0,
    ) -> list[str]:
        now = time.time()
        with self._lock:
            fresh = [
                ticker
                for ticker in self.tickers.values()
                if now - ticker.last_update <= max_age_sec
                and abs(ticker.price_change_pct) >= min_change
                and not self._is_excluded_symbol(ticker.symbol)
            ]
        fresh.sort(key=lambda ticker: abs(ticker.price_change_pct), reverse=True)
        return [ticker.symbol for ticker in fresh[:limit]]

    @staticmethod
    def _is_excluded_symbol(symbol: str) -> bool:
        stable_bases = ("USDC", "FDUSD", "TUSD", "USDE", "BUSD")
        leveraged_suffixes = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
        return symbol in {f"{base}USDT" for base in stable_bases} or symbol.endswith(leveraged_suffixes)

    @staticmethod
    def _change_since(history: deque[tuple[float, float]], now: float, window_sec: float) -> float:
        if not history:
            return 0.0
        latest_price = history[-1][1]
        if latest_price <= 0:
            return 0.0
        baseline = 0.0
        cutoff = now - window_sec
        for ts, price in reversed(history):
            if ts <= cutoff:
                baseline = price
                break
        if baseline <= 0:
            if now - history[0][0] < min(window_sec * 0.35, 30.0):
                return 0.0
            baseline = history[0][1]
        return (latest_price / baseline - 1.0) * 100.0 if baseline > 0 else 0.0

    def get_top_symbols_by_hotness(
        self,
        limit: int,
        min_change: float = 0.5,
        max_age_sec: float = 30.0,
    ) -> list[str]:
        """Rank symbols by fresh WS velocity instead of only 24h change."""
        now = time.time()
        rows: list[tuple[float, str]] = []
        with self._lock:
            tickers = list(self.tickers.values())
            histories = self.price_history

        for ticker in tickers:
            symbol = ticker.symbol.upper()
            if now - ticker.last_update > max_age_sec or self._is_excluded_symbol(symbol):
                continue
            history = histories.get(symbol)
            if not history:
                continue
            change_60 = self._change_since(history, now, 60.0)
            change_180 = self._change_since(history, now, 180.0)
            change_300 = self._change_since(history, now, 300.0)
            change_24h = float(ticker.price_change_pct or 0.0)
            if not (
                abs(change_60) >= min_change
                or abs(change_180) >= min_change * 1.6
                or abs(change_300) >= min_change * 2.2
                or abs(change_24h) >= max(min_change * 4.0, 2.0)
            ):
                continue
            liquidity_score = min(float(ticker.quote_volume_24h or 0.0) / 5_000_000.0, 8.0)
            hot_score = (
                abs(change_60) * 6.0
                + abs(change_180) * 3.0
                + abs(change_300) * 2.0
                + abs(change_24h) * 0.25
                + liquidity_score
            )
            rows.append((hot_score, symbol))

        rows.sort(reverse=True)
        return [symbol for _, symbol in rows[:limit]]

    def size(self, max_age_sec: float = 180.0) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for ticker in self.tickers.values() if now - ticker.last_update <= max_age_sec)


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
            elif (
                ("ALGO" in event_type or "CONDITIONAL" in event_type)
                and "on_algo_update" in self.callbacks
            ):
                self.callbacks["on_algo_update"](data)
            elif event_type == "listenKeyExpired":
                logger.warning("Binance user data listenKey expired; reconnecting")
                self._reconnect_async()

            if "on_event" in self.callbacks:
                self.callbacks["on_event"](data)
        except Exception as e:
            logger.error(f"Error processing user data WebSocket message: {e}")

    def _on_error(self, ws, error):
        text = _network_error_text(error)
        if _is_transient_ws_error(error):
            logger.warning(f"User data WebSocket transient network error: {text}")
        else:
            logger.error(f"User data WebSocket error: {text}")

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
        ws_settings = _ws_runtime_settings("user_data")
        reconnect_delay = 2.0
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
                self.ws.run_forever(
                    ping_interval=ws_settings["ping_interval"],
                    ping_timeout=ws_settings["ping_timeout"],
                    skip_utf8_validation=True,
                    sockopt=_ws_sockopt(),
                )
            except Exception as e:
                logger.error(f"User data WebSocket loop failed: {e}")

            if self.running:
                logger.info(f"User data WebSocket reconnecting in {reconnect_delay:.1f}s")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, float(ws_settings["reconnect_max_delay"]))

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
        # Startup should not wait for the socket handshake; REST remains authoritative.
        time.sleep(0.1)

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
