"""Native Binance USD-M futures REST client."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from exchange_client import ExchangeClient
from hermes_paths import hermes_config_dir


logger = logging.getLogger(__name__)

MAINNET_BASE_URL = "https://fapi.binance.com"


class BinanceApiClient:
    """Small native REST client for Binance USD-M futures."""

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        base_url: str = "",
        recv_window: int = 5000,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = (base_url or MAINNET_BASE_URL).rstrip("/")
        self.recv_window = recv_window

    @classmethod
    def from_environment(cls) -> "BinanceApiClient":
        """Build client from environment or Hermes config files."""
        config = _load_binance_config()
        base_url = (
            os.environ.get("BINANCE_FAPI_BASE_URL")
            or config.get("base_url")
            or config.get("endpoint")
            or MAINNET_BASE_URL
        )

        return cls(
            api_key=os.environ.get("BINANCE_API_KEY") or config.get("api_key", ""),
            api_secret=os.environ.get("BINANCE_API_SECRET") or config.get("api_secret", ""),
            base_url=base_url,
            recv_window=int(os.environ.get("BINANCE_RECV_WINDOW", config.get("recv_window", 5000))),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def exchange_info(self) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/exchangeInfo", signed=False)

    def ticker_24hr(self, symbol: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v1/ticker/24hr", params=params, signed=False)

    def klines(self, symbol: str, interval: str = "1h", limit: int = 50) -> list[Any]:
        data = self._request(
            "GET",
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            signed=False,
        )
        return data if isinstance(data, list) else []

    def open_interest(self, symbol: str) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/openInterest", params={"symbol": symbol}, signed=False)

    def open_interest_statistics(self, symbol: str, period: str = "1h", limit: int = 24) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/futures/data/openInterestHist",
            params={"symbol": symbol, "period": period, "limit": limit},
            signed=False,
        )
        return data if isinstance(data, list) else []

    def long_short_ratio(self, symbol: str, period: str = "1h", limit: int = 24) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            signed=False,
        )
        return data if isinstance(data, list) else []

    def funding_rate(self, symbol: str, limit: int = 3) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": limit},
            signed=False,
        )
        return data if isinstance(data, list) else []

    def account_information(self) -> dict[str, Any]:
        last_error: Exception | None = None
        account: dict[str, Any] = {}
        # Some Binance environments/proxies can behave differently across
        # account versions; try v3 first and gracefully fall back to v2.
        for path in ("/fapi/v3/account", "/fapi/v2/account"):
            try:
                data = self._request("GET", path, signed=True)
                if isinstance(data, dict) and data:
                    account = data
                    break
            except Exception as exc:
                last_error = exc
                logger.debug(f"Account endpoint failed via {path}: {exc}")

        if not account:
            if last_error:
                raise last_error
            raise RuntimeError("Failed to fetch Binance account information")

        # Keep the existing code path compatible with account-information-v2.
        if "positions" not in account:
            account["positions"] = self.position_risk()
        return account

    def position_risk(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        last_error: Exception | None = None
        for path in ("/fapi/v3/positionRisk", "/fapi/v2/positionRisk"):
            try:
                data = self._request("GET", path, params=params, signed=True)
                return data if isinstance(data, list) else []
            except Exception as exc:
                last_error = exc
                logger.debug(f"Position risk endpoint failed via {path}: {exc}")
        if last_error:
            raise last_error
        return []

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        data = self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)
        return data if isinstance(data, list) else []

    def get_trade_history(self, symbol: str | None = None, start_time: int | None = None, end_time: int | None = None, limit: int = 500) -> list[dict[str, Any]]:
        """Get trade history for a symbol or all symbols."""
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        data = self._request("GET", "/fapi/v1/userTrades", params=params, signed=True)
        return data if isinstance(data, list) else []

    def all_orders(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get all futures orders for one symbol."""
        if not symbol:
            return []
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        data = self._request("GET", "/fapi/v1/allOrders", params=params, signed=True)
        return data if isinstance(data, list) else []

    def open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol

        # Binance has changed conditional/algo order endpoints over time; try
        # known variants; normal conditional orders are also visible in openOrders.
        for path in ("/fapi/v1/openAlgoOrders", "/fapi/v1/algoOpenOrders"):
            try:
                data = self._request("GET", path, params=params, signed=True)
                return data if isinstance(data, list) else data.get("orders", [])
            except Exception as e:
                logger.debug(f"Native open algo orders unsupported via {path}: {e}")
        return []

    def start_user_data_stream(self) -> str:
        """Create or extend a futures user data stream listenKey."""
        data = self._request("POST", "/fapi/v1/listenKey", signed=False, api_key=True)
        listen_key = data.get("listenKey") if isinstance(data, dict) else ""
        if not listen_key:
            raise RuntimeError("Binance listenKey response missing listenKey")
        return str(listen_key)

    def keepalive_user_data_stream(self, listen_key: str) -> dict[str, Any]:
        """Keep the futures user data stream alive."""
        params = {"listenKey": listen_key} if listen_key else {}
        data = self._request("PUT", "/fapi/v1/listenKey", params=params, signed=False, api_key=True)
        return data if isinstance(data, dict) else {}

    def close_user_data_stream(self, listen_key: str) -> dict[str, Any]:
        """Close the futures user data stream."""
        params = {"listenKey": listen_key} if listen_key else {}
        data = self._request("DELETE", "/fapi/v1/listenKey", params=params, signed=False, api_key=True)
        return data if isinstance(data, dict) else {}

    def websocket_base_url(self) -> str:
        """Return the USD-M futures WebSocket base URL."""
        return "wss://fstream.binance.com"

    def change_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return self._request(
            "POST",
            "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": leverage},
            signed=True,
        )

    def new_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float | None = None,
        position_side: str | None = None,
        reduce_only: bool = False,
        stop_price: float | None = None,
        working_type: str = "MARK_PRICE",
        new_order_resp_type: str = "RESULT",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "newOrderRespType": new_order_resp_type,
        }
        if quantity is not None:
            params["quantity"] = _format_decimal(quantity)
        if position_side:
            params["positionSide"] = position_side
        # In Hedge Mode, positionSide already determines whether the order closes
        # LONG or SHORT. Binance may reject reduceOnly together with positionSide.
        if reduce_only and not position_side:
            params["reduceOnly"] = "true"
        if stop_price is not None:
            params["stopPrice"] = _format_decimal(stop_price)
            params["workingType"] = working_type

        return self._request("POST", "/fapi/v1/order", params=params, signed=True)

    def new_algo_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float | None = None,
        position_side: str | None = None,
        reduce_only: bool = False,
        trigger_price: float | None = None,
        working_type: str = "MARK_PRICE",
        new_order_resp_type: str = "RESULT",
    ) -> dict[str, Any]:
        """Create a USD-M Futures conditional algo order."""
        params: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "newOrderRespType": new_order_resp_type,
        }
        if quantity is not None:
            params["quantity"] = _format_decimal(quantity)
        if position_side:
            params["positionSide"] = position_side
        # Hedge Mode uses positionSide to determine which leg is closed.
        # Binance rejects reduceOnly when it is not required for this endpoint.
        if reduce_only and not position_side:
            params["reduceOnly"] = "true"
        if trigger_price is not None:
            params["triggerPrice"] = _format_decimal(trigger_price)
            params["workingType"] = working_type

        return self._request("POST", "/fapi/v1/algoOrder", params=params, signed=True)

    def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id},
            signed=True,
        )

    def cancel_algo_order(self, symbol: str, algo_id: int) -> dict[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/algoOrder",
            params={"symbol": symbol, "algoId": algo_id},
            signed=True,
        )

    def command_compat(self, args: list[str]) -> Any:
        """Compatibility adapter for old command-style futures calls."""
        if not args:
            raise RuntimeError("Empty Binance command")

        command = args[0]
        params = _args_to_params(args[1:])

        if command in {"exchange-information", "exchangeInfo"}:
            return self.exchange_info()
        if command in {"ticker24hr-price-change-statistics", "ticker-24hr"}:
            return self.ticker_24hr(params.get("symbol"))
        if command in {"open-interest", "openInterest"}:
            return self.open_interest(_required(params, "symbol"))
        if command in {"open-interest-statistics", "openInterestHist"}:
            return self.open_interest_statistics(
                _required(params, "symbol"),
                period=params.get("period", "1h"),
                limit=int(params.get("limit", 24)),
            )
        if command in {"long-short-ratio", "globalLongShortAccountRatio"}:
            return self.long_short_ratio(
                _required(params, "symbol"),
                period=params.get("period", "1h"),
                limit=int(params.get("limit", 24)),
            )
        if command in {"get-funding-rate-history", "fundingRate"}:
            return self.funding_rate(_required(params, "symbol"), limit=int(params.get("limit", 3)))
        if command in {"kline-candlestick-data", "klines"}:
            return self.klines(
                _required(params, "symbol"),
                interval=params.get("interval", "1h"),
                limit=int(params.get("limit", 50)),
            )
        if command in {"account-information-v2", "account-information-v3"}:
            return self.account_information()
        if command in {"current-all-open-orders", "open-orders"}:
            return self.open_orders(params.get("symbol"))

        raise RuntimeError(f"Unsupported native Binance command: {command}")

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        api_key: bool = False,
    ) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        headers = {"Content-Type": "application/json"}
        query = ""

        if signed:
            if not self.is_configured():
                raise RuntimeError("Binance API key/secret not configured")
            params.setdefault("recvWindow", self.recv_window)
            params["timestamp"] = int(time.time() * 1000)
            query = urllib.parse.urlencode(params, doseq=True)
            signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            query = f"{query}&signature={signature}"
            headers["X-MBX-APIKEY"] = self.api_key
        elif api_key:
            if not self.api_key:
                raise RuntimeError("Binance API key not configured")
            headers["X-MBX-APIKEY"] = self.api_key
            if params:
                query = urllib.parse.urlencode(params, doseq=True)
        else:
            # Public endpoint without API key - still need to encode params
            if params:
                query = urllib.parse.urlencode(params, doseq=True)

        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        request = urllib.request.Request(url, method=method.upper(), headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance API HTTP {e.code}: {body[:300]}") from e


def _format_decimal(value: float) -> str:
    from decimal import Decimal, InvalidOperation

    try:
        normalized = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        normalized = Decimal("0")
    text = format(normalized, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _args_to_params(args: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    index = 0
    while index < len(args):
        key = args[index]
        if key.startswith("--"):
            normalized = _kebab_to_camel(key[2:])
            if index + 1 < len(args) and not args[index + 1].startswith("--"):
                params[normalized] = args[index + 1]
                index += 2
            else:
                params[normalized] = "true"
                index += 1
        index += 1
    return params


def _kebab_to_camel(value: str) -> str:
    parts = value.split("-")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def _required(params: dict[str, str], key: str) -> str:
    value = params.get(key)
    if not value:
        raise RuntimeError(f"Missing required parameter: {key}")
    return value


def _load_binance_config() -> dict[str, Any]:
    """Load Binance credentials from common Hermes config files."""
    repo_config_dir = Path(__file__).resolve().parent / "config"
    candidates = [
        repo_config_dir / "binance.json",
        repo_config_dir / "binance_live.json",
        hermes_config_dir() / "binance.json",
        hermes_config_dir() / "binance_live.json",
    ]

    for path in candidates:
        try:
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to load Binance config {path}: {e}")

    return {}


_native_client: BinanceApiClient | None = None


def get_native_binance_client() -> ExchangeClient:
    """Return a cached native Binance client."""
    global _native_client
    if _native_client is None:
        _native_client = BinanceApiClient.from_environment()
    return _native_client


def is_native_binance_configured() -> bool:
    """Return whether native Binance credentials are available."""
    return get_native_binance_client().is_configured()
