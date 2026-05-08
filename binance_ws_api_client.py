"""Binance USD-M Futures WebSocket API order client.

This module is intentionally narrow: it handles low-latency order placement
and cancellation through Binance's request/response WebSocket API, while the
existing REST client remains the authoritative fallback for snapshots,
accounting and recovery.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import socket
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any

try:
    import websocket
except Exception:  # pragma: no cover - runtime dependency fallback
    websocket = None

from binance_api_client import get_native_binance_client, is_native_binance_configured

logger = logging.getLogger(__name__)

MAINNET_WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
TESTNET_WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
DEFAULT_TIMEOUT_SEC = 4.0


class BinanceWsApiError(RuntimeError):
    """Raised when a WebSocket API request fails.

    ``safe_to_fallback`` is False when the request may have reached Binance but
    no response was received. In that case, blindly retrying through REST could
    duplicate an entry order.
    """

    def __init__(self, message: str, *, safe_to_fallback: bool = True):
        super().__init__(message)
        self.safe_to_fallback = safe_to_fallback


def _enabled_from_env() -> bool:
    value = os.environ.get("BINANCE_WS_ORDER_ENABLED", os.environ.get("HERMES_WS_ORDER_ENABLED", "1"))
    return str(value).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _format_decimal(value: Any) -> str:
    try:
        normalized = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        normalized = Decimal("0")
    text = format(normalized, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _sockopt() -> tuple[tuple[int, int, int], ...]:
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


def _default_ws_api_url() -> str:
    env_url = os.environ.get("BINANCE_WS_API_BASE_URL") or os.environ.get("BINANCE_FAPI_WS_API_URL")
    if env_url:
        return env_url.rstrip("/")
    client = get_native_binance_client()
    base_url = str(getattr(client, "base_url", "") or "").lower()
    if "testnet" in base_url:
        return TESTNET_WS_API_URL
    return MAINNET_WS_API_URL


class BinanceWsApiClient:
    """Small persistent request/response WebSocket API client."""

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        url: str = "",
        recv_window: int = 5000,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.url = (url or _default_ws_api_url()).rstrip("/")
        self.recv_window = int(recv_window or 5000)
        self.timeout_sec = max(float(timeout_sec or DEFAULT_TIMEOUT_SEC), 1.0)
        self._lock = threading.RLock()
        self._ws: Any = None
        self._last_connect_ts = 0.0

    @classmethod
    def from_rest_client(cls) -> "BinanceWsApiClient":
        client = get_native_binance_client()
        return cls(
            api_key=str(getattr(client, "api_key", "") or ""),
            api_secret=str(getattr(client, "api_secret", "") or ""),
            recv_window=int(getattr(client, "recv_window", 5000) or 5000),
            timeout_sec=float(
                os.environ.get("BINANCE_WS_ORDER_TIMEOUT_SEC")
                or os.environ.get("HERMES_WS_ORDER_TIMEOUT_SEC")
                or min(float(getattr(client, "request_timeout_sec", DEFAULT_TIMEOUT_SEC) or DEFAULT_TIMEOUT_SEC), 5.0)
            ),
        )

    def is_configured(self) -> bool:
        return bool(_enabled_from_env() and websocket is not None and self.api_key and self.api_secret)

    def close(self) -> None:
        with self._lock:
            ws = self._ws
            self._ws = None
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def _connect_locked(self) -> Any:
        if websocket is None:
            raise BinanceWsApiError("websocket-client 未安装，无法使用 WS API 下单")
        if not self.api_key or not self.api_secret:
            raise BinanceWsApiError("Binance API key/secret 未配置，无法使用 WS API 下单")
        if self._ws is not None:
            return self._ws
        try:
            self._ws = websocket.create_connection(
                self.url,
                timeout=self.timeout_sec,
                sockopt=_sockopt(),
                enable_multithread=True,
            )
            self._last_connect_ts = time.time()
            logger.info("Binance WS API order channel connected: %s", self.url)
            return self._ws
        except Exception as exc:
            self._ws = None
            raise BinanceWsApiError(f"Binance WS API 连接失败：{exc}", safe_to_fallback=True) from exc

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
        signed = {k: v for k, v in params.items() if v is not None}
        signed["apiKey"] = self.api_key
        signed.setdefault("recvWindow", self.recv_window)
        signed["timestamp"] = int(time.time() * 1000)
        payload = "&".join(f"{key}={signed[key]}" for key in sorted(signed))
        signed["signature"] = hmac.new(
            self.api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signed

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        payload = {
            "id": request_id,
            "method": method,
            "params": self._sign_params(params),
        }
        sent = False
        with self._lock:
            try:
                ws = self._connect_locked()
                raw_payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                ws.send(raw_payload)
                sent = True
                deadline = time.time() + self.timeout_sec
                while True:
                    if time.time() > deadline:
                        raise TimeoutError("等待 WS API 响应超时")
                    raw = ws.recv()
                    data = json.loads(raw)
                    if str(data.get("id", "")) != request_id:
                        logger.debug("Ignoring unmatched WS API frame: %s", data)
                        continue
                    status = int(data.get("status", 0) or 0)
                    if status == 200:
                        result = data.get("result", {})
                        return result if isinstance(result, dict) else {"result": result}
                    error = data.get("error") or data
                    raise BinanceWsApiError(
                        f"Binance WS API {method} status={status}: {error}",
                        safe_to_fallback=True,
                    )
            except BinanceWsApiError:
                if sent:
                    self.close()
                raise
            except Exception as exc:
                self.close()
                raise BinanceWsApiError(
                    f"Binance WS API {method} 请求失败：{exc}",
                    safe_to_fallback=not sent,
                ) from exc

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
        if reduce_only and not position_side:
            params["reduceOnly"] = "true"
        if stop_price is not None:
            params["stopPrice"] = _format_decimal(stop_price)
            params["workingType"] = working_type
        return self.request("order.place", params)

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
        if reduce_only and not position_side:
            params["reduceOnly"] = "true"
        if trigger_price is not None:
            params["triggerPrice"] = _format_decimal(trigger_price)
            params["workingType"] = working_type
        return self.request("algoOrder.place", params)

    def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self.request("order.cancel", {"symbol": symbol, "orderId": int(order_id)})

    def cancel_algo_order(self, symbol: str, algo_id: int) -> dict[str, Any]:
        return self.request("algoOrder.cancel", {"symbol": symbol, "algoId": int(algo_id)})


_ws_order_client: BinanceWsApiClient | None = None
_ws_order_lock = threading.Lock()


def get_ws_order_client() -> BinanceWsApiClient:
    global _ws_order_client
    with _ws_order_lock:
        if _ws_order_client is None:
            _ws_order_client = BinanceWsApiClient.from_rest_client()
        return _ws_order_client


def is_ws_order_enabled() -> bool:
    return bool(_enabled_from_env() and is_native_binance_configured() and get_ws_order_client().is_configured())

