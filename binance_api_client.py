"""Native Binance USD-M futures REST client.

This client is intentionally read-only for the first migration stage. Order
execution still goes through the existing binance-cli path until the native
client has been verified on the server.
"""

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


logger = logging.getLogger(__name__)

MAINNET_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://testnet.binancefuture.com"


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
        use_testnet = str(os.environ.get("BINANCE_USE_TESTNET", config.get("testnet", ""))).lower() in {
            "1",
            "true",
            "yes",
            "testnet",
        }
        base_url = (
            os.environ.get("BINANCE_FAPI_BASE_URL")
            or config.get("base_url")
            or (TESTNET_BASE_URL if use_testnet else MAINNET_BASE_URL)
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

    def account_information(self) -> dict[str, Any]:
        account = self._request("GET", "/fapi/v3/account", signed=True)
        # Keep the existing code path compatible with account-information-v2.
        if "positions" not in account:
            account["positions"] = self.position_risk()
        return account

    def position_risk(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        data = self._request("GET", "/fapi/v3/positionRisk", params=params, signed=True)
        return data if isinstance(data, list) else []

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        data = self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)
        return data if isinstance(data, list) else []

    def open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol

        # Binance has changed conditional/algo order endpoints over time; try
        # known variants and let callers fall back to binance-cli when absent.
        for path in ("/fapi/v1/openAlgoOrders", "/fapi/v1/algoOpenOrders"):
            try:
                data = self._request("GET", path, params=params, signed=True)
                return data if isinstance(data, list) else data.get("orders", [])
            except Exception as e:
                logger.debug(f"Native open algo orders unsupported via {path}: {e}")
        return []

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        headers = {"Content-Type": "application/json"}

        if signed:
            if not self.is_configured():
                raise RuntimeError("Binance API key/secret not configured")
            params.setdefault("recvWindow", self.recv_window)
            params["timestamp"] = int(time.time() * 1000)
            query = urllib.parse.urlencode(params, doseq=True)
            signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            query = f"{query}&signature={signature}"
            headers["X-MBX-APIKEY"] = self.api_key
        else:
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


def _load_binance_config() -> dict[str, Any]:
    """Load Binance credentials from common Hermes config files."""
    candidates = [
        Path("/root/.hermes/config/binance.json"),
        Path("/root/.hermes/config/binance_live.json"),
        Path("config/binance.json"),
        Path("config/binance_live.json"),
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
