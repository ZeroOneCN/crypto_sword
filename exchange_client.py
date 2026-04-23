"""Exchange client interfaces for trading backends."""

from __future__ import annotations

from typing import Any, Protocol


class ExchangeClient(Protocol):
    """Read-only exchange client contract used during CLI-to-API migration."""

    def is_configured(self) -> bool:
        """Return True when credentials are available."""
        ...

    def exchange_info(self) -> dict[str, Any]:
        """Return exchange metadata, including symbol filters."""
        ...

    def account_information(self) -> dict[str, Any]:
        """Return account information with balances and positions."""
        ...

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Return open normal orders."""
        ...

    def open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Return open conditional/algo orders when supported."""
        ...
