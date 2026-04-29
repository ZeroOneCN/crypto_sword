"""Order service wrapper for protective and market order operations."""

from __future__ import annotations

from typing import Any

from binance_trading_executor import (
    cancel_protective_order,
    cancel_stop_loss_order,
    fetch_open_algo_orders,
    fetch_open_orders,
    place_market_order,
    place_stop_loss_order,
    place_take_profit_order,
)


class OrderService:
    """Isolate direct order-operation dependency surface."""

    @staticmethod
    def cancel_stop_loss(symbol: str, order_id: int) -> bool:
        return bool(cancel_stop_loss_order(symbol, order_id))

    @staticmethod
    def cancel_protective(symbol: str, order_id: int) -> bool:
        return bool(cancel_protective_order(symbol, order_id))

    @staticmethod
    def place_stop_loss(
        symbol: str,
        side: str,
        quantity: float,
        stop_price: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        return place_stop_loss_order(
            symbol,
            side,
            quantity,
            stop_price,
            position_side=position_side,
            reduce_only=reduce_only,
        )

    @staticmethod
    def place_take_profit(
        symbol: str,
        side: str,
        quantity: float,
        target_price: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        return place_take_profit_order(
            symbol,
            side,
            quantity,
            target_price,
            position_side=position_side,
            reduce_only=reduce_only,
        )

    @staticmethod
    def place_market(
        symbol: str,
        side: str,
        quantity: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        return place_market_order(
            symbol,
            side,
            quantity,
            position_side=position_side,
            reduce_only=reduce_only,
        )

    @staticmethod
    def fetch_open(symbol: str):
        return fetch_open_orders(symbol)

    @staticmethod
    def fetch_open_algo(symbol: str):
        return fetch_open_algo_orders(symbol)

    @staticmethod
    def _order_id(order: dict[str, Any]) -> int:
        for key in ("algoId", "orderId", "orderID"):
            try:
                value = int(order.get(key, 0) or 0)
            except Exception:
                value = 0
            if value > 0:
                return value
        return 0

    @staticmethod
    def _is_protective_order(order: dict[str, Any], position_side: str | None = None) -> bool:
        if position_side:
            order_position_side = str(order.get("positionSide", order.get("position_side", "")) or "").upper()
            if order_position_side and order_position_side not in {position_side.upper(), "BOTH"}:
                return False

        order_type = str(
            order.get("type", order.get("origType", order.get("orderType", order.get("algoType", "")))) or ""
        ).upper()
        if any(token in order_type for token in ("STOP", "TAKE_PROFIT", "TRAILING", "CONDITIONAL")):
            return True

        if order.get("triggerPrice") or order.get("stopPrice") or order.get("activatePrice"):
            return True

        close_position = str(order.get("closePosition", "")).lower() == "true"
        reduce_only = str(order.get("reduceOnly", "")).lower() == "true"
        return close_position or reduce_only

    def cancel_symbol_protective_orders(self, symbol: str, position_side: str | None = None) -> dict[str, Any]:
        """Cancel all open exchange-side protective orders for a symbol."""
        seen: set[int] = set()
        canceled: list[int] = []
        failed: list[int] = []

        orders: list[dict[str, Any]] = []
        for fetcher in (self.fetch_open, self.fetch_open_algo):
            try:
                orders.extend(fetcher(symbol) or [])
            except Exception:
                continue

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_id = self._order_id(order)
            if order_id <= 0 or order_id in seen:
                continue
            seen.add(order_id)
            if self.cancel_protective(symbol, order_id):
                canceled.append(order_id)
            else:
                failed.append(order_id)

        return {
            "checked": len(orders),
            "canceled": canceled,
            "failed": failed,
        }


order_service = OrderService()
