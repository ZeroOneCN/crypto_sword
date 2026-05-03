"""Order service wrapper for protective and market order operations."""

from __future__ import annotations

import threading
import time
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

    _cache_lock = threading.RLock()
    _open_orders_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    @classmethod
    def invalidate_symbol(cls, symbol: str):
        with cls._cache_lock:
            cls._open_orders_cache.pop(str(symbol or "").upper(), None)

    @classmethod
    def _fetch_open_orders_cached(cls, symbol: str, ttl_sec: float = 0.75) -> list[dict[str, Any]]:
        symbol_key = str(symbol or "").upper()
        now = time.time()
        with cls._cache_lock:
            cached = cls._open_orders_cache.get(symbol_key)
            if cached and now - cached[0] < max(0.0, ttl_sec):
                return [dict(item) for item in cached[1]]

        orders = fetch_open_algo_orders(symbol_key) or []
        normalized = [dict(item) for item in orders if isinstance(item, dict)]
        with cls._cache_lock:
            cls._open_orders_cache[symbol_key] = (now, normalized)
        return [dict(item) for item in normalized]

    @staticmethod
    def cancel_stop_loss(symbol: str, order_id: int) -> bool:
        ok = bool(cancel_stop_loss_order(symbol, order_id))
        if ok:
            OrderService.invalidate_symbol(symbol)
        return ok

    @staticmethod
    def cancel_protective(symbol: str, order_id: int) -> bool:
        ok = bool(cancel_protective_order(symbol, order_id))
        if ok:
            OrderService.invalidate_symbol(symbol)
        return ok

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
        result = place_stop_loss_order(
            symbol,
            side,
            quantity,
            stop_price,
            position_side=position_side,
            reduce_only=reduce_only,
        )
        OrderService.invalidate_symbol(symbol)
        return result

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
        result = place_take_profit_order(
            symbol,
            side,
            quantity,
            target_price,
            position_side=position_side,
            reduce_only=reduce_only,
        )
        OrderService.invalidate_symbol(symbol)
        return result

    @staticmethod
    def place_market(
        symbol: str,
        side: str,
        quantity: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        result = place_market_order(
            symbol,
            side,
            quantity,
            position_side=position_side,
            reduce_only=reduce_only,
        )
        OrderService.invalidate_symbol(symbol)
        return result

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

    @staticmethod
    def _order_type(order: dict[str, Any]) -> str:
        return str(
            order.get("type", order.get("origType", order.get("orderType", order.get("algoType", "")))) or ""
        ).upper()

    @staticmethod
    def _trigger_price(order: dict[str, Any]) -> float:
        for key in ("triggerPrice", "stopPrice", "activatePrice", "price"):
            try:
                value = float(order.get(key, 0) or 0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    @staticmethod
    def _order_quantity(order: dict[str, Any]) -> float:
        for key in ("origQty", "quantity", "executedQty"):
            try:
                value = float(order.get(key, 0) or 0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    def list_symbol_protective_orders(
        self,
        symbol: str,
        position_side: str | None = None,
        close_side: str | None = None,
    ) -> dict[str, Any]:
        """Return currently open protective orders grouped by stop-loss / take-profit."""
        seen: set[int] = set()
        stop_loss_orders: list[dict[str, Any]] = []
        take_profit_orders: list[dict[str, Any]] = []
        unknown_orders: list[dict[str, Any]] = []

        orders = self._fetch_open_orders_cached(symbol)

        close_side_upper = (close_side or "").upper()
        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_side = str(order.get("side", "") or "").upper()
            if close_side_upper and order_side and order_side != close_side_upper:
                continue
            order_id = self._order_id(order)
            if order_id <= 0 or order_id in seen:
                continue
            seen.add(order_id)

            order_type = self._order_type(order)
            item = {
                "order_id": order_id,
                "type": order_type,
                "side": order_side,
                "position_side": str(order.get("positionSide", order.get("position_side", "")) or "").upper(),
                "price": self._trigger_price(order),
                "quantity": self._order_quantity(order),
                "raw": order,
            }
            if "TAKE_PROFIT" in order_type:
                take_profit_orders.append(item)
            elif "STOP" in order_type or "TRAILING" in order_type:
                stop_loss_orders.append(item)
            else:
                unknown_orders.append(item)

        return {
            "checked": len(orders),
            "stop_loss_orders": stop_loss_orders,
            "take_profit_orders": take_profit_orders,
            "unknown_orders": unknown_orders,
        }

    def cancel_symbol_protective_orders(self, symbol: str, position_side: str | None = None) -> dict[str, Any]:
        """Cancel all open exchange-side protective orders for a symbol."""
        seen: set[int] = set()
        canceled: list[int] = []
        failed: list[int] = []

        orders = self._fetch_open_orders_cached(symbol)

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

    def prune_duplicate_protective_orders(
        self,
        symbol: str,
        position_side: str | None = None,
        close_side: str | None = None,
    ) -> dict[str, Any]:
        """Cancel exact duplicate protective orders while preserving TP ladders.

        Duplicates are defined narrowly as same type/side/position side/trigger
        price. Different TP prices are legitimate ladder orders and are kept.
        """
        close_side_upper = (close_side or "").upper()
        orders = self._fetch_open_orders_cached(symbol, ttl_sec=0.0)
        grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        checked = 0

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_side = str(order.get("side", "") or "").upper()
            if close_side_upper and order_side and order_side != close_side_upper:
                continue
            order_id = self._order_id(order)
            if order_id <= 0:
                continue
            checked += 1
            order_type = self._order_type(order)
            order_position_side = str(order.get("positionSide", order.get("position_side", "")) or "").upper()
            price_key = f"{self._trigger_price(order):.12g}"
            key = (order_type, order_side, order_position_side, price_key)
            grouped.setdefault(key, []).append(order)

        canceled: list[int] = []
        failed: list[int] = []
        for duplicates in grouped.values():
            if len(duplicates) <= 1:
                continue
            duplicates.sort(key=lambda item: self._order_id(item), reverse=True)
            for order in duplicates[1:]:
                order_id = self._order_id(order)
                if self.cancel_protective(symbol, order_id):
                    canceled.append(order_id)
                else:
                    failed.append(order_id)

        if canceled or failed:
            self.invalidate_symbol(symbol)
        return {"checked": checked, "canceled": canceled, "failed": failed}


order_service = OrderService()
