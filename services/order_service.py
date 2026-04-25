"""Order service wrapper for protective and market order operations."""

from __future__ import annotations

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


order_service = OrderService()
