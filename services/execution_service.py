"""Execution service wrapper around trading executor primitives."""

from __future__ import annotations

from typing import Any

from binance_trading_executor import TradingSignal, execute_trade, should_trade


class ExecutionService:
    """Isolate direct dependency on `binance_trading_executor` APIs."""

    @staticmethod
    def build_trading_signal(symbol: str, stage: str, direction: str, entry_price: float, metrics: dict[str, Any]) -> TradingSignal:
        return TradingSignal(
            symbol=symbol,
            stage=stage,
            direction=direction,
            entry_price=entry_price,
            metrics=metrics,
        )

    @staticmethod
    def should_trade(signal: TradingSignal) -> bool:
        return bool(should_trade(signal))

    @staticmethod
    def execute_entry_trade(
        *,
        signal: TradingSignal,
        account_balance: float,
        risk_per_trade_pct: float,
        stop_loss_pct: float,
        max_position_pct: float,
        leverage: int,
        quantity: float | None,
        stop_loss_price: float | None,
        take_profit_target_pcts: list[float],
        take_profit_ratios: list[float],
        take_profit_mode: str,
        stop_trigger_buffer_pct: float,
        defer_protection_orders: bool = False,
    ) -> dict[str, Any]:
        roi_targets = take_profit_target_pcts if take_profit_mode == "roi" else None
        price_targets = take_profit_target_pcts if take_profit_mode != "roi" else None
        return execute_trade(
            signal=signal,
            account_balance=account_balance,
            risk_per_trade_pct=risk_per_trade_pct,
            stop_loss_pct=stop_loss_pct,
            max_position_pct=max_position_pct,
            leverage=leverage,
            quantity=quantity,
            stop_loss_price=stop_loss_price,
            take_profit_roi_pcts=roi_targets,
            take_profit_price_pcts=price_targets,
            take_profit_ratios=take_profit_ratios,
            take_profit_mode=take_profit_mode,
            stop_trigger_buffer_pct=stop_trigger_buffer_pct,
            defer_protection_orders=defer_protection_orders,
        )

execution_service = ExecutionService()
