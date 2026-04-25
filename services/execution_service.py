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
    def execute_trade(**kwargs) -> dict[str, Any]:
        return execute_trade(**kwargs)


execution_service = ExecutionService()
