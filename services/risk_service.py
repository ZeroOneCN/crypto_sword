"""Risk service wrapper around risk manager primitives."""

from __future__ import annotations

from typing import Any

from risk_manager import RiskConfig, assess_trade_risk


class RiskService:
    """Isolate risk config assembly and evaluation calls."""

    @staticmethod
    def build_config(
        *,
        risk_per_trade_pct: float,
        base_stop_loss_pct: float,
        base_take_profit_pct: float,
        max_position_pct: float,
        max_total_exposure: float = 50.0,
        max_correlated_positions: int = 3,
    ) -> RiskConfig:
        return RiskConfig(
            risk_per_trade_pct=risk_per_trade_pct,
            base_stop_loss_pct=base_stop_loss_pct,
            base_take_profit_pct=base_take_profit_pct,
            max_position_pct=max_position_pct,
            max_total_exposure=max_total_exposure,
            max_correlated_positions=max_correlated_positions,
        )

    @staticmethod
    def assess(
        *,
        symbol: str,
        side: str,
        entry_price: float,
        account_balance: float,
        existing_positions: list[dict[str, Any]],
        config: RiskConfig,
    ) -> dict[str, Any]:
        return assess_trade_risk(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            account_balance=account_balance,
            existing_positions=existing_positions,
            config=config,
        )


risk_service = RiskService()
