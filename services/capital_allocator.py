"""Capital allocation policy for each candidate entry.

The allocator is intentionally separate from signal scoring and order
execution. It decides how much capital/risk a signal deserves after looking at
signal quality, current day performance, expected reward/risk and account
drawdown. This keeps the trading loop fast while avoiding fixed-size entries in
very different market conditions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _score_value(signal: dict[str, Any]) -> float:
    score_data = signal.get("score") or {}
    if isinstance(score_data, dict):
        return float(score_data.get("total_score", score_data.get("total", 0)) or 0)
    return float(score_data or 0)


def _metric(signal: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    metrics = signal.get("metrics") or {}
    for key in keys:
        if key in metrics:
            try:
                return float(metrics.get(key, default) or default)
            except Exception:
                return default
    return default


def _normalize_ratios(ratios: list[float], count: int) -> list[float]:
    if count <= 0:
        return []
    values = [max(float(r), 0.0) for r in ratios[:count]]
    if len(values) < count:
        values.extend([0.0] * (count - len(values)))
    total = sum(values)
    if total <= 0:
        return [1.0 / count for _ in range(count)]
    return [value / total for value in values]


@dataclass
class CapitalPlan:
    allowed: bool
    mode: str
    reason: str
    leverage: int
    risk_per_trade_pct: float
    max_position_pct: float
    max_total_exposure_pct: float
    max_correlated_positions: int
    effective_balance: float
    locked_profit: float
    expected_reward_pct: float
    expected_rr: float
    min_expected_rr: float
    drawdown_pct: float
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "mode": self.mode,
            "reason": self.reason,
            "leverage": self.leverage,
            "risk_per_trade_pct": round(self.risk_per_trade_pct, 4),
            "max_position_pct": round(self.max_position_pct, 2),
            "max_total_exposure_pct": round(self.max_total_exposure_pct, 2),
            "max_correlated_positions": self.max_correlated_positions,
            "effective_balance": round(self.effective_balance, 2),
            "locked_profit": round(self.locked_profit, 2),
            "expected_reward_pct": round(self.expected_reward_pct, 3),
            "expected_rr": round(self.expected_rr, 3),
            "min_expected_rr": round(self.min_expected_rr, 3),
            "drawdown_pct": round(self.drawdown_pct, 3),
            "notes": list(self.notes),
        }


class CapitalAllocator:
    """Institutional-style capital allocator for small-account compounding."""

    def build_plan(
        self,
        *,
        config: Any,
        signal: dict[str, Any],
        exit_profile: dict[str, Any],
        dynamic_limits: dict[str, Any],
        account_balance: float,
        day_start_balance: float,
        daily_pnl: float,
        daily_report: dict[str, Any],
        market_style_mode: str = "balanced",
    ) -> CapitalPlan:
        base_leverage = int(getattr(config, "leverage", 5) or 5)
        max_leverage = int(getattr(config, "capital_max_leverage", 10) or 10)
        leverage = max(1, min(max_leverage, base_leverage))
        base_risk = float(getattr(config, "risk_per_trade_pct", 1.0) or 1.0)
        base_max_position = float(getattr(config, "max_position_pct", 35.0) or 35.0)
        base_exposure = float(dynamic_limits.get("max_total_exposure", getattr(config, "max_total_exposure_pct", 220.0)) or 220.0)
        max_correlated = int(dynamic_limits.get("max_correlated_positions", 5) or 5)

        score = _score_value(signal)
        strategy_line = str(signal.get("strategy_line", "") or "")
        direction = str(signal.get("direction", "") or "")
        oi_change = abs(_metric(signal, "oi_24h_pct", "oi_change_pct"))
        funding = _metric(signal, "funding_rate", "funding_current")
        change_24h = _metric(signal, "change_24h_pct", "price_change_pct")

        stop_loss_pct = max(float(exit_profile.get("stop_loss_pct", getattr(config, "stop_loss_pct", 7.0)) or 0.0), 0.01)
        targets = [float(item) for item in (exit_profile.get("take_profit_targets") or [])]
        ratios = _normalize_ratios([float(item) for item in (exit_profile.get("take_profit_ratios") or [])], len(targets))
        take_profit_mode = str(exit_profile.get("take_profit_mode", getattr(config, "take_profit_mode", "roi")) or "roi")
        target_leverage_for_rr = max(base_leverage, 1)
        price_moves = [
            abs(target / target_leverage_for_rr) if take_profit_mode == "roi" else abs(target)
            for target in targets
        ]
        gross_reward_pct = sum(move * ratios[index] for index, move in enumerate(price_moves)) if price_moves else 0.0
        roundtrip_cost_pct = max(0.0, float(getattr(config, "capital_estimated_roundtrip_cost_pct", 0.22) or 0.0))
        expected_reward_pct = max(0.0, gross_reward_pct - roundtrip_cost_pct)
        effective_risk_pct = stop_loss_pct + roundtrip_cost_pct
        expected_rr = expected_reward_pct / effective_risk_pct if effective_risk_pct > 0 else 0.0

        if not bool(getattr(config, "capital_allocator_enabled", True)):
            return CapitalPlan(
                allowed=True,
                mode="固定资金",
                reason="资金分配器关闭",
                leverage=leverage,
                risk_per_trade_pct=base_risk,
                max_position_pct=base_max_position,
                max_total_exposure_pct=base_exposure,
                max_correlated_positions=max_correlated,
                effective_balance=account_balance,
                locked_profit=0.0,
                expected_reward_pct=expected_reward_pct,
                expected_rr=expected_rr,
                min_expected_rr=0.0,
                drawdown_pct=0.0,
                notes=[],
            )

        closed = int(daily_report.get("closed_trades", 0) or 0)
        win_rate = float(daily_report.get("win_rate", 0) or 0)
        profit_factor = float(daily_report.get("profit_factor", 0) or 0)
        total_pnl = float(daily_report.get("total_pnl", daily_pnl) or 0)
        basis = day_start_balance if day_start_balance > 0 else account_balance
        drawdown_pct = abs(min(daily_pnl, total_pnl, 0.0)) / basis * 100 if basis > 0 else 0.0

        mode = "标准复利"
        notes: list[str] = []
        risk_multiplier = 1.0
        max_position_multiplier = 1.0
        max_exposure = base_exposure

        if closed >= 3 and total_pnl < 0 and (profit_factor < 1.0 or win_rate < 45.0):
            mode = "防守复利"
            risk_multiplier = min(risk_multiplier, 0.45)
            max_position_multiplier = min(max_position_multiplier, 0.70)
            max_exposure = min(max_exposure, 100.0)
            max_correlated = min(max_correlated, 3)
            notes.append(f"日内弱势 PF={profit_factor:.2f} 胜率={win_rate:.0f}%")

        if drawdown_pct >= float(getattr(config, "capital_hard_drawdown_pct", 6.0)):
            mode = "深度防守"
            risk_multiplier = min(risk_multiplier, 0.45)
            max_position_multiplier = min(max_position_multiplier, 0.65)
            max_exposure = min(max_exposure, 80.0)
            max_correlated = min(max_correlated, 2)
            notes.append(f"日内回撤 {drawdown_pct:.2f}%")
        elif drawdown_pct >= float(getattr(config, "capital_defensive_drawdown_pct", 3.0)):
            mode = "防守复利"
            risk_multiplier = min(risk_multiplier, 0.70)
            max_position_multiplier = min(max_position_multiplier, 0.80)
            max_exposure = min(max_exposure, 100.0)
            max_correlated = min(max_correlated, 3)
            notes.append(f"日内回撤 {drawdown_pct:.2f}%")

        is_breakout = strategy_line == "趋势突破线"
        strong_signal = (
            is_breakout
            and score >= float(getattr(config, "capital_aggressive_score", 95.0))
            and 8.0 <= abs(change_24h) <= 38.0
            and 24.0 <= oi_change <= float(getattr(config, "max_oi_change_pct", 90.0))
            and abs(funding) <= float(getattr(config, "max_abs_funding_rate", 0.004)) * 0.70
        )
        elite_signal = strong_signal and score >= 97.0 and expected_rr >= 1.70 and drawdown_pct < 1.0

        if strong_signal and mode not in {"深度防守"} and total_pnl >= 0 and drawdown_pct < 1.0:
            mode = "进攻复利" if not elite_signal else "精英强攻"
            risk_multiplier = max(risk_multiplier, 1.05 if not elite_signal else 1.15)
            max_position_multiplier = max(max_position_multiplier, 1.05 if not elite_signal else 1.12)
            max_exposure = max(max_exposure, min(float(getattr(config, "max_total_exposure_pct", base_exposure)), base_exposure))
            max_correlated = max(max_correlated, 3)
            notes.append(f"强趋势 score={score:.1f} OI={oi_change:.1f}%")

        if score < 78.0:
            risk_multiplier = min(risk_multiplier, 0.65)
            max_position_multiplier = min(max_position_multiplier, 0.75)
            notes.append(f"评分偏普通 {score:.1f}")

        if expected_rr < 1.70:
            risk_multiplier = min(risk_multiplier, 0.80)
            max_position_multiplier = min(max_position_multiplier, 0.85)
            notes.append(f"扣成本后盈亏比 {expected_rr:.2f}R")

        if market_style_mode == "major" and signal.get("symbol", "").upper() not in getattr(config, "major_symbols", []):
            risk_multiplier = min(risk_multiplier, 0.80)
            max_position_multiplier = min(max_position_multiplier, 0.85)
            notes.append("市场风格偏主流，山寨仓降档")
        elif market_style_mode == "alt" and signal.get("symbol", "").upper() in getattr(config, "major_symbols", []):
            risk_multiplier = min(risk_multiplier, 0.85)
            notes.append("市场风格偏山寨，主流仓降档")

        min_expected_rr = float(getattr(config, "capital_min_expected_rr", 1.18) or 1.18)
        if mode in {"防守复利", "深度防守"}:
            min_expected_rr = max(min_expected_rr, 1.55)
        if strong_signal:
            min_expected_rr = max(1.40, min_expected_rr - 0.05)

        allowed = expected_rr >= min_expected_rr
        reason = "通过资本分配"
        if not allowed:
            reason = f"期望盈亏比不足 {expected_rr:.2f}R < {min_expected_rr:.2f}R"

        locked_profit = 0.0
        if bool(getattr(config, "capital_profit_lock_enabled", True)):
            lock_start_pct = float(getattr(config, "capital_profit_lock_start_pct", 3.0) or 3.0)
            lock_ratio = float(getattr(config, "capital_profit_lock_ratio", 0.35) or 0.35)
            profit_pct = max(total_pnl, daily_pnl, 0.0) / basis * 100 if basis > 0 else 0.0
            if profit_pct >= lock_start_pct:
                locked_profit = max(total_pnl, daily_pnl, 0.0) * max(0.0, min(lock_ratio, 0.8))
                notes.append(f"盈利锁仓 {locked_profit:.2f}U")

        risk_cap = float(getattr(config, "capital_max_risk_pct", 1.6) or 1.6)
        risk_floor = float(getattr(config, "capital_min_risk_pct", 0.35) or 0.35)
        risk_pct = max(risk_floor, min(risk_cap, base_risk * risk_multiplier))
        max_position_pct = max(10.0, min(55.0, base_max_position * max_position_multiplier))
        effective_balance = max(0.0, account_balance - locked_profit)

        return CapitalPlan(
            allowed=allowed,
            mode=mode,
            reason=reason,
            leverage=leverage,
            risk_per_trade_pct=risk_pct,
            max_position_pct=max_position_pct,
            max_total_exposure_pct=max_exposure,
            max_correlated_positions=max_correlated,
            effective_balance=effective_balance,
            locked_profit=locked_profit,
            expected_reward_pct=expected_reward_pct,
            expected_rr=expected_rr,
            min_expected_rr=min_expected_rr,
            drawdown_pct=drawdown_pct,
            notes=notes,
        )


capital_allocator = CapitalAllocator()
