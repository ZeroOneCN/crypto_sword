"""Trade review payload builder for post-trade replay/training."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _bucket_change(change_24h: float) -> str:
    change_abs = abs(change_24h)
    if change_abs >= 20:
        return "extended"
    if change_abs >= 8:
        return "active"
    return "quiet"


def _bucket_oi(oi_24h: float) -> str:
    oi_abs = abs(oi_24h)
    if oi_abs >= 60:
        return "overheated"
    if oi_abs >= 18:
        return "expanding"
    if oi_abs >= 6:
        return "warming"
    return "normal"


def _build_outcome_reason(
    *,
    pnl: float,
    exit_reason: str,
    hold_hours: float,
    total_score: float,
    change_24h: float,
    oi_24h: float,
    funding: float,
    volume_mult: float,
    oi_bonus: float,
) -> tuple[str, list[str]]:
    """Return a compact human reason plus machine-friendly reason codes."""
    reason_codes: list[str] = []
    exit_upper = str(exit_reason or "").upper()

    if pnl > 0:
        headline = "盈利：入场方向获得兑现"
        reason_codes.append("outcome_profit")
    elif pnl < 0:
        headline = "亏损：入场后价格未延续"
        reason_codes.append("outcome_loss")
    else:
        headline = "持平：价格波动不足"
        reason_codes.append("outcome_flat")

    if "TAKE_PROFIT" in exit_upper or "TP" in exit_upper:
        reason_codes.append("exit_take_profit")
        headline = "盈利：触发止盈或分批止盈完成" if pnl >= 0 else "亏损：止盈后剩余仓位回撤"
    elif "STOP_LOSS" in exit_upper:
        reason_codes.append("exit_stop_loss")
        headline = "亏损：触发止损，趋势确认失败" if pnl < 0 else "盈利：保护止损锁定收益"
    elif "TRAIL" in exit_upper:
        reason_codes.append("exit_trailing_stop")
        headline = "盈利：追踪止损锁定趋势收益" if pnl >= 0 else "亏损：追踪保护后回撤出场"
    elif "MANUAL" in exit_upper:
        reason_codes.append("exit_manual")

    if total_score >= 75:
        reason_codes.append("entry_high_score")
    elif total_score and total_score < 55:
        reason_codes.append("entry_low_score")

    if oi_bonus > 0:
        reason_codes.append("oi_funding_enhanced")
    if abs(oi_24h) >= 60:
        reason_codes.append("oi_overheated")
    elif abs(oi_24h) >= 18:
        reason_codes.append("oi_expanding")
    if abs(change_24h) >= 20:
        reason_codes.append("price_extended")
    elif abs(change_24h) >= 8:
        reason_codes.append("price_active")
    if abs(funding) >= 0.003:
        reason_codes.append("funding_hot")
    if volume_mult >= 2:
        reason_codes.append("volume_expanded")
    if hold_hours <= 0.25:
        reason_codes.append("very_short_hold")

    return headline, reason_codes


def build_trade_review(
    *,
    symbol: str,
    session_id: str,
    direction: str,
    stage: str,
    strategy_line: str,
    entry_price: float,
    exit_price: float,
    pnl: float,
    pnl_pct: float,
    exit_reason: str,
    hold_hours: float,
    score: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    oi_funding: dict[str, Any] | None = None,
) -> dict[str, Any]:
    score = score or {}
    metrics = metrics or {}
    oi_funding = oi_funding or {}

    total_score = _safe_float(score.get("total_score"))
    confidence = str(score.get("confidence", ""))
    oi_bonus = _safe_float(oi_funding.get("score_bonus"))
    oi_change = _safe_float(oi_funding.get("oi_change_pct"))
    funding = _safe_float(oi_funding.get("funding_current"))
    change_24h = _safe_float(metrics.get("change_24h_pct"))
    volume_mult = _safe_float(metrics.get("volume_24h_mult"))
    oi_24h = _safe_float(metrics.get("oi_24h_pct"))
    outcome_reason, reason_codes = _build_outcome_reason(
        pnl=pnl,
        exit_reason=exit_reason,
        hold_hours=hold_hours,
        total_score=total_score,
        change_24h=change_24h,
        oi_24h=oi_24h,
        funding=funding,
        volume_mult=volume_mult,
        oi_bonus=oi_bonus,
    )

    labels: list[str] = []
    if pnl > 0:
        labels.append("profit")
    elif pnl < 0:
        labels.append("loss")
    else:
        labels.append("flat")
    if "STOP_LOSS" in str(exit_reason).upper():
        labels.append("stop_loss_exit")
    if "TAKE_PROFIT" in str(exit_reason).upper():
        labels.append("take_profit_exit")
    if oi_bonus > 0:
        labels.append("oi_funding_enhanced")

    why_in = {
        "stage": stage,
        "strategy_line": strategy_line,
        "score_total": round(total_score, 2),
        "score_confidence": confidence,
        "change_24h_pct": round(change_24h, 2),
        "volume_24h_mult": round(volume_mult, 2),
        "oi_24h_pct": round(oi_24h, 2),
        "oi_funding_bonus": round(oi_bonus, 2),
        "oi_change_pct": round(oi_change, 2),
        "funding_current": funding,
    }
    why_out = {
        "exit_reason": exit_reason,
        "hold_hours": round(_safe_float(hold_hours), 3),
        "pnl": round(_safe_float(pnl), 4),
        "pnl_pct": round(_safe_float(pnl_pct), 4),
        "outcome_reason": outcome_reason,
        "reason_codes": reason_codes,
    }

    return {
        "schema_version": 2,
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "symbol": symbol.upper(),
        "session_id": session_id,
        "direction": direction,
        "entry_price": _safe_float(entry_price),
        "exit_price": _safe_float(exit_price),
        "labels": labels,
        "outcome": "profit" if pnl > 0 else "loss" if pnl < 0 else "flat",
        "reason_codes": reason_codes,
        "outcome_reason": outcome_reason,
        "training_features": {
            "strategy_line": strategy_line,
            "stage": stage,
            "score_total": round(total_score, 2),
            "change_bucket": _bucket_change(change_24h),
            "oi_bucket": _bucket_oi(oi_24h),
            "funding_current": funding,
            "volume_24h_mult": round(volume_mult, 2),
            "hold_hours": round(_safe_float(hold_hours), 3),
            "exit_reason": exit_reason,
        },
        "why_in": why_in,
        "why_out": why_out,
    }
