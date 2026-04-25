"""Trade review payload builder for post-trade replay/training."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


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
    }

    return {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "symbol": symbol.upper(),
        "session_id": session_id,
        "direction": direction,
        "entry_price": _safe_float(entry_price),
        "exit_price": _safe_float(exit_price),
        "labels": labels,
        "why_in": why_in,
        "why_out": why_out,
    }
