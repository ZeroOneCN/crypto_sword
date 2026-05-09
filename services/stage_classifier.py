"""Core breakout-stage classifier.

This module replaces the old Surf-backed ``token_anomaly_radar.py`` path.
It is intentionally data-source agnostic: callers pass normalized Binance
metrics and receive a compact stage/trigger/risk tuple for the trading chain.
"""

from __future__ import annotations

from typing import Any


def classify_breakout_stage(metrics: dict[str, Any]) -> tuple[str, str, str]:
    """Classify a symbol into a trading stage from normalized market metrics.

    Returns:
        tuple(stage, trigger, risk)

    Stages used by the scanner:
        pre_break: early momentum / accumulation before full breakout
        confirmed_breakout: strong confirmed trend
        mania: crowded extreme move, often reverse-risk
        exhaustion: follow-through failure / momentum exhaustion
        neutral: no clean setup
    """

    change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
    change_72h = float(metrics.get("change_72h_pct", 0.0) or 0.0)
    volume_24h = float(metrics.get("volume_24h_mult", 0.0) or 0.0)
    oi_24h = float(metrics.get("oi_24h_pct", 0.0) or 0.0)
    funding = float(metrics.get("funding_rate", 0.0) or 0.0)
    ls_now = float(metrics.get("ls_ratio_now", 0.0) or 0.0)
    ls_prev = float(metrics.get("ls_ratio_prev_24h", 0.0) or 0.0)
    drawdown = float(metrics.get("drawdown_from_24h_high_pct", 0.0) or 0.0)
    rebound = float(metrics.get("rebound_from_24h_low_pct", 0.0) or 0.0)
    range_position = float(metrics.get("range_position_24h_pct", 50.0) or 50.0)

    ls_rising = ls_now > ls_prev
    crowded_longs = ls_now >= 2.5 or (ls_now >= 2.0 and funding > 0.01)
    crowded_shorts = ls_now <= 0.6 or (ls_now <= 0.7 and funding < -0.0005)

    strong_confirmation = change_72h >= 12 and volume_24h >= 1.2 and oi_24h >= 20 and ls_rising
    early_break = change_24h >= 5 and volume_24h >= 1.0 and oi_24h >= 10
    strong_breakdown = change_72h <= -10 and volume_24h >= 1.2 and oi_24h >= 15
    early_breakdown = change_24h <= -5 and volume_24h >= 1.0 and oi_24h >= 10
    top_reversal_short = (
        change_24h >= 6
        and drawdown >= 1.2
        and volume_24h >= 0.8
        and oi_24h >= 7
        and range_position <= 97
    )
    bottom_reversal_long = (
        change_24h <= -6
        and rebound >= 1.2
        and volume_24h >= 0.8
        and oi_24h >= 7
        and range_position >= 3
    )

    overheated_long = change_24h >= 25 or change_72h >= 50 or volume_24h >= 6
    overheated_short = change_24h <= -20 or change_72h <= -45
    failed_followthrough = drawdown >= 8 or (
        funding < 0 and ls_now < ls_prev and abs(change_24h) < 15
    )

    if overheated_long and crowded_longs:
        return (
            "mania",
            "极端多头行情，量价与情绪过热",
            "拥挤多头叠加过热，回调或爆仓风险升高，避免盲目追多",
        )

    if overheated_short and crowded_shorts:
        return (
            "mania",
            "极端空头行情，价格严重超跌",
            "拥挤空头叠加超跌，反弹逼空风险升高，避免盲目追空",
        )

    if strong_confirmation:
        risk = (
            "确认突破，但多头拥挤，需防冲高回落"
            if crowded_longs
            else "确认突破，关注后续量能和OI延续"
        )
        return (
            "confirmed_breakout",
            "72h突破确认 + 放量 + OI上升 + 多空比改善",
            risk,
        )

    if strong_breakdown:
        risk = (
            "确认跌破，但空头拥挤，需防急速反弹"
            if crowded_shorts
            else "确认跌破，关注量能和OI延续"
        )
        return (
            "confirmed_breakout",
            "72h跌破确认 + 放量 + OI上升 + 空头主导",
            risk,
        )

    if top_reversal_short:
        return (
            "pre_break",
            "高位冲高回落 + OI/量能仍活跃",
            "24h仍偏强但短线已从高点回撤，允许转弱做空，需防急反抽",
        )

    if bottom_reversal_long:
        return (
            "pre_break",
            "低位急跌反抽 + OI/量能仍活跃",
            "24h仍偏弱但短线已从低点反弹，允许转强做多，需防二次下杀",
        )

    if early_break:
        risk = (
            "早期突破，但多头略拥挤，需要持续放量确认"
            if crowded_longs
            else "早期异动，重点观察量能和OI能否延续"
        )
        return (
            "pre_break",
            "24h异动 + 放量 + OI上升",
            risk,
        )

    if early_breakdown:
        risk = (
            "早期跌破，但空头略拥挤，需要防反抽"
            if crowded_shorts
            else "早期做空信号，重点观察量能和OI能否延续"
        )
        return (
            "pre_break",
            "24h下跌 + 放量 + OI上升",
            risk,
        )

    if failed_followthrough:
        return (
            "exhaustion",
            "突破动能衰竭，高位回落或低位反弹",
            "动能衰竭，反转风险升高，已有持仓建议降低预期",
        )

    return (
        "neutral",
        "信号证据不足或互相矛盾",
        "没有明确突破结构，建议继续观察",
    )
