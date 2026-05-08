# -*- coding: utf-8 -*-
"""Shared Telegram labels and formatting helpers."""

from __future__ import annotations

import html
import re
from typing import Any

def _fmt_price(price: float) -> str:
    """Smart price formatting: BTC-like → 2 dec, mid → 4 dec, cheap → 6-8 dec."""
    if price <= 0:
        return "0"
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.4f}"
    if price >= 0.01:
        return f"{price:,.4f}"
    if price >= 0.0001:
        return f"{price:,.6f}"
    return f"{price:,.8f}"

def _fmt_usdt(amount: float) -> str:
    """Smart USDT amount formatting: big→2dp, medium→4dp, small→6dp.
    Balances, PnL, risk amounts that are USDT-denominated."""
    try:
        value = float(amount)
    except Exception:
        value = 0.0
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value <= 0:
        return "0"
    if value >= 100:
        return f"{sign}{value:,.2f}"
    if value >= 1:
        return f"{sign}{value:,.4f}"
    if value >= 0.0001:
        return f"{sign}{value:,.6f}"
    return f"{sign}{value:,.8f}"

def _fmt_price_code(price: float) -> str:
    """Like _fmt_price but wrapped in <code> tags."""
    return f"<code>{_fmt_price(price)}</code>"

def _normalize_telegram_value(value: Any) -> str:
    text = str(value or "").strip().strip('"').strip("'")
    return text

def _sanitize_token_preview(token: str) -> str:
    token = _normalize_telegram_value(token)
    if len(token) <= 10:
        return token
    return f"{token[:6]}...{token[-4:]}"

def _looks_like_placeholder(value: str) -> bool:
    upper = value.upper()
    return upper.startswith("YOUR_") or "PLACEHOLDER" in upper or upper in {"BOT_TOKEN", "CHAT_ID"}

def _is_valid_bot_token(token: str) -> bool:
    token = _normalize_telegram_value(token)
    if token.startswith("bot"):
        token = token[3:]
    return bool(re.match(r"^\d{6,}:[A-Za-z0-9_-]{20,}$", token))

def _escape(value: Any) -> str:
    """Escape dynamic content for Telegram HTML messages."""
    return html.escape(str(value), quote=False)

def _strip_html(message: str) -> str:
    """Convert a simple Telegram HTML message to plain text."""
    text = re.sub(r"(?i)<br\\s*/?>", "\n", message)
    text = re.sub(r"</?(b|code|i|u|s|pre)>", "", text)
    return html.unescape(text)

def _fmt_num(value: Any, decimals: int = 6) -> str:
    """Format noisy float values for Telegram."""
    try:
        return f"{float(value):.{decimals}f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)

def format_direction_label(direction: Any) -> str:
    """Translate internal side/direction codes into user-facing Chinese."""
    key = str(direction or "").upper()
    mapping = {
        "LONG": "做多",
        "BUY": "做多",
        "CONSIDER_LONG": "偏多",
        "SHORT": "做空",
        "SELL": "做空",
        "CONSIDER_SHORT": "偏空",
    }
    return mapping.get(key, str(direction or "未知方向"))

def _format_close_reason_label(reason: Any) -> str:
    """Translate close reason codes into concise Chinese labels."""
    key = str(reason or "").upper()
    mapping = {
        "PROTECTIVE_STOP_EXCHANGE": "防守止损盈利离场",
        "PROTECTIVE_STOP": "防守止损盈利离场",
        "TAKE_PROFIT_TP_FULL_EXCHANGE": "TP1/TP2/TP3 全部成交｜交易所分批止盈完成",
        "TAKE_PROFIT_EXCHANGE": "交易所止盈完成",
        "TAKE_PROFIT_LOCAL_FALLBACK": "本地止盈兜底",
        "TAKE_PROFIT": "止盈触发",
        "STOP_LOSS_EXCHANGE": "交易所止损触发",
        "STOP_LOSS": "止损触发",
        "TRAILING_STOP": "追踪止损触发",
        "TRAILING": "追踪止损触发",
        "MANUAL": "手动平仓",
        "ENTRY_PROTECTION_FAILED": "开仓保护失败回滚",
        "SIDEWAYS_TIMEOUT": "横盘超时退出",
        "SIDEWAYS_REPLACED_BY_STRONG_SIGNAL": "横盘仓位被强信号替换",
        "EARLY_PROFIT_EXCHANGE": "提前微利退出（未达TP1）",
        "EARLY_PROFIT": "提前微利退出（未达TP1）",
        "EXCHANGE_REALIZED_EXCHANGE": "交易所已实现盈亏同步",
        "EXCHANGE_REALIZED": "交易所已实现盈亏同步",
        "FILLED": "完全成交",
        "PARTIALLY_FILLED": "部分成交",
        "CANCELED": "已撤销",
        "EXPIRED": "已过期",
        "REJECTED": "已拒绝",
        "UNKNOWN": "未知原因",
    }
    if key in mapping:
        return mapping[key]
    if "TAKE_PROFIT" in key:
        return "止盈触发"
    if "STOP_LOSS" in key:
        return "止损触发"
    if "TRAIL" in key:
        return "追踪止损触发"
    if "EXCHANGE_REALIZED" in key:
        return "交易所已实现盈亏同步"
    if "EARLY_PROFIT" in key:
        return "提前微利退出（未达TP1）"
    return key if key else "未知原因"

def format_entry_failure_detail(detail: Any) -> str:
    """Translate common exchange/execution failures while preserving the raw clue."""
    raw = str(detail or "").strip()
    lower = raw.lower()
    if not raw:
        return "未提供具体失败原因"

    reason = ""
    if "leverage" in lower or "杠杆" in raw:
        reason = "杠杆设置或复查失败，系统已拒绝继续开仓"
    elif "min notional" in lower or "notional" in lower or "名义" in raw:
        reason = "名义价值低于交易所最低下单要求"
    elif "insufficient" in lower or "margin" in lower or "balance" in lower or "余额" in raw or "保证金" in raw:
        reason = "可用余额或保证金不足"
    elif "precision" in lower or "ticksize" in lower or "stepsize" in lower:
        reason = "价格或数量精度不符合交易所规则"
    elif "reduceonly" in lower or "reduce only" in lower:
        reason = "交易所拒绝 reduce-only 参数"
    elif "immediately trigger" in lower or "would immediately trigger" in lower:
        reason = "保护单价格会立即触发，交易所拒绝挂单"
    elif "connection reset" in lower or "errno 104" in lower:
        reason = "网络连接被重置，请求未稳定完成"
    elif "timeout" in lower or "timed out" in lower:
        reason = "交易所请求超时"
    elif "http 400" in lower:
        reason = "交易所参数校验失败"
    elif "liquidity" in lower or "流动性" in raw:
        reason = "流动性过滤未通过"
    elif "slippage" in lower or "滑点" in raw or "价格偏移" in raw:
        reason = "下单前价格偏移或滑点超过阈值"
    elif "risk" in lower or "风控" in raw or "敞口" in raw:
        reason = "风控条件未通过"

    if not reason:
        return raw
    if raw == reason:
        return reason
    return f"{reason}\n原始信息：{raw}"

def _format_take_profit_targets(
    targets: list[dict[str, Any]] | None,
    entry_price: float = 0.0,
    direction: str = "LONG",
) -> str:
    """Format staged take-profit targets for Telegram."""
    if not targets:
        return ""

    lines: list[str] = []
    for target in targets:
        roi_pct = float(target.get("target_roi_pct", 0) or 0)
        price_move_pct = float(target.get("price_move_pct", 0) or 0)
        price = float(target.get("price", 0) or 0)
        ratio = float(target.get("ratio", 0) or 0) * 100
        quantity = float(target.get("quantity", 0) or 0)
        level = int(target.get("level", len(lines) + 1) or (len(lines) + 1))
        expected_pnl = 0.0
        if entry_price > 0 and price > 0 and quantity > 0:
            if direction == "LONG":
                expected_pnl = (price - entry_price) * quantity
            else:
                expected_pnl = (entry_price - price) * quantity
        expected_text = f" | 预计 +{_fmt_usdt(expected_pnl)} USDT" if expected_pnl > 0 else ""
        lines.append(
            f"TP{level}: "
            f"<code>{price_move_pct:.2f}% 价格 / {roi_pct:.2f}% ROI</code> →<code>{_fmt_price(price)}</code> "
            f"({ratio:.0f}% / {_fmt_num(quantity)}){expected_text}"
        )
    return "\n".join(lines)

def _format_oi_funding_brief(oi_funding: dict[str, Any] | None) -> str:
    """Format compact OI/Funding enhancement details for notifications."""
    if not oi_funding:
        return ""
    bonus = float(oi_funding.get("score_bonus", 0) or 0)
    oi_change = float(oi_funding.get("oi_change_pct", 0) or 0)
    funding_current = float(oi_funding.get("funding_current", 0) or 0)
    turned_negative = bool(oi_funding.get("turned_negative", False))
    oi_signal = bool(oi_funding.get("oi_signal", False))
    oi_1h_surge = bool(oi_funding.get("oi_1h_surge", False))
    oi_1h_change = float(oi_funding.get("oi_1h_change_pct", 0) or 0)
    breakdown = oi_funding.get("bonus_breakdown") or []

    tags: list[str] = []
    if turned_negative:
        tags.append("费率转负")
    if oi_signal:
        tags.append("OI扩张")
    if oi_1h_surge:
        tags.append(f"OI1h骤升{oi_1h_change:+.1f}%")
    if not tags and bonus > 0:
        tags.append("评分加成")
    tag_text = " / ".join(tags) if tags else "无"

    # bonus 明细
    bonus_text = f"+{bonus:.1f}"
    if breakdown:
        bonus_text += " (" + " ".join(breakdown) + ")"

    return (
        f"<b>OI/Funding</b>  <code>{_escape(tag_text)}</code> | "
        f"加分 <code>{_escape(bonus_text)}</code> | "
        f"OI <code>{oi_change:+.1f}%</code> | "
        f"Funding <code>{funding_current:+.4%}</code>"
    )

def _format_source_label(source: str) -> str:
    """Translate internal event sources into concise user-facing Chinese labels."""
    source_map = {
        "entry_confirm": "开仓后确认",
        "startup_audit": "启动巡检",
        "audit": "保护巡检",
        "protection_reconcile": "保护单补挂",
        "ws_account_update": "WS账户同步",
        "rest_sync": "REST持仓同步",
        "manual": "手动触发",
    }
    return source_map.get(source, source)

def _format_component_label(component: str) -> str:
    """Translate internal component names into concise user-facing Chinese labels."""
    component_map = {
        "account_query": "账户查询",
        "breakeven_stop": "保本止损",
        "entry_protection": "开仓保护单",
        "execute_entry": "开仓流程",
        "execute_entry_trade": "开仓下单",
        "execute_exit": "平仓流程",
        "loss_guard": "连亏熔断",
        "main_loop": "主循环",
        "protection_guard": "保护单风控",
        "protection_cleanup": "保护条件单清理",
        "protection_reconcile": "保护单补挂",
        "risk_assessment": "风控评估",
        "risk_guard": "风险守卫",
        "startup_checks": "启动健康检查",
        "stop_loss_cleanup": "止损单清理",
    }
    return component_map.get(component, component)

def _format_error_type_label(error_type: Any) -> str:
    """Translate common legacy English error types."""
    raw = str(error_type or "")
    mapping = {
        "startup health checks failed": "启动健康检查失败",
        "Main loop exception": "主循环异常",
        "main loop exception": "主循环异常",
    }
    return mapping.get(raw, raw)

def _format_protection_failure_detail(detail: str) -> str:
    """Translate protection failure internals into user-facing Chinese text."""
    raw = str(detail or "").strip()
    lower = raw.lower()

    if "stop_loss" in lower and "status=error" in lower and "id=0" in lower:
        return "止损保护单创建失败"
    if "stop_loss" in lower and "missing" in lower:
        return "止损保护单缺失"
    if "tp" in lower and "status=error" in lower and "id=0" in lower:
        return "止盈保护单创建失败"
    if "take_profit" in lower and "missing" in lower:
        return "止盈保护单缺失"
    if "protection_deferred" in lower:
        return "保护单被延后创建"
    if "reduceonly" in lower or "reduce only" in lower:
        return "交易所拒绝 reduce-only 参数"
    if "immediately trigger" in lower:
        return "保护单价格会立即触发"
    if "min notional" in lower or "notional" in lower:
        return "名义价值低于交易所最低要求"
    if "precision" in lower:
        return "价格或数量精度不符合交易所规则"
    return raw or "未知保护单失败"

def format_protection_failure_detail(detail: Any) -> str:
    """Public wrapper for protection failure translation."""
    return _format_protection_failure_detail(str(detail or ""))

def _humanize_close_reason(reason: str) -> str:
    return _format_close_reason_label(reason)

def _format_duration_from_hours(duration_hours: float) -> str:
    total_minutes = max(1, int(round(float(duration_hours or 0) * 60)))
    if total_minutes < 60:
        return f"{total_minutes}分钟"
    hours, minutes = divmod(total_minutes, 60)
    if minutes <= 0:
        return f"{hours}小时"
    return f"{hours}小时{minutes}分钟"

