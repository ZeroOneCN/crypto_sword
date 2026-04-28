# -*- coding: utf-8 -*-
"""Telegram notification helpers for Hermes Trader.

This module is intentionally self-contained (no third-party deps) and focuses on:
- loading Telegram config
- sending rate-limited messages
- formatting common trading notifications (HTML by default)
"""

from __future__ import annotations

# Emoji constants for Telegram messages
_E = "\U0001f7e2"  # green circle
_E2 = "\U0001f534"  # red circle
_E3 = "\U0001f6d1"  # stop sign

import html
import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Thread-safe message queue / rate limit
_telegram_lock = threading.Lock()
_last_message_time = 0.0
_min_message_interval = 0.5  # seconds (avoid hitting Telegram rate limits)


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


def _hermes_home() -> Path:
    """Return Hermes home dir (cross-platform).

    Priority:
      1) $HERMES_HOME
      2) ~/.hermes
    """
    env = os.environ.get("HERMES_HOME")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.home() / ".hermes").resolve()


def get_telegram_config() -> dict[str, Any]:
    """Load Telegram config from environment or json file.

    Supported:
      - env: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
      - repo: ./config/telegram.json (recommended for local dev)
      - user: ~/.hermes/config/telegram.json (recommended for prod)
    """
    config: dict[str, Any] = {
        "bot_token": _normalize_telegram_value(os.environ.get("TELEGRAM_BOT_TOKEN", "")),
        "chat_id": _normalize_telegram_value(os.environ.get("TELEGRAM_CHAT_ID", "")),
    }

    if config["bot_token"] and config["chat_id"]:
        return config

    candidates = [
        (Path(__file__).resolve().parent / "config" / "telegram.json"),
        (_hermes_home() / "config" / "telegram.json"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                file_config = json.load(f) or {}
            config["bot_token"] = config["bot_token"] or _normalize_telegram_value(file_config.get("bot_token", ""))
            config["chat_id"] = config["chat_id"] or _normalize_telegram_value(file_config.get("chat_id", ""))
            break
        except Exception as e:
            logger.warning(f"Failed to load Telegram config from {path}: {e}")

    if config["bot_token"].startswith("bot"):
        config["bot_token"] = config["bot_token"][3:]

    return config


def send_telegram_message(message: str, parse_mode: str | None = "HTML") -> bool:
    """Send a message to Telegram (thread-safe with simple rate limiting)."""
    global _last_message_time

    config = get_telegram_config()
    token = _normalize_telegram_value(config.get("bot_token", ""))
    chat_id = _normalize_telegram_value(config.get("chat_id", ""))

    if not token or not chat_id:
        logger.warning("Telegram not configured - skipping notification")
        logger.info(f"[TG] {message}")
        return False

    if _looks_like_placeholder(token) or _looks_like_placeholder(chat_id):
        logger.error(
            "Telegram config uses placeholder values. "
            "Please set real TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID or config/telegram.json."
        )
        return False

    if not _is_valid_bot_token(token):
        logger.error(
            f"Telegram bot token format invalid: {_sanitize_token_preview(token)} "
            "(expected <digits>:<secret>)"
        )
        return False

    with _telegram_lock:
        elapsed = time.time() - _last_message_time
        if elapsed < _min_message_interval:
            time.sleep(_min_message_interval - elapsed)

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        try:
            import urllib.error
            import urllib.request

            attempts: list[tuple[str, str | None]] = [(message, parse_mode)]
            if parse_mode is not None:
                attempts.append((_strip_html(message), None))

            for index, (attempt_message, attempt_parse_mode) in enumerate(attempts):
                payload: dict[str, Any] = {"chat_id": chat_id, "text": attempt_message}
                if attempt_parse_mode:
                    payload["parse_mode"] = attempt_parse_mode

                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        result = json.loads(response.read().decode("utf-8"))
                        _last_message_time = time.time()
                        if result.get("ok"):
                            if index > 0:
                                logger.warning("Telegram HTML failed; plain-text fallback sent successfully")
                            else:
                                logger.info("Telegram message sent successfully")
                            return True
                        logger.error(f"Telegram API error: {result}")
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="replace")
                    logger.error(f"Telegram HTTP {e.code}: {body[:300]}")
                    if e.code == 404:
                        logger.error(
                            "Telegram endpoint 404, usually caused by invalid bot token. "
                            f"token={_sanitize_token_preview(token)}"
                        )
                    if e.code == 400 and index + 1 < len(attempts):
                        continue
                    raise

            return False
        except Exception as e:
            _last_message_time = time.time()
            logger.error(f"Failed to send Telegram message: {e}")
            return False


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


def _format_take_profit_targets(targets: list[dict[str, Any]] | None) -> str:
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
        lines.append(
            f"TP{level}: "
            f"<code>{price_move_pct:.2f}% 价格 / {roi_pct:.2f}% ROI</code> →<code>${price:,.4f}</code> "
            f"({ratio:.0f}% / {_fmt_num(quantity)})"
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

    tags: list[str] = []
    if turned_negative:
        tags.append("funding_turn")
    if oi_signal:
        tags.append("oi_expand")
    if not tags and bonus > 0:
        tags.append("score_boost")
    tag_text = " / ".join(tags) if tags else "none"

    return (
        f"<b>OI/Funding</b>  <code>{_escape(tag_text)}</code> | "
        f"Bonus <code>+{bonus:.1f}</code> | "
        f"OI <code>{oi_change:+.1f}%</code> | "
        f"Funding <code>{funding_current:+.4%}</code>"
    )


def format_open_position_msg(
    symbol: str,
    direction: str,
    entry_price: float,
    quantity: float,
    leverage: int,
    stop_loss: float,
    take_profit: float,
    risk_amount: float,
    risk_pct: float,
    score: float = 0,
    risk_level: str = "",
    session_id: str = "",
    strategy_line: str = "",
    oi_funding: dict[str, Any] | None = None,
    target_roi_pct: float = 0,
    price_move_pct: float = 0,
    take_profit_targets: list[dict[str, Any]] | None = None,
) -> str:
    """格式化开仓通知"""
    direction_emoji = _E if direction == "LONG" else _E2
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"

    sl_pct = abs(entry_price - stop_loss) / entry_price * 100 if entry_price else 0.0
    tp_pct = abs(take_profit - entry_price) / entry_price * 100 if entry_price else 0.0
    notional_value = entry_price * quantity

    msg = f"""{direction_emoji} <b>宙斯交易中枢 | 开仓成功</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>方向</b>  {direction_text}
<b>入场</b>  <code>${entry_price:,.4f}</code>
<b>杠杆</b>  {leverage}x
<b>仓位</b>  <code>{_fmt_num(quantity)}</code>  |  名义 <code>{notional_value:,.2f} USDT</code>
<b>止损</b>  <code>${stop_loss:,.4f}</code>  ({sl_pct:.2f}%)
<b>止盈</b>  <code>${take_profit:,.4f}</code>  ({tp_pct:.2f}%)
<b>风险</b>  <code>${risk_amount:.2f}</code>  |  {risk_pct:.2f}%"""

    if strategy_line:
        msg += f"\n<b>策略</b>  <code>{_escape(strategy_line)}</code>"
    oi_funding_line = _format_oi_funding_brief(oi_funding)
    if oi_funding_line:
        msg += f"\n{oi_funding_line}"
    if target_roi_pct > 0:
        msg += f"\n<b>目标收益率</b>  <code>{target_roi_pct:.2f}% ROI</code>"
    if price_move_pct > 0:
        msg += f"\n<b>实际价格目标</b>  <code>{price_move_pct:.2f}%</code>"
    if take_profit_targets:
        msg += f"\n<b>分批止盈</b>\n{_format_take_profit_targets(take_profit_targets)}"

    if score > 0:
        confidence = "极高" if score >= 80 else ("高" if score >= 60 else ("中" if score >= 40 else "低"))
        msg += f"\n<b>评分</b>  <code>{score:.0f}/100</code>  |  {confidence}"
    if risk_level:
        msg += f"\n<b>风险等级</b>  <code>{_escape(risk_level)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"

    return msg


def _humanize_close_reason(reason: str) -> str:
    reason_key = (reason or "").upper()
    if reason_key == "TAKE_PROFIT_TP_FULL_EXCHANGE":
        return "TP1/TP2/TP3 全部成交｜交易所分批止盈完成"
    if reason_key == "TAKE_PROFIT_EXCHANGE":
        return "交易所止盈完成"
    if reason_key == "STOP_LOSS_EXCHANGE":
        return "交易所止损触发"
    return reason


def format_close_position_msg(
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    pnl: float,
    pnl_pct: float,
    reason: str,
    duration_hours: float = 0,
    session_id: str = "",
    strategy_line: str = "",
    oi_funding: dict[str, Any] | None = None,
    roi_pct: float = 0.0,
    price_move_pct: float = 0.0,
) -> str:
    """格式化平仓通知"""
    direction_emoji = _E if pnl >= 0 else _E2
    pnl_emoji = _E if pnl >= 0 else _E2
    pnl_sign = "+" if pnl >= 0 else ""
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"

    msg = f"""{direction_emoji} <b>宙斯交易中枢 | 平仓完成</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>方向</b>  {direction_text}
<b>入场</b>  <code>${entry_price:,.4f}</code>
<b>出场</b>  <code>${exit_price:,.4f}</code>
<b>数量</b>  <code>{_fmt_num(quantity)}</code>
<b>盈亏</b>  {pnl_emoji} <b>{pnl_sign}${pnl:,.2f}</b>  ({pnl_sign}{pnl_pct:.2f}%)
<b>原因</b>  <code>{_escape(_humanize_close_reason(reason))}</code>"""

    if strategy_line:
        msg += f"\n<b>策略</b>  <code>{_escape(strategy_line)}</code>"
    oi_funding_line = _format_oi_funding_brief(oi_funding)
    if oi_funding_line:
        msg += f"\n{oi_funding_line}"
    if price_move_pct:
        msg += f"\n<b>价格涨幅</b>  <code>{price_move_pct:+.2f}%</code>"
    if roi_pct:
        msg += f"\n<b>实际ROI</b>  <code>{roi_pct:+.2f}%</code>"
    if duration_hours > 0:
        msg += f"\n<b>持仓</b>  {duration_hours:.1f} 小时"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"

    return msg


def format_partial_take_profit_msg(
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    remaining_quantity: float,
    pnl: float,
    pnl_pct: float,
    level: int = 0,
    session_id: str = "",
    strategy_line: str = "",
) -> str:
    """格式化分批止盈成交通知"""
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"
    pnl_sign = "+" if pnl >= 0 else ""
    level_text = f"TP{level}" if level else "部分止盈"

    msg = f"""🔄 <b>宙斯交易中枢 | 分批止盈成交</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>方向</b>  {direction_text}
<b>档位</b>  <code>{_escape(level_text)}</code>
<b>入场</b>  <code>${entry_price:,.4f}</code>
<b>成交价格</b>  <code>${exit_price:,.4f}</code>
<b>止盈数量</b>  <code>{_fmt_num(quantity)}</code>
<b>剩余数量</b>  <code>{_fmt_num(remaining_quantity)}</code>
<b>本次盈亏</b>  {_E} <b>{pnl_sign}${pnl:,.2f}</b>  ({pnl_sign}{pnl_pct:.2f}%)"""

    if strategy_line:
        msg += f"\n<b>策略</b>  <code>{_escape(strategy_line)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"
    return msg


def format_protection_status_msg(
    symbol: str,
    stop_loss_ok: bool,
    take_profit_ok: bool,
    stop_loss_order_id: int = 0,
    take_profit_order_ids: list[int] | None = None,
    session_id: str = "",
    source: str = "audit",
    message: str = "",
) -> str:
    """Format exchange-side protection status notification."""
    tp_ids = take_profit_order_ids or []
    ok = stop_loss_ok and take_profit_ok
    title = "🛡️ <b>宙斯交易中枢 | 保护单确认</b>" if ok else "⚠️ <b>宙斯交易中枢 | 裸仓风险</b>"
    status_text = "✅ 已受保护" if ok else "❌ 保护不完整"
    sl_text = f"✅ {stop_loss_order_id}" if stop_loss_ok else "❌ 缺失"
    tp_text = f"✅ {', '.join(str(x) for x in tp_ids)}" if take_profit_ok else "❌ 缺失"

    msg = f"""{title}

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>状态</b>  <code>{_escape(status_text)}</code>
<b>止损单</b>  <code>{_escape(sl_text)}</code>
<b>止盈单</b>  <code>{_escape(tp_text)}</code>
<b>来源</b>  <code>{_escape(source)}</code>"""

    if message:
        msg += f"\n<b>说明</b>  <code>{_escape(message)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"
    return msg


def format_latency_alert_msg(
    flow: str,
    total_ms: float,
    steps: list[tuple[str, float]],
    symbol: str = "",
    threshold_ms: float = 5000,
) -> str:
    """Format slow-path latency alert."""
    slowest_name = ""
    slowest_ms = 0.0
    if steps:
        slowest_name, slowest_ms = max(steps, key=lambda item: item[1])

    lines = [
        "⚡ <b>宙斯交易中枢 | 延迟警告</b>",
        "",
        f"<b>流程</b>  <code>{_escape(flow)}</code>",
        f"<b>总耗时</b>  <code>{total_ms:.0f} ms</code>",
        f"<b>阈值</b>  <code>{threshold_ms:.0f} ms</code>",
    ]
    if symbol:
        lines.append(f"<b>标的</b>  <code>{_escape(symbol)}</code>")
    if slowest_name:
        lines.append(f"<b>最慢步骤</b>  <code>{_escape(slowest_name)} {slowest_ms:.0f} ms</code>")

    if steps:
        lines.append("")
        lines.append("<b>分段耗时</b>")
        for name, elapsed_ms in steps:
            lines.append(f"•{_escape(name)}  <code>{elapsed_ms:.0f} ms</code>")

    return "\n".join(lines)


def format_summary_msg(
    positions: list,
    total_pnl: float,
    realized_pnl: float,
    total_balance: float = 0.0,
    available_balance: float = 0.0,
    daily_stats: dict | None = None,
) -> str:
    """格式化持仓汇总通知"""
    msg = f"""📊 <b>宙斯交易中枢 | 持仓汇总</b>

<b>持仓数</b>  <code>{len(positions)}</code>
<b>未实现</b>  <code>{total_pnl:+,.2f} USDT</code>
<b>已实现</b>  <code>{realized_pnl:.2f} USDT</code>"""
    if total_balance > 0:
        msg += f"\n<b>总余额</b>  <code>{total_balance:,.2f} USDT</code>"
    if available_balance > 0:
        msg += f"\n<b>可用余额</b>  <code>{available_balance:,.2f} USDT</code>"

    if not positions:
        msg += "\n\n📭 <b>当前无持仓</b>\n<code>系统保持待命，等待下一次高质量入场信号。</code>"
        return msg

    for i, pos in enumerate(positions, 1):
        pnl = float(pos.get("unrealized_pnl", 0) or 0)
        pnl_sign = "+" if pnl >= 0 else ""
        pnl_emoji = _E if pnl >= 0 else _E2
        side = pos.get("side", "UNKNOWN")
        current_price = float(pos.get("current_price", 0) or 0)
        take_profit_display = pos.get("take_profit_targets_text") or f"${float(pos.get('take_profit', 0) or 0):,.4f}"

        msg += f"""

<b>{i}.</b> <code>{_escape(pos.get('symbol', 'UNKNOWN'))}</code>  {side}
入场 <code>${float(pos.get('entry_price', 0) or 0):,.4f}</code>  |  现价 <code>${current_price:,.4f}</code>
止损 <code>${float(pos.get('stop_loss', 0) or 0):,.4f}</code>  |  止盈 <code>{_escape(take_profit_display)}</code>
盈亏 {pnl_emoji} <code>{pnl_sign}${pnl:,.2f}</code>  ({float(pos.get('unrealized_pnl_pct', 0) or 0):+.2f}%)"""

    return msg


def format_error_msg(
    error_type: str,
    message: str,
    symbol: str | None = None,
    session_id: str = "",
    component: str = "",
) -> str:
    """格式化错误通知"""
    msg = f"""❌ <b>宙斯交易中枢 | 交易异常</b>

<b>类型</b>  <code>{_escape(error_type)}</code>"""
    if component:
        msg += f"\n<b>组件</b>  <code>{_escape(component)}</code>"
    if symbol:
        msg += f"\n<b>标的</b>  <code>{_escape(symbol)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"
    msg += f"\n<b>详情</b>\n<code>{_escape(message)}</code>"
    return msg


def format_startup_msg(
    mode_name: str,
    leverage: int,
    risk_pct: float,
    stop_loss_pct: float,
    take_profit_pct: float,
    scan_top_n: int,
    scan_interval_sec: int,
    max_positions: int,
    take_profit_mode: str = "roi",
    trailing_stop_pct: float = 0.0,
    trailing_enabled: bool = False,
) -> str:
    """格式化启动通知"""
    tp_mode_label = "ROI收益率" if take_profit_mode == "roi" else "标的价格"
    trailing_text = f" | 追踪 {trailing_stop_pct}%" if trailing_enabled else ""
    return f"""🚀 <b>宙斯交易中枢 | 系统启动</b>

<b>模式</b>  <code>{_escape(mode_name)}</code>
<b>杠杆</b>  {leverage}x
<b>单笔风险</b>  {risk_pct:.2f}%
<b>止损(基础)</b>  {stop_loss_pct:.2f}%
<b>止盈(基础)</b>  {take_profit_pct:.2f}% ({tp_mode_label}){trailing_text}
<b>止盈策略</b>  分批3档 (50%/100%/150% x 基础TP)
<b>扫描范围</b>  前<code>{scan_top_n}</code> 个币种
<b>扫描间隔</b>  <code>{scan_interval_sec}</code> 秒
<b>最大持仓</b>  <code>{max_positions}</code> 个
<b>提示</b>  实际TP/SL因策略线(突破/回踩)乘数不同而浮动"""


def format_shutdown_msg(
    mode_name: str,
    closed_trades: int,
    realized_pnl: float,
    unrealized_pnl: float,
) -> str:
    """格式化停机通知"""
    return f"""{_E3} <b>宙斯交易中枢 | 系统停止</b>

<b>模式</b>  <code>{_escape(mode_name)}</code>
<b>已平仓</b>  <code>{closed_trades}</code> 笔
<b>已实现</b>  <code>{realized_pnl:.2f} USDT</code>
<b>未实现</b>  <code>{unrealized_pnl:.2f} USDT</code>"""


def format_signal_message(signal: dict[str, Any], trade_result: dict[str, Any]) -> str:
    """Format a trading signal as a Telegram HTML message."""
    symbol = signal.get("symbol", "UNKNOWN")
    stage = signal.get("stage", "UNKNOWN")
    direction = signal.get("direction", "UNKNOWN")
    metrics = signal.get("metrics", {}) or {}
    trade = trade_result or {}

    direction_emoji = "🟢" if direction == "LONG" else "🔴" if direction == "SHORT" else "🟡"
    stage_emoji = {
        "pre_break": "⚡",
        "confirmed_breakout": "🚀",
        "mania": "🔥",
        "exhaustion": "⚠️",
    }.get(stage, "📊")

    stage_label = {
        "pre_break": "预突破",
        "confirmed_breakout": "确认突破",
        "mania": "过热",
        "exhaustion": "衰竭",
    }.get(stage, str(stage))
    stage = stage_label

    action = trade.get("action", "UNKNOWN")
    action_emoji = {
        "EXECUTED": "✅",
        "SKIPPED": "🔔",
        "FAILED": "❌",
    }.get(action, "📦")

    change_24h = float(metrics.get("change_24h_pct", metrics.get("change_24h", 0)) or 0)
    oi_24h = float(metrics.get("oi_24h_pct", metrics.get("oi_24h", 0)) or 0)
    funding = float(metrics.get("funding_rate", metrics.get("funding", 0)) or 0)
    ls_ratio = float(metrics.get("ls_ratio_now", metrics.get("ls_ratio", 0)) or 0)

    msg = f"""{stage_emoji}{stage_emoji} <b>BREAKOUT SIGNAL</b> {stage_emoji}{stage_emoji}

{direction_emoji} <b>{_escape(symbol)}</b>  <code>{_escape(direction)}</code>
📊 <b>阶段</b>  <code>{_escape(stage)}</code>
{action_emoji} <b>Action</b>  <code>{_escape(action)}</code>

<b>Market Metrics</b>
•24h Change  <code>{change_24h:+.2f}%</code>
•OI 24h  <code>{oi_24h:+.2f}%</code>
•Funding  <code>{funding:.6f}</code>
•L/S Ratio  <code>{ls_ratio:.2f}</code>"""

    if action == "EXECUTED":
        msg += f"""

<b>Trade Details</b>
•Entry Price  <code>${float(trade.get('entry_price', 0) or 0):,.4f}</code>
•Quantity  <code>{_fmt_num(trade.get('quantity', 0))}</code>
•Position Value  <code>${float(trade.get('position_value_usdt', 0) or 0):,.2f}</code>
•Stop Loss  <code>${float(trade.get('stop_loss_price', 0) or 0):,.4f}</code>
•Risk  <code>${float(trade.get('risk_amount_usdt', 0) or 0):,.2f}</code>"""
    else:
        msg += f"\n\n<b>Reason</b>  <code>{_escape(trade.get('reason', 'N/A'))}</code>"

    return msg


def send_signal_alert(signal: dict[str, Any], trade_result: dict[str, Any]) -> bool:
    """Send a signal alert to Telegram."""
    return send_telegram_message(format_signal_message(signal, trade_result))


def format_scan_monitor_msg(
    signals: list[dict[str, Any]],
    scanned_count: int = 0,
    max_items: int = 5,
    report_title: str = "宙斯交易中枢 | 币种扫描报告",
    count_label: str = "扫描数量",
) -> str:
    """Format a compact real-time scanner monitor report."""
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"""📡 <b>{_escape(report_title)}</b>
<code>{now_text}</code>

<b>{_escape(count_label)}</b>  <code>{scanned_count}</code>
<b>有效信号</b>  <code>{len(signals)}</code>"""

    if not signals:
        return msg + "\n\n📭 暂无有效信号"

    for item in signals[:max_items]:
        symbol = item.get("symbol", "UNKNOWN")
        direction = item.get("direction", "UNKNOWN")
        score = float((item.get("score") or {}).get("total_score", 0) or 0)
        confidence = (item.get("score") or {}).get("confidence", "")
        metrics = item.get("metrics", {}) or {}

        funding_pct = float(metrics.get("funding_rate", 0) or 0) * 100
        price = float(metrics.get("last_price", item.get("price", 0)) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_24h = float(metrics.get("oi_24h_pct", 0) or 0)

        entry_status_text = item.get("entry_status_text", "")
        entry_note = item.get("entry_note", "")
        strategy_line = item.get("strategy_line", "")
        watch_stage = item.get("watch_stage", "")

        tag = "跟多信号" if direction in {"LONG", "CONSIDER_LONG"} else "跟空信号"
        if funding_pct <= -0.5:
            funding_tag = "极负费率"
        elif funding_pct < 0:
            funding_tag = "负费率"
        else:
            funding_tag = "正费率"

        msg += f"""

🧭 <b>{_escape(tag)} ({_escape(funding_tag)})</b>
•<code>{_escape(symbol)}</code> 评分 <code>{score:.1f}</code> {_escape(str(confidence))}
•费率 <code>{funding_pct:+.4f}%</code>  价格 <code>{change_24h:+.2f}%</code>
•OI <code>{oi_24h:+.2f}%</code>  现价 <code>${price:,.6f}</code>"""
        if strategy_line:
            msg += f"\n•策略 <code>{_escape(strategy_line)}</code>"
        if watch_stage:
            msg += f"\n•阶段 <code>{_escape(watch_stage)}</code>"
        if entry_status_text:
            msg += f"\n•状态 <code>{_escape(entry_status_text)}</code>"
        if entry_note:
            msg += f"\n•说明 <code>{_escape(entry_note)}</code>"

    return msg


def format_daily_report_msg(report: dict[str, Any]) -> str:
    """Format a compact daily trading review."""
    report_date = _escape(report.get("date", ""))
    closed_trades = int(report.get("closed_trades", 0) or 0)
    total_pnl = float(report.get("total_pnl", 0) or 0)
    win_rate = float(report.get("win_rate", 0) or 0)
    avg_pnl = float(report.get("avg_pnl", 0) or 0)
    winning = int(report.get("winning_trades", 0) or 0)
    losing = int(report.get("losing_trades", 0) or 0)
    pnl_emoji = _E if total_pnl >= 0 else _E2

    msg = f"""📝 <b>宙斯交易中枢 | 每日复盘</b>
<code>{report_date}</code>

<b>已平仓</b>  <code>{closed_trades}</code>
<b>总盈亏</b>  <code>{total_pnl:+,.2f} USDT</code> {pnl_emoji}
<b>胜率</b>  <code>{win_rate:.2f}%</code>
<b>平均盈亏</b>  <code>{avg_pnl:+,.2f} USDT</code>
<b>胜 / 负</b>  <code>{winning}</code> / <code>{losing}</code>"""

    if closed_trades <= 0:
        return msg + "\n\n📭 昨日无已平仓交易，系统继续观察中。"

    best_trade = report.get("best_trade") or {}
    if best_trade:
        msg += (
            f"\n\n<b>最佳交易</b>  <code>{_escape(best_trade.get('symbol', ''))}</code>"
            f"\n盈亏 <code>{float(best_trade.get('pnl', 0) or 0):+,.2f} USDT</code>"
            f" / <code>{float(best_trade.get('pnl_pct', 0) or 0):+,.2f}%</code>"
        )

    worst_trade = report.get("worst_trade") or {}
    if worst_trade:
        msg += (
            f"\n\n<b>最差交易</b>  <code>{_escape(worst_trade.get('symbol', ''))}</code>"
            f"\n盈亏 <code>{float(worst_trade.get('pnl', 0) or 0):+,.2f} USDT</code>"
            f" / <code>{float(worst_trade.get('pnl_pct', 0) or 0):+,.2f}%</code>"
        )

    def _reason_zh(reason: str) -> str:
        key = str(reason or "").upper()
        mapping = {
            "TAKE_PROFIT": "止盈触发",
            "STOP_LOSS": "止损触发",
            "FILLED": "完全成交",
            "PARTIALLY_FILLED": "部分成交",
            "CANCELED": "已撤销",
            "EXPIRED": "已过期",
            "REJECTED": "已拒绝",
            "UNKNOWN": "未知原因",
        }
        return mapping.get(key, key if key else "未知原因")

    reason_counts = report.get("reason_counts") or {}
    if reason_counts:
        msg += "\n\n<b>平仓原因</b>"
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-int(item[1] or 0), str(item[0]))):
            msg += f"\n•{_escape(_reason_zh(str(reason)))}  <code>{int(count or 0)}</code>"

    protection = report.get("entry_protection") or {}
    protection_attempts = int(protection.get("attempts", 0) or 0)
    if protection_attempts > 0:
        protection_ok = int(protection.get("ok", 0) or 0)
        protection_failed = int(protection.get("failed", 0) or 0)
        protection_ok_rate = float(protection.get("ok_rate", 0) or 0)
        msg += (
            "\n\n<b>Entry Protection</b>"
            f"\nOK/Fail  <code>{protection_ok}</code> / <code>{protection_failed}</code>"
            f"\nOK Rate  <code>{protection_ok_rate:.2f}%</code>"
        )

        failed_by_direction = protection.get("failed_by_direction") or {}
        if failed_by_direction:
            direction_text = ", ".join(
                f"{_escape(str(direction))}:{int(count or 0)}"
                for direction, count in failed_by_direction.items()
            )
            msg += f"\nFail by Side  <code>{_escape(direction_text)}</code>"

        failed_by_symbol = protection.get("failed_by_symbol") or {}
        if failed_by_symbol:
            top_symbols = list(failed_by_symbol.items())[:3]
            symbol_text = ", ".join(
                f"{_escape(str(symbol))}:{int(count or 0)}"
                for symbol, count in top_symbols
            )
            msg += f"\nFail by Symbol  <code>{_escape(symbol_text)}</code>"

        failed_by_detail = protection.get("failed_by_detail") or {}
        if failed_by_detail:
            top_details = list(failed_by_detail.items())[:3]
            detail_text = ", ".join(
                f"{_escape(str(detail))}:{int(count or 0)}"
                for detail, count in top_details
            )
            msg += f"\nFail by Detail  <code>{_escape(detail_text)}</code>"

    oi_stats = report.get("oi_funding_stats") or {}
    enhanced_trades = int(oi_stats.get("enhanced_trades", 0) or 0)
    if enhanced_trades > 0:
        enhanced_win_rate = float(oi_stats.get("enhanced_win_rate", 0) or 0)
        enhanced_avg_pnl = float(oi_stats.get("enhanced_avg_pnl", 0) or 0)
        enhanced_avg_bonus = float(oi_stats.get("enhanced_avg_bonus", 0) or 0)
        msg += (
            "\n\n<b>OI/Funding增强</b>"
            f"\nTrades  <code>{enhanced_trades}</code>"
            f"\nWinRate  <code>{enhanced_win_rate:.2f}%</code>"
            f"\nAvgPnL  <code>{enhanced_avg_pnl:+,.2f} USDT</code>"
            f"\nAvgBonus  <code>+{enhanced_avg_bonus:.2f}</code>"
        )

    return msg


def format_dark_flow_alert(
    symbol: str,
    oi_change_pct: float,
    price_change_pct: float,
    funding_rate: float,
    market_cap: float,
) -> str:
    """Format a 'dark flow' alert (OI up while price barely moves)."""
    return f"""🔄 <b>暗流信号</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>OI 变化</b>  <code>{oi_change_pct:+.1f}%</code>
<b>价格变化</b>  <code>{price_change_pct:+.1f}%</code>
<b>资金费率</b>  <code>{funding_rate:.4f}%</code>
<b>市值</b>  <code>${market_cap/1e6:.1f}M</code>

📌 <b>解读</b>  OI 上升但价格滞涨，可能有埋伏资金/对冲盘，避免情绪化追涨。"""


def format_accumulation_pool_report(pool: list, limit: int = 10) -> str:
    """Format a compact accumulation pool report."""
    msg = f"""🎯 <b>收筹池报告</b>

<b>池内标的</b>  <code>{len(pool)}</code>
────────────"""

    for i, p in enumerate(pool[:limit], 1):
        symbol = p.get("symbol", "")
        days = int(p.get("sideways_days", 0) or 0)
        range_pct = float(p.get("price_range_pct", 0) or 0)
        cap = float(p.get("market_cap_usd", 0) or 0)
        msg += (
            f"\n{i}. <code>{_escape(symbol)}</code>"
            f"  | 横盘 <code>{days}d</code>"
            f"  | 振幅 <code>{range_pct:.0f}%</code>"
            f"  | 市值 <code>${cap/1e6:.1f}M</code>"
        )

    if len(pool) > limit:
        msg += f"\n… 还有 <code>{len(pool) - limit}</code> 个标的未展示"

    msg += "\n\n📌 <b>提示</b>  横盘时间越久 + 振幅越小，越可能处在“收筹/蓄势”阶段。"
    return msg


def format_short_fuel_report(fuel_list: list, limit: int = 5) -> str:
    """Format a short-fuel (crowded shorts) report."""
    msg = f"""🔥 <b>空头燃料报告</b>

<b>候选标的</b>  <code>{len(fuel_list)}</code>
────────────"""

    for i, f in enumerate(fuel_list[:limit], 1):
        symbol = f.get("symbol", "")
        rate = float(f.get("funding_rate", 0) or 0)
        price_pct = float(f.get("price_change_pct", 0) or 0)
        vol = float(f.get("volume_usd", 0) or 0)
        msg += (
            f"\n{i}. <code>{_escape(symbol)}</code>"
            f"  | 费率 <code>{rate:.3f}%</code>"
            f"  | 涨跌 <code>{price_pct:+.0f}%</code>"
            f"  | Vol <code>${vol/1e6:.1f}M</code>"
        )

    if len(fuel_list) > limit:
        msg += f"\n… 还有 <code>{len(fuel_list) - limit}</code> 个标的未展示"

    msg += "\n\n📌 <b>提示</b>  费率越负，做空越拥挤；若价格转强，容易触发反向挤压。"
    return msg


def format_radar_summary(
    pool_count: int,
    oi_signals: int,
    dark_flows: int,
    short_fuel: int,
    top_dark_flow: str | None = None,
) -> str:
    """Format a compact radar summary."""
    msg = f"""📊 <b>雷达摘要</b>

<b>收筹池</b>  <code>{pool_count}</code>
<b>OI 异动</b>  <code>{oi_signals}</code>
<b>暗流信号</b>  <code>{dark_flows}</code>
<b>空头燃料</b>  <code>{short_fuel}</code>"""

    if top_dark_flow:
        msg += f"\n\n<b>重点暗流</b>  <code>{_escape(top_dark_flow)}</code>"
    return msg


def main() -> None:
    """Manual CLI test."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Telegram notifications")
    parser.add_argument("--message", "-m", default="Test message from Hermes Trader")
    args = parser.parse_args()

    success = send_telegram_message(args.message)
    print(f"Message sent: {success}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
