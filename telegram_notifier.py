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
import queue
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


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return default


_telegram_queue: "queue.Queue[tuple[str, str | None]]" = queue.Queue(
    maxsize=max(10, _int_env("TELEGRAM_QUEUE_SIZE", 1000))
)
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()


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


def send_telegram_message(message: str, parse_mode: str | None = "HTML", async_send: bool | None = None) -> bool:
    """Queue a Telegram message without blocking trading flows."""
    if async_send is None:
        async_send = os.environ.get("TELEGRAM_ASYNC_SEND", "1").strip().lower() not in {"0", "false", "no"}

    if not async_send:
        return _send_telegram_message_sync(message, parse_mode=parse_mode)

    config = get_telegram_config()
    token = _normalize_telegram_value(config.get("bot_token", ""))
    chat_id = _normalize_telegram_value(config.get("chat_id", ""))
    if not _telegram_config_usable(token, chat_id):
        logger.info(f"[TG] {message}")
        return False

    _ensure_telegram_worker()
    try:
        _telegram_queue.put_nowait((message, parse_mode))
        return True
    except queue.Full:
        logger.error("Telegram queue full - dropping notification to keep trading loop non-blocking")
        return False


def _ensure_telegram_worker():
    global _telegram_worker_started
    if _telegram_worker_started:
        return
    with _telegram_worker_lock:
        if _telegram_worker_started:
            return
        thread = threading.Thread(target=_telegram_worker_loop, name="telegram-notifier", daemon=True)
        thread.start()
        _telegram_worker_started = True


def _telegram_worker_loop():
    while True:
        message, parse_mode = _telegram_queue.get()
        try:
            _send_telegram_message_sync(message, parse_mode=parse_mode)
        except Exception as exc:
            logger.error(f"Telegram worker unexpected failure: {exc}")
        finally:
            _telegram_queue.task_done()


def _telegram_config_usable(token: str, chat_id: str) -> bool:
    if not token or not chat_id:
        logger.warning("Telegram not configured - skipping notification")
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
    return True


def _send_telegram_message_sync(message: str, parse_mode: str | None = "HTML") -> bool:
    """Send a message to Telegram (thread-safe with simple rate limiting)."""
    global _last_message_time

    config = get_telegram_config()
    token = _normalize_telegram_value(config.get("bot_token", ""))
    chat_id = _normalize_telegram_value(config.get("chat_id", ""))

    if not _telegram_config_usable(token, chat_id):
        logger.info(f"[TG] {message}")
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
        expected_text = f" | 预计 +${expected_pnl:,.2f}" if expected_pnl > 0 else ""
        lines.append(
            f"TP{level}: "
            f"<code>{price_move_pct:.2f}% 价格 / {roi_pct:.2f}% ROI</code> →<code>${price:,.4f}</code> "
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

    tags: list[str] = []
    if turned_negative:
        tags.append("费率转负")
    if oi_signal:
        tags.append("OI扩张")
    if not tags and bonus > 0:
        tags.append("评分加成")
    tag_text = " / ".join(tags) if tags else "无"

    return (
        f"<b>OI/Funding</b>  <code>{_escape(tag_text)}</code> | "
        f"加分 <code>+{bonus:.1f}</code> | "
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
    capital_plan: dict[str, Any] | None = None,
) -> str:
    """格式化开仓通知"""
    direction_emoji = _E if direction == "LONG" else _E2
    direction_text = format_direction_label(direction)

    sl_pct = abs(entry_price - stop_loss) / entry_price * 100 if entry_price else 0.0
    tp_pct = abs(take_profit - entry_price) / entry_price * 100 if entry_price else 0.0
    notional_value = entry_price * quantity
    expected_sl_loss = abs(entry_price - stop_loss) * quantity if entry_price > 0 and stop_loss > 0 else risk_amount
    expected_tp_total = 0.0
    for target in take_profit_targets or []:
        target_price = float(target.get("price", 0) or 0)
        target_quantity = float(target.get("quantity", 0) or 0)
        if target_price <= 0 or target_quantity <= 0:
            continue
        if direction == "LONG":
            expected_tp_total += max(0.0, (target_price - entry_price) * target_quantity)
        else:
            expected_tp_total += max(0.0, (entry_price - target_price) * target_quantity)

    msg = f"""🟢 <b>宙斯交易中枢 | 开仓成功</b>

<b>标的</b>  <code>{_escape(symbol)}</code>  {direction_text}
<b>入场</b>  <code>${entry_price:,.4f}</code>  {leverage}x
<b>数量</b>  <code>{_fmt_num(quantity)}</code>  |  名义 <code>{notional_value:,.2f} USDT</code>
<b>止损</b>  <code>${stop_loss:,.4f}</code>  ({sl_pct:.1f}%)  |  预计 <code>-${expected_sl_loss:.2f}</code>
<b>止盈</b>  <code>${take_profit:,.4f}</code>  ({tp_pct:.1f}%)
<b>风险</b>  <code>${risk_amount:.2f}</code>  |  {risk_pct:.1f}%"""

    if strategy_line:
        msg += f"\n<b>策略</b>  <code>{_escape(strategy_line)}</code>"
    oi_funding_line = _format_oi_funding_brief(oi_funding)
    if oi_funding_line:
        msg += f"\n{oi_funding_line}"
    if target_roi_pct > 0:
        msg += f"\n<b>目标收益率</b>  <code>{target_roi_pct:.2f}% ROI</code>"
    if price_move_pct > 0:
        msg += f"\n<b>实际价格目标</b>  <code>{price_move_pct:.2f}%</code>"
    if capital_plan:
        notes = capital_plan.get("notes") or []
        note_text = f" | {_escape('；'.join(str(item) for item in notes[:2]))}" if notes else ""
        msg += (
            f"\n<b>资金档位</b>  <code>{_escape(str(capital_plan.get('mode', '')))}</code>"
            f" | EV <code>{float(capital_plan.get('expected_rr', 0) or 0):.2f}R</code>{note_text}"
        )
        locked_profit = float(capital_plan.get("locked_profit", 0) or 0)
        if locked_profit > 0:
            msg += f"\n<b>盈利锁仓</b>  <code>{locked_profit:,.2f} USDT</code>"
    if take_profit_targets:
        if expected_tp_total > 0:
            msg += f"\n<b>预计止盈</b>  <code>+${expected_tp_total:,.2f}</code>"
        msg += f"\n<b>分批止盈</b>\n{_format_take_profit_targets(take_profit_targets, entry_price, direction)}"

    if score > 0:
        confidence = "极高" if score >= 80 else ("高" if score >= 60 else ("中" if score >= 40 else "低"))
        msg += f"\n<b>评分</b>  <code>{score:.0f}/100</code>  |  {confidence}"
    if risk_level:
        msg += f"\n<b>风险等级</b>  <code>{_escape(risk_level)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"

    return msg


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
    direction_text = format_direction_label(direction)

    direction_text = format_direction_label(direction)
    emoji = "🟢" if pnl >= 0 else "🔴"

    msg = f"""{emoji} <b>宙斯交易中枢 | 平仓</b>

<b>{_escape(symbol)}</b>  {direction_text}
<b>入场</b>  <code>${entry_price:,.4f}</code>  →  <b>出场</b>  <code>${exit_price:,.4f}</code>
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
        msg += f"\n<b>持仓</b>  {_format_duration_from_hours(duration_hours)}"
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
    pnl_source: str = "",
) -> str:
    """格式化分批止盈成交通知"""
    direction_text = format_direction_label(direction)
    pnl_sign = "+" if pnl >= 0 else ""
    pnl_emoji = _E if pnl >= 0 else _E2
    level_text = f"TP{level}" if level else "部分止盈"
    entry_text = f"${entry_price:,.4f}" if entry_price > 0 else "待同步"
    pnl_pct_text = f"({pnl_sign}{pnl_pct:.2f}%)" if entry_price > 0 else "(比例待同步)"

    pnl_pct_text = f"({pnl_sign}{pnl_pct:.2f}%)"

    emoji = "🟢" if pnl >= 0 else "🔴"
    msg = f"""{emoji} <b>宙斯交易中枢 | 分批止盈</b>

<b>{_escape(symbol)}</b>  {direction_text}  |  <b>{_escape(level_text)}</b> ✅
<b>入场</b>  <code>{_escape(entry_text)}</code>  →  <b>成交</b>  <code>${exit_price:,.4f}</code>
<b>本次</b>  {pnl_emoji} <b>{pnl_sign}${pnl:,.2f}</b>  {pnl_pct_text}  |  止盈 <code>{_fmt_num(quantity)}</code>
<b>剩余</b>  <code>{_fmt_num(remaining_quantity)}</code>  继续持有"""

    if pnl_source:
        msg += f"\n<b>盈亏来源</b>  <code>{_escape(pnl_source)}</code>"
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
<b>来源</b>  <code>{_escape(_format_source_label(source))}</code>"""

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
        side = format_direction_label(pos.get("side", "UNKNOWN"))
        current_price = float(pos.get("current_price", 0) or 0)
        take_profit_display = pos.get("take_profit_targets_text") or f"${float(pos.get('take_profit', 0) or 0):,.4f}"
        stop_suffix = " 估算" if pos.get("stop_loss_estimated") else ""

        msg += f"""

<b>{i}.</b> <code>{_escape(pos.get('symbol', 'UNKNOWN'))}</code>  {side}
入场 <code>${float(pos.get('entry_price', 0) or 0):,.4f}</code>  |  现价 <code>${current_price:,.4f}</code>
止损 <code>${float(pos.get('stop_loss', 0) or 0):,.4f}</code>{stop_suffix}  |  止盈 <code>{_escape(take_profit_display)}</code>
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

<b>类型</b>  <code>{_escape(_format_error_type_label(error_type))}</code>"""
    if component:
        msg += f"\n<b>组件</b>  <code>{_escape(_format_component_label(component))}</code>"
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
    direction_text = format_direction_label(direction)
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

{direction_emoji} <b>{_escape(symbol)}</b>  <code>{_escape(direction_text)}</code>
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

<b>已平仓</b>  <code>{closed_trades}</code>  |  <b>盈亏</b>  <code>{total_pnl:+,.2f} USDT</code> {pnl_emoji}
<b>胜率</b>  <code>{win_rate:.1f}%</code>  |  <b>胜/负</b>  <code>{winning}</code>/<code>{losing}</code>
<b>平均</b>  <code>{avg_pnl:+,.2f} USDT</code>"""
    source_rows = int(report.get("source_rows", closed_trades) or closed_trades)
    split_rows = int(report.get("split_rows", 0) or 0)
    if source_rows != closed_trades or split_rows > 0:
        msg += f"\n<b>统计口径</b>  <code>{source_rows} 行平仓记录 → {closed_trades} 笔完整交易</code>"

    if closed_trades <= 0:
        return msg + "\n\n📭 昨日无已平仓交易，系统继续观察中。"

    avg_win = float(report.get("avg_win", 0) or 0)
    avg_loss = float(report.get("avg_loss", 0) or 0)
    payoff_ratio = float(report.get("payoff_ratio", 0) or 0)
    profit_factor = float(report.get("profit_factor", 0) or 0)
    max_loss = float(report.get("max_loss", 0) or 0)
    payoff_text = "∞" if payoff_ratio >= 999 else f"{payoff_ratio:.2f}"
    profit_factor_text = "∞" if profit_factor >= 999 else f"{profit_factor:.2f}"
    msg += (
        "\n\n<b>质量</b>"
        f"\n平均盈利  <code>{avg_win:+,.2f}</code>  |  亏损  <code>{avg_loss:+,.2f}</code>"
        f"\n盈亏比  <code>{payoff_text}</code>  |  收益因子  <code>{profit_factor_text}</code>  |  最大亏  <code>{max_loss:+,.2f}</code>"
    )

    best_trade = report.get("best_trade") or {}
    if best_trade:
        msg += (
            f"\n\n<b>最佳</b>  <code>{_escape(best_trade.get('symbol', ''))}</code>"
            f"  |  +${float(best_trade.get('pnl', 0) or 0):.2f}  "
            f"(+{float(best_trade.get('pnl_pct', 0) or 0):.1f}%)"
        )

    worst_trade = report.get("worst_trade") or {}
    if worst_trade:
        msg += (
            f"\n<b>最差</b>  <code>{_escape(worst_trade.get('symbol', ''))}</code>"
            f"  |  ${float(worst_trade.get('pnl', 0) or 0):+.2f}  "
            f"({float(worst_trade.get('pnl_pct', 0) or 0):+.1f}%)"
        )

    reason_counts = report.get("reason_counts") or {}
    if reason_counts:
        parts = []
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-int(item[1] or 0), str(item[0]))):
            parts.append(f"{_escape(_format_close_reason_label(reason))} <code>{int(count or 0)}</code>")
        msg += f"\n<b>原因</b>  {'  |  '.join(parts)}"

    protection = report.get("entry_protection") or {}
    protection_attempts = int(protection.get("attempts", 0) or 0)
    if protection_attempts > 0:
        protection_ok = int(protection.get("ok", 0) or 0)
        protection_failed = int(protection.get("failed", 0) or 0)
        protection_ok_rate = float(protection.get("ok_rate", 0) or 0)
        msg += (
            f"\n\n<b>保护单</b>  {protection_ok}/{protection_failed}  成功率 {protection_ok_rate:.1f}%"
        )

        failed_by_direction = protection.get("failed_by_direction") or {}
        if failed_by_direction:
            direction_text = ", ".join(
                f"{_escape(format_direction_label(direction))}:{int(count or 0)}"
                for direction, count in failed_by_direction.items()
            )
            msg += f"\n失败方向  <code>{_escape(direction_text)}</code>"

        failed_by_symbol = protection.get("failed_by_symbol") or {}
        if failed_by_symbol:
            top_symbols = list(failed_by_symbol.items())[:3]
            symbol_text = ", ".join(
                f"{_escape(str(symbol))}:{int(count or 0)}"
                for symbol, count in top_symbols
            )
            msg += f"\n失败标的  <code>{_escape(symbol_text)}</code>"

        failed_by_detail = protection.get("failed_by_detail") or {}
        if failed_by_detail:
            top_details = list(failed_by_detail.items())[:3]
            detail_text = ", ".join(
                f"{_escape(_format_protection_failure_detail(str(detail)))}:{int(count or 0)}"
                for detail, count in top_details
            )
            msg += f"\n失败原因  <code>{_escape(detail_text)}</code>"

    oi_stats = report.get("oi_funding_stats") or {}
    enhanced_trades = int(oi_stats.get("enhanced_trades", 0) or 0)
    if enhanced_trades > 0:
        enhanced_win_rate = float(oi_stats.get("enhanced_win_rate", 0) or 0)
        enhanced_avg_pnl = float(oi_stats.get("enhanced_avg_pnl", 0) or 0)
        enhanced_avg_bonus = float(oi_stats.get("enhanced_avg_bonus", 0) or 0)
        msg += (
            "\n\n<b>OI/Funding增强</b>"
            f"\n交易数  <code>{enhanced_trades}</code>"
            f"\n胜率  <code>{enhanced_win_rate:.2f}%</code>"
            f"\n平均盈亏  <code>{enhanced_avg_pnl:+,.2f} USDT</code>"
            f"\n平均加分  <code>+{enhanced_avg_bonus:.2f}</code>"
        )

    return msg


def _format_ratio_value(value: Any) -> str:
    try:
        number = float(value or 0)
    except Exception:
        return "0.00"
    return "∞" if number >= 999 else f"{number:.2f}"


def _format_top_counts(counts: dict[str, Any], limit: int = 3) -> str:
    if not counts:
        return "无"
    parts = []
    for key, value in sorted(counts.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))[:limit]:
        parts.append(f"{_format_close_reason_label(key)} {int(value or 0)}")
    return " / ".join(parts) if parts else "无"


def _format_side_stats(stats: dict[str, Any]) -> str:
    if not stats:
        return "无"
    parts = []
    for side, item in stats.items():
        if not isinstance(item, dict):
            continue
        count = int(item.get("count", 0) or 0)
        if count <= 0:
            continue
        wins = int(item.get("wins", 0) or 0)
        pnl = float(item.get("pnl", 0) or 0)
        win_rate = wins / count * 100 if count else 0.0
        parts.append(f"{format_direction_label(side)} {count}笔/{win_rate:.0f}%/{pnl:+.2f}")
    return " / ".join(parts) if parts else "无"


def _format_strategy_line(item: dict[str, Any] | None) -> str:
    if not item:
        return "无"
    name = str(item.get("name", "") or "未知策略")
    if name.upper() == "UNKNOWN":
        name = "历史未标记策略"
    count = int(item.get("count", 0) or 0)
    pnl = float(item.get("pnl", 0) or 0)
    wins = int(item.get("wins", 0) or 0)
    win_rate = wins / count * 100 if count else 0.0
    return f"{name} {count}笔/{win_rate:.0f}%/{pnl:+.2f}"


def _format_period_block(report: dict[str, Any]) -> str:
    label = str(report.get("label") or f"近{int(report.get('period_days', 0) or 0)}天")
    closed = int(report.get("closed_trades", 0) or 0)
    source_rows = int(report.get("source_rows", closed) or closed)
    split_rows = int(report.get("split_rows", 0) or 0)
    total_pnl = float(report.get("total_pnl", 0) or 0)
    pnl_emoji = _E if total_pnl >= 0 else _E2
    best_trade = report.get("best_trade") or {}
    worst_trade = report.get("worst_trade") or {}
    best_day = report.get("best_day") or {}
    worst_day = report.get("worst_day") or {}

    lines = [
        f"<b>{_escape(label)}</b>",
        f"完整交易  <code>{closed}</code> 笔",
    ]
    if source_rows != closed or split_rows > 0:
        lines.append(f"原始平仓记录  <code>{source_rows}</code> 行 | 已聚合分批 <code>{split_rows}</code> 行")
    lines.extend(
        [
            f"总盈亏  <code>{total_pnl:+,.2f} USDT</code> {pnl_emoji}",
            f"胜率  <code>{float(report.get('win_rate', 0) or 0):.1f}%</code>  |  笔均  <code>{float(report.get('avg_pnl', 0) or 0):+,.2f}</code>",
            f"盈亏比  <code>{_format_ratio_value(report.get('payoff_ratio', 0))}</code>  |  收益因子  <code>{_format_ratio_value(report.get('profit_factor', 0))}</code>",
        ]
    )
    if best_trade.get("symbol"):
        lines.append(
            f"最佳  <code>{_escape(best_trade.get('symbol'))}</code> "
            f"<code>{float(best_trade.get('pnl', 0) or 0):+,.2f}</code>"
        )
    if worst_trade.get("symbol"):
        lines.append(
            f"最差  <code>{_escape(worst_trade.get('symbol'))}</code> "
            f"<code>{float(worst_trade.get('pnl', 0) or 0):+,.2f}</code>"
        )
    if best_day.get("date") or worst_day.get("date"):
        lines.append(
            f"最佳/最差日  <code>{_escape(best_day.get('date', '-'))} {float(best_day.get('pnl', 0) or 0):+,.2f}</code>"
            f" / <code>{_escape(worst_day.get('date', '-'))} {float(worst_day.get('pnl', 0) or 0):+,.2f}</code>"
        )
    lines.append(f"方向表现  <code>{_escape(_format_side_stats(report.get('side_stats') or {}))}</code>")
    lines.append(f"主要原因  <code>{_escape(_format_top_counts(report.get('reason_counts') or {}))}</code>")
    lines.append(f"最好策略  <code>{_escape(_format_strategy_line(report.get('best_strategy')))}</code>")
    lines.append(f"最差策略  <code>{_escape(_format_strategy_line(report.get('worst_strategy')))}</code>")
    return "\n".join(lines)


def format_period_report_msg(reports: list[dict[str, Any]]) -> str:
    """Format rolling 7d/30d performance review for Telegram."""
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"""📈 <b>宙斯交易中枢 | 周期复盘</b>

<code>{now_text}</code>"""

    valid_reports = [report for report in reports if isinstance(report, dict)]
    if not valid_reports:
        return msg + "\n\n📭 暂无区间交易数据。"

    for report in valid_reports:
        msg += f"\n\n{_format_period_block(report)}"
    return msg


def format_dark_flow_alert(
    symbol: str,
    oi_change_pct: float,
    price_change_pct: float,
    funding_rate: float,
    market_cap: float,
    volume_24h: float = 0.0,
    dark_flow_score: float = 0.0,
    score_total: float = 0.0,
) -> str:
    """Format a 'dark flow' alert (OI up while price barely moves)."""
    funding_pct = funding_rate * 100 if abs(funding_rate) < 1 else funding_rate
    vol_text = f"${volume_24h/1e6:.1f}M" if volume_24h > 0 else (f"${market_cap/1e6:.1f}M" if market_cap > 0 else "N/A")
    
    # 动态解读：根据实际数据生成
    interpretation = _generate_dark_flow_interpretation(
        oi_change_pct, price_change_pct, funding_rate, dark_flow_score, score_total
    )
    
    return f"""🔄 <b>宙斯交易中枢 | 雷达暗流</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>OI 变化</b>  <code>{oi_change_pct:+.1f}%</code>
<b>价格变化</b>  <code>{price_change_pct:+.1f}%</code>
<b>资金费率</b>  <code>{funding_pct:+.4f}%</code>
<b>24h 交易量</b>  <code>{vol_text}</code>
<b>暗流评分</b>  <code>{dark_flow_score:.1f}</code>

📌 <b>解读</b>  {interpretation}"""


def _generate_dark_flow_interpretation(
    oi_change_pct: float,
    price_change_pct: float,
    funding_rate: float,
    dark_flow_score: float,
    score_total: float,
) -> str:
    """根据实际数据动态生成暗流解读。"""
    parts = []
    
    # OI 方向判断
    if oi_change_pct >= 20:
        parts.append("OI 大幅扩张")
    elif oi_change_pct >= 10:
        parts.append("OI 明显增长")
    elif oi_change_pct >= 5:
        parts.append("OI 温和增长")
    elif oi_change_pct <= -10:
        parts.append("OI 大幅缩减")
    else:
        parts.append("OI 基本稳定")
    
    # 价格与OI的关系
    if oi_change_pct >= 15 and abs(price_change_pct) <= 5:
        parts.append("资金大量流入但价格未动，可能有主力吸筹")
    elif oi_change_pct >= 15 and price_change_pct > 10:
        parts.append("OI与价格同步上涨，趋势健康")
    elif oi_change_pct >= 15 and price_change_pct < -5:
        parts.append("OI增加但价格下跌，警惕空头加仓或多头被套")
    elif oi_change_pct <= -15 and price_change_pct > 5:
        parts.append("价格上涨但OI缩减，可能是空头回补推动")
    elif oi_change_pct <= -15 and price_change_pct < -5:
        parts.append("OI缩减+价格下跌，资金离场")
    
    # 资金费率判断
    funding_pct = funding_rate * 100 if abs(funding_rate) < 1 else funding_rate
    if funding_pct < -0.05:
        parts.append("费率为负，空头付费，偏多信号")
    elif funding_pct > 0.05:
        parts.append("费率为正，多头付费，注意过热")
    
    # 评分判断
    if dark_flow_score >= 60:
        parts.append("暗流评分高，值得关注")
    elif dark_flow_score >= 45:
        parts.append("暗流评分中等，保持观察")
    
    if score_total >= 70:
        parts.append("综合评分高，信号较强")
    
    return "；".join(parts) if parts else "OI 与价格变化正常，无明显暗流特征。"


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
    msg = f"""📡 <b>宙斯交易中枢 | 雷达摘要</b>

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
