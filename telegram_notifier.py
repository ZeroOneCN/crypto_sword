"""Telegram notification module for trading alerts."""

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

# Thread-safe message queue
_telegram_lock = threading.Lock()
_last_message_time = 0
_min_message_interval = 0.5  # 500ms between messages to avoid rate limits


def get_telegram_config() -> dict[str, Any]:
    """Load Telegram config from environment or config file."""
    config = {
        "bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        "chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),
    }

    # Try to load from config file if env vars not set
    if not config["bot_token"] or not config["chat_id"]:
        config_path = Path("/root/.hermes/config/telegram.json")
        if config_path.exists():
            try:
                with open(config_path) as f:
                    file_config = json.load(f)
                    # 修复：应该从 file_config 读取，而不是 config
                    config["bot_token"] = config["bot_token"] or file_config.get("bot_token", "")
                    config["chat_id"] = config["chat_id"] or file_config.get("chat_id", "")
            except Exception as e:
                logger.warning(f"Failed to load Telegram config: {e}")

    return config


def send_telegram_message(message: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Telegram (thread-safe with rate limiting).

    Args:
        message: Message text (supports Markdown/HTML)
        parse_mode: "Markdown", "MarkdownV2", "HTML", or None for plain text

    Returns:
        True if sent successfully
    """
    global _last_message_time

    config = get_telegram_config()

    if not config["bot_token"] or not config["chat_id"]:
        logger.warning("Telegram not configured - skipping notification")
        logger.info(f"[TG] {message}")
        return False

    # Thread-safe sending with rate limiting
    with _telegram_lock:
        # Wait if needed to respect rate limit
        elapsed = time.time() - _last_message_time
        if elapsed < _min_message_interval:
            time.sleep(_min_message_interval - elapsed)

        url = f"https://api.telegram.org/bot{config['bot_token']}/sendMessage"

        try:
            import urllib.request
            import urllib.error

            attempts = [(message, parse_mode)]
            if parse_mode is not None:
                attempts.append((_strip_html(message), None))

            for index, (attempt_message, attempt_parse_mode) in enumerate(attempts):
                payload = {
                    "chat_id": config["chat_id"],
                    "text": attempt_message,
                }
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
    text = re.sub(r"</?(b|code|i|u|s|pre)>", "", message)
    return html.unescape(text)


def _fmt_num(value: Any, decimals: int = 6) -> str:
    """Format noisy float values for Telegram."""
    try:
        return f"{float(value):.{decimals}f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


def _format_take_profit_targets(targets: list[dict[str, Any]]) -> str:
    """Format staged take-profit targets for Telegram."""
    if not targets:
        return ""

    lines = []
    for target in targets:
        roi_pct = float(target.get("target_roi_pct", 0) or 0)
        price_move_pct = float(target.get("price_move_pct", 0) or 0)
        price = float(target.get("price", 0) or 0)
        ratio = float(target.get("ratio", 0) or 0) * 100
        quantity = float(target.get("quantity", 0) or 0)
        lines.append(
            f"TP{int(target.get('level', len(lines) + 1))}: "
            f"<code>{price_move_pct:.2f}% 价格 / {roi_pct:.2f}% ROI</code> → <code>${price:,.4f}</code> "
            f"({ratio:.0f}% / {_fmt_num(quantity)})"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# 结构化通知模板 - 赫尔墨斯的信使
# ═══════════════════════════════════════════════════════════════

def format_open_position_msg(symbol: str, direction: str, entry_price: float, quantity: float, 
                             leverage: int, stop_loss: float, take_profit: float, 
                             risk_amount: float, risk_pct: float, score: float = 0,
                             risk_level: str = "", session_id: str = "",
                             target_roi_pct: float = 0, price_move_pct: float = 0,
                             take_profit_targets: list[dict[str, Any]] = None) -> str:
    """格式化开仓通知"""
    direction_emoji = "🟢" if direction == "LONG" else "🔴"
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"
    
    # 计算止损止盈百分比
    sl_pct = abs(entry_price - stop_loss) / entry_price * 100
    tp_pct = abs(take_profit - entry_price) / entry_price * 100
    
    # 计算名义价值
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

    if target_roi_pct > 0:
        msg += f"\n<b>目标收益率</b>  <code>{target_roi_pct:.2f}% ROI</code>"
    if price_move_pct > 0:
        msg += f"\n<b>实际价格目标</b>  <code>{price_move_pct:.2f}%</code>"
    if take_profit_targets:
        msg += f"\n<b>分批止盈</b>\n{_format_take_profit_targets(take_profit_targets)}"
    
    if score > 0:
        confidence = "极高" if score >= 80 else "高" if score >= 60 else "中" if score >= 40 else "低"
        msg += f"\n<b>评分</b>  <code>{score:.0f}/100</code>  |  {confidence}"
    if risk_level:
        msg += f"\n<b>风险等级</b>  <code>{_escape(risk_level)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"
        
    return msg


def format_close_position_msg(symbol: str, direction: str, entry_price: float, exit_price: float, 
                              quantity: float, pnl: float, pnl_pct: float, reason: str, 
                              duration_hours: float = 0, session_id: str = "") -> str:
    """格式化平仓通知"""
    direction_emoji = "🟢" if pnl >= 0 else "🔴"
    pnl_emoji = "🟢" if pnl >= 0 else "🔴"
    pnl_sign = "+" if pnl >= 0 else ""
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"
    
    msg = f"""{direction_emoji} <b>宙斯交易中枢 | 平仓完成</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>方向</b>  {direction_text}
<b>入场</b>  <code>${entry_price:,.4f}</code>
<b>出场</b>  <code>${exit_price:,.4f}</code>
<b>数量</b>  <code>{quantity}</code>
<b>盈亏</b>  {pnl_emoji} <b>{pnl_sign}${pnl:,.2f}</b>  ({pnl_sign}{pnl_pct:.2f}%)
<b>原因</b>  <code>{_escape(reason)}</code>"""
    
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
) -> str:
    """格式化分批止盈成交通知"""
    direction_text = "做多 LONG" if direction == "LONG" else "做空 SHORT"
    pnl_sign = "+" if pnl >= 0 else ""
    level_text = f"TP{level}" if level else "部分止盈"

    msg = f"""🎯 <b>宙斯交易中枢 | 分批止盈成交</b>

<b>标的</b>  <code>{_escape(symbol)}</code>
<b>方向</b>  {direction_text}
<b>档位</b>  <code>{_escape(level_text)}</code>
<b>入场</b>  <code>${entry_price:,.4f}</code>
<b>成交价</b>  <code>${exit_price:,.4f}</code>
<b>止盈数量</b>  <code>{_fmt_num(quantity)}</code>
<b>剩余数量</b>  <code>{_fmt_num(remaining_quantity)}</code>
<b>本次盈亏</b>  🟢 <b>{pnl_sign}${pnl:,.2f}</b>  ({pnl_sign}{pnl_pct:.2f}%)"""

    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"

    return msg


def format_protection_status_msg(
    symbol: str,
    stop_loss_ok: bool,
    take_profit_ok: bool,
    stop_loss_order_id: int = 0,
    take_profit_order_ids: list[int] = None,
    session_id: str = "",
    source: str = "audit",
    message: str = "",
) -> str:
    """Format exchange-side protection status notification."""
    tp_ids = take_profit_order_ids or []
    ok = stop_loss_ok and take_profit_ok
    title = "🛡️ 宙斯交易中枢 | 保护单确认" if ok else "🚨 宙斯交易中枢 | 裸仓风险"
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


def format_scan_monitor_msg(signals: list[dict[str, Any]], scanned_count: int = 0, max_items: int = 5) -> str:
    """Format a compact real-time scanner monitor report."""
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"""📡 <b>宙斯交易中枢 | 妖币扫描报告</b>
<code>{now_text}</code>

<b>扫描数量</b>  <code>{scanned_count}</code>
<b>有效信号</b>  <code>{len(signals)}</code>"""

    if not signals:
        return msg + "\n\n📭 暂无有效信号"

    for item in signals[:max_items]:
        symbol = item.get("symbol", "UNKNOWN")
        direction = item.get("direction", "UNKNOWN")
        score = (item.get("score") or {}).get("total_score", 0)
        confidence = (item.get("score") or {}).get("confidence", "")
        metrics = item.get("metrics", {}) or {}
        funding = float(metrics.get("funding_rate", 0) or 0) * 100
        price = float(metrics.get("last_price", item.get("price", 0)) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_24h = float(metrics.get("oi_24h_pct", 0) or 0)
        tag = "跟多信号" if direction in {"LONG", "CONSIDER_LONG"} else "跟空信号"
        funding_tag = "极负费率" if funding <= -0.5 else "负费率" if funding < 0 else "正费率"

        msg += f"""

📨 <b>{_escape(tag)} ({_escape(funding_tag)})</b>
• <code>{_escape(symbol)}</code> 评分 <code>{float(score):.1f}</code> {_escape(str(confidence))}
• 费率 <code>{funding:+.4f}%</code>  价格 <code>{change_24h:+.2f}%</code>
• OI <code>{oi_24h:+.2f}%</code>  现价 <code>${price:,.6f}</code>"""

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
        "⏱️ <b>宙斯交易中枢 | 延迟告警</b>",
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
            lines.append(f"• {_escape(name)}  <code>{elapsed_ms:.0f} ms</code>")

    return "\n".join(lines)


def format_summary_msg(positions: list, total_pnl: float, realized_pnl: float) -> str:
    """格式化持仓汇总通知"""
    msg = f"""📊 <b>宙斯交易中枢 | 持仓汇总</b>

<b>持仓数</b>  <code>{len(positions)}</code>
<b>未实现</b>  <code>{total_pnl:+,.2f} USDT</code>
<b>已实现</b>  <code>{realized_pnl:+,.2f} USDT</code>"""
    
    if not positions:
        msg += "\n\n📭 当前无持仓"
    else:
        for i, pos in enumerate(positions, 1):
            pnl = pos.get('unrealized_pnl', 0)
            pnl_sign = "+" if pnl >= 0 else ""
            pnl_emoji = "🟢" if pnl >= 0 else "🔴"
            side = pos.get('side', 'UNKNOWN')
            current_price = pos.get('current_price', 0)
            
            take_profit_display = pos.get('take_profit_targets_text') or f"${pos.get('take_profit', 0):,.4f}"

            msg += f"""

<b>{i}.</b> <code>{_escape(pos['symbol'])}</code>  {side}
入场 <code>${pos.get('entry_price', 0):,.4f}</code>  |  现价 <code>${current_price:,.4f}</code>
止损 <code>${pos.get('stop_loss', 0):,.4f}</code>  |  止盈 <code>{_escape(take_profit_display)}</code>
盈亏 {pnl_emoji} <code>{pnl_sign}${pnl:,.2f}</code>  ({pos.get('unrealized_pnl_pct', 0):+.2f}%)"""
            
    return msg


def format_error_msg(error_type: str, message: str, symbol: str = None,
                     session_id: str = "", component: str = "") -> str:
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
) -> str:
    """格式化启动通知"""
    return f"""⚔️ <b>宙斯交易中枢 | 系统启动</b>

<b>模式</b>  <code>{_escape(mode_name)}</code>
<b>杠杆</b>  {leverage}x
<b>单笔风险</b>  {risk_pct:.2f}%
<b>止损 / 止盈</b>  {stop_loss_pct:.2f}% / {take_profit_pct:.2f}%
<b>扫描范围</b>  前 <code>{scan_top_n}</code> 个币种
<b>扫描间隔</b>  <code>{scan_interval_sec}</code> 秒
<b>最大持仓</b>  <code>{max_positions}</code> 个"""


def format_shutdown_msg(
    mode_name: str,
    closed_trades: int,
    realized_pnl: float,
    unrealized_pnl: float,
) -> str:
    """格式化停机通知"""
    return f"""🛑 <b>宙斯交易中枢 | 系统停止</b>

<b>模式</b>  <code>{_escape(mode_name)}</code>
<b>已平仓</b>  <code>{closed_trades}</code> 笔
<b>已实现</b>  <code>{realized_pnl:+,.2f} USDT</code>
<b>未实现</b>  <code>{unrealized_pnl:+,.2f} USDT</code>"""


def format_signal_message(signal: dict[str, Any], trade_result: dict[str, Any]) -> str:
    """Format a trading signal as a Telegram message.

    Args:
        signal: Signal dict with symbol, stage, direction, metrics
        trade_result: Trade execution result

    Returns:
        Formatted message string
    """
    symbol = signal.get("symbol", "UNKNOWN")
    stage = signal.get("stage", "UNKNOWN")
    direction = signal.get("direction", "UNKNOWN")
    metrics = signal.get("metrics", {})
    trade = trade_result

    # Emoji based on direction
    direction_emoji = "🟢" if direction == "LONG" else "🔴"
    stage_emoji = {
        "pre_break": "⚡",
        "confirmed_breakout": "🚀",
        "mania": "🔥",
        "exhaustion": "⚠️",
    }.get(stage, "📊")

    # Build message
    action = trade.get("action", "UNKNOWN")
    action_emoji = {"EXECUTED": "✅", "DRY_RUN": "🧪", "SKIPPED": "⏭️", "FAILED": "❌"}.get(action, "📝")

    msg = f"""{stage_emoji * 2} *BREAKOUT SIGNAL* {stage_emoji * 2}

{direction_emoji} *{symbol}* {direction}
📊 Stage: *{stage}*
{action_emoji} Action: *{action}*

━━━━━━━━━━━━━━━━━━━━
📈 *Market Metrics*
• 24h Change: `{metrics.get('change_24h', 0):+.2f}%`
• OI 24h: `{metrics.get('oi_24h', 0):+.2f}%`
• Funding: `{metrics.get('funding', 0):.6f}`
• L/S Ratio: `{metrics.get('ls_ratio', 0):.2f}`

━━━━━━━━━━━━━━━━━━━━
💰 *Trade Details*
"""

    if action in {"EXECUTED", "DRY_RUN"}:
        msg += f"""
• Entry Price: *${trade.get('entry_price', 0):,.2f}*
• Quantity: `{trade.get('quantity', 0)}`
• Position Value: *${trade.get('position_value_usdt', 0):,.2f}*
• Stop Loss: *${trade.get('stop_loss_price', 0):,.2f}*
• Risk: *${trade.get('risk_amount_usdt', 0):,.2f}*
"""
    else:
        msg += f"\n• Reason: {trade.get('reason', 'N/A')}\n"

    mode = "🧪 DRY RUN" if trade.get("action") == "DRY_RUN" else "💰 LIVE"
    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n Mode: *{mode}*"

    return msg


def send_signal_alert(signal: dict[str, Any], trade_result: dict[str, Any]) -> bool:
    """Send a signal alert to Telegram.

    Args:
        signal: Signal dict
        trade_result: Trade execution result

    Returns:
        True if sent successfully
    """
    message = format_signal_message(signal, trade_result)
    return send_telegram_message(message)


def send_daily_summary(trades: list[dict], pnl: float, balance: float) -> bool:
    """Send daily trading summary.

    Args:
        trades: List of trade results
        pnl: Daily PnL
        balance: Current balance

    Returns:
        True if sent successfully
    """
    total_trades = len(trades)
    executed = sum(1 for t in trades if t.get("trade_result", {}).get("action") == "EXECUTED")

    pnl_emoji = "🟢" if pnl >= 0 else "🔴"

    message = f"""
📊 *Daily Trading Summary*

{pnl_emoji} PnL: *${pnl:,.2f}*
💰 Balance: *${balance:,.2f}*

📈 Trades: *{total_trades}*
✅ Executed: *{executed}*

━━━━━━━━━━━━━━━━━━━━
_Binance Breakout Monitor_
"""
    return send_telegram_message(message)


def main():
    """Test Telegram notifications."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Telegram notifications")
    parser.add_argument("--message", "-m", default="Test message from binance-monitor")
    args = parser.parse_args()

    success = send_telegram_message(args.message)
    print(f"Message sent: {success}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
