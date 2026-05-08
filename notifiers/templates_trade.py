# -*- coding: utf-8 -*-
"""Telegram templates for trade lifecycle and system events."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from services.time_basis import report_clock_label, utc8_window_label
from .labels import (
    _escape,
    _fmt_num,
    _fmt_price,
    _fmt_price_code,
    _fmt_usdt,
    _format_close_reason_label,
    _format_component_label,
    _format_duration_from_hours,
    _format_error_type_label,
    _format_oi_funding_brief,
    _format_protection_failure_detail,
    _format_source_label,
    _format_take_profit_targets,
    _humanize_close_reason,
    format_direction_label,
)
from .telegram_sender import send_telegram_message

_E = "\U0001f7e2"
_E2 = "\U0001f534"
_E3 = "\U0001f6d1"

def _compact_trade_notify_enabled() -> bool:
    return os.environ.get("TELEGRAM_COMPACT_TRADE_NOTIFY", "1").strip().lower() not in {"0", "false", "no"}

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
    if _compact_trade_notify_enabled():
        lines = [
            f"{direction_emoji} <b>宙斯交易中枢 | 开仓</b>",
            "",
            f"<b>{_escape(symbol)}</b>  {direction_text}  <code>{leverage}x</code>",
            f"<b>入场</b> <code>{_fmt_price(entry_price)}</code>  <b>数量</b> <code>{_fmt_num(quantity)}</code>",
            f"<b>名义</b> <code>{_fmt_usdt(notional_value)} USDT</code>",
            f"<b>SL</b> <code>{_fmt_price(stop_loss)}</code> ({sl_pct:.1f}%)  <b>TP</b> <code>{_fmt_price(take_profit)}</code> ({tp_pct:.1f}%)",
            f"<b>风险</b> <code>{_fmt_usdt(risk_amount)} USDT</code>  |  <code>{risk_pct:.1f}%</code>",
        ]
        if strategy_line:
            lines.append(f"<b>策略</b> <code>{_escape(strategy_line)}</code>")
        if score > 0:
            lines.append(f"<b>评分</b> <code>{score:.0f}/100</code>")
        if session_id:
            lines.append(f"<b>流水</b> <code>{_escape(session_id)}</code>")
        return "\n".join(lines)

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
<b>入场</b>  <code>{_fmt_price(entry_price)}</code>  {leverage}x
<b>数量</b>  <code>{_fmt_num(quantity)}</code>  |  名义 <code>{_fmt_usdt(notional_value)} USDT</code>
<b>止损</b>  <code>{_fmt_price(stop_loss)}</code>  ({sl_pct:.1f}%)  |  预计 <code>-{_fmt_usdt(expected_sl_loss)} USDT</code>
<b>止盈</b>  <code>{_fmt_price(take_profit)}</code>  ({tp_pct:.1f}%)
<b>风险</b>  <code>{_fmt_usdt(risk_amount)} USDT</code>  |  {risk_pct:.1f}%"""

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
            msg += f"\n<b>盈利锁仓</b>  <code>{_fmt_usdt(locked_profit)} USDT</code>"
    if take_profit_targets:
        if expected_tp_total > 0:
            msg += f"\n<b>预计止盈</b>  <code>+{_fmt_usdt(expected_tp_total)} USDT</code>"
        msg += f"\n<b>分批止盈</b>\n{_format_take_profit_targets(take_profit_targets, entry_price, direction)}"

    if score > 0:
        confidence = "极高" if score >= 80 else ("高" if score >= 60 else ("中" if score >= 40 else "低"))
        msg += f"\n<b>评分</b>  <code>{score:.0f}/100</code>  |  {confidence}"
    if risk_level:
        msg += f"\n<b>风险等级</b>  <code>{_escape(risk_level)}</code>"
    if session_id:
        msg += f"\n<b>流水号</b>  <code>{_escape(session_id)}</code>"

    return msg

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
    """Format a compact close-position notification."""
    del quantity, oi_funding
    direction_text = format_direction_label(direction)
    pnl_emoji = _E if pnl >= 0 else _E2
    pnl_sign = "+" if pnl >= 0 else ""
    title_emoji = _E if pnl >= 0 else _E2
    lines = [
        f"{title_emoji} <b>宙斯交易中枢 | 平仓</b>",
        "",
        f"<b>{_escape(symbol)}</b>  {direction_text}",
        f"<b>价格</b> <code>{_fmt_price(entry_price)}</code> → <code>{_fmt_price(exit_price)}</code>",
        f"<b>盈亏</b> {pnl_emoji} <b>{pnl_sign}{_fmt_usdt(pnl)} USDT</b>  (<code>{pnl_sign}{pnl_pct:.2f}%</code>)",
        f"<b>原因</b> <code>{_escape(_humanize_close_reason(reason))}</code>",
    ]
    if price_move_pct:
        lines.append(f"<b>价格涨幅</b> <code>{price_move_pct:+.2f}%</code>")
    if roi_pct:
        lines.append(f"<b>ROI</b> <code>{roi_pct:+.2f}%</code>")
    if strategy_line:
        lines.append(f"<b>策略</b> <code>{_escape(strategy_line)}</code>")
    if duration_hours > 0:
        lines.append(f"<b>持仓</b> {_format_duration_from_hours(duration_hours)}")
    if session_id:
        lines.append(f"<b>流水</b> <code>{_escape(session_id)}</code>")
    return "\n".join(lines)

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
    """Format a compact partial-take-profit notification."""
    del entry_price, pnl_source
    direction_text = format_direction_label(direction)
    pnl_sign = "+" if pnl >= 0 else ""
    pnl_emoji = _E if pnl >= 0 else _E2
    level_text = f"TP{level}" if level else "部分止盈"
    lines = [
        f"{_E} <b>宙斯交易中枢 | 分批止盈</b>",
        "",
        f"<b>{_escape(symbol)}</b>  {direction_text}  |  <b>{_escape(level_text)}</b>",
        f"<b>成交</b> <code>{_fmt_price(exit_price)}</code>  <b>数量</b> <code>{_fmt_num(quantity)}</code>",
        f"<b>本次</b> {pnl_emoji} <b>{pnl_sign}{_fmt_usdt(pnl)} USDT</b>  (<code>{pnl_sign}{pnl_pct:.2f}%</code>)",
        f"<b>剩余</b> <code>{_fmt_num(remaining_quantity)}</code>",
    ]
    if strategy_line:
        lines.append(f"<b>策略</b> <code>{_escape(strategy_line)}</code>")
    if session_id:
        lines.append(f"<b>流水</b> <code>{_escape(session_id)}</code>")
    return "\n".join(lines)

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
    stats_date = ""
    stats_window = ""
    db_total_pnl: float | None = None
    pnl_source = ""
    income_summary: dict[str, Any] = {}
    if isinstance(daily_stats, dict):
        stats_date = str(daily_stats.get("date", "") or "")
        stats_window = str(daily_stats.get("utc8_window", "") or "")
        pnl_source = str(daily_stats.get("pnl_source", "") or "")
        income_summary = daily_stats.get("income_summary") or {}
        if "db_total_pnl" in daily_stats:
            try:
                db_total_pnl = float(daily_stats.get("db_total_pnl", 0) or 0)
            except Exception:
                db_total_pnl = None
    realized_label = "交易所净盈亏(UTC日)" if pnl_source == "exchange_income" else "已实现(UTC日)"
    msg = f"""📊 <b>宙斯交易中枢 | 持仓汇总</b>

<b>持仓数</b>  <code>{len(positions)}</code>
<b>未实现</b>  <code>{"+" if total_pnl >= 0 else "-"}{_fmt_usdt(abs(total_pnl))} USDT</code>
<b>{realized_label}</b>  <code>{"+" if realized_pnl >= 0 else "-"}{_fmt_usdt(abs(realized_pnl))} USDT</code>"""
    if pnl_source == "exchange_income":
        msg += (
            f"\n<b>收入流水</b>  实现 <code>{float(income_summary.get('realized_pnl', 0) or 0):+,.4f}</code>"
            f" | 手续费 <code>{float(income_summary.get('commission', 0) or 0):+,.4f}</code>"
            f" | 资金费 <code>{float(income_summary.get('funding_fee', 0) or 0):+,.4f}</code>"
        )
    if db_total_pnl is not None and abs(db_total_pnl - float(realized_pnl or 0.0)) >= 0.005:
        msg += f"\n<b>本地平仓明细</b>  <code>{db_total_pnl:+,.4f} USDT</code>"
    if stats_date:
        msg += f"\n<b>统计日</b>  <code>{_escape(stats_date)} UTC</code>"
        if stats_window:
            msg += f"\n<b>北京时间</b>  <code>{_escape(stats_window)}</code>"
    if total_balance > 0:
        msg += f"\n<b>总余额</b>  <code>{_fmt_usdt(total_balance)} USDT</code>"
    if available_balance > 0:
        msg += f"\n<b>可用余额</b>  <code>{_fmt_usdt(available_balance)} USDT</code>"

    if not positions:
        msg += "\n\n📭 <b>当前无持仓</b>\n<code>系统保持待命，等待下一次高质量入场信号。</code>"
        return msg

    for i, pos in enumerate(positions, 1):
        pnl = float(pos.get("unrealized_pnl", 0) or 0)
        pnl_sign = "+" if pnl >= 0 else ""
        pnl_emoji = _E if pnl >= 0 else _E2
        side = format_direction_label(pos.get("side", "UNKNOWN"))
        current_price = float(pos.get("current_price", 0) or 0)
        price_move_pct = float(pos.get("unrealized_pnl_pct", 0) or 0)
        roi_pct = float(pos.get("unrealized_roi_pct", price_move_pct) or 0)
        take_profit_display = pos.get("take_profit_targets_text") or f"{_fmt_price(float(pos.get('take_profit', 0) or 0))}"
        stop_suffix = " 估算" if pos.get("stop_loss_estimated") else ""

        msg += f"""

<b>{i}.</b> <code>{_escape(pos.get('symbol', 'UNKNOWN'))}</code>  {side}
入场 <code>{_fmt_price(float(pos.get('entry_price', 0) or 0))}</code>  |  现价 <code>{_fmt_price(current_price)}</code>
止损 <code>{_fmt_price(float(pos.get('stop_loss', 0) or 0))}</code>{stop_suffix}  |  止盈 <code>{_escape(take_profit_display)}</code>
盈亏 {pnl_emoji} <code>{pnl_sign}{_fmt_usdt(pnl)} USDT</code>
价格 <code>{price_move_pct:+.2f}%</code>  |  ROI <code>{roi_pct:+.2f}%</code>"""

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
<b>已实现</b>  <code>{"+" if realized_pnl >= 0 else "-"}{_fmt_usdt(abs(realized_pnl))} USDT</code>
<b>未实现</b>  <code>{"+" if unrealized_pnl >= 0 else "-"}{_fmt_usdt(abs(unrealized_pnl))} USDT</code>"""

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

        is_long_signal = direction in {"LONG", "CONSIDER_LONG"}
        tag = "跟多信号" if is_long_signal else "跟空信号"
        signal_emoji = "📈" if is_long_signal else "📉"
        if funding_pct <= -0.5:
            funding_tag = "极负费率"
        elif funding_pct < 0:
            funding_tag = "负费率"
        else:
            funding_tag = "正费率"

        msg += f"""

{signal_emoji} <b>{_escape(tag)} ({_escape(funding_tag)})</b>
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

