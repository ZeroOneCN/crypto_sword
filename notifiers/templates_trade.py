# -*- coding: utf-8 -*-
"""Telegram templates for trade lifecycle and system events."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .labels import (
    _escape,
    _fmt_num,
    _fmt_price,
    _fmt_usdt,
    _format_close_reason_label,
    _format_component_label,
    _format_duration_from_hours,
    _format_error_type_label,
    _format_oi_funding_brief,
    _format_source_label,
    _format_take_profit_targets,
    _humanize_close_reason,
    format_direction_label,
)
from .telegram_sender import send_telegram_message

SEP = "━━━━━━━━━━━━━━━━━━━━"
GREEN = "🟢"
RED = "🔴"
WARN = "⚠️"
EMPTY = "📭"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _signed_usdt(value: float) -> str:
    return f"{'+' if value >= 0 else '-'}{_fmt_usdt(abs(value))}"


def _pnl_emoji(value: float) -> str:
    return GREEN if value >= 0 else RED


def _score_value(item: dict[str, Any]) -> float:
    score = item.get("score", 0)
    if isinstance(score, dict):
        return float(score.get("total_score", 0) or 0)
    return float(score or 0)


def _confidence_label(score: float, fallback: Any = "") -> str:
    if fallback:
        return str(fallback)
    if score >= 90:
        return "王炸"
    if score >= 80:
        return "极高"
    if score >= 60:
        return "高"
    if score >= 40:
        return "中"
    return "低"


def _status_emoji(status: str) -> str:
    status = str(status or "").lower()
    if status in {"ready", "confirmed", "entry", "open"} or "入场" in status:
        return GREEN
    if status in {"invalid", "failed", "blocked", "rejected"} or any(word in status for word in ("失效", "拒绝", "失败")):
        return RED
    if status in {"watch", "waiting"} or any(word in status for word in ("观察", "等待")):
        return WARN
    return EMPTY


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
        expected_tp_total += max(0.0, (target_price - entry_price) * target_quantity if direction == "LONG" else (entry_price - target_price) * target_quantity)

    lines = [
        f"{GREEN} <b>宙斯交易中枢 | 开仓成功</b>",
        f"🕒 <code>{_now_text()}</code>",
        "",
        f"🔥 <b>{_escape(symbol)}</b>｜{direction_text}｜<code>{leverage}x</code>",
        SEP,
        f"💵 <b>入场</b>：<code>{_fmt_price(entry_price)}</code>",
        f"📦 <b>仓位</b>：<code>{_fmt_num(quantity)}</code>｜名义 <code>{_fmt_usdt(notional_value)} USDT</code>",
        f"🛑 <b>止损</b>：<code>{_fmt_price(stop_loss)}</code>（{sl_pct:.2f}%）｜预计 <code>-{_fmt_usdt(expected_sl_loss)} USDT</code>",
        f"📈 <b>止盈</b>：<code>{_fmt_price(take_profit)}</code>（{tp_pct:.2f}%）",
        f"🎯 <b>风险</b>：<code>{_fmt_usdt(risk_amount)} USDT</code>｜<code>{risk_pct:.2f}%</code>",
    ]
    if expected_tp_total > 0:
        lines.append(f"💰 <b>预计止盈</b>：<code>+{_fmt_usdt(expected_tp_total)} USDT</code>")
    if strategy_line:
        lines.append(f"🧭 <b>策略</b>：<code>{_escape(strategy_line)}</code>")
    oi_funding_line = _format_oi_funding_brief(oi_funding)
    if oi_funding_line:
        lines.append(oi_funding_line)
    if target_roi_pct > 0:
        lines.append(f"🚀 <b>目标收益率</b>：<code>{target_roi_pct:.2f}% ROI</code>")
    if price_move_pct > 0:
        lines.append(f"📊 <b>实际价格目标</b>：<code>{price_move_pct:.2f}%</code>")
    if capital_plan:
        mode = _escape(capital_plan.get("mode", ""))
        expected_rr = float(capital_plan.get("expected_rr", 0) or 0)
        lines.append(f"🧮 <b>资金档位</b>：<code>{mode}</code>｜EV <code>{expected_rr:.2f}R</code>")
        locked_profit = float(capital_plan.get("locked_profit", 0) or 0)
        if locked_profit > 0:
            lines.append(f"🔐 <b>盈利锁定</b>：<code>{_fmt_usdt(locked_profit)} USDT</code>")
    if take_profit_targets:
        lines.append("")
        lines.append("📊 <b>分批止盈</b>")
        lines.append(_format_take_profit_targets(take_profit_targets, entry_price, direction))
    if score > 0:
        lines.append(f"⭐ <b>评分</b>：<code>{score:.0f}/100</code>｜{_confidence_label(score)}")
    if risk_level:
        lines.append(f"🛡️ <b>风险等级</b>：<code>{_escape(risk_level)}</code>")
    if session_id:
        lines.append(f"🧾 <b>流水号</b>：<code>{_escape(session_id)}</code>")
    lines.extend([SEP, "✅ 已成交，保护单将同步确认"])
    return "\n".join(lines)


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
    del oi_funding
    direction_text = format_direction_label(direction)
    emoji = _pnl_emoji(pnl)
    reason_text = _humanize_close_reason(reason)
    lines = [
        f"{emoji} <b>宙斯交易中枢 | 平仓完成</b>",
        f"🕒 <code>{_now_text()}</code>",
        "",
        f"<b>{_escape(symbol)}</b>｜{direction_text}",
        SEP,
        f"💵 <b>价格</b>：<code>{_fmt_price(entry_price)}</code> → <code>{_fmt_price(exit_price)}</code>",
        f"📦 <b>数量</b>：<code>{_fmt_num(quantity)}</code>",
        f"💰 <b>盈亏</b>：{emoji} <b>{_signed_usdt(pnl)} USDT</b>（<code>{pnl_pct:+.2f}%</code>）",
        f"📌 <b>原因</b>：<code>{_escape(reason_text)}</code>",
    ]
    if price_move_pct:
        lines.append(f"📊 <b>价格涨幅</b>：<code>{price_move_pct:+.2f}%</code>")
    if roi_pct:
        lines.append(f"🚀 <b>实际 ROI</b>：<code>{roi_pct:+.2f}%</code>")
    if strategy_line:
        lines.append(f"🧭 <b>策略</b>：<code>{_escape(strategy_line)}</code>")
    if duration_hours > 0:
        lines.append(f"⏱ <b>持仓</b>：{_format_duration_from_hours(duration_hours)}")
    if session_id:
        lines.append(f"🧾 <b>流水号</b>：<code>{_escape(session_id)}</code>")
    status = "✅ 盈利离场，记录已入库" if pnl >= 0 else "🔴 亏损离场，等待复盘优化"
    lines.extend([SEP, status])
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
    del pnl_source
    direction_text = format_direction_label(direction)
    level_text = f"TP{level}" if level else "部分止盈"
    price_move = (exit_price - entry_price) / entry_price * 100 if entry_price else 0.0
    if direction == "SHORT":
        price_move = -price_move
    lines = [
        f"🔄 <b>宙斯交易中枢 | 分批止盈成交</b>",
        f"🕒 <code>{_now_text()}</code>",
        "",
        f"<b>{_escape(symbol)}</b>｜{direction_text}｜<b>{_escape(level_text)}</b>",
        SEP,
        f"💵 <b>成交价格</b>：<code>{_fmt_price(exit_price)}</code>",
        f"📦 <b>止盈数量</b>：<code>{_fmt_num(quantity)}</code>｜剩余 <code>{_fmt_num(remaining_quantity)}</code>",
        f"💰 <b>本次盈亏</b>：{_pnl_emoji(pnl)} <b>{_signed_usdt(pnl)} USDT</b>（<code>{pnl_pct:+.2f}%</code>）",
        f"📊 <b>价格涨幅</b>：<code>{price_move:+.2f}%</code>",
    ]
    if strategy_line:
        lines.append(f"🧭 <b>策略</b>：<code>{_escape(strategy_line)}</code>")
    if session_id:
        lines.append(f"🧾 <b>流水号</b>：<code>{_escape(session_id)}</code>")
    lines.extend([SEP, "✅ 已落袋一档，剩余仓位继续跟踪"])
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
    tp_ids = take_profit_order_ids or []
    ok = stop_loss_ok and take_profit_ok
    title = f"🛡️ <b>宙斯交易中枢 | 保护单确认</b>" if ok else f"{WARN} <b>宙斯交易中枢 | 裸仓风险</b>"
    status = "✅ 已受保护" if ok else "❌ 保护不完整"
    sl_text = f"✅ {stop_loss_order_id}" if stop_loss_ok else "❌ 缺失"
    tp_text = f"✅ {', '.join(str(x) for x in tp_ids)}" if take_profit_ok else "❌ 缺失"
    lines = [
        title,
        f"🕒 <code>{_now_text()}</code>",
        "",
        f"<b>标的</b>  <code>{_escape(symbol)}</code>",
        SEP,
        f"<b>状态</b>  <code>{_escape(status)}</code>",
        f"<b>止损单</b>  <code>{_escape(sl_text)}</code>",
        f"<b>止盈单</b>  <code>{_escape(tp_text)}</code>",
        f"<b>来源</b>  <code>{_escape(_format_source_label(source))}</code>",
    ]
    if message:
        lines.append(f"<b>说明</b>  <code>{_escape(message)}</code>")
    if session_id:
        lines.append(f"<b>流水号</b>  <code>{_escape(session_id)}</code>")
    lines.extend([SEP, "✅ 保护完整" if ok else "⚠️ 暂停裸奔，等待系统处理"])
    return "\n".join(lines)


def format_latency_alert_msg(
    flow: str,
    total_ms: float,
    steps: list[tuple[str, float]],
    symbol: str = "",
    threshold_ms: float = 5000,
) -> str:
    slowest_name = ""
    slowest_ms = 0.0
    if steps:
        slowest_name, slowest_ms = max(steps, key=lambda item: item[1])
    lines = [
        f"⚡ <b>宙斯交易中枢 | 延迟警告</b>",
        f"🕒 <code>{_now_text()}</code>",
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
        lines.extend(f"• {_escape(name)}  <code>{elapsed_ms:.0f} ms</code>" for name, elapsed_ms in steps)
    return "\n".join(lines)


def format_summary_msg(
    positions: list,
    total_pnl: float,
    realized_pnl: float,
    total_balance: float = 0.0,
    available_balance: float = 0.0,
    daily_stats: dict | None = None,
) -> str:
    stats_date = str((daily_stats or {}).get("date", "") or "")
    closed = int((daily_stats or {}).get("closed_trades", 0) or 0)
    win_rate = float((daily_stats or {}).get("win_rate", 0) or 0)
    avg_pnl = float((daily_stats or {}).get("avg_pnl", 0) or 0)
    best_trade = (daily_stats or {}).get("best_trade") or {}
    worst_trade = (daily_stats or {}).get("worst_trade") or {}
    income_summary = (daily_stats or {}).get("income_summary") or {}
    pnl_source = str((daily_stats or {}).get("pnl_source", "") or "")

    date_line = f"📅 <code>{_escape(stats_date)} UTC</code>" if stats_date else f"🕒 <code>{_now_text()}</code>"
    lines = [
        "📊 <b>宙斯交易中枢 | 持仓汇总</b>",
        date_line,
        SEP,
        "💼 <b>账户状态</b>",
        f"• 持仓数：<code>{len(positions)}</code>",
        f"• 未实现盈亏：<code>{_signed_usdt(total_pnl)} USDT</code>",
        f"• 已实现盈亏：<code>{_signed_usdt(realized_pnl)} USDT</code>",
    ]
    if total_balance > 0:
        lines.append(f"• 总余额：<code>{_fmt_usdt(total_balance)} USDT</code>")
    if available_balance > 0:
        lines.append(f"• 可用余额：<code>{_fmt_usdt(available_balance)} USDT</code>")
    if pnl_source == "exchange_income":
        lines.append(
            "• 收入流水："
            f"实现 <code>{float(income_summary.get('realized_pnl', 0) or 0):+,.4f}</code>｜"
            f"手续费 <code>{float(income_summary.get('commission', 0) or 0):+,.4f}</code>｜"
            f"资金费 <code>{float(income_summary.get('funding_fee', 0) or 0):+,.4f}</code>"
        )

    if not positions:
        lines.extend([SEP, "📭 当前无持仓，系统保持待命。"])
    else:
        lines.append(SEP)
        lines.append("📌 <b>当前持仓</b>")
        for i, pos in enumerate(positions, 1):
            pnl = float(pos.get("unrealized_pnl", 0) or 0)
            price_move_pct = float(pos.get("unrealized_pnl_pct", 0) or 0)
            roi_pct = float(pos.get("unrealized_roi_pct", price_move_pct) or 0)
            take_profit_display = pos.get("take_profit_targets_text") or _fmt_price(float(pos.get("take_profit", 0) or 0))
            stop_suffix = "（估算）" if pos.get("stop_loss_estimated") else ""
            lines.extend(
                [
                    "",
                    f"{i}. <b>{_escape(pos.get('symbol', 'UNKNOWN'))}</b>｜{format_direction_label(pos.get('side', 'UNKNOWN'))}",
                    f"入场 <code>{_fmt_price(float(pos.get('entry_price', 0) or 0))}</code>｜现价 <code>{_fmt_price(float(pos.get('current_price', 0) or 0))}</code>",
                    f"止损 <code>{_fmt_price(float(pos.get('stop_loss', 0) or 0))}</code>{stop_suffix}｜止盈 <code>{_escape(take_profit_display)}</code>",
                    f"盈亏 {_pnl_emoji(pnl)} <code>{_signed_usdt(pnl)} USDT</code>｜价格 <code>{price_move_pct:+.2f}%</code>｜ROI <code>{roi_pct:+.2f}%</code>",
                ]
            )

    lines.extend(
        [
            SEP,
            f"📈 <b>当日统计</b>：已平仓 <code>{closed}</code> 笔｜胜率 <code>{win_rate:.1f}%</code>｜笔均 <code>{avg_pnl:+.4f}</code>",
        ]
    )
    if best_trade:
        lines.append(f"🏆 最佳：<code>{_escape(best_trade.get('symbol', '-'))}</code> <code>{float(best_trade.get('pnl', 0) or 0):+,.4f}</code>")
    if worst_trade:
        lines.append(f"⚠️ 最差：<code>{_escape(worst_trade.get('symbol', '-'))}</code> <code>{float(worst_trade.get('pnl', 0) or 0):+,.4f}</code>")
    status = "🟢 当前整体健康" if total_pnl >= 0 else "🔴 当前浮亏，优先确认保护单"
    lines.append(status)
    return "\n".join(lines)


def format_error_msg(
    error_type: str,
    message: str,
    symbol: str | None = None,
    session_id: str = "",
    component: str = "",
) -> str:
    lines = [
        f"{RED} <b>宙斯交易中枢 | 交易异常</b>",
        f"🕒 <code>{_now_text()}</code>",
        SEP,
        f"<b>类型</b>  <code>{_escape(_format_error_type_label(error_type))}</code>",
    ]
    if component:
        lines.append(f"<b>组件</b>  <code>{_escape(_format_component_label(component))}</code>")
    if symbol:
        lines.append(f"<b>标的</b>  <code>{_escape(symbol)}</code>")
    if session_id:
        lines.append(f"<b>流水号</b>  <code>{_escape(session_id)}</code>")
    lines.extend(["", "<b>详情</b>", f"<code>{_escape(message)}</code>", SEP, "⚠️ 已记录异常，系统会按风控继续处理"])
    return "\n".join(lines)


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
    risk_level = "进取" if risk_pct >= 1.0 else "稳健"
    tp_mode_label = "ROI" if take_profit_mode == "roi" else "价格"
    trail_text = f"追踪 {trailing_stop_pct:.1f}%" if trailing_enabled else "追踪关闭"
    return "\n".join(
        [
            "🚀 <b>宙斯交易中枢 | 系统启动</b>",
            f"🕒 <code>{_now_text()}</code>",
            "",
            f"🔥 <b>风险等级</b>：<code>{risk_level}</code>",
            "",
            f"💵 <b>模式</b>：<code>{_escape(mode_name)}</code>",
            f"⚙️ <b>杠杆</b>：<code>{leverage}x</code>",
            f"🎯 <b>单笔风险</b>：<code>{risk_pct:.2f}%</code>",
            "",
            f"🛑 <b>止损</b>：<code>{stop_loss_pct:.2f}%</code>",
            f"📈 <b>止盈</b>：<code>{take_profit_pct:.2f}% {tp_mode_label}</code>（{trail_text}）",
            "📊 <b>止盈策略</b>：分批 3 档（50% / 100% / 150%）",
            "",
            f"🔍 <b>扫描范围</b>：前 <code>{scan_top_n}</code> 币种",
            f"⏱ <b>扫描间隔</b>：<code>{scan_interval_sec}</code> 秒",
            f"📛 <b>最大持仓</b>：<code>{max_positions}</code>",
            "",
            "📝 <b>说明</b>",
            "实际 TP / SL 会因策略线（突破 / 回踩）乘数而浮动",
            SEP,
            "✅ 系统已就绪",
        ]
    )


def format_shutdown_msg(
    mode_name: str,
    closed_trades: int,
    realized_pnl: float,
    unrealized_pnl: float,
) -> str:
    return "\n".join(
        [
            "🛑 <b>宙斯交易中枢 | 系统停止</b>",
            f"🕒 <code>{_now_text()}</code>",
            SEP,
            f"<b>模式</b>  <code>{_escape(mode_name)}</code>",
            f"<b>已平仓</b>  <code>{closed_trades}</code> 笔",
            f"<b>已实现</b>  <code>{_signed_usdt(realized_pnl)} USDT</code>",
            f"<b>未实现</b>  <code>{_signed_usdt(unrealized_pnl)} USDT</code>",
            SEP,
            "✅ 已安全退出",
        ]
    )


def format_signal_message(signal: dict[str, Any], trade_result: dict[str, Any]) -> str:
    symbol = signal.get("symbol", "UNKNOWN")
    direction = signal.get("direction", "UNKNOWN")
    metrics = signal.get("metrics", {}) or {}
    trade = trade_result or {}
    action = trade.get("action", "UNKNOWN")
    change_24h = float(metrics.get("change_24h_pct", metrics.get("change_24h", 0)) or 0)
    oi_24h = float(metrics.get("oi_24h_pct", metrics.get("oi_24h", 0)) or 0)
    funding = float(metrics.get("funding_rate", metrics.get("funding", 0)) or 0)
    lines = [
        "📗 <b>宙斯交易中枢 | 信号执行</b>",
        f"🕒 <code>{_now_text()}</code>",
        SEP,
        f"<b>{_escape(symbol)}</b>｜{format_direction_label(direction)}｜<code>{_escape(action)}</code>",
        f"• 24h <code>{change_24h:+.2f}%</code>｜OI <code>{oi_24h:+.2f}%</code>｜Funding <code>{funding:+.4%}</code>",
    ]
    if action == "EXECUTED":
        lines.extend(
            [
                f"• 入场 <code>{_fmt_price(float(trade.get('entry_price', 0) or 0))}</code>｜数量 <code>{_fmt_num(trade.get('quantity', 0))}</code>",
                f"• 名义 <code>{_fmt_usdt(float(trade.get('position_value_usdt', 0) or 0))} USDT</code>｜止损 <code>{_fmt_price(float(trade.get('stop_loss_price', 0) or 0))}</code>",
                "✅ 信号已执行",
            ]
        )
    else:
        lines.append(f"• 原因 <code>{_escape(trade.get('reason', 'N/A'))}</code>")
        lines.append("⚠️ 信号未执行")
    return "\n".join(lines)


def send_signal_alert(signal: dict[str, Any], trade_result: dict[str, Any]) -> bool:
    return send_telegram_message(format_signal_message(signal, trade_result))


def format_scan_monitor_msg(
    signals: list[dict[str, Any]],
    scanned_count: int = 0,
    max_items: int = 5,
    report_title: str = "宙斯交易中枢 | 候选变化",
    count_label: str = "候选范围",
) -> str:
    long_items: list[str] = []
    short_items: list[str] = []
    ready_count = 0
    watch_count = 0
    for item in signals[:max_items]:
        direction = str(item.get("direction", "") or "").upper()
        score = _score_value(item)
        score_obj = item.get("score") if isinstance(item.get("score"), dict) else {}
        confidence = _confidence_label(score, score_obj.get("confidence", ""))
        metrics = item.get("metrics", {}) or {}
        funding_pct = float(metrics.get("funding_rate", 0) or 0) * 100
        price = float(metrics.get("last_price", item.get("price", 0)) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_24h = float(metrics.get("oi_24h_pct", 0) or 0)
        strategy_line = item.get("strategy_line", "")
        entry_status_text = item.get("entry_status_text", "")
        entry_note = item.get("entry_note", "")
        watch_stage = item.get("watch_stage", "")
        status_emoji = _status_emoji(entry_status_text or item.get("entry_status", ""))
        if status_emoji == GREEN:
            ready_count += 1
        else:
            watch_count += 1
        block = [
            f"• <b>{_escape(item.get('symbol', 'UNKNOWN'))}</b> 评分 <code>{score:.1f}</code>（{_escape(confidence)}）",
            f"• 费率 <code>{funding_pct:+.4f}%</code>｜价格 <code>{change_24h:+.2f}%</code>",
            f"• OI <code>{oi_24h:+.2f}%</code>｜现价 <code>${price:,.6f}</code>",
        ]
        if strategy_line:
            block.append(f"• 策略：<code>{_escape(strategy_line)}</code>")
        if watch_stage:
            block.append(f"• 阶段：<code>{_escape(watch_stage)}</code>")
        if entry_status_text:
            block.append(f"• 状态：{status_emoji} {_escape(entry_status_text)}")
        if entry_note:
            block.append(f"• 说明：{_escape(entry_note)}")
        target = long_items if direction in {"LONG", "BUY", "CONSIDER_LONG"} else short_items
        target.append("\n".join(block))

    title = report_title or "宙斯交易中枢 | 候选变化"
    lines = [
        f"📗 <b>{_escape(title)}</b>",
        f"🕒 <code>{_now_text()}</code>",
        "",
        f"🔍 <b>{_escape(count_label)}</b>：<code>{scanned_count}</code>｜<b>有效信号</b>：<code>{len(signals)}</code>",
        SEP,
        "📈 <b>跟多信号</b>",
        "\n\n".join(long_items) if long_items else "📭 暂无跟多信号",
        SEP,
        "📉 <b>跟空信号</b>",
        "\n\n".join(short_items) if short_items else "📭 暂无跟空信号",
        SEP,
    ]
    if not signals:
        lines.append("📭 暂无有效信号，继续等待。")
    elif ready_count > 0:
        lines.append(f"🟢 {ready_count} 个信号已就绪，系统将按风控尝试入场。")
    else:
        lines.append(f"⚠️ {watch_count} 个信号观察中，等待确认。")
    return "\n".join(lines)
