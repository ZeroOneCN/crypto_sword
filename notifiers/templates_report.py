# -*- coding: utf-8 -*-
"""Telegram templates for reports, radar and analytics summaries."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from services.time_basis import report_clock_label, utc8_window_label
from .labels import (
    _escape,
    _fmt_usdt,
    _format_close_reason_label,
    format_direction_label,
    format_protection_failure_detail,
)

SEP = "━━━━━━━━━━━━━━━━━━━━"
GREEN = "🟢"
RED = "🔴"
WARN = "⚠️"
EMPTY = "📭"


def _signed_usdt(value: float) -> str:
    return f"{'+' if value >= 0 else '-'}{_fmt_usdt(abs(value))}"


def _pnl_emoji(value: float) -> str:
    return GREEN if value >= 0 else RED


def _ratio_value(value: Any) -> str:
    try:
        number = float(value or 0)
    except Exception:
        return "0.00"
    return "∞" if number >= 999 else f"{number:.2f}"


def _top_counts(counts: dict[str, Any], limit: int = 3) -> str:
    if not counts:
        return "无"
    parts = []
    for key, value in sorted(counts.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))[:limit]:
        parts.append(f"{_format_close_reason_label(key)} <code>{int(value or 0)}</code>")
    return " / ".join(parts) if parts else "无"


def _side_stats(stats: dict[str, Any]) -> str:
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


def _strategy_line(item: dict[str, Any] | None) -> str:
    if not item:
        return "无"
    name = str(item.get("name", "") or "未知策略")
    if name.upper() == "UNKNOWN":
        name = "历史未标记策略"
    count = int(item.get("count", 0) or 0)
    pnl = float(item.get("pnl", 0) or 0)
    wins = int(item.get("wins", 0) or 0)
    win_rate = wins / count * 100 if count else 0.0
    return f"{_escape(name)} {count}笔/{win_rate:.0f}%/{pnl:+.2f}"


def _trade_line(trade: dict[str, Any], default: str = "无") -> str:
    if not trade or not trade.get("symbol"):
        return default
    return f"{_escape(trade.get('symbol'))} <code>{float(trade.get('pnl', 0) or 0):+,.4f}</code>"


def format_daily_report_msg(report: dict[str, Any]) -> str:
    """Format a compact daily trading review."""
    report_date = str(report.get("date", "") or "")
    utc8_window = str(report.get("utc8_window", "") or "") or utc8_window_label(report_date)
    closed_trades = int(report.get("closed_trades", 0) or 0)
    total_pnl = float(report.get("total_pnl", 0) or 0)
    pnl_source = str(report.get("pnl_source", "") or "")
    income_summary = report.get("income_summary") or {}
    win_rate = float(report.get("win_rate", 0) or 0)
    avg_pnl = float(report.get("avg_pnl", 0) or 0)
    winning = int(report.get("winning_trades", 0) or 0)
    losing = int(report.get("losing_trades", 0) or 0)
    avg_win = float(report.get("avg_win", 0) or 0)
    avg_loss = float(report.get("avg_loss", 0) or 0)
    payoff_ratio = float(report.get("payoff_ratio", 0) or 0)
    profit_factor = float(report.get("profit_factor", 0) or 0)
    max_loss = float(report.get("max_loss", 0) or 0)

    lines = [
        "📝 <b>宙斯交易中枢 | 每日复盘</b>",
        f"📅 <code>{_escape(report_date)} UTC</code>",
        f"🕒 <code>{_escape(utc8_window)}</code>",
        SEP,
        f"💵 <b>{'交易所净盈亏' if pnl_source == 'exchange_income' else '净盈亏'}</b>：{_pnl_emoji(total_pnl)} <code>{_signed_usdt(total_pnl)} USDT</code>",
        f"📊 <b>胜率</b>：<code>{win_rate:.1f}%</code>（{winning}/{losing}）",
        f"📈 <b>平均单笔</b>：<code>{avg_pnl:+,.4f} USDT</code>",
        f"🔥 <b>收益因子</b>：<code>{_ratio_value(profit_factor)}</code>｜盈亏比 <code>{_ratio_value(payoff_ratio)}</code>",
    ]
    if pnl_source == "exchange_income":
        lines.append(
            "💸 <b>收入流水</b>："
            f"实现 <code>{float(income_summary.get('realized_pnl', 0) or 0):+,.4f}</code>｜"
            f"手续费 <code>{float(income_summary.get('commission', 0) or 0):+,.4f}</code>｜"
            f"资金费 <code>{float(income_summary.get('funding_fee', 0) or 0):+,.4f}</code>"
        )

    db_total_pnl = report.get("db_total_pnl")
    if db_total_pnl is not None:
        try:
            db_value = float(db_total_pnl or 0)
            if abs(db_value - total_pnl) >= 0.005:
                lines.append(f"📚 <b>本地平仓明细</b>：<code>{db_value:+,.4f} USDT</code>")
        except Exception:
            pass

    source_rows = int(report.get("source_rows", closed_trades) or closed_trades)
    split_rows = int(report.get("split_rows", 0) or 0)
    if source_rows != closed_trades or split_rows > 0:
        lines.append(f"🧾 <b>统计口径</b>：<code>{source_rows}</code> 行平仓记录 → <code>{closed_trades}</code> 笔完整交易")

    if closed_trades <= 0:
        lines.extend([SEP, "📭 当日无已平仓交易，系统继续观察。"])
        return "\n".join(lines)

    lines.extend(
        [
            SEP,
            "📌 <b>质量指标</b>",
            f"• 均盈 / 均亏：<code>{avg_win:+,.4f}</code> / <code>{avg_loss:+,.4f}</code>",
            f"• 最大单笔亏损：<code>{max_loss:+,.4f}</code>",
            f"🏆 <b>最佳交易</b>：{_trade_line(report.get('best_trade') or {})}",
            f"⚠️ <b>最差交易</b>：{_trade_line(report.get('worst_trade') or {})}",
        ]
    )

    reason_counts = report.get("reason_counts") or {}
    if reason_counts:
        lines.append(f"📌 <b>出场原因</b>：{_top_counts(reason_counts, limit=4)}")

    protection = report.get("entry_protection") or {}
    attempts = int(protection.get("attempts", 0) or 0)
    if attempts > 0:
        ok = int(protection.get("ok", 0) or 0)
        failed = int(protection.get("failed", 0) or 0)
        ok_rate = float(protection.get("ok_rate", 0) or 0)
        lines.append(f"🛡️ <b>保护单</b>：<code>{ok}</code>/<code>{failed}</code>｜成功率 <code>{ok_rate:.1f}%</code>")
        failed_by_detail = protection.get("failed_by_detail") or {}
        if failed_by_detail:
            detail_text = " / ".join(
                f"{_escape(format_protection_failure_detail(detail))}:{int(count or 0)}"
                for detail, count in list(failed_by_detail.items())[:3]
            )
            lines.append(f"• 失败原因：<code>{detail_text}</code>")

    oi_stats = report.get("oi_funding_stats") or {}
    enhanced_trades = int(oi_stats.get("enhanced_trades", 0) or 0)
    if enhanced_trades > 0:
        lines.extend(
            [
                "⚡ <b>OI/Funding 增强</b>",
                f"• 交易数：<code>{enhanced_trades}</code>",
                f"• 胜率：<code>{float(oi_stats.get('enhanced_win_rate', 0) or 0):.1f}%</code>",
                f"• 平均加分：<code>+{float(oi_stats.get('enhanced_avg_bonus', 0) or 0):.2f}</code>",
            ]
        )

    status = "当日表现健康" if total_pnl >= 0 else "当日回撤，下一轮优先控风险"
    lines.extend([SEP, f"{_pnl_emoji(total_pnl)} <b>当日状态</b>：{status}"])
    return "\n".join(lines)


def _format_period_block(report: dict[str, Any]) -> str:
    label = str(report.get("label") or f"近{int(report.get('period_days', 0) or 0)}天")
    closed = int(report.get("closed_trades", 0) or 0)
    total_pnl = float(report.get("total_pnl", 0) or 0)
    pnl_source = str(report.get("pnl_source", "") or "")
    source_rows = int(report.get("source_rows", closed) or closed)
    split_rows = int(report.get("split_rows", 0) or 0)
    lines = [
        f"📊 <b>{_escape(label)}（UTC）</b>",
        f"完整交易  <code>{closed}</code> 笔",
        f"{'交易所净盈亏' if pnl_source == 'exchange_income' else '总盈亏'}  {_pnl_emoji(total_pnl)} <code>{_signed_usdt(total_pnl)} USDT</code>",
        f"胜率  <code>{float(report.get('win_rate', 0) or 0):.1f}%</code>｜笔均 <code>{float(report.get('avg_pnl', 0) or 0):+,.4f}</code>",
        f"盈亏比  <code>{_ratio_value(report.get('payoff_ratio', 0))}</code>｜收益因子 <code>{_ratio_value(report.get('profit_factor', 0))}</code>",
    ]
    if source_rows != closed or split_rows > 0:
        lines.append(f"统计口径  <code>{source_rows}</code> 行 → <code>{closed}</code> 笔完整交易")

    db_total_pnl = report.get("db_total_pnl")
    if db_total_pnl is not None:
        try:
            db_value = float(db_total_pnl or 0)
            if abs(db_value - total_pnl) >= 0.005:
                lines.append(f"本地平仓明细  <code>{db_value:+,.4f} USDT</code>")
        except Exception:
            pass

    best_day = report.get("best_day") or {}
    worst_day = report.get("worst_day") or {}
    if best_day.get("date") or worst_day.get("date"):
        lines.append(
            f"最佳/最差日  <code>{_escape(best_day.get('date', '-'))} {float(best_day.get('pnl', 0) or 0):+,.2f}</code>"
            f" / <code>{_escape(worst_day.get('date', '-'))} {float(worst_day.get('pnl', 0) or 0):+,.2f}</code>"
        )

    lines.extend(
        [
            f"最佳  {_trade_line(report.get('best_trade') or {})}",
            f"最差  {_trade_line(report.get('worst_trade') or {})}",
            f"方向表现  <code>{_escape(_side_stats(report.get('side_stats') or {}))}</code>",
            f"主要原因  {_top_counts(report.get('reason_counts') or {})}",
            f"最好策略  <code>{_strategy_line(report.get('best_strategy'))}</code>",
            f"最差策略  <code>{_strategy_line(report.get('worst_strategy'))}</code>",
        ]
    )
    return "\n".join(lines)


def format_period_report_msg(reports: list[dict[str, Any]]) -> str:
    """Format rolling 7d/30d performance review for Telegram."""
    valid_reports = [report for report in reports if isinstance(report, dict)]
    lines = [
        "📈 <b>宙斯交易中枢 | 周期复盘</b>",
        f"🕒 <code>{_escape(report_clock_label())}</code>",
        "",
        "统计口径  <code>按流水号聚合；分批止盈只算 1 笔完整交易；以 Binance UTC 日为准</code>",
    ]
    if not valid_reports:
        lines.extend([SEP, "📭 暂无区间交易数据。"])
        return "\n".join(lines)

    total_pnl = sum(float(report.get("total_pnl", 0) or 0) for report in valid_reports)
    for report in valid_reports:
        lines.extend([SEP, _format_period_block(report)])
    status = "周期表现为正，继续观察盈亏比" if total_pnl >= 0 else "周期回撤，建议降低频率并复盘策略"
    lines.extend([SEP, f"{_pnl_emoji(total_pnl)} <b>当前状态</b>：{status}"])
    return "\n".join(lines)


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
    funding_pct = funding_rate * 100 if abs(funding_rate) < 1 else funding_rate
    vol_text = f"${volume_24h/1e6:.1f}M" if volume_24h > 0 else (f"${market_cap/1e6:.1f}M" if market_cap > 0 else "N/A")
    interpretation = _generate_dark_flow_interpretation(oi_change_pct, price_change_pct, funding_rate, dark_flow_score, score_total)
    return "\n".join(
        [
            "🔄 <b>宙斯交易中枢 | 雷达暗流</b>",
            f"🕒 <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>",
            SEP,
            f"<b>标的</b>  <code>{_escape(symbol)}</code>",
            f"<b>OI变化</b>  <code>{oi_change_pct:+.1f}%</code>",
            f"<b>价格变化</b>  <code>{price_change_pct:+.1f}%</code>",
            f"<b>资金费率</b>  <code>{funding_pct:+.4f}%</code>",
            f"<b>24h成交</b>  <code>{vol_text}</code>",
            f"<b>暗流评分</b>  <code>{dark_flow_score:.1f}</code>",
            SEP,
            f"📝 <b>解读</b>  {interpretation}",
        ]
    )


def _generate_dark_flow_interpretation(
    oi_change_pct: float,
    price_change_pct: float,
    funding_rate: float,
    dark_flow_score: float,
    score_total: float,
) -> str:
    parts: list[str] = []
    if oi_change_pct >= 20:
        parts.append("OI大幅扩张")
    elif oi_change_pct >= 10:
        parts.append("OI明显增长")
    elif oi_change_pct <= -10:
        parts.append("OI明显缩减")
    else:
        parts.append("OI相对稳定")

    if oi_change_pct >= 15 and abs(price_change_pct) <= 5:
        parts.append("资金进入但价格未明显启动，可能存在吸筹")
    elif oi_change_pct >= 15 and price_change_pct > 10:
        parts.append("OI与价格同步上行，趋势较健康")
    elif oi_change_pct >= 15 and price_change_pct < -5:
        parts.append("OI增加但价格下跌，警惕多头被套")

    funding_pct = funding_rate * 100 if abs(funding_rate) < 1 else funding_rate
    if funding_pct < -0.05:
        parts.append("费率为负，偏多信号")
    elif funding_pct > 0.05:
        parts.append("费率偏高，注意过热")

    if dark_flow_score >= 60:
        parts.append("暗流评分高，值得重点关注")
    elif dark_flow_score >= 45:
        parts.append("暗流评分中等，保持观察")
    if score_total >= 70:
        parts.append("综合评分较强")
    return "；".join(parts)


def format_accumulation_pool_report(pool: list, limit: int = 10) -> str:
    lines = [
        "🎯 <b>宙斯交易中枢 | 收筹池报告</b>",
        f"🕒 <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>",
        SEP,
        f"<b>池内标的</b>  <code>{len(pool)}</code>",
    ]
    for i, item in enumerate(pool[:limit], 1):
        lines.append(
            f"{i}. <code>{_escape(item.get('symbol', ''))}</code>｜"
            f"横盘 <code>{int(item.get('sideways_days', 0) or 0)}d</code>｜"
            f"振幅 <code>{float(item.get('price_range_pct', 0) or 0):.0f}%</code>｜"
            f"市值 <code>${float(item.get('market_cap_usd', 0) or 0)/1e6:.1f}M</code>"
        )
    if len(pool) > limit:
        lines.append(f"… 还有 <code>{len(pool) - limit}</code> 个标的未展示")
    lines.extend([SEP, "📝 横盘越久、振幅越小，越可能处在蓄势阶段。"])
    return "\n".join(lines)


def format_short_fuel_report(fuel_list: list, limit: int = 5) -> str:
    lines = [
        "🔥 <b>宙斯交易中枢 | 空头燃料报告</b>",
        f"🕒 <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>",
        SEP,
        f"<b>候选标的</b>  <code>{len(fuel_list)}</code>",
    ]
    for i, item in enumerate(fuel_list[:limit], 1):
        lines.append(
            f"{i}. <code>{_escape(item.get('symbol', ''))}</code>｜"
            f"费率 <code>{float(item.get('funding_rate', 0) or 0):+.3f}%</code>｜"
            f"涨跌 <code>{float(item.get('price_change_pct', 0) or 0):+.0f}%</code>｜"
            f"Vol <code>${float(item.get('volume_usd', 0) or 0)/1e6:.1f}M</code>"
        )
    if len(fuel_list) > limit:
        lines.append(f"… 还有 <code>{len(fuel_list) - limit}</code> 个标的未展示")
    lines.extend([SEP, "📝 费率越负，做空越拥挤；若价格转强，容易触发反向挤压。"])
    return "\n".join(lines)


def format_radar_summary(
    pool_count: int,
    oi_signals: int,
    dark_flows: int,
    short_fuel: int,
    top_dark_flow: str | None = None,
) -> str:
    lines = [
        "📡 <b>宙斯交易中枢 | 雷达摘要</b>",
        f"🕒 <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>",
        SEP,
        f"<b>收筹池</b>  <code>{pool_count}</code>",
        f"<b>OI异动</b>  <code>{oi_signals}</code>",
        f"<b>暗流信号</b>  <code>{dark_flows}</code>",
        f"<b>空头燃料</b>  <code>{short_fuel}</code>",
    ]
    if top_dark_flow:
        lines.append(f"<b>重点暗流</b>  <code>{_escape(top_dark_flow)}</code>")
    lines.extend([SEP, "✅ 雷达扫描完成"])
    return "\n".join(lines)
