# -*- coding: utf-8 -*-
"""Telegram templates for reports, radar and analytics summaries."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from services.time_basis import report_clock_label, utc8_window_label
from .labels import (
    _escape,
    _fmt_price,
    _fmt_usdt,
    _format_close_reason_label,
    format_direction_label,
)

_E = "\U0001f7e2"
_E2 = "\U0001f534"
_E3 = "\U0001f6d1"

def format_daily_report_msg(report: dict[str, Any]) -> str:
    """Format a compact daily trading review."""
    report_date = _escape(report.get("date", ""))
    utc8_window = str(report.get("utc8_window", "") or "")
    if not utc8_window and report_date:
        utc8_window = utc8_window_label(str(report.get("date", "")))
    closed_trades = int(report.get("closed_trades", 0) or 0)
    total_pnl = float(report.get("total_pnl", 0) or 0)
    pnl_source = str(report.get("pnl_source", "") or "")
    db_total_pnl = report.get("db_total_pnl")
    income_summary = report.get("income_summary") or {}
    win_rate = float(report.get("win_rate", 0) or 0)
    avg_pnl = float(report.get("avg_pnl", 0) or 0)
    winning = int(report.get("winning_trades", 0) or 0)
    losing = int(report.get("losing_trades", 0) or 0)
    pnl_emoji = _E if total_pnl >= 0 else _E2

    msg = f"""📝 <b>宙斯交易中枢 | 每日复盘</b>

<b>统计日</b>  <code>{report_date} UTC</code>
<b>北京时间</b>  <code>{_escape(utc8_window)}</code>

<b>已平仓</b>  <code>{closed_trades}</code>  |  <b>{'交易所净盈亏' if pnl_source == 'exchange_income' else '盈亏'}</b>  <code>{total_pnl:+,.2f} USDT</code> {pnl_emoji}
<b>胜率</b>  <code>{win_rate:.1f}%</code>  |  <b>胜/负</b>  <code>{winning}</code>/<code>{losing}</code>
<b>平均</b>  <code>{avg_pnl:+,.2f} USDT</code>"""
    if pnl_source == "exchange_income":
        msg += (
            f"\n<b>收入流水</b>  实现 <code>{float(income_summary.get('realized_pnl', 0) or 0):+,.4f}</code>"
            f" | 手续费 <code>{float(income_summary.get('commission', 0) or 0):+,.4f}</code>"
            f" | 资金费 <code>{float(income_summary.get('funding_fee', 0) or 0):+,.4f}</code>"
        )
    if db_total_pnl is not None:
        try:
            db_value = float(db_total_pnl or 0)
            if abs(db_value - total_pnl) >= 0.005:
                msg += f"\n<b>本地平仓明细</b>  <code>{db_value:+,.4f} USDT</code>"
        except Exception:
            pass
    source_rows = int(report.get("source_rows", closed_trades) or closed_trades)
    split_rows = int(report.get("split_rows", 0) or 0)
    if source_rows != closed_trades or split_rows > 0:
        msg += f"\n<b>统计口径</b>  <code>{source_rows} 行平仓记录 → {closed_trades} 笔完整交易</code>"

    if closed_trades <= 0:
        return msg + "\n\n📭 该 UTC 统计日无已平仓交易，系统继续观察中。"

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
    pnl_source = str(report.get("pnl_source", "") or "")
    db_total_pnl = report.get("db_total_pnl")
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
            f"{'交易所净盈亏' if pnl_source == 'exchange_income' else '总盈亏'}  <code>{total_pnl:+,.2f} USDT</code> {pnl_emoji}",
            f"胜率  <code>{float(report.get('win_rate', 0) or 0):.1f}%</code>  |  笔均  <code>{float(report.get('avg_pnl', 0) or 0):+,.2f}</code>",
            f"盈亏比  <code>{_format_ratio_value(report.get('payoff_ratio', 0))}</code>  |  收益因子  <code>{_format_ratio_value(report.get('profit_factor', 0))}</code>",
        ]
    )
    if db_total_pnl is not None:
        try:
            db_value = float(db_total_pnl or 0)
            if abs(db_value - total_pnl) >= 0.005:
                lines.append(f"本地平仓明细  <code>{db_value:+,.2f} USDT</code>")
        except Exception:
            pass
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
    now_text = report_clock_label()
    msg = f"""📈 <b>宙斯交易中枢 | 周期复盘</b>

<code>{now_text}</code>
<b>统计口径</b>  <code>Binance UTC 自然日；UTC+8 仅作北京时间对照</code>"""

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

