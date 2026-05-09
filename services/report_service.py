# -*- coding: utf-8 -*-
"""Unified report aggregation for Telegram and dashboard consumers."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from feature_store import feature_store
from repositories.trade_repository import TradeDatabase, TradeRecord
from services.accounting_service import fetch_income_rows, summarize_income_rows
from services.time_basis import (
    parse_db_datetime,
    utc_cutoff_for_days,
    utc_day_window,
    utc_now,
    utc_today_key,
    utc8_window_label,
)

logger = logging.getLogger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _reason_label(reason: Any) -> str:
    key = str(reason or "UNKNOWN").upper()
    mapping = {
        "PROTECTIVE_STOP_EXCHANGE": "防守止损盈利离场",
        "PROTECTIVE_STOP": "防守止损盈利离场",
        "TAKE_PROFIT_TP_FULL_EXCHANGE": "TP1/TP2/TP3 全部成交",
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
        "EARLY_PROFIT_EXCHANGE": "提前微利退出",
        "EARLY_PROFIT": "提前微利退出",
        "EXCHANGE_REALIZED_EXCHANGE": "交易所已实现盈亏同步",
        "EXCHANGE_REALIZED": "交易所已实现盈亏同步",
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
    return key or "未知原因"


def _side_label(side: Any) -> str:
    key = str(side or "").upper()
    if key in {"LONG", "BUY"}:
        return "做多"
    if key in {"SHORT", "SELL"}:
        return "做空"
    return key or "未知"


def _iso_or_empty(value: Any) -> str:
    parsed = parse_db_datetime(value)
    if not parsed:
        return str(value or "")
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def _hold_minutes(entry_time: Any, exit_time: Any) -> float:
    start = parse_db_datetime(entry_time)
    end = parse_db_datetime(exit_time)
    if not start or not end:
        return 0.0
    return max(0.0, (end - start).total_seconds() / 60.0)


class ReportService:
    """One reporting source for TG, dashboard and operational gates."""

    def __init__(self, db: TradeDatabase | None = None):
        self.db = db or TradeDatabase()

    def daily_report(self, report_date: str | None = None, mode: str = "live") -> dict[str, Any]:
        date_key = report_date or utc_today_key()
        report: dict[str, Any] = {
            "date": date_key,
            "time_basis": "Binance UTC",
            "utc8_window": utc8_window_label(date_key),
            "mode": mode,
            "closed_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": 0.0,
            "max_loss": 0.0,
            "best_trade": None,
            "worst_trade": None,
            "reason_counts": {},
        }
        try:
            db_report = self.db.get_daily_report(date_key, mode=mode)
            if isinstance(db_report, dict):
                report.update(db_report)
                report["date"] = date_key
                report["time_basis"] = "Binance UTC"
                report["utc8_window"] = utc8_window_label(date_key)
                report["mode"] = mode
        except Exception as exc:
            logger.warning("daily report DB build failed [%s]: %s", date_key, exc)

        try:
            report["entry_protection"] = feature_store.summarize_entry_protection(date_key, tz_offset_hours=0)
        except Exception as exc:
            logger.debug("entry protection summary skipped [%s]: %s", date_key, exc)
            report.setdefault("entry_protection", {})

        start, end = utc_day_window(date_key)
        self._merge_exchange_income(report, self._income_summary_for_window(start, end))
        return report

    def period_report(self, days: int = 7, mode: str = "live") -> dict[str, Any]:
        try:
            report = self.db.get_period_report(days=days, mode=mode)
        except Exception as exc:
            logger.warning("period report DB build failed [%sd]: %s", days, exc)
            report = {
                "period_days": days,
                "label": f"近{days}天(UTC)",
                "time_basis": "Binance UTC",
                "mode": mode,
                "closed_trades": 0,
                "total_pnl": 0.0,
                "reason_counts": {},
            }
        self._merge_exchange_income(report, self._income_summary_for_window(utc_cutoff_for_days(days), utc_now()))
        return report

    def period_reports(self, days_list: tuple[int, ...] = (7, 30), mode: str = "live") -> list[dict[str, Any]]:
        return [self.period_report(days=days, mode=mode) for days in days_list]

    def recent_trades(
        self,
        days: int = 30,
        limit: int = 80,
        mode: str = "live",
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        trades = self.db.get_closed_trades(days=days, mode=mode)
        sessions = self.db._aggregate_closed_trade_sessions(trades)
        sessions.sort(
            key=lambda item: (parse_db_datetime(item.get("exit_time")) or datetime.fromtimestamp(0)).timestamp(),
            reverse=True,
        )
        result = []
        offset = max(0, int(offset or 0))
        limit = max(1, min(int(limit or 80), 100000))
        for item in sessions[offset : offset + limit]:
            pnl = _safe_float(item.get("pnl"))
            income = self._income_for_trade_session(item)
            result.append(
                {
                    "session_id": item.get("session_id", ""),
                    "symbol": item.get("symbol", ""),
                    "side": item.get("side", ""),
                    "side_label": _side_label(item.get("side")),
                    "strategy_line": item.get("strategy_line", "") or "UNKNOWN",
                    "stage": item.get("stage", "") or "UNKNOWN",
                    "entry_price": _safe_float(item.get("entry_price")),
                    "exit_price": _safe_float(item.get("exit_price")),
                    "quantity": _safe_float(item.get("quantity")),
                    "entry_time": _iso_or_empty(item.get("entry_time")),
                    "exit_time": _iso_or_empty(item.get("exit_time")),
                    "hold_minutes": round(_hold_minutes(item.get("entry_time"), item.get("exit_time")), 1),
                    "exit_reason": item.get("exit_reason", "") or "UNKNOWN",
                    "exit_reason_label": _reason_label(item.get("exit_reason")),
                    "pnl": round(pnl, 8),
                    "commission": round(_safe_float(income.get("commission")), 8),
                    "fee": round(abs(_safe_float(income.get("commission"))), 8),
                    "funding_fee": round(_safe_float(income.get("funding_fee")), 8),
                    "exchange_net_pnl": round(_safe_float(income.get("net_pnl")), 8),
                    "pnl_pct": round(_safe_float(item.get("pnl_pct")), 4),
                    "rows": _safe_int(item.get("rows"), 1),
                    "result": "win" if pnl > 0 else ("loss" if pnl < 0 else "flat"),
                }
            )
        return result

    def recent_trades_count(self, days: int = 30, mode: str = "live") -> int:
        trades = self.db.get_closed_trades(days=days, mode=mode)
        return len(self.db._aggregate_closed_trade_sessions(trades))

    def open_db_trades(self, mode: str = "live") -> list[dict[str, Any]]:
        result = []
        for trade in self.db.get_open_trades(mode=mode):
            result.append(
                {
                    "id": trade.id,
                    "symbol": trade.symbol,
                    "side": trade.side,
                    "side_label": _side_label(trade.side),
                    "entry_price": trade.entry_price,
                    "quantity": trade.quantity,
                    "leverage": trade.leverage,
                    "stop_loss": trade.stop_loss,
                    "take_profit": trade.take_profit,
                    "entry_time": _iso_or_empty(trade.entry_time),
                    "stage": trade.stage,
                }
            )
        return result

    def _income_summary_for_window(self, start: datetime, end: datetime) -> dict[str, Any]:
        """Fetch Binance income first, persist it, then report from local DB."""
        source = "db"
        fetch_error = ""
        try:
            rows = fetch_income_rows(start, end)
            if rows:
                self.db.upsert_exchange_income_rows(rows)
                source = "binance_synced_db"
        except Exception as exc:
            fetch_error = str(exc)
            logger.warning("Binance income sync failed; using local DB cache: %s", exc)

        db_rows = self.db.get_exchange_income_rows(start, end)
        summary = summarize_income_rows(db_rows)
        summary["source"] = source
        summary["available"] = bool(db_rows)
        summary["start_utc"] = start.isoformat().replace("+00:00", "Z")
        summary["end_utc"] = end.isoformat().replace("+00:00", "Z")
        summary["cache_bounds"] = self.db.get_exchange_income_bounds()
        if fetch_error:
            summary["sync_error"] = fetch_error
        if not db_rows and fetch_error:
            summary["reason"] = fetch_error
        return summary

    def _income_for_trade_session(self, item: dict[str, Any]) -> dict[str, Any]:
        start = parse_db_datetime(item.get("entry_time"))
        end = parse_db_datetime(item.get("exit_time"))
        symbol = str(item.get("symbol", "") or "").upper()
        if not start or not end or not symbol:
            return {"net_pnl": 0.0, "commission": 0.0, "funding_fee": 0.0, "rows": 0}
        rows = self.db.get_exchange_income_rows(start, end, symbol=symbol)
        return summarize_income_rows(rows)

    def _merge_exchange_income(self, report: dict[str, Any], income_summary: dict[str, Any]) -> None:
        db_total_pnl = _safe_float(report.get("total_pnl"))
        report["exchange_income"] = income_summary
        report["income_summary"] = income_summary
        report["db_total_pnl"] = db_total_pnl
        if not income_summary.get("available"):
            report["exchange_net_pnl"] = None
            report["exchange_total_pnl"] = None
            report["pnl_source"] = "db"
            report["pnl_diff_vs_db"] = None
            return
        exchange_net_pnl = _safe_float(income_summary.get("net_pnl"))
        report["exchange_net_pnl"] = exchange_net_pnl
        report["db_total_pnl"] = db_total_pnl
        report["exchange_total_pnl"] = exchange_net_pnl
        report["total_pnl"] = exchange_net_pnl
        closed_trades = _safe_int(report.get("closed_trades"))
        if closed_trades > 0:
            report["avg_pnl"] = round(exchange_net_pnl / closed_trades, 4)
        report["pnl_source"] = "exchange_income"
        report["pnl_diff_vs_db"] = round(exchange_net_pnl - db_total_pnl, 8)
