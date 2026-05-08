# -*- coding: utf-8 -*-
"""Dashboard data aggregation service.

Keeps Binance/SQLite/log aggregation separate from HTTP routing and static UI.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any

from binance_api_client import get_native_binance_client, is_native_binance_configured
from hermes_paths import hermes_logs_dir
from services.accounting_service import fetch_daily_income_summary, fetch_period_income_summary
from services.time_basis import parse_db_datetime, report_clock_label, utc_today_key, utc8_window_label
from trade_logger import TradeDatabase

logger = logging.getLogger("dashboard")

from binance_api_client import get_native_binance_client, is_native_binance_configured
from hermes_paths import hermes_logs_dir
from services.accounting_service import fetch_daily_income_summary, fetch_period_income_summary
from services.time_basis import parse_db_datetime, report_clock_label, utc_today_key, utc8_window_label
from trade_logger import TradeDatabase

logger = logging.getLogger("dashboard")


REALTIME_TTL_SEC = 5.0
ORDERS_TTL_SEC = 12.0
STATS_TTL_SEC = 60.0
LOG_TTL_SEC = 5.0


REASON_LABELS = {
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
    if key in REASON_LABELS:
        return REASON_LABELS[key]
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


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class TimedCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._items: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, ttl: float, factory):
        now = time.time()
        with self._lock:
            item = self._items.get(key)
            if item and now - item[0] <= ttl:
                return item[1]
        value = factory()
        with self._lock:
            self._items[key] = (now, value)
        return value


class DashboardData:
    def __init__(self):
        self.db = TradeDatabase()
        self.cache = TimedCache()

    def account_snapshot(self) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            if not is_native_binance_configured():
                return {"available": False, "reason": "Binance API 未配置"}
            client = get_native_binance_client()
            account = client.account_information()  # type: ignore[union-attr]
            positions = self._parse_positions(account.get("positions") or [])
            return {
                "available": True,
                "total_wallet_balance": _safe_float(account.get("totalWalletBalance")),
                "available_balance": _safe_float(account.get("availableBalance")),
                "total_margin_balance": _safe_float(account.get("totalMarginBalance")),
                "total_unrealized_pnl": _safe_float(account.get("totalUnrealizedProfit")),
                "positions": positions,
                "raw_update_time": account.get("updateTime"),
            }

        try:
            return self.cache.get("account", REALTIME_TTL_SEC, _load)
        except Exception as exc:
            logger.warning("account snapshot failed: %s", exc)
            return {"available": False, "reason": str(exc), "positions": []}

    def order_snapshot(self) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            if not is_native_binance_configured():
                return {"available": False, "orders": [], "by_symbol": {}}
            client = get_native_binance_client()
            normal_orders = client.open_orders()  # type: ignore[union-attr]
            algo_orders = client.open_algo_orders()  # type: ignore[union-attr]
            orders = []
            for item in list(normal_orders or []) + list(algo_orders or []):
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "") or "")
                order_type = str(item.get("type", item.get("orderType", "")) or "")
                side = str(item.get("side", "") or "")
                orders.append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "type": order_type,
                        "status": str(item.get("status", "") or ""),
                        "price": _safe_float(item.get("price")),
                        "trigger_price": _safe_float(item.get("triggerPrice", item.get("stopPrice"))),
                        "quantity": _safe_float(item.get("origQty", item.get("quantity"))),
                        "order_id": item.get("orderId", item.get("algoId")),
                    }
                )
            by_symbol: dict[str, dict[str, Any]] = {}
            for order in orders:
                symbol = order["symbol"]
                if not symbol:
                    continue
                bucket = by_symbol.setdefault(symbol, {"total": 0, "stop": 0, "take_profit": 0})
                bucket["total"] += 1
                kind = str(order.get("type", "")).upper()
                if "TAKE_PROFIT" in kind:
                    bucket["take_profit"] += 1
                elif "STOP" in kind:
                    bucket["stop"] += 1
            return {"available": True, "orders": orders, "by_symbol": by_symbol}

        try:
            return self.cache.get("orders", ORDERS_TTL_SEC, _load)
        except Exception as exc:
            logger.warning("order snapshot failed: %s", exc)
            return {"available": False, "reason": str(exc), "orders": [], "by_symbol": {}}

    def today_income(self) -> dict[str, Any]:
        key = f"income_today:{utc_today_key()}"
        return self.cache.get(key, STATS_TTL_SEC, lambda: fetch_daily_income_summary(utc_today_key()))

    def period_income(self, days: int) -> dict[str, Any]:
        return self.cache.get(f"income:{days}", STATS_TTL_SEC, lambda: fetch_period_income_summary(days))

    def period_report(self, days: int) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            report = self.db.get_period_report(days=days, mode="live")
            income = self.period_income(days)
            report["exchange_income"] = income
            report["exchange_net_pnl"] = _safe_float(income.get("net_pnl"))
            return report

        return self.cache.get(f"period:{days}", STATS_TTL_SEC, _load)

    def today_report(self) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            date_key = utc_today_key()
            report = self.db.get_daily_report(date_key, mode="live")
            income = self.today_income()
            report["date"] = date_key
            report["utc8_window"] = utc8_window_label(date_key)
            report["exchange_income"] = income
            report["exchange_net_pnl"] = _safe_float(income.get("net_pnl"))
            return report

        return self.cache.get(f"today:{utc_today_key()}", STATS_TTL_SEC, _load)

    def recent_trades(self, days: int = 30, limit: int = 80) -> list[dict[str, Any]]:
        trades = self.db.get_closed_trades(days=days, mode="live")
        sessions = self.db._aggregate_closed_trade_sessions(trades)
        sessions.sort(
            key=lambda item: (parse_db_datetime(item.get("exit_time")) or datetime.fromtimestamp(0)).timestamp(),
            reverse=True,
        )
        result = []
        for item in sessions[: max(1, min(limit, 500))]:
            pnl = _safe_float(item.get("pnl"))
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
                    "pnl_pct": round(_safe_float(item.get("pnl_pct")), 4),
                    "rows": _safe_int(item.get("rows"), 1),
                    "result": "win" if pnl > 0 else ("loss" if pnl < 0 else "flat"),
                }
            )
        return result

    def open_db_trades(self) -> list[dict[str, Any]]:
        result = []
        for trade in self.db.get_open_trades(mode="live"):
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

    def log_tail(self, lines: int = 50) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            path = hermes_logs_dir() / "crypto_sword.log"
            if not path.exists():
                return {"available": False, "path": str(path), "lines": []}
            try:
                data = path.read_text(encoding="utf-8", errors="replace").splitlines()
            except Exception as exc:
                return {"available": False, "path": str(path), "reason": str(exc), "lines": []}
            return {"available": True, "path": str(path), "lines": data[-max(1, min(lines, 300)):]}

        return self.cache.get(f"log:{lines}", LOG_TTL_SEC, _load)

    def overview(self) -> dict[str, Any]:
        account = self.account_snapshot()
        orders = self.order_snapshot()
        today = self.today_report()
        period7 = self.period_report(7)
        period30 = self.period_report(30)

        by_symbol = orders.get("by_symbol", {}) if isinstance(orders, dict) else {}
        positions = []
        for pos in account.get("positions", []) or []:
            symbol = pos.get("symbol", "")
            protection = by_symbol.get(symbol, {}) if isinstance(by_symbol, dict) else {}
            merged = dict(pos)
            merged["protection"] = protection
            merged["protected"] = bool(protection.get("stop", 0) >= 1 and protection.get("take_profit", 0) >= 1)
            positions.append(merged)

        return {
            "ok": True,
            "clock": report_clock_label(),
            "time_basis": "Binance UTC 自然日，页面同时显示 UTC+8 辅助窗口",
            "account": {**account, "positions": positions},
            "orders": {
                "available": orders.get("available", False),
                "total": len(orders.get("orders", []) or []),
                "by_symbol": by_symbol,
                "reason": orders.get("reason", ""),
            },
            "today": today,
            "periods": {"7": period7, "30": period30},
            "recent_trades": self.recent_trades(days=30, limit=80),
            "open_db_trades": self.open_db_trades(),
            "log_tail": self.log_tail(60),
        }

    def _parse_positions(self, raw_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        positions = []
        for pos in raw_positions:
            amount = _safe_float(pos.get("positionAmt"))
            if abs(amount) <= 0:
                continue
            symbol = str(pos.get("symbol", "") or "")
            side = str(pos.get("positionSide", "") or "")
            if not side or side == "BOTH":
                side = "LONG" if amount > 0 else "SHORT"
            entry_price = _safe_float(pos.get("entryPrice"))
            break_even_price = _safe_float(pos.get("breakEvenPrice"))
            mark_price = _safe_float(pos.get("markPrice"))
            unrealized = _safe_float(pos.get("unRealizedProfit", pos.get("unrealizedProfit")))
            notional = abs(_safe_float(pos.get("notional")))
            if notional <= 0 and mark_price > 0:
                notional = abs(amount) * mark_price
            leverage = max(_safe_int(pos.get("leverage"), 1), 1)
            price_move_pct = (unrealized / notional * 100.0) if notional > 0 else 0.0
            roi_pct = price_move_pct * leverage
            positions.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "side_label": _side_label(side),
                    "quantity": abs(amount),
                    "entry_price": entry_price,
                    "break_even_price": break_even_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": unrealized,
                    "notional": notional,
                    "leverage": leverage,
                    "price_move_pct": round(price_move_pct, 4),
                    "roi_pct": round(roi_pct, 4),
                    "liquidation_price": _safe_float(pos.get("liquidationPrice")),
                }
            )
        positions.sort(key=lambda item: abs(item.get("unrealized_pnl", 0.0)), reverse=True)
        return positions
