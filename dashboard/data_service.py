# -*- coding: utf-8 -*-
"""Dashboard data aggregation service.

Keeps Binance/SQLite/log aggregation separate from HTTP routing and static UI.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any

from binance_api_client import get_native_binance_client, is_native_binance_configured
from hermes_paths import hermes_logs_dir
from repositories.trade_repository import TradeDatabase
from services.report_service import ReportService
from services.time_basis import report_clock_label, utc_today_key

logger = logging.getLogger("dashboard")


REALTIME_TTL_SEC = 5.0
ORDERS_TTL_SEC = 12.0
STATS_TTL_SEC = 60.0
LOG_TTL_SEC = 5.0
ERROR_TTL_SEC = 8.0

LOG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \| (?P<level>[A-Z]+) \| (?P<msg>.*)$")
SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,24}USDT)\b")
SESSION_RE = re.compile(r"\b([A-Z0-9]{2,24}USDT-\d{14}-[a-f0-9]{8})\b")


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


def _side_label(side: Any) -> str:
    key = str(side or "").upper()
    if key in {"LONG", "BUY"}:
        return "做多"
    if key in {"SHORT", "SELL"}:
        return "做空"
    return key or "未知"


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _error_type_from_message(message: str) -> str:
    text = str(message or "")
    rules = [
        ("开仓保护单失败", "开仓保护单失败"),
        ("entry protection", "开仓保护单失败"),
        ("开仓流程异常", "开仓流程异常"),
        ("开仓下单失败", "开仓下单失败"),
        ("开仓失败", "开仓失败"),
        ("平仓失败", "平仓失败"),
        ("止损单撤销失败", "止损单撤销失败"),
        ("保护止损补挂失败", "保护止损补挂失败"),
        ("保护止盈补挂失败", "保护止盈补挂失败"),
        ("保护单修复失败", "保护单修复失败"),
        ("Main loop exception", "主循环异常"),
        ("Binance API HTTP", "Binance接口异常"),
        ("Connection reset", "网络连接异常"),
        ("Telegram", "通知异常"),
    ]
    for token, label in rules:
        if token in text:
            return label
    if "失败" in text:
        return "交易流程失败"
    if "异常" in text or "exception" in text.lower():
        return "系统异常"
    return "运行异常"


def _component_from_message(message: str) -> str:
    text = str(message or "")
    rules = [
        ("execute_entry_trade", "开仓下单"),
        ("execute_entry", "开仓流程"),
        ("execute_exit", "平仓流程"),
        ("entry_protection", "开仓保护单"),
        ("protection_reconcile", "保护单补挂"),
        ("protection_guard", "保护单守卫"),
        ("stop_loss_cleanup", "止损清理"),
        ("breakeven_stop", "保本止损"),
        ("risk_assessment", "风控评估"),
        ("account_query", "账户查询"),
        ("Main loop", "主循环"),
        ("Telegram", "Telegram通知"),
    ]
    for token, label in rules:
        if token in text:
            return label
    if "保护" in text:
        return "保护单"
    if "开仓" in text:
        return "开仓流程"
    if "平仓" in text:
        return "平仓流程"
    return "系统"


def _is_transaction_error_line(level: str, message: str) -> bool:
    text = str(message or "")
    if level == "ERROR":
        return True
    tokens = (
        "❌",
        "开仓失败",
        "开仓流程异常",
        "开仓保护单失败",
        "平仓失败",
        "保护止损补挂失败",
        "保护止盈补挂失败",
        "保护单修复失败",
        "止损单撤销失败",
        "Main loop exception",
        "Binance API HTTP",
    )
    return any(token in text for token in tokens)


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
        self.reports = ReportService(self.db)
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
                bucket = by_symbol.setdefault(symbol, {"total": 0, "stop": 0, "take_profit": 0, "stop_prices": [], "take_profit_prices": []})
                bucket["total"] += 1
                kind = str(order.get("type", "")).upper()
                trigger_price = _safe_float(order.get("trigger_price"))
                if "TAKE_PROFIT" in kind:
                    bucket["take_profit"] += 1
                    if trigger_price > 0:
                        bucket["take_profit_prices"].append(trigger_price)
                elif "STOP" in kind:
                    bucket["stop"] += 1
                    if trigger_price > 0:
                        bucket["stop_prices"].append(trigger_price)
            return {"available": True, "orders": orders, "by_symbol": by_symbol}

        try:
            return self.cache.get("orders", ORDERS_TTL_SEC, _load)
        except Exception as exc:
            logger.warning("order snapshot failed: %s", exc)
            return {"available": False, "reason": str(exc), "orders": [], "by_symbol": {}}

    def today_income(self) -> dict[str, Any]:
        key = f"income_today:{utc_today_key()}"
        report = self.today_report()
        return report.get("exchange_income", {})

    def period_income(self, days: int) -> dict[str, Any]:
        report = self.period_report(days)
        return report.get("exchange_income", {})

    def period_report(self, days: int) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            return self.reports.period_report(days=days, mode="live")

        return self.cache.get(f"period:{days}", STATS_TTL_SEC, _load)

    def today_report(self) -> dict[str, Any]:
        def _load() -> dict[str, Any]:
            return self.reports.daily_report(utc_today_key(), mode="live")

        return self.cache.get(f"today:{utc_today_key()}", STATS_TTL_SEC, _load)

    def recent_trades(self, days: int = 30, limit: int = 80, offset: int = 0) -> list[dict[str, Any]]:
        return self.reports.recent_trades(days=days, limit=limit, offset=offset, mode="live")

    def recent_trades_page(self, days: int = 30, page: int = 1, per_page: int = 15) -> dict[str, Any]:
        page = max(1, _safe_int(page, 1))
        per_page = max(1, min(_safe_int(per_page, 15), 100))
        total = self.reports.recent_trades_count(days=days, mode="live")
        offset = (page - 1) * per_page
        rows = self.recent_trades(days=days, limit=per_page, offset=offset)
        total_pages = max(1, (total + per_page - 1) // per_page)
        return {
            "days": days,
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages,
            "trades": rows,
        }

    def open_db_trades(self) -> list[dict[str, Any]]:
        return self.reports.open_db_trades(mode="live")

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

    def transaction_errors(
        self,
        limit: int = 15,
        scan_lines: int = 5000,
        page: int = 1,
        per_page: int | None = None,
    ) -> dict[str, Any]:
        """Return DB-backed trading exceptions with recent log fallback."""
        if per_page is not None:
            limit = per_page
        page = max(1, _safe_int(page, 1))
        limit = max(1, min(_safe_int(limit, 15), 300))
        scan_lines = max(limit, min(_safe_int(scan_lines, 5000), 20000))
        offset = (page - 1) * limit

        db_rows = self.db.get_transaction_errors(limit=1000, offset=0)
        db_errors = [
            {
                "time": row.get("event_time", ""),
                "level": "ERROR",
                "type": row.get("error_type", "") or "交易异常",
                "component": row.get("component", "") or "系统",
                "symbol": row.get("symbol", "") or "",
                "session_id": row.get("session_id", "") or "",
                "summary": row.get("summary", "") or "",
                "detail": row.get("detail", "") or row.get("raw_text", "") or "",
                "source": row.get("source", "db"),
            }
            for row in db_rows
        ]

        def _load_log_events() -> list[dict[str, Any]]:
            path = hermes_logs_dir() / "crypto_sword.log"
            if not path.exists():
                return []
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-scan_lines:]
            except Exception:
                return []

            events: list[dict[str, Any]] = []
            index = 0
            while index < len(lines):
                line = lines[index]
                match = LOG_LINE_RE.match(line)
                if not match:
                    index += 1
                    continue
                level = match.group("level")
                message = match.group("msg")
                if not _is_transaction_error_line(level, message):
                    index += 1
                    continue

                detail_lines = [message]
                cursor = index + 1
                while cursor < len(lines) and not LOG_LINE_RE.match(lines[cursor]):
                    extra = lines[cursor].rstrip()
                    if extra:
                        detail_lines.append(extra)
                    cursor += 1

                detail = "\n".join(detail_lines)[:3000]
                symbol_match = SYMBOL_RE.search(detail)
                session_match = SESSION_RE.search(detail)
                events.append(
                    {
                        "time": match.group("ts"),
                        "level": level,
                        "type": _error_type_from_message(detail),
                        "component": _component_from_message(detail),
                        "symbol": symbol_match.group(1) if symbol_match else "",
                        "session_id": session_match.group(1) if session_match else "",
                        "summary": message[:260],
                        "detail": detail,
                        "source": "log",
                    }
                )
                index = max(cursor, index + 1)

            events.reverse()
            return events

        log_errors = self.cache.get(f"errors-log:{scan_lines}", ERROR_TTL_SEC, _load_log_events)
        combined: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str, str, str]] = set()
        for event in [*db_errors, *log_errors]:
            key = (
                str(event.get("time", "")),
                str(event.get("type", "")),
                str(event.get("component", "")),
                str(event.get("symbol", "")),
                str(event.get("session_id", "")),
                str(event.get("summary", ""))[:160],
            )
            if key in seen:
                continue
            seen.add(key)
            combined.append(event)

        combined.sort(key=lambda item: str(item.get("time", "")), reverse=True)
        total = len(combined)
        total_pages = max(1, (total + limit - 1) // limit)
        return {
            "available": True,
            "source": "db+log" if db_errors and log_errors else ("db" if db_errors else "log"),
            "total": total,
            "page": page,
            "per_page": limit,
            "total_pages": total_pages,
            "errors": combined[offset : offset + limit],
        }

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
            merged["expected_pnl"] = self._estimate_position_expected_pnl(pos, protection)
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
            "recent_trades": self.recent_trades_page(days=30, page=1, per_page=15),
            "open_db_trades": self.open_db_trades(),
            "transaction_errors": self.transaction_errors(page=1, per_page=15),
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

    def _estimate_position_expected_pnl(self, pos: dict[str, Any], protection: dict[str, Any]) -> dict[str, Any]:
        entry = _safe_float(pos.get("entry_price"))
        quantity = _safe_float(pos.get("quantity"))
        side = str(pos.get("side", "") or "").upper()
        if entry <= 0 or quantity <= 0:
            return {"available": False, "stop_price": 0.0, "stop_pnl": 0.0, "take_profit_prices": [], "take_profit_pnl": 0.0}

        stop_prices = sorted([_safe_float(item) for item in protection.get("stop_prices", []) if _safe_float(item) > 0])
        tp_prices = sorted([_safe_float(item) for item in protection.get("take_profit_prices", []) if _safe_float(item) > 0])
        if side == "SHORT":
            stop_prices = sorted(stop_prices, reverse=True)
            tp_prices = sorted(tp_prices, reverse=True)

        def pnl_at(price_value: float, qty: float = quantity) -> float:
            if side == "SHORT":
                return (entry - price_value) * qty
            return (price_value - entry) * qty

        stop_price = stop_prices[0] if stop_prices else 0.0
        stop_pnl = pnl_at(stop_price) if stop_price > 0 else 0.0
        tp_pnl = 0.0
        if tp_prices:
            # Open orders do not always expose each slice quantity consistently; use
            # an equal-weight estimate so the dashboard stays directionally useful.
            slice_qty = quantity / len(tp_prices)
            tp_pnl = sum(pnl_at(price_value, slice_qty) for price_value in tp_prices)

        return {
            "available": bool(stop_price or tp_prices),
            "stop_price": round(stop_price, 10),
            "stop_pnl": round(stop_pnl, 8),
            "take_profit_prices": [round(item, 10) for item in tp_prices],
            "take_profit_pnl": round(tp_pnl, 8),
        }
