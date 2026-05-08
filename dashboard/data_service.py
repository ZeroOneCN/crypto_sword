# -*- coding: utf-8 -*-
"""Dashboard data aggregation service.

Keeps Binance/SQLite/log aggregation separate from HTTP routing and static UI.
"""

from __future__ import annotations

import logging
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
            "recent_trades": self.recent_trades_page(days=30, page=1, per_page=15),
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
