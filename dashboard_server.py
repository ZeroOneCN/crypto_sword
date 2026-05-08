#!/usr/bin/env python3
"""Lightweight read-only web dashboard for Hermes Trader.

Run on the server:
    python3 dashboard_server.py --host 127.0.0.1 --port 8787

Then open through SSH tunnel from your local machine:
    ssh -L 8787:127.0.0.1:8787 root@SERVER_IP
    http://127.0.0.1:8787
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import threading
import time
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

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


DATA = DashboardData()


HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>宙斯交易中枢 | 数据看板</title>
  <style>
    :root {
      --bg: #0b111b;
      --panel: rgba(20, 31, 48, 0.88);
      --panel-2: rgba(13, 21, 34, 0.92);
      --line: rgba(148, 163, 184, 0.18);
      --text: #e5eefc;
      --muted: #91a0b8;
      --good: #26d695;
      --bad: #ff5c7a;
      --warn: #ffd166;
      --blue: #70b7ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at 15% 10%, rgba(78, 141, 255, 0.20), transparent 30%),
        radial-gradient(circle at 80% 0%, rgba(38, 214, 149, 0.12), transparent 28%),
        linear-gradient(145deg, #080c13 0%, #10192a 55%, #090f18 100%);
      font-family: "Microsoft YaHei", "PingFang SC", "Segoe UI", sans-serif;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 5;
      padding: 18px clamp(14px, 3vw, 34px);
      backdrop-filter: blur(18px);
      background: rgba(8, 12, 19, 0.72);
      border-bottom: 1px solid var(--line);
    }
    .title-row {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      flex-wrap: wrap;
    }
    h1 { margin: 0; font-size: clamp(22px, 4vw, 34px); letter-spacing: 0.04em; }
    .sub { color: var(--muted); font-size: 13px; margin-top: 6px; }
    .pill {
      border: 1px solid var(--line);
      color: var(--blue);
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(112, 183, 255, 0.08);
      font-size: 13px;
      white-space: nowrap;
    }
    main { padding: 22px clamp(14px, 3vw, 34px) 40px; }
    .grid { display: grid; gap: 14px; }
    .cards { grid-template-columns: repeat(6, minmax(140px, 1fr)); }
    .two { grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr); }
    .three { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .panel, .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.25);
    }
    .card { padding: 16px; min-height: 105px; }
    .label { color: var(--muted); font-size: 13px; }
    .value { font-size: clamp(20px, 3vw, 30px); font-weight: 800; margin-top: 8px; }
    .hint { margin-top: 8px; color: var(--muted); font-size: 12px; }
    .panel { padding: 18px; margin-top: 14px; overflow: hidden; }
    .panel h2 { margin: 0 0 14px; font-size: 18px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 11px 10px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }
    th { color: var(--muted); font-weight: 600; background: rgba(255,255,255,0.02); }
    tr:hover td { background: rgba(255,255,255,0.025); }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .muted { color: var(--muted); }
    .scroll { overflow-x: auto; }
    .status-dot {
      display: inline-block; width: 8px; height: 8px; border-radius: 50%;
      margin-right: 7px; background: var(--good); box-shadow: 0 0 18px var(--good);
    }
    .log {
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      max-height: 360px;
      overflow: auto;
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 12px;
      line-height: 1.55;
      color: #b8c7de;
      white-space: pre-wrap;
    }
    .toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    button, a.button {
      border: 1px solid var(--line);
      color: var(--text);
      background: rgba(255,255,255,0.06);
      padding: 9px 12px;
      border-radius: 12px;
      cursor: pointer;
      text-decoration: none;
      font: inherit;
    }
    button:hover, a.button:hover { background: rgba(112,183,255,0.15); }
    @media (max-width: 1100px) {
      .cards, .three { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .two { grid-template-columns: 1fr; }
    }
    @media (max-width: 620px) {
      .cards, .three { grid-template-columns: 1fr; }
      th, td { padding: 9px 8px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="title-row">
      <div>
        <h1>宙斯交易中枢 · 数据看板</h1>
        <div class="sub">只读自动刷新，不参与下单；统计口径以 Binance UTC 为准。</div>
      </div>
      <div class="toolbar">
        <span class="pill"><span class="status-dot"></span><span id="status">连接中</span></span>
        <span class="pill" id="clock">--</span>
        <button onclick="loadAll(true)">立即刷新</button>
        <a class="button" href="/api/export/trades.csv" target="_blank">下载CSV</a>
      </div>
    </div>
  </header>
  <main>
    <section class="grid cards">
      <div class="card"><div class="label">总余额</div><div class="value" id="totalBalance">--</div><div class="hint">Binance Futures</div></div>
      <div class="card"><div class="label">可用余额</div><div class="value" id="availableBalance">--</div><div class="hint">可开仓资金</div></div>
      <div class="card"><div class="label">未实现盈亏</div><div class="value" id="unrealized">--</div><div class="hint">当前持仓浮盈亏</div></div>
      <div class="card"><div class="label">今日已实现(UTC)</div><div class="value" id="todayPnl">--</div><div class="hint" id="todayWindow">--</div></div>
      <div class="card"><div class="label">近7天净盈亏</div><div class="value" id="pnl7">--</div><div class="hint" id="pf7">--</div></div>
      <div class="card"><div class="label">近30天净盈亏</div><div class="value" id="pnl30">--</div><div class="hint" id="pf30">--</div></div>
    </section>

    <section class="grid two">
      <div class="panel">
        <h2>当前持仓</h2>
        <div class="scroll"><table>
          <thead><tr><th>币种</th><th>方向</th><th>数量</th><th>入场</th><th>标记</th><th>未实现</th><th>ROI</th><th>保护单</th></tr></thead>
          <tbody id="positions"></tbody>
        </table></div>
      </div>
      <div class="panel">
        <h2>今日战况</h2>
        <div class="grid three">
          <div class="card"><div class="label">完整交易</div><div class="value" id="todayTrades">--</div><div class="hint">分批TP按1笔算</div></div>
          <div class="card"><div class="label">胜率</div><div class="value" id="todayWinRate">--</div><div class="hint">SQLite 聚合</div></div>
          <div class="card"><div class="label">盈亏比</div><div class="value" id="todayPayoff">--</div><div class="hint">均盈 / 均亏</div></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>周期复盘</h2>
      <div class="scroll"><table>
        <thead><tr><th>周期</th><th>完整交易</th><th>总盈亏(DB)</th><th>交易所净收入</th><th>胜率</th><th>笔均</th><th>盈亏比</th><th>收益因子</th><th>最佳</th><th>最差</th></tr></thead>
        <tbody id="periods"></tbody>
      </table></div>
    </section>

    <section class="panel">
      <h2>最近完整交易</h2>
      <div class="scroll"><table>
        <thead><tr><th>时间(UTC)</th><th>币种</th><th>方向</th><th>策略</th><th>入场</th><th>出场</th><th>盈亏</th><th>涨幅</th><th>原因</th><th>持仓</th></tr></thead>
        <tbody id="recentTrades"></tbody>
      </table></div>
    </section>

    <section class="panel">
      <h2>运行日志尾部</h2>
      <div class="log" id="logs">读取中...</div>
    </section>
  </main>

  <script>
    const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 6 });
    const money = v => `${Number(v || 0) >= 0 ? '+' : ''}${fmt.format(Number(v || 0))} USDT`;
    const plainMoney = v => `${fmt.format(Number(v || 0))} USDT`;
    const pct = v => `${Number(v || 0) >= 0 ? '+' : ''}${Number(v || 0).toFixed(2)}%`;
    const cls = v => Number(v || 0) > 0 ? 'good' : (Number(v || 0) < 0 ? 'bad' : 'muted');
    const cell = (v, c='') => `<td class="${c}">${v}</td>`;
    const price = v => Number(v || 0) ? Number(v).toPrecision(8).replace(/\.?0+$/, '') : '--';
    const hold = m => {
      m = Number(m || 0);
      if (m < 60) return `${Math.round(m)}分钟`;
      const h = Math.floor(m / 60), mm = Math.round(m % 60);
      return `${h}小时${mm}分钟`;
    };

    function setText(id, value, className='') {
      const el = document.getElementById(id);
      el.textContent = value;
      el.className = className ? `value ${className}` : 'value';
    }

    function renderPositions(rows) {
      const body = document.getElementById('positions');
      if (!rows || !rows.length) {
        body.innerHTML = `<tr><td colspan="8" class="muted">当前无持仓，系统待命。</td></tr>`;
        return;
      }
      body.innerHTML = rows.map(p => {
        const prot = p.protected ? '✅ 完整' : `⚠️ SL ${p.protection?.stop || 0} / TP ${p.protection?.take_profit || 0}`;
        return `<tr>
          ${cell(`<b>${p.symbol}</b>`)}
          ${cell(p.side_label)}
          ${cell(fmt.format(p.quantity))}
          ${cell(price(p.entry_price))}
          ${cell(price(p.mark_price))}
          ${cell(money(p.unrealized_pnl), cls(p.unrealized_pnl))}
          ${cell(pct(p.roi_pct), cls(p.roi_pct))}
          ${cell(prot, p.protected ? 'good' : 'warn')}
        </tr>`;
      }).join('');
    }

    function renderPeriods(periods) {
      const body = document.getElementById('periods');
      const rows = [['近7天', periods['7']], ['近30天', periods['30']]];
      body.innerHTML = rows.map(([label, r]) => {
        const best = r.best_trade ? `${r.best_trade.symbol} ${money(r.best_trade.pnl)}` : '--';
        const worst = r.worst_trade ? `${r.worst_trade.symbol} ${money(r.worst_trade.pnl)}` : '--';
        return `<tr>
          ${cell(label)}
          ${cell(r.closed_trades || 0)}
          ${cell(money(r.total_pnl), cls(r.total_pnl))}
          ${cell(money(r.exchange_net_pnl), cls(r.exchange_net_pnl))}
          ${cell(`${Number(r.win_rate || 0).toFixed(2)}%`)}
          ${cell(money(r.avg_pnl), cls(r.avg_pnl))}
          ${cell(Number(r.payoff_ratio || 0).toFixed(2))}
          ${cell(Number(r.profit_factor || 0).toFixed(2))}
          ${cell(best, 'good')}
          ${cell(worst, 'bad')}
        </tr>`;
      }).join('');
    }

    function renderTrades(rows) {
      const body = document.getElementById('recentTrades');
      if (!rows || !rows.length) {
        body.innerHTML = `<tr><td colspan="10" class="muted">暂无已平仓交易。</td></tr>`;
        return;
      }
      body.innerHTML = rows.slice(0, 80).map(t => `<tr>
        ${cell((t.exit_time || '').replace('T', ' ').replace('Z', ''))}
        ${cell(`<b>${t.symbol}</b>`)}
        ${cell(t.side_label)}
        ${cell(t.strategy_line || 'UNKNOWN')}
        ${cell(price(t.entry_price))}
        ${cell(price(t.exit_price))}
        ${cell(money(t.pnl), cls(t.pnl))}
        ${cell(pct(t.pnl_pct), cls(t.pnl_pct))}
        ${cell(t.exit_reason_label)}
        ${cell(hold(t.hold_minutes))}
      </tr>`).join('');
    }

    async function loadAll(manual=false) {
      try {
        const res = await fetch('/api/overview', { cache: 'no-store' });
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'unknown error');
        document.getElementById('status').textContent = manual ? '已手动刷新' : '自动刷新中';
        document.getElementById('clock').textContent = data.clock || '--';
        const account = data.account || {};
        const today = data.today || {};
        const periods = data.periods || {};
        setText('totalBalance', plainMoney(account.total_wallet_balance));
        setText('availableBalance', plainMoney(account.available_balance));
        setText('unrealized', money(account.total_unrealized_pnl), cls(account.total_unrealized_pnl));
        setText('todayPnl', money(today.exchange_net_pnl ?? today.total_pnl), cls(today.exchange_net_pnl ?? today.total_pnl));
        document.getElementById('todayWindow').textContent = today.utc8_window || data.time_basis || '';
        setText('pnl7', money(periods['7']?.exchange_net_pnl ?? periods['7']?.total_pnl), cls(periods['7']?.exchange_net_pnl ?? periods['7']?.total_pnl));
        document.getElementById('pf7').textContent = `PF ${Number(periods['7']?.profit_factor || 0).toFixed(2)} | 胜率 ${Number(periods['7']?.win_rate || 0).toFixed(1)}%`;
        setText('pnl30', money(periods['30']?.exchange_net_pnl ?? periods['30']?.total_pnl), cls(periods['30']?.exchange_net_pnl ?? periods['30']?.total_pnl));
        document.getElementById('pf30').textContent = `PF ${Number(periods['30']?.profit_factor || 0).toFixed(2)} | 胜率 ${Number(periods['30']?.win_rate || 0).toFixed(1)}%`;
        setText('todayTrades', today.closed_trades || 0);
        setText('todayWinRate', `${Number(today.win_rate || 0).toFixed(1)}%`);
        setText('todayPayoff', Number(today.payoff_ratio || 0).toFixed(2));
        renderPositions(account.positions || []);
        renderPeriods(periods);
        renderTrades(data.recent_trades || []);
        const log = data.log_tail || {};
        document.getElementById('logs').textContent = (log.lines || []).join('\n') || '暂无日志';
      } catch (err) {
        document.getElementById('status').textContent = `连接异常：${err.message}`;
      }
    }

    loadAll();
    setInterval(loadAll, 5000);
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "HermesDashboard/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _send_bytes(self, body: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(data, ensure_ascii=False, default=_json_default).encode("utf-8")
        self._send_bytes(body, "application/json; charset=utf-8", status)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        try:
            if path == "/":
                self._send_bytes(HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif path == "/api/health":
                self._send_json({"ok": True, "clock": report_clock_label()})
            elif path == "/api/overview":
                self._send_json(DATA.overview())
            elif path == "/api/trades":
                days = _safe_int((query.get("days") or ["30"])[0], 30)
                limit = _safe_int((query.get("limit") or ["200"])[0], 200)
                self._send_json({"ok": True, "trades": DATA.recent_trades(days=days, limit=limit)})
            elif path == "/api/positions":
                account = DATA.account_snapshot()
                orders = DATA.order_snapshot()
                self._send_json({"ok": True, "account": account, "orders": orders})
            elif path == "/api/export/trades.csv":
                self._send_trade_csv()
            else:
                self._send_json({"ok": False, "error": "not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            logger.exception("dashboard request failed: %s", self.path)
            self._send_json({"ok": False, "error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def _send_trade_csv(self) -> None:
        rows = DATA.recent_trades(days=365, limit=5000)
        buf = io.StringIO()
        fieldnames = [
            "exit_time",
            "symbol",
            "side_label",
            "strategy_line",
            "stage",
            "entry_price",
            "exit_price",
            "quantity",
            "pnl",
            "pnl_pct",
            "exit_reason_label",
            "session_id",
            "rows",
        ]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
        body = buf.getvalue().encode("utf-8-sig")
        self.send_response(HTTPStatus.OK.value)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", "attachment; filename=hermes_trades.csv")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hermes Trader read-only dashboard")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host, keep 127.0.0.1 for SSH tunnel safety")
    parser.add_argument("--port", type=int, default=8787, help="Bind port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    logger.info("Hermes dashboard listening on http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard stopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
