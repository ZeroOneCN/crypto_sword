# -*- coding: utf-8 -*-
"""Dashboard HTTP API and read-only static serving."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from services.time_basis import report_clock_label
from .data_service import DashboardData, _json_default, _safe_int

logger = logging.getLogger("dashboard")
DATA = DashboardData()
STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_HTML = (STATIC_DIR / "index.html").read_text(encoding="utf-8")

class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "HermesDashboard/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        message = fmt % args
        # Dashboard refreshes every few seconds. Keep normal 2xx polling out of
        # the trading log, but still surface redirects, client errors and server errors.
        if any(token in message for token in ('" 200 ', '" 204 ', '" 304 ')):
            logger.debug("%s - %s", self.address_string(), message)
            return
        logger.warning("%s - %s", self.address_string(), message)

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
                self._send_bytes(INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif path == "/api/health":
                self._send_json({"ok": True, "clock": report_clock_label()})
            elif path == "/api/overview":
                self._send_json(DATA.overview())
            elif path == "/api/trades":
                days = _safe_int((query.get("days") or ["30"])[0], 30)
                page = _safe_int((query.get("page") or ["1"])[0], 1)
                per_page = _safe_int((query.get("per_page") or query.get("limit") or ["15"])[0], 15)
                payload = DATA.recent_trades_page(days=days, page=page, per_page=per_page)
                self._send_json({"ok": True, **payload})
            elif path == "/api/errors":
                limit = _safe_int((query.get("limit") or ["80"])[0], 80)
                scan_lines = _safe_int((query.get("scan_lines") or ["5000"])[0], 5000)
                self._send_json({"ok": True, **DATA.transaction_errors(limit=limit, scan_lines=scan_lines)})
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
        DATA.period_report(365)
        rows = DATA.recent_trades(days=3650, limit=100000)
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
            "commission",
            "funding_fee",
            "exchange_net_pnl",
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
