#!/usr/bin/env python3
"""Unified runtime entry for Hermes Trader.

Starts the trading engine and the read-only dashboard in one process. The
dashboard is bound to localhost by default, so it is intended to be viewed via
SSH tunnel rather than exposed publicly.
"""

from __future__ import annotations

import logging
import threading
from http.server import ThreadingHTTPServer

from core.models import TradingConfig
from crypto_sword import CryptoSword
from dashboard.api import DashboardHandler
from run_live import build_config, build_parser

logger = logging.getLogger(__name__)


def start_dashboard(host: str, port: int) -> ThreadingHTTPServer:
    """Start the dashboard HTTP server in a daemon thread."""
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    thread = threading.Thread(target=server.serve_forever, name="hermes-dashboard", daemon=True)
    thread.start()
    actual_host, actual_port = server.server_address[:2]
    logger.info("Hermes dashboard listening on http://%s:%s", actual_host, actual_port)
    return server


def build_unified_parser():
    parser = build_parser()
    parser.description = "Run Hermes Trader trading engine with optional read-only dashboard"
    parser.add_argument("--dashboard", dest="dashboard_enabled", action="store_true", default=True, help="Start dashboard server")
    parser.add_argument("--no-dashboard", dest="dashboard_enabled", action="store_false", help="Disable dashboard server")
    parser.add_argument("--dashboard-host", default="127.0.0.1", help="Dashboard bind host")
    parser.add_argument("--dashboard-port", type=int, default=8787, help="Dashboard bind port")
    return parser


def main() -> None:
    args = build_unified_parser().parse_args()
    dashboard_server: ThreadingHTTPServer | None = None

    if args.dashboard_enabled:
        dashboard_server = start_dashboard(args.dashboard_host, args.dashboard_port)

    config: TradingConfig = build_config(args)
    trader = CryptoSword(config)
    try:
        trader.run()
    finally:
        if dashboard_server is not None:
            dashboard_server.shutdown()
            dashboard_server.server_close()
            logger.info("Hermes dashboard stopped")


if __name__ == "__main__":
    main()
