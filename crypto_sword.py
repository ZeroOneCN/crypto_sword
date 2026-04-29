#!/usr/bin/env python3
"""Crypto Sword runtime orchestrator."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from hermes_paths import hermes_logs_dir, hermes_scripts_dir
_DEFAULT_SCRIPTS_DIR = hermes_scripts_dir()
_SCRIPTS_DIR = Path(os.environ.get("HERMES_SCRIPTS_DIR", str(_DEFAULT_SCRIPTS_DIR)))
if str(_SCRIPTS_DIR) and str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

try:
    from adapters.rest_gateway import load_account_balance
    from telegram_notifier import (
        format_error_msg,
        format_shutdown_msg,
        format_startup_msg,
        send_telegram_message,
    )
    from trade_logger import TradeDatabase
except ImportError as e:
    print(f"Import failed: {e}")
    print(f"Please ensure scripts dir is accessible: {_SCRIPTS_DIR} (or set HERMES_SCRIPTS_DIR)")
    sys.exit(1)

_DEFAULT_LOG_DIR = hermes_logs_dir()
_LOG_DIR = Path(os.environ.get("HERMES_LOG_DIR", str(_DEFAULT_LOG_DIR)))
_LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            str(_LOG_DIR / "crypto_sword.log"),
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.models import PositionTracker, TradingConfig
from core.execution_mixin import ExecutionMixin
from core.scanner_mixin import ScannerMixin
from core.cycle_mixin import CycleMixin
from core.sync_mixin import SyncMixin
from core.confirmation_mixin import ConfirmationMixin
from core.market_mixin import MarketMixin
from core.bootstrap_service import BootstrapService
from feature_store import feature_store


class CryptoSword(ExecutionMixin, ScannerMixin, CycleMixin, SyncMixin, ConfirmationMixin, MarketMixin):
    """Trading runtime orchestrator."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self._log_dir = _LOG_DIR
        self.tracker = PositionTracker()
        self.db = TradeDatabase()
        self.daily_pnl = 0.0
        self.day_start_balance: float = 0.0
        self._daily_marker = datetime.now().date().isoformat()
        self._daily_loss_alert_sent = False
        self.traded_symbols_today: set = set()
        self.running = True
        self._last_summary_time: float = 0
        self._summary_interval: int = 6 * 3600
        
        self._last_radar_scan_time: float = 0
        self._radar_scan_interval: int = 3600
        self._last_pool_scan_time: float = 0
        self._pool_scan_interval: int = 86400
        
        self._base_scan_interval = config.scan_interval_sec
        self._current_scan_interval = config.scan_interval_sec
        self._market_overview: dict[str, Any] = {}
        self._tp_multiplier: float = 1.0
        self._market_ws_client = None
        self._ws_client = None
        self._user_ws_client = None
        self._ws_symbols: set[str] = set()
        self._ws_last_refresh: float = 0.0
        self._state_lock = threading.RLock()
        self._last_user_stream_sync: float = 0.0
        self._new_entries_suspended = False
        self._new_entries_suspended_alert_sent = False
        self._startup_audit_started = False
        self._last_monitor_time: float = 0.0
        self._monitor_interval: int = 300
        self._latency_alert_threshold_ms: float = 5000.0
        self._fast_candidates: list[str] = []
        self._last_fast_scan_time: float = 0.0
        self._last_deep_scan_time: float = 0.0
        self._last_position_sync_time: float = 0.0
        self._consecutive_losses: int = 0
        self._entry_watchlist: dict[str, dict[str, Any]] = {}
        self._last_daily_report_sent_for: str = ""
        self._last_watch_monitor_time: float = 0.0
        self._market_style_mode: str = "balanced"
        self._market_style_stats: dict[str, Any] = {}
        self._last_market_style_refresh: float = 0.0
        self._last_summary_signature: str = ""
        self._last_scan_monitor_signature: str = ""
        self._last_watch_monitor_signature: str = ""
        self._last_scan_monitor_snapshot: dict[str, str] = {}
        self._last_watch_monitor_snapshot: dict[str, str] = {}
        self._last_scan_monitor_order: dict[str, int] = {}
        self._last_watch_monitor_order: dict[str, int] = {}
        self._account_info_cache: Optional[dict[str, Any]] = None
        self._account_info_cache_at: float = 0.0
        self._market_style_trade_marker: tuple[int, str] = (0, "")
        self._daily_report_cache: Optional[dict[str, Any]] = None
        self._daily_report_cache_date: str = ""
        self._daily_report_cache_at: float = 0.0

    def _new_session_id(self, symbol: str) -> str:
        return f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _record_latency_step(self, steps: list[tuple[str, float]], name: str, started_at: float):
        steps.append((name, (time.perf_counter() - started_at) * 1000.0))

    def _enrich_summary_with_db(self, summary: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(summary)
        try:
            daily_report = self._get_daily_report_snapshot()
            enriched["closed_today"] = int(daily_report.get("closed_trades", 0) or 0)
            enriched["realized_pnl"] = round(float(daily_report.get("total_pnl", 0) or 0), 2)
        except Exception as e:
            logger.debug(f"summary api enrichment skipped: {e}")
        return enriched

    def _enrich_daily_report_with_api(self, report: dict[str, Any], date_str: str) -> dict[str, Any]:
        """Build daily report strictly from Binance API userTrades (no local DB metrics)."""
        api_report: dict[str, Any] = {
            "date": date_str,
            "mode": self.config.mode,
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
            "entry_protection": {
                "attempts": 0,
                "ok": 0,
                "failed": 0,
                "ok_rate": 0.0,
                "failed_by_symbol": {},
                "failed_by_direction": {},
                "failed_by_detail": {},
            },
        }
        del report  # explicitly ignore local report data
        try:
            api_report["entry_protection"] = feature_store.summarize_entry_protection(date_str, tz_offset_hours=8)
        except Exception as e:
            logger.debug(f"entry protection summary skipped [{date_str}]: {e}")
        try:
            from binance_api_client import get_native_binance_client
            from datetime import timezone, timedelta

            client = get_native_binance_client()

            tz_utc8 = timezone(timedelta(hours=8))
            target_date = datetime.fromisoformat(date_str).replace(tzinfo=tz_utc8)
            day_start_utc8 = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end_utc8 = day_start_utc8 + timedelta(days=1)
            start_ms = int(day_start_utc8.timestamp() * 1000)
            end_ms = int(day_end_utc8.timestamp() * 1000)

            trades = client.get_trade_history(start_time=start_ms, end_time=end_ms, limit=1000)
            order_stats: dict[tuple[str, int], dict[str, float | str]] = {}

            for trade in trades:
                pnl = float(trade.get("realizedPnl", 0) or 0)
                if abs(pnl) <= 1e-12:
                    continue

                symbol = str(trade.get("symbol", "") or "")
                order_id = int(trade.get("orderId", 0) or 0)
                if not symbol or order_id <= 0:
                    continue

                price = float(trade.get("price", 0) or 0)
                qty = abs(float(trade.get("qty", 0) or 0))
                quote_qty = abs(float(trade.get("quoteQty", 0) or 0))
                notional = quote_qty if quote_qty > 0 else abs(price * qty)

                key = (symbol, order_id)
                bucket = order_stats.setdefault(
                    key,
                    {
                        "symbol": symbol,
                        "pnl": 0.0,
                        "close_notional": 0.0,
                    },
                )
                bucket["pnl"] = float(bucket["pnl"]) + pnl
                bucket["close_notional"] = float(bucket["close_notional"]) + notional

            if not order_stats:
                return api_report

            symbols = sorted({str(key[0]) for key in order_stats.keys()})
            order_meta_map: dict[tuple[str, int], dict[str, Any]] = {}
            for symbol in symbols:
                try:
                    symbol_orders = client.all_orders(
                        symbol=symbol,
                        start_time=start_ms,
                        end_time=end_ms,
                        limit=1000,
                    )
                    for item in symbol_orders:
                        oid = int(item.get("orderId", 0) or 0)
                        if oid <= 0:
                            continue
                        order_meta_map[(symbol, oid)] = item
                except Exception as sub_e:
                    logger.debug(f"API all_orders skipped [{date_str}] {symbol}: {sub_e}")

            reason_counts: dict[str, int] = {}

            def _reason_from_order(order: dict[str, Any] | None) -> str:
                if not order:
                    return "FILLED"
                status = str(order.get("status", "") or "").upper() or "FILLED"
                order_type = str(order.get("type", "") or "").upper()
                if order_type in {"STOP", "STOP_MARKET", "TRAILING_STOP_MARKET"}:
                    return "STOP_LOSS"
                if order_type in {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}:
                    return "TAKE_PROFIT"
                if status in {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                    return status
                return "UNKNOWN"

            order_rows: list[dict[str, float | str]] = []
            for key, row in order_stats.items():
                pnl = float(row["pnl"])
                close_notional = float(row["close_notional"])
                pnl_pct = (pnl / close_notional * 100.0) if close_notional > 0 else 0.0
                reason = _reason_from_order(order_meta_map.get(key))
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                order_rows.append(
                    {
                        "symbol": str(row["symbol"]),
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "close_notional": close_notional,
                        "reason": reason,
                    }
                )

            total_pnl = sum(float(item["pnl"]) for item in order_rows)
            closed_count = len(order_rows)
            winning_count = sum(1 for item in order_rows if float(item["pnl"]) > 0)
            losing_count = sum(1 for item in order_rows if float(item["pnl"]) < 0)
            gross_profit = sum(float(item["pnl"]) for item in order_rows if float(item["pnl"]) > 0)
            gross_loss_abs = abs(sum(float(item["pnl"]) for item in order_rows if float(item["pnl"]) < 0))
            avg_win = gross_profit / winning_count if winning_count else 0.0
            avg_loss = -(gross_loss_abs / losing_count) if losing_count else 0.0
            payoff_ratio = (avg_win / abs(avg_loss)) if avg_loss else (999.0 if avg_win > 0 else 0.0)
            profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else (999.0 if gross_profit > 0 else 0.0)
            best_trade = max(order_rows, key=lambda item: float(item["pnl"]))
            worst_trade = min(order_rows, key=lambda item: float(item["pnl"]))

            api_report["closed_trades"] = closed_count
            api_report["winning_trades"] = winning_count
            api_report["losing_trades"] = losing_count
            api_report["total_pnl"] = round(total_pnl, 2)
            api_report["win_rate"] = round(winning_count / closed_count * 100, 2) if closed_count else 0.0
            api_report["avg_pnl"] = round(total_pnl / closed_count, 2) if closed_count else 0.0
            api_report["avg_win"] = round(avg_win, 2)
            api_report["avg_loss"] = round(avg_loss, 2)
            api_report["payoff_ratio"] = round(payoff_ratio, 2)
            api_report["profit_factor"] = round(profit_factor, 2)
            api_report["max_loss"] = round(min(0.0, float(worst_trade["pnl"])), 2)
            api_report["best_trade"] = {
                "symbol": str(best_trade["symbol"]),
                "pnl": round(float(best_trade["pnl"]), 2),
                "pnl_pct": round(float(best_trade["pnl_pct"]), 2),
            }
            api_report["worst_trade"] = {
                "symbol": str(worst_trade["symbol"]),
                "pnl": round(float(worst_trade["pnl"]), 2),
                "pnl_pct": round(float(worst_trade["pnl_pct"]), 2),
            }
            api_report["reason_counts"] = reason_counts

            logger.info(
                f"Daily report from API [{date_str}] | orders={closed_count} "
                f"pnl={float(api_report['total_pnl']):+,.2f} "
                f"best={api_report['best_trade']['symbol']}({float(api_report['best_trade']['pnl']):+,.2f},"
                f"{float(api_report['best_trade']['pnl_pct']):+,.2f}%) "
                f"worst={api_report['worst_trade']['symbol']}({float(api_report['worst_trade']['pnl']):+,.2f},"
                f"{float(api_report['worst_trade']['pnl_pct']):+,.2f}%) "
                f"reasons={reason_counts}"
            )
        except Exception as e:
            logger.debug(f"API daily report build failed [{date_str}]: {e}")

        return api_report

    def _get_daily_report_snapshot(self, ttl_sec: float = 60.0, force: bool = False) -> dict[str, Any]:
        report_date = datetime.now().date().isoformat()
        now = time.time()
        if (
            not force
            and self._daily_report_cache is not None
            and self._daily_report_cache_date == report_date
            and now - self._daily_report_cache_at < max(0.0, ttl_sec)
        ):
            return self._daily_report_cache

        snapshot = self._enrich_daily_report_with_api({}, report_date)
        if isinstance(snapshot, dict):
            self._daily_report_cache = snapshot
            self._daily_report_cache_date = report_date
            self._daily_report_cache_at = now
            return snapshot
        return {}

    def _get_account_info_cached(self, ttl_sec: float = 3.0, force: bool = False) -> dict[str, Any]:
        now = time.time()
        if (
            not force
            and self._account_info_cache is not None
            and now - self._account_info_cache_at < max(0.0, ttl_sec)
        ):
            return self._account_info_cache

        account_info = load_account_balance()
        if isinstance(account_info, dict):
            self._account_info_cache = account_info
            self._account_info_cache_at = now
            return account_info
        raise RuntimeError("??????????")

    def _emit_latency_trace(
        self,
        flow: str,
        started_at: float,
        steps: list[tuple[str, float]],
        symbol: str = "",
        threshold_ms: float | None = None,
    ):
        total_ms = (time.perf_counter() - started_at) * 1000.0
        threshold = threshold_ms if threshold_ms is not None else self._latency_alert_threshold_ms
        step_text = " | ".join(f"{name}={elapsed_ms:.0f}ms" for name, elapsed_ms in steps)
        logger.info(
            f"{flow} latency{f' {symbol}' if symbol else ''}: "
            f"total={total_ms:.0f}ms"
            + (f" | {step_text}" if step_text else "")
        )
        if total_ms >= threshold:
            logger.warning(
                f"Latency threshold exceeded | flow={flow} | symbol={symbol or '-'} | "
                f"total={total_ms:.0f}ms | threshold={threshold:.0f}ms"
            )

    def _should_sync_positions(self, now: float, deep_due: bool) -> bool:
        """Keep exchange sync frequent when we have risk to manage, lighter when flat."""
        open_count = self.tracker.get_open_count()
        ws_live = self._user_ws_client is not None
        if open_count > 0:
            base_interval = max(240, self._current_scan_interval) if ws_live else max(120, self._current_scan_interval)
            return deep_due or now - self._last_position_sync_time >= base_interval
        if ws_live:
            idle_interval = max(1800, self._current_scan_interval * 6)
        else:
            idle_interval = max(900, self._current_scan_interval * 4)
        return self._last_position_sync_time <= 0 or now - self._last_position_sync_time >= idle_interval

    def run(self):
        """Main trading loop."""
        bootstrap = BootstrapService(self, logger)
        mode_text = bootstrap.mode_text()
        bootstrap.log_startup_banner()
        bootstrap.startup()

        while self.running:
            try:
                self.run_scan_cycle()
                sleep_sec = max(10, min(self.config.fast_scan_interval_sec, self._current_scan_interval))
                logger.info(f"Wait {sleep_sec}s before next scan cycle")
                time.sleep(sleep_sec)
            except KeyboardInterrupt:
                logger.info("Interrupted by user, stopping...")
                self.running = False
            except Exception as e:
                error_text = str(e)
                transient_network_error = any(
                    token in error_text
                    for token in (
                        "Connection reset by peer",
                        "Remote end closed connection",
                        "timed out",
                        "Temporary failure",
                    )
                )
                if transient_network_error:
                    logger.warning(f"Main loop transient network error: {error_text}")
                    time.sleep(10)
                    continue
                logger.error(f"Main loop exception: {e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="主循环异常",
                        message=error_text,
                        component="main_loop",
                    )
                )
                time.sleep(10)

        bootstrap.shutdown(mode_text)


def main():
    parser = argparse.ArgumentParser(
        description="Crypto Sword - Binance futures live trading runtime",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example:\n  python3 crypto_sword.py --live",
    )

    parser.add_argument("--leverage", "-l", type=int, default=5, choices=range(1, 11), metavar="1-10", help="Leverage (1-10x)")
    parser.add_argument("--risk", "-r", type=float, default=1.0, help="Risk per trade (%%)")
    parser.add_argument("--stop-loss", "-s", type=float, default=7.0, help="Stop loss (%%)")
    parser.add_argument("--take-profit", "-t", type=float, default=18.0, help="Take profit (%%)")
    parser.add_argument("--take-profit-mode", choices=["price", "roi"], default="roi", help="Take profit mode")
    parser.add_argument("--max-positions", "-m", type=int, default=10, help="Max open positions")
    parser.add_argument("--max-position-pct", type=float, default=35.0, help="Max notional position size (%% of balance)")
    parser.add_argument("--max-total-exposure", type=float, default=220.0, help="Max total notional exposure (%% of balance)")
    parser.add_argument("--max-daily-loss", type=float, default=0.0, help="Max daily loss (%%), 0 disables daily loss circuit breaker")

    parser.add_argument("--top", type=int, default=50, help="Top N symbols")
    parser.add_argument("--interval", "-i", type=int, default=120, help="Deep scan interval (seconds)")
    parser.add_argument("--fast-interval", type=int, default=30, help="Fast scan interval (seconds)")
    parser.add_argument("--scan-workers", type=int, default=8, help="Scan workers")
    parser.add_argument("--min-change", type=float, default=1.0, help="Min 24h change (%%)")
    parser.add_argument("--min-pullback", type=float, default=1.0, help="Min pullback (%%)")
    parser.add_argument("--reclaim-volume", type=float, default=1.05, help="5m volume reclaim ratio")
    parser.add_argument("--by-volume", action="store_true", help="Rank by volume instead of change")
    parser.add_argument("--no-entry-confirm", action="store_true", help="Disable entry confirmation")
    parser.add_argument("--entry-confirm-timeout", type=int, default=1800, help="Entry confirmation timeout (seconds)")
    parser.add_argument("--no-momentum-entry", action="store_true", help="Disable momentum entry")
    parser.add_argument("--momentum-score", type=float, default=60.0, help="Momentum entry min score")
    parser.add_argument("--accumulation-score", type=float, default=55.0, help="Accumulation entry min score")
    parser.add_argument("--accumulation-min-oi", type=float, default=8.0, help="Accumulation entry min OI change (%%)")
    parser.add_argument("--accumulation-max-change", type=float, default=20.0, help="Accumulation entry max 24h change (%%)")
    parser.add_argument("--max-abs-funding-rate", type=float, default=0.004, help="Max abs funding rate")
    parser.add_argument("--max-range-position", type=float, default=95.0, help="Max 24h range position (%%)")
    parser.add_argument("--max-chase-change", type=float, default=35.0, help="Max chase 24h change (%%)")
    parser.add_argument("--no-daily-report", action="store_true", help="Disable daily report")
    parser.add_argument("--trailing", type=float, default=5.0, help="Trailing stop (%%)")
    parser.add_argument("--no-trailing", action="store_true", help="Disable trailing stop")

    args = parser.parse_args()

    mode = "live"
    print("\n" + "=" * 50)
    print("WARNING: LIVE TRADING MODE")
    print("=" * 50)
    print(f"\nAbout to trade real funds | leverage={args.leverage}x risk={args.risk}% stop_loss={args.stop_loss}%")
    print("Confirm? type 'y' to continue")
    if not sys.stdin.isatty():
        print("Non-interactive mode, auto-confirm")
        confirm = "y"
    else:
        confirm = input("> ").strip().lower()
    if confirm != "y":
        print("Canceled")
        sys.exit(0)

    config = TradingConfig(
        mode=mode,
        leverage=args.leverage,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        take_profit_mode=args.take_profit_mode,
        max_position_pct=max(5.0, args.max_position_pct),
        max_total_exposure_pct=max(args.max_position_pct, args.max_total_exposure),
        max_daily_loss_pct=args.max_daily_loss,
        max_open_positions=args.max_positions,
        trailing_stop_pct=args.trailing,
        trailing_stop_enabled=not args.no_trailing,
        scan_top_n=args.top,
        scan_interval_sec=args.interval,
        fast_scan_interval_sec=max(10, args.fast_interval),
        scan_workers=max(1, args.scan_workers),
        min_stage="pre_break",
        scan_by_change=not args.by_volume,
        min_change_pct=args.min_change,
        max_chase_change_pct=args.max_chase_change,
        min_pullback_pct=max(0.5, args.min_pullback),
        reclaim_volume_ratio=max(0.8, args.reclaim_volume),
        max_range_position_pct=args.max_range_position,
        max_abs_funding_rate=args.max_abs_funding_rate,
        entry_confirmation_enabled=not args.no_entry_confirm,
        entry_confirmation_timeout_sec=max(300, args.entry_confirm_timeout),
        momentum_entry_enabled=not args.no_momentum_entry,
        momentum_entry_score=max(0.0, args.momentum_score),
        accumulation_entry_score=max(0.0, args.accumulation_score),
        accumulation_entry_min_oi_pct=max(0.0, args.accumulation_min_oi),
        accumulation_entry_max_change_pct=max(0.0, args.accumulation_max_change),
        daily_report_enabled=not args.no_daily_report,
    )

    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()

