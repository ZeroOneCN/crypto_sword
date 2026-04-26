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
        self._symbol_cooldowns: dict[str, float] = {}
        self._consecutive_losses: int = 0
        self._loss_pause_until: float = 0.0
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

    def _new_session_id(self, symbol: str) -> str:
        return f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _record_latency_step(self, steps: list[tuple[str, float]], name: str, started_at: float):
        steps.append((name, (time.perf_counter() - started_at) * 1000.0))

    def _enrich_summary_with_db(self, summary: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(summary)
        report_date = datetime.now().date().isoformat()
        try:
            daily_report = self.db.get_daily_report(report_date, mode=self.config.mode)
            enriched["closed_today"] = int(daily_report.get("closed_trades", 0) or 0)
            enriched["realized_pnl"] = round(float(daily_report.get("total_pnl", 0) or 0), 2)
        except Exception as e:
            logger.debug(f"summary db enrichment skipped: {e}")
        return enriched

    def _enrich_daily_report_with_api(self, report: dict[str, Any], date_str: str) -> dict[str, Any]:
        """从 Binance API 获取指定日期的真实交易数据，修正最佳/最差交易和统计。"""
        try:
            from binance_api_client import get_native_binance_client
            from collections import defaultdict
            from datetime import timezone, timedelta

            client = get_native_binance_client()

            # 解析日期字符串，计算该日 00:00-23:59 UTC+8 的时间戳
            target_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
            day_start_utc8 = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end_utc8 = day_start_utc8 + timedelta(days=1)

            # 转换为 UTC 时间戳 (Binance API 使用 UTC 毫秒时间戳)
            start_ms = int(day_start_utc8.timestamp() * 1000)
            end_ms = int(day_end_utc8.timestamp() * 1000)

            trades = client.get_trade_history(start_time=start_ms, end_time=end_ms, limit=500)

            # 按交易对聚合已实现盈亏
            symbol_pnl: dict[str, float] = defaultdict(float)
            for trade in trades:
                pnl = float(trade.get("realizedPnl", 0) or 0)
                if pnl != 0:
                    symbol = trade.get("symbol", "")
                    symbol_pnl[symbol] += pnl

            if symbol_pnl:
                # 找出最佳交易
                best_symbol = max(symbol_pnl, key=symbol_pnl.get)
                best_pnl = symbol_pnl[best_symbol]
                report["best_trade"] = {
                    "symbol": best_symbol,
                    "pnl": round(best_pnl, 2),
                }

                # 找出最差交易
                worst_symbol = min(symbol_pnl, key=symbol_pnl.get)
                worst_pnl = symbol_pnl[worst_symbol]
                report["worst_trade"] = {
                    "symbol": worst_symbol,
                    "pnl": round(worst_pnl, 2),
                }

                # 用 API 数据修正总盈亏和胜率
                total_pnl_api = round(sum(symbol_pnl.values()), 2)
                winning_count = sum(1 for pnl in symbol_pnl.values() if pnl > 0)
                losing_count = sum(1 for pnl in symbol_pnl.values() if pnl < 0)
                closed_count = len(symbol_pnl)

                report["total_pnl"] = total_pnl_api
                report["closed_trades"] = closed_count
                report["winning_trades"] = winning_count
                report["losing_trades"] = losing_count
                report["win_rate"] = round(winning_count / closed_count * 100, 2) if closed_count else 0.0
                report["avg_pnl"] = round(total_pnl_api / closed_count, 2) if closed_count else 0.0

                logger.info(f"Daily report enriched from API [{date_str}]: {closed_count} trades, PnL={total_pnl_api}, best={best_symbol}({best_pnl:+.2f}), worst={worst_symbol}({worst_pnl:+.2f})")
        except Exception as e:
            logger.debug(f"API daily report enrichment skipped [{date_str}]: {e}")

        return report

    def _get_daily_report_snapshot(self) -> dict[str, Any]:
        report_date = datetime.now().date().isoformat()
        try:
            report = self.db.get_daily_report(report_date, mode=self.config.mode)
        except Exception as e:
            logger.debug(f"daily report snapshot skipped: {e}")
            report = {
                "closed_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "best_trade": None,
                "worst_trade": None,
            }

        return self._enrich_daily_report_with_api(report, report_date)

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
                logger.error(f"Main loop exception: {e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="Main loop exception",
                        message=str(e),
                    )
                )
                time.sleep(10)

        bootstrap.shutdown(mode_text)


def main():
    parser = argparse.ArgumentParser(
        description="CRYPTO SWORD 实盘交易程序",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="示例:\n  crypto-sword\n  crypto-sword --leverage 10",
    )

    parser.add_argument("--live", action="store_true", default=True, help="实盘模式（默认开启）")
    parser.add_argument("--leverage", "-l", type=int, default=5, choices=range(1, 11),
                        metavar="1-10", help="杠杆倍数 (1-10x)")

    parser.add_argument("--risk", "-r", type=float, default=2.0, help="每笔风险百分比")
    parser.add_argument("--stop-loss", "-s", type=float, default=8.0, help="止损百分比")
    parser.add_argument("--take-profit", "-t", type=float, default=20.0, help="止盈百分比")
    parser.add_argument(
        "--take-profit-mode",
        choices=["price", "roi"],
        default="roi",
        help="止盈口径：roi=杠杆后收益率，price=标的价格涨跌幅",
    )
    parser.add_argument("--max-positions", "-m", type=int, default=5, help="最大持仓数")
    parser.add_argument("--max-daily-loss", type=float, default=5.0, help="每日最大亏损百分比")

    parser.add_argument("--top", type=int, default=30, help="扫描前 N 个币种")
    parser.add_argument("--interval", "-i", type=int, default=300, help="扫描间隔秒数")
    parser.add_argument("--scan-workers", type=int, default=6, help="深度扫描并发数")
    parser.add_argument("--min-change", type=float, default=2.0, help="最小涨跌幅百分比")
    parser.add_argument("--min-pullback", type=float, default=2.0, help="最小回踩百分比")
    parser.add_argument("--reclaim-volume", type=float, default=1.05, help="回踩后 5m 量能回归倍数")
    parser.add_argument("--by-volume", action="store_true", help="按成交量排序（默认按涨幅）")
    parser.add_argument("--no-entry-confirm", action="store_true", help="禁用回踩确认入场")
    parser.add_argument("--entry-confirm-timeout", type=int, default=1800, help="候选观察超时秒数")
    parser.add_argument("--no-momentum-entry", action="store_true", help="禁用强趋势动量入场")
    parser.add_argument("--momentum-score", type=float, default=60.0, help="动量入场最低评分 (默认：60)")
    parser.add_argument("--accumulation-score", type=float, default=50.0, help="暗流最低评分 (默认：50)")
    parser.add_argument("--accumulation-min-oi", type=float, default=10.0, help="暗流最小OI变化% (默认：10)")
    parser.add_argument("--accumulation-max-change", type=float, default=15.0, help="暗流最大涨跌幅% (默认：15)")
    parser.add_argument("--max-abs-funding-rate", type=float, default=0.005, help="最大绝对资金费率 (默认：0.5%%)")
    parser.add_argument("--max-range-position", type=float, default=92.0, help="最大24h区间位置%% (默认：92)")
    parser.add_argument("--max-chase-change", type=float, default=35.0, help="最大追涨24h涨幅%% (默认：35)")
    parser.add_argument("--max-consecutive-losses", type=int, default=3, help="连续亏损熔断笔数")
    parser.add_argument("--loss-pause-mins", type=int, default=30, help="连续亏损后暂停分钟")
    parser.add_argument("--no-daily-report", action="store_true", help="禁用每日复盘通知")

    parser.add_argument("--trailing", type=float, default=5.0, help="追踪止损百分比")
    parser.add_argument("--no-trailing", action="store_true", help="禁用追踪止损")

    args = parser.parse_args()

    mode = "live"
    print("\n" + "=" * 50)
    print("⚠️  ⚠️  ⚠️  实盘交易警告  ⚠️  ⚠️  ⚠️")
    print("=" * 50)
    print("\n即将使用真实资金进行交易")
    print(f"杠杆: {args.leverage}x | 风险: {args.risk}% | 止损: {args.stop_loss}%")
    print("\n确认继续？输入 'y' 继续，其他键取消")
    if not sys.stdin.isatty():
        print("ℹ️ 后台模式，跳过确认")
        confirm = "y"
    else:
        confirm = input("> ").strip().lower()
    if confirm != "y":
        print("❌ 已取消")
        sys.exit(0)

    # 创建配置
    config = TradingConfig(
        mode=mode,
        leverage=args.leverage,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        take_profit_mode=args.take_profit_mode,
        max_position_pct=20.0,
        max_daily_loss_pct=args.max_daily_loss,
        max_open_positions=args.max_positions,
        trailing_stop_pct=args.trailing,
        trailing_stop_enabled=not args.no_trailing,
        scan_top_n=args.top,
        scan_interval_sec=args.interval,
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
        max_consecutive_losses=max(1, int(args.max_consecutive_losses)),
        loss_pause_sec=max(300, int(args.loss_pause_mins) * 60),
        accumulation_entry_score=max(0.0, args.accumulation_score),
        accumulation_entry_min_oi_pct=max(0.0, args.accumulation_min_oi),
        accumulation_entry_max_change_pct=max(0.0, args.accumulation_max_change),
        daily_report_enabled=not args.no_daily_report,
    )

    # 启动交易引擎
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()
