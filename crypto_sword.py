#!/usr/bin/env python3
"""Crypto Sword runtime orchestrator."""

from __future__ import annotations

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
    print(f"导入失败: {e}")
    print(f"请确认脚本目录可访问: {_SCRIPTS_DIR}（或设置 HERMES_SCRIPTS_DIR）")
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

    def _get_daily_report_snapshot(self) -> dict[str, Any]:
        report_date = datetime.now().date().isoformat()
        try:
            return self.db.get_daily_report(report_date, mode=self.config.mode)
        except Exception as e:
            logger.debug(f"daily report snapshot skipped: {e}")
            return {
                "closed_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "best_trade": None,
                "worst_trade": None,
            }

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
        raise RuntimeError("璐︽埛淇℃伅杩斿洖鏍煎紡寮傚父")

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
            f"鈴憋笍 {flow} latency{f' {symbol}' if symbol else ''}: "
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
        mode_text = f"{self.config.mode_emoji} {self.config.mode_name} 模式"
        logger.info("=" * 60)
        logger.info(f"🗡️  {mode_text} 启动")
        logger.info(f"🔡 杠杆: {self.config.leverage}x | 风险: {self.config.risk_per_trade_pct}%")
        logger.info(
            f"🛝 止损: {self.config.stop_loss_pct}% | 止盈: {self.config.take_profit_pct}% "
            f"({self.config.take_profit_mode})"
        )
        logger.info(f"👀 扫描: 前 {self.config.scan_top_n} 币种 | 间隔: {self.config.scan_interval_sec}s")
        logger.info(f"📱 最大持仓: {self.config.max_open_positions}")
        logger.info("=" * 60)

        try:
            self.day_start_balance = self._run_health_checks()
            self._start_market_ticker_stream()
            self._start_user_data_stream()
            self._start_background_protection_audit(source="startup_audit")
            logger.info(f"🚀 启动健康检查通过 | 可用余额: ${self.day_start_balance:.2f}")
        except Exception as e:
            logger.error(f"❌ 启动健康检查失败: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="启动健康检查失败",
                    message=str(e),
                    component="startup_checks",
                )
            )
            raise

        send_telegram_message(
            format_startup_msg(
                mode_name=mode_text,
                leverage=self.config.leverage,
                risk_pct=self.config.risk_per_trade_pct,
                stop_loss_pct=self.config.stop_loss_pct,
                take_profit_pct=self.config.take_profit_pct,
                scan_top_n=self.config.scan_top_n,
                scan_interval_sec=self.config.scan_interval_sec,
                max_positions=self.config.max_open_positions,
            )
        )

        self._refresh_market_profile()

        while self.running:
            try:
                self.run_scan_cycle()
                sleep_sec = max(10, min(self.config.fast_scan_interval_sec, self._current_scan_interval))
                logger.info(f"⏳ 等待 {sleep_sec}s 后进入下一轮扫描...")
                time.sleep(sleep_sec)
            except KeyboardInterrupt:
                logger.info("\n🛑 收到中断信号，准备停止...")
                self.running = False
            except Exception as e:
                logger.error(f"❌ 主循环异常: {e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="主循环异常",
                        message=str(e),
                    )
                )
                time.sleep(10)

        summary = self._enrich_summary_with_db(self.tracker.get_summary())
        send_telegram_message(
            format_shutdown_msg(
                mode_name=mode_text,
                closed_trades=summary["closed_today"],
                realized_pnl=summary["realized_pnl"],
                unrealized_pnl=summary["total_unrealized_pnl"],
            )
        )
        if self._ws_client:
            try:
                self._ws_client.stop()
            except Exception:
                pass
        if self._market_ws_client:
            try:
                self._market_ws_client.stop()
            except Exception:
                pass
        if self._user_ws_client:
            try:
                self._user_ws_client.stop()
            except Exception:
                pass

