#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║              🗡️  CRYPTO SWORD - 诸神黄昏之剑 🗡️               ║
║                                                               ║
║    统一 Binance 自动交易系统 — 实盘专用，1-10x 杠杆，          ║
║    山寨/meme 币专项扫描与执行                                  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

用法:
    crypto-sword --live        # 实盘模式（⚠️ 真实资金）
"""

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

# 支持环境变量配置路径，兼容现有部署
_DEFAULT_SCRIPTS_DIR = hermes_scripts_dir()
_SCRIPTS_DIR = Path(os.environ.get("HERMES_SCRIPTS_DIR", str(_DEFAULT_SCRIPTS_DIR)))
if str(_SCRIPTS_DIR) and str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

# ═══════════════════════════════════════════════════════════════
# 导入模块
# ═══════════════════════════════════════════════════════════════

try:
    from binance_breakout_scanner import (
        get_top_symbols_by_volume,
        get_top_symbols_by_change,
    )
    from binance_trading_executor import (
        get_account_balance,
    )
    from telegram_notifier import (
        format_error_msg,
        format_shutdown_msg,
        format_startup_msg,
        send_telegram_message,
    )
    from trade_logger import TradeDatabase, TradeRecord  # 📜 神圣交易日志
    from surf_enhancer import (  # 🌊 Surf 数据增强
        get_market_overview,
        enhance_symbol_data,
        get_social_mindshare,
    )
    from signal_enhancer import (  # 🎯 信号增强
        analyze_trend,
        analyze_volume,
        get_klines,
    )
    from risk_manager import (  # 🛡️ 风控系统
        assess_trade_risk,
        RiskConfig,
        calculate_position_size,
    )
    try:
        from binance_websocket import (
            BinanceAllMarketTickerWebSocketClient,
        )
    except Exception:
        BinanceAllMarketTickerWebSocketClient = None
except ImportError as e:
    print(f"❌ 导入失败：{e}")
    print(f"请确保脚本目录可见：{_SCRIPTS_DIR}（或设置 HERMES_SCRIPTS_DIR）")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 日志配置
# ═══════════════════════════════════════════════════════════════

# 支持环境变量配置日志路径
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

# 核心模型拆分到子模块，主程序专注编排逻辑
from core.models import Position, PositionTracker, TradingConfig
from core.execution_mixin import ExecutionMixin
from core.scanner_mixin import ScannerMixin
from core.cycle_mixin import CycleMixin
from core.sync_mixin import SyncMixin


# ═══════════════════════════════════════════════════════════════
# 主交易引擎 - 诸神黄昏之剑
# ═══════════════════════════════════════════════════════════════

class CryptoSword(ExecutionMixin, ScannerMixin, CycleMixin, SyncMixin):
    """
    🗡️ CRYPTO SWORD - 诸神黄昏之剑

    监控与交易的化身，于测试之荒原与实战的腥风血雨间切换
    捕捉山寨与 meme 的血腥气息，执行雷霆般的杀伐
    """

    def __init__(self, config: TradingConfig):
        self.config = config
        self._log_dir = _LOG_DIR
        self.tracker = PositionTracker()
        self.db = TradeDatabase()  # 📜 神圣交易日志
        self.daily_pnl = 0.0
        self.day_start_balance: float = 0.0
        self._daily_marker = datetime.now().date().isoformat()
        self._daily_loss_alert_sent = False
        self.traded_symbols_today: set = set()
        self.running = True
        
        # 通知控制
        self._last_summary_time: float = 0
        self._summary_interval: int = 6 * 3600  # 每 6 小时发送一次持仓汇总
        
        # 🏦 庄家雷达后台监控（初始化到 __init__ 防止 AttributeError）
        self._last_radar_scan_time: float = 0
        self._radar_scan_interval: int = 3600  # 每小时扫描一次 OI 异动
        self._last_pool_scan_time: float = 0
        self._pool_scan_interval: int = 86400  # 每天更新一次收筹池
        
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

        account_info = get_account_balance()
        if isinstance(account_info, dict):
            self._account_info_cache = account_info
            self._account_info_cache_at = now
            return account_info
        raise RuntimeError("账户信息返回格式异常")

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
            f"⏱️ {flow} latency{f' {symbol}' if symbol else ''}: "
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

    def _run_radar_background_scan(self, now: float):
        """庄家雷达后台扫描（每小时 OI 异动 + 每天收筹池更新）"""
        try:
            from accumulation_radar import scan_oi_changes, scan_accumulation_pool
            from telegram_notifier import format_dark_flow_alert, format_radar_summary

            # 每小时 OI 异动扫描
            if now - self._last_radar_scan_time >= self._radar_scan_interval:
                logger.info("🏦 开始 OI 异动扫描...")
                oi_signals = scan_oi_changes()
                self._last_radar_scan_time = now

                # 检测暗流信号并发送通知
                dark_flows = [s for s in oi_signals if s.is_dark_flow]
                if dark_flows:
                    for df in dark_flows[:3]:  # 最多通知 3 个
                        msg = format_dark_flow_alert(
                            symbol=df.symbol,
                            oi_change_pct=df.oi_change_pct,
                            price_change_pct=df.price_change_pct,
                            funding_rate=df.funding_rate,
                            market_cap=0,
                        )
                        send_telegram_message(msg)
                        logger.info(f"🎯 暗流信号已推送：{df.symbol}")

                # 发送雷达摘要
                if oi_signals:
                    summary = format_radar_summary(
                        pool_count=0,
                        oi_signals=len(oi_signals),
                        dark_flows=len(dark_flows),
                        short_fuel=0,
                        top_dark_flow=dark_flows[0].symbol if dark_flows else None,
                    )
                    send_telegram_message(summary)

            # 每天收筹池更新
            if now - self._last_pool_scan_time >= self._pool_scan_interval:
                logger.info("🏦 开始收筹池扫描...")
                pool = scan_accumulation_pool()
                self._last_pool_scan_time = now

                if pool:
                    from telegram_notifier import format_accumulation_pool_report
                    msg = format_accumulation_pool_report(pool)
                    send_telegram_message(msg)
                    logger.info(f"🏦 收筹池已更新：{len(pool)} 个标的")

        except ImportError:
            logger.debug("accumulation_radar 模块不可用，跳过雷达扫描")
        except Exception as e:
            logger.warning(f"雷达后台扫描失败：{e}")

    def _refresh_market_profile(self):
        """Update market-aware scan interval and TP multiplier."""
        try:
            overview = get_market_overview()
            self._market_overview = overview if isinstance(overview, dict) else {}
        except Exception as e:
            logger.warning(f"🌊 市场环境刷新失败：{e}")
            return

        fear_greed = self._market_overview.get("fear_greed", {})
        fear_greed_value = 50
        if isinstance(fear_greed, dict):
            fear_greed_value = int(float(fear_greed.get("value", fear_greed.get("score", 50)) or 50))
        liquidation_risk = str(self._market_overview.get("liquidation_risk", "LOW")).upper()
        sentiment = str(self._market_overview.get("market_sentiment", "NEUTRAL")).upper()

        interval = self._base_scan_interval
        tp_multiplier = 1.0

        if liquidation_risk in {"HIGH", "EXTREME"}:
            interval = max(60, min(interval, 120))
            tp_multiplier = 0.75
        elif sentiment in {"BULLISH", "RISK_ON"} and fear_greed_value >= 55:
            interval = max(90, min(interval, 180))
            tp_multiplier = 1.2
        elif fear_greed_value <= 30:
            interval = max(90, min(interval, 180))
            tp_multiplier = 0.8
        else:
            interval = max(120, interval)

        self._current_scan_interval = int(interval)
        self.config.scan_interval_sec = int(interval)
        self._tp_multiplier = tp_multiplier
        logger.info(
            f"🌊 动态参数：扫描间隔={self._current_scan_interval}s, TP倍率={self._tp_multiplier:.2f}, "
            f"情绪={sentiment}, 恐贪={fear_greed_value}, 清算={liquidation_risk}"
        )

    def _refresh_market_style(self, force: bool = False):
        """Auto-detect whether recent profits favor majors or hot alts."""
        now = time.time()
        if not force and now - self._last_market_style_refresh < self.config.market_style_refresh_sec:
            return
        self._last_market_style_refresh = now

        try:
            recent_closed = self.db.get_closed_trades(days=30, mode=self.config.mode)[: self.config.market_style_lookback_trades]
        except Exception as e:
            logger.warning(f"市场风格刷新失败：{e}")
            return

        marker = (
            len(recent_closed),
            str(recent_closed[0].exit_time) if recent_closed else "",
        )
        if not force and marker == self._market_style_trade_marker:
            return
        self._market_style_trade_marker = marker

        major_trades = [t for t in recent_closed if t.symbol.upper() in self.config.major_symbols]
        alt_trades = [t for t in recent_closed if t.symbol.upper() not in self.config.major_symbols]

        def _avg_pnl_pct(trades: list[Any]) -> float:
            if not trades:
                return 0.0
            return sum(float(t.pnl_pct or 0.0) for t in trades) / len(trades)

        major_avg = _avg_pnl_pct(major_trades)
        alt_avg = _avg_pnl_pct(alt_trades)
        major_win = sum(1 for t in major_trades if float(t.pnl or 0.0) > 0)
        alt_win = sum(1 for t in alt_trades if float(t.pnl or 0.0) > 0)

        style_mode = "balanced"
        if len(major_trades) >= 3 and (not alt_trades or major_avg >= alt_avg + 0.30):
            style_mode = "major"
        elif len(alt_trades) >= 3 and (not major_trades or alt_avg >= major_avg + 0.30):
            style_mode = "alt"

        self._market_style_mode = style_mode
        self._market_style_stats = {
            "lookback": len(recent_closed),
            "major_count": len(major_trades),
            "alt_count": len(alt_trades),
            "major_avg_pnl_pct": round(major_avg, 2),
            "alt_avg_pnl_pct": round(alt_avg, 2),
            "major_win": major_win,
            "alt_win": alt_win,
        }
        logger.info(
            f"📈 市场风格切换：mode={style_mode} | "
            f"major={len(major_trades)} avg={major_avg:+.2f}% | "
            f"alt={len(alt_trades)} avg={alt_avg:+.2f}%"
        )

    def _start_market_ticker_stream(self):
        """Start all-market mini ticker stream for fast anomaly ranking."""
        if BinanceAllMarketTickerWebSocketClient is None:
            logger.warning("All-market WebSocket unavailable; scanner will use REST ranking")
            return
        if self._market_ws_client:
            return

        try:
            self._market_ws_client = BinanceAllMarketTickerWebSocketClient()
            self._market_ws_client.start()
            logger.info("All-market WebSocket started: realtime anomaly ranking enabled")
        except Exception as e:
            self._market_ws_client = None
            logger.warning(f"All-market WebSocket start failed; scanner will use REST ranking: {e}")

    def _get_ws_top_symbols_by_change(self, limit: int, min_change: float) -> list[str]:
        if not self._market_ws_client:
            return []
        try:
            symbols = self._market_ws_client.get_top_symbols_by_change(
                limit=limit,
                min_change=min_change,
                max_age_sec=max(180, self._current_scan_interval * 2),
            )
            if symbols:
                logger.info(
                    f"📡 WS异动榜命中 {len(symbols)} 个币种 "
                    f"(缓存新鲜币种 {self._market_ws_client.size()}): {symbols[:5]}..."
                )
            return symbols
        except Exception as e:
            logger.debug(f"WS异动榜不可用，回退REST：{e}")
            return []

    def _fast_scan_candidates(self) -> list[str]:
        """Refresh lightweight candidate pool from all-market WS."""
        now = time.time()
        min_gap = max(10, int(self.config.fast_scan_interval_sec))
        if self._fast_candidates and now - self._last_fast_scan_time < min_gap:
            return self._fast_candidates

        candidates = []
        if self.config.scan_by_change:
            candidates = self._get_ws_top_symbols_by_change(
                self.config.scan_top_n,
                self.config.min_change_pct,
            )

        if candidates:
            self._fast_candidates = candidates
            self._last_fast_scan_time = now
            logger.info(f"⚡ Fast scan candidates: {len(candidates)} symbols | {candidates[:5]}...")
        elif self._fast_candidates:
            logger.info(f"⚡ Fast scan keeps previous candidates: {self._fast_candidates[:5]}...")

        return self._fast_candidates

    def _prune_entry_watchlist(self):
        """Drop expired observation candidates so stale breakouts do not auto-fire later."""
        if not self._entry_watchlist:
            return
        now = time.time()
        timeout_sec = max(300, int(self.config.entry_confirmation_timeout_sec))
        expired_symbols = [
            symbol
            for symbol, item in self._entry_watchlist.items()
            if now - float(item.get("first_seen_ts", now)) >= timeout_sec
        ]
        for symbol in expired_symbols:
            self._entry_watchlist.pop(symbol, None)
            logger.info(f"🗑️ {symbol} 候选观察超时，已淘汰")

    def _load_confirmation_trend(self, symbol: str) -> dict[str, Any]:
        """Reuse cached klines to confirm 1h trend and 15m reclaim."""
        klines_1h = get_klines(symbol, interval="1h", limit=50) or []
        klines_15m = get_klines(symbol, interval="15m", limit=50) or []
        klines_5m = get_klines(symbol, interval="5m", limit=50) or []
        trend_1h = analyze_trend(klines_1h)
        trend_15m = analyze_trend(klines_15m)
        trend_5m = analyze_trend(klines_5m)
        volume_5m = analyze_volume(klines_5m) if klines_5m else {"score": 0, "volume_ratio": 0, "volume_trend": "UNKNOWN"}
        return {
            "1h": trend_1h,
            "15m": trend_15m,
            "5m": trend_5m,
            "5m_volume": volume_5m,
        }

    def _short_tf_breakout_ready(self, trend: dict[str, Any], direction: str, current_price: float) -> bool:
        trend_5m = trend.get("5m", {}) or {}
        ma5 = float(trend_5m.get("ma5", 0) or 0)
        ma_alignment = str(trend_5m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        if direction == "LONG":
            return ma_alignment == "BULLISH" and current_price >= ma5 > 0
        return ma_alignment == "BEARISH" and 0 < current_price <= ma5

    def _volume_reclaim_ready(self, trend: dict[str, Any]) -> bool:
        volume_5m = trend.get("5m_volume", {}) or {}
        volume_ratio = float(volume_5m.get("volume_ratio", 0) or 0)
        volume_trend = str(volume_5m.get("volume_trend", "UNKNOWN") or "UNKNOWN")
        return volume_ratio >= self.config.reclaim_volume_ratio and volume_trend in {"INCREASING", "FLAT"}

    def _required_pullback_pct(self, metrics: dict[str, Any], score_total: float = 0.0) -> float:
        """Use a shallower pullback for strong momentum leaders and a deeper one for ordinary setups."""
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))

        if score_total >= 78 and change_24h >= 18 and oi_change >= 45:
            return max(1.2, self.config.shallow_pullback_pct - 0.6)
        if score_total >= self.config.momentum_entry_score and change_24h >= 12 and oi_change >= self.config.momentum_entry_min_oi_pct:
            return max(1.5, self.config.shallow_pullback_pct)
        if change_24h >= 15 and oi_change >= 35:
            return max(2.0, self.config.shallow_pullback_pct + 0.4)
        return self.config.min_pullback_pct

    def _soft_breakout_candidate(self, metrics: dict[str, Any], score_total: float) -> bool:
        """Allow elite movers with moderate OI expansion to join the breakout line."""
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        return (
            score_total >= 55.0
            and change_24h >= 10.0
            and oi_change >= 10.0
            and abs(funding) < self.config.max_abs_funding_rate
        )

    def _is_accumulation_candidate(self, metrics: dict[str, Any], score_total: float) -> bool:
        """Detect early accumulation before a full breakout extension prints."""
        if not self.config.accumulation_entry_enabled:
            return False
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        volume_mult = float(metrics.get("volume_24h_mult", 0) or 0)
        range_position = float(metrics.get("range_position_24h_pct", 50) or 50)
        return (
            score_total >= self.config.accumulation_entry_score
            and 0 < change_24h <= self.config.accumulation_entry_max_change_pct
            and oi_change >= self.config.accumulation_entry_min_oi_pct
            and volume_mult >= self.config.accumulation_entry_min_volume_mult
            and range_position <= self.config.accumulation_entry_max_range_pct
            and abs(funding) < self.config.max_abs_funding_rate
        )

    def _strategy_line_for_signal(self, signal: dict[str, Any]) -> str:
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        if (
            self.config.momentum_entry_enabled
            and score_total >= self.config.momentum_entry_score
            and change_24h >= self.config.momentum_entry_min_change_pct
            and oi_change >= self.config.momentum_entry_min_oi_pct
        ):
            return "趋势突破线"
        if (
            score_total >= 55.0
            and change_24h >= 12.0
            and oi_change >= 20.0
            and funding <= 0
        ):
            return "趋势突破线"
        if self._is_accumulation_candidate(metrics, score_total):
            return "趋势突破线"
        if self.config.momentum_entry_enabled and self._soft_breakout_candidate(metrics, score_total):
            return "趋势突破线"
        return "回踩确认线"

    def _current_pullback_pct(self, watch: dict[str, Any], direction: str, current_price: float) -> float:
        if direction == "LONG":
            anchor_price = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            return ((anchor_price - current_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0
        anchor_price = min(float(watch.get("lowest_price", current_price) or current_price), current_price)
        return ((current_price - anchor_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0

    def _update_watch_state(
        self,
        watch: dict[str, Any],
        *,
        strategy_line: str,
        stage_name: str,
        entry_note: str,
        required_pullback: float,
        current_pullback: float,
        trend: dict[str, Any] | None = None,
    ):
        watch["strategy_line"] = strategy_line
        watch["watch_stage"] = stage_name
        watch["entry_note"] = entry_note
        watch["required_pullback_pct"] = required_pullback
        watch["current_pullback_pct"] = current_pullback
        if trend is not None:
            watch["confirmation_trend"] = trend

    def _mark_watch_in_position(self, symbol: str, strategy_line: str, note: str = ""):
        now = time.time()
        watch = self._entry_watchlist.get(symbol)
        if not watch:
            watch = {
                "symbol": symbol,
                "direction": "",
                "stage": "in_position",
                "first_seen_ts": now,
                "last_seen_ts": now,
                "first_price": 0.0,
                "highest_price": 0.0,
                "lowest_price": 0.0,
                "score_total": 0.0,
                "pullback_seen": True,
                "required_pullback_pct": 0.0,
                "current_pullback_pct": 0.0,
                "metrics": {},
                "score": {},
            }
            self._entry_watchlist[symbol] = watch
        watch["strategy_line"] = strategy_line
        watch["watch_stage"] = "持仓中"
        watch["entry_note"] = note or "已开仓，继续跟踪后续再入场机会"
        watch["last_seen_ts"] = now

    def _is_momentum_entry_ready(
        self,
        signal: dict[str, Any],
        trend: dict[str, Any],
        current_price: float,
    ) -> tuple[bool, str]:
        """Allow exceptional momentum entries when waiting for pullback misses the move."""
        if not self.config.momentum_entry_enabled:
            return False, ""

        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        funding = abs(float(metrics.get("funding_rate", 0) or 0))

        if score_total < self.config.momentum_entry_score:
            if not self._soft_breakout_candidate(metrics, score_total):
                return False, ""
        if abs(change_24h) < self.config.momentum_entry_min_change_pct:
            return False, ""
        if oi_change < self.config.momentum_entry_min_oi_pct and not self._soft_breakout_candidate(metrics, score_total):
            return False, ""
        if funding >= self.config.max_abs_funding_rate:
            return False, ""

        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5 = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        higher_alignment = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)

        if direction == "LONG":
            if change_24h <= 0:
                return False, ""
            ready = higher_alignment == "BULLISH" and ma_alignment == "BULLISH" and current_price >= ma5 > 0 and short_tf_ok
        else:
            if change_24h >= 0:
                return False, ""
            ready = higher_alignment == "BEARISH" and ma_alignment == "BEARISH" and 0 < current_price <= ma5 and short_tf_ok

        if not ready:
            return False, ""
        return True, (
            f"强趋势动量确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%，"
            f"OI {oi_change:+.1f}%"
        )

    def _is_trend_continuation_ready(
        self,
        signal: dict[str, Any],
        trend: dict[str, Any],
        current_price: float,
    ) -> tuple[bool, str]:
        """More aggressive breakout continuation entry for fresh hot symbols."""
        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        funding = abs(float(metrics.get("funding_rate", 0) or 0))

        if score_total < 54:
            return False, ""
        if abs(change_24h) < 8.0:
            return False, ""
        if funding >= self.config.max_abs_funding_rate * 0.95:
            return False, ""

        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5_15m = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment_15m = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)

        if direction == "LONG":
            ready = (
                change_24h > 0
                and ma_alignment_1h == "BULLISH"
                and ma_alignment_15m == "BULLISH"
                and current_price >= ma5_15m > 0
                and short_tf_ok
            )
        else:
            ready = (
                change_24h < 0
                and ma_alignment_1h == "BEARISH"
                and ma_alignment_15m == "BEARISH"
                and 0 < current_price <= ma5_15m
                and short_tf_ok
            )

        if not ready:
            return False, ""
        return True, f"热点延续确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%"

    def _is_flow_reclaim_ready(
        self,
        signal: dict[str, Any],
        trend: dict[str, Any],
        current_price: float,
        pullback_pct: float,
    ) -> tuple[bool, str]:
        """After pullback, allow fast re-entry from flow/funding/OI instead of waiting full 15m reclaim."""
        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        funding = float(metrics.get("funding_rate", 0) or 0)
        trend_1h = trend.get("1h", {}) or {}
        trend_5m = trend.get("5m", {}) or {}
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma5_5m = float(trend_5m.get("ma5", 0) or 0)
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)

        if score_total < 60 or oi_change < 18:
            return False, ""

        if direction == "LONG":
            ready = (
                funding <= self.config.max_abs_funding_rate
                and ma_alignment_1h == "BULLISH"
                and current_price >= ma5_5m > 0
                and short_tf_ok
                and pullback_pct >= max(1.2, self.config.min_pullback_pct * 0.5)
            )
        else:
            ready = (
                funding >= -self.config.max_abs_funding_rate
                and ma_alignment_1h == "BEARISH"
                and 0 < current_price <= ma5_5m
                and short_tf_ok
                and pullback_pct >= max(1.2, self.config.min_pullback_pct * 0.5)
            )

        if not ready:
            return False, ""
        funding_text = f"{funding:+.4%}" if abs(funding) < 1 else f"{funding:+.2f}"
        return True, f"资金/OI快线入场：评分 {score_total:.1f}，OI {oi_change:+.1f}%，费率 {funding_text}"

    def _is_accumulation_entry_ready(
        self,
        signal: dict[str, Any],
        trend: dict[str, Any],
        current_price: float,
    ) -> tuple[bool, str]:
        """Early breakout trigger for accumulation-style setups inspired by OI-led radar scans."""
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        if not self._is_accumulation_candidate(metrics, score_total):
            return False, ""

        direction = signal.get("direction", "")
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5_15m = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma_alignment_15m = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)

        if direction == "LONG":
            ready = ma_alignment_1h == "BULLISH" and ma_alignment_15m == "BULLISH" and current_price >= ma5_15m > 0 and short_tf_ok
        else:
            ready = ma_alignment_1h == "BEARISH" and ma_alignment_15m == "BEARISH" and 0 < current_price <= ma5_15m and short_tf_ok

        if not ready:
            return False, ""

        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        return True, f"吸筹暗流确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%，OI {oi_change:+.1f}%"

    def _apply_entry_confirmation(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Convert raw signal into watch/ready/invalid states."""
        signal["entry_status"] = "ready"
        signal["entry_status_text"] = "确认入场"
        signal["entry_note"] = ""
        if not self.config.entry_confirmation_enabled:
            return signal

        symbol = signal["symbol"]
        direction = signal["direction"]
        current_price = float(signal.get("price", 0) or 0)
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        required_pullback = self._required_pullback_pct(signal.get("metrics", {}) or {}, score_total)
        now = time.time()

        watch = self._entry_watchlist.get(symbol)
        if watch and watch.get("direction") != direction:
            self._entry_watchlist.pop(symbol, None)
            signal["entry_status"] = "invalid"
            signal["entry_status_text"] = "失效淘汰"
            signal["entry_note"] = "方向反转"
            return signal

        if watch and score_total > 0:
            previous_score = float(watch.get("score_total", 0) or 0)
            threshold = max(40.0, previous_score * 0.7) if previous_score > 0 else 40.0
            if score_total < threshold:
                self._entry_watchlist.pop(symbol, None)
                signal["entry_status"] = "invalid"
                signal["entry_status_text"] = "失效淘汰"
                signal["entry_note"] = f"评分回落至 {score_total:.1f}"
                return signal

        if not watch:
            strategy_line = self._strategy_line_for_signal(signal)
            initial_note = "首次发现，等待回踩确认"
            trend = self._load_confirmation_trend(symbol)
            continuation_ready, continuation_note = self._is_trend_continuation_ready(signal, trend, current_price)
            momentum_ready, momentum_note = self._is_momentum_entry_ready(signal, trend, current_price)
            accumulation_ready, accumulation_note = self._is_accumulation_entry_ready(signal, trend, current_price)
            if strategy_line == "趋势突破线":
                initial_note = "首次发现，等待趋势延续确认"
            if continuation_ready or momentum_ready or accumulation_ready:
                signal["entry_status"] = "ready"
                signal["entry_status_text"] = "突破确认入场"
                signal["strategy_line"] = "趋势突破线"
                signal["watch_stage"] = "首发现直通"
                signal["entry_note"] = accumulation_note or momentum_note or continuation_note
                signal["confirmation_trend"] = trend
                return signal

            self._entry_watchlist[symbol] = {
                "symbol": symbol,
                "direction": direction,
                "stage": signal.get("stage", ""),
                "first_seen_ts": now,
                "last_seen_ts": now,
                "first_price": current_price,
                "highest_price": current_price,
                "lowest_price": current_price,
                "score_total": score_total,
                "pullback_seen": False,
                "strategy_line": strategy_line,
                "watch_stage": "首发现",
                "required_pullback_pct": required_pullback,
                "current_pullback_pct": 0.0,
                "entry_note": initial_note,
                "price": current_price,
                "metrics": signal.get("metrics", {}),
                "score": signal.get("score"),
            }
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["strategy_line"] = self._entry_watchlist[symbol]["strategy_line"]
            signal["watch_stage"] = "首发现"
            signal["entry_note"] = initial_note
            return signal

        watch["last_seen_ts"] = now
        watch["stage"] = signal.get("stage", watch.get("stage", ""))
        watch["score_total"] = score_total or float(watch.get("score_total", 0) or 0)
        watch["price"] = current_price
        watch["metrics"] = signal.get("metrics", {})
        watch["score"] = signal.get("score")
        watch["strategy_line"] = self._strategy_line_for_signal(signal)
        if current_price > 0:
            watch["highest_price"] = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            low_seed = float(watch.get("lowest_price", current_price) or current_price)
            watch["lowest_price"] = current_price if low_seed <= 0 else min(low_seed, current_price)

        pullback_pct = self._current_pullback_pct(watch, direction, current_price)

        if pullback_pct >= required_pullback:
            watch["pullback_seen"] = True

        if not watch.get("pullback_seen"):
            trend = self._load_confirmation_trend(symbol)
            momentum_ready, momentum_note = self._is_momentum_entry_ready(signal, trend, current_price)
            accumulation_ready, accumulation_note = self._is_accumulation_entry_ready(signal, trend, current_price)
            if momentum_ready:
                signal["entry_status"] = "ready"
                signal["entry_status_text"] = "动量确认入场"
                signal["strategy_line"] = "趋势突破线"
                signal["watch_stage"] = "动量突破"
                signal["entry_note"] = momentum_note
                signal["confirmation_trend"] = trend
                return signal

            if accumulation_ready:
                signal["entry_status"] = "ready"
                signal["entry_status_text"] = "吸筹暗流入场"
                signal["strategy_line"] = "趋势突破线"
                signal["watch_stage"] = "吸筹启动"
                signal["entry_note"] = accumulation_note
                signal["confirmation_trend"] = trend
                return signal

            if watch.get("strategy_line") == "趋势突破线":
                continuation_ready, continuation_note = self._is_trend_continuation_ready(signal, trend, current_price)
                if continuation_ready:
                    signal["entry_status"] = "ready"
                    signal["entry_status_text"] = "突破确认入场"
                    signal["strategy_line"] = "趋势突破线"
                    signal["watch_stage"] = "趋势延续"
                    signal["entry_note"] = continuation_note
                    signal["confirmation_trend"] = trend
                    return signal

            stage_name = "趋势待命" if watch.get("strategy_line") == "趋势突破线" else "回踩等待"
            self._update_watch_state(
                watch,
                strategy_line=watch.get("strategy_line", "回踩确认线"),
                stage_name=stage_name,
                entry_note=(
                    "等待趋势延续确认"
                    if watch.get("strategy_line") == "趋势突破线"
                    else f"等待至少 {required_pullback:.1f}% 回踩"
                ),
                required_pullback=required_pullback,
                current_pullback=pullback_pct,
                trend=trend,
            )
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["strategy_line"] = watch.get("strategy_line", "回踩确认线")
            signal["watch_stage"] = stage_name
            signal["entry_note"] = (
                "等待趋势延续确认"
                if watch.get("strategy_line") == "趋势突破线"
                else f"等待至少 {required_pullback:.1f}% 回踩"
            )
            return signal

        trend = self._load_confirmation_trend(symbol)
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5 = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        higher_alignment = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)
        volume_ok = self._volume_reclaim_ready(trend)
        trend_ok = False
        if direction == "LONG":
            trend_ok = higher_alignment == "BULLISH" and ma_alignment == "BULLISH" and current_price >= ma5 > 0 and short_tf_ok
        else:
            trend_ok = higher_alignment == "BEARISH" and ma_alignment == "BEARISH" and 0 < current_price <= ma5 and short_tf_ok

        flow_ready, flow_note = self._is_flow_reclaim_ready(signal, trend, current_price, pullback_pct)
        if flow_ready:
            signal["entry_status"] = "ready"
            signal["entry_status_text"] = "快线确认入场"
            signal["strategy_line"] = watch.get("strategy_line", "回踩确认线")
            signal["watch_stage"] = "资金OI快线"
            signal["entry_note"] = flow_note
            signal["confirmation_trend"] = trend
            return signal

        if not trend_ok:
            self._update_watch_state(
                watch,
                strategy_line=watch.get("strategy_line", "回踩确认线"),
                stage_name="均线确认",
                entry_note="已回踩，等待 15m 重站均线",
                required_pullback=required_pullback,
                current_pullback=pullback_pct,
                trend=trend,
            )
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["strategy_line"] = watch.get("strategy_line", "回踩确认线")
            signal["watch_stage"] = "均线确认"
            signal["entry_note"] = "已回踩，等待 15m 重站均线"
            return signal

        if watch.get("strategy_line") == "回踩确认线" and not volume_ok:
            self._update_watch_state(
                watch,
                strategy_line=watch.get("strategy_line", "回踩确认线"),
                stage_name="量能回归",
                entry_note=f"已回踩，等待 5m 量能回归 ≥ {self.config.reclaim_volume_ratio:.2f}",
                required_pullback=required_pullback,
                current_pullback=pullback_pct,
                trend=trend,
            )
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["strategy_line"] = watch.get("strategy_line", "回踩确认线")
            signal["watch_stage"] = "量能回归"
            signal["entry_note"] = f"已回踩，等待 5m 量能回归 ≥ {self.config.reclaim_volume_ratio:.2f}"
            return signal

        signal["entry_status"] = "ready"
        signal["entry_status_text"] = "确认入场"
        signal["strategy_line"] = watch.get("strategy_line", "回踩确认线")
        signal["watch_stage"] = "触发入场"
        signal["entry_note"] = f"回踩 {pullback_pct:.2f}% 后重站 15m 均线"
        signal["confirmation_trend"] = trend
        return signal

    def run(self):
        """主循环 - 诸神黄昏的永恒之战"""
        mode_text = f"{self.config.mode_emoji} {self.config.mode_name} 模式"
        logger.info("=" * 60)
        logger.info(f"⚔️  {mode_text} 启动")
        logger.info(f"🔧 杠杆：{self.config.leverage}x | 风险：{self.config.risk_per_trade_pct}%")
        logger.info(
            f"🛡️  止损：{self.config.stop_loss_pct}% | 止盈：{self.config.take_profit_pct}% "
            f"({self.config.take_profit_mode})"
        )
        logger.info(f"👁️  扫描：前{self.config.scan_top_n}币种 | 间隔：{self.config.scan_interval_sec}s")
        logger.info(f"📈 最大持仓：{self.config.max_open_positions} 个")
        logger.info("=" * 60)

        try:
            self.day_start_balance = self._run_health_checks()
            self._start_market_ticker_stream()
            self._start_user_data_stream()
            self._start_background_protection_audit(source="startup_audit")
            logger.info(f"🩺 启动健康检查通过 | 可用余额: ${self.day_start_balance:.2f}")
        except Exception as e:
            logger.error(f"❌ 启动健康检查失败：{e}")
            send_telegram_message(
                format_error_msg(
                    error_type="启动健康检查失败",
                    message=str(e),
                    component="startup_checks",
                )
            )
            raise

        # 发送启动通知
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

        # 🌊 获取市场概览（Surf 数据增强）
        self._refresh_market_profile()

        # 主循环
        while self.running:
            try:
                self.run_scan_cycle()
                sleep_sec = max(10, min(self.config.fast_scan_interval_sec, self._current_scan_interval))
                logger.info(f"⏳ 等待 {sleep_sec}s 后下次扫描...")
                time.sleep(sleep_sec)
            except KeyboardInterrupt:
                logger.info("\n🛑 用户中断，正在停止...")
                self.running = False
            except Exception as e:
                logger.error(f"❌ 循环错误：{e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="主循环异常",
                        message=str(e),
                    )
                )
                time.sleep(10)

        # 停止通知
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


# ═══════════════════════════════════════════════════════════════
# CLI 入口 - 英灵殿的大门
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="🗡️  CRYPTO SWORD - 诸神黄昏之剑",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  crypto-sword                        # 默认实盘基线参数
  crypto-sword --leverage 10          # 实盘 + 10x 杠杆
        """,
    )

    # 实盘模式（默认开启）
    parser.add_argument("--live", action="store_true", default=True, help="实盘模式（默认开启，⚠️ 真实资金）")

    # 杠杆 - 奥丁的长矛
    parser.add_argument("--leverage", "-l", type=int, default=5, choices=range(1, 11),
                        metavar="1-10", help="杠杆倍数 (1-10x, 默认：5x)")

    # 风控 - 英灵殿的盾牌
    parser.add_argument("--risk", "-r", type=float, default=1.0, help="每笔风险 %% (默认：1%%)")
    parser.add_argument("--stop-loss", "-s", type=float, default=8.0, help="止损 %% (默认：8%%)")
    parser.add_argument("--take-profit", "-t", type=float, default=20.0, help="止盈 %% (默认：20%%)")
    parser.add_argument(
        "--take-profit-mode",
        choices=["price", "roi"],
        default="roi",
        help="止盈百分比口径：roi=杠杆后收益率，price=标的价格涨跌幅 (默认：roi)",
    )
    parser.add_argument("--max-positions", "-m", type=int, default=3, help="最大持仓数 (默认：3)")
    parser.add_argument("--max-daily-loss", type=float, default=5.0, help="每日最大亏损 %% (默认：5%%)")

    # 扫描 - 弗丽嘉的鹰眼
    parser.add_argument("--top", type=int, default=30, help="扫描前 N 个币种 (默认：30)")
    parser.add_argument("--interval", "-i", type=int, default=300, help="扫描间隔秒数 (默认：300)")
    parser.add_argument("--scan-workers", type=int, default=6, help="深度扫描并发数 (默认：6)")
    parser.add_argument("--min-change", type=float, default=3.0, help="最小涨幅 %% (默认：3%%)")
    parser.add_argument("--min-pullback", type=float, default=3.0, help="普通信号最小回踩 %% (默认：3%%)")
    parser.add_argument("--reclaim-volume", type=float, default=1.15, help="回踩线 5m 量能回归倍数 (默认：1.15)")
    parser.add_argument("--by-volume", action="store_true", help="按成交量排序（默认按涨幅）")
    parser.add_argument("--no-entry-confirm", action="store_true", help="禁用回踩确认入场")
    parser.add_argument("--entry-confirm-timeout", type=int, default=1800, help="候选观察超时秒数")
    parser.add_argument("--no-momentum-entry", action="store_true", help="禁用强趋势动量确认入场")
    parser.add_argument("--momentum-score", type=float, default=68.0, help="动量入场最低评分 (默认：68)")
    parser.add_argument("--accumulation-score", type=float, default=58.0, help="吸筹暗流最低评分 (默认：58)")
    parser.add_argument("--accumulation-min-oi", type=float, default=18.0, help="吸筹暗流最小 OI 变化%% (默认：18)")
    parser.add_argument("--accumulation-max-change", type=float, default=10.0, help="吸筹暗流最大涨跌幅%% (默认：10)")
    parser.add_argument("--max-consecutive-losses", type=int, default=3, help="连续亏损熔断笔数 (默认：3)")
    parser.add_argument("--loss-pause-mins", type=int, default=30, help="连续亏损后暂停分钟 (默认：30)")
    parser.add_argument("--no-daily-report", action="store_true", help="禁用每日复盘通知")

    # 追踪止损 - 海姆达尔的守望
    parser.add_argument("--trailing", type=float, default=5.0, help="追踪止损 %% (默认：5%%)")
    parser.add_argument("--no-trailing", action="store_true", help="禁用追踪止损")

    args = parser.parse_args()

    # 实盘模式确认
    mode = "live"
    print("\n" + "=" * 50)
    print("⚠️  ⚠️  ⚠️  实盘交易警告  ⚠️  ⚠️  ⚠️")
    print("=" * 50)
    print("\n即将使用 REAL MONEY 进行交易！")
    print(f"杠杆：{args.leverage}x | 风险：{args.risk}% | 止损：{args.stop_loss}%")
    print("\n确认继续？输入 'y' 继续，其他键取消：")
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
        min_pullback_pct=max(0.5, args.min_pullback),
        reclaim_volume_ratio=max(0.8, args.reclaim_volume),
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
