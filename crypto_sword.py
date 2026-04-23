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
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

# Add scripts to path
sys.path.insert(0, str(Path("/root/.hermes/scripts")))

# ═══════════════════════════════════════════════════════════════
# 导入模块
# ═══════════════════════════════════════════════════════════════

try:
    from binance_breakout_scanner import (
        get_top_symbols_by_volume,
        get_top_symbols_by_change,
        scan_symbols,
        classify_and_direction,
        fetch_ticker_24hr,
    )
    from binance_trading_executor import (
        TradingSignal,
        cancel_protective_order,
        cancel_stop_loss_order,
        execute_trade,
        fetch_open_algo_orders,
        fetch_open_orders,
        get_account_balance,
        is_native_binance_configured,
        place_market_order,
        place_stop_loss_order,
        place_take_profit_order,
        should_trade,
        OrderResult,
    )
    from telegram_notifier import (
        format_close_position_msg,
        format_daily_report_msg,
        format_error_msg,
        format_latency_alert_msg,
        format_partial_take_profit_msg,
        format_protection_status_msg,
        format_scan_monitor_msg,
        format_shutdown_msg,
        format_summary_msg,
        format_startup_msg,
        get_telegram_config,
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
        get_klines,
        score_signal,
        SignalScore,
    )
    from risk_manager import (  # 🛡️ 风控系统
        assess_trade_risk,
        RiskConfig,
        calculate_position_size,
    )
    try:
        from binance_websocket import (
            BinanceAllMarketTickerWebSocketClient,
            BinanceUserDataWebSocketClient,
            BinanceWebSocketClient,
        )
    except Exception:
        BinanceAllMarketTickerWebSocketClient = None
        BinanceUserDataWebSocketClient = None
        BinanceWebSocketClient = None
except ImportError as e:
    print(f"❌ 导入失败：{e}")
    print("请确保所有脚本位于 /root/.hermes/scripts/")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 日志配置
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/root/.hermes/logs/crypto_sword.log"),
    ],
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 配置类 - 雷神之锤的参数
# ═══════════════════════════════════════════════════════════════

class TradingConfig:
    """交易配置 - 诸神黄昏之剑的灵魂"""

    def __init__(
        self,
        # 模式（强制实盘）
        mode: str = "live",
        # 杠杆 - 奥丁的长矛
        leverage: int = 5,
        # 风控 - 英灵殿的盾牌
        risk_per_trade_pct: float = 0.5,
        stop_loss_pct: float = 8.0,
        take_profit_pct: float = 20.0,
        take_profit_mode: str = "roi",
        max_position_pct: float = 20.0,
        max_daily_loss_pct: float = 5.0,
        max_open_positions: int = 5,
        # 追踪止损 - 海姆达尔的守望
        trailing_stop_pct: float = 5.0,
        trailing_stop_enabled: bool = True,
        # 扫描 - 弗丽嘉的鹰眼
        scan_top_n: int = 50,
        scan_interval_sec: int = 300,
        fast_scan_interval_sec: int = 60,
        scan_workers: int = 6,
        min_stage: str = "pre_break",
        scan_by_change: bool = True,
        min_change_pct: float = 3.0,
        max_chase_change_pct: float = 25.0,
        min_pullback_pct: float = 3.0,
        max_range_position_pct: float = 88.0,
        max_abs_funding_rate: float = 0.003,
        max_oi_change_pct: float = 120.0,
        max_entry_slippage_pct: float = 0.8,
        symbol_cooldown_sec: int = 24 * 3600,
        max_consecutive_losses: int = 2,
        loss_pause_sec: int = 60 * 60,
        breakeven_after_tp: bool = True,
        breakeven_offset_pct: float = 0.10,
        entry_confirmation_enabled: bool = True,
        entry_confirmation_timeout_sec: int = 30 * 60,
        daily_report_enabled: bool = True,
        daily_report_on_first_cycle: bool = True,
        # 目标 - 矮人锻造的利刃
        target_altcoins: bool = True,
        target_memes: bool = True,
    ):
        self.mode = mode
        self.leverage = leverage
        self.risk_per_trade_pct = risk_per_trade_pct
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.take_profit_mode = take_profit_mode
        self.max_position_pct = max_position_pct
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_open_positions = max_open_positions
        self.trailing_stop_pct = trailing_stop_pct
        self.trailing_stop_enabled = trailing_stop_enabled
        self.scan_top_n = scan_top_n
        self.scan_interval_sec = scan_interval_sec
        self.fast_scan_interval_sec = fast_scan_interval_sec
        self.scan_workers = scan_workers
        self.min_stage = min_stage
        self.scan_by_change = scan_by_change
        self.min_change_pct = min_change_pct
        self.max_chase_change_pct = max_chase_change_pct
        self.min_pullback_pct = min_pullback_pct
        self.max_range_position_pct = max_range_position_pct
        self.max_abs_funding_rate = max_abs_funding_rate
        self.max_oi_change_pct = max_oi_change_pct
        self.max_entry_slippage_pct = max_entry_slippage_pct
        self.symbol_cooldown_sec = symbol_cooldown_sec
        self.max_consecutive_losses = max_consecutive_losses
        self.loss_pause_sec = loss_pause_sec
        self.breakeven_after_tp = breakeven_after_tp
        self.breakeven_offset_pct = breakeven_offset_pct
        self.entry_confirmation_enabled = entry_confirmation_enabled
        self.entry_confirmation_timeout_sec = entry_confirmation_timeout_sec
        self.daily_report_enabled = daily_report_enabled
        self.daily_report_on_first_cycle = daily_report_on_first_cycle
        self.target_altcoins = target_altcoins
        self.target_memes = target_memes

    @property
    def mode_emoji(self) -> str:
        return "💰"

    @property
    def mode_name(self) -> str:
        return "实盘"


# ═══════════════════════════════════════════════════════════════
# 持仓跟踪 - 瓦尔基里的记录
# ═══════════════════════════════════════════════════════════════

class Position:
    """持仓信息 - 英灵战士的荣耀"""

    def __init__(
        self,
        symbol: str,
        side: str,  # BUY (long) or SELL (short)
        entry_price: float,
        quantity: float,
        order_id: int,
        stop_loss_price: float,
        take_profit_price: float,
        entry_time: datetime,
        stage_at_entry: str,
        stop_loss_order_id: int = 0,
        session_id: str = "",
        target_roi_pct: float = 0.0,
        take_profit_targets: Optional[List[dict[str, Any]]] = None,
        take_profit_order_ids: Optional[List[int]] = None,
    ):
        self.symbol = symbol
        self.side = side
        self.entry_price = entry_price
        self.quantity = quantity
        self.order_id = order_id
        self.stop_loss_price = stop_loss_price
        self.take_profit_price = take_profit_price
        self.entry_time = entry_time
        self.stage_at_entry = stage_at_entry
        self.stop_loss_order_id = stop_loss_order_id
        self.session_id = session_id
        self.target_roi_pct = target_roi_pct
        self.take_profit_targets = take_profit_targets or []
        self.take_profit_order_ids = take_profit_order_ids or []
        self.initial_quantity = quantity
        self.last_synced_quantity = quantity
        self.partial_tp_count = 0
        self.protection_failures = 0
        self.last_protection_error = ""

        # 动态跟踪
        self.highest_price: float = entry_price
        self.lowest_price: float = entry_price
        self.current_stop: float = stop_loss_price
        self.exit_price: Optional[float] = None
        self.exit_time: Optional[datetime] = None
        self.exit_reason: Optional[str] = None
        self.pnl: float = 0.0
        self.pnl_pct: float = 0.0

    def update_price(self, current_price: float, trailing_stop_pct: float):
        """更新价格并计算追踪止损"""
        if self.side == "BUY":  # Long
            if current_price > self.highest_price:
                self.highest_price = current_price
                if self.current_stop < self.highest_price * (1 - trailing_stop_pct / 100):
                    self.current_stop = self.highest_price * (1 - trailing_stop_pct / 100)
        else:  # Short
            if current_price < self.lowest_price:
                self.lowest_price = current_price
                if self.current_stop > self.lowest_price * (1 + trailing_stop_pct / 100):
                    self.current_stop = self.lowest_price * (1 + trailing_stop_pct / 100)

        # 计算未实现盈亏
        if self.side == "BUY":
            self.pnl = (current_price - self.entry_price) * self.quantity
            self.pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
        else:
            self.pnl = (self.entry_price - current_price) * self.quantity
            self.pnl_pct = (self.entry_price - current_price) / self.entry_price * 100

    def check_exit_conditions(self, current_price: float) -> Optional[str]:
        """检查平仓条件"""
        if self.side == "BUY":
            if current_price <= self.current_stop:
                return "STOP_LOSS"
            if not self.take_profit_order_ids and current_price >= self.take_profit_price:
                return "TAKE_PROFIT"
        else:
            if current_price >= self.current_stop:
                return "STOP_LOSS"
            if not self.take_profit_order_ids and current_price <= self.take_profit_price:
                return "TAKE_PROFIT"
        return None

    def _format_take_profit_targets_text(self) -> str:
        if not self.take_profit_targets:
            return f"${self.take_profit_price:,.4f}"

        parts = []
        for target in self.take_profit_targets:
            roi_pct = float(target.get("target_roi_pct", 0) or 0)
            price = float(target.get("price", 0) or 0)
            parts.append(f"{roi_pct:.0f}%→${price:,.4f}")
        return " | ".join(parts)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": "LONG" if self.side == "BUY" else "SHORT",
            "entry_price": self.entry_price,
            "current_price": round(self.entry_price * (1 + self.pnl_pct / 100), 4) if self.side == "BUY" else round(self.entry_price * (1 - self.pnl_pct / 100), 4),
            "quantity": self.quantity,
            "entry_time": self.entry_time.isoformat(),
            "stop_loss": round(self.current_stop, 4),
            "take_profit": round(self.take_profit_price, 4),
            "target_roi_pct": round(self.target_roi_pct, 2),
            "take_profit_targets": self.take_profit_targets,
            "take_profit_targets_text": self._format_take_profit_targets_text(),
            "highest": round(self.highest_price, 4),
            "lowest": round(self.lowest_price, 4),
            "unrealized_pnl": round(self.pnl, 2),
            "unrealized_pnl_pct": round(self.pnl_pct, 2),
            "session_id": self.session_id,
        }


class PositionTracker:
    """持仓跟踪器 - 海姆达尔的守望"""

    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []

    def add_position(self, position: Position):
        self.positions[position.symbol] = position
        logger.info(f"📊 开仓：{position.symbol} {position.side} @ ${position.entry_price}")

    def remove_position(self, symbol: str):
        if symbol in self.positions:
            pos = self.positions.pop(symbol)
            self.closed_positions.append(pos)
            logger.info(f"✅ 平仓：{pos.symbol} | PnL: ${pos.pnl:.2f} ({pos.pnl_pct:.2f}%) | 原因：{pos.exit_reason}")

    def get_position(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)

    def get_open_count(self) -> int:
        return len(self.positions)

    def update_all_prices(self, prices: Dict[str, float], trailing_stop_pct: float):
        for symbol, position in self.positions.items():
            if symbol in prices:
                position.update_price(prices[symbol], trailing_stop_pct)

    def check_all_exits(self, prices: Dict[str, float]) -> Dict[str, str]:
        exits = {}
        for symbol, position in self.positions.items():
            if symbol in prices:
                reason = position.check_exit_conditions(prices[symbol])
                if reason:
                    exits[symbol] = reason
        return exits

    def get_summary(self) -> dict:
        total_pnl = sum(p.pnl for p in self.positions.values())
        return {
            "open_positions": len(self.positions),
            "total_unrealized_pnl": round(total_pnl, 2),
            "positions": [p.to_dict() for p in self.positions.values()],
            "closed_today": len(self.closed_positions),
            "realized_pnl": round(sum(p.pnl for p in self.closed_positions), 2),
        }

    def reset_daily_summary(self):
        self.closed_positions = []


# ═══════════════════════════════════════════════════════════════
# 主交易引擎 - 诸神黄昏之剑
# ═══════════════════════════════════════════════════════════════

class CryptoSword:
    """
    🗡️ CRYPTO SWORD - 诸神黄昏之剑

    监控与交易的化身，于测试之荒原与实战的腥风血雨间切换
    捕捉山寨与 meme 的血腥气息，执行雷霆般的杀伐
    """

    def __init__(self, config: TradingConfig):
        self.config = config
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

    def _new_session_id(self, symbol: str) -> str:
        return f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _record_latency_step(self, steps: list[tuple[str, float]], name: str, started_at: float):
        steps.append((name, (time.perf_counter() - started_at) * 1000.0))

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
            send_telegram_message(
                format_latency_alert_msg(
                    flow=flow,
                    symbol=symbol,
                    total_ms=total_ms,
                    steps=steps,
                    threshold_ms=threshold,
                )
            )

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

    def _build_take_profit_plan(self) -> tuple[list[float], list[float]]:
        """Build default staged take-profit plan around the configured TP percentage."""
        base_pct = max(float(self.config.take_profit_pct) * self._tp_multiplier, 0.0)
        if base_pct <= 0:
            return [0.0], [1.0]

        staged_levels = []
        for multiplier in (0.5, 1.0, 1.5):
            target_pct = round(base_pct * multiplier, 2)
            if target_pct > 0 and target_pct not in staged_levels:
                staged_levels.append(target_pct)

        ratios = [0.5, 0.3, 0.2][:len(staged_levels)]
        ratio_total = sum(ratios) or 1.0
        ratios = [ratio / ratio_total for ratio in ratios]
        return staged_levels, ratios

    def _calculate_local_take_profit_price(self, entry_price: float, side: str, target_pct: float) -> float:
        if self.config.take_profit_mode == "roi":
            price_move_pct = target_pct / max(self.config.leverage, 1)
        else:
            price_move_pct = target_pct
        if side == "BUY":
            return entry_price * (1 + price_move_pct / 100.0)
        return entry_price * (1 - price_move_pct / 100.0)

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
        trend_1h = analyze_trend(get_klines(symbol, interval="1h", limit=50) or [])
        trend_15m = analyze_trend(get_klines(symbol, interval="15m", limit=50) or [])
        return {
            "1h": trend_1h,
            "15m": trend_15m,
        }

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
            }
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["entry_note"] = "首次发现，等待回踩确认"
            return signal

        watch["last_seen_ts"] = now
        watch["stage"] = signal.get("stage", watch.get("stage", ""))
        watch["score_total"] = score_total or float(watch.get("score_total", 0) or 0)
        if current_price > 0:
            watch["highest_price"] = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            low_seed = float(watch.get("lowest_price", current_price) or current_price)
            watch["lowest_price"] = current_price if low_seed <= 0 else min(low_seed, current_price)

        if direction == "LONG":
            anchor_price = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            pullback_pct = ((anchor_price - current_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0
        else:
            anchor_price = min(float(watch.get("lowest_price", current_price) or current_price), current_price)
            pullback_pct = ((current_price - anchor_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0

        if pullback_pct >= self.config.min_pullback_pct:
            watch["pullback_seen"] = True

        if not watch.get("pullback_seen"):
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["entry_note"] = f"等待至少 {self.config.min_pullback_pct:.1f}% 回踩"
            return signal

        trend = self._load_confirmation_trend(symbol)
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5 = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        higher_alignment = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        trend_ok = False
        if direction == "LONG":
            trend_ok = higher_alignment == "BULLISH" and ma_alignment == "BULLISH" and current_price >= ma5 > 0
        else:
            trend_ok = higher_alignment == "BEARISH" and ma_alignment == "BEARISH" and 0 < current_price <= ma5

        if not trend_ok:
            signal["entry_status"] = "watch"
            signal["entry_status_text"] = "观察中"
            signal["entry_note"] = "已回踩，等待 15m 重站均线"
            return signal

        self._entry_watchlist.pop(symbol, None)
        signal["entry_status"] = "ready"
        signal["entry_status_text"] = "确认入场"
        signal["entry_note"] = f"回踩 {pullback_pct:.2f}% 后重站 15m 均线"
        signal["confirmation_trend"] = trend
        return signal

    def _send_daily_report_if_due(self):
        if not self.config.daily_report_enabled or not self.config.daily_report_on_first_cycle:
            return

        today = datetime.now().date().isoformat()
        if self._last_daily_report_sent_for == today:
            return

        report_date = (datetime.now().date() - timedelta(days=1)).isoformat()
        report = self.db.get_daily_report(report_date=report_date, mode=self.config.mode)
        send_telegram_message(format_daily_report_msg(report))
        self._last_daily_report_sent_for = today

    def _select_deep_scan_symbols(self) -> list[str]:
        """Pick symbols for expensive deep scan, preferring fresh fast-scan candidates."""
        candidates = self._fast_scan_candidates()
        if candidates:
            return candidates[: self.config.scan_top_n]

        if self.config.scan_by_change:
            symbols = get_top_symbols_by_change(
                self.config.scan_top_n,
                min_change=self.config.min_change_pct,
            )
            logger.info(f"🔥 妖币模式(REST) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
            return symbols

        symbols = get_top_symbols_by_volume(self.config.scan_top_n)
        logger.info(f"📊 成交量模式 - 扫描 {len(symbols)} 个币种：{symbols[:5]}...")
        return symbols

    def _refresh_price_stream(self, symbols: list[str]):
        """Keep a lightweight WebSocket price stream for open positions."""
        if BinanceWebSocketClient is None:
            return

        symbol_set = {symbol.upper() for symbol in symbols if symbol}
        now = time.time()
        if symbol_set == self._ws_symbols and self._ws_client and now - self._ws_last_refresh < 300:
            return

        if self._ws_client:
            try:
                self._ws_client.stop()
            except Exception:
                pass
            self._ws_client = None

        self._ws_symbols = symbol_set
        self._ws_last_refresh = now
        if not symbol_set:
            return

        try:
            self._ws_client = BinanceWebSocketClient(
                sorted(symbol_set),
                stream_types=["mark_price"],
            )
            self._ws_client.start()
            logger.info(f"📡 WebSocket 实时价格监听已启动：{', '.join(sorted(symbol_set))}")
        except Exception as e:
            self._ws_client = None
            logger.warning(f"📡 WebSocket 启动失败，继续使用 REST 价格：{e}")

    def _get_ws_price(self, symbol: str) -> float:
        if not self._ws_client:
            return 0.0
        try:
            return float(self._ws_client.get_price(symbol, max_age_sec=10))
        except Exception:
            return 0.0
        return 0.0

    def _start_user_data_stream(self):
        """Start private WebSocket for order/account state updates."""
        if BinanceUserDataWebSocketClient is None:
            logger.warning("User data WebSocket unavailable; continuing with REST reconciliation")
            return
        if self._user_ws_client:
            return

        try:
            self._user_ws_client = BinanceUserDataWebSocketClient(
                callbacks={
                    "on_order_update": self._handle_ws_order_update,
                    "on_account_update": self._handle_ws_account_update,
                    "on_algo_update": self._handle_ws_algo_update,
                }
            )
            self._user_ws_client.start()
            logger.info("Binance user data WebSocket started: realtime order/account sync")
        except Exception as e:
            self._user_ws_client = None
            logger.warning(f"Binance user data WebSocket start failed; REST sync remains active: {e}")

    def _request_state_sync_from_ws(self, reason: str, symbol: str = ""):
        """Debounced REST reconciliation triggered by private WebSocket events."""
        now = time.time()
        if now - self._last_user_stream_sync < 2.0:
            return
        self._last_user_stream_sync = now
        logger.info(f"WS state sync requested: {reason}{f' | {symbol}' if symbol else ''}")
        try:
            with self._state_lock:
                self._sync_positions_with_exchange()
        except Exception as e:
            logger.warning(f"WS state sync failed: {e}")

    def _handle_ws_order_update(self, event: dict[str, Any]):
        """React to user stream order updates."""
        order = event.get("o", {}) if isinstance(event, dict) else {}
        symbol = str(order.get("s", "") or "")
        status = str(order.get("X", "") or "")
        execution_type = str(order.get("x", "") or "")
        order_type = str(order.get("o", "") or "")
        realized_pnl = float(order.get("rp", 0) or 0)

        if status in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"} or execution_type == "TRADE":
            logger.info(
                f"WS order update: {symbol} {order_type} {execution_type}/{status} "
                f"filled={order.get('z', '0')} price={order.get('L', '0')} rp={realized_pnl:.4f}"
            )
            self._request_state_sync_from_ws(f"{execution_type}/{status}", symbol)

    def _handle_ws_account_update(self, event: dict[str, Any]):
        """React to account/position updates from user stream."""
        account = event.get("a", {}) if isinstance(event, dict) else {}
        positions = account.get("P", []) or []
        changed_symbols = [str(pos.get("s", "")) for pos in positions if pos.get("s")]
        if changed_symbols:
            logger.info(f"WS position update: {', '.join(changed_symbols[:8])}")
        self._request_state_sync_from_ws("ACCOUNT_UPDATE", changed_symbols[0] if changed_symbols else "")

    def _handle_ws_algo_update(self, event: dict[str, Any]):
        """React to conditional/algo order updates from user stream."""
        symbol = str(event.get("s", event.get("symbol", "")) or "")
        event_type = str(event.get("e", "") or "ALGO_UPDATE")
        logger.info(f"WS algo update: {event_type}{f' | {symbol}' if symbol else ''}")
        self._request_state_sync_from_ws(event_type, symbol)

    def _parse_trade_notes(self, notes: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for note in (notes or "").split(";"):
            if "=" not in note:
                continue
            key, value = note.split("=", 1)
            parsed[key] = value
        return parsed

    def _cancel_position_protection(self, position: Position):
        if position.stop_loss_order_id:
            if cancel_stop_loss_order(position.symbol, position.stop_loss_order_id):
                logger.info(f"🔕 已撤销 {position.symbol} 保护止损单：{position.stop_loss_order_id}")
            else:
                logger.warning(f"⚠️ {position.symbol} 保护止损单撤销失败：{position.stop_loss_order_id}")
                send_telegram_message(
                    format_error_msg(
                        error_type="止损单撤销失败",
                        message=f"order_id={position.stop_loss_order_id}",
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="stop_loss_cleanup",
                    )
                )

        for order_id in position.take_profit_order_ids:
            if not order_id:
                continue
            if cancel_protective_order(position.symbol, order_id):
                logger.info(f"🔕 已撤销 {position.symbol} 止盈委托：{order_id}")
            else:
                logger.warning(f"⚠️ {position.symbol} 止盈委托撤销失败：{order_id}")

    def _record_closed_trade_result(self, position: Position, pnl: float):
        """Update cooldown and consecutive-loss guards from a closed trade."""
        now = time.time()
        if pnl < 0:
            self._consecutive_losses += 1
            self._symbol_cooldowns[position.symbol] = now + self.config.symbol_cooldown_sec
            logger.warning(
                f"🧊 {position.symbol} 亏损冷却 {int(self.config.symbol_cooldown_sec / 60)} 分钟 | "
                f"连续亏损={self._consecutive_losses}"
            )
            if self._consecutive_losses >= self.config.max_consecutive_losses:
                self._loss_pause_until = now + self.config.loss_pause_sec
                logger.warning(
                    f"🛑 连续亏损达到 {self._consecutive_losses} 笔，暂停新开仓 "
                    f"{int(self.config.loss_pause_sec / 60)} 分钟"
                )
                send_telegram_message(
                    format_error_msg(
                        error_type="连续亏损熔断",
                        message=(
                            f"连续亏损 {self._consecutive_losses} 笔，暂停新开仓 "
                            f"{int(self.config.loss_pause_sec / 60)} 分钟"
                        ),
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="loss_guard",
                    )
                )
        else:
            self._consecutive_losses = 0

    def _move_stop_to_breakeven(self, position: Position, remaining_qty: float) -> bool:
        """After first TP, move stop loss near breakeven so winners do not turn red."""
        if not self.config.breakeven_after_tp or remaining_qty <= 0:
            return False

        if position.side == "BUY":
            breakeven_price = position.entry_price * (1 + self.config.breakeven_offset_pct / 100.0)
            close_side = "SELL"
            position_side = "LONG"
            if position.current_stop >= breakeven_price:
                return True
        else:
            breakeven_price = position.entry_price * (1 - self.config.breakeven_offset_pct / 100.0)
            close_side = "BUY"
            position_side = "SHORT"
            if position.current_stop <= breakeven_price:
                return True

        old_order_id = position.stop_loss_order_id
        if old_order_id and not cancel_stop_loss_order(position.symbol, old_order_id):
            logger.warning(f"⚠️ {position.symbol} 保本止损移动失败：旧止损撤销失败 {old_order_id}")
            return False

        sl_result = place_stop_loss_order(
            position.symbol,
            close_side,
            remaining_qty,
            breakeven_price,
            position_side=position_side,
            reduce_only=True,
        )
        if sl_result.status != "ERROR" and sl_result.order_id:
            position.stop_loss_order_id = sl_result.order_id
            position.stop_loss_price = breakeven_price
            position.current_stop = breakeven_price
            logger.warning(f"🛡️ {position.symbol} TP后止损已移动到保本：{sl_result.order_id} @ {breakeven_price:.8f}")
            return True

        position.stop_loss_order_id = 0
        position.protection_failures += 1
        position.last_protection_error = sl_result.message
        send_telegram_message(
            format_error_msg(
                error_type="保本止损移动失败",
                message=sl_result.message,
                symbol=position.symbol,
                session_id=position.session_id,
                component="breakeven_stop",
            )
        )
        return False

    def _position_protection_status(self, position: Position) -> dict[str, Any]:
        stop_loss_ok = bool(position.stop_loss_order_id)
        take_profit_ids = [int(x) for x in position.take_profit_order_ids if x]
        expected_tp_count = len(position.take_profit_targets) if position.take_profit_targets else 1
        take_profit_ok = bool(take_profit_ids)
        return {
            "stop_loss_ok": stop_loss_ok,
            "take_profit_ok": take_profit_ok,
            "protected": stop_loss_ok and take_profit_ok,
            "take_profit_order_ids": take_profit_ids,
            "expected_tp_count": expected_tp_count,
        }

    def _send_protection_status(self, position: Position, source: str, force: bool = False):
        status = self._position_protection_status(position)
        if not force and status["protected"]:
            return
        send_telegram_message(
            format_protection_status_msg(
                symbol=position.symbol,
                stop_loss_ok=status["stop_loss_ok"],
                take_profit_ok=status["take_profit_ok"],
                stop_loss_order_id=position.stop_loss_order_id,
                take_profit_order_ids=status["take_profit_order_ids"],
                session_id=position.session_id,
                source=source,
                message=position.last_protection_error,
            )
        )

    def _refresh_protection_risk_switch(self):
        naked = []
        for position in self.tracker.positions.values():
            status = self._position_protection_status(position)
            if not status["protected"]:
                naked.append(position.symbol)

        if naked:
            self._new_entries_suspended = True
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="裸仓保护失败，暂停新开仓",
                        message=f"以下持仓保护单不完整：{', '.join(naked)}。系统会继续管理已有持仓，但暂停新开仓。",
                        component="protection_guard",
                    )
                )
                self._new_entries_suspended_alert_sent = True
        else:
            if self._new_entries_suspended:
                logger.warning("🛡️ 所有持仓保护单已恢复，新开仓限制解除")
            self._new_entries_suspended = False
            self._new_entries_suspended_alert_sent = False

    def _ensure_position_protection(self, position: Position):
        """Place missing exchange-side SL/TP orders for tracked or restored positions."""
        close_side = "SELL" if position.side == "BUY" else "BUY"
        position_side = "LONG" if position.side == "BUY" else "SHORT"

        if not position.stop_loss_order_id:
            sl_result = place_stop_loss_order(
                position.symbol,
                close_side,
                position.quantity,
                position.stop_loss_price,
                position_side=position_side,
                reduce_only=True,
            )
            if sl_result.status != "ERROR" and sl_result.order_id:
                position.stop_loss_order_id = sl_result.order_id
                logger.warning(f"🛡️ {position.symbol} 已补挂交易所止损单：{sl_result.order_id}")
            else:
                position.protection_failures += 1
                position.last_protection_error = sl_result.message
                send_telegram_message(
                    format_error_msg(
                        error_type="保护止损补挂失败",
                        message=sl_result.message,
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="protection_reconcile",
                    )
                )

        active_tp_targets = position.take_profit_targets or [{
            "level": 1,
            "price": position.take_profit_price,
            "quantity": position.quantity,
            "ratio": 1.0,
            "target_roi_pct": position.target_roi_pct,
            "price_move_pct": abs(position.take_profit_price - position.entry_price) / position.entry_price * 100 if position.entry_price else 0.0,
        }]
        if position.take_profit_order_ids:
            self._refresh_protection_risk_switch()
            return self._position_protection_status(position)["protected"]

        new_tp_order_ids: list[int] = []
        target_ratios = [max(float(target.get("ratio", 0) or 0), 0.0) for target in active_tp_targets]
        ratio_total = sum(target_ratios) or 1.0
        target_ratios = [ratio / ratio_total for ratio in target_ratios]
        remaining_qty = position.quantity

        for index, target in enumerate(active_tp_targets):
            if index == len(active_tp_targets) - 1:
                tp_quantity = remaining_qty
            else:
                tp_quantity = position.quantity * target_ratios[index]
                remaining_qty = max(remaining_qty - tp_quantity, 0.0)
            tp_price = float(target.get("price", position.take_profit_price) or position.take_profit_price)
            if tp_quantity <= 0 or tp_price <= 0:
                continue
            target["quantity"] = tp_quantity
            tp_result = place_take_profit_order(
                position.symbol,
                close_side,
                tp_quantity,
                tp_price,
                position_side=position_side,
                reduce_only=True,
            )
            target["status"] = tp_result.status
            target["message"] = tp_result.message
            target["order_id"] = tp_result.order_id
            if tp_result.status != "ERROR" and tp_result.order_id:
                new_tp_order_ids.append(tp_result.order_id)
                logger.warning(f"🎯 {position.symbol} 已补挂交易所止盈单：{tp_result.order_id} @ {tp_price}")
            else:
                position.protection_failures += 1
                position.last_protection_error = tp_result.message
                send_telegram_message(
                    format_error_msg(
                        error_type="保护止盈补挂失败",
                        message=tp_result.message,
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="protection_reconcile",
                    )
                )

        position.take_profit_order_ids = new_tp_order_ids
        protected = self._position_protection_status(position)["protected"]
        if protected:
            position.protection_failures = 0
            position.last_protection_error = ""
        self._refresh_protection_risk_switch()
        return protected

    def _notify_partial_take_profit(self, position: Position, reduced_qty: float, remaining_qty: float, price: float):
        if reduced_qty <= 0 or price <= 0:
            return

        if position.side == "BUY":
            pnl = (price - position.entry_price) * reduced_qty
        else:
            pnl = (position.entry_price - price) * reduced_qty

        notional = position.entry_price * reduced_qty
        pnl_pct = pnl / notional * 100 if notional > 0 else 0.0
        position.partial_tp_count += 1
        self.daily_pnl += pnl
        send_telegram_message(
            format_partial_take_profit_msg(
                symbol=position.symbol,
                direction="LONG" if position.side == "BUY" else "SHORT",
                entry_price=position.entry_price,
                exit_price=price,
                quantity=reduced_qty,
                remaining_quantity=remaining_qty,
                pnl=pnl,
                pnl_pct=pnl_pct,
                level=position.partial_tp_count,
                session_id=position.session_id,
            )
        )

    def _sync_protective_order_snapshot(self, position: Position):
        """Best-effort order snapshot check without blocking trading."""
        try:
            normal_orders = fetch_open_orders(position.symbol)
            algo_orders = fetch_open_algo_orders(position.symbol)
        except Exception as e:
            logger.debug(f"{position.symbol} 委托快照同步跳过：{e}")
            return

        open_ids = set()
        for order in (normal_orders or []) + (algo_orders or []):
            for key in ("algoId", "orderId", "orderID"):
                if key in order and order.get(key):
                    try:
                        open_ids.add(int(order.get(key)))
                    except Exception:
                        pass

        if not open_ids:
            return

        expected_ids = set(position.take_profit_order_ids)
        if position.stop_loss_order_id:
            expected_ids.add(position.stop_loss_order_id)

        missing_ids = sorted(order_id for order_id in expected_ids if order_id and order_id not in open_ids)
        if missing_ids:
            logger.warning(f"⚠️ {position.symbol} 保护委托可能已成交/失效：{missing_ids}")
            position.last_protection_error = f"missing_order_ids={missing_ids}"
            if position.stop_loss_order_id in missing_ids:
                position.stop_loss_order_id = 0
            position.take_profit_order_ids = [oid for oid in position.take_profit_order_ids if oid not in missing_ids]
            self._refresh_protection_risk_switch()

    def _audit_all_position_protection(self, source: str = "startup_audit"):
        """Audit all tracked positions and confirm exchange-side protection."""
        if not self.tracker.positions:
            return

        for position in list(self.tracker.positions.values()):
            self._ensure_position_protection(position)
            self._sync_protective_order_snapshot(position)
            self._send_protection_status(position, source=source, force=True)

        self._refresh_protection_risk_switch()

    def _start_background_protection_audit(self, source: str = "startup_audit"):
        """Run protection audit outside the startup critical path."""
        if self._startup_audit_started:
            return
        self._startup_audit_started = True

        def worker():
            try:
                logger.info("🛡️ 后台保护单审计启动")
                self._audit_all_position_protection(source=source)
                logger.info("🛡️ 后台保护单审计完成")
            except Exception as e:
                logger.warning(f"后台保护单审计失败：{e}")

        threading.Thread(target=worker, daemon=True).start()

    def _sync_positions_with_exchange(self):
        """Sync local tracked positions with real exchange positions for staged TP fills."""
        if not self.tracker.positions:
            return

        try:
            account_info = get_account_balance()
        except Exception as e:
            logger.warning(f"同步交易所持仓失败：{e}")
            return

        live_positions = {
            (item["symbol"], item["side"]): item
            for item in self._extract_live_positions(account_info if isinstance(account_info, dict) else {})
        }

        for symbol, position in list(self.tracker.positions.items()):
            side_key = "LONG" if position.side == "BUY" else "SHORT"
            live_pos = live_positions.get((symbol, side_key))

            if not live_pos:
                logger.warning(f"♻️ {symbol} 本地有仓位但交易所已无持仓，按交易所状态移除")
                current_price = self.get_current_prices([symbol]).get(symbol, position.take_profit_price or position.current_stop or position.entry_price)
                if position.side == "BUY":
                    inferred_reason = "STOP_LOSS" if current_price <= position.current_stop else "TAKE_PROFIT"
                    pnl = (current_price - position.entry_price) * position.quantity
                else:
                    inferred_reason = "STOP_LOSS" if current_price >= position.current_stop else "TAKE_PROFIT"
                    pnl = (position.entry_price - current_price) * position.quantity

                position.exit_price = current_price
                position.exit_time = datetime.now()
                position.exit_reason = f"{inferred_reason}_EXCHANGE"
                position.pnl = pnl
                position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100 if position.entry_price and position.quantity else 0.0
                self.daily_pnl += pnl
                self._record_closed_trade_result(position, pnl)

                self._cancel_position_protection(position)
                self.tracker.remove_position(symbol)
                send_telegram_message(
                    format_close_position_msg(
                        symbol=symbol,
                        direction="LONG" if position.side == "BUY" else "SHORT",
                        entry_price=position.entry_price,
                        exit_price=current_price,
                        quantity=position.quantity,
                        pnl=pnl,
                        pnl_pct=position.pnl_pct,
                        reason=position.exit_reason,
                        duration_hours=(position.exit_time - position.entry_time).total_seconds() / 3600,
                        session_id=position.session_id,
                    )
                )

                open_trades = self.db.get_open_trades(mode=self.config.mode)
                for trade in open_trades:
                    if trade.symbol == symbol:
                        self.db.update_exit(
                            trade_id=trade.id,
                            exit_price=current_price,
                            exit_reason=position.exit_reason,
                            pnl=pnl,
                            pnl_pct=position.pnl_pct,
                            realized_pnl=pnl,
                        )
                        break
                continue

            live_qty = float(live_pos.get("quantity", 0) or 0)
            if live_qty <= 0:
                continue

            if live_qty + 1e-9 < position.quantity:
                reduced_qty = position.quantity - live_qty
                logger.info(f"🎯 {symbol} 交易所已部分止盈：减少 {reduced_qty:.6f}，剩余 {live_qty:.6f}")
                current_price = self.get_current_prices([symbol]).get(symbol, position.take_profit_price)
                self._notify_partial_take_profit(position, reduced_qty, live_qty, current_price)
                self._move_stop_to_breakeven(position, live_qty)
            position.quantity = live_qty
            position.last_synced_quantity = live_qty
            self._ensure_position_protection(position)
            self._sync_protective_order_snapshot(position)

    def _check_new_day(self):
        today = datetime.now().date().isoformat()
        if today != self._daily_marker:
            self._daily_marker = today
            self.daily_pnl = 0.0
            self.traded_symbols_today.clear()
            self.tracker.reset_daily_summary()
            self._daily_loss_alert_sent = False
            self._symbol_cooldowns.clear()
            self._consecutive_losses = 0
            self._loss_pause_until = 0.0
            self._entry_watchlist.clear()
            try:
                balance_info = get_account_balance()
                if isinstance(balance_info, dict):
                    self.day_start_balance = float(balance_info.get("availableBalance", self.day_start_balance or 0))
            except Exception:
                pass

    def _is_daily_loss_limit_hit(self) -> bool:
        if self.day_start_balance <= 0:
            return False
        limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
        return self.daily_pnl <= -limit_amount

    def _is_loss_pause_active(self) -> bool:
        return self._loss_pause_until > time.time()

    def _passes_liquidity_filter(self, symbol: str, desired_position_value: float) -> bool:
        try:
            ticker = fetch_ticker_24hr(symbol)
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < 250000:
                logger.warning(f"⚠️ {symbol} 24h 成交额过低，跳过：{quote_volume:.2f} USDT")
                return False

            position_to_volume_ratio = desired_position_value / quote_volume if quote_volume > 0 else 1.0
            if position_to_volume_ratio > 0.002:
                logger.warning(
                    f"⚠️ {symbol} 流动性不足，仓位/成交额占比过高：{position_to_volume_ratio:.4%}"
                )
                return False
            return True
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 流动性检查失败：{e}")
            return False

    def _extract_live_positions(self, account_info: dict[str, Any]) -> list[dict[str, Any]]:
        live_positions = []
        for pos in account_info.get("positions", []) or []:
            position_amt = float(pos.get("positionAmt", 0) or 0)
            if abs(position_amt) <= 0:
                continue

            side = pos.get("positionSide")
            if not side or side == "BOTH":
                side = "LONG" if position_amt > 0 else "SHORT"

            live_positions.append({
                "symbol": pos.get("symbol", ""),
                "side": side,
                "quantity": abs(position_amt),
                "entry_price": float(pos.get("entryPrice", 0) or 0),
                "unrealized_pnl": float(pos.get("unRealizedProfit", 0) or 0),
            })
        return [p for p in live_positions if p["symbol"]]

    def _restore_positions(self, account_info: dict[str, Any]):
        live_positions = self._extract_live_positions(account_info)
        if not live_positions:
            return

        open_trades = self.db.get_open_trades(mode=self.config.mode)
        trades_by_symbol = {t.symbol: t for t in open_trades}

        for live_pos in live_positions:
            if live_pos["symbol"] in self.tracker.positions:
                continue

            side = "BUY" if live_pos["side"] == "LONG" else "SELL"
            trade = trades_by_symbol.get(live_pos["symbol"])
            entry_price = trade.entry_price if trade else live_pos["entry_price"]
            stop_loss_price = trade.stop_loss if trade else (
                entry_price * (1 - self.config.stop_loss_pct / 100) if side == "BUY"
                else entry_price * (1 + self.config.stop_loss_pct / 100)
            )
            notes_map = self._parse_trade_notes(trade.notes if trade else "")
            take_profit_targets: list[dict[str, Any]] = []
            take_profit_order_ids: list[int] = []
            target_roi_pct = float(notes_map.get("target_roi_pct", self.config.take_profit_pct) or self.config.take_profit_pct)
            if notes_map.get("tp_plan"):
                try:
                    take_profit_targets = json.loads(notes_map["tp_plan"])
                except Exception:
                    take_profit_targets = []
            if notes_map.get("tp_order_ids"):
                try:
                    take_profit_order_ids = [int(x) for x in notes_map["tp_order_ids"].split(",") if x.strip()]
                except Exception:
                    take_profit_order_ids = []

            take_profit_price = trade.take_profit if trade else self._calculate_local_take_profit_price(
                entry_price,
                side,
                self.config.take_profit_pct,
            )
            entry_time = datetime.fromisoformat(trade.entry_time) if trade and trade.entry_time else datetime.now()
            session_id = notes_map.get("session_id", "")
            if not session_id:
                session_id = self._new_session_id(live_pos["symbol"])

            restored = Position(
                symbol=live_pos["symbol"],
                side=side,
                entry_price=entry_price,
                quantity=live_pos["quantity"],
                order_id=trade.id if trade and trade.id else 0,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                entry_time=entry_time,
                stage_at_entry=trade.stage if trade else "restored",
                stop_loss_order_id=0,
                session_id=session_id,
                target_roi_pct=target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=take_profit_order_ids,
            )
            self.tracker.add_position(restored)
            logger.warning(f"♻️ 已恢复持仓：{restored.symbol} {restored.side} session={session_id}")

    def _run_health_checks(self) -> float:
        telegram_config = get_telegram_config()
        if not telegram_config.get("bot_token") or not telegram_config.get("chat_id"):
            raise RuntimeError("Telegram 未配置 bot_token/chat_id")

        native_ready = is_native_binance_configured()
        if not native_ready:
            raise RuntimeError("原生 Binance API 未配置：请设置 BINANCE_API_KEY / BINANCE_API_SECRET")
        logger.info("🧬 原生 Binance API 交易通道已启用")

        account_info = get_account_balance()
        if not isinstance(account_info, dict):
            raise RuntimeError("账户信息返回格式异常")

        balance = float(account_info.get("availableBalance", 0) or 0)
        if balance <= 0:
            raise RuntimeError("账户可用余额为 0")

        log_dir = Path("/root/.hermes/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(log_dir, os.W_OK):
            raise RuntimeError(f"日志目录不可写: {log_dir}")

        self._restore_positions(account_info)
        return balance

    def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        """获取当前价格"""
        prices = {}
        self._refresh_price_stream(symbols)
        for symbol in symbols:
            try:
                ws_price = self._get_ws_price(symbol)
                if ws_price > 0:
                    prices[symbol] = ws_price
                    continue
                ticker = fetch_ticker_24hr(symbol)
                prices[symbol] = float(ticker.get("lastPrice", 0))
            except Exception as e:
                logger.warning(f"获取 {symbol} 价格失败：{e}")
        return prices

    def _entry_rejection_reason(self, symbol: str, direction: str, metrics: dict) -> str:
        """Reject obvious chase entries before expensive scoring and live orders."""
        change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
        drawdown = float(metrics.get("drawdown_from_24h_high_pct", 0.0) or 0.0)
        range_position = float(metrics.get("range_position_24h_pct", 50.0) or 50.0)
        funding = float(metrics.get("funding_rate", 0.0) or 0.0)
        oi_change = float(metrics.get("oi_24h_pct", 0.0) or 0.0)
        volume_mult = float(metrics.get("volume_24h_mult", 1.0) or 1.0)
        now = time.time()

        if self._is_loss_pause_active():
            remaining_min = max(1, int((self._loss_pause_until - now) / 60))
            return f"连续亏损暂停中，剩余 {remaining_min} 分钟"
        cooldown_until = self._symbol_cooldowns.get(symbol, 0.0)
        if cooldown_until > now:
            remaining_min = max(1, int((cooldown_until - now) / 60))
            return f"亏损冷却中，剩余 {remaining_min} 分钟"

        if abs(funding) >= self.config.max_abs_funding_rate:
            return f"资金费率过热 {funding * 100:.3f}%"
        if oi_change >= self.config.max_oi_change_pct:
            return f"OI过热 {oi_change:.1f}%"

        if direction == "LONG":
            if change_24h <= -12:
                return f"大跌中不接多 {change_24h:.1f}%"
            if change_24h >= self.config.max_chase_change_pct:
                return f"24h涨幅过大 {change_24h:.1f}%"
            if change_24h >= 12 and drawdown < self.config.min_pullback_pct:
                return f"未回踩，距24h高点仅回落 {drawdown:.1f}%"
            if change_24h >= 8 and range_position >= self.config.max_range_position_pct:
                return f"价格处于24h区间高位 {range_position:.1f}%"
        elif direction == "SHORT":
            if change_24h >= 12:
                return f"大涨中不追空 {change_24h:.1f}%"
            if change_24h <= -self.config.max_chase_change_pct:
                return f"24h跌幅过大 {change_24h:.1f}%"
            if change_24h <= -12 and range_position <= 100 - self.config.max_range_position_pct:
                return f"价格处于24h区间低位 {range_position:.1f}%"

        if volume_mult < 0.8 and abs(change_24h) >= 10:
            return f"量能不足 volume_mult={volume_mult:.2f}"
        return ""

    def scan_for_signals(self, symbols: Optional[List[str]] = None, scan_source: str = "deep") -> List[dict]:
        """扫描交易信号 - 弗丽嘉的鹰眼"""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        self._prune_entry_watchlist()
        step_started = time.perf_counter()
        if symbols is not None:
            symbols = list(dict.fromkeys(symbols))[: self.config.scan_top_n]
            logger.info(f"⚡ Deep scan from {scan_source}: {len(symbols)} symbols | {symbols[:5]}...")
        elif self.config.scan_by_change:
            symbols = self._get_ws_top_symbols_by_change(
                self.config.scan_top_n,
                self.config.min_change_pct,
            )
            if not symbols:
                symbols = get_top_symbols_by_change(
                    self.config.scan_top_n,
                    min_change=self.config.min_change_pct
                )
                logger.info(f"🔥 妖币模式(REST) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
            else:
                logger.info(f"🔥 妖币模式(WS) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
        else:
            symbols = get_top_symbols_by_volume(self.config.scan_top_n)
            logger.info(f"📊 成交量模式 - 扫描 {len(symbols)} 个币种：{symbols[:5]}...")
        self._record_latency_step(latency_steps, "select_symbols", step_started)

        step_started = time.perf_counter()
        results = scan_symbols(
            symbols,
            min_stage=self.config.min_stage,
            max_workers=self.config.scan_workers,
        )
        self._record_latency_step(latency_steps, "deep_scan", step_started)

        signals = []
        step_started = time.perf_counter()
        for r in results:
            if r.stage in {"neutral", "error"}:
                continue
            if r.direction not in {"LONG", "SHORT"}:
                continue
            if r.symbol in self.tracker.positions:
                continue
            if r.symbol in self.traded_symbols_today:
                continue
            rejection_reason = self._entry_rejection_reason(r.symbol, r.direction, r.metrics)
            if rejection_reason:
                logger.info(f"🧊 {r.symbol} 入场过滤：{rejection_reason}")
                continue

            # 🎯 信号质量评分
            try:
                signal_score = score_signal(
                    symbol=r.symbol,
                    stage=r.stage,
                    direction=r.direction,
                    metrics=r.metrics,
                )
                
                # 过滤低质量信号（降低阈值：允许中等以上）
                if signal_score.confidence in {"低"}:
                    logger.info(f"🎯 {r.symbol} 信号质量过低 ({signal_score.total_score:.1f})，跳过")
                    continue
                
                # 记录评分
                logger.info(f"🎯 {r.symbol} 信号评分：{signal_score.total_score:.1f}/100 ({signal_score.confidence})")
                
            except Exception as e:
                logger.warning(f"信号评分失败 {r.symbol}: {e}")
                signal_score = None

            signal_data = {
                "symbol": r.symbol,
                "stage": r.stage,
                "direction": r.direction,
                "price": r.metrics.get("last_price", 0),
                "metrics": r.metrics,
                "trigger": r.trigger,
                "risk": r.risk,
                "score": signal_score.to_dict() if signal_score else None,
            }
            signals.append(self._apply_entry_confirmation(signal_data))

        # 按评分排序
        signals.sort(key=lambda s: s.get("score", {}).get("total_score", 0), reverse=True)
        self._record_latency_step(latency_steps, "score_filter", step_started)
        
        logger.info(f"📡 发现 {len(signals)} 个有效信号（已按质量排序）")
        self._emit_latency_trace("scan_for_signals", trace_started, latency_steps)

        return signals

    def execute_entry(self, signal: dict) -> Optional[Position]:
        """执行开仓 - 奥丁的长矛"""
        symbol = signal["symbol"]
        direction = signal["direction"]
        price = signal["price"]
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []

        if self._new_entries_suspended:
            logger.warning(f"🛡️ {symbol} 新开仓暂停：存在保护单不完整的持仓")
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="新开仓已暂停",
                        message="存在未完整受保护的持仓，请先确认止损/止盈保护单。",
                        symbol=symbol,
                        component="protection_guard",
                    )
                )
                self._new_entries_suspended_alert_sent = True
            return None

        try:
            trading_signal = TradingSignal(
                symbol=symbol,
                stage=signal["stage"],
                direction=direction,
                entry_price=price,
                metrics=signal["metrics"],
            )
            session_id = self._new_session_id(symbol)
            risk_level = "UNKNOWN"

            if not should_trade(trading_signal):
                return None

            # 获取账户余额
            step_started = time.perf_counter()
            try:
                balance_info = get_account_balance()
                if isinstance(balance_info, dict):
                    balance = float(balance_info.get("availableBalance", 10000))
                else:
                    balance = 10000
            except Exception:
                balance = 10000
            self._record_latency_step(latency_steps, "account_query", step_started)

            step_started = time.perf_counter()
            latest_price = self.get_current_prices([symbol]).get(symbol, price)
            if latest_price and price > 0:
                if direction == "LONG":
                    slippage_pct = (latest_price - price) / price * 100.0
                else:
                    slippage_pct = (price - latest_price) / price * 100.0
                if slippage_pct > self.config.max_entry_slippage_pct:
                    logger.warning(
                        f"🧊 {symbol} 下单前价格偏移过大，放弃开仓: signal={price:.8f}, latest={latest_price:.8f}, "
                        f"slippage={slippage_pct:.2f}%"
                    )
                    return None
                if latest_price > 0:
                    price = latest_price
                    trading_signal.entry_price = latest_price
            self._record_latency_step(latency_steps, "price_recheck", step_started)

            quantity = None
            stop_loss = None

            # 🛡️ 风控评估
            step_started = time.perf_counter()
            try:
                existing_positions = []
                for pos_symbol, pos in self.tracker.positions.items():
                    existing_positions.append({
                        "symbol": pos_symbol,
                        "side": "LONG" if pos.side == "BUY" else "SHORT",
                        "position_value": pos.entry_price * pos.quantity,
                    })

                risk_config = RiskConfig(
                    risk_per_trade_pct=self.config.risk_per_trade_pct,
                    base_stop_loss_pct=self.config.stop_loss_pct,
                    base_take_profit_pct=self.config.take_profit_pct,
                    max_position_pct=self.config.max_position_pct,
                    max_total_exposure=50.0,
                    max_correlated_positions=3,
                )

                risk_result = assess_trade_risk(
                    symbol=symbol,
                    side="LONG" if direction == "LONG" else "SHORT",
                    entry_price=price,
                    account_balance=balance,
                    existing_positions=existing_positions,
                    config=risk_config,
                )

                if not risk_result.get("can_open", False):
                    logger.warning(f"🛡️ {symbol} 风控拒绝：{risk_result.get('warnings', [])}")
                    return None

                logger.info(f"🛡️ {symbol} 风控评分：{risk_result.get('risk_score', 0)}/100 ({risk_result.get('risk_level', 'UNKNOWN')})")
                risk_level = risk_result.get("risk_level", "UNKNOWN")

                position_size = risk_result.get("position_size", {})
                quantity = position_size.get("quantity")
                stop_loss = risk_result.get("stop_loss", {}).get("stop_loss")
                position_value = float(position_size.get("position_value", 0) or 0)

                if quantity is not None and quantity <= 0:
                    logger.warning(f"🛡️ {symbol} 仓位计算失败")
                    return None

                if position_value > 0 and not self._passes_liquidity_filter(symbol, position_value):
                    return None

                logger.info(
                    f"🔍 {symbol} 风控参数: 余额=${balance:.2f}, 杠杆={self.config.leverage}x, "
                    f"名义仓位=${position_size.get('position_value', 0):.2f}, "
                    f"数量={quantity}, 止损=${(stop_loss or 0):.4f}"
                )

            except Exception as e:
                logger.warning(f"🛡️ 风控评估失败 {symbol}: {e}，回退到执行器默认计算")
            self._record_latency_step(latency_steps, "risk_assessment", step_started)

            # 执行交易（优先使用风控计算的仓位和止损）
            take_profit_target_pcts, take_profit_ratios = self._build_take_profit_plan()
            step_started = time.perf_counter()
            result = execute_trade(
                signal=trading_signal,
                account_balance=balance,
                risk_per_trade_pct=self.config.risk_per_trade_pct,
                stop_loss_pct=self.config.stop_loss_pct,
                max_position_pct=self.config.max_position_pct,
                leverage=self.config.leverage,
                quantity=quantity,
                stop_loss_price=stop_loss,
                take_profit_roi_pcts=take_profit_target_pcts if self.config.take_profit_mode == "roi" else None,
                take_profit_price_pcts=take_profit_target_pcts if self.config.take_profit_mode != "roi" else None,
                take_profit_ratios=take_profit_ratios,
                take_profit_mode=self.config.take_profit_mode,
            )
            self._record_latency_step(latency_steps, "execute_trade", step_started)

            if result.get("action") != "EXECUTED":
                logger.warning(f"❌ {symbol} 开仓失败：{result.get('reason', 'Unknown')}")
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            entry_order = result.get("entry_order", {})
            executed_entry_price = float(entry_order.get("executed_price", price) or price)
            take_profit_targets = result.get("take_profit_orders", [])
            take_profit_prices = result.get("take_profit_prices", [])
            target_roi_pcts = result.get("take_profit_roi_pcts", [])
            target_price_pcts = result.get("take_profit_price_pcts", take_profit_target_pcts)
            primary_target_roi_pct = float(target_roi_pcts[0] if target_roi_pcts else self.config.take_profit_pct * self.config.leverage)
            primary_price_move_pct = float(target_price_pcts[0] if target_price_pcts else self.config.take_profit_pct)
            tp_price = float(take_profit_prices[0] if take_profit_prices else executed_entry_price)
            if direction == "LONG":
                side = "BUY"
            else:
                side = "SELL"

            stop_loss_order = result.get("stop_loss_order", {})
            position = Position(
                symbol=symbol,
                side=side,
                entry_price=executed_entry_price,
                quantity=result.get("quantity", 0),
                order_id=result.get("order_id", 0),
                stop_loss_price=result.get("stop_loss_price", 0),
                take_profit_price=tp_price,
                entry_time=datetime.now(),
                stage_at_entry=signal["stage"],
                stop_loss_order_id=stop_loss_order.get("order_id", 0),
                session_id=session_id,
                target_roi_pct=primary_target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=[int(item.get("order_id", 0) or 0) for item in take_profit_targets if item.get("order_id")],
            )
            step_started = time.perf_counter()
            self._ensure_position_protection(position)
            self._send_protection_status(position, source="entry_confirm", force=True)
            self._record_latency_step(latency_steps, "protection_confirm", step_started)

            from telegram_notifier import format_open_position_msg

            score = signal.get("score", {}).get("total_score", 0) if signal.get("score") else 0

            msg = format_open_position_msg(
                symbol=symbol,
                direction=direction,
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=self.config.leverage,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                risk_amount=result.get('risk_amount_usdt', 0),
                risk_pct=self.config.risk_per_trade_pct,
                score=score,
                risk_level=risk_level,
                session_id=session_id,
                target_roi_pct=primary_target_roi_pct,
                price_move_pct=primary_price_move_pct,
                take_profit_targets=take_profit_targets,
            )
            send_telegram_message(msg)

            notes_parts = [
                f"session_id={session_id}",
                f"risk_level={risk_level}",
                f"target_roi_pct={primary_target_roi_pct}",
                f"price_move_pct={primary_price_move_pct}",
                f"take_profit_mode={self.config.take_profit_mode}",
                f"tp_multiplier={self._tp_multiplier}",
                f"tp_plan={json.dumps(take_profit_targets, separators=(',', ':'))}",
                f"tp_order_ids={','.join(str(int(item.get('order_id', 0))) for item in take_profit_targets if item.get('order_id'))}",
            ]

            trade = TradeRecord(
                symbol=symbol,
                side=direction,
                direction=side,
                stage=signal["stage"],
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=self.config.leverage,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                entry_time=position.entry_time.isoformat(),
                mode=self.config.mode,
                market_snapshot=signal.get("metrics", {}),
                notes=";".join(notes_parts),
            )
            step_started = time.perf_counter()
            trade_id = self.db.add_trade(trade)
            logger.info(f"📜 交易已记录 (ID: {trade_id})")
            self._record_latency_step(latency_steps, "db_write", step_started)
            self._emit_latency_trace("execute_entry", trace_started, latency_steps, symbol=symbol)

            return position

        except Exception as e:
            # 捕获所有异常，防止单币种失败阻塞主循环
            logger.error(f"❌ {symbol} 开仓流程异常：{e}", exc_info=True)
            send_telegram_message(
                format_error_msg(
                    error_type="开仓流程异常",
                    message=str(e),
                    symbol=symbol,
                    session_id=session_id if 'session_id' in locals() else "",
                    component="execute_entry",
                )
            )
            self._emit_latency_trace("execute_entry_exception", trace_started, latency_steps, symbol=symbol)
            return None

    def execute_exit(self, symbol: str, reason: str) -> bool:
        """执行平仓 - 托尔的雷霆"""
        position = self.tracker.get_position(symbol)
        if not position:
            return False
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []

        step_started = time.perf_counter()
        try:
            prices = self.get_current_prices([symbol])
            current_price = prices.get(symbol, 0)
        except Exception:
            current_price = 0
        self._record_latency_step(latency_steps, "price_fetch", step_started)

        close_side = "SELL" if position.side == "BUY" else "BUY"
        position_side = "LONG" if position.side == "BUY" else "SHORT"

        # 实盘平仓
        try:
            step_started = time.perf_counter()
            result = place_market_order(
                symbol,
                close_side,
                position.quantity,
                position_side=position_side,
                reduce_only=True,
            )
            self._record_latency_step(latency_steps, "market_close", step_started)

            if result.status == "FILLED":
                if position.side == "BUY":
                    pnl = (result.executed_price - position.entry_price) * position.quantity
                else:
                    pnl = (position.entry_price - result.executed_price) * position.quantity

                position.exit_price = result.executed_price
                position.exit_time = datetime.now()
                position.exit_reason = reason
                position.pnl = pnl
                position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100

                self.tracker.remove_position(symbol)
                self.daily_pnl += pnl
                self._record_closed_trade_result(position, pnl)
                step_started = time.perf_counter()
                self._cancel_position_protection(position)
                self._record_latency_step(latency_steps, "cancel_protection", step_started)

                duration_hours = (position.exit_time - position.entry_time).total_seconds() / 3600
                step_started = time.perf_counter()
                send_telegram_message(
                    format_close_position_msg(
                        symbol=symbol,
                        direction="LONG" if position.side == "BUY" else "SHORT",
                        entry_price=position.entry_price,
                        exit_price=result.executed_price,
                        quantity=position.quantity,
                        pnl=pnl,
                        pnl_pct=position.pnl_pct,
                        reason=reason,
                        duration_hours=duration_hours,
                        session_id=position.session_id,
                    )
                )
                self._record_latency_step(latency_steps, "telegram_notify", step_started)

                # 📜 更新交易日志
                step_started = time.perf_counter()
                open_trades = self.db.get_open_trades(mode=self.config.mode)
                for t in open_trades:
                    if t.symbol == symbol:
                        self.db.update_exit(
                            trade_id=t.id,
                            exit_price=result.executed_price,
                            exit_reason=reason,
                            pnl=pnl,
                            pnl_pct=position.pnl_pct,
                            realized_pnl=pnl,
                        )
                        logger.info(f"📜 交易已更新 (ID: {t.id})")
                        break
                self._record_latency_step(latency_steps, "db_update", step_started)
                self._emit_latency_trace("execute_exit", trace_started, latency_steps, symbol=symbol)

                return True

        except Exception as e:
            logger.error(f"平仓失败 {symbol}: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="平仓失败",
                    message=str(e),
                    symbol=symbol,
                    session_id=position.session_id,
                    component="execute_exit",
                )
            )
            self._emit_latency_trace("execute_exit_exception", trace_started, latency_steps, symbol=symbol)
            return False

        self._emit_latency_trace("execute_exit_failed", trace_started, latency_steps, symbol=symbol)
        return False

    def _deprecated_run_scan_cycle_legacy(self):
        """Deprecated legacy full-scan loop retained only for historical reference."""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        step_started = time.perf_counter()
        self._check_new_day()
        self._send_daily_report_if_due()
        self._refresh_market_profile()
        with self._state_lock:
            self._sync_positions_with_exchange()
        self._record_latency_step(latency_steps, "daily_market_position_sync", step_started)
        logger.info("=" * 60)
        logger.info(f"🔄 扫描周期开始 | 持仓：{self.tracker.get_open_count()}/{self.config.max_open_positions}")

        # 1. 更新持仓价格并检查平仓
        open_symbols = list(self.tracker.positions.keys())
        if open_symbols:
            step_started = time.perf_counter()
            prices = self.get_current_prices(open_symbols)
            self.tracker.update_all_prices(prices, self.config.trailing_stop_pct)

            exits = self.tracker.check_all_exits(prices)
            for symbol, reason in exits.items():
                logger.info(f"🚨 {symbol} 触发 {reason}")
                self.execute_exit(symbol, reason)
            self._record_latency_step(latency_steps, "manage_open_positions", step_started)

        # 2. 扫描新信号
        step_started = time.perf_counter()
        signals = self.scan_for_signals()
        self._record_latency_step(latency_steps, "scan_for_signals", step_started)
        logger.info(f"📡 发现 {len(signals)} 个交易信号")
        self._send_scan_monitor(signals)

        if self._is_daily_loss_limit_hit():
            if not self._daily_loss_alert_sent:
                limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
                msg = format_error_msg(
                    error_type="日内熔断",
                    message=f"当日亏损已达到 {self.daily_pnl:.2f} USDT，超过限制 {-limit_amount:.2f} USDT，暂停新开仓",
                    component="risk_guard",
                )
                send_telegram_message(msg)
                self._daily_loss_alert_sent = True
            signals = []

        # 3. 执行开仓
        step_started = time.perf_counter()
        for signal in signals:
            if self.tracker.get_open_count() >= self.config.max_open_positions:
                logger.info(f"⏸️ 已达最大持仓数 ({self.config.max_open_positions})")
                break
            if signal.get("entry_status") != "ready":
                continue

            position = self.execute_entry(signal)
            if position:
                with self._state_lock:
                    self.tracker.add_position(position)
                    self.traded_symbols_today.add(signal["symbol"])
                    self._entry_watchlist.pop(signal["symbol"], None)
        self._record_latency_step(latency_steps, "execute_entries", step_started)

        # 4. 发送持仓摘要（日志 + 定期 Telegram 通知）
        step_started = time.perf_counter()
        summary = self.tracker.get_summary()
        logger.info(f"📊 持仓摘要：{summary['open_positions']} 个 | 未实现 PnL: ${summary['total_unrealized_pnl']:.2f}")

        # 定期发送 Telegram 持仓汇总（每 6 小时）
        import time
        current_time = time.time()
        if current_time - self._last_summary_time >= self._summary_interval:
            self._send_position_summary(summary)
            self._last_summary_time = current_time
        self._record_latency_step(latency_steps, "summary_notify", step_started)

        self.last_scan_time = datetime.now()
        self._emit_latency_trace("run_scan_cycle", trace_started, latency_steps)

    def run_scan_cycle(self):
        """Run one fast or deep trading cycle."""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        now = time.time()
        deep_due = self._last_deep_scan_time <= 0 or now - self._last_deep_scan_time >= self._current_scan_interval

        step_started = time.perf_counter()
        self._check_new_day()
        self._send_daily_report_if_due()
        if deep_due:
            self._refresh_market_profile()
        if deep_due or now - self._last_position_sync_time >= max(180, self._current_scan_interval):
            with self._state_lock:
                self._sync_positions_with_exchange()
            self._last_position_sync_time = now
        self._record_latency_step(latency_steps, "daily_market_position_sync", step_started)

        cycle_type = "deep" if deep_due else "fast"
        logger.info("=" * 60)
        logger.info(
            f"Scan cycle start | type={cycle_type} | "
            f"positions={self.tracker.get_open_count()}/{self.config.max_open_positions}"
        )

        open_symbols = list(self.tracker.positions.keys())
        if open_symbols:
            step_started = time.perf_counter()
            prices = self.get_current_prices(open_symbols)
            self.tracker.update_all_prices(prices, self.config.trailing_stop_pct)

            exits = self.tracker.check_all_exits(prices)
            for symbol, reason in exits.items():
                logger.info(f"Exit trigger {symbol}: {reason}")
                self.execute_exit(symbol, reason)
            self._record_latency_step(latency_steps, "manage_open_positions", step_started)

        step_started = time.perf_counter()
        candidates = self._fast_scan_candidates()
        self._record_latency_step(latency_steps, "fast_scan_candidates", step_started)

        signals = []
        if deep_due:
            step_started = time.perf_counter()
            deep_symbols = candidates[: self.config.scan_top_n] if candidates else None
            signals = self.scan_for_signals(deep_symbols, scan_source="fast_candidates")
            self._last_deep_scan_time = now
            self._record_latency_step(latency_steps, "deep_scan_signals", step_started)
            ready_count = sum(1 for item in signals if item.get("entry_status") == "ready")
            watch_count = sum(1 for item in signals if item.get("entry_status") == "watch")
            logger.info(f"Trade signals found: {len(signals)} | ready={ready_count} | watch={watch_count}")
            self._send_scan_monitor(signals)
        else:
            next_deep_sec = max(0, int(self._current_scan_interval - (now - self._last_deep_scan_time)))
            logger.info(f"Fast scan only | candidates={len(candidates)} | next_deep={next_deep_sec}s")

        if self._is_daily_loss_limit_hit():
            if not self._daily_loss_alert_sent:
                limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
                msg = format_error_msg(
                    error_type="日内熔断",
                    message=f"当日亏损已达到 {self.daily_pnl:.2f} USDT，超过限制 {-limit_amount:.2f} USDT，暂停新开仓",
                    component="risk_guard",
                )
                send_telegram_message(msg)
                self._daily_loss_alert_sent = True
            signals = []

        step_started = time.perf_counter()
        if deep_due:
            for signal in signals:
                if self.tracker.get_open_count() >= self.config.max_open_positions:
                    logger.info(f"Max open positions reached ({self.config.max_open_positions})")
                    break
                if signal.get("entry_status") != "ready":
                    continue

                position = self.execute_entry(signal)
                if position:
                    with self._state_lock:
                        self.tracker.add_position(position)
                        self.traded_symbols_today.add(signal["symbol"])
                        self._entry_watchlist.pop(signal["symbol"], None)
        self._record_latency_step(latency_steps, "execute_entries", step_started)

        step_started = time.perf_counter()
        summary = self.tracker.get_summary()
        logger.info(
            f"Position summary: {summary['open_positions']} open | "
            f"unrealized PnL=${summary['total_unrealized_pnl']:.2f}"
        )

        current_time = time.time()
        if current_time - self._last_summary_time >= self._summary_interval:
            self._send_position_summary(summary)
            self._last_summary_time = current_time
        self._record_latency_step(latency_steps, "summary_notify", step_started)

        self.last_scan_time = datetime.now()
        self._emit_latency_trace(f"run_scan_cycle_{cycle_type}", trace_started, latency_steps)

    def _send_position_summary(self, summary: dict):
        """发送持仓汇总通知"""
        msg = format_summary_msg(
            positions=summary["positions"],
            total_pnl=summary["total_unrealized_pnl"],
            realized_pnl=summary["realized_pnl"],
        )
        msg += f"\n\n<b>已平仓</b>  <code>{summary['closed_today']}</code> 笔"
        send_telegram_message(msg)

    def _send_scan_monitor(self, signals: list[dict]):
        """Send a compact Telegram scanner monitor report."""
        now = time.time()
        interval = max(60, min(self._monitor_interval, self._current_scan_interval))
        if now - self._last_monitor_time < interval:
            return
        self._last_monitor_time = now
        try:
            msg = format_scan_monitor_msg(
                signals=signals,
                scanned_count=self.config.scan_top_n,
                max_items=5,
            )
            send_telegram_message(msg)
        except Exception as e:
            logger.debug(f"扫描监控通知发送失败：{e}")

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
        summary = self.tracker.get_summary()
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
  crypto-sword --live                 # 实盘模式（⚠️ 真实资金）
  crypto-sword --live --leverage 10   # 实盘 + 10x 杠杆
        """,
    )

    # 实盘模式（强制）
    parser.add_argument("--live", action="store_true", required=True, help="实盘模式（⚠️ 真实资金）")

    # 杠杆 - 奥丁的长矛
    parser.add_argument("--leverage", "-l", type=int, default=5, choices=range(1, 11),
                        metavar="1-10", help="杠杆倍数 (1-10x, 默认：5x)")

    # 风控 - 英灵殿的盾牌
    parser.add_argument("--risk", "-r", type=float, default=0.5, help="每笔风险 %% (默认：0.5%%)")
    parser.add_argument("--stop-loss", "-s", type=float, default=8.0, help="止损 %% (默认：8%%)")
    parser.add_argument("--take-profit", "-t", type=float, default=20.0, help="止盈 %% (默认：20%%)")
    parser.add_argument(
        "--take-profit-mode",
        choices=["price", "roi"],
        default="roi",
        help="止盈百分比口径：roi=杠杆后收益率，price=标的价格涨跌幅 (默认：roi)",
    )
    parser.add_argument("--max-positions", "-m", type=int, default=5, help="最大持仓数 (默认：5)")
    parser.add_argument("--max-daily-loss", type=float, default=5.0, help="每日最大亏损 %% (默认：5%%)")

    # 扫描 - 弗丽嘉的鹰眼
    parser.add_argument("--top", type=int, default=50, help="扫描前 N 个币种 (默认：50)")
    parser.add_argument("--interval", "-i", type=int, default=300, help="扫描间隔秒数 (默认：300)")
    parser.add_argument("--scan-workers", type=int, default=6, help="深度扫描并发数 (默认：6)")
    parser.add_argument("--min-change", type=float, default=3.0, help="最小涨幅 %% (默认：3%%)")
    parser.add_argument("--by-volume", action="store_true", help="按成交量排序（默认按涨幅）")
    parser.add_argument("--no-entry-confirm", action="store_true", help="禁用回踩确认入场")
    parser.add_argument("--entry-confirm-timeout", type=int, default=1800, help="候选观察超时秒数")
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
        entry_confirmation_enabled=not args.no_entry_confirm,
        entry_confirmation_timeout_sec=max(300, args.entry_confirm_timeout),
        daily_report_enabled=not args.no_daily_report,
    )

    # 启动交易引擎
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()
