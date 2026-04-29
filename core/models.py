"""Core trading models extracted from the main runtime module."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TradingConfig:
    """Runtime trading configuration."""

    def __init__(
        self,
        mode: str = "live",
        leverage: int = 5,
        risk_per_trade_pct: float = 1.5,
        stop_loss_pct: float = 7.0,
        take_profit_pct: float = 18.0,
        take_profit_mode: str = "roi",
        max_position_pct: float = 35.0,
        max_total_exposure_pct: float = 220.0,
        max_daily_loss_pct: float = 0.0,
        max_open_positions: int = 10,
        trailing_stop_pct: float = 5.0,
        trailing_stop_enabled: bool = True,
        scan_top_n: int = 50,
        scan_interval_sec: int = 120,
        fast_scan_interval_sec: int = 30,
        scan_workers: int = 8,
        min_stage: str = "pre_break",
        scan_by_change: bool = True,
        min_change_pct: float = 1.0,
        max_chase_change_pct: float = 45.0,
        min_pullback_pct: float = 1.0,
        shallow_pullback_pct: float = 0.6,
        reclaim_volume_ratio: float = 1.05,
        max_range_position_pct: float = 95.0,
        max_abs_funding_rate: float = 0.008,
        max_oi_change_pct: float = 120.0,
        max_entry_slippage_pct: float = 0.8,
        min_quote_volume_usdt: float = 250000.0,
        alt_min_quote_volume_usdt: float = 600000.0,
        max_position_to_volume_ratio: float = 0.002,
        alt_max_position_to_volume_ratio: float = 0.0018,
        oi_funding_enabled: bool = True,
        oi_funding_min_oi_change_pct: float = 6.0,
        oi_funding_turn_bonus: float = 6.0,
        oi_funding_rising_bonus: float = 10.0,
        oi_funding_bonus_cap: float = 16.0,
        oi_funding_cache_sec: int = 120,
        breakeven_after_tp: bool = True,
        breakeven_offset_pct: float = 0.08,
        stop_trigger_buffer_pct: float = 0.15,
        breakout_stop_trigger_buffer_pct: float = 0.22,
        pullback_stop_trigger_buffer_pct: float = 0.10,
        entry_confirmation_enabled: bool = True,
        entry_confirmation_timeout_sec: int = 30 * 60,
        momentum_entry_enabled: bool = True,
        momentum_entry_score: float = 52.0,
        momentum_entry_min_change_pct: float = 10.0,
        momentum_entry_min_oi_pct: float = 22.0,
        accumulation_entry_enabled: bool = True,
        accumulation_entry_score: float = 40.0,
        accumulation_entry_min_oi_pct: float = 6.0,
        accumulation_entry_max_change_pct: float = 20.0,
        accumulation_entry_max_range_pct: float = 80.0,
        accumulation_entry_min_volume_mult: float = 1.05,
        breakout_tp_multiplier: float = 1.1,
        breakout_stop_multiplier: float = 0.85,
        pullback_tp_multiplier: float = 0.95,
        pullback_stop_multiplier: float = 1.0,
        daily_report_enabled: bool = True,
        daily_report_on_first_cycle: bool = True,
        major_symbols: Optional[List[str]] = None,
        market_style_lookback_trades: int = 20,
        market_style_refresh_sec: int = 900,
        target_altcoins: bool = True,
        target_memes: bool = True,
        require_telegram_notify: bool = True,
    ):
        self.mode = mode
        self.leverage = leverage
        self.risk_per_trade_pct = risk_per_trade_pct
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.take_profit_mode = take_profit_mode
        self.max_position_pct = max_position_pct
        self.max_total_exposure_pct = max_total_exposure_pct
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
        self.shallow_pullback_pct = shallow_pullback_pct
        self.reclaim_volume_ratio = reclaim_volume_ratio
        self.max_range_position_pct = max_range_position_pct
        self.max_abs_funding_rate = max_abs_funding_rate
        self.max_oi_change_pct = max_oi_change_pct
        self.max_entry_slippage_pct = max_entry_slippage_pct
        self.min_quote_volume_usdt = min_quote_volume_usdt
        self.alt_min_quote_volume_usdt = alt_min_quote_volume_usdt
        self.max_position_to_volume_ratio = max_position_to_volume_ratio
        self.alt_max_position_to_volume_ratio = alt_max_position_to_volume_ratio
        self.oi_funding_enabled = oi_funding_enabled
        self.oi_funding_min_oi_change_pct = oi_funding_min_oi_change_pct
        self.oi_funding_turn_bonus = oi_funding_turn_bonus
        self.oi_funding_rising_bonus = oi_funding_rising_bonus
        self.oi_funding_bonus_cap = oi_funding_bonus_cap
        self.oi_funding_cache_sec = max(30, int(oi_funding_cache_sec))
        self.breakeven_after_tp = breakeven_after_tp
        self.breakeven_offset_pct = breakeven_offset_pct
        self.stop_trigger_buffer_pct = stop_trigger_buffer_pct
        self.breakout_stop_trigger_buffer_pct = breakout_stop_trigger_buffer_pct
        self.pullback_stop_trigger_buffer_pct = pullback_stop_trigger_buffer_pct
        self.entry_confirmation_enabled = entry_confirmation_enabled
        self.entry_confirmation_timeout_sec = entry_confirmation_timeout_sec
        self.momentum_entry_enabled = momentum_entry_enabled
        self.momentum_entry_score = momentum_entry_score
        self.momentum_entry_min_change_pct = momentum_entry_min_change_pct
        self.momentum_entry_min_oi_pct = momentum_entry_min_oi_pct
        self.accumulation_entry_enabled = accumulation_entry_enabled
        self.accumulation_entry_score = accumulation_entry_score
        self.accumulation_entry_min_oi_pct = accumulation_entry_min_oi_pct
        self.accumulation_entry_max_change_pct = accumulation_entry_max_change_pct
        self.accumulation_entry_max_range_pct = accumulation_entry_max_range_pct
        self.accumulation_entry_min_volume_mult = accumulation_entry_min_volume_mult
        self.breakout_tp_multiplier = breakout_tp_multiplier
        self.breakout_stop_multiplier = breakout_stop_multiplier
        self.pullback_tp_multiplier = pullback_tp_multiplier
        self.pullback_stop_multiplier = pullback_stop_multiplier
        self.daily_report_enabled = daily_report_enabled
        self.daily_report_on_first_cycle = daily_report_on_first_cycle
        self.major_symbols = [symbol.upper() for symbol in (major_symbols or ["BTCUSDT", "ETHUSDT"]) if symbol]
        self.market_style_lookback_trades = max(6, int(market_style_lookback_trades))
        self.market_style_refresh_sec = max(300, int(market_style_refresh_sec))
        self.target_altcoins = target_altcoins
        self.target_memes = target_memes
        self.require_telegram_notify = require_telegram_notify

    @property
    def mode_emoji(self) -> str:
        return "💰"

    @property
    def mode_name(self) -> str:
        return "实盘"


class Position:
    """Open position runtime state."""

    def __init__(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        quantity: float,
        order_id: int,
        stop_loss_price: float,
        take_profit_price: float,
        entry_time: datetime,
        stage_at_entry: str,
        strategy_line: str = "",
        stop_loss_order_id: int = 0,
        session_id: str = "",
        oi_funding: Optional[Dict[str, Any]] = None,
        entry_score: Optional[Dict[str, Any]] = None,
        entry_metrics: Optional[Dict[str, Any]] = None,
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
        self.strategy_line = strategy_line
        self.stop_loss_order_id = stop_loss_order_id
        self.session_id = session_id
        self.oi_funding = dict(oi_funding or {})
        self.entry_score = dict(entry_score or {})
        self.entry_metrics = dict(entry_metrics or {})
        self.target_roi_pct = target_roi_pct
        self.take_profit_targets = take_profit_targets or []
        self.take_profit_order_ids = take_profit_order_ids or []
        self.initial_quantity = quantity
        self.last_synced_quantity = quantity
        self.partial_tp_count = 0
        self.realized_pnl = 0.0
        self.realized_exit_value = 0.0
        self.realized_quantity = 0.0
        self.exchange_realized_pnl = 0.0
        self.exchange_realized_exit_value = 0.0
        self.exchange_realized_quantity = 0.0
        self.protection_failures = 0
        self.last_protection_error = ""
        self.highest_price: float = entry_price
        self.lowest_price: float = entry_price
        self.current_stop: float = stop_loss_price
        self.exit_price: Optional[float] = None
        self.exit_time: Optional[datetime] = None
        self.exit_reason: Optional[str] = None
        self.pnl: float = 0.0
        self.pnl_pct: float = 0.0

    def update_price(self, current_price: float, trailing_stop_pct: float):
        """Update price and trailing stop."""
        if self.side == "BUY":
            if current_price > self.highest_price:
                self.highest_price = current_price
                if self.current_stop < self.highest_price * (1 - trailing_stop_pct / 100):
                    self.current_stop = self.highest_price * (1 - trailing_stop_pct / 100)
        else:
            if current_price < self.lowest_price:
                self.lowest_price = current_price
                if self.current_stop > self.lowest_price * (1 + trailing_stop_pct / 100):
                    self.current_stop = self.lowest_price * (1 + trailing_stop_pct / 100)

        if self.entry_price <= 0:
            self.pnl = 0.0
            self.pnl_pct = 0.0
            return
        if self.side == "BUY":
            self.pnl = (current_price - self.entry_price) * self.quantity
            self.pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
        else:
            self.pnl = (self.entry_price - current_price) * self.quantity
            self.pnl_pct = (self.entry_price - current_price) / self.entry_price * 100

    def check_exit_conditions(self, current_price: float) -> Optional[str]:
        """Check TP/SL exit trigger."""
        if self.side == "BUY":
            if current_price <= self.current_stop:
                return "STOP_LOSS"
            if current_price >= self.take_profit_price:
                if self.take_profit_order_ids:
                    return "TAKE_PROFIT_LOCAL_FALLBACK"
                return "TAKE_PROFIT"
        else:
            if current_price >= self.current_stop:
                return "STOP_LOSS"
            if current_price <= self.take_profit_price:
                if self.take_profit_order_ids:
                    return "TAKE_PROFIT_LOCAL_FALLBACK"
                return "TAKE_PROFIT"
        return None

    def _format_take_profit_targets_text(self) -> str:
        if not self.take_profit_targets:
            return f"${self.take_profit_price:,.4f}"

        parts = []
        for target in self.take_profit_targets:
            roi_pct = float(target.get("target_roi_pct", 0) or 0)
            price = float(target.get("price", 0) or 0)
            parts.append(f"{roi_pct:.0f}%->${price:,.4f}")
        return " | ".join(parts)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": "LONG" if self.side == "BUY" else "SHORT",
            "entry_price": self.entry_price,
            "current_price": round(self.entry_price * (1 + self.pnl_pct / 100), 4)
            if self.side == "BUY"
            else round(self.entry_price * (1 - self.pnl_pct / 100), 4),
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
            "strategy_line": self.strategy_line,
            "oi_funding": self.oi_funding,
            "entry_score": self.entry_score,
            "entry_metrics": self.entry_metrics,
        }


class PositionTracker:
    """In-memory position state tracker."""

    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []

    def add_position(self, position: Position):
        self.positions[position.symbol] = position
        logger.info(f"open: {position.symbol} {position.side} @ ${position.entry_price}")

    def remove_position(self, symbol: str):
        if symbol in self.positions:
            pos = self.positions.pop(symbol)
            self.closed_positions.append(pos)
            logger.info(f"close: {pos.symbol} | PnL: ${pos.pnl:.2f} ({pos.pnl_pct:.2f}%) | reason: {pos.exit_reason}")

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
