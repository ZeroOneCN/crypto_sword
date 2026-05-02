"""Execution and protection logic mixin for the trading engine."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

from binance_trading_executor import (
    OrderResult,
)
from feature_store import build_trade_review, feature_store
from speed_executor import quick_close_position
from telegram_notifier import (
    format_direction_label,
    format_entry_failure_detail,
    format_close_position_msg,
    format_error_msg,
    format_partial_take_profit_msg,
    format_protection_failure_detail,
    format_protection_status_msg,
    send_telegram_message,
)
from trade_logger import TradeRecord
from services.capital_allocator import capital_allocator
from services.execution_service import execution_service
from services.order_service import order_service
from services.risk_service import risk_service
from signal_enhancer import get_klines

from .monitoring import build_execution_event, message_signature
from .models import Position

logger = logging.getLogger(__name__)


class ExecutionMixin:
    """Open/close execution and protective order lifecycle."""

    def _strategy_profile(self, strategy_line: str) -> dict[str, float]:
        if strategy_line == "趋势突破线":
            return {
                "tp_multiplier": self.config.breakout_tp_multiplier,
                "stop_multiplier": self.config.breakout_stop_multiplier,
            }
        if strategy_line == "均线二启线":
            return {
                "tp_multiplier": 1.20,
                "stop_multiplier": 0.72,
            }
        return {
            "tp_multiplier": self.config.pullback_tp_multiplier,
            "stop_multiplier": self.config.pullback_stop_multiplier,
        }

    def _strategy_take_profit_ratios(self, strategy_line: str, levels_count: int) -> list[float]:
        if strategy_line == "趋势突破线":
            base_ratios = [0.20, 0.30, 0.50]
        elif strategy_line == "均线二启线":
            base_ratios = [0.20, 0.30, 0.50]
        else:
            base_ratios = [0.20, 0.30, 0.50]
        ratios = base_ratios[:levels_count]
        ratio_total = sum(ratios) or 1.0
        return [ratio / ratio_total for ratio in ratios]

    def _build_take_profit_plan(self, strategy_line: str = "") -> tuple[list[float], list[float]]:
        """Build default staged take-profit plan around the configured TP percentage."""
        profile = self._strategy_profile(strategy_line)
        base_pct = max(float(self.config.take_profit_pct) * self._tp_multiplier * profile["tp_multiplier"], 0.0)
        if base_pct <= 0:
            return [0.0], [1.0]

        staged_levels = []
        for multiplier in (0.6, 1.2, 2.0):
            target_pct = round(base_pct * multiplier, 2)
            if target_pct > 0 and target_pct not in staged_levels:
                staged_levels.append(target_pct)

        ratios = self._strategy_take_profit_ratios(strategy_line, len(staged_levels))
        return staged_levels, ratios

    def _is_strong_trend_signal(self, signal: dict[str, Any]) -> bool:
        """Detect high-conviction momentum breakouts for wider profit targets."""
        if str(signal.get("strategy_line", "") or "") != "趋势突破线":
            return False
        metrics = signal.get("metrics") or {}
        score_data = signal.get("score") or {}
        score = float(score_data.get("total_score", score_data.get("total", 0)) if isinstance(score_data, dict) else score_data or 0)
        change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
        oi_change = float(metrics.get("oi_24h_pct", 0.0) or metrics.get("oi_change_pct", 0.0) or 0.0)
        funding = float(metrics.get("funding_rate", 0.0) or 0.0)
        return score >= 95.0 and 10.0 <= change_24h <= 35.0 and oi_change >= 30.0 and funding <= 0.001

    def _dynamic_risk_limits(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Calculate adaptive exposure and correlation limits for the next entry."""
        base_exposure = float(self.config.max_total_exposure_pct)
        hard_cap = max(base_exposure, float(getattr(self.config, "dynamic_total_exposure_hard_cap_pct", base_exposure)))
        min_cap = min(base_exposure, float(getattr(self.config, "min_total_exposure_pct", 100.0)))
        max_correlated = 5
        if not getattr(self.config, "dynamic_exposure_enabled", True):
            return {
                "max_total_exposure": base_exposure,
                "max_correlated_positions": max_correlated,
                "mode": "固定",
                "reason": "动态敞口关闭",
            }

        score_data = signal.get("score") or {}
        score = float(score_data.get("total_score", score_data.get("total", 0)) if isinstance(score_data, dict) else score_data or 0)
        strategy_line = str(signal.get("strategy_line", "") or "")
        metrics = signal.get("metrics") or {}
        oi_change = abs(float(metrics.get("oi_24h_pct", metrics.get("oi_change_pct", 0)) or 0))
        funding = abs(float(metrics.get("funding_rate", metrics.get("funding_current", 0)) or 0))

        exposure = min(base_exposure, 180.0)
        mode = "标准"
        reasons: list[str] = []
        try:
            report = self._get_daily_report_snapshot(ttl_sec=90.0)
        except Exception as exc:
            report = {}
            logger.debug(f"dynamic exposure daily snapshot skipped: {exc}")

        closed = int(report.get("closed_trades", 0) or 0)
        win_rate = float(report.get("win_rate", 0) or 0)
        profit_factor = float(report.get("profit_factor", 0) or 0)
        total_pnl = float(report.get("total_pnl", 0) or 0)
        protection = report.get("entry_protection", {}) or {}
        protection_attempts = int(protection.get("attempts", 0) or 0)
        protection_ok_rate = float(protection.get("ok_rate", 100.0) or 100.0)

        if closed >= 5:
            if total_pnl < 0 and (profit_factor < 0.90 or win_rate < 38.0):
                exposure = min(exposure, 120.0)
                max_correlated = 3
                mode = "防守"
                reasons.append(f"今日弱势 PF={profit_factor:.2f} 胜率={win_rate:.0f}%")
            elif profit_factor < 1.10 or win_rate < 45.0:
                exposure = min(exposure, 150.0)
                max_correlated = 4
                mode = "谨慎"
                reasons.append(f"今日一般 PF={profit_factor:.2f} 胜率={win_rate:.0f}%")
            elif total_pnl > 0 and profit_factor >= 1.25 and win_rate >= 50.0:
                exposure = base_exposure
                mode = "进攻"
                reasons.append(f"今日顺风 PF={profit_factor:.2f} 胜率={win_rate:.0f}%")

        if protection_attempts >= 3 and protection_ok_rate < 85.0:
            exposure = min(exposure, 140.0)
            max_correlated = min(max_correlated, 4)
            mode = "保护单谨慎"
            reasons.append(f"保护单成功率={protection_ok_rate:.0f}%")

        if strategy_line == "趋势突破线" and score >= 95.0 and oi_change >= 30.0 and funding < self.config.max_abs_funding_rate * 0.60:
            exposure = min(hard_cap, max(exposure, base_exposure + 20.0))
            max_correlated = max(max_correlated, 4)
            mode = "强信号进攻"
            reasons.append(f"强趋势评分={score:.0f} OI={oi_change:.0f}%")
        elif score < 72.0:
            exposure = max(min_cap, min(exposure, base_exposure - 40.0))
            max_correlated = min(max_correlated, 4)
            reasons.append(f"评分偏普通={score:.0f}")

        exposure = max(min_cap, min(hard_cap, exposure))
        return {
            "max_total_exposure": round(exposure, 1),
            "max_correlated_positions": int(max_correlated),
            "mode": mode,
            "reason": "；".join(reasons) if reasons else "常规预算",
            "score": round(score, 1),
            "daily_closed": closed,
            "daily_profit_factor": round(profit_factor, 2),
            "daily_win_rate": round(win_rate, 1),
        }

    def _exit_profile_for_signal(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Return TP/SL profile for this entry signal."""
        strategy_line = str(signal.get("strategy_line", "") or "")
        if strategy_line == "趋势突破线":
            if self._is_strong_trend_signal(signal):
                return {
                    "name": "强趋势",
                    "take_profit_mode": "roi",
                    "take_profit_targets": [20.0, 40.0, 70.0],
                    "take_profit_ratios": [0.15, 0.30, 0.55],
                    "stop_loss_pct": 3.6,
                }
            return {
                "name": "普通趋势",
                "take_profit_mode": "roi",
                "take_profit_targets": [15.0, 30.0, 55.0],
                "take_profit_ratios": [0.20, 0.30, 0.50],
                "stop_loss_pct": 3.0,
            }
        if strategy_line == "均线二启线":
            return {
                "name": "均线二次启动",
                "take_profit_mode": "roi",
                "take_profit_targets": [12.0, 24.0, 45.0],
                "take_profit_ratios": [0.20, 0.30, 0.50],
                "stop_loss_pct": 2.8,
            }

        return {
            "name": "默认策略",
            "take_profit_mode": "roi",
            "take_profit_targets": [12.0, 24.0, 45.0],
            "take_profit_ratios": [0.20, 0.30, 0.50],
            "stop_loss_pct": 3.2,
        }

    def _strategy_stop_loss_pct(self, strategy_line: str = "") -> float:
        profile = self._strategy_profile(strategy_line)
        return max(0.5, float(self.config.stop_loss_pct) * profile["stop_multiplier"])

    def _strategy_stop_trigger_buffer_pct(self, strategy_line: str = "") -> float:
        if strategy_line == "趋势突破线":
            return max(0.0, float(self.config.breakout_stop_trigger_buffer_pct))
        if strategy_line == "均线二启线":
            return max(0.0, float(self.config.pullback_stop_trigger_buffer_pct))
        if strategy_line == "回踩确认线":
            return max(0.0, float(self.config.pullback_stop_trigger_buffer_pct))
        return max(0.0, float(self.config.stop_trigger_buffer_pct))

    def _calculate_local_take_profit_price(self, entry_price: float, side: str, target_pct: float) -> float:
        if self.config.take_profit_mode == "roi":
            price_move_pct = target_pct / max(self.config.leverage, 1)
        else:
            price_move_pct = target_pct
        if side == "BUY":
            return entry_price * (1 + price_move_pct / 100.0)
        return entry_price * (1 - price_move_pct / 100.0)

    def _recent_spike_reversal_reason(self, symbol: str, direction: str, current_price: float) -> str:
        """Avoid entering immediately after a short-timeframe wick reversal."""
        if not getattr(self.config, "spike_reversal_guard_enabled", True):
            return ""
        if current_price <= 0:
            return ""
        try:
            klines = get_klines(symbol, interval="5m", limit=8) or []
        except Exception as exc:
            logger.debug(f"{symbol} spike reversal guard skipped: {exc}")
            return ""
        if len(klines) < 4:
            return ""

        recent = klines[-4:]
        highs = [float(k.get("high", 0) or 0) for k in recent]
        lows = [float(k.get("low", 0) or 0) for k in recent]
        if not highs or not lows or min(lows) <= 0:
            return ""

        high = max(highs)
        low = min(lows)
        runup_pct = (high - low) / low * 100.0
        if direction == "LONG":
            pullback_pct = (high - current_price) / high * 100.0 if high > 0 else 0.0
        else:
            recent_low = low
            pullback_pct = (current_price - recent_low) / recent_low * 100.0 if recent_low > 0 else 0.0

        if runup_pct < float(self.config.spike_guard_min_runup_pct):
            return ""
        if pullback_pct < float(self.config.spike_guard_min_pullback_pct):
            return ""

        wick_ratio = 0.0
        reversal_candle = False
        for candle in recent[-2:]:
            open_price = float(candle.get("open", 0) or 0)
            close_price = float(candle.get("close", 0) or 0)
            high_price = float(candle.get("high", 0) or 0)
            low_price = float(candle.get("low", 0) or 0)
            candle_range = max(high_price - low_price, 0.0)
            if candle_range <= 0:
                continue
            if direction == "LONG":
                wick = high_price - max(open_price, close_price)
                wick_ratio = max(wick_ratio, wick / candle_range)
                if close_price < open_price and wick / candle_range >= float(self.config.spike_guard_min_wick_ratio):
                    reversal_candle = True
            else:
                wick = min(open_price, close_price) - low_price
                wick_ratio = max(wick_ratio, wick / candle_range)
                if close_price > open_price and wick / candle_range >= float(self.config.spike_guard_min_wick_ratio):
                    reversal_candle = True

        if wick_ratio < float(self.config.spike_guard_min_wick_ratio) and not reversal_candle:
            return ""

        return (
            f"短线冲高回落：5m拉升 {runup_pct:.2f}%，"
            f"距短线高点回落 {pullback_pct:.2f}%，影线占比 {wick_ratio:.0%}"
        )

    def _cancel_symbol_stale_protection(
        self,
        symbol: str,
        *,
        position_side: str | None = None,
        session_id: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        """Cancel all exchange-side protective orders that may outlive a position."""
        try:
            result = order_service.cancel_symbol_protective_orders(symbol, position_side=position_side)
        except Exception as exc:
            logger.warning(f"⚠️ {symbol} 保护条件单批量清理失败：{exc}")
            send_telegram_message(
                format_error_msg(
                    error_type="保护条件单批量清理失败",
                    message=str(exc),
                    symbol=symbol,
                    session_id=session_id,
                    component="protection_cleanup",
                )
            )
            return {"checked": 0, "canceled": [], "failed": [], "error": str(exc)}

        canceled = result.get("canceled", []) or []
        failed = result.get("failed", []) or []
        if canceled:
            logger.warning(f"🔕 {symbol} 已清理遗留保护条件单：{canceled} reason={reason}")
        if failed:
            logger.warning(f"⚠️ {symbol} 遗留保护条件单清理失败：{failed} reason={reason}")
            send_telegram_message(
                format_error_msg(
                    error_type="遗留保护条件单清理失败",
                    message=f"order_ids={failed}",
                    symbol=symbol,
                    session_id=session_id,
                    component="protection_cleanup",
                )
            )
        return result

    def _cancel_position_protection(self, position: Position):
        if position.stop_loss_order_id:
            if order_service.cancel_stop_loss(position.symbol, position.stop_loss_order_id):
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
            if order_service.cancel_protective(position.symbol, order_id):
                logger.info(f"🔕 已撤销 {position.symbol} 止盈委托：{order_id}")
            else:
                logger.warning(f"⚠️ {position.symbol} 止盈委托撤销失败：{order_id}")

        self._cancel_symbol_stale_protection(
            position.symbol,
            session_id=position.session_id,
            reason="position_closed",
        )
        position.stop_loss_order_id = 0
        position.take_profit_order_ids = []

    def _record_closed_trade_result(self, position: Position, pnl: float):
        """Record closed trade outcome without applying loss cooldowns."""
        if pnl >= 0:
            self._consecutive_losses = 0
        else:
            logger.info(f"{position.symbol} loss cooldown disabled | pnl={pnl:+.4f} pnl_pct={position.pnl_pct:+.2f}%")

    def _breakeven_offset_for_position(self, position: Position) -> float:
        """Lock profits faster after TP, with tighter rules for breakout entries."""
        base_offset = max(float(self.config.breakeven_offset_pct), 0.3)
        tp_count = max(int(position.partial_tp_count), 1)
        if position.strategy_line == "趋势突破线":
            if tp_count <= 1:
                return 0.3
            return base_offset + 0.20 + 0.15 * (tp_count - 2)
        return base_offset + 0.15 + 0.10 * (tp_count - 1)

    def _move_stop_to_breakeven(self, position: Position, remaining_qty: float) -> bool:
        """After first TP, move stop loss near breakeven so winners do not turn red."""
        if not self.config.breakeven_after_tp or remaining_qty <= 0:
            return False

        offset_pct = self._breakeven_offset_for_position(position)
        if position.side == "BUY":
            breakeven_price = position.entry_price * (1 + offset_pct / 100.0)
            close_side = "SELL"
            position_side = "LONG"
            if position.current_stop >= breakeven_price:
                return True
        else:
            breakeven_price = position.entry_price * (1 - offset_pct / 100.0)
            close_side = "BUY"
            position_side = "SHORT"
            if position.current_stop <= breakeven_price:
                return True

        latest_price = 0.0
        try:
            latest_price = float(self.get_current_prices([position.symbol]).get(position.symbol, 0) or 0)
        except Exception as exc:
            logger.debug(f"{position.symbol} breakeven latest price fetch skipped: {exc}")

        stop_price = breakeven_price
        # Binance rejects stop orders that would immediately trigger. Keep the
        # old hard stop unless the adjusted trigger still improves protection.
        if latest_price > 0:
            trigger_buffer = max(float(self.config.stop_trigger_buffer_pct), 0.10) / 100.0
            if position.side == "BUY":
                max_valid_stop = latest_price * (1 - trigger_buffer)
                stop_price = min(stop_price, max_valid_stop)
                if stop_price <= position.current_stop:
                    logger.info(
                        f"{position.symbol} breakeven move skipped: safe stop {stop_price:.8f} "
                        f"does not improve current stop {position.current_stop:.8f}"
                    )
                    return False
            else:
                min_valid_stop = latest_price * (1 + trigger_buffer)
                stop_price = max(stop_price, min_valid_stop)
                if stop_price >= position.current_stop:
                    logger.info(
                        f"{position.symbol} breakeven move skipped: safe stop {stop_price:.8f} "
                        f"does not improve current stop {position.current_stop:.8f}"
                    )
                    return False

        old_order_id = position.stop_loss_order_id
        sl_result = order_service.place_stop_loss(
            position.symbol,
            close_side,
            remaining_qty,
            stop_price,
            position_side=position_side,
            reduce_only=True,
        )
        if sl_result.status != "ERROR" and sl_result.order_id:
            position.stop_loss_order_id = sl_result.order_id
            position.stop_loss_price = stop_price
            position.current_stop = stop_price
            if old_order_id and not order_service.cancel_stop_loss(position.symbol, old_order_id):
                logger.warning(f"⚠️ {position.symbol} 新保本止损已生效，但旧止损撤销失败：{old_order_id}")
            logger.warning(f"🛡️ {position.symbol} TP后止损已移动到防守位：{sl_result.order_id} @ {stop_price:.8f}")
            return True

        position.protection_failures += 1
        position.last_protection_error = sl_result.message
        send_telegram_message(
            format_error_msg(
                error_type="防守止损移动失败，旧止损保留",
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

    def _adopt_existing_protection(self, position: Position) -> bool:
        """Adopt already-open exchange protection orders before placing new ones."""
        close_side = "SELL" if position.side == "BUY" else "BUY"
        position_side = "LONG" if position.side == "BUY" else "SHORT"
        try:
            snapshot = order_service.list_symbol_protective_orders(
                position.symbol,
                position_side=position_side,
                close_side=close_side,
            )
        except Exception as exc:
            logger.debug(f"{position.symbol} 保护单接管快照失败：{exc}")
            return False

        stop_orders = snapshot.get("stop_loss_orders") or []
        tp_orders = snapshot.get("take_profit_orders") or []
        open_stop_ids = [int(order.get("order_id", 0) or 0) for order in stop_orders if order.get("order_id")]
        open_tp_ids = [int(order.get("order_id", 0) or 0) for order in tp_orders if order.get("order_id")]
        open_ids = set(open_stop_ids + open_tp_ids)
        if not open_ids:
            return False

        changed = False
        if position.stop_loss_order_id and position.stop_loss_order_id not in open_ids:
            logger.info(f"{position.symbol} 本地止损单已不在交易所打开列表，等待补挂：{position.stop_loss_order_id}")
            position.stop_loss_order_id = 0
            changed = True
        if not position.stop_loss_order_id and open_stop_ids:
            position.stop_loss_order_id = open_stop_ids[0]
            stop_price = float((stop_orders[0] or {}).get("price", 0) or 0)
            if stop_price > 0:
                position.stop_loss_price = stop_price
                position.current_stop = stop_price
            logger.info(f"🛡️ {position.symbol} 已接管交易所现有止损单：{position.stop_loss_order_id}")
            changed = True

        current_tp_ids = [int(order_id) for order_id in position.take_profit_order_ids if int(order_id or 0) > 0]
        filtered_tp_ids = [order_id for order_id in current_tp_ids if order_id in open_ids]
        if current_tp_ids and len(filtered_tp_ids) != len(current_tp_ids):
            logger.info(f"{position.symbol} 本地止盈单含失效ID，已按交易所打开列表修正")
            current_tp_ids = filtered_tp_ids
            changed = True
        if not current_tp_ids and open_tp_ids:
            current_tp_ids = open_tp_ids
            changed = True
            logger.info(f"🎯 {position.symbol} 已接管交易所现有止盈单：{', '.join(str(x) for x in open_tp_ids)}")
        elif open_tp_ids:
            merged_tp_ids = list(dict.fromkeys(current_tp_ids + open_tp_ids))
            if merged_tp_ids != position.take_profit_order_ids:
                current_tp_ids = merged_tp_ids
                changed = True

        if current_tp_ids != position.take_profit_order_ids:
            position.take_profit_order_ids = current_tp_ids
        if position.take_profit_targets and tp_orders:
            sorted_orders = sorted(
                tp_orders,
                key=lambda item: float(item.get("price", 0) or 0),
                reverse=position.side != "BUY",
            )
            for target, order in zip(position.take_profit_targets, sorted_orders):
                order_id = int(order.get("order_id", 0) or 0)
                if order_id > 0:
                    target["order_id"] = order_id
                    target["status"] = "ADOPTED"
                    if float(order.get("quantity", 0) or 0) > 0:
                        target["quantity"] = float(order.get("quantity", 0) or 0)
                    if float(order.get("price", 0) or 0) > 0:
                        target["price"] = float(order.get("price", 0) or 0)
                    changed = True

        if self._position_protection_status(position)["protected"]:
            position.protection_failures = 0
            position.last_protection_error = ""
        return changed

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
        """Auto-repair incomplete protection orders instead of blocking all entries."""
        naked = []
        repaired = []
        failed = []
        
        for position in list(self.tracker.positions.values()):
            status = self._position_protection_status(position)
            if not status["protected"]:
                naked.append(position.symbol)
                # Try to auto-repair
                try:
                    protected = self._ensure_position_protection(position, refresh_guard=False)
                    if protected:
                        repaired.append(position.symbol)
                        logger.info(f"🛡️ {position.symbol} 保护单已自动修复")
                    else:
                        failed.append(position.symbol)
                        logger.warning(f"🛡️ {position.symbol} 保护单修复失败")
                except Exception as e:
                    failed.append(position.symbol)
                    logger.warning(f"🛡️ {position.symbol} 保护单修复异常: {e}")

        if failed:
            self._new_entries_suspended = True
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="保护单修复失败，暂停新开仓",
                        message=f"以下持仓保护单修复失败：{', '.join(failed)}。系统会继续管理已有持仓，但暂停新开仓。",
                        component="protection_guard",
                    )
                )
                self._new_entries_suspended_alert_sent = True
        else:
            if self._new_entries_suspended:
                logger.warning("🛡️ 所有持仓保护单已恢复，新开仓限制解除")
            self._new_entries_suspended = False
            self._new_entries_suspended_alert_sent = False
        
        if repaired:
            logger.info(f"🛡️ 保护单自动修复成功：{', '.join(repaired)}")

    def _ensure_position_protection(self, position: Position, refresh_guard: bool = True):
        """Place missing exchange-side SL/TP orders for tracked or restored positions."""
        close_side = "SELL" if position.side == "BUY" else "BUY"
        position_side = "LONG" if position.side == "BUY" else "SHORT"
        self._adopt_existing_protection(position)

        if not position.stop_loss_order_id:
            sl_result = order_service.place_stop_loss(
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

        active_tp_targets = position.take_profit_targets or [
            {
                "level": 1,
                "price": position.take_profit_price,
                "quantity": position.quantity,
                "ratio": 1.0,
                "target_roi_pct": position.target_roi_pct,
                "price_move_pct": abs(position.take_profit_price - position.entry_price) / position.entry_price * 100
                if position.entry_price
                else 0.0,
            }
        ]
        if position.take_profit_order_ids:
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
            tp_result = order_service.place_take_profit(
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
        if refresh_guard:
            self._refresh_protection_risk_switch()
        return protected

    def _notify_partial_take_profit(
        self,
        position: Position,
        reduced_qty: float,
        remaining_qty: float,
        price: float,
        exchange_realized_pnl: Optional[float] = None,
    ):
        if reduced_qty <= 0 or price <= 0:
            return

        entry_price = float(position.entry_price or 0.0)
        pnl_source = "交易所真实"
        if exchange_realized_pnl is not None:
            pnl = float(exchange_realized_pnl)
        else:
            if entry_price <= 0:
                logger.warning(
                    f"{position.symbol} partial TP notification skipped: missing entry price, "
                    f"qty={reduced_qty:.8f} price={price:.8f}; requesting state sync"
                )
                self._request_state_sync_from_ws("PARTIAL_TP_MISSING_ENTRY_PRICE", position.symbol)
                return
            if position.side == "BUY":
                pnl = (price - entry_price) * reduced_qty
            else:
                pnl = (entry_price - price) * reduced_qty
            pnl_source = "本地估算"

        notional = entry_price * reduced_qty
        pnl_pct = pnl / notional * 100 if notional > 0 else 0.0
        position.partial_tp_count += 1
        position.realized_pnl += pnl
        position.realized_exit_value += price * reduced_qty
        position.realized_quantity += reduced_qty
        if exchange_realized_pnl is not None:
            position.exchange_realized_pnl += float(exchange_realized_pnl)
            position.exchange_realized_exit_value += price * reduced_qty
            position.exchange_realized_quantity += reduced_qty
        position.last_partial_notify_qty = reduced_qty
        position.last_partial_notify_price = price
        position.last_partial_notify_ts = time.time()
        self.daily_pnl += pnl
        send_telegram_message(
            format_partial_take_profit_msg(
                symbol=position.symbol,
                direction="LONG" if position.side == "BUY" else "SHORT",
                entry_price=entry_price,
                exit_price=price,
                quantity=reduced_qty,
                remaining_quantity=remaining_qty,
                pnl=pnl,
                pnl_pct=pnl_pct,
                level=position.partial_tp_count,
                session_id=position.session_id,
                strategy_line=position.strategy_line,
                pnl_source=pnl_source,
            )
        )

    def _close_summary_from_realized_state(
        self,
        position: Position,
        remaining_qty: float,
        remaining_exit_price: float,
    ) -> tuple[float, float, float, float]:
        """Build a full close summary from prior partial fills plus the remaining fill."""
        total_qty = max(float(position.initial_quantity or 0.0), float(position.realized_quantity + remaining_qty))
        if total_qty <= 0:
            total_qty = max(float(position.realized_quantity), float(remaining_qty), 0.0)

        if position.side == "BUY":
            remaining_pnl = (remaining_exit_price - position.entry_price) * remaining_qty
        else:
            remaining_pnl = (position.entry_price - remaining_exit_price) * remaining_qty

        total_pnl = float(position.realized_pnl) + remaining_pnl
        total_exit_value = float(position.realized_exit_value) + remaining_exit_price * remaining_qty
        avg_exit_price = total_exit_value / total_qty if total_qty > 0 else remaining_exit_price
        entry_notional = position.entry_price * total_qty
        pnl_pct = total_pnl / entry_notional * 100 if entry_notional > 0 else 0.0
        return avg_exit_price, total_pnl, pnl_pct, remaining_pnl

    def _close_summary_from_exchange_realized(
        self,
        position: Position,
        fallback_exit_price: float,
    ) -> Optional[tuple[float, float, float, float]]:
        """Build close summary from exchange realized PnL collected by WS/userTrades."""
        realized_qty = float(getattr(position, "exchange_realized_quantity", 0.0) or 0.0)
        realized_pnl = float(getattr(position, "exchange_realized_pnl", 0.0) or 0.0)
        realized_exit_value = float(getattr(position, "exchange_realized_exit_value", 0.0) or 0.0)
        total_qty = max(float(position.initial_quantity or 0.0), realized_qty)
        if total_qty <= 0 or realized_qty <= 0:
            return None
        avg_exit_price = realized_exit_value / realized_qty if realized_exit_value > 0 else fallback_exit_price
        entry_notional = position.entry_price * total_qty
        pnl_pct = realized_pnl / entry_notional * 100 if entry_notional > 0 else 0.0
        remaining_pnl_delta = realized_pnl - float(position.realized_pnl or 0.0)
        return avg_exit_price, realized_pnl, pnl_pct, remaining_pnl_delta

    def _find_open_trade_for_session(self, symbol: str, session_id: str) -> tuple[Optional[TradeRecord], str]:
        """Find the best open trade row for a closing position."""
        open_trades = self.db.get_open_trades(mode=self.config.mode)
        fallback_trade: Optional[TradeRecord] = None
        for trade in open_trades:
            if trade.symbol != symbol:
                continue
            if fallback_trade is None:
                fallback_trade = trade
            if not session_id:
                continue
            notes_map = {}
            try:
                notes_map = self._parse_trade_notes(getattr(trade, "notes", "") or "")
            except Exception:
                notes_map = {}
            if notes_map.get("session_id", "") == session_id:
                return trade, "session_id"
        return fallback_trade, ("symbol" if fallback_trade else "none")

    def _persist_trade_exit(
        self,
        *,
        symbol: str,
        session_id: str,
        exit_price: float,
        exit_reason: str,
        pnl: float,
        pnl_pct: float,
        realized_pnl: float,
    ) -> bool:
        """Persist close result to DB while preventing cross-session contamination."""
        trade, matched_by = self._find_open_trade_for_session(symbol, session_id)
        if not trade:
            logger.warning(f"⚠️ 未找到可更新的开仓记录：{symbol} session={session_id}")
            return False

        if session_id and matched_by != "session_id":
            logger.warning(
                f"⚠️ 跳过DB平仓更新（会话不匹配）：{symbol} "
                f"expected_session={session_id} fallback_trade_id={trade.id}"
            )
            return False

        self.db.update_exit(
            trade_id=trade.id,
            exit_price=exit_price,
            exit_reason=exit_reason,
            pnl=pnl,
            pnl_pct=pnl_pct,
            realized_pnl=realized_pnl,
        )
        logger.info(f"📜 交易已更新 (ID: {trade.id}, matched_by={matched_by})")
        return True

    def _estimate_exchange_take_profit_close(self, position: Position) -> Optional[tuple[float, float, float, float]]:
        """Estimate final close when staged TP orders complete on the exchange between syncs."""
        remaining_qty = float(position.quantity or 0.0)
        if remaining_qty <= 0:
            return None

        targets = sorted(
            [target for target in (position.take_profit_targets or []) if float(target.get("price", 0) or 0) > 0],
            key=lambda item: int(item.get("level", 0) or 0),
        )
        if not targets:
            return None

        remaining_targets = [
            target for target in targets if int(target.get("level", 0) or 0) > int(position.partial_tp_count)
        ]
        if not remaining_targets:
            remaining_targets = [targets[-1]]

        weighted_exit_value = 0.0
        allocated_qty = 0.0
        qty_left = remaining_qty
        for target in remaining_targets:
            target_qty = float(target.get("quantity", 0) or 0)
            if target_qty <= 0:
                continue
            fill_qty = min(target_qty, qty_left)
            if fill_qty <= 0:
                continue
            weighted_exit_value += float(target.get("price", 0) or 0) * fill_qty
            allocated_qty += fill_qty
            qty_left -= fill_qty
            if qty_left <= 1e-9:
                break

        if qty_left > 1e-9:
            fallback_price = float(remaining_targets[-1].get("price", position.take_profit_price) or position.take_profit_price)
            weighted_exit_value += fallback_price * qty_left
            allocated_qty += qty_left

        if allocated_qty <= 0:
            return None

        remaining_exit_price = weighted_exit_value / allocated_qty
        return self._close_summary_from_realized_state(position, remaining_qty, remaining_exit_price)

    def _sync_protective_order_snapshot(self, position: Position):
        """Best-effort order snapshot check without blocking trading."""
        try:
            normal_orders = order_service.fetch_open(position.symbol)
            algo_orders = order_service.fetch_open_algo(position.symbol)
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

    def _entry_rejection_reason(self, symbol: str, direction: str, metrics: dict) -> str:
        """Reject obvious chase entries before expensive scoring and live orders."""
        is_major_symbol = symbol.upper() in self.config.major_symbols
        if getattr(self.config, "target_altcoins", False) and is_major_symbol:
            return "altcoin-only mode: skip major symbols"
        change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
        drawdown = float(metrics.get("drawdown_from_24h_high_pct", 0.0) or 0.0)
        range_position = float(metrics.get("range_position_24h_pct", 50.0) or 50.0)
        funding = float(metrics.get("funding_rate", 0.0) or 0.0)
        oi_change = float(metrics.get("oi_24h_pct", 0.0) or 0.0)
        volume_mult = float(metrics.get("volume_24h_mult", 1.0) or 1.0)
        required_pullback = self._required_pullback_pct(metrics)

        if abs(funding) >= self.config.max_abs_funding_rate:
            return f"资金费率过热 {funding * 100:.3f}%"
        if oi_change >= self.config.max_oi_change_pct:
            return f"OI过热 {oi_change:.1f}%"

        if direction == "LONG":
            if change_24h <= (-15 if is_major_symbol else -12):
                return f"大跌中不接多 {change_24h:.1f}%"
            if change_24h >= (
                self.config.max_chase_change_pct + 10 if is_major_symbol else self.config.max_chase_change_pct
            ):
                return f"24h涨幅过大 {change_24h:.1f}%"
            if (
                (not is_major_symbol)
                and change_24h >= 12
                and drawdown < required_pullback
                and oi_change < self.config.momentum_entry_min_oi_pct
            ):
                return f"未回踩，距24h高点仅回落 {drawdown:.1f}%"
            if change_24h >= 8 and range_position >= (96.0 if is_major_symbol else self.config.max_range_position_pct):
                return f"价格处于24h区间高位 {range_position:.1f}%"
        elif direction == "SHORT":
            if change_24h >= (15 if is_major_symbol else 12):
                return f"大涨中不追空 {change_24h:.1f}%"
            if change_24h <= (
                -(self.config.max_chase_change_pct + 10) if is_major_symbol else -self.config.max_chase_change_pct
            ):
                return f"24h跌幅过大 {change_24h:.1f}%"
            if change_24h <= -12 and range_position <= (4.0 if is_major_symbol else 100 - self.config.max_range_position_pct):
                return f"价格处于24h区间低位 {range_position:.1f}%"

        if volume_mult < (0.6 if is_major_symbol else 0.8) and abs(change_24h) >= 10:
            return f"量能不足 volume_mult={volume_mult:.2f}"
        return ""

    def execute_entry(self, signal: dict) -> Optional[Position]:
        """执行开仓 - 奥丁的长矛"""
        symbol = signal["symbol"]
        direction = signal["direction"]
        price = float(signal.get("price", 0) or 0)
        position: Optional[Position] = None
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        entry_status = str(signal.get("entry_status", "") or "")
        status_text = str(signal.get("entry_status_text", "") or "")
        watch_stage = str(signal.get("watch_stage", "") or "")
        entry_note = str(signal.get("entry_note", "") or "")
        score_conf = str((signal.get("score") or {}).get("confidence", "") or "")
        guard_text = f"{status_text}|{watch_stage}|{entry_note}|{score_conf}"

        if entry_status != "ready":
            logger.warning(f"entry guard reject {symbol}: entry_status={entry_status}")
            return None
        if price <= 0:
            logger.warning(f"entry guard reject {symbol}: invalid price={price}")
            return None
        if any(token in guard_text for token in ("失效", "淘汰", "移出监控", "状态变更")):
            logger.warning(f"entry guard reject {symbol}: blocked by monitor state [{guard_text}]")
            return None

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
            trading_signal = execution_service.build_trading_signal(
                symbol=symbol,
                stage=signal["stage"],
                direction=direction,
                entry_price=price,
                metrics=signal["metrics"],
            )
            session_id = self._new_session_id(symbol)
            risk_level = "未评估"
            strategy_line = str(signal.get("strategy_line", "回踩确认线") or "回踩确认线")
            strategy_profile = self._strategy_profile(strategy_line)
            exit_profile = self._exit_profile_for_signal(signal)
            exit_profile_name = str(exit_profile.get("name", "默认策略") or "默认策略")
            take_profit_mode_for_trade = str(exit_profile.get("take_profit_mode", self.config.take_profit_mode) or self.config.take_profit_mode)
            stop_loss_pct = float(exit_profile.get("stop_loss_pct", self._strategy_stop_loss_pct(strategy_line)) or self._strategy_stop_loss_pct(strategy_line))
            stop_trigger_buffer_pct = self._strategy_stop_trigger_buffer_pct(strategy_line)
            score_data = signal.get("score") or {}
            score = float(score_data.get("total_score", score_data.get("total", 0)) if isinstance(score_data, dict) else score_data or 0)
            direction_label = format_direction_label(direction)

            if not execution_service.should_trade(trading_signal):
                if score >= 85.0:
                    send_telegram_message(
                        format_error_msg(
                            error_type="执行服务拒绝开仓",
                            message=(
                                "阶段：开仓前执行过滤\n"
                                "原因：信号未满足执行服务的最终下单条件\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                return None

            if symbol not in self.tracker.positions:
                step_started = time.perf_counter()
                self._cancel_symbol_stale_protection(symbol, session_id=session_id, reason="before_new_entry")
                self._record_latency_step(latency_steps, "stale_protection_cleanup", step_started)

            step_started = time.perf_counter()
            try:
                balance_hint = signal.get("_balance_hint")
                if balance_hint is not None:
                    balance = float(balance_hint)
                else:
                    balance_info = self._get_account_info_cached(ttl_sec=3.0)
                    balance = float(balance_info.get("availableBalance", 10000))
            except Exception as e:
                logger.error(f"entry guard reject {symbol}: account balance query failed: {e}")
                send_telegram_message(
                    format_error_msg(
                        error_type="账户查询失败，拒绝开仓",
                        message=(
                            "阶段：账户余额查询\n"
                            f"原因：{format_entry_failure_detail(e)}\n"
                            f"方向：{direction_label}\n"
                            f"策略：{strategy_line}｜{exit_profile_name}\n"
                            f"评分：{score:.1f}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="account_query",
                    )
                )
                return None
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
                    if score >= 85.0:
                        send_telegram_message(
                            format_error_msg(
                                error_type="价格偏移过大，拒绝开仓",
                                message=(
                                    "阶段：下单前价格复查\n"
                                    f"原因：{format_entry_failure_detail('价格偏移过大')}\n"
                                    f"方向：{direction_label}\n"
                                    f"信号价：{price:.8f}\n"
                                    f"最新价：{latest_price:.8f}\n"
                                    f"偏移：{slippage_pct:.2f}% > {self.config.max_entry_slippage_pct:.2f}%\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="execute_entry",
                            )
                        )
                    return None
                if latest_price > 0:
                    price = latest_price
                    trading_signal.entry_price = latest_price
            spike_reason = self._recent_spike_reversal_reason(symbol, direction, price)
            if spike_reason:
                logger.warning(f"🧊 {symbol} 开仓前过滤：{spike_reason}")
                if score >= 85.0:
                    send_telegram_message(
                        format_error_msg(
                            error_type="短线插针风险，拒绝开仓",
                            message=(
                                "阶段：下单前K线复查\n"
                                f"原因：{spike_reason}\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                return None
            self._record_latency_step(latency_steps, "price_recheck", step_started)

            quantity = None
            stop_loss = None
            capital_plan = None
            risk_balance = balance
            entry_risk_pct = self.config.risk_per_trade_pct
            entry_max_position_pct = self.config.max_position_pct
            entry_leverage = self.config.leverage

            step_started = time.perf_counter()
            try:
                existing_positions = []
                for pos_symbol, pos in self.tracker.positions.items():
                    existing_positions.append(
                        {
                            "symbol": pos_symbol,
                            "side": "LONG" if pos.side == "BUY" else "SHORT",
                            "position_value": pos.entry_price * pos.quantity,
                        }
                    )
                dynamic_limits = self._dynamic_risk_limits(signal)
                logger.info(
                    f"🛡️ {symbol} 风控预算：敞口={dynamic_limits['max_total_exposure']}% "
                    f"相关仓={dynamic_limits['max_correlated_positions']} "
                    f"模式={dynamic_limits['mode']} 原因={dynamic_limits['reason']}"
                )

                capital_plan = capital_allocator.build_plan(
                    config=self.config,
                    signal=signal,
                    exit_profile=exit_profile,
                    dynamic_limits=dynamic_limits,
                    account_balance=balance,
                    day_start_balance=self.day_start_balance,
                    daily_pnl=self.daily_pnl,
                    daily_report=self._get_daily_report_snapshot(ttl_sec=90.0),
                    market_style_mode=self._market_style_mode,
                )
                logger.info(
                    f"🏦 {symbol} 资金分配：{capital_plan.mode} | "
                    f"风险={capital_plan.risk_per_trade_pct:.2f}% 杠杆={capital_plan.leverage}x "
                    f"仓位上限={capital_plan.max_position_pct:.1f}% 敞口={capital_plan.max_total_exposure_pct:.1f}% "
                    f"EV={capital_plan.expected_rr:.2f}R 原因={capital_plan.reason}"
                )
                if not capital_plan.allowed:
                    if score >= 85.0:
                        send_telegram_message(
                            format_error_msg(
                                error_type="资本分配拒绝",
                                message=(
                                    "阶段：资金分配\n"
                                    f"原因：{capital_plan.reason}\n"
                                    f"方向：{direction_label}\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}\n"
                                    f"资金档位：{capital_plan.mode}\n"
                                    f"期望收益：{capital_plan.expected_reward_pct:.2f}%\n"
                                    f"止损：{stop_loss_pct:.2f}%"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="risk_assessment",
                            )
                        )
                    return None

                risk_balance = capital_plan.effective_balance if capital_plan.effective_balance > 0 else balance
                entry_risk_pct = capital_plan.risk_per_trade_pct
                entry_max_position_pct = capital_plan.max_position_pct
                entry_leverage = capital_plan.leverage

                risk_config = risk_service.build_config(
                    risk_per_trade_pct=entry_risk_pct,
                    base_stop_loss_pct=stop_loss_pct,
                    base_take_profit_pct=self.config.take_profit_pct * strategy_profile["tp_multiplier"],
                    max_position_pct=entry_max_position_pct,
                    max_total_exposure=float(capital_plan.max_total_exposure_pct),
                    max_correlated_positions=int(capital_plan.max_correlated_positions),
                )

                risk_result = risk_service.assess(
                    symbol=symbol,
                    side="LONG" if direction == "LONG" else "SHORT",
                    entry_price=price,
                    account_balance=risk_balance,
                    existing_positions=existing_positions,
                    config=risk_config,
                )

                if not risk_result.get("can_open", False):
                    warnings = risk_result.get("warnings", [])
                    logger.warning(f"🛡️ {symbol} 风控拒绝：{warnings}")
                    send_telegram_message(
                        format_error_msg(
                            error_type="强信号风控拒绝",
                            message=(
                                "阶段：风控评估\n"
                                f"原因：{'; '.join(str(item) for item in warnings) or '风控未通过'}\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}\n"
                                f"资金档位：{capital_plan.mode}\n"
                                f"敞口上限：{capital_plan.max_total_exposure_pct}%\n"
                                f"相关仓上限：{capital_plan.max_correlated_positions}\n"
                                f"说明：{capital_plan.reason}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="risk_assessment",
                        )
                    )
                    return None

                logger.info(
                    f"🛡️ {symbol} 风控评分：{risk_result.get('risk_score', 0)}/100 ({risk_result.get('risk_level', 'UNKNOWN')})"
                )
                risk_level = risk_result.get("risk_level") or "未评估"

                position_size = risk_result.get("position_size", {})
                quantity = position_size.get("quantity")
                stop_loss = risk_result.get("stop_loss", {}).get("stop_loss")
                position_value = float(position_size.get("position_value", 0) or 0)

                if quantity is not None and quantity <= 0:
                    logger.warning(f"🛡️ {symbol} 仓位计算失败")
                    send_telegram_message(
                        format_error_msg(
                            error_type="仓位计算失败，拒绝开仓",
                            message=(
                                "阶段：风控仓位计算\n"
                                "原因：计算出的下单数量小于等于 0，可能是余额、精度或最小名义价值限制导致\n"
                                f"方向：{direction_label}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}\n"
                                f"余额：{balance:.2f} USDT\n"
                                f"风险余额：{risk_balance:.2f} USDT"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="risk_assessment",
                        )
                    )
                    return None

                if position_value > 0 and not self._passes_liquidity_filter(symbol, position_value):
                    if score >= 85.0:
                        send_telegram_message(
                            format_error_msg(
                                error_type="流动性过滤失败，拒绝开仓",
                                message=(
                                    "阶段：流动性检查\n"
                                    f"原因：{format_entry_failure_detail('流动性过滤未通过')}\n"
                                    f"方向：{direction_label}\n"
                                    f"策略：{strategy_line}｜{exit_profile_name}\n"
                                    f"评分：{score:.1f}\n"
                                    f"计划名义仓位：{position_value:.2f} USDT"
                                ),
                                symbol=symbol,
                                session_id=session_id,
                                component="risk_assessment",
                            )
                        )
                    return None

                logger.info(
                    f"🔍 {symbol} 风控参数: 余额=${balance:.2f}, 风险余额=${risk_balance:.2f}, 杠杆={capital_plan.leverage}x, "
                    f"名义仓位=${position_size.get('position_value', 0):.2f}, "
                    f"数量={quantity}, 止损=${(stop_loss or 0):.4f}"
                )

            except Exception as e:
                logger.warning(f"🛡️ 风控评估失败 {symbol}: {e}，回退到执行器默认计算")
            self._record_latency_step(latency_steps, "risk_assessment", step_started)

            take_profit_target_pcts = [float(item) for item in (exit_profile.get("take_profit_targets") or [])]
            take_profit_ratios = [float(item) for item in (exit_profile.get("take_profit_ratios") or [])]
            if not take_profit_target_pcts or not take_profit_ratios:
                take_profit_target_pcts, take_profit_ratios = self._build_take_profit_plan(strategy_line)
            step_started = time.perf_counter()
            result = execution_service.execute_entry_trade(
                signal=trading_signal,
                account_balance=risk_balance,
                risk_per_trade_pct=entry_risk_pct,
                stop_loss_pct=stop_loss_pct,
                max_position_pct=entry_max_position_pct,
                leverage=entry_leverage,
                quantity=quantity,
                stop_loss_price=stop_loss,
                take_profit_target_pcts=take_profit_target_pcts,
                take_profit_ratios=take_profit_ratios,
                take_profit_mode=take_profit_mode_for_trade,
                stop_trigger_buffer_pct=stop_trigger_buffer_pct,
                defer_protection_orders=False,
            )
            self._record_latency_step(latency_steps, "execute_trade", step_started)

            if result.get("action") != "EXECUTED":
                failure_reason = str(result.get("reason", "Unknown") or "Unknown")
                logger.warning(f"❌ {symbol} 开仓失败：{failure_reason}")
                send_telegram_message(
                    format_error_msg(
                        error_type="开仓下单失败",
                        message=(
                            "阶段：交易所下单\n"
                            f"原因：{format_entry_failure_detail(failure_reason)}\n"
                            f"方向：{direction_label}\n"
                            f"数量：{quantity}\n"
                            f"杠杆：{entry_leverage}x\n"
                            f"策略：{strategy_line}｜{exit_profile_name}\n"
                            f"评分：{score:.1f}\n"
                            f"计划入场价：{price:.8f}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="execute_entry_trade",
                    )
                )
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            entry_order = result.get("entry_order", {})
            executed_entry_price = float(entry_order.get("executed_price", price) or price)
            order_status = entry_order.get("status", "UNKNOWN")
            actual_quantity = float(entry_order.get("quantity", 0) or result.get("quantity", 0))

            if quantity is None:
                quantity = actual_quantity if actual_quantity > 0 else 0

            if order_status == "PARTIALLY_FILLED":
                logger.warning(f"⚠️ {symbol} 部分成交！请求数量：{quantity}，实际成交：{actual_quantity}")
                if actual_quantity < quantity * 0.5:
                    logger.error(f"❌ {symbol} 部分成交比例过低，放弃持仓")
                    send_telegram_message(
                        format_error_msg(
                            error_type="开仓部分成交异常",
                            message=(
                                "阶段：成交结果确认\n"
                                "原因：部分成交比例过低，系统放弃继续持仓\n"
                                f"方向：{direction_label}\n"
                                f"请求数量：{quantity}\n"
                                f"实际成交：{actual_quantity}\n"
                                f"策略：{strategy_line}｜{exit_profile_name}\n"
                                f"评分：{score:.1f}"
                            ),
                            symbol=symbol,
                            session_id=session_id,
                            component="execute_entry",
                        )
                    )
                    self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                    return None

            take_profit_targets = result.get("take_profit_orders", [])
            take_profit_prices = result.get("take_profit_prices", [])
            target_roi_pcts = result.get("take_profit_roi_pcts", [])
            target_price_pcts = result.get("take_profit_price_pcts", take_profit_target_pcts)
            primary_target_roi_pct = float(
                target_roi_pcts[0] if target_roi_pcts else self.config.take_profit_pct * self.config.leverage
            )
            primary_price_move_pct = float(target_price_pcts[0] if target_price_pcts else self.config.take_profit_pct)
            tp_price = float(take_profit_prices[0] if take_profit_prices else executed_entry_price)
            if direction == "LONG":
                side = "BUY"
            else:
                side = "SELL"

            stop_loss_order = result.get("stop_loss_order", {})
            protection_deferred = bool(result.get("protection_deferred", False))
            protection_errors: list[str] = []
            if protection_deferred:
                protection_errors.append("protection_deferred=true")

            stop_loss_order_id = int(stop_loss_order.get("order_id", 0) or 0)
            stop_loss_status = str(stop_loss_order.get("status", "") or "").upper()
            if stop_loss_order_id <= 0 or stop_loss_status == "ERROR":
                sl_message = str(stop_loss_order.get("message", "") or "").strip()
                protection_errors.append(
                    f"stop_loss status={stop_loss_status or 'UNKNOWN'} id={stop_loss_order_id}"
                    + (f" msg={sl_message}" if sl_message else "")
                )

            for idx, target in enumerate(take_profit_targets, start=1):
                tp_order_id = int(target.get("order_id", 0) or 0)
                tp_status = str(target.get("status", "") or "").upper()
                if tp_order_id <= 0 or tp_status == "ERROR":
                    tp_message = str(target.get("message", "") or "").strip()
                    protection_errors.append(
                        f"tp{idx} status={tp_status or 'UNKNOWN'} id={tp_order_id}"
                        + (f" msg={tp_message}" if tp_message else "")
                    )

            if protection_errors:
                close_side = "SELL" if direction == "LONG" else "BUY"
                flat_result = quick_close_position(
                    symbol=symbol,
                    side=close_side,
                    quantity=actual_quantity,
                    reason="ENTRY_PROTECTION_FAILED",
                )
                detail = "; ".join(protection_errors)
                logger.error(f"entry protection hard-fail {symbol}: {detail} | flat={flat_result}")
                protection_event = build_execution_event(
                    event="entry_protection_failed",
                    symbol=symbol,
                    direction=direction,
                    session_id=session_id,
                    metrics={
                        "detail": detail,
                        "flat_success": bool(flat_result.get("success")),
                        "flat_order_id": int(flat_result.get("order_id", 0) or 0),
                        "flat_elapsed_ms": float(flat_result.get("elapsed_ms", 0) or 0),
                        "entry_order_id": int(result.get("order_id", 0) or 0),
                        "entry_price": executed_entry_price,
                        "entry_quantity": actual_quantity,
                    },
                )
                logger.info(f"execution_event {message_signature(protection_event)}")
                feature_store.append_event(protection_event)
                send_telegram_message(
                    format_error_msg(
                        error_type="开仓保护单失败已回滚",
                        message=(
                            "阶段：开仓后保护单确认\n"
                            f"原因：{format_protection_failure_detail(detail)}\n"
                            f"方向：{direction_label}\n"
                            f"成交数量：{actual_quantity}\n"
                            f"成交价：{executed_entry_price:.8f}\n"
                            f"回滚结果：{'已尝试市价平仓' if flat_result else '未获得回滚结果'}\n"
                            f"原始信息：{detail}"
                        ),
                        symbol=symbol,
                        session_id=session_id,
                        component="entry_protection",
                    )
                )
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            oi_funding = signal.get("oi_funding") or {}
            leverage_applied = int(result.get("leverage_applied", entry_leverage) or entry_leverage or self.config.leverage)
            position = Position(
                symbol=symbol,
                side=side,
                entry_price=executed_entry_price,
                quantity=actual_quantity,
                order_id=result.get("order_id", 0),
                stop_loss_price=result.get("stop_loss_price", 0),
                take_profit_price=tp_price,
                entry_time=datetime.now(),
                stage_at_entry=signal["stage"],
                strategy_line=strategy_line,
                stop_loss_order_id=stop_loss_order_id,
                session_id=session_id,
                oi_funding=oi_funding,
                entry_score=dict(signal.get("score") or {}),
                entry_metrics=dict(signal.get("metrics") or {}),
                target_roi_pct=primary_target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=[
                    int(item.get("order_id", 0) or 0) for item in take_profit_targets if item.get("order_id")
                ],
                leverage=leverage_applied,
            )

            from telegram_notifier import format_open_position_msg

            msg = format_open_position_msg(
                symbol=symbol,
                direction=direction,
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=leverage_applied,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                risk_amount=result.get("risk_amount_usdt", 0),
                risk_pct=entry_risk_pct,
                score=score,
                risk_level=risk_level,
                session_id=session_id,
                strategy_line=f"{strategy_line}｜{exit_profile_name}",
                oi_funding=oi_funding,
                target_roi_pct=primary_target_roi_pct,
                price_move_pct=primary_price_move_pct,
                take_profit_targets=take_profit_targets,
                capital_plan=capital_plan.to_dict() if capital_plan else None,
            )
            notify_ok = send_telegram_message(msg)
            if not notify_ok:
                logger.error(f"entry notify failed: {symbol} session={session_id}")
                notify_event = build_execution_event(
                    event="entry_notify_failed",
                    symbol=symbol,
                    direction=direction,
                    session_id=session_id,
                    metrics={"reason": "telegram_send_failed"},
                )
                feature_store.append_event(notify_event)

            step_started = time.perf_counter()
            self._ensure_position_protection(position)
            self._send_protection_status(position, source="entry_confirm", force=True)
            self._record_latency_step(latency_steps, "protection_confirm", step_started)
            protection_ok_event = build_execution_event(
                event="entry_protection_ok",
                symbol=symbol,
                direction=direction,
                session_id=session_id,
                metrics={
                    "stop_loss_order_id": int(position.stop_loss_order_id or 0),
                    "take_profit_order_count": len(position.take_profit_order_ids or []),
                    "entry_order_id": int(position.order_id or 0),
                    "entry_price": float(position.entry_price or 0),
                    "entry_quantity": float(position.quantity or 0),
                },
            )
            feature_store.append_event(protection_ok_event)

            notes_parts = [
                f"session_id={session_id}",
                f"entry_gate={signal.get('_entry_gate_override') or 'normal'}",
                f"entry_gate_note={signal.get('_entry_gate_note') or ''}",
                f"strategy_line={strategy_line}",
                f"exit_profile={exit_profile_name}",
                f"risk_level={risk_level}",
                f"target_roi_pct={primary_target_roi_pct}",
                f"price_move_pct={primary_price_move_pct}",
                f"take_profit_mode={take_profit_mode_for_trade}",
                f"tp_multiplier={self._tp_multiplier}",
                f"strategy_tp_multiplier={strategy_profile['tp_multiplier']}",
                f"strategy_stop_pct={stop_loss_pct}",
                f"stop_trigger_buffer_pct={stop_trigger_buffer_pct}",
                f"leverage_applied={leverage_applied}",
                f"capital_plan={json.dumps(capital_plan.to_dict() if capital_plan else {}, ensure_ascii=False, separators=(',', ':'))}",
                f"oi_funding_bonus={float(oi_funding.get('score_bonus', 0) or 0):.2f}",
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
                leverage=leverage_applied,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                entry_time=position.entry_time.isoformat(),
                mode=self.config.mode,
                market_snapshot={
                    **(signal.get("metrics", {}) or {}),
                    "_oi_funding": oi_funding,
                    "_entry_score": signal.get("score", {}) or {},
                    "_entry_gate": signal.get("_entry_gate_override") or "normal",
                    "_entry_gate_note": signal.get("_entry_gate_note") or "",
                    "_leverage_applied": leverage_applied,
                    "_capital_plan": capital_plan.to_dict() if capital_plan else {},
                },
                notes=";".join(notes_parts),
            )
            step_started = time.perf_counter()
            trade_id = self.db.add_trade(trade)
            logger.info(f"交易已记录 (ID: {trade_id})")
            entry_event = build_execution_event(
                event="entry_opened",
                symbol=symbol,
                direction=direction,
                session_id=session_id,
                metrics={
                    "trade_id": trade_id,
                    "entry_price": executed_entry_price,
                    "quantity": position.quantity,
                    "stop_loss": position.stop_loss_price,
                    "take_profit": tp_price,
                },
            )
            logger.info(f"execution_event {message_signature(entry_event)}")
            feature_store.append_event(entry_event)
            self._record_latency_step(latency_steps, "db_write", step_started)
            self._emit_latency_trace("execute_entry", trace_started, latency_steps, symbol=symbol)
            return position

        except Exception as e:
            logger.error(f"❌ {symbol} 开仓流程异常：{e}", exc_info=True)
            score_text = f"{score:.1f}" if "score" in locals() else "未知"
            send_telegram_message(
                format_error_msg(
                    error_type="开仓流程异常",
                    message=(
                        "阶段：开仓流程总异常\n"
                        f"原因：{format_entry_failure_detail(e)}\n"
                        f"方向：{format_direction_label(direction) if 'direction' in locals() else '未知方向'}\n"
                        f"策略：{strategy_line if 'strategy_line' in locals() else '未知策略'}\n"
                        f"评分：{score_text}"
                    ),
                    symbol=symbol,
                    session_id=session_id if "session_id" in locals() else "",
                    component="execute_entry",
                )
            )
            if position is not None:
                logger.error(f"entry post-process failed but position exists: {symbol} session={position.session_id}")
                self._emit_latency_trace("execute_entry_post_error", trace_started, latency_steps, symbol=symbol)
                return position
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

        try:
            step_started = time.perf_counter()
            result = order_service.place_market(
                symbol,
                close_side,
                position.quantity,
                position_side=position_side,
                reduce_only=True,
            )
            if result.status != "FILLED":
                fast_result = quick_close_position(
                    symbol=symbol,
                    side=close_side,
                    quantity=position.quantity,
                    reason=reason,
                )
                if fast_result.get("success"):
                    fallback_price = float(fast_result.get("executed_price", 0) or current_price or position.entry_price)
                    result = OrderResult(
                        symbol=symbol,
                        side=close_side,
                        quantity=float(fast_result.get("quantity", position.quantity) or position.quantity),
                        executed_price=fallback_price,
                        order_id=int(fast_result.get("order_id", 0) or 0),
                        status="FILLED",
                        message=f"fast_exit_fallback elapsed={fast_result.get('elapsed_ms')}ms",
                    )
            self._record_latency_step(latency_steps, "market_close", step_started)

            if result.status == "FILLED":
                exit_price, pnl, pnl_pct, remaining_pnl = self._close_summary_from_realized_state(
                    position,
                    position.quantity,
                    result.executed_price,
                )
                position.exit_price = exit_price
                position.exit_time = datetime.now()
                position.exit_reason = reason
                position.pnl = pnl
                position.pnl_pct = pnl_pct

                self.tracker.remove_position(symbol)
                self.daily_pnl += remaining_pnl
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
                        exit_price=exit_price,
                        quantity=position.initial_quantity,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        reason=reason,
                        duration_hours=duration_hours,
                        session_id=position.session_id,
                        strategy_line=position.strategy_line,
                        oi_funding=getattr(position, "oi_funding", None),
                        roi_pct=pnl_pct * max(int(getattr(position, "leverage", 0) or self.config.leverage), 1),
                        price_move_pct=pnl_pct,
                    )
                )
                self._record_latency_step(latency_steps, "telegram_notify", step_started)

                step_started = time.perf_counter()
                self._persist_trade_exit(
                    symbol=symbol,
                    session_id=position.session_id,
                    exit_price=exit_price,
                    exit_reason=reason,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    realized_pnl=pnl,
                )
                self._record_latency_step(latency_steps, "db_update", step_started)
                close_direction = "LONG" if position.side == "BUY" else "SHORT"
                close_event = build_execution_event(
                    event="position_closed",
                    symbol=symbol,
                    direction=close_direction,
                    session_id=position.session_id,
                    metrics={
                        "exit_reason": reason,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                    },
                )
                logger.info(f"execution_event {message_signature(close_event)}")
                feature_store.append_event(close_event)
                review = build_trade_review(
                    symbol=symbol,
                    session_id=position.session_id,
                    direction=close_direction,
                    stage=position.stage_at_entry,
                    strategy_line=position.strategy_line,
                    entry_price=position.entry_price,
                    exit_price=exit_price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    exit_reason=reason,
                    hold_hours=duration_hours,
                    score=getattr(position, "entry_score", {}) or {},
                    metrics=getattr(position, "entry_metrics", {}) or {},
                    oi_funding=getattr(position, "oi_funding", {}) or {},
                )
                feature_store.append_review(review)
                try:
                    self.db.save_trade_review(
                        review,
                        session_id=position.session_id,
                        symbol=symbol,
                        mode=self.config.mode,
                    )
                except Exception as e:
                    logger.warning(f"trade review db save failed {symbol}: {e}")
                logger.info(f"trade_review {message_signature(review)}")
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


