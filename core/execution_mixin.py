"""Execution and protection logic mixin for the trading engine."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

from binance_trading_executor import (
    OrderResult,
    TradingSignal,
    cancel_protective_order,
    cancel_stop_loss_order,
    execute_trade,
    fetch_open_algo_orders,
    fetch_open_orders,
    place_market_order,
    place_stop_loss_order,
    place_take_profit_order,
    should_trade,
)
from feature_store import build_trade_review, feature_store
from risk_manager import RiskConfig, assess_trade_risk
from speed_executor import quick_close_position
from telegram_notifier import (
    format_close_position_msg,
    format_error_msg,
    format_partial_take_profit_msg,
    format_protection_status_msg,
    send_telegram_message,
)
from trade_logger import TradeRecord
from .monitoring import build_execution_event, message_signature
from .models import Position

logger = logging.getLogger(__name__)


class ExecutionMixin:
    """Open/close execution and protective order lifecycle."""

    def _strategy_profile(self, strategy_line: str) -> dict[str, float]:
        if strategy_line == "瓒嬪娍绐佺牬绾?:
            return {
                "tp_multiplier": self.config.breakout_tp_multiplier,
                "stop_multiplier": self.config.breakout_stop_multiplier,
            }
        return {
            "tp_multiplier": self.config.pullback_tp_multiplier,
            "stop_multiplier": self.config.pullback_stop_multiplier,
        }

    def _strategy_take_profit_ratios(self, strategy_line: str, levels_count: int) -> list[float]:
        if strategy_line == "瓒嬪娍绐佺牬绾?:
            base_ratios = [0.40, 0.35, 0.25]
        else:
            base_ratios = [0.55, 0.30, 0.15]
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
        for multiplier in (0.5, 1.0, 1.5):
            target_pct = round(base_pct * multiplier, 2)
            if target_pct > 0 and target_pct not in staged_levels:
                staged_levels.append(target_pct)

        ratios = self._strategy_take_profit_ratios(strategy_line, len(staged_levels))
        return staged_levels, ratios

    def _strategy_stop_loss_pct(self, strategy_line: str = "") -> float:
        profile = self._strategy_profile(strategy_line)
        return max(0.5, float(self.config.stop_loss_pct) * profile["stop_multiplier"])

    def _strategy_stop_trigger_buffer_pct(self, strategy_line: str = "") -> float:
        if strategy_line == "瓒嬪娍绐佺牬绾?:
            return max(0.0, float(self.config.breakout_stop_trigger_buffer_pct))
        if strategy_line == "鍥炶俯纭绾?:
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

    def _cancel_position_protection(self, position: Position):
        if position.stop_loss_order_id:
            if cancel_stop_loss_order(position.symbol, position.stop_loss_order_id):
                logger.info(f"馃敃 宸叉挙閿€ {position.symbol} 淇濇姢姝㈡崯鍗曪細{position.stop_loss_order_id}")
            else:
                logger.warning(f"鈿狅笍 {position.symbol} 淇濇姢姝㈡崯鍗曟挙閿€澶辫触锛歿position.stop_loss_order_id}")
                send_telegram_message(
                    format_error_msg(
                        error_type="姝㈡崯鍗曟挙閿€澶辫触",
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
                logger.info(f"馃敃 宸叉挙閿€ {position.symbol} 姝㈢泩濮旀墭锛歿order_id}")
            else:
                logger.warning(f"鈿狅笍 {position.symbol} 姝㈢泩濮旀墭鎾ら攢澶辫触锛歿order_id}")

    def _record_closed_trade_result(self, position: Position, pnl: float):
        """Update cooldown and consecutive-loss guards from a closed trade."""
        now = time.time()
        if pnl < 0:
            severe_loss = abs(float(position.pnl_pct or 0)) >= 2.0 or str(position.exit_reason or "").upper().startswith(
                "STOP_LOSS"
            )
            self._consecutive_losses += 1
            self._symbol_cooldowns[position.symbol] = now + self.config.symbol_cooldown_sec
            logger.warning(
                f"馃 {position.symbol} 浜忔崯鍐峰嵈 {int(self.config.symbol_cooldown_sec / 60)} 鍒嗛挓 | "
                f"杩炵画浜忔崯={self._consecutive_losses}"
            )
            if severe_loss and self._consecutive_losses >= self.config.max_consecutive_losses:
                self._loss_pause_until = now + self.config.loss_pause_sec
                logger.warning(
                    f"馃洃 杩炵画浜忔崯杈惧埌 {self._consecutive_losses} 绗旓紝鏆傚仠鏂板紑浠?"
                    f"{int(self.config.loss_pause_sec / 60)} 鍒嗛挓"
                )
                send_telegram_message(
                    format_error_msg(
                        error_type="杩炵画浜忔崯鐔旀柇",
                        message=(
                            f"杩炵画浜忔崯 {self._consecutive_losses} 绗旓紝鏆傚仠鏂板紑浠?"
                            f"{int(self.config.loss_pause_sec / 60)} 鍒嗛挓"
                        ),
                        symbol=position.symbol,
                        session_id=position.session_id,
                        component="loss_guard",
                    )
                )
            elif not severe_loss:
                logger.info(f"{position.symbol} small loss ignored by pause guard | pnl_pct={position.pnl_pct:+.2f}%")
        else:
            self._consecutive_losses = 0

    def _breakeven_offset_for_position(self, position: Position) -> float:
        """Lock profits faster after TP, with tighter rules for breakout entries."""
        base_offset = max(float(self.config.breakeven_offset_pct), 0.05)
        tp_count = max(int(position.partial_tp_count), 1)
        if position.strategy_line == "瓒嬪娍绐佺牬绾?:
            if tp_count <= 1:
                return 0.0
            return base_offset + 0.12 + 0.10 * (tp_count - 2)
        return base_offset + 0.05 + 0.08 * (tp_count - 1)

    def _move_stop_to_breakeven(self, position: Position, remaining_qty: float) -> bool:
        """After first TP, move stop loss near breakeven so winners do not turn red."""
        if not self.config.breakeven_after_tp or remaining_qty <= 0:
            return False

        offset_pct = self._breakeven_offset_for_position(position)
        if position.strategy_line == "瓒嬪娍绐佺牬绾? and position.partial_tp_count < 2:
            logger.info(f"{position.symbol} breakout TP1 hit; keep original stop until TP2 to let trend run")
            return False
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

        old_order_id = position.stop_loss_order_id
        if old_order_id and not cancel_stop_loss_order(position.symbol, old_order_id):
            logger.warning(f"鈿狅笍 {position.symbol} 淇濇湰姝㈡崯绉诲姩澶辫触锛氭棫姝㈡崯鎾ら攢澶辫触 {old_order_id}")
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
            logger.warning(f"馃洝锔?{position.symbol} TP鍚庢鎹熷凡绉诲姩鍒颁繚鏈細{sl_result.order_id} @ {breakeven_price:.8f}")
            return True

        position.stop_loss_order_id = 0
        position.protection_failures += 1
        position.last_protection_error = sl_result.message
        send_telegram_message(
            format_error_msg(
                error_type="淇濇湰姝㈡崯绉诲姩澶辫触",
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
                        logger.info(f"馃洝锔?{position.symbol} 淇濇姢鍗曞凡鑷姩淇")
                    else:
                        failed.append(position.symbol)
                        logger.warning(f"馃洝锔?{position.symbol} 淇濇姢鍗曚慨澶嶅け璐?)
                except Exception as e:
                    failed.append(position.symbol)
                    logger.warning(f"馃洝锔?{position.symbol} 淇濇姢鍗曚慨澶嶅紓甯? {e}")

        if failed:
            self._new_entries_suspended = True
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="淇濇姢鍗曚慨澶嶅け璐ワ紝鏆傚仠鏂板紑浠?,
                        message=f"浠ヤ笅鎸佷粨淇濇姢鍗曚慨澶嶅け璐ワ細{', '.join(failed)}銆傜郴缁熶細缁х画绠＄悊宸叉湁鎸佷粨锛屼絾鏆傚仠鏂板紑浠撱€?,
                        component="protection_guard",
                    )
                )
                self._new_entries_suspended_alert_sent = True
        else:
            if self._new_entries_suspended:
                logger.warning("馃洝锔?鎵€鏈夋寔浠撲繚鎶ゅ崟宸叉仮澶嶏紝鏂板紑浠撻檺鍒惰В闄?)
            self._new_entries_suspended = False
            self._new_entries_suspended_alert_sent = False
        
        if repaired:
            logger.info(f"馃洝锔?淇濇姢鍗曡嚜鍔ㄤ慨澶嶆垚鍔燂細{', '.join(repaired)}")

    def _ensure_position_protection(self, position: Position, refresh_guard: bool = True):
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
                logger.warning(f"馃洝锔?{position.symbol} 宸茶ˉ鎸備氦鏄撴墍姝㈡崯鍗曪細{sl_result.order_id}")
            else:
                position.protection_failures += 1
                position.last_protection_error = sl_result.message
                send_telegram_message(
                    format_error_msg(
                        error_type="淇濇姢姝㈡崯琛ユ寕澶辫触",
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
                logger.warning(f"馃幆 {position.symbol} 宸茶ˉ鎸備氦鏄撴墍姝㈢泩鍗曪細{tp_result.order_id} @ {tp_price}")
            else:
                position.protection_failures += 1
                position.last_protection_error = tp_result.message
                send_telegram_message(
                    format_error_msg(
                        error_type="淇濇姢姝㈢泩琛ユ寕澶辫触",
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
        position.realized_pnl += pnl
        position.realized_exit_value += price * reduced_qty
        position.realized_quantity += reduced_qty
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
                strategy_line=position.strategy_line,
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
            logger.warning(f"鈿狅笍 鏈壘鍒板彲鏇存柊鐨勫紑浠撹褰曪細{symbol} session={session_id}")
            return False

        if session_id and matched_by != "session_id":
            logger.warning(
                f"鈿狅笍 璺宠繃DB骞充粨鏇存柊锛堜細璇濅笉鍖归厤锛夛細{symbol} "
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
        logger.info(f"馃摐 浜ゆ槗宸叉洿鏂?(ID: {trade.id}, matched_by={matched_by})")
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
            normal_orders = fetch_open_orders(position.symbol)
            algo_orders = fetch_open_algo_orders(position.symbol)
        except Exception as e:
            logger.debug(f"{position.symbol} 濮旀墭蹇収鍚屾璺宠繃锛歿e}")
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
            logger.warning(f"鈿狅笍 {position.symbol} 淇濇姢濮旀墭鍙兘宸叉垚浜?澶辨晥锛歿missing_ids}")
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
        now = time.time()

        if self._is_loss_pause_active():
            remaining_min = max(1, int((self._loss_pause_until - now) / 60))
            return f"杩炵画浜忔崯鏆傚仠涓紝鍓╀綑 {remaining_min} 鍒嗛挓"
        cooldown_until = self._symbol_cooldowns.get(symbol, 0.0)
        if cooldown_until > now:
            remaining_min = max(1, int((cooldown_until - now) / 60))
            return f"浜忔崯鍐峰嵈涓紝鍓╀綑 {remaining_min} 鍒嗛挓"

        if abs(funding) >= self.config.max_abs_funding_rate:
            return f"璧勯噾璐圭巼杩囩儹 {funding * 100:.3f}%"
        if oi_change >= self.config.max_oi_change_pct:
            return f"OI杩囩儹 {oi_change:.1f}%"

        if direction == "LONG":
            if change_24h <= (-15 if is_major_symbol else -12):
                return f"澶ц穼涓笉鎺ュ {change_24h:.1f}%"
            if change_24h >= (
                self.config.max_chase_change_pct + 10 if is_major_symbol else self.config.max_chase_change_pct
            ):
                return f"24h娑ㄥ箙杩囧ぇ {change_24h:.1f}%"
            if (
                (not is_major_symbol)
                and change_24h >= 12
                and drawdown < required_pullback
                and oi_change < self.config.momentum_entry_min_oi_pct
            ):
                return f"鏈洖韪╋紝璺?4h楂樼偣浠呭洖钀?{drawdown:.1f}%"
            if change_24h >= 8 and range_position >= (96.0 if is_major_symbol else self.config.max_range_position_pct):
                return f"浠锋牸澶勪簬24h鍖洪棿楂樹綅 {range_position:.1f}%"
        elif direction == "SHORT":
            if change_24h >= (15 if is_major_symbol else 12):
                return f"澶ф定涓笉杩界┖ {change_24h:.1f}%"
            if change_24h <= (
                -(self.config.max_chase_change_pct + 10) if is_major_symbol else -self.config.max_chase_change_pct
            ):
                return f"24h璺屽箙杩囧ぇ {change_24h:.1f}%"
            if change_24h <= -12 and range_position <= (4.0 if is_major_symbol else 100 - self.config.max_range_position_pct):
                return f"浠锋牸澶勪簬24h鍖洪棿浣庝綅 {range_position:.1f}%"

        if volume_mult < (0.6 if is_major_symbol else 0.8) and abs(change_24h) >= 10:
            return f"閲忚兘涓嶈冻 volume_mult={volume_mult:.2f}"
        return ""

    def _collect_entry_protection_errors(
        self,
        result: dict[str, Any],
        take_profit_targets: list[dict[str, Any]],
    ) -> tuple[int, list[str]]:
        stop_loss_order = result.get("stop_loss_order", {}) or {}
        protection_deferred = bool(result.get("protection_deferred", False))
        protection_errors: list[str] = []
        if protection_deferred:
            protection_errors.append("protection_deferred=true")

        stop_loss_order_id = int(stop_loss_order.get("order_id", 0) or 0)
        stop_loss_status = str(stop_loss_order.get("status", "") or "").upper()
        if stop_loss_order_id <= 0 or stop_loss_status == "ERROR":
            protection_errors.append(f"stop_loss status={stop_loss_status or 'UNKNOWN'} id={stop_loss_order_id}")

        for idx, target in enumerate(take_profit_targets, start=1):
            tp_order_id = int(target.get("order_id", 0) or 0)
            tp_status = str(target.get("status", "") or "").upper()
            if tp_order_id <= 0 or tp_status == "ERROR":
                protection_errors.append(f"tp{idx} status={tp_status or 'UNKNOWN'} id={tp_order_id}")

        return stop_loss_order_id, protection_errors

    def _abort_entry_on_protection_failure(
        self,
        *,
        symbol: str,
        direction: str,
        session_id: str,
        result: dict[str, Any],
        protection_errors: list[str],
        executed_entry_price: float,
        actual_quantity: float,
        trace_started: float,
        latency_steps: list[tuple[str, float]],
    ) -> None:
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
                error_type="寮€浠撲繚鎶ゅ崟澶辫触宸插洖婊?,
                message=f"{symbol} {detail}",
                symbol=symbol,
                session_id=session_id,
                component="entry_protection",
            )
        )
        self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)

    def execute_entry(self, signal: dict) -> Optional[Position]:
        """鎵ц寮€浠?- 濂ヤ竵鐨勯暱鐭?""
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
        if any(token in guard_text for token in ("澶辨晥", "娣樻卑", "绉诲嚭鐩戞帶", "鐘舵€佸彉鏇?)):
            logger.warning(f"entry guard reject {symbol}: blocked by monitor state [{guard_text}]")
            return None

        if self._new_entries_suspended:
            logger.warning(f"馃洝锔?{symbol} 鏂板紑浠撴殏鍋滐細瀛樺湪淇濇姢鍗曚笉瀹屾暣鐨勬寔浠?)
            if not self._new_entries_suspended_alert_sent:
                send_telegram_message(
                    format_error_msg(
                        error_type="鏂板紑浠撳凡鏆傚仠",
                        message="瀛樺湪鏈畬鏁村彈淇濇姢鐨勬寔浠擄紝璇峰厛纭姝㈡崯/姝㈢泩淇濇姢鍗曘€?,
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
            strategy_line = str(signal.get("strategy_line", "鍥炶俯纭绾?) or "鍥炶俯纭绾?)
            strategy_profile = self._strategy_profile(strategy_line)
            stop_loss_pct = self._strategy_stop_loss_pct(strategy_line)
            stop_trigger_buffer_pct = self._strategy_stop_trigger_buffer_pct(strategy_line)

            if not should_trade(trading_signal):
                return None

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
                        error_type="璐︽埛鏌ヨ澶辫触锛屾嫆缁濆紑浠?,
                        message=str(e),
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
                        f"馃 {symbol} 涓嬪崟鍓嶄环鏍煎亸绉昏繃澶э紝鏀惧純寮€浠? signal={price:.8f}, latest={latest_price:.8f}, "
                        f"slippage={slippage_pct:.2f}%"
                    )
                    return None
                if latest_price > 0:
                    price = latest_price
                    trading_signal.entry_price = latest_price
            self._record_latency_step(latency_steps, "price_recheck", step_started)

            quantity = None
            stop_loss = None

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

                risk_config = RiskConfig(
                    risk_per_trade_pct=self.config.risk_per_trade_pct,
                    base_stop_loss_pct=stop_loss_pct,
                    base_take_profit_pct=self.config.take_profit_pct * strategy_profile["tp_multiplier"],
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
                    logger.warning(f"馃洝锔?{symbol} 椋庢帶鎷掔粷锛歿risk_result.get('warnings', [])}")
                    return None

                logger.info(
                    f"馃洝锔?{symbol} 椋庢帶璇勫垎锛歿risk_result.get('risk_score', 0)}/100 ({risk_result.get('risk_level', 'UNKNOWN')})"
                )
                risk_level = risk_result.get("risk_level", "UNKNOWN")

                position_size = risk_result.get("position_size", {})
                quantity = position_size.get("quantity")
                stop_loss = risk_result.get("stop_loss", {}).get("stop_loss")
                position_value = float(position_size.get("position_value", 0) or 0)

                if quantity is not None and quantity <= 0:
                    logger.warning(f"馃洝锔?{symbol} 浠撲綅璁＄畻澶辫触")
                    return None

                if position_value > 0 and not self._passes_liquidity_filter(symbol, position_value):
                    return None

                logger.info(
                    f"馃攳 {symbol} 椋庢帶鍙傛暟: 浣欓=${balance:.2f}, 鏉犳潌={self.config.leverage}x, "
                    f"鍚嶄箟浠撲綅=${position_size.get('position_value', 0):.2f}, "
                    f"鏁伴噺={quantity}, 姝㈡崯=${(stop_loss or 0):.4f}"
                )

            except Exception as e:
                logger.warning(f"馃洝锔?椋庢帶璇勪及澶辫触 {symbol}: {e}锛屽洖閫€鍒版墽琛屽櫒榛樿璁＄畻")
            self._record_latency_step(latency_steps, "risk_assessment", step_started)

            take_profit_target_pcts, take_profit_ratios = self._build_take_profit_plan(strategy_line)
            step_started = time.perf_counter()
            result = execute_trade(
                signal=trading_signal,
                account_balance=balance,
                risk_per_trade_pct=self.config.risk_per_trade_pct,
                stop_loss_pct=stop_loss_pct,
                max_position_pct=self.config.max_position_pct,
                leverage=self.config.leverage,
                quantity=quantity,
                stop_loss_price=stop_loss,
                take_profit_target_pcts=take_profit_target_pcts,
                take_profit_ratios=take_profit_ratios,
                take_profit_mode=self.config.take_profit_mode,
                stop_trigger_buffer_pct=stop_trigger_buffer_pct,
                defer_protection_orders=False,
            )
            self._record_latency_step(latency_steps, "execute_trade", step_started)

            if result.get("action") != "EXECUTED":
                logger.warning(f"鉂?{symbol} 寮€浠撳け璐ワ細{result.get('reason', 'Unknown')}")
                self._emit_latency_trace("execute_entry_failed", trace_started, latency_steps, symbol=symbol)
                return None

            entry_order = result.get("entry_order", {})
            executed_entry_price = float(entry_order.get("executed_price", price) or price)
            order_status = entry_order.get("status", "UNKNOWN")
            actual_quantity = float(entry_order.get("quantity", 0) or result.get("quantity", 0))

            if quantity is None:
                quantity = actual_quantity if actual_quantity > 0 else 0

            if order_status == "PARTIALLY_FILLED":
                logger.warning(f"鈿狅笍 {symbol} 閮ㄥ垎鎴愪氦锛佽姹傛暟閲忥細{quantity}锛屽疄闄呮垚浜わ細{actual_quantity}")
                if actual_quantity < quantity * 0.5:
                    logger.error(f"鉂?{symbol} 閮ㄥ垎鎴愪氦姣斾緥杩囦綆锛屾斁寮冩寔浠?)
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

            stop_loss_order_id, protection_errors = self._collect_entry_protection_errors(
                result=result,
                take_profit_targets=take_profit_targets,
            )

            if protection_errors:
                self._abort_entry_on_protection_failure(
                    symbol=symbol,
                    direction=direction,
                    session_id=session_id,
                    result=result,
                    protection_errors=protection_errors,
                    executed_entry_price=executed_entry_price,
                    actual_quantity=actual_quantity,
                    trace_started=trace_started,
                    latency_steps=latency_steps,
                )
                return None

            oi_funding = signal.get("oi_funding") or {}
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
            )

            from telegram_notifier import format_open_position_msg

            score = signal.get("score", {}).get("total_score", 0) if signal.get("score") else 0
            leverage_applied = int(result.get("leverage_applied", self.config.leverage) or self.config.leverage)
            msg = format_open_position_msg(
                symbol=symbol,
                direction=direction,
                entry_price=executed_entry_price,
                quantity=position.quantity,
                leverage=leverage_applied,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                risk_amount=result.get("risk_amount_usdt", 0),
                risk_pct=self.config.risk_per_trade_pct,
                score=score,
                risk_level=risk_level,
                session_id=session_id,
                strategy_line=strategy_line,
                oi_funding=oi_funding,
                target_roi_pct=primary_target_roi_pct,
                price_move_pct=primary_price_move_pct,
                take_profit_targets=take_profit_targets,
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
                f"strategy_line={strategy_line}",
                f"risk_level={risk_level}",
                f"target_roi_pct={primary_target_roi_pct}",
                f"price_move_pct={primary_price_move_pct}",
                f"take_profit_mode={self.config.take_profit_mode}",
                f"tp_multiplier={self._tp_multiplier}",
                f"strategy_tp_multiplier={strategy_profile['tp_multiplier']}",
                f"strategy_stop_pct={stop_loss_pct}",
                f"stop_trigger_buffer_pct={stop_trigger_buffer_pct}",
                f"leverage_applied={leverage_applied}",
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
                leverage=self.config.leverage,
                stop_loss=position.stop_loss_price,
                take_profit=tp_price,
                entry_time=position.entry_time.isoformat(),
                mode=self.config.mode,
                market_snapshot={
                    **(signal.get("metrics", {}) or {}),
                    "_oi_funding": oi_funding,
                    "_leverage_applied": leverage_applied,
                },
                notes=";".join(notes_parts),
            )
            step_started = time.perf_counter()
            trade_id = self.db.add_trade(trade)
            logger.info(f"浜ゆ槗宸茶褰?(ID: {trade_id})")
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
            logger.error(f"鉂?{symbol} 寮€浠撴祦绋嬪紓甯革細{e}", exc_info=True)
            send_telegram_message(
                format_error_msg(
                    error_type="寮€浠撴祦绋嬪紓甯?,
                    message=str(e),
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
        """鎵ц骞充粨 - 鎵樺皵鐨勯浄闇?""
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
            result = place_market_order(
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
                        roi_pct=pnl_pct * self.config.leverage,
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
                logger.info(f"trade_review {message_signature(review)}")
                self._emit_latency_trace("execute_exit", trace_started, latency_steps, symbol=symbol)
                return True

        except Exception as e:
            logger.error(f"骞充粨澶辫触 {symbol}: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="骞充粨澶辫触",
                    message=str(e),
                    symbol=symbol,
                    session_id=position.session_id,
                    component="execute_exit",
                )
            )
            self._emit_latency_trace("execute_exit_exception", trace_started, latency_steps, symbol=symbol)
            return False





