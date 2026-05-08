from __future__ import annotations

import logging
import time
from typing import Optional

from binance_trading_executor import OrderResult
from feature_store import build_trade_review, feature_store
from speed_executor import quick_close_position
from telegram_notifier import (
    format_close_position_msg,
    format_error_msg,
    format_partial_take_profit_msg,
    send_telegram_message,
)

logger = logging.getLogger(__name__)

class ExitServiceMixin:
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

    def _close_price_matches_pnl_direction(self, position: Position, exit_price: float, pnl: float) -> bool:
        entry = float(position.entry_price or 0.0)
        if entry <= 0 or exit_price <= 0 or abs(float(pnl or 0.0)) <= 1e-9:
            return True
        if position.side == "BUY":
            return (pnl > 0 and exit_price >= entry) or (pnl < 0 and exit_price <= entry)
        return (pnl > 0 and exit_price <= entry) or (pnl < 0 and exit_price >= entry)

    def _derive_exit_price_from_pnl(self, position: Position, pnl: float, qty: float | None = None) -> float:
        entry = float(position.entry_price or 0.0)
        close_qty = float(qty or position.initial_quantity or position.quantity or 0.0)
        if entry <= 0 or close_qty <= 0:
            return 0.0
        if position.side == "BUY":
            return max(entry + float(pnl or 0.0) / close_qty, 0.0)
        return max(entry - float(pnl or 0.0) / close_qty, 0.0)

    def _repair_close_summary_consistency(
        self,
        position: Position,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
        *,
        source: str = "",
        qty: float | None = None,
    ) -> tuple[float, float]:
        """Keep close notifications internally consistent when exchange sync data is partial/stale."""
        close_qty = float(qty or position.initial_quantity or position.quantity or 0.0)
        if not self._close_price_matches_pnl_direction(position, exit_price, pnl):
            repaired_price = self._derive_exit_price_from_pnl(position, pnl, close_qty)
            if repaired_price > 0 and self._close_price_matches_pnl_direction(position, repaired_price, pnl):
                logger.warning(
                    f"⚠️ {position.symbol} 平仓均价与盈亏方向矛盾，已修正展示价："
                    f"source={source or '-'} entry={position.entry_price:.8f} "
                    f"old_exit={exit_price:.8f} repaired_exit={repaired_price:.8f} pnl={pnl:+.6f}"
                )
                exit_price = repaired_price

        entry_notional = float(position.entry_price or 0.0) * close_qty
        if entry_notional > 0:
            pnl_pct = float(pnl or 0.0) / entry_notional * 100.0
        return exit_price, pnl_pct

    def _normalize_close_reason_for_pnl(self, reason: str, pnl: float) -> str:
        reason_text = str(reason or "")
        if "STOP_LOSS" in reason_text and float(pnl or 0.0) > 0:
            return reason_text.replace("STOP_LOSS", "PROTECTIVE_STOP")
        return reason_text

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
        avg_exit_price, pnl_pct = self._repair_close_summary_consistency(
            position,
            avg_exit_price,
            realized_pnl,
            pnl_pct,
            source="exchange_realized_state",
            qty=total_qty,
        )
        return avg_exit_price, realized_pnl, pnl_pct, remaining_pnl_delta

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

    def _execute_exit_impl(self, symbol: str, reason: str) -> bool:
        """执行平仓 - 托尔的雷霆"""
        from core.monitoring import build_execution_event, message_signature

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

        if self._pre_tp_micro_exit_guard_blocks(position, reason, price=current_price):
            logger.warning(
                f"🛑 {symbol} 主动微利平仓被拦截：reason={reason} "
                f"price={current_price:.8f} roi={self._position_roi_pct(position):+.2f}% 未达TP1"
            )
            self._emit_latency_trace("execute_exit_blocked_pre_tp", trace_started, latency_steps, symbol=symbol)
            return False

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
                exit_price, pnl_pct = self._repair_close_summary_consistency(
                    position,
                    exit_price,
                    pnl,
                    pnl_pct,
                    source="active_market_close",
                )
                reason = self._normalize_close_reason_for_pnl(reason, pnl)
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
