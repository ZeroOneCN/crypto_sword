from __future__ import annotations

import logging
import time
from typing import Any

from services.order_service import order_service
from telegram_notifier import (
    format_error_msg,
    format_protection_failure_detail,
    format_protection_status_msg,
    send_telegram_message,
)

logger = logging.getLogger(__name__)

class ProtectionServiceMixin:
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
            prune_result = order_service.prune_duplicate_protective_orders(
                position.symbol,
                position_side=position_side,
                close_side=close_side,
            )
            if prune_result.get("canceled") or prune_result.get("failed"):
                logger.info(
                    f"🧹 {position.symbol} 保护单去重："
                    f"撤销={prune_result.get('canceled', [])} 失败={prune_result.get('failed', [])}"
                )
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
            targets = sorted(
                position.take_profit_targets,
                key=lambda item: int(item.get("level", 0) or 0),
            )
            orders_by_id = {
                int(order.get("order_id", 0) or 0): order
                for order in sorted_orders
                if int(order.get("order_id", 0) or 0) > 0
            }
            assigned_order_ids: set[int] = set()

            # Keep the original TP plan immutable. Exchange snapshots are used
            # only to adopt order ids/status/quantity; they must not shift TP2
            # into the TP1 slot after TP1 has filled.
            for target in targets:
                target_order_id = int(target.get("order_id", 0) or 0)
                order = orders_by_id.get(target_order_id)
                if not order:
                    continue
                order_id = int(order.get("order_id", 0) or 0)
                if order_id <= 0:
                    continue
                target["order_id"] = order_id
                target["status"] = "ADOPTED"
                if float(order.get("quantity", 0) or 0) > 0:
                    target["quantity"] = float(order.get("quantity", 0) or 0)
                if float(target.get("price", 0) or 0) <= 0 and float(order.get("price", 0) or 0) > 0:
                    target["price"] = float(order.get("price", 0) or 0)
                assigned_order_ids.add(order_id)
                changed = True

            unmatched_orders = [
                order
                for order in sorted_orders
                if int(order.get("order_id", 0) or 0) not in assigned_order_ids
            ]
            for target in targets:
                current_id = int(target.get("order_id", 0) or 0)
                if current_id in open_tp_ids:
                    continue
                planned_price = float(target.get("price", 0) or 0)
                if planned_price <= 0 or not unmatched_orders:
                    continue

                best_order = None
                best_diff = float("inf")
                for order in unmatched_orders:
                    order_price = float(order.get("price", 0) or 0)
                    if order_price <= 0:
                        continue
                    diff = abs(order_price - planned_price) / planned_price
                    if diff < best_diff:
                        best_diff = diff
                        best_order = order

                if best_order is None or best_diff > 0.01:
                    continue
                order_id = int(best_order.get("order_id", 0) or 0)
                if order_id <= 0:
                    continue
                target["order_id"] = order_id
                target["status"] = "ADOPTED"
                if float(best_order.get("quantity", 0) or 0) > 0:
                    target["quantity"] = float(best_order.get("quantity", 0) or 0)
                unmatched_orders = [
                    order for order in unmatched_orders if int(order.get("order_id", 0) or 0) != order_id
                ]
                assigned_order_ids.add(order_id)
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
        completed_tp_count = int(getattr(position, "partial_tp_count", 0) or 0)
        if position.take_profit_targets and completed_tp_count > 0:
            active_tp_targets = [
                target
                for target in active_tp_targets
                if int(target.get("level", 0) or 0) > completed_tp_count
            ]
            if not active_tp_targets:
                active_tp_targets = [position.take_profit_targets[-1]]
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
