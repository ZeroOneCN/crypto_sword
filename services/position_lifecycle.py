from __future__ import annotations

import logging
import json
import time
from datetime import datetime
from typing import Any, Optional

from feature_store import feature_store
from services.order_service import order_service
from signal_enhancer import get_klines
from telegram_notifier import format_direction_label, send_telegram_message
from trade_logger import TradeRecord

logger = logging.getLogger(__name__)

class PositionLifecycleMixin:
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
        rebound = float(metrics.get("rebound_from_24h_low_pct", 0.0) or 0.0)
        required_pullback = self._required_pullback_pct(metrics)
        top_reversal_short = (
            change_24h >= 6.0
            and drawdown >= max(1.2, min(required_pullback, 2.5))
            and oi_change >= 7.0
            and volume_mult >= 0.7
            and range_position <= 97.0
        )
        bottom_reversal_long = (
            change_24h <= -6.0
            and rebound >= max(1.2, min(required_pullback, 2.5))
            and oi_change >= 7.0
            and volume_mult >= 0.7
            and range_position >= 3.0
        )

        if abs(funding) >= self.config.max_abs_funding_rate:
            return f"资金费率过热 {funding * 100:.3f}%"
        if oi_change >= self.config.max_oi_change_pct:
            return f"OI过热 {oi_change:.1f}%"

        if direction == "LONG":
            if change_24h <= (-15 if is_major_symbol else -12) and not bottom_reversal_long:
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
            if change_24h >= (15 if is_major_symbol else 12) and not top_reversal_short:
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

    def _persist_entry_opened(
        self,
        *,
        signal: dict,
        position: Position,
        direction: str,
        side: str,
        session_id: str,
        strategy_line: str,
        exit_profile_name: str,
        risk_level: str,
        primary_target_roi_pct: float,
        primary_price_move_pct: float,
        take_profit_mode_for_trade: str,
        strategy_profile: dict,
        stop_loss_pct: float,
        stop_trigger_buffer_pct: float,
        leverage_applied: int,
        capital_plan: Any,
        oi_funding: dict,
        take_profit_targets: list[dict],
        tp_price: float,
        executed_entry_price: float,
        entry_scale: dict[str, Any] | None = None,
    ) -> int:
        from core.monitoring import build_execution_event, message_signature
        entry_scale = entry_scale or {}

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
            f"entry_scale_mode={entry_scale.get('mode', 'full')}",
            f"entry_scale_ratio={float(entry_scale.get('ratio', 1.0) or 1.0):.4f}",
            f"intended_quantity={float(entry_scale.get('intended_quantity', position.quantity) or position.quantity):.8f}",
            f"add_on_done={1 if entry_scale.get('add_on_done') else 0}",
            f"oi_funding_bonus={float(oi_funding.get('score_bonus', 0) or 0):.2f}",
            f"tp_plan={json.dumps(take_profit_targets, separators=(',', ':'))}",
            f"tp_order_ids={','.join(str(int(item.get('order_id', 0))) for item in take_profit_targets if item.get('order_id'))}",
        ]

        trade = TradeRecord(
            symbol=position.symbol,
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
                "_entry_score": {
                    **(signal.get("score", {}) or {}),
                    "strategy_line": strategy_line,
                },
                "_entry_gate": signal.get("_entry_gate_override") or "normal",
                "_entry_gate_note": signal.get("_entry_gate_note") or "",
                "_leverage_applied": leverage_applied,
                "_capital_plan": capital_plan.to_dict() if capital_plan else {},
                "_entry_scale": entry_scale,
            },
            notes=";".join(notes_parts),
        )
        trade_id = self.db.add_trade(trade)
        logger.info(f"交易已记录 (ID: {trade_id})")
        entry_event = build_execution_event(
            event="entry_opened",
            symbol=position.symbol,
            direction=direction,
            session_id=session_id,
            metrics={
                "trade_id": trade_id,
                "entry_price": executed_entry_price,
                "quantity": position.quantity,
                "stop_loss": position.stop_loss_price,
                "take_profit": tp_price,
                "entry_scale_ratio": float(entry_scale.get("ratio", 1.0) or 1.0),
                "intended_quantity": float(entry_scale.get("intended_quantity", position.quantity) or position.quantity),
            },
        )
        logger.info(f"execution_event {message_signature(entry_event)}")
        feature_store.append_event(entry_event)
        return trade_id

    def _entry_scale_profile(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Return first-fill scale profile for strong signals that should probe first."""
        if not getattr(self.config, "probe_entry_enabled", True):
            return {"mode": "full", "ratio": 1.0, "label": "完整仓"}
        status_text = str(signal.get("entry_status_text", "") or "")
        watch_stage = str(signal.get("watch_stage", "") or "")
        strategy_line = str(signal.get("strategy_line", "") or "")
        score = self._signal_score_value(signal) if hasattr(self, "_signal_score_value") else 0.0
        fast_lane = status_text == "中枢快线入场" or watch_stage.endswith("快线")
        strong_direct = status_text in {"优势阶段神级直通", "预突破强信号直通", "突破确认入场", "动量确认入场"}
        if strategy_line == "趋势突破线" and (fast_lane or strong_direct) and score >= float(self.config.probe_add_on_min_score):
            ratio = float(self.config.probe_entry_ratio)
            return {
                "mode": "probe",
                "ratio": ratio,
                "label": f"试探仓{ratio * 100:.0f}%",
                "reason": watch_stage or status_text,
            }
        return {"mode": "full", "ratio": 1.0, "label": "完整仓"}

    def _probe_add_on_ready(self, position: Position) -> tuple[bool, str]:
        if not getattr(self.config, "probe_add_on_enabled", True):
            return False, "确认加仓关闭"
        if getattr(position, "entry_scale_mode", "full") != "probe":
            return False, "非试探仓"
        if getattr(position, "add_on_done", False) or getattr(position, "add_on_attempted", False):
            return False, "已处理"
        if position.quantity <= 0 or position.entry_price <= 0:
            return False, "仓位无效"
        intended_qty = float(getattr(position, "intended_quantity", 0.0) or 0.0)
        if intended_qty <= position.quantity * 1.05:
            return False, "目标数量不足"
        age = self._position_age_minutes(position)
        if age > float(self.config.probe_add_on_max_age_minutes):
            return False, f"超过加仓窗口 {age:.0f}m"
        roi = self._position_roi_pct(position)
        if roi < float(self.config.probe_add_on_min_roi_pct):
            return False, f"ROI {roi:.2f}% 未达确认阈值"
        score = self._position_entry_score_value(position)
        if score < float(self.config.probe_add_on_min_score):
            return False, f"评分 {score:.1f} 不足"
        if self._position_has_taken_profit(position):
            return False, "已触发止盈，不再加仓"
        return True, f"ROI {roi:.2f}% 评分 {score:.1f} age {age:.0f}m"

    def _try_probe_add_on(self, position: Position) -> bool:
        ready, reason = self._probe_add_on_ready(position)
        if not ready:
            return False

        position.add_on_attempted = True
        target_qty = float(getattr(position, "intended_quantity", position.quantity) or position.quantity)
        add_qty = max(target_qty - float(position.quantity or 0.0), 0.0)
        if add_qty <= 0:
            return False

        side = "BUY" if position.side == "BUY" else "SELL"
        position_side = "LONG" if position.side == "BUY" else "SHORT"
        result = order_service.place_market_order(
            position.symbol,
            side,
            add_qty,
            leverage=max(int(position.leverage or self.config.leverage), 1),
            position_side=position_side,
            reduce_only=False,
        )
        if result.status not in {"FILLED", "HIGH_SLIPPAGE"} or result.quantity <= 0 or result.executed_price <= 0:
            position.add_on_error = result.message
            logger.warning(f"{position.symbol} 试探仓确认加仓失败：{result.status} {result.message}")
            return False

        old_qty = float(position.quantity or 0.0)
        old_notional = old_qty * float(position.entry_price or 0.0)
        add_notional = result.quantity * result.executed_price
        new_qty = old_qty + result.quantity
        if new_qty <= 0:
            return False
        position.entry_price = (old_notional + add_notional) / new_qty if old_notional + add_notional > 0 else position.entry_price
        position.quantity = new_qty
        position.initial_quantity = max(float(position.initial_quantity or 0.0), new_qty)
        position.last_synced_quantity = new_qty
        position.add_on_done = True
        position.add_on_order_id = int(result.order_id or 0)
        position.add_on_time = datetime.now()
        position.add_on_error = ""
        leverage = max(int(position.leverage or self.config.leverage or 1), 1)
        for target in position.take_profit_targets or []:
            target_roi = float(target.get("target_roi_pct", 0) or 0)
            if target_roi <= 0:
                continue
            price_move_pct = target_roi / leverage
            if position.side == "BUY":
                target["price"] = position.entry_price * (1 + price_move_pct / 100.0)
            else:
                target["price"] = position.entry_price * (1 - price_move_pct / 100.0)
            target["order_id"] = 0
            target["status"] = "PENDING_REBUILD"
            target["message"] = "rebuilt after probe add-on"
        if position.take_profit_targets:
            position.take_profit_price = float(position.take_profit_targets[0].get("price", position.take_profit_price) or position.take_profit_price)

        try:
            trade, matched_by = self._find_open_trade_for_session(position.symbol, position.session_id)
            if trade and matched_by == "session_id":
                self.db.update_open_trade(
                    trade.id,
                    entry_price=position.entry_price,
                    quantity=position.quantity,
                    stop_loss=position.stop_loss_price,
                    take_profit=position.take_profit_price,
                    note_updates={
                        "add_on_done": 1,
                        "add_on_order_id": position.add_on_order_id,
                        "add_on_time": position.add_on_time.isoformat() if position.add_on_time else "",
                        "entry_scale_mode": position.entry_scale_mode,
                        "entry_scale_ratio": f"{float(position.entry_scale_ratio or 1.0):.4f}",
                        "intended_quantity": f"{float(position.intended_quantity or position.quantity):.8f}",
                    },
                )
        except Exception as exc:
            logger.debug(f"{position.symbol} add-on DB sync skipped: {exc}")

        try:
            self._cancel_position_protection(position)
            protected = self._ensure_position_protection(position)
            self._send_protection_status(position, source="probe_add_on", force=True)
        except Exception as exc:
            protected = False
            position.protection_failures += 1
            position.last_protection_error = str(exc)
            logger.exception(f"{position.symbol} 加仓后保护单重建失败")

        logger.warning(
            f"{position.symbol} 试探仓确认加仓完成：add_qty={result.quantity} price={result.executed_price:.8f} "
            f"new_qty={new_qty} avg={position.entry_price:.8f} protected={protected} | {reason}"
        )
        direction = "LONG" if position.side == "BUY" else "SHORT"
        send_telegram_message(
            "➕ <b>宙斯交易中枢 | 确认加仓</b>\n\n"
            f"<b>标的</b>  <code>{position.symbol}</code>\n"
            f"<b>方向</b>  <code>{format_direction_label(direction)}</code>\n"
            f"<b>加仓数量</b>  <code>{result.quantity:g}</code>\n"
            f"<b>成交价</b>  <code>{result.executed_price:.8f}</code>\n"
            f"<b>当前总仓</b>  <code>{position.quantity:g}</code>\n"
            f"<b>新均价</b>  <code>{position.entry_price:.8f}</code>\n"
            f"<b>保护单</b>  <code>{'已重建' if protected else '重建失败，请检查'}</code>\n"
            f"<b>流水号</b>  <code>{position.session_id}</code>"
        )
        return True

    def _manage_probe_positions(self):
        for position in list(self.tracker.positions.values()):
            self._try_probe_add_on(position)

    def _record_closed_trade_result(self, position: Position, pnl: float):
        """Record closed trade outcome without applying loss cooldowns."""
        if pnl >= 0:
            self._consecutive_losses = 0
        else:
            logger.info(f"{position.symbol} loss cooldown disabled | pnl={pnl:+.4f} pnl_pct={position.pnl_pct:+.2f}%")

    def _position_age_minutes(self, position: Position) -> float:
        try:
            return max(0.0, (datetime.now() - position.entry_time).total_seconds() / 60.0)
        except Exception:
            return 0.0

    def _position_price_move_pct(self, position: Position) -> float:
        if position.entry_price <= 0 or position.current_price <= 0:
            return float(position.pnl_pct or 0.0)
        if position.side == "BUY":
            return (position.current_price - position.entry_price) / position.entry_price * 100.0
        return (position.entry_price - position.current_price) / position.entry_price * 100.0

    def _position_roi_pct(self, position: Position) -> float:
        leverage = max(int(getattr(position, "leverage", 0) or self.config.leverage or 1), 1)
        return self._position_price_move_pct(position) * leverage

    def _position_entry_score_value(self, position: Position) -> float:
        score_data = getattr(position, "entry_score", {}) or {}
        if isinstance(score_data, dict):
            return float(score_data.get("total_score", score_data.get("total", 0)) or 0)
        return float(score_data or 0)

    def _position_has_taken_profit(self, position: Position) -> bool:
        return (
            int(getattr(position, "partial_tp_count", 0) or 0) > 0
            or float(getattr(position, "realized_quantity", 0.0) or 0.0) > 0
            or float(getattr(position, "exchange_realized_quantity", 0.0) or 0.0) > 0
        )

    def _take_profit_target_for_level(self, position: Position, level_index: int | None = None) -> float:
        targets = sorted(
            [target for target in (position.take_profit_targets or []) if float(target.get("price", 0) or 0) > 0],
            key=lambda item: int(item.get("level", 0) or 0),
        )
        if targets:
            if level_index is None:
                level_index = min(max(int(getattr(position, "partial_tp_count", 0) or 0), 0), len(targets) - 1)
            return float(targets[level_index].get("price", 0) or 0)
        return float(position.take_profit_price or 0.0)

    def _price_reaches_take_profit(self, position: Position, price: float, target_price: float, tolerance_pct: float = 0.0) -> bool:
        if price <= 0 or target_price <= 0:
            return False
        tolerance = max(0.0, float(tolerance_pct)) / 100.0
        if position.side == "BUY":
            return price >= target_price * (1 - tolerance)
        return price <= target_price * (1 + tolerance)

    def _position_reached_tp1(self, position: Position, price: float | None = None, tolerance_pct: float = 0.0) -> bool:
        if self._position_has_taken_profit(position):
            return True
        target_price = self._take_profit_target_for_level(position, 0)
        current_price = float(price if price is not None else (position.current_price or 0.0))
        return self._price_reaches_take_profit(position, current_price, target_price, tolerance_pct)

    def _pre_tp_micro_exit_guard_blocks(self, position: Position, reason: str, price: float | None = None) -> bool:
        if not getattr(self.config, "pre_tp_micro_exit_guard_enabled", True):
            return False
        if reason not in {"SIDEWAYS_TIMEOUT", "SIDEWAYS_REPLACED_BY_STRONG_SIGNAL"}:
            return False
        if self._position_has_taken_profit(position):
            return False
        roi_pct = self._position_roi_pct(position)
        min_roi = float(getattr(self.config, "pre_tp_micro_exit_guard_min_roi_pct", 0.2) or 0.0)
        if roi_pct <= min_roi:
            return False
        if self._position_reached_tp1(position, price=price, tolerance_pct=0.0):
            return False
        return True

    def _tp_fill_price_is_valid(self, position: Position, fill_price: float) -> bool:
        target_price = self._take_profit_target_for_level(position)
        tolerance_pct = float(getattr(self.config, "tp_fill_price_tolerance_pct", 0.35) or 0.0)
        return self._price_reaches_take_profit(position, fill_price, target_price, tolerance_pct)

    def _infer_exchange_close_reason(self, position: Position, exit_price: float, pnl: float) -> str:
        if exit_price <= 0:
            return "EXCHANGE_REALIZED"
        if self._position_reached_tp1(position, price=exit_price, tolerance_pct=float(getattr(self.config, "tp_fill_price_tolerance_pct", 0.35) or 0.0)):
            return "TAKE_PROFIT"
        if position.current_stop > 0:
            if position.side == "BUY" and exit_price <= position.current_stop * 1.003:
                return "PROTECTIVE_STOP" if pnl > 0 else "STOP_LOSS"
            if position.side != "BUY" and exit_price >= position.current_stop * 0.997:
                return "PROTECTIVE_STOP" if pnl > 0 else "STOP_LOSS"
        if pnl > 0:
            return "EARLY_PROFIT"
        return "EXCHANGE_REALIZED"

    def _is_sideways_position_candidate(
        self,
        position: Position,
        min_age_minutes: float,
        max_roi_pct: float,
        require_near_entry: bool = True,
    ) -> bool:
        if not getattr(self.config, "sideways_management_enabled", True):
            return False
        if position.entry_price <= 0 or position.quantity <= 0:
            return False
        if self._position_has_taken_profit(position):
            return False
        if self._position_age_minutes(position) < float(min_age_minutes):
            return False

        price_move_pct = self._position_price_move_pct(position)
        roi_pct = self._position_roi_pct(position)
        if require_near_entry and abs(price_move_pct) > float(self.config.sideways_max_price_move_pct):
            return False
        return abs(roi_pct) <= float(max_roi_pct)

    def _move_stop_to_sideways_defense(self, position: Position) -> bool:
        """Move a stale sideways position's exchange stop near entry without removing the hard stop."""
        if getattr(position, "sideways_defense_moved", False):
            return False
        if position.quantity <= 0 or position.entry_price <= 0:
            return False

        offset_pct = float(self.config.sideways_stop_offset_pct)
        if position.side == "BUY":
            desired_stop = position.entry_price * (1 - offset_pct / 100.0)
            close_side = "SELL"
            position_side = "LONG"
            old_exchange_stop = float(position.stop_loss_price or position.current_stop or 0.0)
            if old_exchange_stop >= desired_stop > 0:
                position.sideways_defense_moved = True
                return True
        else:
            desired_stop = position.entry_price * (1 + offset_pct / 100.0)
            close_side = "BUY"
            position_side = "SHORT"
            old_exchange_stop = float(position.stop_loss_price or position.current_stop or 0.0)
            if 0 < old_exchange_stop <= desired_stop:
                position.sideways_defense_moved = True
                return True

        latest_price = float(position.current_price or 0.0)
        if latest_price <= 0:
            try:
                latest_price = float(self.get_current_prices([position.symbol]).get(position.symbol, 0) or 0)
            except Exception as exc:
                logger.debug(f"{position.symbol} sideways defense price fetch skipped: {exc}")

        stop_price = desired_stop
        if latest_price > 0:
            trigger_buffer_pct = max(0.10, min(float(self.config.stop_trigger_buffer_pct), float(self.config.sideways_stop_offset_pct)))
            trigger_buffer = trigger_buffer_pct / 100.0
            if position.side == "BUY":
                stop_price = min(stop_price, latest_price * (1 - trigger_buffer))
                if old_exchange_stop and stop_price <= old_exchange_stop:
                    logger.info(
                        f"{position.symbol} 横盘防守跳过：安全止损 {stop_price:.8f} "
                        f"未优于当前交易所止损 {old_exchange_stop:.8f}"
                    )
                    return False
            else:
                stop_price = max(stop_price, latest_price * (1 + trigger_buffer))
                if old_exchange_stop and stop_price >= old_exchange_stop:
                    logger.info(
                        f"{position.symbol} 横盘防守跳过：安全止损 {stop_price:.8f} "
                        f"未优于当前交易所止损 {old_exchange_stop:.8f}"
                    )
                    return False

        if stop_price <= 0:
            return False

        old_order_id = position.stop_loss_order_id
        sl_result = order_service.place_stop_loss(
            position.symbol,
            close_side,
            position.quantity,
            stop_price,
            position_side=position_side,
            reduce_only=True,
        )
        if sl_result.status != "ERROR" and sl_result.order_id:
            position.stop_loss_order_id = sl_result.order_id
            position.stop_loss_price = stop_price
            position.current_stop = stop_price
            position.sideways_defense_moved = True
            position.sideways_last_action_ts = time.time()
            if old_order_id and not order_service.cancel_stop_loss(position.symbol, old_order_id):
                logger.warning(f"⚠️ {position.symbol} 横盘防守止损已生效，但旧止损撤销失败：{old_order_id}")
            logger.warning(
                f"🛡️ {position.symbol} 横盘防守止损已移动："
                f"age={self._position_age_minutes(position):.0f}m roi={self._position_roi_pct(position):+.2f}% "
                f"order={sl_result.order_id} @ {stop_price:.8f}"
            )
            return True

        position.protection_failures += 1
        position.last_protection_error = sl_result.message
        logger.warning(f"⚠️ {position.symbol} 横盘防守止损移动失败：{sl_result.message}")
        return False

    def _manage_sideways_positions(self):
        if not getattr(self.config, "sideways_management_enabled", True):
            return

        defense_after = float(self.config.sideways_defense_after_minutes)
        exit_after = float(self.config.sideways_exit_after_minutes)
        max_roi = float(self.config.sideways_max_roi_pct)
        for symbol, position in list(self.tracker.positions.items()):
            if not self.tracker.get_position(symbol):
                continue
            if exit_after > 0 and self._is_sideways_position_candidate(position, exit_after, max_roi):
                if self._pre_tp_micro_exit_guard_blocks(position, "SIDEWAYS_TIMEOUT"):
                    logger.warning(
                        f"🛑 {symbol} 横盘微利退出被拦截："
                        f"age={self._position_age_minutes(position):.0f}m "
                        f"roi={self._position_roi_pct(position):+.2f}% 未达TP1"
                    )
                    self._move_stop_to_sideways_defense(position)
                    continue
                logger.warning(
                    f"⏳ {symbol} 横盘超时退出："
                    f"age={self._position_age_minutes(position):.0f}m "
                    f"price={self._position_price_move_pct(position):+.2f}% "
                    f"roi={self._position_roi_pct(position):+.2f}%"
                )
                self.execute_exit(symbol, "SIDEWAYS_TIMEOUT")
                continue
            if self._is_sideways_position_candidate(position, defense_after, max_roi):
                self._move_stop_to_sideways_defense(position)

    def _try_replace_sideways_position_for_signal(self, signal: dict[str, Any]) -> bool:
        """Free one weak sideways slot for a much stronger ready signal."""
        if not getattr(self.config, "sideways_replacement_enabled", True):
            return False
        if self.tracker.get_open_count() < int(self.config.max_open_positions):
            return True
        if signal.get("entry_status") != "ready":
            return False

        symbol = str(signal.get("symbol", "") or "")
        if not symbol or symbol in self.tracker.positions:
            return False

        try:
            new_score = float(self._signal_score_value(signal))
        except Exception:
            score_data = signal.get("score") or {}
            new_score = float(score_data.get("total_score", score_data.get("total", 0)) or 0) if isinstance(score_data, dict) else float(score_data or 0)
        if new_score < float(self.config.sideways_replacement_min_score):
            return False

        candidates: list[tuple[float, float, float, Position]] = []
        for position in list(self.tracker.positions.values()):
            if not self._is_sideways_position_candidate(
                position,
                float(self.config.sideways_replacement_min_age_minutes),
                float(self.config.sideways_replacement_max_roi_pct),
            ):
                continue
            if self._pre_tp_micro_exit_guard_blocks(position, "SIDEWAYS_REPLACED_BY_STRONG_SIGNAL"):
                continue
            old_score = self._position_entry_score_value(position)
            score_gap = new_score - old_score
            if score_gap < float(self.config.sideways_replacement_score_gap) and new_score < 95.0:
                continue
            candidates.append((self._position_roi_pct(position), old_score, -self._position_age_minutes(position), position))

        if not candidates:
            return False

        _, old_score, _, weakest = sorted(candidates, key=lambda item: (item[0], item[1], item[2]))[0]
        logger.warning(
            f"🔁 机会替换：{symbol} score={new_score:.1f} 替换横盘仓 {weakest.symbol} "
            f"score={old_score:.1f} age={self._position_age_minutes(weakest):.0f}m "
            f"roi={self._position_roi_pct(weakest):+.2f}%"
        )
        closed = self.execute_exit(weakest.symbol, "SIDEWAYS_REPLACED_BY_STRONG_SIGNAL")
        return bool(closed and self.tracker.get_open_count() < int(self.config.max_open_positions))

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
            base_ratios = [0.10, 0.30, 0.60]
        elif strategy_line == "均线二启线":
            base_ratios = [0.15, 0.35, 0.50]
        else:
            base_ratios = [0.15, 0.35, 0.50]
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
        if str(signal.get("stage", "") or "") == "mania":
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
        stage = str(signal.get("stage", "") or "")
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

        if (
            strategy_line == "趋势突破线"
            and stage == "pre_break"
            and score >= 95.0
            and oi_change >= 30.0
            and funding < self.config.max_abs_funding_rate * 0.60
        ):
            exposure = min(hard_cap, max(exposure, base_exposure + 20.0))
            max_correlated = max(max_correlated, 4)
            mode = "强信号进攻"
            reasons.append(f"强趋势评分={score:.0f} OI={oi_change:.0f}%")
        elif stage == "confirmed_breakout":
            exposure = min(exposure, 140.0)
            max_correlated = min(max_correlated, 3)
            reasons.append("确认突破降权")
        elif stage == "mania":
            exposure = min(exposure, 100.0)
            max_correlated = min(max_correlated, 2)
            reasons.append("过热阶段降权")
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
        stage = str(signal.get("stage", "") or "")
        if strategy_line == "趋势突破线":
            if self._is_strong_trend_signal(signal):
                return {
                    "name": "强趋势主升",
                    "take_profit_mode": "roi",
                    "take_profit_targets": [45.0, 80.0, 130.0],
                    "take_profit_ratios": [0.10, 0.25, 0.65],
                    "stop_loss_pct": 7.0,
                }
            if stage == "pre_break":
                return {
                    "name": "预突破主攻",
                    "take_profit_mode": "roi",
                    "take_profit_targets": [40.0, 70.0, 110.0],
                    "take_profit_ratios": [0.10, 0.30, 0.60],
                    "stop_loss_pct": 6.5,
                }
            if stage == "confirmed_breakout":
                return {
                    "name": "确认突破轻仓",
                    "take_profit_mode": "roi",
                    "take_profit_targets": [35.0, 60.0, 95.0],
                    "take_profit_ratios": [0.15, 0.35, 0.50],
                    "stop_loss_pct": 6.0,
                }
            if stage == "mania":
                return {
                    "name": "过热反向观察",
                    "take_profit_mode": "roi",
                    "take_profit_targets": [30.0, 50.0, 80.0],
                    "take_profit_ratios": [0.20, 0.35, 0.45],
                    "stop_loss_pct": 5.5,
                }
            return {
                "name": "普通趋势",
                "take_profit_mode": "roi",
                "take_profit_targets": [35.0, 60.0, 95.0],
                "take_profit_ratios": [0.15, 0.35, 0.50],
                "stop_loss_pct": 6.0,
            }
        if strategy_line == "均线二启线":
            return {
                "name": "均线二次启动",
                "take_profit_mode": "roi",
                "take_profit_targets": [32.0, 55.0, 85.0],
                "take_profit_ratios": [0.15, 0.35, 0.50],
                "stop_loss_pct": 5.5,
            }

        return {
            "name": "默认策略",
            "take_profit_mode": "roi",
            "take_profit_targets": [35.0, 60.0, 95.0],
            "take_profit_ratios": [0.15, 0.35, 0.50],
            "stop_loss_pct": 6.0,
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
        """Avoid entering after a blow-off spike that has already started reversing."""
        if not getattr(self.config, "spike_reversal_guard_enabled", True):
            return ""
        if current_price <= 0:
            return ""

        side = str(direction or "").upper()
        is_long = side in {"LONG", "BUY"}
        min_runup = float(self.config.spike_guard_min_runup_pct)
        min_pullback = float(self.config.spike_guard_min_pullback_pct)
        min_wick = float(self.config.spike_guard_min_wick_ratio)

        def _as_float(candle: dict, key: str) -> float:
            try:
                return float(candle.get(key, 0) or 0)
            except (TypeError, ValueError):
                return 0.0

        def _evaluate(klines: list[dict], interval: str, min_candles: int) -> str:
            if len(klines) < min_candles:
                return ""
            recent = klines[-min(len(klines), 12):]
            opens = [_as_float(k, "open") for k in recent]
            closes = [_as_float(k, "close") for k in recent]
            highs = [_as_float(k, "high") for k in recent]
            lows = [_as_float(k, "low") for k in recent]
            if not opens or not highs or not lows or min(lows) <= 0:
                return ""

            high = max(highs)
            low = min(lows)
            first_open = opens[0]
            if first_open <= 0 or high <= 0 or low <= 0:
                return ""

            if is_long:
                impulse_pct = (high - first_open) / first_open * 100.0
                fallback_impulse_pct = (high - low) / low * 100.0
                pullback_pct = (high - current_price) / high * 100.0
            else:
                impulse_pct = (first_open - low) / first_open * 100.0
                fallback_impulse_pct = (high - low) / low * 100.0
                pullback_pct = (current_price - low) / low * 100.0
            impulse_pct = max(impulse_pct, fallback_impulse_pct)

            if impulse_pct < min_runup or pullback_pct < min_pullback:
                return ""

            wick_ratio = 0.0
            reversal_candle = False
            recent_direction_bars = 0
            for candle in recent[-5:]:
                open_price = _as_float(candle, "open")
                close_price = _as_float(candle, "close")
                high_price = _as_float(candle, "high")
                low_price = _as_float(candle, "low")
                candle_range = max(high_price - low_price, 0.0)
                if candle_range <= 0:
                    continue
                if is_long:
                    wick = high_price - max(open_price, close_price)
                    this_wick_ratio = max(wick, 0.0) / candle_range
                    wick_ratio = max(wick_ratio, this_wick_ratio)
                    if close_price < open_price:
                        recent_direction_bars += 1
                    if close_price < open_price and this_wick_ratio >= min_wick:
                        reversal_candle = True
                else:
                    wick = min(open_price, close_price) - low_price
                    this_wick_ratio = max(wick, 0.0) / candle_range
                    wick_ratio = max(wick_ratio, this_wick_ratio)
                    if close_price > open_price:
                        recent_direction_bars += 1
                    if close_price > open_price and this_wick_ratio >= min_wick:
                        reversal_candle = True

            ma_window = closes[-min(7, len(closes)):]
            short_ma = sum(ma_window) / len(ma_window) if ma_window else current_price
            ma_broken = current_price < short_ma if is_long else current_price > short_ma
            violent_range = fallback_impulse_pct >= max(min_runup * 1.4, min_runup + 2.0)

            if not (wick_ratio >= min_wick or reversal_candle or ma_broken or recent_direction_bars >= 2 or violent_range):
                return ""

            if is_long:
                return (
                    f"{interval}冲高回落：先拉升 {impulse_pct:.2f}%，"
                    f"现距高点回落 {pullback_pct:.2f}%，"
                    f"{'已跌破短均' if ma_broken else f'影线占比 {wick_ratio:.0%}'}"
                )
            return (
                f"{interval}急跌反抽：先下跌 {impulse_pct:.2f}%，"
                f"现距低点反弹 {pullback_pct:.2f}%，"
                f"{'已站回短均' if ma_broken else f'影线占比 {wick_ratio:.0%}'}"
            )

        for interval, limit, min_candles in (("1m", 12, 6), ("5m", 8, 4)):
            try:
                klines = get_klines(symbol, interval=interval, limit=limit) or []
            except Exception as exc:
                logger.debug(f"{symbol} spike reversal guard skipped ({interval}): {exc}")
                continue
            reason = _evaluate(klines, interval, min_candles)
            if reason:
                return reason

        return ""
