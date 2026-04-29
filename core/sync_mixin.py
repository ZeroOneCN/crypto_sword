"""Exchange sync and restoration mixin for the trading engine."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

from adapters.rest_gateway import fetch_symbol_ticker_24h, is_exchange_ready
from adapters.ws_gateway import get_market_price_client_class, get_user_data_client_class
from binance_api_client import get_native_binance_client, is_native_binance_configured
from hermes_paths import hermes_logs_dir
from telegram_notifier import format_close_position_msg, format_error_msg, get_telegram_config, send_telegram_message

from .models import Position

BinanceUserDataWebSocketClient = get_user_data_client_class()
BinanceWebSocketClient = get_market_price_client_class()

logger = logging.getLogger(__name__)


class SyncMixin:
    """Realtime stream sync, exchange reconciliation and restore flows."""

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

    def _handle_ws_tp_fill(
        self,
        position: Position,
        order_id: int,
        filled_qty: float,
        fill_price: float,
        realized_pnl: float | None = None,
        trade_key: str = "",
    ):
        if filled_qty <= 0:
            return
        if trade_key and trade_key in position.processed_tp_trade_keys:
            if order_id in position.take_profit_order_ids:
                position.take_profit_order_ids = [oid for oid in position.take_profit_order_ids if oid != order_id]
            logger.debug(f"{position.symbol} duplicate TP fill skipped: {trade_key}")
            return
        recent_ts = float(getattr(position, "last_partial_notify_ts", 0.0) or 0.0)
        recent_qty = float(getattr(position, "last_partial_notify_qty", 0.0) or 0.0)
        recent_price = float(getattr(position, "last_partial_notify_price", 0.0) or 0.0)
        qty_close = recent_qty > 0 and abs(filled_qty - recent_qty) <= max(1e-9, recent_qty * 0.01)
        price_close = (
            recent_price > 0
            and fill_price > 0
            and abs(fill_price - recent_price) / recent_price <= 0.003
        )
        if recent_ts > 0 and time.time() - recent_ts <= 5.0 and qty_close and price_close:
            if order_id in position.take_profit_order_ids:
                position.take_profit_order_ids = [oid for oid in position.take_profit_order_ids if oid != order_id]
            if trade_key:
                position.processed_tp_trade_keys.add(trade_key)
            logger.debug(f"{position.symbol} duplicate TP fill skipped by recent reduction: {trade_key or order_id}")
            return
        if trade_key:
            position.processed_tp_trade_keys.add(trade_key)
        reduced_qty = min(filled_qty, position.quantity)
        remaining_qty = max(position.quantity - reduced_qty, 0.0)
        if order_id in position.take_profit_order_ids:
            position.take_profit_order_ids = [oid for oid in position.take_profit_order_ids if oid != order_id]
        position.quantity = remaining_qty
        position.last_synced_quantity = remaining_qty
        self._notify_partial_take_profit(position, reduced_qty, remaining_qty, fill_price, realized_pnl)
        if remaining_qty > 0:
            self._move_stop_to_breakeven(position, remaining_qty)
            self._ensure_position_protection(position)

    def _handle_ws_position_snapshot(self, pos_event: dict[str, Any]):
        symbol = str(pos_event.get("s", "") or "")
        if not symbol:
            return
        side_key = str(pos_event.get("ps", "") or "")
        position = self.tracker.get_position(symbol)
        if not position:
            return

        expected_side = "LONG" if position.side == "BUY" else "SHORT"
        if side_key and side_key not in {expected_side, "BOTH"}:
            return

        live_qty = abs(float(pos_event.get("pa", 0) or 0))
        if live_qty <= 0:
            self._request_state_sync_from_ws("ACCOUNT_ZERO", symbol)
            return

        entry_price = self._first_positive(pos_event.get("ep"), position.entry_price)
        unrealized_pnl = self._safe_float(pos_event.get("up", position.pnl), position.pnl)
        if self._rebase_position_from_exchange_snapshot(
            position,
            live_qty=live_qty,
            live_entry_price=entry_price,
            source="ws_account_update",
        ):
            self._apply_live_position_snapshot(
                position,
                {
                    "quantity": live_qty,
                    "entry_price": entry_price,
                    "unrealized_pnl": unrealized_pnl,
                },
                source="ws_account_update_rebase",
            )
            self._ensure_position_protection(position)
            return

        if live_qty + 1e-9 < position.quantity:
            reduced_qty = position.quantity - live_qty
            current_price = self.get_current_prices([symbol]).get(symbol, position.take_profit_price or position.entry_price)
            self._notify_partial_take_profit(position, reduced_qty, live_qty, current_price)
            self._move_stop_to_breakeven(position, live_qty)

        current_price = self.get_current_prices([symbol]).get(symbol, 0.0)
        self._apply_live_position_snapshot(
            position,
            {
                "quantity": live_qty,
                "entry_price": entry_price,
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
            },
            source="ws_account_update",
        )
        self._ensure_position_protection(position)

    def _handle_ws_order_update(self, event: dict[str, Any]):
        """React to user stream order updates."""
        order = event.get("o", {}) if isinstance(event, dict) else {}
        symbol = str(order.get("s", "") or "")
        status = str(order.get("X", "") or "")
        execution_type = str(order.get("x", "") or "")
        order_type = str(order.get("o", "") or "")
        realized_pnl_raw = order.get("rp")
        realized_pnl = (
            float(realized_pnl_raw)
            if realized_pnl_raw not in {None, ""}
            else None
        )
        order_id = int(order.get("i", 0) or 0)
        trade_id = str(order.get("t", "") or "")
        last_fill_qty = abs(float(order.get("l", 0) or 0))
        avg_price = float(order.get("ap", 0) or order.get("L", 0) or 0)
        position_side = str(order.get("ps", "") or "")
        is_take_profit_order = "TAKE_PROFIT" in order_type.upper()

        if status in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"} or execution_type == "TRADE":
            logger.info(
                f"WS order update: {symbol} {order_type} {execution_type}/{status} "
                f"filled={order.get('z', '0')} price={order.get('L', '0')} "
                f"rp={(realized_pnl if realized_pnl is not None else 0.0):.4f}"
            )
            position = self.tracker.get_position(symbol)
            if position:
                expected_side = "LONG" if position.side == "BUY" else "SHORT"
                if position_side in {"", "BOTH", expected_side}:
                    if order_id == position.stop_loss_order_id and execution_type == "TRADE":
                        if last_fill_qty > 0:
                            position.exchange_realized_pnl += float(realized_pnl or 0.0)
                            position.exchange_realized_exit_value += (avg_price or position.current_stop) * last_fill_qty
                            position.exchange_realized_quantity += last_fill_qty
                        self._request_state_sync_from_ws(f"{execution_type}/{status}", symbol)
                        return
                    if (order_id in position.take_profit_order_ids or is_take_profit_order) and execution_type == "TRADE":
                        tp_trade_key = (
                            f"{order_id}:{trade_id}"
                            if order_id > 0 and trade_id
                            else f"{order_id}:{status}:{last_fill_qty:.12f}:{avg_price:.12f}"
                        )
                        self._handle_ws_tp_fill(
                            position,
                            order_id,
                            last_fill_qty,
                            avg_price or position.take_profit_price,
                            realized_pnl,
                            trade_key=tp_trade_key,
                        )
                        return
            self._request_state_sync_from_ws(f"{execution_type}/{status}", symbol)

    def _handle_ws_account_update(self, event: dict[str, Any]):
        """React to account/position updates from user stream."""
        account = event.get("a", {}) if isinstance(event, dict) else {}
        positions = account.get("P", []) or []
        changed_symbols = [str(pos.get("s", "")) for pos in positions if pos.get("s")]
        if changed_symbols:
            logger.info(f"WS position update: {', '.join(changed_symbols[:8])}")
        handled = False
        for pos in positions:
            if pos.get("s"):
                self._handle_ws_position_snapshot(pos)
                handled = True
        if not handled:
            self._request_state_sync_from_ws("ACCOUNT_UPDATE", changed_symbols[0] if changed_symbols else "")

    def _handle_ws_algo_update(self, event: dict[str, Any]):
        """React to conditional/algo order updates from user stream."""
        symbol = str(event.get("s", event.get("symbol", "")) or "")
        event_type = str(event.get("e", "") or "ALGO_UPDATE")
        logger.info(f"WS algo update: {event_type}{f' | {symbol}' if symbol else ''}")
        self._request_state_sync_from_ws(event_type, symbol)

    def _fetch_exchange_realized_close_summary(self, position: Position) -> tuple[float, float, float, float] | None:
        """Fetch real close fills from Binance userTrades and build an exchange-authoritative summary."""
        if not is_native_binance_configured():
            return None
        try:
            start_ms = int((position.entry_time.timestamp() - 10) * 1000)
            end_ms = int((time.time() + 5) * 1000)
            trades = get_native_binance_client().get_trade_history(  # type: ignore[union-attr]
                symbol=position.symbol,
                start_time=start_ms,
                end_time=end_ms,
                limit=1000,
            )
        except Exception as exc:
            logger.warning(f"{position.symbol} 交易所真实盈亏查询失败：{exc}")
            return None

        close_side = "SELL" if position.side == "BUY" else "BUY"
        realized_pnl = 0.0
        exit_value = 0.0
        qty = 0.0
        for trade in trades or []:
            if str(trade.get("side", "")).upper() != close_side:
                continue
            trade_qty = abs(float(trade.get("qty", 0) or 0))
            if trade_qty <= 0:
                continue
            trade_price = float(trade.get("price", 0) or 0)
            realized_pnl += float(trade.get("realizedPnl", 0) or 0)
            exit_value += trade_price * trade_qty
            qty += trade_qty

        if qty <= 0:
            return None
        avg_exit_price = exit_value / qty
        total_qty = max(float(position.initial_quantity or 0.0), qty)
        entry_notional = position.entry_price * total_qty
        pnl_pct = realized_pnl / entry_notional * 100 if entry_notional > 0 else 0.0
        remaining_pnl_delta = realized_pnl - float(position.realized_pnl or 0.0)
        return avg_exit_price, realized_pnl, pnl_pct, remaining_pnl_delta

    def _parse_trade_notes(self, notes: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for note in (notes or "").split(";"):
            if "=" not in note:
                continue
            key, value = note.split("=", 1)
            parsed[key] = value
        return parsed

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _first_positive(*values: Any) -> float:
        for value in values:
            number = SyncMixin._safe_float(value, 0.0)
            if number > 0:
                return number
        return 0.0

    def _derive_current_price_from_pnl(self, position: Position, unrealized_pnl: float) -> float:
        qty = float(position.quantity or 0.0)
        entry = float(position.entry_price or 0.0)
        if qty <= 0 or entry <= 0:
            return 0.0
        if position.side == "BUY":
            return max(entry + unrealized_pnl / qty, 0.0)
        return max(entry - unrealized_pnl / qty, 0.0)

    def _repair_position_targets(self, position: Position):
        entry = float(position.entry_price or 0.0)
        if entry <= 0:
            return

        if float(position.highest_price or 0.0) <= 0:
            position.highest_price = entry
        if float(position.lowest_price or 0.0) <= 0:
            position.lowest_price = entry

        if float(position.stop_loss_price or 0.0) <= 0:
            stop_loss_pct = self._strategy_stop_loss_pct(position.strategy_line)
            if position.side == "BUY":
                position.stop_loss_price = entry * (1 - stop_loss_pct / 100.0)
            else:
                position.stop_loss_price = entry * (1 + stop_loss_pct / 100.0)
        if float(position.current_stop or 0.0) <= 0:
            position.current_stop = float(position.stop_loss_price or 0.0)

        valid_targets = [
            target
            for target in (position.take_profit_targets or [])
            if self._safe_float(target.get("price", 0), 0.0) > 0
        ]
        if valid_targets:
            if float(position.take_profit_price or 0.0) <= 0:
                position.take_profit_price = self._safe_float(valid_targets[0].get("price", 0), 0.0)
            return

        target_roi_pcts, target_ratios = self._build_take_profit_plan(position.strategy_line)
        qty = float(position.quantity or position.initial_quantity or 0.0)
        rebuilt_targets: list[dict[str, Any]] = []
        remaining_qty = qty
        for index, target_pct in enumerate(target_roi_pcts):
            ratio = target_ratios[index] if index < len(target_ratios) else 0.0
            if index == len(target_roi_pcts) - 1:
                target_qty = remaining_qty
            else:
                target_qty = qty * ratio
                remaining_qty = max(remaining_qty - target_qty, 0.0)
            target_price = self._calculate_local_take_profit_price(entry, position.side, float(target_pct))
            rebuilt_targets.append(
                {
                    "level": index + 1,
                    "price": target_price,
                    "quantity": target_qty,
                    "ratio": ratio,
                    "target_roi_pct": float(target_pct),
                    "price_move_pct": abs(target_price - entry) / max(entry, 1e-9) * 100,
                }
            )
        if rebuilt_targets:
            position.take_profit_targets = rebuilt_targets
            position.take_profit_price = float(rebuilt_targets[0]["price"])
            position.target_roi_pct = float(rebuilt_targets[0].get("target_roi_pct", position.target_roi_pct))

    def _apply_live_position_snapshot(self, position: Position, live_pos: dict[str, Any], source: str = ""):
        live_qty = self._safe_float(live_pos.get("quantity", position.quantity), position.quantity)
        live_entry_price = self._first_positive(
            live_pos.get("entry_price"),
            live_pos.get("break_even_price"),
            live_pos.get("avg_price"),
            position.entry_price,
        )
        current_price = self._first_positive(
            live_pos.get("current_price"),
            live_pos.get("mark_price"),
            live_pos.get("last_price"),
        )
        unrealized_pnl = self._safe_float(live_pos.get("unrealized_pnl", position.pnl), position.pnl)

        if live_entry_price > 0:
            position.entry_price = live_entry_price
        if live_qty > 0:
            position.quantity = live_qty
            position.last_synced_quantity = live_qty
            if float(position.initial_quantity or 0.0) <= 0:
                position.initial_quantity = live_qty

        self._repair_position_targets(position)

        if current_price <= 0:
            current_price = self._derive_current_price_from_pnl(position, unrealized_pnl)
        if current_price > 0:
            position.update_price(current_price, self.config.trailing_stop_pct)
            if position.side == "BUY":
                position.highest_price = max(float(position.highest_price or 0.0), current_price)
            else:
                position.lowest_price = min(float(position.lowest_price or current_price), current_price)

        entry_notional = float(position.entry_price or 0.0) * float(position.quantity or 0.0)
        position.pnl = unrealized_pnl
        if entry_notional > 0:
            position.pnl_pct = unrealized_pnl / entry_notional * 100.0
        logger.debug(
            "live position snapshot applied | source=%s symbol=%s qty=%.8f entry=%.8f mark=%.8f pnl=%.4f",
            source or "-",
            position.symbol,
            float(position.quantity or 0.0),
            float(position.entry_price or 0.0),
            float(position.current_price or 0.0),
            float(position.pnl or 0.0),
        )

    def _positions_need_summary_refresh(self) -> bool:
        for position in self.tracker.positions.values():
            if float(position.entry_price or 0.0) <= 0:
                return True
            if float(getattr(position, "current_price", 0.0) or 0.0) <= 0:
                return True
            if float(position.take_profit_price or 0.0) <= 0:
                return True
        return False

    def _rebase_position_from_exchange_snapshot(
        self,
        position: Position,
        *,
        live_qty: float,
        live_entry_price: float,
        source: str,
    ) -> bool:
        """
        Rebase local position when exchange side was externally changed (manual add/reopen).
        Returns True when a full rebase happened.
        """
        qty_increase = live_qty > float(position.quantity or 0.0) + 1e-9
        previous_entry = float(position.entry_price or 0.0)
        entry_shift_pct = 0.0
        if previous_entry > 0 and live_entry_price > 0:
            entry_shift_pct = abs(live_entry_price - previous_entry) / previous_entry * 100.0

        should_rebase = qty_increase or entry_shift_pct >= 1.0
        if not should_rebase:
            if live_entry_price > 0:
                position.entry_price = live_entry_price
            return False

        old_session_id = position.session_id
        if live_entry_price > 0:
            position.entry_price = live_entry_price
        position.quantity = live_qty
        position.initial_quantity = live_qty
        position.last_synced_quantity = live_qty
        position.realized_pnl = 0.0
        position.realized_exit_value = 0.0
        position.realized_quantity = 0.0
        position.partial_tp_count = 0
        position.stop_loss_order_id = 0
        position.take_profit_order_ids = []

        stop_loss_pct = self._strategy_stop_loss_pct(position.strategy_line)
        if position.side == "BUY":
            position.stop_loss_price = position.entry_price * (1 - stop_loss_pct / 100.0)
        else:
            position.stop_loss_price = position.entry_price * (1 + stop_loss_pct / 100.0)
        position.current_stop = position.stop_loss_price

        position.target_roi_pct = float(position.target_roi_pct or self.config.take_profit_pct)
        target_ratios = []
        target_roi_pcts = []
        for target in position.take_profit_targets or []:
            target_ratios.append(max(float(target.get("ratio", 0) or 0), 0.0))
            target_roi_pcts.append(float(target.get("target_roi_pct", position.target_roi_pct) or position.target_roi_pct))
        ratio_total = sum(target_ratios)
        if ratio_total <= 0 or len(target_roi_pcts) != len(target_ratios):
            target_roi_pcts, target_ratios = self._build_take_profit_plan(position.strategy_line)
        else:
            target_ratios = [ratio / ratio_total for ratio in target_ratios]

        rebuilt_targets: list[dict[str, Any]] = []
        remaining_qty = live_qty
        for index, target_pct in enumerate(target_roi_pcts):
            ratio = target_ratios[index] if index < len(target_ratios) else 0.0
            if index == len(target_roi_pcts) - 1:
                target_qty = remaining_qty
            else:
                target_qty = live_qty * ratio
                remaining_qty = max(remaining_qty - target_qty, 0.0)
            target_price = self._calculate_local_take_profit_price(position.entry_price, position.side, float(target_pct))
            rebuilt_targets.append(
                {
                    "level": index + 1,
                    "price": target_price,
                    "quantity": target_qty,
                    "ratio": ratio,
                    "target_roi_pct": float(target_pct),
                    "price_move_pct": abs(target_price - position.entry_price) / max(position.entry_price, 1e-9) * 100,
                }
            )
        if rebuilt_targets:
            position.take_profit_targets = rebuilt_targets
            position.take_profit_price = float(rebuilt_targets[0]["price"])
            position.target_roi_pct = float(rebuilt_targets[0].get("target_roi_pct", position.target_roi_pct))

        position.session_id = self._new_session_id(position.symbol)
        logger.warning(
            f"♻️ {position.symbol} 检测到交易所持仓重基准 source={source} "
            f"qty={live_qty:.6f} entry={position.entry_price:.8f} "
            f"old_session={old_session_id} new_session={position.session_id}"
        )
        send_telegram_message(
            f"⚠️ <b>宙斯交易中枢 | 外部持仓变更接管</b>\n\n"
            f"<b>标的</b>  <code>{position.symbol}</code>\n"
            f"<b>来源</b>  <code>{source}</code>\n"
            f"<b>说明</b>  <code>检测到交易所持仓变化，已重建本地仓位并接管保护单</code>\n"
            f"<b>数量</b>  <code>{live_qty:.6f}</code>\n"
            f"<b>开仓价</b>  <code>{position.entry_price:.8f}</code>"
        )
        return True

    def _audit_all_position_protection(self, source: str = "startup_audit"):
        """Audit all tracked positions and confirm exchange-side protection."""
        if not self.tracker.positions:
            return

        for position in list(self.tracker.positions.values()):
            self._adopt_existing_protection(position)
            self._sync_protective_order_snapshot(position)
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
            account_info = self._get_account_info_cached(ttl_sec=2.5)
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
                close_summary = self._fetch_exchange_realized_close_summary(position)
                if close_summary:
                    exit_price, pnl, pnl_pct, remaining_pnl = close_summary
                    inferred_reason = "EXCHANGE_REALIZED"
                else:
                    close_summary = self._close_summary_from_exchange_realized(
                        position,
                        position.take_profit_price or position.current_stop or position.entry_price,
                    )
                    if close_summary:
                        exit_price, pnl, pnl_pct, remaining_pnl = close_summary
                        inferred_reason = "EXCHANGE_REALIZED"
                    else:
                        current_price = self.get_current_prices([symbol]).get(symbol, position.entry_price)
                        if position.side == "BUY":
                            inferred_reason = "STOP_LOSS" if current_price <= position.current_stop else "TAKE_PROFIT"
                        else:
                            inferred_reason = "STOP_LOSS" if current_price >= position.current_stop else "TAKE_PROFIT"
                        exit_price, pnl, pnl_pct, remaining_pnl = self._close_summary_from_realized_state(
                            position,
                            position.quantity,
                            current_price,
                        )

                position.exit_price = exit_price
                position.exit_time = datetime.now()
                position.exit_reason = f"{inferred_reason}_EXCHANGE"
                position.pnl = pnl
                position.pnl_pct = pnl_pct
                self.daily_pnl += remaining_pnl
                self._record_closed_trade_result(position, pnl)

                self._cancel_position_protection(position)
                self.tracker.remove_position(symbol)
                send_telegram_message(
                    format_close_position_msg(
                        symbol=symbol,
                        direction="LONG" if position.side == "BUY" else "SHORT",
                        entry_price=position.entry_price,
                        exit_price=exit_price,
                        quantity=position.initial_quantity,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        reason=position.exit_reason,
                        duration_hours=(position.exit_time - position.entry_time).total_seconds() / 3600,
                        session_id=position.session_id,
                        strategy_line=position.strategy_line,
                        roi_pct=pnl_pct * max(int(getattr(position, "leverage", 0) or self.config.leverage), 1),
                        price_move_pct=pnl_pct,
                    )
                )

                self._persist_trade_exit(
                    symbol=symbol,
                    session_id=position.session_id,
                    exit_price=exit_price,
                    exit_reason=position.exit_reason,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    realized_pnl=pnl,
                )
                continue

            live_qty = self._safe_float(live_pos.get("quantity", 0), 0.0)
            live_entry_price = self._first_positive(live_pos.get("entry_price"), position.entry_price)
            if live_qty <= 0:
                continue

            if self._rebase_position_from_exchange_snapshot(
                position,
                live_qty=live_qty,
                live_entry_price=live_entry_price,
                source="rest_sync",
            ):
                self._apply_live_position_snapshot(position, live_pos, source="rest_sync_rebase")
                self._ensure_position_protection(position)
                self._sync_protective_order_snapshot(position)
                continue

            if live_qty + 1e-9 < position.quantity:
                reduced_qty = position.quantity - live_qty
                logger.info(f"🔆 {symbol} 交易所已部分止盈：减少 {reduced_qty:.6f}，剩余 {live_qty:.6f}")
                current_price = self.get_current_prices([symbol]).get(symbol, position.take_profit_price)
                self._notify_partial_take_profit(position, reduced_qty, live_qty, current_price)
                self._move_stop_to_breakeven(position, live_qty)
            self._apply_live_position_snapshot(position, live_pos, source="rest_sync")
            self._ensure_position_protection(position)
            self._sync_protective_order_snapshot(position)

    def _passes_liquidity_filter(self, symbol: str, desired_position_value: float) -> bool:
        try:
            ticker = fetch_symbol_ticker_24h(symbol)
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            is_major = symbol.upper() in self.config.major_symbols
            min_quote_volume = (
                self.config.min_quote_volume_usdt
                if is_major or not self.config.target_altcoins
                else self.config.alt_min_quote_volume_usdt
            )
            max_position_to_volume_ratio = (
                self.config.max_position_to_volume_ratio
                if is_major or not self.config.target_altcoins
                else self.config.alt_max_position_to_volume_ratio
            )

            if quote_volume < float(min_quote_volume):
                logger.warning(
                    f"Liquidity filter reject {symbol}: quote_volume={quote_volume:.2f} < {float(min_quote_volume):.0f}"
                )
                return False

            position_to_volume_ratio = desired_position_value / quote_volume if quote_volume > 0 else 1.0
            if position_to_volume_ratio > float(max_position_to_volume_ratio):
                logger.warning(
                    f"Liquidity filter reject {symbol}: pos/vol={position_to_volume_ratio:.4%} > {float(max_position_to_volume_ratio):.4%}"
                )
                return False
            return True
        except Exception as e:
            logger.warning(f"Liquidity filter failed {symbol}: {e}")
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

            live_positions.append(
                {
                    "symbol": pos.get("symbol", ""),
                    "side": side,
                    "quantity": abs(position_amt),
                    "entry_price": self._first_positive(
                        pos.get("entryPrice"),
                        pos.get("breakEvenPrice"),
                        pos.get("avgPrice"),
                    ),
                    "break_even_price": self._first_positive(pos.get("breakEvenPrice")),
                    "current_price": self._first_positive(
                        pos.get("markPrice"),
                        pos.get("lastPrice"),
                    ),
                    "mark_price": self._first_positive(pos.get("markPrice")),
                    "unrealized_pnl": self._safe_float(
                        pos.get("unRealizedProfit", pos.get("unrealizedProfit", 0)),
                        0.0,
                    ),
                }
            )
        return [p for p in live_positions if p["symbol"]]

    def _restore_positions(self, account_info: dict[str, Any]):
        live_positions = self._extract_live_positions(account_info)
        if not live_positions:
            return

        open_trades = self.db.get_open_trades(mode=self.config.mode)
        trades_by_symbol: dict[str, Any] = {}
        for trade in open_trades:
            if trade.symbol not in trades_by_symbol:
                trades_by_symbol[trade.symbol] = trade

        for live_pos in live_positions:
            if live_pos["symbol"] in self.tracker.positions:
                continue

            side = "BUY" if live_pos["side"] == "LONG" else "SELL"
            trade = trades_by_symbol.get(live_pos["symbol"])
            trade_entry_price = float(getattr(trade, "entry_price", 0.0) or 0.0) if trade else 0.0
            live_entry_price = float(live_pos.get("entry_price", 0.0) or 0.0)
            entry_price = trade_entry_price if trade_entry_price > 0 else live_entry_price
            stop_loss_price = float(getattr(trade, "stop_loss", 0.0) or 0.0) if trade else 0.0
            if stop_loss_price <= 0:
                stop_loss_price = (
                    entry_price * (1 - self.config.stop_loss_pct / 100)
                    if side == "BUY"
                    else entry_price * (1 + self.config.stop_loss_pct / 100)
                )
            notes_map = self._parse_trade_notes(trade.notes if trade else "")
            take_profit_targets: list[dict[str, Any]] = []
            take_profit_order_ids: list[int] = []
            target_roi_pct = float(
                notes_map.get("target_roi_pct", self.config.take_profit_pct) or self.config.take_profit_pct
            )
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

            take_profit_price = float(getattr(trade, "take_profit", 0.0) or 0.0) if trade else 0.0
            if take_profit_price <= 0 and entry_price > 0:
                take_profit_price = self._calculate_local_take_profit_price(
                    entry_price,
                    side,
                    self.config.take_profit_pct,
                )
            entry_time = datetime.fromisoformat(trade.entry_time) if trade and trade.entry_time else datetime.now()
            session_id = notes_map.get("session_id", "")
            if not session_id:
                session_id = self._new_session_id(live_pos["symbol"])
            try:
                restored_leverage = int(float(notes_map.get("leverage_applied", self.config.leverage) or self.config.leverage))
            except Exception:
                restored_leverage = int(self.config.leverage)

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
                strategy_line=notes_map.get("strategy_line", ""),
                stop_loss_order_id=0,
                session_id=session_id,
                target_roi_pct=target_roi_pct,
                take_profit_targets=take_profit_targets,
                take_profit_order_ids=take_profit_order_ids,
                leverage=restored_leverage,
            )
            self._apply_live_position_snapshot(restored, live_pos, source="startup_restore")
            self._adopt_existing_protection(restored)
            self.tracker.add_position(restored)
            logger.warning(f"♻️ 已恢复持仓：{restored.symbol} {restored.side} session={session_id}")
            send_telegram_message(
                f"ℹ️ <b>宙斯交易中枢 | 启动持仓恢复</b>\n\n"
                f"<b>标的</b>  <code>{restored.symbol}</code>\n"
                f"<b>方向</b>  <code>{'LONG' if restored.side == 'BUY' else 'SHORT'}</code>\n"
                f"<b>数量</b>  <code>{restored.quantity:.6f}</code>\n"
                f"<b>开仓价</b>  <code>{restored.entry_price:.8f}</code>\n"
                f"<b>说明</b>  <code>该仓位来自交易所现有持仓恢复，不是本轮新开仓</code>"
            )

    def _run_health_checks(self) -> float:
        telegram_config = get_telegram_config()
        if not telegram_config.get("bot_token") or not telegram_config.get("chat_id"):
            raise RuntimeError("Telegram 未配置 bot_token/chat_id")

        native_ready = is_exchange_ready()
        if not native_ready:
            raise RuntimeError("原生 Binance API 未配置：请设置 BINANCE_API_KEY / BINANCE_API_SECRET")
        logger.info("🧬 原生 Binance API 交易通道已启用")

        account_info: dict[str, Any] | None = None
        last_error: Exception | None = None
        startup_attempts = 3
        for attempt in range(1, startup_attempts + 1):
            try:
                account_info = self._get_account_info_cached(ttl_sec=0.0, force=True)
                break
            except Exception as exc:
                last_error = exc
                if attempt >= startup_attempts:
                    raise
                delay_sec = min(2.0 * attempt, 5.0)
                logger.warning(
                    f"Startup account snapshot failed ({attempt}/{startup_attempts}): {exc}; "
                    f"retrying in {delay_sec:.1f}s"
                )
                time.sleep(delay_sec)

        if account_info is None:
            if last_error:
                raise last_error
            raise RuntimeError("Binance account snapshot unavailable during startup")

        balance = float(account_info.get("availableBalance", 0) or 0)
        if balance <= 0:
            raise RuntimeError("账户可用余额为 0")

        log_dir = getattr(self, "_log_dir", hermes_logs_dir())
        if not getattr(log_dir, "exists", lambda: False)():
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
        if not getattr(log_dir, "is_dir", lambda: False)():
            raise RuntimeError(f"日志目录不可用: {log_dir}")

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
                ticker = fetch_symbol_ticker_24h(symbol)
                prices[symbol] = float(ticker.get("lastPrice", 0))
            except Exception as e:
                logger.warning(f"获取 {symbol} 价格失败：{e}")
        return prices
