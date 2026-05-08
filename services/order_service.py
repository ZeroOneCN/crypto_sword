"""Order service wrapper for protective and market order operations."""

from __future__ import annotations

import threading
import time
from typing import Any

import logging
from binance_trading_executor import (
    OrderResult,
    TradingSignal,
    _build_take_profit_slices,
    _ensure_symbol_leverage,
    _get_lot_step_size,
    _is_precision_error,
    _normalize_take_profit_ratios,
    _truncate_to_step,
    _warn_ws_fallback,
    _ws_ambiguous_order_result,
    _ws_fallback_allowed,
    adjust_price_precision,
    adjust_quantity_precision,
    calculate_effective_roi_pcts,
    calculate_min_quantity_for_notional,
    calculate_position_size,
    calculate_stop_loss,
    calculate_take_profit,
    calculate_take_profit_prices_by_roi,
    check_exchange_health,
    check_slippage,
    get_cached_market_price,
    get_native_binance_client,
    get_ticker_24hr,
    get_ws_order_client,
    is_native_binance_configured,
    is_ws_order_enabled,
    should_trade,
    validate_symbol_tradeable,
)

logger = logging.getLogger(__name__)


class OrderService:
    """Isolate direct order-operation dependency surface."""

    _cache_lock = threading.RLock()
    _open_orders_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    @classmethod
    def invalidate_symbol(cls, symbol: str):
        with cls._cache_lock:
            cls._open_orders_cache.pop(str(symbol or "").upper(), None)

    @classmethod
    def _fetch_open_orders_cached(cls, symbol: str, ttl_sec: float = 0.75) -> list[dict[str, Any]]:
        symbol_key = str(symbol or "").upper()
        now = time.time()
        with cls._cache_lock:
            cached = cls._open_orders_cache.get(symbol_key)
            if cached and now - cached[0] < max(0.0, ttl_sec):
                return [dict(item) for item in cached[1]]

        orders = cls.fetch_open_algo_orders(symbol_key) or []
        normalized = [dict(item) for item in orders if isinstance(item, dict)]
        with cls._cache_lock:
            cls._open_orders_cache[symbol_key] = (now, normalized)
        return [dict(item) for item in normalized]

    @staticmethod
    def cancel_stop_loss(symbol: str, order_id: int) -> bool:
        ok = bool(OrderService.cancel_protective_order(symbol, order_id))
        if ok:
            OrderService.invalidate_symbol(symbol)
        return ok

    @staticmethod
    def cancel_protective(symbol: str, order_id: int) -> bool:
        ok = bool(OrderService.cancel_protective_order(symbol, order_id))
        if ok:
            OrderService.invalidate_symbol(symbol)
        return ok

    @staticmethod
    def place_stop_loss(
        symbol: str,
        side: str,
        quantity: float,
        stop_price: float,
        *,
        position_side: str,
        reduce_only: bool = True,
        trigger_buffer_pct: float = 0.0,
    ):
        result = OrderService.place_stop_loss_order(
            symbol,
            side,
            quantity,
            stop_price,
            position_side=position_side,
            reduce_only=reduce_only,
            trigger_buffer_pct=trigger_buffer_pct,
        )
        OrderService.invalidate_symbol(symbol)
        return result

    @staticmethod
    def place_take_profit(
        symbol: str,
        side: str,
        quantity: float,
        target_price: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        result = OrderService.place_take_profit_order(
            symbol,
            side,
            quantity,
            target_price,
            position_side=position_side,
            reduce_only=reduce_only,
        )
        OrderService.invalidate_symbol(symbol)
        return result

    @staticmethod
    def place_market(
        symbol: str,
        side: str,
        quantity: float,
        *,
        position_side: str,
        reduce_only: bool = True,
    ):
        result = OrderService.place_market_order(
            symbol,
            side,
            quantity,
            position_side=position_side,
            reduce_only=reduce_only,
        )
        OrderService.invalidate_symbol(symbol)
        return result

    @staticmethod
    def place_market_order(
        symbol: str,
        side: str,
        quantity: float,
        leverage: int = 5,
        position_side: str | None = None,
        reduce_only: bool = False,
    ) -> OrderResult:
        """Place a market order through WS API first, REST as safe fallback."""
        try:
            current_price = get_cached_market_price(symbol, max_age_sec=5.0)
            try:
                if current_price <= 0:
                    ticker = get_ticker_24hr(symbol)
                    if isinstance(ticker, dict):
                        current_price = float(ticker.get("lastPrice", 0))
            except Exception as exc:
                logger.debug(f"获取 {symbol} 价格失败，跳过校验: {exc}")

            if not check_exchange_health():
                return OrderResult(symbol, side, quantity, 0, 0, "REJECTED", "交易所维护中，暂停交易")

            tradeable, reject_reason = validate_symbol_tradeable(symbol)
            if not tradeable:
                return OrderResult(symbol, side, quantity, 0, 0, "REJECTED", reject_reason)

            quantity = adjust_quantity_precision(symbol, quantity, current_price)

            if not is_native_binance_configured() or not get_native_binance_client:
                raise RuntimeError("原生 Binance API 未配置，无法下单")

            resolved_position_side = position_side or ("LONG" if side == "BUY" else "SHORT")
            applied_leverage = int(leverage)
            if not reduce_only:
                applied_leverage = _ensure_symbol_leverage(symbol, int(leverage))

            ws_used = False
            try:
                if is_ws_order_enabled() and get_ws_order_client is not None:
                    result = get_ws_order_client().new_order(  # type: ignore[union-attr]
                        symbol=symbol,
                        side=side,
                        order_type="MARKET",
                        quantity=quantity,
                        position_side=resolved_position_side,
                        reduce_only=reduce_only,
                        new_order_resp_type="RESULT",
                    )
                    ws_used = True
                else:
                    raise RuntimeError("WS API 下单未启用")
            except Exception as ws_error:
                if not _ws_fallback_allowed(ws_error):
                    return _ws_ambiguous_order_result(symbol, side, quantity, str(ws_error))
                if "未启用" not in str(ws_error):
                    _warn_ws_fallback("MARKET", symbol, ws_error)
                result = get_native_binance_client().new_order(  # type: ignore[union-attr]
                    symbol=symbol,
                    side=side,
                    order_type="MARKET",
                    quantity=quantity,
                    position_side=resolved_position_side,
                    reduce_only=reduce_only,
                    new_order_resp_type="RESULT",
                )

            executed_qty = float(result.get("executedQty", 0))
            executed_price = float(result.get("avgPrice", 0)) or float(result.get("price", 0))
            order_id = int(result.get("orderId", result.get("orderID", 0)))
            status = result.get("status", "UNKNOWN")

            high_slippage = False
            if current_price > 0 and executed_price > 0 and not check_slippage(current_price, executed_price):
                logger.warning(f"⚠️ {symbol} 滑点过大，已成交但需关注")
                high_slippage = True

            message = f"Leverage: {applied_leverage}x | {'WS_API' if ws_used else 'REST'}"
            if high_slippage:
                message += " | HIGH_SLIPPAGE"

            return OrderResult(symbol, side, executed_qty, executed_price, order_id, status, message)
        except Exception as exc:
            return OrderResult(symbol, side, quantity, 0, 0, "ERROR", str(exc))

    @staticmethod
    def place_stop_loss_order(
        symbol: str,
        side: str,
        quantity: float,
        stop_price: float,
        position_side: str | None = None,
        reduce_only: bool = True,
        trigger_buffer_pct: float = 0.0,
    ) -> OrderResult:
        """Place a STOP_MARKET protective order through the unified order service."""
        resolved_position_side = position_side or ("LONG" if side == "SELL" else "SHORT")
        stop_rounding = "floor" if side == "SELL" else "ceil"
        trigger_price = stop_price
        try:
            if not is_native_binance_configured() or not get_native_binance_client:
                raise RuntimeError("Native Binance API is not configured; cannot place stop loss")

            quantity = adjust_quantity_precision(symbol, quantity)
            stop_price = adjust_price_precision(symbol, stop_price, rounding=stop_rounding)
            trigger_price = stop_price
            if trigger_buffer_pct > 0:
                if side == "SELL":
                    trigger_price = stop_price * (1 - trigger_buffer_pct / 100.0)
                else:
                    trigger_price = stop_price * (1 + trigger_buffer_pct / 100.0)
                trigger_price = adjust_price_precision(symbol, trigger_price, rounding=stop_rounding)

            ws_used = False
            try:
                if is_ws_order_enabled() and get_ws_order_client is not None:
                    result = get_ws_order_client().new_algo_order(  # type: ignore[union-attr]
                        symbol=symbol,
                        side=side,
                        order_type="STOP_MARKET",
                        quantity=quantity,
                        position_side=resolved_position_side,
                        reduce_only=reduce_only,
                        trigger_price=trigger_price,
                        working_type="MARK_PRICE",
                        new_order_resp_type="RESULT",
                    )
                    ws_used = True
                else:
                    raise RuntimeError("WS API 下单未启用")
            except Exception as ws_error:
                if not _ws_fallback_allowed(ws_error):
                    return _ws_ambiguous_order_result(symbol, side, quantity, str(ws_error))
                if "未启用" not in str(ws_error):
                    _warn_ws_fallback("STOP_MARKET", symbol, ws_error)
                result = get_native_binance_client().new_algo_order(  # type: ignore[union-attr]
                    symbol=symbol,
                    side=side,
                    order_type="STOP_MARKET",
                    quantity=quantity,
                    position_side=resolved_position_side,
                    reduce_only=reduce_only,
                    trigger_price=trigger_price,
                    working_type="MARK_PRICE",
                    new_order_resp_type="RESULT",
                )

            executed_qty = float(result.get("executedQty", 0))
            order_id = int(result.get("algoId", result.get("orderId", result.get("orderID", 0))))
            status = result.get("status", result.get("algoStatus", "ALGO_ORDER_PLACED"))
            return OrderResult(
                symbol,
                side,
                executed_qty,
                trigger_price,
                order_id,
                status,
                f"Stop loss order placed via {'WS_API' if ws_used else 'REST'} | logical={stop_price:.8f} trigger={trigger_price:.8f}",
            )
        except Exception as exc:
            if _is_precision_error(exc):
                try:
                    step = _get_lot_step_size(symbol)
                    quantity_retry = _truncate_to_step(max(quantity - step, step), step)
                    trigger_retry = adjust_price_precision(symbol, trigger_price, rounding=stop_rounding)
                    result = get_native_binance_client().new_algo_order(  # type: ignore[union-attr]
                        symbol=symbol,
                        side=side,
                        order_type="STOP_MARKET",
                        quantity=quantity_retry,
                        position_side=resolved_position_side,
                        reduce_only=reduce_only,
                        trigger_price=trigger_retry,
                        working_type="MARK_PRICE",
                        new_order_resp_type="RESULT",
                    )
                    executed_qty = float(result.get("executedQty", 0))
                    order_id = int(result.get("algoId", result.get("orderId", result.get("orderID", 0))))
                    status = result.get("status", result.get("algoStatus", "ALGO_ORDER_PLACED"))
                    logger.warning(f"⚖️ {symbol} stop-loss precision retry success: qty {quantity} -> {quantity_retry}")
                    return OrderResult(
                        symbol,
                        side,
                        executed_qty or quantity_retry,
                        trigger_retry,
                        order_id,
                        status,
                        f"Stop loss order placed after precision retry | logical={stop_price:.8f} trigger={trigger_retry:.8f}",
                    )
                except Exception as retry_error:
                    exc = retry_error
            return OrderResult(symbol, side, quantity, stop_price, 0, "ERROR", str(exc))

    @staticmethod
    def place_take_profit_order(
        symbol: str,
        side: str,
        quantity: float,
        trigger_price: float,
        position_side: str | None = None,
        reduce_only: bool = True,
    ) -> OrderResult:
        """Place a TAKE_PROFIT_MARKET protective order through the unified order service."""
        resolved_position_side = position_side or ("LONG" if side == "SELL" else "SHORT")
        tp_rounding = "ceil" if side == "SELL" else "floor"
        try:
            if not is_native_binance_configured() or not get_native_binance_client:
                raise RuntimeError("Native Binance API is not configured; cannot place take profit")

            quantity = adjust_quantity_precision(symbol, quantity)
            trigger_price = adjust_price_precision(symbol, trigger_price, rounding=tp_rounding)

            ws_used = False
            try:
                if is_ws_order_enabled() and get_ws_order_client is not None:
                    result = get_ws_order_client().new_algo_order(  # type: ignore[union-attr]
                        symbol=symbol,
                        side=side,
                        order_type="TAKE_PROFIT_MARKET",
                        quantity=quantity,
                        position_side=resolved_position_side,
                        reduce_only=reduce_only,
                        trigger_price=trigger_price,
                        working_type="MARK_PRICE",
                        new_order_resp_type="RESULT",
                    )
                    ws_used = True
                else:
                    raise RuntimeError("WS API 下单未启用")
            except Exception as ws_error:
                if not _ws_fallback_allowed(ws_error):
                    return _ws_ambiguous_order_result(symbol, side, quantity, str(ws_error))
                if "未启用" not in str(ws_error):
                    _warn_ws_fallback("TAKE_PROFIT_MARKET", symbol, ws_error)
                result = get_native_binance_client().new_algo_order(  # type: ignore[union-attr]
                    symbol=symbol,
                    side=side,
                    order_type="TAKE_PROFIT_MARKET",
                    quantity=quantity,
                    position_side=resolved_position_side,
                    reduce_only=reduce_only,
                    trigger_price=trigger_price,
                    working_type="MARK_PRICE",
                    new_order_resp_type="RESULT",
                )

            executed_qty = float(result.get("executedQty", 0))
            order_id = int(result.get("algoId", result.get("orderId", result.get("orderID", 0))))
            status = result.get("status", result.get("algoStatus", "ALGO_ORDER_PLACED"))
            return OrderResult(
                symbol,
                side,
                executed_qty or quantity,
                trigger_price,
                order_id,
                status,
                f"Take profit order placed via {'WS_API' if ws_used else 'REST'}",
            )
        except Exception as exc:
            if _is_precision_error(exc):
                try:
                    step = _get_lot_step_size(symbol)
                    quantity_retry = _truncate_to_step(max(quantity - step, step), step)
                    trigger_retry = adjust_price_precision(symbol, trigger_price, rounding=tp_rounding)
                    result = get_native_binance_client().new_algo_order(  # type: ignore[union-attr]
                        symbol=symbol,
                        side=side,
                        order_type="TAKE_PROFIT_MARKET",
                        quantity=quantity_retry,
                        position_side=resolved_position_side,
                        reduce_only=reduce_only,
                        trigger_price=trigger_retry,
                        working_type="MARK_PRICE",
                        new_order_resp_type="RESULT",
                    )
                    executed_qty = float(result.get("executedQty", 0))
                    order_id = int(result.get("algoId", result.get("orderId", result.get("orderID", 0))))
                    status = result.get("status", result.get("algoStatus", "ALGO_ORDER_PLACED"))
                    logger.warning(f"⚖️ {symbol} take-profit precision retry success: qty {quantity} -> {quantity_retry}")
                    return OrderResult(
                        symbol,
                        side,
                        executed_qty or quantity_retry,
                        trigger_retry,
                        order_id,
                        status,
                        "Take profit order placed after precision retry",
                    )
                except Exception as retry_error:
                    exc = retry_error
            return OrderResult(symbol, side, quantity, trigger_price, 0, "ERROR", str(exc))

    @staticmethod
    def quick_close(symbol: str, side: str, quantity: float, reason: str = "MANUAL") -> dict[str, Any]:
        """Fast reduce-only market close through the unified order service."""
        started = time.time()
        position_side = "LONG" if side == "SELL" else "SHORT"
        result = OrderService.place_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            position_side=position_side,
            reduce_only=True,
        )
        elapsed_ms = round((time.time() - started) * 1000, 2)
        if result.status in {"FILLED", "NEW"} or (result.order_id > 0 and result.quantity > 0):
            return {
                "success": True,
                "order_id": result.order_id,
                "executed_price": result.executed_price,
                "quantity": result.quantity,
                "elapsed_ms": elapsed_ms,
                "reason": reason,
                "message": result.message,
            }
        return {
            "success": False,
            "error": result.message or result.status,
            "elapsed_ms": elapsed_ms,
            "reason": reason,
        }

    @staticmethod
    def execute_trade(
        signal: TradingSignal,
        account_balance: float,
        risk_per_trade_pct: float = 1.5,
        stop_loss_pct: float = 7.0,
        max_position_pct: float = 35.0,
        leverage: int = 5,
        quantity: float | None = None,
        stop_loss_price: float | None = None,
        take_profit_roi_pcts: list[float] | None = None,
        take_profit_price_pcts: list[float] | None = None,
        take_profit_ratios: list[float] | None = None,
        take_profit_mode: str = "roi",
        stop_trigger_buffer_pct: float = 0.0,
        defer_protection_orders: bool = False,
    ) -> dict[str, Any]:
        """Execute entry plus protective orders through OrderService."""
        if not should_trade(signal):
            return {
                "symbol": signal.symbol,
                "action": "SKIPPED",
                "reason": f"Signal does not meet trading criteria (stage={signal.stage}, direction={signal.direction})",
            }

        side = "BUY" if signal.direction == "LONG" else "SELL"
        opposite_side = "SELL" if signal.direction == "LONG" else "BUY"

        if stop_loss_price is None:
            stop_loss_price = calculate_stop_loss(signal.entry_price, stop_loss_pct, signal.direction, symbol=signal.symbol)
        if quantity is None:
            quantity = calculate_position_size(
                account_balance=account_balance,
                risk_per_trade_pct=risk_per_trade_pct,
                entry_price=signal.entry_price,
                stop_loss_price=stop_loss_price,
                max_position_pct=max_position_pct,
            )
        if quantity <= 0:
            return {"symbol": signal.symbol, "action": "SKIPPED", "reason": "Calculated position size is zero or negative"}

        min_quantity = calculate_min_quantity_for_notional(signal.symbol, signal.entry_price)
        if min_quantity > quantity:
            old_quantity = quantity
            quantity = min_quantity
            logger.warning(
                f"⚖️ {signal.symbol} 仓位低于最小名义价值，数量从 {old_quantity} 上调到 {quantity} "
                f"(预估名义 ${quantity * signal.entry_price:.2f})"
            )

        logger.info(f"📤 {signal.symbol} 准备下单: 方向={side}, 数量={quantity}, 杠杆={leverage}x, 止损=${stop_loss_price:.4f}")
        entry_result = OrderService.place_market_order(
            signal.symbol,
            side,
            quantity,
            leverage,
            position_side=signal.direction,
        )
        leverage_applied = int(leverage)
        try:
            if isinstance(entry_result.message, str) and "Leverage:" in entry_result.message:
                leverage_applied = int(str(entry_result.message).split("Leverage:")[1].split("x")[0].strip())
        except Exception:
            leverage_applied = int(leverage)

        terminal_failed_statuses = {"ERROR", "REJECTED", "EXPIRED", "CANCELED", "CANCELLED"}
        entry_filled = entry_result.status in {"FILLED", "HIGH_SLIPPAGE"} or (
            entry_result.status not in terminal_failed_statuses
            and entry_result.order_id > 0
            and entry_result.quantity > 0
            and entry_result.executed_price > 0
        )
        if not entry_filled:
            return {
                "symbol": signal.symbol,
                "action": "FAILED",
                "reason": f"Entry order failed: {entry_result.message}",
                "order_result": entry_result.to_dict(),
            }
        if entry_result.status != "FILLED":
            logger.warning(
                f"⚠️ {signal.symbol} 入场订单状态为 {entry_result.status}，"
                f"但检测到已成交 quantity={entry_result.quantity} price={entry_result.executed_price}，继续挂保护单"
            )

        actual_entry_price = entry_result.executed_price or signal.entry_price
        actual_quantity = entry_result.quantity or quantity
        if take_profit_mode == "roi":
            target_pcts = take_profit_roi_pcts or [20.0]
            take_profit_prices = calculate_take_profit_prices_by_roi(
                entry_price=actual_entry_price,
                target_roi_pcts=target_pcts,
                leverage=leverage,
                side=signal.direction,
                symbol=signal.symbol,
            )
            price_move_pcts = [pct / float(leverage) for pct in target_pcts]
            effective_roi_pcts = list(target_pcts)
        else:
            target_pcts = take_profit_price_pcts or take_profit_roi_pcts or [20.0]
            take_profit_prices = calculate_take_profit(actual_entry_price, target_pcts, signal.direction, symbol=signal.symbol)
            price_move_pcts = list(target_pcts)
            effective_roi_pcts = calculate_effective_roi_pcts(actual_entry_price, take_profit_prices, leverage, signal.direction)

        normalized_ratios = _normalize_take_profit_ratios(len(take_profit_prices), take_profit_ratios)
        take_profit_quantities = _build_take_profit_slices(signal.symbol, actual_quantity, normalized_ratios)
        take_profit_orders: list[dict[str, Any]] = []

        for index, trigger_price in enumerate(take_profit_prices):
            if index >= len(take_profit_quantities):
                break
            tp_quantity = take_profit_quantities[index]
            if tp_quantity <= 0:
                continue
            if defer_protection_orders:
                tp_result = OrderResult(
                    signal.symbol,
                    opposite_side,
                    tp_quantity,
                    trigger_price,
                    0,
                    "DEFERRED",
                    "Take profit deferred to runtime protection step",
                )
            else:
                tp_result = OrderService.place_take_profit_order(
                    signal.symbol,
                    opposite_side,
                    tp_quantity,
                    trigger_price,
                    position_side=signal.direction,
                )
            take_profit_orders.append(
                {
                    "level": index + 1,
                    "target_roi_pct": effective_roi_pcts[index],
                    "price_move_pct": price_move_pcts[index],
                    "price": trigger_price,
                    "quantity": tp_quantity,
                    "ratio": normalized_ratios[index] if index < len(normalized_ratios) else 0.0,
                    "order_id": tp_result.order_id,
                    "status": tp_result.status,
                    "message": tp_result.message,
                }
            )

        if defer_protection_orders:
            sl_result = OrderResult(
                signal.symbol,
                opposite_side,
                actual_quantity,
                stop_loss_price,
                0,
                "DEFERRED",
                "Stop loss deferred to runtime protection step",
            )
        else:
            sl_result = OrderService.place_stop_loss_order(
                signal.symbol,
                opposite_side,
                actual_quantity,
                stop_loss_price,
                position_side=signal.direction,
                trigger_buffer_pct=stop_trigger_buffer_pct,
            )

        return {
            "symbol": signal.symbol,
            "action": "EXECUTED",
            "direction": signal.direction,
            "quantity": actual_quantity,
            "order_id": entry_result.order_id,
            "entry_order": entry_result.to_dict(),
            "stop_loss_order": sl_result.to_dict(),
            "stop_loss_price": stop_loss_price,
            "stop_trigger_buffer_pct": stop_trigger_buffer_pct,
            "take_profit_orders": take_profit_orders,
            "take_profit_prices": take_profit_prices,
            "take_profit_roi_pcts": effective_roi_pcts,
            "take_profit_price_pcts": price_move_pcts,
            "take_profit_mode": take_profit_mode,
            "leverage_applied": leverage_applied,
            "protection_deferred": bool(defer_protection_orders),
            "risk_amount_usdt": round(account_balance * risk_per_trade_pct / 100, 2),
            "stage": signal.stage,
        }

    @staticmethod
    def fetch_open(symbol: str):
        return OrderService.fetch_open_orders(symbol)

    @staticmethod
    def fetch_open_algo(symbol: str):
        return OrderService.fetch_open_algo_orders(symbol)

    @staticmethod
    def cancel_protective_order(symbol: str, order_id: int) -> bool:
        """Best-effort cancellation for stop-loss / take-profit protective orders."""
        if not order_id:
            return False
        if not is_native_binance_configured() or not get_native_binance_client:
            raise RuntimeError("Native Binance API is not configured; cannot cancel order")

        try:
            if is_ws_order_enabled() and get_ws_order_client is not None:
                try:
                    get_ws_order_client().cancel_algo_order(symbol, order_id)  # type: ignore[union-attr]
                    return True
                except Exception as ws_algo_error:
                    if not _ws_fallback_allowed(ws_algo_error):
                        logger.warning(f"{symbol} WS cancel ambiguous id={order_id}: {ws_algo_error}")
                        return False
                    logger.debug(f"{symbol} WS algo cancel fallback id={order_id}: {ws_algo_error}")
                    try:
                        get_ws_order_client().cancel_order(symbol, order_id)  # type: ignore[union-attr]
                        return True
                    except Exception as ws_order_error:
                        if not _ws_fallback_allowed(ws_order_error):
                            logger.warning(f"{symbol} WS normal cancel ambiguous id={order_id}: {ws_order_error}")
                            return False
                        logger.debug(f"{symbol} WS normal cancel fallback id={order_id}: {ws_order_error}")

            get_native_binance_client().cancel_algo_order(symbol, order_id)  # type: ignore[union-attr]
            return True
        except Exception as algo_error:
            algo_text = str(algo_error)
            if any(token in algo_text for token in ("Unknown order", "-2011", "Order does not exist")):
                logger.info(f"{symbol} protective order already gone id={order_id}: {algo_error}")
                return True
            try:
                get_native_binance_client().cancel_order(symbol, order_id)  # type: ignore[union-attr]
                return True
            except Exception as order_error:
                order_text = str(order_error)
                if any(token in order_text for token in ("Unknown order", "-2011", "Order does not exist")):
                    logger.info(f"{symbol} protective order already gone id={order_id}: {order_error}")
                    return True
                logger.warning(
                    f"{symbol} native cancel failed id={order_id}: "
                    f"algo={algo_error}; order={order_error}"
                )
                return False

    @staticmethod
    def fetch_open_orders(symbol: str | None = None) -> list[dict[str, Any]]:
        """Best-effort fetch for normal open orders."""
        if not is_native_binance_configured() or not get_native_binance_client:
            return []
        return get_native_binance_client().open_orders(symbol)  # type: ignore[union-attr]

    @staticmethod
    def fetch_open_algo_orders(symbol: str | None = None) -> list[dict[str, Any]]:
        """Best-effort fetch for algo open orders such as STOP_MARKET / TAKE_PROFIT_MARKET."""
        orders = OrderService.fetch_open_orders(symbol)
        if is_native_binance_configured() and get_native_binance_client:
            try:
                orders.extend(get_native_binance_client().open_algo_orders(symbol))  # type: ignore[union-attr]
            except Exception:
                pass
        return orders

    @staticmethod
    def _order_id(order: dict[str, Any]) -> int:
        for key in ("algoId", "orderId", "orderID"):
            try:
                value = int(order.get(key, 0) or 0)
            except Exception:
                value = 0
            if value > 0:
                return value
        return 0

    @staticmethod
    def _is_protective_order(order: dict[str, Any], position_side: str | None = None) -> bool:
        if position_side:
            order_position_side = str(order.get("positionSide", order.get("position_side", "")) or "").upper()
            if order_position_side and order_position_side not in {position_side.upper(), "BOTH"}:
                return False

        order_type = str(
            order.get("type", order.get("origType", order.get("orderType", order.get("algoType", "")))) or ""
        ).upper()
        if any(token in order_type for token in ("STOP", "TAKE_PROFIT", "TRAILING", "CONDITIONAL")):
            return True

        if order.get("triggerPrice") or order.get("stopPrice") or order.get("activatePrice"):
            return True

        close_position = str(order.get("closePosition", "")).lower() == "true"
        reduce_only = str(order.get("reduceOnly", "")).lower() == "true"
        return close_position or reduce_only

    @staticmethod
    def _order_type(order: dict[str, Any]) -> str:
        return str(
            order.get("type", order.get("origType", order.get("orderType", order.get("algoType", "")))) or ""
        ).upper()

    @staticmethod
    def _trigger_price(order: dict[str, Any]) -> float:
        for key in ("triggerPrice", "stopPrice", "activatePrice", "price"):
            try:
                value = float(order.get(key, 0) or 0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    @staticmethod
    def _order_quantity(order: dict[str, Any]) -> float:
        for key in ("origQty", "quantity", "executedQty"):
            try:
                value = float(order.get(key, 0) or 0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    def list_symbol_protective_orders(
        self,
        symbol: str,
        position_side: str | None = None,
        close_side: str | None = None,
    ) -> dict[str, Any]:
        """Return currently open protective orders grouped by stop-loss / take-profit."""
        seen: set[int] = set()
        stop_loss_orders: list[dict[str, Any]] = []
        take_profit_orders: list[dict[str, Any]] = []
        unknown_orders: list[dict[str, Any]] = []

        orders = self._fetch_open_orders_cached(symbol)

        close_side_upper = (close_side or "").upper()
        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_side = str(order.get("side", "") or "").upper()
            if close_side_upper and order_side and order_side != close_side_upper:
                continue
            order_id = self._order_id(order)
            if order_id <= 0 or order_id in seen:
                continue
            seen.add(order_id)

            order_type = self._order_type(order)
            item = {
                "order_id": order_id,
                "type": order_type,
                "side": order_side,
                "position_side": str(order.get("positionSide", order.get("position_side", "")) or "").upper(),
                "price": self._trigger_price(order),
                "quantity": self._order_quantity(order),
                "raw": order,
            }
            if "TAKE_PROFIT" in order_type:
                take_profit_orders.append(item)
            elif "STOP" in order_type or "TRAILING" in order_type:
                stop_loss_orders.append(item)
            else:
                unknown_orders.append(item)

        return {
            "checked": len(orders),
            "stop_loss_orders": stop_loss_orders,
            "take_profit_orders": take_profit_orders,
            "unknown_orders": unknown_orders,
        }

    def cancel_symbol_protective_orders(self, symbol: str, position_side: str | None = None) -> dict[str, Any]:
        """Cancel all open exchange-side protective orders for a symbol."""
        seen: set[int] = set()
        canceled: list[int] = []
        failed: list[int] = []

        orders = self._fetch_open_orders_cached(symbol)

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_id = self._order_id(order)
            if order_id <= 0 or order_id in seen:
                continue
            seen.add(order_id)
            if self.cancel_protective(symbol, order_id):
                canceled.append(order_id)
            else:
                failed.append(order_id)

        return {
            "checked": len(orders),
            "canceled": canceled,
            "failed": failed,
        }

    def prune_duplicate_protective_orders(
        self,
        symbol: str,
        position_side: str | None = None,
        close_side: str | None = None,
    ) -> dict[str, Any]:
        """Cancel exact duplicate protective orders while preserving TP ladders.

        Duplicates are defined narrowly as same type/side/position side/trigger
        price. Different TP prices are legitimate ladder orders and are kept.
        """
        close_side_upper = (close_side or "").upper()
        orders = self._fetch_open_orders_cached(symbol, ttl_sec=0.0)
        grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        checked = 0

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not self._is_protective_order(order, position_side=position_side):
                continue
            order_side = str(order.get("side", "") or "").upper()
            if close_side_upper and order_side and order_side != close_side_upper:
                continue
            order_id = self._order_id(order)
            if order_id <= 0:
                continue
            checked += 1
            order_type = self._order_type(order)
            order_position_side = str(order.get("positionSide", order.get("position_side", "")) or "").upper()
            price_key = f"{self._trigger_price(order):.12g}"
            key = (order_type, order_side, order_position_side, price_key)
            grouped.setdefault(key, []).append(order)

        canceled: list[int] = []
        failed: list[int] = []
        for duplicates in grouped.values():
            if len(duplicates) <= 1:
                continue
            duplicates.sort(key=lambda item: self._order_id(item), reverse=True)
            for order in duplicates[1:]:
                order_id = self._order_id(order)
                if self.cancel_protective(symbol, order_id):
                    canceled.append(order_id)
                else:
                    failed.append(order_id)

        if canceled or failed:
            self.invalidate_symbol(symbol)
        return {"checked": checked, "canceled": canceled, "failed": failed}


order_service = OrderService()
