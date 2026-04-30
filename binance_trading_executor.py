"""Binance trading executor.

Handles order execution, position sizing, stop-loss/take-profit calculation,
and risk management for the breakout scanner trading system.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING
from typing import Any, Optional

try:
    from binance_api_client import get_native_binance_client, is_native_binance_configured
except Exception:
    get_native_binance_client = None

    def is_native_binance_configured() -> bool:
        return False

# 导入行情获取函数（用于滑点保护和名义价值校验）
try:
    from binance_breakout_scanner import fetch_ticker_24hr as get_ticker_24hr
except ImportError:
    def get_ticker_24hr(symbol: str = None) -> dict:
        """Fallback: 返回空字典"""
        return {}


logger = logging.getLogger(__name__)
_leverage_cache: dict[str, int] = {}


def _query_symbol_leverage(symbol: str) -> int:
    """Read current symbol leverage from exchange position risk."""
    if not get_native_binance_client:
        raise RuntimeError("原生 Binance API 客户端不可用")
    positions = get_native_binance_client().position_risk(symbol)  # type: ignore
    if not isinstance(positions, list):
        return 0
    for item in positions:
        if str(item.get("symbol", "")).upper() != symbol.upper():
            continue
        try:
            value = int(float(item.get("leverage", 0) or 0))
            if value > 0:
                return value
        except Exception:
            continue
    return 0


def _ensure_symbol_leverage(symbol: str, target_leverage: int) -> int:
    """Force symbol leverage to target value and return applied leverage."""
    if not get_native_binance_client:
        raise RuntimeError("原生 Binance API 客户端不可用")
    symbol_key = symbol.upper()
    target = int(target_leverage)
    current = 0
    try:
        current = _query_symbol_leverage(symbol)
    except Exception as exc:
        logger.warning(f"{symbol} 杠杆读取失败，尝试直接设置 {target}x：{exc}")

    if current == target:
        _leverage_cache[symbol_key] = target
        return target

    try:
        get_native_binance_client().change_leverage(symbol, target)  # type: ignore
        _leverage_cache[symbol_key] = target
    except Exception as exc:
        cached = int(_leverage_cache.get(symbol_key, 0) or 0)
        if cached == target:
            logger.warning(f"{symbol} 杠杆设置接口失败，但缓存显示已是 {target}x，继续下单：{exc}")
            return target
        raise RuntimeError(f"{symbol} 杠杆设置失败，拒绝下单：{exc}") from exc

    try:
        verified = _query_symbol_leverage(symbol)
    except Exception as exc:
        logger.warning(f"{symbol} 杠杆设置后复查失败，按设置成功结果继续：{exc}")
        return target

    if verified <= 0:
        logger.warning(f"{symbol} 杠杆复查为空，按设置成功结果继续：{target}x")
        return target
    if verified != target:
        raise RuntimeError(f"{symbol} 杠杆未对齐: 期望 {target}x, 实际 {int(verified)}x")
    _leverage_cache[symbol_key] = int(verified)
    return int(verified)


@dataclass
class TradingSignal:
    """A trading signal from the scanner."""
    symbol: str
    stage: str
    direction: str
    entry_price: float
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderResult:
    """Result of an executed order."""
    symbol: str
    side: str
    quantity: float
    executed_price: float
    order_id: int
    status: str
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "executed_price": self.executed_price,
            "order_id": self.order_id,
            "status": self.status,
            "message": self.message,
        }


def _run_native_binance_compat(args: list[str], max_retries: int = 5) -> dict[str, Any] | list[Any]:
    """Compatibility wrapper backed by native Binance REST."""
    if not get_native_binance_client:
        raise RuntimeError("原生 Binance API 客户端不可用")
    
    # 动态节流：根据最近 API 响应时间自动调整延迟
    _throttle_wait()
    
    for attempt in range(max_retries + 1):
        try:
            return get_native_binance_client().command_compat(args)  # type: ignore
        except Exception as e:
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"原生 Binance API 调用失败：{e}")


# 动态节流器
_last_api_call_time = 0.0
_recent_latencies = []
_MAX_LATENCY_HISTORY = 10

def _throttle_wait():
    """动态 API 节流：响应快时延迟短，响应慢时延迟长"""
    global _last_api_call_time, _recent_latencies
    
    avg_latency = sum(_recent_latencies) / len(_recent_latencies) if _recent_latencies else 0.3
    
    if avg_latency < 0.2:
        delay = 0.1
    elif avg_latency < 0.5:
        delay = 0.3
    else:
        delay = 0.5
    
    now = time.time()
    elapsed = now - _last_api_call_time
    if elapsed < delay:
        time.sleep(delay - elapsed)
    
    _last_api_call_time = time.time()

def _record_latency(latency: float):
    """记录 API 响应时间"""
    global _recent_latencies
    _recent_latencies.append(latency)
    if len(_recent_latencies) > _MAX_LATENCY_HISTORY:
        _recent_latencies.pop(0)


# ═══════════════════════════════════════════════════════════════
# P3 生产级稳定性功能
# ═══════════════════════════════════════════════════════════════

_exchange_health_cache = {"status": "OK", "last_check": 0.0}
_EXCHANGE_HEALTH_CACHE_TTL = 60  # 60 秒缓存

def check_exchange_health() -> bool:
    """检查交易所系统状态 (P3-1)"""
    global _exchange_health_cache
    now = time.time()

    # 使用缓存避免频繁请求
    if now - _exchange_health_cache["last_check"] < _EXCHANGE_HEALTH_CACHE_TTL:
        return _exchange_health_cache["status"] == "OK"

    try:
        # 获取系统状态（使用原生客户端）
        if get_native_binance_client and is_native_binance_configured():
            client = get_native_binance_client()
            result = client.exchange_info()
            if isinstance(result, dict):
                is_normal = bool(result.get("symbols"))
                status = 0 if is_normal else 1
                msg = "exchange_info_ok" if is_normal else "exchange_info_empty"

                _exchange_health_cache = {
                    "status": "OK" if is_normal else f"UNHEALTHY ({msg})",
                    "last_check": now
                }

                if not is_normal:
                    logger.warning(f"⚠️ 交易所状态异常：{msg} (Status: {status})")
                return is_normal
        return True  # 默认视为正常
    except Exception as e:
        logger.warning(f"检查交易所状态失败：{e}")
        return True  # 失败时不阻塞交易


def check_slippage(entry_price: float, executed_price: float, max_slippage_pct: float = 0.5) -> bool:
    """检查滑点是否超过阈值 (P3-2)"""
    if executed_price == 0 or entry_price == 0:
        return True  # 无法计算时放行
    
    slippage = abs(executed_price - entry_price) / entry_price * 100
    
    if slippage > max_slippage_pct:
        logger.warning(f"⚠️ 滑点过大：{slippage:.2f}% > {max_slippage_pct}% | 入场价：{entry_price:.4f} -> 成交价：{executed_price:.4f}")
        return False
    return True


def ensure_profile_selected(profile: str = "main") -> None:
    """Deprecated no-op retained for older imports."""
    return None


# ═══ 交易所精度信息缓存 ═══
_exchange_info_cache: Optional[dict] = None
_exchange_info_cache_time: float = 0
EXCHANGE_INFO_CACHE_TTL = 300  # 5 分钟缓存


def get_exchange_info() -> dict:
    """获取交易所信息（带 5 分钟缓存）"""
    global _exchange_info_cache, _exchange_info_cache_time
    now = time.time()
    if _exchange_info_cache and (now - _exchange_info_cache_time) < EXCHANGE_INFO_CACHE_TTL:
        return _exchange_info_cache

    if not get_native_binance_client:
        raise RuntimeError("原生 Binance API 客户端不可用")
    info = get_native_binance_client().exchange_info()

    _exchange_info_cache = info
    _exchange_info_cache_time = now
    return info


def get_symbol_info(symbol: str) -> Optional[dict]:
    """获取指定交易对的精度信息（带缓存）"""
    info = get_exchange_info()
    if isinstance(info, dict) and "symbols" in info:
        for s in info["symbols"]:
            if s.get("symbol") == symbol:
                return s
    return None


def _env_flag(name: str, default: bool = False) -> bool:
    value = str(os.environ.get(name, "") or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def is_tradifi_perpetual_symbol(symbol: str) -> bool:
    sym_info = get_symbol_info(symbol)
    if not sym_info:
        return False
    contract_type = str(sym_info.get("contractType", "") or "").upper()
    if contract_type == "TRADIFI_PERPETUAL":
        return True
    for item in sym_info.get("underlyingSubType", []) or []:
        subtype = str(item or "").strip().upper()
        if subtype in {"TRADFI", "TRADIFI"}:
            return True
    return False


def validate_symbol_tradeable(symbol: str) -> tuple[bool, str]:
    allow_tradifi = _env_flag("HERMES_ALLOW_TRADIFI_PERPS") or _env_flag("BINANCE_ALLOW_TRADIFI_PERPS")
    if not allow_tradifi and is_tradifi_perpetual_symbol(symbol):
        return (
            False,
            (
                f"{symbol} requires the Binance TradFi-Perps agreement "
                "(contractType=TRADIFI_PERPETUAL); blocked before order submission"
            ),
        )
    return True, ""


def _truncate_to_step(value: float, step_size: float) -> float:
    """Truncate value to step size (Binance requires truncation, not rounding)."""
    from decimal import ROUND_FLOOR
    step = Decimal(str(step_size))
    val = Decimal(str(value))
    step_str = str(step)
    if '.' in step_str:
        decimals = len(step_str.split('.')[1].rstrip('0'))
    else:
        decimals = 0
    quantize_str = '0.' + '0' * decimals if decimals > 0 else '0'
    result = val.quantize(Decimal(quantize_str), rounding=ROUND_FLOOR)
    if result < step:
        result = step
    return float(result)


def _ceil_to_step(value: float, step_size: float) -> float:
    """Round value up to the next valid exchange step."""
    step = Decimal(str(step_size))
    val = Decimal(str(value))
    steps = (val / step).to_integral_value(rounding=ROUND_CEILING)
    result = steps * step
    return float(result)


def _get_lot_step_size(symbol: str) -> float:
    try:
        sym_info = get_symbol_info(symbol)
        if sym_info:
            for f in sym_info.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    return float(f.get("stepSize", "0.001"))
    except Exception:
        pass
    return 0.001


def _get_price_tick_size(symbol: str) -> float:
    try:
        sym_info = get_symbol_info(symbol)
        if sym_info:
            for f in sym_info.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    return float(f.get("tickSize", "0.00001"))
    except Exception:
        pass
    return 0.00001


def _is_precision_error(exc: Exception) -> bool:
    text = str(exc)
    return "Precision is over the maximum defined for this asset" in text or '"code":-1111' in text


def get_symbol_min_notional(symbol: str, default: float = 5.0) -> float:
    """Return the exchange minimum notional for a symbol."""
    try:
        sym_info = get_symbol_info(symbol)
        if sym_info:
            for f in sym_info.get("filters", []):
                if f.get("filterType") in {"MIN_NOTIONAL", "NOTIONAL"}:
                    return float(f.get("notional", f.get("minNotional", default)) or default)
    except Exception as e:
        logger.debug(f"获取 {symbol} 最小名义价值失败: {e}")
    return default


def calculate_min_quantity_for_notional(symbol: str, price: float, min_notional: float | None = None, buffer_pct: float = 8.0) -> float:
    """Calculate a step-aligned quantity that remains above min notional after truncation."""
    if price <= 0:
        return 0.0
    minimum = min_notional if min_notional is not None else get_symbol_min_notional(symbol)
    target_notional = minimum * (1 + buffer_pct / 100.0)
    step_size = _get_lot_step_size(symbol)
    return _ceil_to_step(target_notional / price, step_size)


def adjust_quantity_precision(symbol: str, quantity: float, price: float = 0.0) -> float:
    """Adjust quantity precision based on Binance symbol stepSize.

    Most USDT-M futures use stepSize of 0.001 or 0.01.
    Falls back to 3 decimal places if exchange info is unavailable.
    
    新增：名义价值二次校验，确保调整后 quantity 仍满足交易所最小名义价值要求。
    """
    original_quantity = quantity
    
    try:
        sym_info = get_symbol_info(symbol)
        if sym_info:
            for f in sym_info.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    step_size = float(f.get("stepSize", "0.001"))
                    quantity = _truncate_to_step(quantity, step_size)
                    logger.info(f"🔧 {symbol} 精度适配：stepSize={step_size}, 调整后 quantity={quantity}")
                    
                    # 名义价值二次校验
                    if price > 0:
                        notional_value = quantity * price
                        min_notional = 5.0  # 币安默认最小名义价值 5 USDT
                        
                        # 检查 NOTIONAL 过滤器
                        for nf in sym_info.get("filters", []):
                            if nf.get("filterType") == "NOTIONAL":
                                min_notional = float(nf.get("minNotional", 5.0))
                                break
                        
                        if notional_value < min_notional:
                            # 向上调整到满足最小名义价值
                            adjusted_quantity = _ceil_to_step(min_notional / price, step_size)
                            logger.warning(f"⚠️ {symbol} 名义价值 ${notional_value:.2f} < 最小 ${min_notional:.2f}，向上调整至 {adjusted_quantity}")
                            quantity = adjusted_quantity
                    
                    return quantity
    except Exception as e:
        logger.debug(f"获取 {symbol} 精度信息失败，使用默认 3 位小数: {e}")

    # 默认截断到 3 位小数（适用于大多数 USDT 合约）
    quantity = _truncate_to_step(quantity, 0.001)
    
    # 默认名义价值校验（保守估计）
    if price > 0:
        notional_value = quantity * price
        if notional_value < 5.0:
            step_size = 0.001
            quantity = _ceil_to_step(5.0 / price, step_size)
            logger.warning(f"⚠️ {symbol} 默认名义价值校验：${notional_value:.2f} < $5.0，调整至 {quantity}")
    
    return quantity


def adjust_price_precision(symbol: str, price: float) -> float:
    """Adjust price precision based on Binance symbol tickSize."""
    try:
        sym_info = get_symbol_info(symbol)
        if sym_info:
            for f in sym_info.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", "0.00001"))
                    price = _truncate_to_step(price, tick_size)
                    logger.info(f"🔧 {symbol} 价格精度：tickSize={tick_size}, 调整后 price={price}")
                    return price
    except Exception as e:
        logger.debug(f"获取 {symbol} 价格精度失败: {e}")
    # 默认 5 位小数
    return _truncate_to_step(price, 0.00001)


def get_account_balance() -> dict[str, Any]:
    """Fetch futures account information."""
    if not is_native_binance_configured() or not get_native_binance_client:
        raise RuntimeError("原生 Binance API 未配置，无法查询账户")
    return get_native_binance_client().account_information()  # type: ignore


def calculate_position_size(
    account_balance: float,
    risk_per_trade_pct: float,
    entry_price: float,
    stop_loss_price: float,
    max_position_pct: float = 20.0,
    min_notional: float = 5.5,
) -> float:
    """Calculate position size based on risk parameters.

    Args:
        account_balance: Total account balance in USDT
        risk_per_trade_pct: Risk per trade as percentage (e.g., 1.0 = 1%)
        entry_price: Entry price of the trade
        stop_loss_price: Stop loss price
        max_position_pct: Maximum position size as % of account (default 20%)
        min_notional: Minimum target notional value with safety buffer

    Returns:
        Quantity of the asset to buy/sell
    """
    risk_amount = account_balance * (risk_per_trade_pct / 100.0)
    stop_loss_pct = abs(entry_price - stop_loss_price) / entry_price * 100

    # 修复：浮点数精度问题，使用阈值判断代替 == 0
    if stop_loss_pct < 0.01:
        return 0.0

    # Position size = risk_amount / stop_loss_percentage
    position_value = risk_amount / (stop_loss_pct / 100.0)
    
    # 确保最小名义价值（Binance 要求至少 5 USDT）
    position_value = max(position_value, min_notional)

    # Cap at max position
    max_position_value = account_balance * (max_position_pct / 100.0)
    position_value = min(position_value, max_position_value)

    quantity = position_value / entry_price
    return round(quantity, 3)


def calculate_stop_loss(entry_price: float, stop_loss_pct: float, side: str) -> float:
    """Calculate stop loss price.

    Args:
        entry_price: Entry price
        stop_loss_pct: Stop loss percentage
        side: 'LONG' or 'SHORT'

    Returns:
        Stop loss price
    """
    if side == "LONG":
        return round(entry_price * (1 - stop_loss_pct / 100.0), 2)
    else:  # SHORT
        return round(entry_price * (1 + stop_loss_pct / 100.0), 2)


def calculate_take_profit(
    entry_price: float,
    take_profit_pcts: list[float],
    side: str,
    symbol: Optional[str] = None,
) -> list[float]:
    """Calculate take profit levels.

    Args:
        entry_price: Entry price
        take_profit_pcts: List of take profit percentages
        side: 'LONG' or 'SHORT'

    Returns:
        List of take profit prices
    """
    levels = []
    for tp_pct in take_profit_pcts:
        if side == "LONG":
            price = entry_price * (1 + tp_pct / 100.0)
        else:  # SHORT
            price = entry_price * (1 - tp_pct / 100.0)
        if symbol:
            price = adjust_price_precision(symbol, price)
        levels.append(price)
    return levels


def calculate_effective_roi_pcts(entry_price: float, target_prices: list[float], leverage: int, side: str) -> list[float]:
    """Calculate leveraged ROI percentages for display from actual target prices."""
    if entry_price <= 0:
        return [0.0 for _ in target_prices]

    roi_pcts: list[float] = []
    for price in target_prices:
        if side == "LONG":
            price_move_pct = (price - entry_price) / entry_price * 100.0
        else:
            price_move_pct = (entry_price - price) / entry_price * 100.0
        roi_pcts.append(price_move_pct * leverage)
    return roi_pcts


def calculate_take_profit_prices_by_roi(
    entry_price: float,
    target_roi_pcts: list[float],
    leverage: int,
    side: str,
    symbol: Optional[str] = None,
) -> list[float]:
    """Convert leveraged target ROI percentages into actual take-profit prices."""
    if leverage <= 0:
        raise ValueError("Leverage must be greater than zero")

    levels: list[float] = []
    for roi_pct in target_roi_pcts:
        price_move_pct = roi_pct / float(leverage)
        if side == "LONG":
            price = entry_price * (1 + price_move_pct / 100.0)
        else:
            price = entry_price * (1 - price_move_pct / 100.0)
        if symbol:
            price = adjust_price_precision(symbol, price)
        levels.append(price)
    return levels


def _normalize_take_profit_ratios(level_count: int, take_profit_ratios: Optional[list[float]]) -> list[float]:
    """Normalize take-profit ratios to match the number of target levels."""
    if level_count <= 0:
        return []

    if not take_profit_ratios:
        return [1.0 / level_count] * level_count

    ratios = [max(float(r), 0.0) for r in take_profit_ratios[:level_count]]
    if len(ratios) < level_count:
        remaining = max(1.0 - sum(ratios), 0.0)
        missing = level_count - len(ratios)
        fill = remaining / missing if missing else 0.0
        ratios.extend([fill] * missing)

    total = sum(ratios)
    if total <= 0:
        return [1.0 / level_count] * level_count

    return [ratio / total for ratio in ratios]


def _build_take_profit_slices(symbol: str, quantity: float, take_profit_ratios: list[float]) -> list[float]:
    """Split the entry quantity into exchange-compatible partial take-profit slices."""
    if not take_profit_ratios:
        return []

    quantities: list[float] = []
    remaining = quantity

    for index, ratio in enumerate(take_profit_ratios):
        if index == len(take_profit_ratios) - 1:
            slice_qty = remaining
        else:
            slice_qty = quantity * ratio
        adjusted_qty = adjust_quantity_precision(symbol, slice_qty)
        remaining = max(remaining - adjusted_qty, 0.0)
        quantities.append(adjusted_qty)

    if quantities:
        quantities[-1] = adjust_quantity_precision(symbol, max(quantity - sum(quantities[:-1]), 0.0))

    return [qty for qty in quantities if qty > 0]


def signal_to_order_params(signal: TradingSignal, side: str) -> dict[str, Any]:
    """Convert trading signal to order parameters.

    Args:
        signal: Trading signal
        side: 'LONG' or 'SHORT' (maps to BUY/SELL for futures)

    Returns:
        Order parameters dict for market execution
    """
    binance_side = "BUY" if side == "LONG" else "SELL"

    return {
        "symbol": signal.symbol,
        "side": binance_side,
        "type": "MARKET",
        "quantity": 0,  # To be filled by position sizing
    }


def should_trade(signal: TradingSignal) -> bool:
    """Determine if a signal should be traded.

    支持双向交易：
    - LONG/SHORT: 可交易
    - CONSIDER_LONG/CONSIDER_SHORT: 可交易（反向信号）
    - 其他：不交易
    
    放宽条件：接受所有有明确方向的信号
    """
    # Don't trade neutral signals
    if signal.stage == "neutral":
        return False
    
    # Don't trade unclear directions
    if signal.direction in {"NO_TRADE", "WATCH", "RISK_OFF"}:
        return False
    
    # Trade all stages with clear direction (mania and exhaustion can trade too)
    if signal.direction in {"LONG", "SHORT", "CONSIDER_LONG", "CONSIDER_SHORT"}:
        return True
    
    return False


def place_market_order(
    symbol: str,
    side: str,
    quantity: float,
    leverage: int = 5,
    position_side: Optional[str] = None,
    reduce_only: bool = False,
) -> OrderResult:
    """Place a market order with leverage.

    Args:
        symbol: Trading symbol
        side: 'BUY' or 'SELL'
        quantity: Quantity to trade
        leverage: Leverage (default 5x)

    Returns:
        OrderResult with execution details
    """
    try:
        # 获取当前价格用于名义价值校验和滑点保护
        current_price = 0.0
        try:
            ticker = get_ticker_24hr(symbol)
            if isinstance(ticker, dict):
                current_price = float(ticker.get("lastPrice", 0))
        except Exception as e:
            logger.debug(f"获取 {symbol} 价格失败，跳过校验: {e}")
        
        # P3-1: 交易所健康检查
        if not check_exchange_health():
            return OrderResult(
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=0,
                order_id=0,
                status="REJECTED",
                message="交易所维护中，暂停交易",
            )
        
        # 调整 quantity 精度，防止 LOT_SIZE 过滤失败
        tradeable, reject_reason = validate_symbol_tradeable(symbol)
        if not tradeable:
            return OrderResult(
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=0,
                order_id=0,
                status="REJECTED",
                message=reject_reason,
            )

        quantity = adjust_quantity_precision(symbol, quantity, current_price)

        if not is_native_binance_configured() or not get_native_binance_client:
            raise RuntimeError("原生 Binance API 未配置，无法下单")

        resolved_position_side = position_side or ("LONG" if side == "BUY" else "SHORT")
        applied_leverage = int(leverage)
        if not reduce_only:
            applied_leverage = _ensure_symbol_leverage(symbol, int(leverage))

        result = get_native_binance_client().new_order(  # type: ignore
            symbol=symbol,
            side=side,
            order_type="MARKET",
            quantity=quantity,
            position_side=resolved_position_side,
            reduce_only=reduce_only,
            new_order_resp_type="RESULT",
        )

        # Parse result
        executed_qty = float(result.get("executedQty", 0))
        executed_price = float(result.get("avgPrice", 0)) or float(result.get("price", 0))
        order_id = int(result.get("orderId", result.get("orderID", 0)))
        status = result.get("status", "UNKNOWN")

        # P3-2: 滑点保护。订单已经成交时，滑点只能作为风险提示，
        # 不能覆盖交易所返回的 FILLED 状态，否则后续保护单流程会被误判为开仓失败。
        high_slippage = False
        if current_price > 0 and executed_price > 0:
            if not check_slippage(current_price, executed_price):
                logger.warning(f"⚠️ {symbol} 滑点过大，已成交但需关注")
                high_slippage = True

        message = f"Leverage: {applied_leverage}x"
        if high_slippage:
            message += " | HIGH_SLIPPAGE"

        return OrderResult(
            symbol=symbol,
            side=side,
            quantity=executed_qty,
            executed_price=executed_price,
            order_id=order_id,
            status=status,
            message=message,
        )
    except Exception as e:
        return OrderResult(
            symbol=symbol,
            side=side,
            quantity=quantity,
            executed_price=0,
            order_id=0,
            status="ERROR",
            message=str(e),
        )


def place_stop_loss_order(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
    position_side: Optional[str] = None,
    reduce_only: bool = True,
    trigger_buffer_pct: float = 0.0,
) -> OrderResult:
    """Place a stop-loss order."""
    resolved_position_side = position_side or ("LONG" if side == "SELL" else "SHORT")
    try:
        if not is_native_binance_configured() or not get_native_binance_client:
            raise RuntimeError("Native Binance API is not configured; cannot place stop loss")

        quantity = adjust_quantity_precision(symbol, quantity)
        stop_price = adjust_price_precision(symbol, stop_price)
        trigger_price = stop_price
        if trigger_buffer_pct > 0:
            if side == "SELL":
                trigger_price = stop_price * (1 - trigger_buffer_pct / 100.0)
            else:
                trigger_price = stop_price * (1 + trigger_buffer_pct / 100.0)
            trigger_price = adjust_price_precision(symbol, trigger_price)

        result = get_native_binance_client().new_algo_order(  # type: ignore
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
            symbol=symbol,
            side=side,
            quantity=executed_qty,
            executed_price=trigger_price,
            order_id=order_id,
            status=status,
            message=f"Stop loss order placed | logical={stop_price:.8f} trigger={trigger_price:.8f}",
        )
    except Exception as e:
        if _is_precision_error(e):
            try:
                step = _get_lot_step_size(symbol)
                tick = _get_price_tick_size(symbol)
                quantity_retry = _truncate_to_step(max(quantity - step, step), step)
                trigger_retry = _truncate_to_step(trigger_price, tick)
                result = get_native_binance_client().new_algo_order(  # type: ignore
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
                logger.warning(
                    f"?? {symbol} stop-loss precision retry success: qty {quantity} -> {quantity_retry}, "
                    f"trigger {trigger_price} -> {trigger_retry}"
                )
                return OrderResult(
                    symbol=symbol,
                    side=side,
                    quantity=executed_qty or quantity_retry,
                    executed_price=trigger_retry,
                    order_id=order_id,
                    status=status,
                    message=f"Stop loss order placed after precision retry | logical={stop_price:.8f} trigger={trigger_retry:.8f}",
                )
            except Exception as retry_error:
                e = retry_error
        return OrderResult(
            symbol=symbol,
            side=side,
            quantity=quantity,
            executed_price=stop_price,
            order_id=0,
            status="ERROR",
            message=str(e),
        )


def place_take_profit_order(
    symbol: str,
    side: str,
    quantity: float,
    trigger_price: float,
    position_side: Optional[str] = None,
    reduce_only: bool = True,
) -> OrderResult:
    """Place a TAKE_PROFIT_MARKET order."""
    resolved_position_side = position_side or ("LONG" if side == "SELL" else "SHORT")
    try:
        if not is_native_binance_configured() or not get_native_binance_client:
            raise RuntimeError("Native Binance API is not configured; cannot place take profit")

        quantity = adjust_quantity_precision(symbol, quantity)
        trigger_price = adjust_price_precision(symbol, trigger_price)

        result = get_native_binance_client().new_algo_order(  # type: ignore
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
            symbol=symbol,
            side=side,
            quantity=executed_qty or quantity,
            executed_price=trigger_price,
            order_id=order_id,
            status=status,
            message="Take profit order placed",
        )
    except Exception as e:
        if _is_precision_error(e):
            try:
                step = _get_lot_step_size(symbol)
                tick = _get_price_tick_size(symbol)
                quantity_retry = _truncate_to_step(max(quantity - step, step), step)
                trigger_retry = _truncate_to_step(trigger_price, tick)
                result = get_native_binance_client().new_algo_order(  # type: ignore
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
                logger.warning(
                    f"?? {symbol} take-profit precision retry success: qty {quantity} -> {quantity_retry}, "
                    f"trigger {trigger_price} -> {trigger_retry}"
                )
                return OrderResult(
                    symbol=symbol,
                    side=side,
                    quantity=executed_qty or quantity_retry,
                    executed_price=trigger_retry,
                    order_id=order_id,
                    status=status,
                    message="Take profit order placed after precision retry",
                )
            except Exception as retry_error:
                e = retry_error
        return OrderResult(
            symbol=symbol,
            side=side,
            quantity=quantity,
            executed_price=trigger_price,
            order_id=0,
            status="ERROR",
            message=str(e),
        )


def cancel_protective_order(symbol: str, order_id: int) -> bool:
    """Best-effort cancellation for stop-loss / take-profit protective orders."""
    if not order_id:
        return False

    if not is_native_binance_configured() or not get_native_binance_client:
        raise RuntimeError("Native Binance API is not configured; cannot cancel order")

    try:
        get_native_binance_client().cancel_algo_order(symbol, order_id)  # type: ignore
        return True
    except Exception as algo_error:
        algo_text = str(algo_error)
        if any(token in algo_text for token in ("Unknown order", "-2011", "Order does not exist")):
            logger.info(f"{symbol} protective order already gone id={order_id}: {algo_error}")
            return True
        try:
            get_native_binance_client().cancel_order(symbol, order_id)  # type: ignore
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

def cancel_stop_loss_order(symbol: str, order_id: int) -> bool:
    """Backward-compatible stop-loss cancellation wrapper."""
    return cancel_protective_order(symbol, order_id)


def fetch_open_orders(symbol: Optional[str] = None) -> list[dict[str, Any]]:
    """Best-effort fetch for normal open orders."""
    if not is_native_binance_configured() or not get_native_binance_client:
        return []
    return get_native_binance_client().open_orders(symbol)  # type: ignore


def fetch_open_algo_orders(symbol: Optional[str] = None) -> list[dict[str, Any]]:
    """Best-effort fetch for algo open orders such as STOP_MARKET / TAKE_PROFIT_MARKET."""
    # Native conditional orders created through /fapi/v1/order are returned by
    # openOrders, so merge both sources for compatibility with older metadata.
    orders = fetch_open_orders(symbol)
    if is_native_binance_configured() and get_native_binance_client:
        try:
            orders.extend(get_native_binance_client().open_algo_orders(symbol))  # type: ignore
        except Exception:
            pass
    return orders


def execute_trade(
    signal: TradingSignal,
    account_balance: float,
    risk_per_trade_pct: float = 1.5,
    stop_loss_pct: float = 7.0,
    max_position_pct: float = 35.0,
    leverage: int = 5,
    quantity: float = None,  # 可选：使用外部计算的仓位
    stop_loss_price: float = None,  # 可选：使用外部计算的止损
    take_profit_roi_pcts: Optional[list[float]] = None,
    take_profit_price_pcts: Optional[list[float]] = None,
    take_profit_ratios: Optional[list[float]] = None,
    take_profit_mode: str = "roi",
    stop_trigger_buffer_pct: float = 0.0,
    defer_protection_orders: bool = False,
) -> dict[str, Any]:
    """Execute a trade based on a signal.

    Args:
        signal: Trading signal from scanner
        account_balance: Account balance in USDT
        risk_per_trade_pct: Risk per trade percentage.
        stop_loss_pct: Stop loss percentage.
        max_position_pct: Maximum position size.
        leverage: Leverage (default 5x)
        quantity: Optional pre-calculated quantity (overrides internal calculation)
        stop_loss_price: Optional pre-calculated stop loss price (overrides internal calculation)

    Returns:
        Trade execution result dict
    """
    if not should_trade(signal):
        return {
            "symbol": signal.symbol,
            "action": "SKIPPED",
            "reason": f"Signal does not meet trading criteria (stage={signal.stage}, direction={signal.direction})",
        }

    # Determine order side
    side = "BUY" if signal.direction == "LONG" else "SELL"
    opposite_side = "SELL" if signal.direction == "LONG" else "BUY"

    # Calculate position size (use external values if provided)
    if stop_loss_price is None:
        stop_loss_price = calculate_stop_loss(signal.entry_price, stop_loss_pct, signal.direction)
    
    if quantity is None:
        quantity = calculate_position_size(
            account_balance=account_balance,
            risk_per_trade_pct=risk_per_trade_pct,
            entry_price=signal.entry_price,
            stop_loss_price=stop_loss_price,
            max_position_pct=max_position_pct,
        )

    if quantity <= 0:
        return {
            "symbol": signal.symbol,
            "action": "SKIPPED",
            "reason": "Calculated position size is zero or negative",
        }

    min_quantity = calculate_min_quantity_for_notional(signal.symbol, signal.entry_price)
    if min_quantity > quantity:
        old_quantity = quantity
        quantity = min_quantity
        logger.warning(
            f"⚖️ {signal.symbol} 仓位低于最小名义价值，数量从 {old_quantity} 上调到 {quantity} "
            f"(预估名义 ${quantity * signal.entry_price:.2f})"
        )

    # 打印下单参数，便于排查
    logger.info(f"📤 {signal.symbol} 准备下单: 方向={side}, 数量={quantity}, 杠杆={leverage}x, 止损=${stop_loss_price:.4f}")

    # Place market order
    entry_result = place_market_order(
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
        take_profit_prices = calculate_take_profit(
            actual_entry_price,
            target_pcts,
            signal.direction,
            symbol=signal.symbol,
        )
        price_move_pcts = list(target_pcts)
        effective_roi_pcts = calculate_effective_roi_pcts(
            actual_entry_price,
            take_profit_prices,
            leverage,
            signal.direction,
        )
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
                symbol=signal.symbol,
                side=opposite_side,
                quantity=tp_quantity,
                executed_price=trigger_price,
                order_id=0,
                status="DEFERRED",
                message="Take profit deferred to runtime protection step",
            )
        else:
            tp_result = place_take_profit_order(
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
            symbol=signal.symbol,
            side=opposite_side,
            quantity=actual_quantity,
            executed_price=stop_loss_price,
            order_id=0,
            status="DEFERRED",
            message="Stop loss deferred to runtime protection step",
        )
    else:
        # Place stop loss order
        sl_result = place_stop_loss_order(
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
        "order_id": entry_result.order_id,  # 添加 order_id 字段
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


def main():
    """CLI entrypoint for testing trading executor."""
    import argparse

    parser = argparse.ArgumentParser(description="Binance trading executor")
    parser.add_argument("--symbol", "-s", required=True, help="Trading symbol")
    parser.add_argument("--direction", "-d", choices=["LONG", "SHORT"], required=True)
    parser.add_argument("--stage", default="confirmed_breakout")
    parser.add_argument("--entry-price", "-p", type=float, required=True)
    parser.add_argument("--balance", "-b", type=float, default=10000.0, help="Account balance (USDT)")
    parser.add_argument("--risk", "-r", type=float, default=2.0, help="Risk per trade (%)")
    parser.add_argument("--stop-loss", "-l", type=float, default=5.0, help="Stop loss (%)")
    parser.add_argument("--max-position", "-m", type=float, default=20.0, help="Max position (%)")
    args = parser.parse_args()

    signal = TradingSignal(
        symbol=args.symbol,
        stage=args.stage,
        direction=args.direction,
        entry_price=args.entry_price,
        metrics={},
    )

    result = execute_trade(
        signal=signal,
        account_balance=args.balance,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        max_position_pct=args.max_position,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
