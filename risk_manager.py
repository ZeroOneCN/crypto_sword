#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         🛡️ RISK MANAGER - 神圣风控系统 🛡️                    ║
║                                                               ║
║    ATR 动态止损 + 分级止盈 + 相关性风控 + 仓位管理            ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""

import logging
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from dataclasses import dataclass, field
import math

logger = logging.getLogger(__name__)

try:
    from binance_api_client import get_native_binance_client
except Exception:
    get_native_binance_client = None

# ═══════════════════════════════════════════════
# API 调用限流 - 避免 Binance 限流
# ═══════════════════════════════════════════════
_last_api_call_time = 0.0
_API_CALL_INTERVAL = 0.5  # 每次 API 调用间隔 0.5 秒

def _throttle_api_call():
    """限流：确保 API 调用间隔"""
    global _last_api_call_time
    now = time.time()
    elapsed = now - _last_api_call_time
    if elapsed < _API_CALL_INTERVAL:
        time.sleep(_API_CALL_INTERVAL - elapsed)
    _last_api_call_time = time.time()


# ═══════════════════════════════════════════════════════════════
# 数据结构 - 雅典娜的神盾
# ═══════════════════════════════════════════════════════════════

@dataclass
class RiskConfig:
    """风控配置"""
    # 基础风控
    risk_per_trade_pct: float = 1.0       # 每笔风险百分比
    base_stop_loss_pct: float = 8.0       # 基础止损百分比
    base_take_profit_pct: float = 20.0    # 基础止盈百分比
    
    # ATR 动态调整
    atr_multiplier: float = 2.0           # ATR 倍数
    atr_period: int = 14                  # ATR 周期
    
    # 分级止盈
    take_profit_levels: List[float] = field(default_factory=lambda: [20.0, 40.0, 60.0])
    take_profit_ratios: List[float] = field(default_factory=lambda: [0.5, 0.3, 0.2])
    
    # 仓位管理
    max_position_pct: float = 20.0        # 单笔最大仓位
    max_total_exposure: float = 50.0      # 总敞口上限
    max_correlation_exposure: float = 30.0  # 相关性资产总敞口
    
    # 相关性风控
    correlation_threshold: float = 0.7    # 相关性阈值
    max_correlated_positions: int = 3     # 最多持有几个高相关资产
    
    # 动态调整
    volatility_adjustment: bool = True    # 波动率调整
    max_daily_loss_pct: float = 5.0       # 每日最大亏损


@dataclass
class PositionRisk:
    """持仓风险评估"""
    symbol: str
    side: str  # LONG/SHORT
    entry_price: float
    current_price: float
    quantity: float
    leverage: int
    
    # 止损止盈
    stop_loss: float
    take_profit: float
    trailing_stop: Optional[float] = None
    
    # 风险指标
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    liquidation_price: Optional[float] = None
    risk_reward_ratio: float = 0.0
    
    # 风险评估
    risk_level: str = "低"  # 低 / 中 / 高 / 严重
    
    def __post_init__(self):
        # 计算未实现盈亏
        if self.side == "LONG":
            self.unrealized_pnl = (self.current_price - self.entry_price) * self.quantity
            self.unrealized_pnl_pct = (self.current_price - self.entry_price) / self.entry_price * 100
        else:
            self.unrealized_pnl = (self.entry_price - self.current_price) * self.quantity
            self.unrealized_pnl_pct = (self.entry_price - self.current_price) / self.entry_price * 100
        
        # 计算风险回报比
        risk = abs(self.entry_price - self.stop_loss)
        reward = abs(self.take_profit - self.entry_price)
        self.risk_reward_ratio = reward / risk if risk > 0 else 0
        
        # 评估风险等级
        self._assess_risk_level()
    
    def _assess_risk_level(self):
        """评估风险等级"""
        if self.unrealized_pnl_pct <= -5:
            self.risk_level = "严重"
        elif self.unrealized_pnl_pct <= -3:
            self.risk_level = "高"
        elif self.unrealized_pnl_pct <= -1:
            self.risk_level = "中"
        else:
            self.risk_level = "低"
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "unrealized_pnl_pct": round(self.unrealized_pnl_pct, 2),
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "risk_reward_ratio": round(self.risk_reward_ratio, 2),
            "risk_level": self.risk_level,
        }


# ═══════════════════════════════════════════════════════════════
# Binance CLI 封装
# ═══════════════════════════════════════════════════════════════

def run_binance_cli(args: List[str], timeout: int = 60, max_retries: int = 5) -> Optional[Any]:
    """Compatibility wrapper backed by native Binance REST.
    
    Added retry logic and empty response handling to prevent JSON parse errors.
    Increased retries to 5 with exponential backoff for rate limit handling.
    Added API call throttling to avoid rate limiting.
    """
    if get_native_binance_client is None:
        logger.error("原生 Binance API 客户端不可用")
        return None

    for attempt in range(max_retries + 1):
        try:
            _throttle_api_call()
            return get_native_binance_client().command_compat(list(args))  # type: ignore
        except Exception as e:
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"原生 Binance API 异常：{e}")
            return None

    return None


# ═══════════════════════════════════════════════════════════════
# ATR 计算 - 赫菲斯托斯的熔炉
# ═══════════════════════════════════════════════════════════════

def get_klines(symbol: str, interval: str = "1h", limit: int = 50) -> Optional[List[Dict]]:
    """获取 K 线数据"""
    data = run_binance_cli([
        "kline-candlestick-data",
        "--symbol", symbol,
        "--interval", interval,
        "--limit", str(limit)
    ])
    
    if not data:
        return None
    
    klines = []
    for k in data:
        klines.append({
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
        })
    
    return klines


def calculate_atr(klines: List[Dict], period: int = 14) -> float:
    """
    计算 ATR (Average True Range)
    
    True Range = max(high - low, |high - prev_close|, |low - prev_close|)
    ATR = MA(True Range, period)
    """
    if len(klines) < period + 1:
        return 0.0
    
    true_ranges = []
    
    for i in range(1, len(klines)):
        high = klines[i]["high"]
        low = klines[i]["low"]
        prev_close = klines[i-1]["close"]
        
        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)
        
        true_range = max(tr1, tr2, tr3)
        true_ranges.append(true_range)
    
    # 简单移动平均
    atr = sum(true_ranges[-period:]) / period
    
    return atr


def calculate_atr_stop_loss(
    symbol: str,
    entry_price: float,
    side: str,
    atr_multiplier: float = 2.0,
    atr_period: int = 14,
) -> Dict[str, Any]:
    """
    计算 ATR 动态止损
    
    Returns:
        {
            "atr": float,
            "stop_loss": float,
            "stop_loss_pct": float,
            "is_wider": bool,  # 是否比固定止损宽
        }
    """
    klines = get_klines(symbol, interval="1h", limit=50)
    
    if not klines:
        #  fallback 到固定止损
        if side == "LONG":
            stop_loss = entry_price * (1 - 0.08)
        else:
            stop_loss = entry_price * (1 + 0.08)
        
        return {
            "atr": 0,
            "stop_loss": round(stop_loss, 4),
            "stop_loss_pct": 8.0,
            "method": "FALLBACK",
        }
    
    atr = calculate_atr(klines, period=atr_period)
    current_price = klines[-1]["close"]
    
    # ATR 止损（使用 entry_price 而非 current_price，避免计算错误）
    # 降低 multiplier 到 1.5，妖币波动大但止损不能太宽
    atr_multiplier = min(atr_multiplier, 1.5)  # 最大 1.5x ATR
    atr_distance = atr * atr_multiplier
    
    if side == "LONG":
        stop_loss = entry_price - atr_distance
        stop_loss_pct = (entry_price - stop_loss) / entry_price * 100
    else:
        stop_loss = entry_price + atr_distance
        stop_loss_pct = (stop_loss - entry_price) / entry_price * 100
    
    # 确保止损在合理范围：3% - 12%
    min_stop_pct = 3.0
    max_stop_pct = 12.0
    if stop_loss_pct < min_stop_pct:
        if side == "LONG":
            stop_loss = entry_price * (1 - min_stop_pct / 100)
        else:
            stop_loss = entry_price * (1 + min_stop_pct / 100)
        stop_loss_pct = min_stop_pct
    elif stop_loss_pct > max_stop_pct:
        if side == "LONG":
            stop_loss = entry_price * (1 - max_stop_pct / 100)
        else:
            stop_loss = entry_price * (1 + max_stop_pct / 100)
        stop_loss_pct = max_stop_pct
    
    return {
        "atr": round(atr, 4),
        "stop_loss": round(stop_loss, 4),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "method": "ATR",
        "is_wider": stop_loss_pct > 8.0,
    }


# ═══════════════════════════════════════════════════════════════
# 分级止盈 - 宙斯的雷霆
# ═══════════════════════════════════════════════════════════════

def calculate_take_profit_levels(
    entry_price: float,
    side: str,
    levels: List[float] = None,
    ratios: List[float] = None,
) -> List[Dict[str, Any]]:
    """
    计算分级止盈
    
    Args:
        entry_price: 入场价
        side: LONG/SHORT
        levels: 止盈目标 [%]，默认 [20, 40, 60]
        ratios: 每级平仓比例，默认 [0.5, 0.3, 0.2]
    
    Returns:
        [
            {"level": 1, "price": float, "ratio": 0.5, "quantity_pct": 50},
            {"level": 2, "price": float, "ratio": 0.3, "quantity_pct": 30},
            {"level": 3, "price": float, "ratio": 0.2, "quantity_pct": 20},
        ]
    """
    if levels is None:
        levels = [20.0, 40.0, 60.0]
    if ratios is None:
        ratios = [0.5, 0.3, 0.2]
    
    tp_levels = []
    
    for i, (level, ratio) in enumerate(zip(levels, ratios)):
        if side == "LONG":
            tp_price = entry_price * (1 + level / 100)
        else:
            tp_price = entry_price * (1 - level / 100)
        
        tp_levels.append({
            "level": i + 1,
            "target_pct": level,
            "price": round(tp_price, 4),
            "ratio": ratio,
            "quantity_pct": round(ratio * 100, 1),
        })
    
    return tp_levels


def calculate_trailing_stop(
    entry_price: float,
    current_price: float,
    side: str,
    trailing_pct: float = 5.0,
) -> Dict[str, Any]:
    """
    计算追踪止损
    
    Returns:
        {
            "trailing_stop": float,
            "locked_profit": float,
            "locked_profit_pct": float,
        }
    """
    if side == "LONG":
        # 最高价回撤 trailing_pct
        if current_price > entry_price:
            trailing_stop = current_price * (1 - trailing_pct / 100)
            locked_profit = trailing_stop - entry_price
            locked_profit_pct = locked_profit / entry_price * 100
        else:
            # 还未盈利，使用初始止损
            trailing_stop = entry_price * (1 - trailing_pct / 100)
            locked_profit = 0
            locked_profit_pct = 0
    else:
        # 最低价反弹 trailing_pct
        if current_price < entry_price:
            trailing_stop = current_price * (1 + trailing_pct / 100)
            locked_profit = entry_price - trailing_stop
            locked_profit_pct = locked_profit / entry_price * 100
        else:
            trailing_stop = entry_price * (1 + trailing_pct / 100)
            locked_profit = 0
            locked_profit_pct = 0
    
    return {
        "trailing_stop": round(trailing_stop, 4),
        "locked_profit": round(locked_profit, 4),
        "locked_profit_pct": round(locked_profit_pct, 2),
    }


# ═══════════════════════════════════════════════════════════════
# 相关性分析 - 阿波罗的金车
# ═══════════════════════════════════════════════════════════════

# 币种相关性分组（简化版）
CORRELATION_GROUPS = {
    "L1": ["BTC", "ETH"],
    "L2": ["SOL", "BNB", "AVAX", "MATIC"],
    "MEME": ["DOGE", "SHIB", "PEPE", "FLOKI"],
    "DEFI": ["UNI", "AAVE", "MKR", "CRV"],
    "GAMING": ["AXS", "SAND", "MANA", "GALA"],
    "AI": ["FET", "AGIX", "OCEAN", "RNDR"],
}


def get_correlation_group(symbol: str) -> str:
    """获取币种所属相关性分组"""
    base = symbol.replace("USDT", "").replace("PERP", "")
    
    for group, members in CORRELATION_GROUPS.items():
        if base in members:
            return group
    
    # BTC 相关
    if base in ["BTC", "ETH"]:
        return "L1"
    
    # 其他 L1
    if any(l1 in base for l1 in ["SOL", "BNB", "AVAX", "MATIC", "ADA", "DOT"]):
        return "L2"
    
    # Meme
    if any(meme in base for meme in ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK"]):
        return "MEME"
    
    return "OTHER"


def check_correlation_risk(
    symbol: str,
    existing_positions: List[Dict[str, str]],
    max_correlated: int = 3,
) -> Dict[str, Any]:
    """
    检查相关性风险
    
    Args:
        symbol: 拟开仓币种
        existing_positions: 现有持仓 [{"symbol": "BTCUSDT", "side": "LONG"}, ...]
        max_correlated: 最多持有几个高相关资产
    
    Returns:
        {
            "can_open": bool,
            "correlated_count": int,
            "correlated_positions": list,
            "risk_level": str,
        }
    """
    target_group = get_correlation_group(symbol)
    
    correlated = []
    for pos in existing_positions:
        pos_group = get_correlation_group(pos["symbol"])
        if pos_group == target_group:
            correlated.append(pos)
    
    can_open = len(correlated) < max_correlated
    
    if len(correlated) >= max_correlated:
        risk_level = "高"
    elif len(correlated) >= max_correlated - 1:
        risk_level = "中"
    else:
        risk_level = "低"
    
    return {
        "can_open": can_open,
        "correlated_count": len(correlated),
        "correlated_positions": correlated,
        "group": target_group,
        "risk_level": risk_level,
        "message": f"同一板块已持有 {len(correlated)}/{max_correlated} 个仓位" if correlated else "无相关性风险",
    }


# ═══════════════════════════════════════════════════════════════
# 仓位管理 - 赫斯提亚的圣火
# ═══════════════════════════════════════════════════════════════

def calculate_position_size(
    account_balance: float,
    risk_per_trade_pct: float,
    entry_price: float,
    stop_loss_price: float,
    max_position_pct: float = 20.0,
    leverage: int = 5,
    min_notional: float = 5.5,
) -> Dict[str, Any]:
    """
    计算仓位大小
    
    Returns:
        {
            "quantity": float,
            "position_value": float,
            "risk_amount": float,
            "position_pct": float,
            "is_capped": bool,
        }
    """
    # 基于风险计算仓位
    risk_amount = account_balance * (risk_per_trade_pct / 100)
    stop_distance_pct = abs(entry_price - stop_loss_price) / entry_price * 100
    
    if stop_distance_pct == 0:
        return {
            "quantity": 0,
            "position_value": 0,
            "risk_amount": 0,
            "position_pct": 0,
            "is_capped": False,
            "error": "止损距离为 0",
        }
    
    # 仓位价值 = 风险金额 / 止损百分比
    position_value = risk_amount / (stop_distance_pct / 100)
    
    # 先确保满足交易所最小名义价值
    position_value = max(position_value, min_notional)

    # 应用最大仓位限制
    max_position_value = account_balance * (max_position_pct / 100)
    is_capped = position_value > max_position_value

    if is_capped:
        position_value = max_position_value

    # 计算数量（修复：杠杆不应该放大仓位价值，而是减少所需保证金）
    # position_value 已经是基于风险计算出的名义价值，不需要再乘以杠杆
    quantity = position_value / entry_price
    
    # 实际风险百分比
    actual_risk_pct = (position_value * stop_distance_pct / 100) / account_balance * 100
    
    return {
        "quantity": round(quantity, 4),
        "position_value": round(position_value, 2),
        "risk_amount": round(risk_amount, 2),
        "position_pct": round(position_value / account_balance * 100, 2),
        "actual_risk_pct": round(actual_risk_pct, 2),
        "is_capped": is_capped,
        "leverage": leverage,
    }


def check_total_exposure(
    account_balance: float,
    existing_positions: List[Dict],
    new_position_value: float,
    max_exposure_pct: float = 50.0,
) -> Dict[str, Any]:
    """
    检查总敞口风险
    
    Returns:
        {
            "can_open": bool,
            "current_exposure": float,
            "new_exposure": float,
            "remaining_capacity": float,
        }
    """
    current_exposure = sum(pos.get("position_value", 0) for pos in existing_positions)
    new_exposure = current_exposure + new_position_value
    
    max_exposure = account_balance * (max_exposure_pct / 100)
    
    can_open = new_exposure <= max_exposure
    remaining_capacity = max_exposure - current_exposure
    
    return {
        "can_open": can_open,
        "current_exposure": round(current_exposure, 2),
        "current_exposure_pct": round(current_exposure / account_balance * 100, 2),
        "new_exposure": round(new_exposure, 2),
        "new_exposure_pct": round(new_exposure / account_balance * 100, 2),
        "remaining_capacity": round(remaining_capacity, 2),
        "remaining_capacity_pct": round(remaining_capacity / account_balance * 100, 2),
    }


# ═══════════════════════════════════════════════════════════════
# 综合风控评估 - 众神的裁决
# ═══════════════════════════════════════════════════════════════

def assess_trade_risk(
    symbol: str,
    side: str,
    entry_price: float,
    account_balance: float,
    existing_positions: List[Dict],
    config: RiskConfig = None,
) -> Dict[str, Any]:
    """
    综合评估交易风险
    
    Returns:
        {
            "can_open": bool,
            "risk_score": 0-100,
            "position_size": dict,
            "stop_loss": dict,
            "take_profit": list,
            "correlation": dict,
            "exposure": dict,
            "warnings": list,
        }
    """
    if config is None:
        config = RiskConfig()
    
    warnings = []
    risk_score = 0  # 越低越好
    
    # 1. ATR 动态止损
    atr_result = calculate_atr_stop_loss(
        symbol, entry_price, side,
        atr_multiplier=config.atr_multiplier,
        atr_period=config.atr_period,
    )
    stop_loss = atr_result["stop_loss"]
    
    # 2. 计算仓位
    position_result = calculate_position_size(
        account_balance,
        config.risk_per_trade_pct,
        entry_price,
        stop_loss,
        max_position_pct=config.max_position_pct,
    )
    
    if position_result["quantity"] <= 0:
        return {
            "can_open": False,
            "risk_score": 100,
            "error": "仓位计算失败",
        }
    
    # 3. 相关性检查
    correlation_result = check_correlation_risk(
        symbol,
        existing_positions,
        max_correlated=config.max_correlated_positions,
    )
    
    if not correlation_result["can_open"]:
        warnings.append(f"相关性风险：{correlation_result['message']}")
        risk_score += 30
    
    # 4. 总敞口检查
    exposure_result = check_total_exposure(
        account_balance,
        existing_positions,
        position_result["position_value"],
        max_exposure_pct=config.max_total_exposure,
    )
    
    if not exposure_result["can_open"]:
        warnings.append(f"总敞口超限：{exposure_result['new_exposure_pct']:.1f}% > {config.max_total_exposure}%")
        risk_score += 40
    
    # 5. 分级止盈
    take_profit_levels = calculate_take_profit_levels(
        entry_price, side,
        levels=config.take_profit_levels,
        ratios=config.take_profit_ratios,
    )
    
    # 6. 风险回报比（放宽阈值：妖币波动大，1.0 即可）
    if side == "LONG":
        reward = take_profit_levels[0]["price"] - entry_price
        risk = entry_price - stop_loss
    else:
        reward = entry_price - take_profit_levels[0]["price"]
        risk = stop_loss - entry_price
    
    risk_reward_ratio = reward / risk if risk > 0 else 0
    
    if risk_reward_ratio < 1.0:  # 放宽到 1.0（妖币波动大）
        warnings.append(f"风险回报比过低：{risk_reward_ratio:.2f} < 1.0")
        risk_score += 20
    
    # 7. 波动率调整
    if config.volatility_adjustment:
        atr = atr_result.get("atr", 0)
        atr_pct = atr / entry_price * 100
        
        if atr_pct > 10:  # 高波动
            warnings.append(f"高波动：ATR {atr_pct:.1f}%，建议降低仓位")
            risk_score += 15
    
    # 基础风险分
    risk_score += (100 - min(100, risk_reward_ratio * 20))
    
    can_open = len([w for w in warnings if "超限" in w or "过低" in w]) == 0
    
    return {
        "can_open": can_open and risk_score < 70,
        "risk_score": min(100, risk_score),
        "risk_level": "高" if risk_score >= 60 else "中" if risk_score >= 40 else "低",
        "position_size": position_result,
        "stop_loss": atr_result,
        "take_profit": take_profit_levels,
        "correlation": correlation_result,
        "exposure": exposure_result,
        "warnings": warnings,
        "risk_reward_ratio": round(risk_reward_ratio, 2),
    }


# ═══════════════════════════════════════════════════════════════
# CLI 测试入口
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="🛡️ 神圣风控系统")
    parser.add_argument("--symbol", type=str, required=True, help="币种符号")
    parser.add_argument("--side", type=str, default="LONG", choices=["LONG", "SHORT"], help="方向")
    parser.add_argument("--entry-price", type=float, required=True, help="入场价")
    parser.add_argument("--balance", type=float, default=10000, help="账户余额")
    
    args = parser.parse_args()
    
    print("\n" + "═" * 70)
    print(f"🛡️ 风控评估 - {args.symbol}")
    print("═" * 70)
    
    # 模拟现有持仓
    existing_positions = [
        {"symbol": "BTCUSDT", "side": "LONG", "position_value": 1000},
    ]
    
    result = assess_trade_risk(
        symbol=args.symbol,
        side=args.side,
        entry_price=args.entry_price,
        account_balance=args.balance,
        existing_positions=existing_positions,
    )
    
    print(f"\n开仓许可：{'✅ 允许' if result['can_open'] else '❌ 拒绝'}")
    print(f"风险评分：{result['risk_score']}/100 ({result['risk_level']})")
    print(f"风险回报比：{result['risk_reward_ratio']:.2f}")
    
    print(f"\n📊 仓位计算:")
    ps = result['position_size']
    print(f"  数量：{ps.get('quantity', 0)}")
    print(f"  仓位价值：${ps.get('position_value', 0):.2f}")
    print(f"  风险金额：${ps.get('risk_amount', 0):.2f}")
    print(f"  仓位占比：{ps.get('position_pct', 0):.1f}%")
    
    print(f"\n🛑 止损:")
    sl = result['stop_loss']
    print(f"  止损价：${sl.get('stop_loss', 0):.4f}")
    print(f"  止损百分比：{sl.get('stop_loss_pct', 0):.1f}%")
    print(f"  方法：{sl.get('method', 'UNKNOWN')}")
    
    print(f"\n🎯 分级止盈:")
    for tp in result['take_profit']:
        print(f"  Level {tp['level']}: ${tp['price']:.4f} ({tp['quantity_pct']:.0f}% 仓位)")
    
    print(f"\n🔗 相关性:")
    corr = result['correlation']
    print(f"  板块：{corr.get('group', 'UNKNOWN')}")
    print(f"  已持有：{corr.get('correlated_count', 0)} 个")
    print(f"  风险：{corr.get('risk_level', 'UNKNOWN')}")
    
    if result['warnings']:
        print(f"\n⚠️  警告:")
        for w in result['warnings']:
            print(f"  - {w}")
    
    print("\n" + "═" * 70 + "\n")


if __name__ == "__main__":
    main()
