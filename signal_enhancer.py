#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         🎯 SIGNAL ENHANCER - 神圣信号增强系统 🎯              ║
║                                                               ║
║    多时间框架确认 + 成交量验证 + 信号评分                      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""

import logging
import os
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)

try:
    from binance_api_client import get_native_binance_client
except Exception:
    get_native_binance_client = None

# ═══════════════════════════════════════════════
# K 线缓存 - 减少 API 调用
# ═══════════════════════════════════════════════
_klines_cache: Dict[str, Tuple[List, float]] = {}  # {key: (data, timestamp)}
_KLINES_CACHE_TTL = 30  # 30 秒缓存

def _get_cached_klines(symbol: str, interval: str) -> Optional[List]:
    """获取缓存的 K 线数据"""
    key = f"{symbol}_{interval}"
    if key in _klines_cache:
        data, ts = _klines_cache[key]
        if time.time() - ts < _KLINES_CACHE_TTL:
            return data
    return None

def _set_cached_klines(symbol: str, interval: str, data: List):
    """缓存 K 线数据"""
    key = f"{symbol}_{interval}"
    _klines_cache[key] = (data, time.time())


# ═══════════════════════════════════════════════════════════════
# 数据结构 - 阿尔忒弥斯的神箭
# ═══════════════════════════════════════════════════════════════

@dataclass
class SignalScore:
    """信号评分 - 综合质量评估"""
    symbol: str
    stage: str
    direction: str
    
    # 各项评分 (0-100)
    trend_score: float = 0.0      # 趋势强度
    volume_score: float = 0.0     # 成交量确认
    momentum_score: float = 0.0   # 动量强度
    breakout_score: float = 0.0   # 突破质量
    market_score: float = 0.0     # 市场环境
    
    # 综合评分
    total_score: float = 0.0
    confidence: str = "中"  # 低 / 中 / 高 / 极高
    
    # 否决项
    veto_signals: List[str] = None
    
    def __post_init__(self):
        if self.veto_signals is None:
            self.veto_signals = []
        self._calculate_total()
    
    def _calculate_total(self):
        """计算综合评分"""
        weights = {
            'trend': 0.25,
            'volume': 0.25,
            'momentum': 0.20,
            'breakout': 0.20,
            'market': 0.10,
        }
        
        self.total_score = (
            self.trend_score * weights['trend'] +
            self.volume_score * weights['volume'] +
            self.momentum_score * weights['momentum'] +
            self.breakout_score * weights['breakout'] +
            self.market_score * weights['market']
        )
        
        # 确定置信度（降低阈值，更容易交易）
        if self.total_score >= 70:
            self.confidence = "极高"
        elif self.total_score >= 50:
            self.confidence = "高"
        elif self.total_score >= 30:
            self.confidence = "中"
        else:
            self.confidence = "低"
        
        # 否决信号只警告，不再强制降级（数据不足时不惩罚）
        # 只有真正的风险信号才降级
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "stage": self.stage,
            "direction": self.direction,
            "scores": {
                "trend": round(self.trend_score, 2),
                "volume": round(self.volume_score, 2),
                "momentum": round(self.momentum_score, 2),
                "breakout": round(self.breakout_score, 2),
                "market": round(self.market_score, 2),
            },
            "total_score": round(self.total_score, 2),
            "confidence": self.confidence,
            "veto_signals": self.veto_signals,
        }


# ═══════════════════════════════════════════════
# API 调用限流 - 避免 Binance 限流
# ═══════════════════════════════════════════════
_last_api_call_time = 0.0
_API_CALL_INTERVAL = float(os.getenv("HERMES_SIGNAL_API_THROTTLE_SEC", "0.05"))
_MARKET_ENV_CACHE: Tuple[Dict[str, Any], float] | None = None
_MARKET_ENV_CACHE_TTL = float(os.getenv("HERMES_MARKET_ENV_CACHE_TTL_SEC", "300"))

def _throttle_api_call():
    """限流：确保 API 调用间隔"""
    global _last_api_call_time
    now = time.time()
    elapsed = now - _last_api_call_time
    if elapsed < _API_CALL_INTERVAL:
        time.sleep(_API_CALL_INTERVAL - elapsed)
    _last_api_call_time = time.time()


# ═══════════════════════════════════════════════════════════════
# Binance CLI 封装 - 赫尔墨斯的信使
# ═══════════════════════════════════════════════════════════════

def run_binance_cli(args: List[str], timeout: int = 60, max_retries: int = 5) -> Optional[Any]:
    """Compatibility wrapper backed by native Binance REST.
    
    Added retry logic and empty response handling to prevent JSON parse errors.
    Increased retries to 5 with exponential backoff for rate limit handling.
    Added API call throttling to avoid rate limiting.
    """
    # 限流：确保 API 调用间隔
    _throttle_api_call()
    
    if get_native_binance_client is None:
        logger.error("原生 Binance API 客户端不可用")
        return None

    for attempt in range(max_retries + 1):
        try:
            public_throttle = float(os.getenv("HERMES_SIGNAL_PUBLIC_THROTTLE_SEC", "0.05"))
            if public_throttle > 0:
                time.sleep(public_throttle)
            return get_native_binance_client().command_compat(list(args))  # type: ignore
        except Exception as e:
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"原生 Binance API 异常：{e}")
            return None

    return None


# ═══════════════════════════════════════════════════════════════
# 多时间框架分析 - 克罗诺斯的时间之轮
# ═══════════════════════════════════════════════════════════════

def get_klines(symbol: str, interval: str = "1h", limit: int = 50, use_cache: bool = True) -> Optional[List[Dict]]:
    """
    获取 K 线数据（带缓存）
    
    Intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    """
    # 尝试从缓存获取
    if use_cache:
        cached = _get_cached_klines(symbol, interval)
        if cached is not None:
            return cached
    
    data = run_binance_cli([
        "kline-candlestick-data",
        "--symbol", symbol,
        "--interval", interval,
        "--limit", str(limit)
    ])
    
    if not data or not isinstance(data, list):
        return None
    
    # 标准化 K 线数据
    klines = []
    for k in data:
        klines.append({
            "open_time": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": k[6],
            "quote_volume": float(k[7]),
            "trades": k[8],
        })
    
    # 缓存结果
    if use_cache:
        _set_cached_klines(symbol, interval, klines)
    
    return klines


def analyze_trend(klines: List[Dict]) -> Dict[str, Any]:
    """
    分析趋势强度
    
    Returns:
        {
            "direction": "UP" / "DOWN" / "SIDEWAYS",
            "strength": 0-100,
            "higher_highs": bool,
            "higher_lows": bool,
            "ma_alignment": str,
        }
    """
    if len(klines) < 20:
        return {"direction": "UNKNOWN", "strength": 0}
    
    closes = [k["close"] for k in klines]
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]
    
    # 计算均线
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    
    # 判断趋势方向
    if ma5 > ma10 > ma20:
        direction = "UP"
        base_strength = 70
    elif ma5 < ma10 < ma20:
        direction = "DOWN"
        base_strength = 70
    else:
        direction = "SIDEWAYS"
        base_strength = 30
    
    # 检查高低点
    recent_highs = highs[-10:]
    recent_lows = lows[-10:]
    
    higher_highs = recent_highs[-1] > recent_highs[0] if len(recent_highs) > 1 else False
    higher_lows = recent_lows[-1] > recent_lows[0] if len(recent_lows) > 1 else False
    
    # 调整强度
    if direction == "UP" and higher_highs and higher_lows:
        base_strength = min(100, base_strength + 20)
    elif direction == "DOWN" and not higher_highs and not higher_lows:
        base_strength = min(100, base_strength + 20)
    
    # 均线发散程度
    ma_spread = (ma5 - ma20) / ma20 * 100
    if abs(ma_spread) > 5:
        base_strength = min(100, base_strength + 10)
    
    return {
        "direction": direction,
        "strength": round(base_strength, 2),
        "higher_highs": higher_highs,
        "higher_lows": higher_lows,
        "ma_alignment": "BULLISH" if ma5 > ma20 else "BEARISH" if ma5 < ma20 else "NEUTRAL",
        "ma5": round(ma5, 4),
        "ma10": round(ma10, 4),
        "ma20": round(ma20, 4),
    }


def multi_timeframe_analysis(symbol: str) -> Dict[str, Any]:
    """
    多时间框架分析（优化：只使用 1h 减少 API 调用）
    
    Returns:
        {
            "1h": {...},
            "alignment": "BULLISH" / "BEARISH" / "MIXED",
            "score": 0-100,
        }
    """
    # 只使用 1h 时间框架（减少 66% API 调用）
    klines = get_klines(symbol, interval="1h", limit=50)
    if klines:
        trend = analyze_trend(klines)
        direction = trend.get("direction", "UNKNOWN")
        strength = trend.get("strength", 50)
        
        # 基于 1h 趋势判断
        if direction == "UP":
            alignment = "BULLISH"
        elif direction == "DOWN":
            alignment = "BEARISH"
        else:
            alignment = "MIXED"
        
        return {
            "1h": trend,
            "alignment": alignment,
            "score": strength,
        }
    else:
        return {
            "1h": {"direction": "UNKNOWN", "strength": 50},
            "alignment": "MIXED",
            "score": 50,
        }


# ═══════════════════════════════════════════════════════════════
# 成交量分析 - 赫菲斯托斯的熔炉
# ═══════════════════════════════════════════════════════════════

def analyze_volume(klines: List[Dict], lookback: int = 20) -> Dict[str, Any]:
    """
    分析成交量
    
    Returns:
        {
            "avg_volume": float,
            "current_volume": float,
            "volume_ratio": float,
            "volume_trend": "INCREASING" / "DECREASING" / "FLAT",
            "score": 0-100,
        }
    """
    if len(klines) < lookback:
        return {"score": 0}
    
    volumes = [k["volume"] for k in klines]
    quote_volumes = [k["quote_volume"] for k in klines]
    
    # 计算平均成交量
    avg_volume = sum(volumes[-lookback:-1]) / (lookback - 1)
    current_volume = volumes[-1]
    
    avg_quote = sum(quote_volumes[-lookback:-1]) / (lookback - 1)
    current_quote = quote_volumes[-1]
    
    # 成交量比率
    volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
    
    # 成交量趋势
    recent_avg = sum(volumes[-5:]) / 5
    older_avg = sum(volumes[-15:-10]) / 5 if len(volumes) >= 15 else recent_avg
    
    if recent_avg > older_avg * 1.2:
        volume_trend = "INCREASING"
    elif recent_avg < older_avg * 0.8:
        volume_trend = "DECREASING"
    else:
        volume_trend = "FLAT"
    
    # 评分
    if volume_ratio >= 3.0:
        score = 100  # 巨量
    elif volume_ratio >= 2.0:
        score = 80   # 放量
    elif volume_ratio >= 1.5:
        score = 60   # 温和放量
    elif volume_ratio >= 0.8:
        score = 40   # 正常
    else:
        score = 20   # 缩量
    
    return {
        "avg_volume": round(avg_volume, 2),
        "current_volume": round(current_volume, 2),
        "avg_quote_volume": round(avg_quote, 2),
        "current_quote_volume": round(current_quote, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_trend": volume_trend,
        "score": score,
    }


# ═══════════════════════════════════════════════════════════════
# 动量分析 - 赫尔墨斯的速度
# ═══════════════════════════════════════════════════════════════

def analyze_momentum(klines: List[Dict]) -> Dict[str, Any]:
    """
    分析动量（RSI 等指标）
    
    Returns:
        {
            "rsi": float,
            "rsi_signal": "OVERBOUGHT" / "OVERSOLD" / "NEUTRAL",
            "momentum_score": 0-100,
        }
    """
    if len(klines) < 14:
        return {"rsi": 50, "rsi_signal": "NEUTRAL", "momentum_score": 50}
    
    closes = [k["close"] for k in klines]
    
    # 计算 RSI
    gains = []
    losses = []
    
    for i in range(1, min(14, len(closes))):
        change = closes[-i] - closes[-i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    avg_gain = sum(gains) / len(gains) if gains else 0
    avg_loss = sum(losses) / len(losses) if losses else 1
    
    if avg_loss == 0:
        rsi = 100
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    
    # RSI 信号
    if rsi >= 70:
        rsi_signal = "OVERBOUGHT"
    elif rsi <= 30:
        rsi_signal = "OVERSOLD"
    else:
        rsi_signal = "NEUTRAL"
    
    # 动量评分（适中最好）
    if 40 <= rsi <= 60:
        momentum_score = 70  # 中性，有空间
    elif (30 <= rsi < 40) or (60 < rsi <= 70):
        momentum_score = 60  # 温和
    elif rsi < 30:
        momentum_score = 80  # 超卖，可能反弹
    elif rsi > 70:
        momentum_score = 40  # 超买，风险高
    else:
        momentum_score = 50
    
    return {
        "rsi": round(rsi, 2),
        "rsi_signal": rsi_signal,
        "momentum_score": momentum_score,
    }


# ═══════════════════════════════════════════════════════════════
# 突破质量分析 - 阿瑞斯的战锤
# ═══════════════════════════════════════════════════════════════

def analyze_breakout(symbol: str, stage: str, metrics: Dict) -> Dict[str, Any]:
    """
    分析突破质量
    
    Returns:
        {
            "is_valid_breakout": bool,
            "breakout_strength": 0-100,
            "resistance_levels": list,
            "veto_signals": list,
        }
    """
    veto_signals = []
    strength = 50  # 基础分数
    
    # 24h 涨幅检查
    change_24h = metrics.get("change_24h_pct", 0)
    if change_24h > 100:
        veto_signals.append("24h 涨幅过大 (>100%)，警惕回调")
        strength -= 30
    elif change_24h > 50:
        strength -= 15
    elif change_24h > 20:
        strength += 10
    elif change_24h > 10:
        strength += 20
    
    # 成交量检查
    volume_mult = metrics.get("volume_24h_mult", 1)
    if volume_mult >= 5:
        strength += 25  # 巨量确认
    elif volume_mult >= 3:
        strength += 15
    elif volume_mult >= 2:
        strength += 5
    else:
        veto_signals.append("成交量未放大，突破存疑")
        strength -= 20
    
    # OI 检查
    oi_change = metrics.get("oi_24h_pct", 0)
    if oi_change >= 50:
        strength += 20  # OI 大幅增加
    elif oi_change >= 20:
        strength += 10
    elif oi_change < 0:
        veto_signals.append("OI 下降，可能是假突破")
        strength -= 15
    
    # 阶段检查
    stage_scores = {
        "pre_break": 60,
        "confirmed_breakout": 90,
        "mania": 40,  # 狂热阶段风险高
        "exhaustion": 20,  # 衰竭阶段
        "neutral": 50,
    }
    stage_base = stage_scores.get(stage, 50)
    
    strength = (strength + stage_base) / 2
    strength = max(0, min(100, strength))
    
    return {
        "is_valid_breakout": len(veto_signals) == 0 and strength >= 50,
        "breakout_strength": round(strength, 2),
        "veto_signals": veto_signals,
    }


# ═══════════════════════════════════════════════════════════════
# 市场环境评分 - 雅典娜的智慧
# ═══════════════════════════════════════════════════════════════

def score_market_environment() -> Dict[str, Any]:
    """
    评分当前市场环境
    
    Returns:
        {
            "sentiment": "BULLISH" / "BEARISH" / "NEUTRAL",
            "score": 0-100,
            "fear_greed": dict,
        }
    """
    global _MARKET_ENV_CACHE
    now = time.time()
    if _MARKET_ENV_CACHE is not None:
        cached, cached_at = _MARKET_ENV_CACHE
        if now - cached_at < _MARKET_ENV_CACHE_TTL:
            return cached

    try:
        from surf_enhancer import get_market_overview
        overview = get_market_overview()
        
        fg = overview.get("fear_greed", {})
        fg_value = fg.get("value", 50)
        liquidation_risk = overview.get("liquidation_risk", "低")
        
        # 恐惧贪婪评分
        if fg_value >= 75:
            fg_score = 40  # 过度贪婪，风险高
            sentiment = "贪婪"
        elif fg_value >= 50:
            fg_score = 70
            sentiment = "看涨"
        elif fg_value >= 25:
            fg_score = 60
            sentiment = "看跌"
        else:
            fg_score = 80  # 极度恐惧，可能是机会
            sentiment = "恐惧"
        
        # 清算风险调整
        if liquidation_risk == "高":
            fg_score -= 20
        elif liquidation_risk == "中":
            fg_score -= 10
        
        result = {
            "sentiment": sentiment,
            "score": max(0, min(100, fg_score)),
            "fear_greed": fg,
            "liquidation_risk": liquidation_risk,
        }
        _MARKET_ENV_CACHE = (result, now)
        return result
        
    except Exception as e:
        logger.warning(f"获取市场环境失败：{e}")
        result = {
            "sentiment": "中性",
            "score": 50,
            "fear_greed": {},
            "liquidation_risk": "未知",
        }
        _MARKET_ENV_CACHE = (result, now)
        return result


# ═══════════════════════════════════════════════════════════════
# 综合信号评分 - 众神的裁决
# ═══════════════════════════════════════════════════════════════

def score_signal(
    symbol: str,
    stage: str,
    direction: str,
    metrics: Dict[str, Any],
) -> SignalScore:
    """
    综合评分信号质量
    
    Args:
        symbol: 币种符号
        stage: 突破阶段
        direction: 方向 (LONG/SHORT)
        metrics: 指标数据
    
    Returns:
        SignalScore 对象
    """
    score = SignalScore(
        symbol=symbol,
        stage=stage,
        direction=direction,
    )
    
    # 1. 多时间框架分析
    try:
        mtf = multi_timeframe_analysis(symbol)
        trend_alignment = mtf.get("alignment", "MIXED")
        
        # 趋势评分（不添加否决信号，只影响分数）
        if direction == "LONG":
            if trend_alignment == "BULLISH":
                score.trend_score = 90
            elif trend_alignment == "MIXED":
                score.trend_score = 60  # 中性也给较高分数
            else:
                score.trend_score = 40
        else:  # SHORT
            if trend_alignment == "BEARISH":
                score.trend_score = 90
            elif trend_alignment == "MIXED":
                score.trend_score = 60
            else:
                score.trend_score = 40
    except Exception as e:
        logger.warning(f"多时间框架分析失败：{e}")
        score.trend_score = 60  # 数据不足时给中性分数
    
    # 2. 成交量分析
    try:
        klines = get_klines(symbol, interval="1h", limit=50)
        if klines:
            vol_analysis = analyze_volume(klines)
            score.volume_score = vol_analysis.get("score", 50)
            # 不再因成交量不足添加否决信号
        else:
            score.volume_score = 50
    except Exception as e:
        logger.warning(f"成交量分析失败：{e}")
        score.volume_score = 50
    
    # 3. 动量分析
    try:
        if klines:
            momentum = analyze_momentum(klines)
            score.momentum_score = momentum.get("momentum_score", 50)
            # 不再因 RSI 超买/超卖添加否决信号（妖币可以持续超买）
    except Exception as e:
        logger.warning(f"动量分析失败：{e}")
        score.momentum_score = 50
    
    # 4. 突破质量分析
    try:
        breakout = analyze_breakout(symbol, stage, metrics)
        score.breakout_score = breakout.get("breakout_strength", 50)
        # 不再因突破无效添加否决信号
    except Exception as e:
        logger.warning(f"突破分析失败：{e}")
        score.breakout_score = 50
    
    # 5. 市场环境评分
    try:
        market = score_market_environment()
        score.market_score = market.get("score", 50)
    except Exception as e:
        logger.warning(f"市场环境评分失败：{e}")
        score.market_score = 50
    
    # 重新计算总分（会自动调用 _calculate_total）
    score._calculate_total()
    
    return score


# ═══════════════════════════════════════════════════════════════
# CLI 测试入口
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="🎯 神圣信号增强系统")
    parser.add_argument("--symbol", type=str, required=True, help="币种符号")
    parser.add_argument("--stage", type=str, default="pre_break", help="突破阶段")
    parser.add_argument("--direction", type=str, default="LONG", choices=["LONG", "SHORT"], help="方向")
    
    args = parser.parse_args()
    
    print("\n" + "═" * 70)
    print(f"🎯 信号质量评分 - {args.symbol}")
    print("═" * 70)
    
    # 模拟指标数据
    metrics = {
        "change_24h_pct": 25.0,
        "volume_24h_mult": 3.5,
        "oi_24h_pct": 30.0,
    }
    
    score = score_signal(args.symbol, args.stage, args.direction, metrics)
    
    result = score.to_dict()
    
    print(f"\n方向：{result['direction']}")
    print(f"阶段：{result['stage']}")
    print(f"\n各项评分:")
    print(f"  趋势强度：{result['scores']['trend']:.1f}/100")
    print(f"  成交量确认：{result['scores']['volume']:.1f}/100")
    print(f"  动量强度：{result['scores']['momentum']:.1f}/100")
    print(f"  突破质量：{result['scores']['breakout']:.1f}/100")
    print(f"  市场环境：{result['scores']['market']:.1f}/100")
    print(f"\n综合评分：{result['total_score']:.1f}/100")
    print(f"置信度：{result['confidence']}")
    
    if result['veto_signals']:
        print(f"\n⚠️  否决信号:")
        for v in result['veto_signals']:
            print(f"  - {v}")
    else:
        print(f"\n✅ 无否决信号")
    
    print("\n" + "═" * 70 + "\n")


if __name__ == "__main__":
    main()
