#!/usr/bin/env python3
"""
鈺斺晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
鈺?                                                              鈺?
鈺?        馃幆 SIGNAL ENHANCER - 绁炲湥淇″彿澧炲己绯荤粺 馃幆              鈺?
鈺?                                                              鈺?
鈺?   澶氭椂闂存鏋剁‘璁?+ 鎴愪氦閲忛獙璇?+ 淇″彿璇勫垎                      鈺?
鈺?                                                              鈺?
鈺氣晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
"""

import logging
import os
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from dataclasses import dataclass

from binance_compat import run_native_binance_compat

logger = logging.getLogger(__name__)


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# TTLCache - 缁熶竴缂撳瓨鏈哄埗锛堟浛浠ｅ叏灞€瀛楀吀锛岄槻姝㈠唴瀛樻硠婕忥級
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

class TTLCache:
    """甯?TTL 鍜?LRU 娓呯悊鐨勭紦瀛樼被
    
    Args:
        ttl_sec: 缂撳瓨杩囨湡鏃堕棿锛堢锛?
        max_size: 鏈€澶х紦瀛樻潯鐩暟锛堣秴杩囨椂娓呯悊鏈€鏃х殑鏉＄洰锛?
    """
    def __init__(self, ttl_sec: float = 30, max_size: int = 100):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl_sec
        self._max_size = max_size
    
    def get(self, key: str) -> Any:
        """鑾峰彇缂撳瓨锛岃繃鏈熷垯杩斿洖 None"""
        if key in self._cache:
            value, expires_at = self._cache[key]
            if time.time() < expires_at:
                return value
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """璁剧疆缂撳瓨锛岃秴杩?max_size 鏃舵竻鐞嗘渶鏃х殑鏉＄洰"""
        if len(self._cache) >= self._max_size:
            # 娓呯悊鏈€鏃х殑鏉＄洰
            oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
        self._cache[key] = (value, time.time() + self._ttl)
    
    def clear(self):
        """娓呯┖缂撳瓨"""
        self._cache.clear()
    
    def __len__(self) -> int:
        return len(self._cache)


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# K 绾跨紦瀛?- 鍑忓皯 API 璋冪敤
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

_klines_cache = TTLCache(ttl_sec=30, max_size=200)  # 30 绉?TTL锛屾渶澶?200 涓潯鐩?

def _get_cached_klines(symbol: str, interval: str) -> Optional[List]:
    """鑾峰彇缂撳瓨鐨?K 绾挎暟鎹?""
    key = f"{symbol}_{interval}"
    return _klines_cache.get(key)

def _set_cached_klines(symbol: str, interval: str, data: List):
    """缂撳瓨 K 绾挎暟鎹?""
    key = f"{symbol}_{interval}"
    _klines_cache.set(key, data)


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 鏁版嵁缁撴瀯 - 闃垮皵蹇掑讥鏂殑绁炵
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

@dataclass
class SignalScore:
    """淇″彿璇勫垎 - 缁煎悎璐ㄩ噺璇勪及"""
    symbol: str
    stage: str
    direction: str

    # 鍚勯」璇勫垎 (0-100)
    trend_score: float = 0.0      # 瓒嬪娍寮哄害
    volume_score: float = 0.0     # 鎴愪氦閲忕‘璁?
    momentum_score: float = 0.0   # 鍔ㄩ噺寮哄害
    breakout_score: float = 0.0   # 绐佺牬璐ㄩ噺
    market_score: float = 0.0     # 甯傚満鐜

    # 馃彟 搴勫闆疯揪璇勫垎锛堟柊澧烇級
    chase_score: float = 0.0      # 馃敟 杩藉绛栫暐璇勫垎
    composite_score: float = 0.0  # 馃搳 缁煎悎绛栫暐璇勫垎
    ambush_score: float = 0.0     # 馃幆 鍩紡绛栫暐璇勫垎
    dark_flow_score: float = 0.0  # 鏆楁祦淇″彿璇勫垎
    sideways_days: int = 0        # 妯洏澶╂暟
    market_cap_usd: float = 0.0   # 娴侀€氬競鍊?

    # 缁煎悎璇勫垎
    total_score: float = 0.0
    confidence: str = "涓?  # 浣?/ 涓?/ 楂?/ 鏋侀珮

    # 鍚﹀喅椤?
    veto_signals: List[str] = None

    def __post_init__(self):
        if self.veto_signals is None:
            self.veto_signals = []
        self._calculate_total()

    def _calculate_total(self):
        """璁＄畻缁煎悎璇勫垎锛堟敮鎸佺幆澧冨彉閲忛厤缃潈閲嶏級"""
        # 榛樿鏉冮噸
        default_weights = {
            'trend': 0.20,
            'volume': 0.20,
            'momentum': 0.15,
            'breakout': 0.15,
            'market': 0.10,
            'composite': 0.20,  # 馃彟 鏂板搴勫闆疯揪鏉冮噸
        }
        
        # 浠庣幆澧冨彉閲忚鍙栨潈閲嶏紙JSON 鏍煎紡锛?
        weights = default_weights
        try:
            env_weights = os.environ.get("HERMES_SCORE_WEIGHTS")
            if env_weights:
                import json
                weights = json.loads(env_weights)
                logger.debug(f"馃幆 浣跨敤鐜鍙橀噺閰嶇疆鐨勮瘎鍒嗘潈閲? {weights}")
        except Exception as e:
            logger.debug(f"璇诲彇璇勫垎鏉冮噸閰嶇疆澶辫触锛屼娇鐢ㄩ粯璁ゅ€? {e}")
        
        # 鍩虹璇勫垎
        base_score = (
            self.trend_score * weights.get('trend', default_weights['trend']) +
            self.volume_score * weights.get('volume', default_weights['volume']) +
            self.momentum_score * weights.get('momentum', default_weights['momentum']) +
            self.breakout_score * weights.get('breakout', default_weights['breakout']) +
            self.market_score * weights.get('market', default_weights['market'])
        )

        # 馃彟 搴勫闆疯揪璇勫垎鍔犳垚锛堝鏋滃彲鐢級
        radar_bonus = 0
        if self.composite_score > 0:
            radar_bonus = self.composite_score * weights.get('composite', default_weights['composite'])

        # 鏆楁祦淇″彿棰濆鍔犲垎锛堟渶楂?15鍒嗭級
        if self.dark_flow_score > 0:
            radar_bonus += min(self.dark_flow_score / 100 * 15, 15)

        self.total_score = base_score + radar_bonus

        # 纭畾缃俊搴︼紙闄嶄綆闃堝€硷紝鏇村鏄撲氦鏄擄級
        if self.total_score >= 70:
            self.confidence = "鏋侀珮"
        elif self.total_score >= 50:
            self.confidence = "楂?
        elif self.total_score >= 30:
            self.confidence = "涓?
        else:
            self.confidence = "浣?

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
                "chase": round(self.chase_score, 2),
                "composite": round(self.composite_score, 2),
                "ambush": round(self.ambush_score, 2),
                "dark_flow": round(self.dark_flow_score, 2),
            },
            "total_score": round(self.total_score, 2),
            "confidence": self.confidence,
            "veto_signals": self.veto_signals,
            "sideways_days": self.sideways_days,
            "market_cap_usd": round(self.market_cap_usd, 2),
        }


_MARKET_ENV_CACHE: Tuple[Dict[str, Any], float] | None = None
_MARKET_ENV_CACHE_TTL = float(os.getenv("HERMES_MARKET_ENV_CACHE_TTL_SEC", "300"))

# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 澶氭椂闂存鏋跺垎鏋?- 鍏嬬綏璇烘柉鐨勬椂闂翠箣杞?
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def get_klines(symbol: str, interval: str = "1h", limit: int = 50, use_cache: bool = True) -> Optional[List[Dict]]:
    """
    鑾峰彇 K 绾挎暟鎹紙甯︾紦瀛橈級
    
    Intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    """
    # 灏濊瘯浠庣紦瀛樿幏鍙?
    if use_cache:
        cached = _get_cached_klines(symbol, interval)
        if cached is not None:
            return cached
    
    data = run_native_binance_compat([
        "kline-candlestick-data",
        "--symbol", symbol,
        "--interval", interval,
        "--limit", str(limit)
    ])
    
    if not data or not isinstance(data, list):
        return None
    
    # 鏍囧噯鍖?K 绾挎暟鎹?
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
    
    # 缂撳瓨缁撴灉
    if use_cache:
        _set_cached_klines(symbol, interval, klines)
    
    return klines


def analyze_trend(klines: List[Dict]) -> Dict[str, Any]:
    """
    鍒嗘瀽瓒嬪娍寮哄害
    
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
    
    # 璁＄畻鍧囩嚎
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    
    # 鍒ゆ柇瓒嬪娍鏂瑰悜
    if ma5 > ma10 > ma20:
        direction = "UP"
        base_strength = 70
    elif ma5 < ma10 < ma20:
        direction = "DOWN"
        base_strength = 70
    else:
        direction = "SIDEWAYS"
        base_strength = 30
    
    # 妫€鏌ラ珮浣庣偣
    recent_highs = highs[-10:]
    recent_lows = lows[-10:]
    
    higher_highs = recent_highs[-1] > recent_highs[0] if len(recent_highs) > 1 else False
    higher_lows = recent_lows[-1] > recent_lows[0] if len(recent_lows) > 1 else False
    
    # 璋冩暣寮哄害
    if direction == "UP" and higher_highs and higher_lows:
        base_strength = min(100, base_strength + 20)
    elif direction == "DOWN" and not higher_highs and not higher_lows:
        base_strength = min(100, base_strength + 20)
    
    # 鍧囩嚎鍙戞暎绋嬪害
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


def multi_timeframe_analysis(symbol: str, klines_1h: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    澶氭椂闂存鏋跺垎鏋愶紙浼樺寲锛氬彧浣跨敤 1h 鍑忓皯 API 璋冪敤锛?
    
    Returns:
        {
            "1h": {...},
            "alignment": "BULLISH" / "BEARISH" / "MIXED",
            "score": 0-100,
        }
    """
    # 鍙娇鐢?1h 鏃堕棿妗嗘灦锛堝噺灏?66% API 璋冪敤锛?
    klines = klines_1h or get_klines(symbol, interval="1h", limit=50)
    if klines:
        trend = analyze_trend(klines)
        direction = trend.get("direction", "UNKNOWN")
        strength = trend.get("strength", 50)
        
        # 鍩轰簬 1h 瓒嬪娍鍒ゆ柇
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


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 鎴愪氦閲忓垎鏋?- 璧彶鏂墭鏂殑鐔旂倝
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def analyze_volume(klines: List[Dict], lookback: int = 20) -> Dict[str, Any]:
    """
    鍒嗘瀽鎴愪氦閲?
    
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
    
    # 璁＄畻骞冲潎鎴愪氦閲?
    avg_volume = sum(volumes[-lookback:-1]) / (lookback - 1)
    current_volume = volumes[-1]
    
    avg_quote = sum(quote_volumes[-lookback:-1]) / (lookback - 1)
    current_quote = quote_volumes[-1]
    
    # 鎴愪氦閲忔瘮鐜?
    volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
    
    # 鎴愪氦閲忚秼鍔?
    recent_avg = sum(volumes[-5:]) / 5
    older_avg = sum(volumes[-15:-10]) / 5 if len(volumes) >= 15 else recent_avg
    
    if recent_avg > older_avg * 1.2:
        volume_trend = "INCREASING"
    elif recent_avg < older_avg * 0.8:
        volume_trend = "DECREASING"
    else:
        volume_trend = "FLAT"
    
    # 璇勫垎
    if volume_ratio >= 3.0:
        score = 100  # 宸ㄩ噺
    elif volume_ratio >= 2.0:
        score = 80   # 鏀鹃噺
    elif volume_ratio >= 1.5:
        score = 60   # 娓╁拰鏀鹃噺
    elif volume_ratio >= 0.8:
        score = 40   # 姝ｅ父
    else:
        score = 20   # 缂╅噺
    
    return {
        "avg_volume": round(avg_volume, 2),
        "current_volume": round(current_volume, 2),
        "avg_quote_volume": round(avg_quote, 2),
        "current_quote_volume": round(current_quote, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_trend": volume_trend,
        "score": score,
    }


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 鍔ㄩ噺鍒嗘瀽 - 璧皵澧ㄦ柉鐨勯€熷害
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def analyze_momentum(klines: List[Dict]) -> Dict[str, Any]:
    """
    鍒嗘瀽鍔ㄩ噺锛圧SI 绛夋寚鏍囷級
    
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
    
    # 璁＄畻 RSI
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
    
    # RSI 淇″彿
    if rsi >= 70:
        rsi_signal = "OVERBOUGHT"
    elif rsi <= 30:
        rsi_signal = "OVERSOLD"
    else:
        rsi_signal = "NEUTRAL"
    
    # 鍔ㄩ噺璇勫垎锛堥€備腑鏈€濂斤級
    if 40 <= rsi <= 60:
        momentum_score = 70  # 涓€э紝鏈夌┖闂?
    elif (30 <= rsi < 40) or (60 < rsi <= 70):
        momentum_score = 60  # 娓╁拰
    elif rsi < 30:
        momentum_score = 80  # 瓒呭崠锛屽彲鑳藉弽寮?
    elif rsi > 70:
        momentum_score = 40  # 瓒呬拱锛岄闄╅珮
    else:
        momentum_score = 50
    
    return {
        "rsi": round(rsi, 2),
        "rsi_signal": rsi_signal,
        "momentum_score": momentum_score,
    }


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 绐佺牬璐ㄩ噺鍒嗘瀽 - 闃跨憺鏂殑鎴橀敜
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def analyze_breakout(symbol: str, stage: str, metrics: Dict) -> Dict[str, Any]:
    """
    鍒嗘瀽绐佺牬璐ㄩ噺
    
    Returns:
        {
            "is_valid_breakout": bool,
            "breakout_strength": 0-100,
            "resistance_levels": list,
            "veto_signals": list,
        }
    """
    veto_signals = []
    strength = 50  # 鍩虹鍒嗘暟
    
    # 24h 娑ㄥ箙妫€鏌?
    change_24h = metrics.get("change_24h_pct", 0)
    if change_24h > 100:
        veto_signals.append("24h 娑ㄥ箙杩囧ぇ (>100%)锛岃鎯曞洖璋?)
        strength -= 30
    elif change_24h > 50:
        strength -= 15
    elif change_24h > 20:
        strength += 10
    elif change_24h > 10:
        strength += 20
    
    # 鎴愪氦閲忔鏌?
    volume_mult = metrics.get("volume_24h_mult", 1)
    if volume_mult >= 5:
        strength += 25  # 宸ㄩ噺纭
    elif volume_mult >= 3:
        strength += 15
    elif volume_mult >= 2:
        strength += 5
    else:
        veto_signals.append("鎴愪氦閲忔湭鏀惧ぇ锛岀獊鐮村瓨鐤?)
        strength -= 20
    
    # OI 妫€鏌?
    oi_change = metrics.get("oi_24h_pct", 0)
    if oi_change >= 50:
        strength += 20  # OI 澶у箙澧炲姞
    elif oi_change >= 20:
        strength += 10
    elif oi_change < 0:
        veto_signals.append("OI 涓嬮檷锛屽彲鑳芥槸鍋囩獊鐮?)
        strength -= 15
    
    # 闃舵妫€鏌?
    stage_scores = {
        "pre_break": 60,
        "confirmed_breakout": 90,
        "mania": 40,  # 鐙傜儹闃舵椋庨櫓楂?
        "exhaustion": 20,  # 琛扮闃舵
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


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 甯傚満鐜璇勫垎 - 闆呭吀濞滅殑鏅烘収
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def score_market_environment() -> Dict[str, Any]:
    """
    璇勫垎褰撳墠甯傚満鐜
    
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
        overview = {}
        fg = overview.get("fear_greed", {})
        fg_value = fg.get("value", 50)
        liquidation_risk = overview.get("liquidation_risk", "浣?)
        
        # 鎭愭儳璐┆璇勫垎
        if fg_value >= 75:
            fg_score = 40  # 杩囧害璐┆锛岄闄╅珮
            sentiment = "璐┆"
        elif fg_value >= 50:
            fg_score = 70
            sentiment = "鐪嬫定"
        elif fg_value >= 25:
            fg_score = 60
            sentiment = "鐪嬭穼"
        else:
            fg_score = 80  # 鏋佸害鎭愭儳锛屽彲鑳芥槸鏈轰細
            sentiment = "鎭愭儳"
        
        # 娓呯畻椋庨櫓璋冩暣
        if liquidation_risk == "楂?:
            fg_score -= 20
        elif liquidation_risk == "涓?:
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
        logger.warning(f"鑾峰彇甯傚満鐜澶辫触锛歿e}")
        result = {
            "sentiment": "涓€?,
            "score": 50,
            "fear_greed": {},
            "liquidation_risk": "鏈煡",
        }
        _MARKET_ENV_CACHE = (result, now)
        return result


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 缁煎悎淇″彿璇勫垎 - 浼楃鐨勮鍐?
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def score_signal(
    symbol: str,
    stage: str,
    direction: str,
    metrics: Dict[str, Any],
    klines_1h: Optional[List[Dict[str, Any]]] = None,
) -> SignalScore:
    """
    缁煎悎璇勫垎淇″彿璐ㄩ噺
    
    Args:
        symbol: 甯佺绗﹀彿
        stage: 绐佺牬闃舵
        direction: 鏂瑰悜 (LONG/SHORT)
        metrics: 鎸囨爣鏁版嵁
    
    Returns:
        SignalScore 瀵硅薄
    """
    score = SignalScore(
        symbol=symbol,
        stage=stage,
        direction=direction,
    )
    klines = klines_1h
    if not klines:
        cached_klines = metrics.get("klines_1h")
        if isinstance(cached_klines, list) and cached_klines:
            klines = cached_klines
    
    # 1. 澶氭椂闂存鏋跺垎鏋?
    try:
        mtf = multi_timeframe_analysis(symbol, klines_1h=klines)
        trend_alignment = mtf.get("alignment", "MIXED")
        
        # 瓒嬪娍璇勫垎锛堜笉娣诲姞鍚﹀喅淇″彿锛屽彧褰卞搷鍒嗘暟锛?
        if direction == "LONG":
            if trend_alignment == "BULLISH":
                score.trend_score = 90
            elif trend_alignment == "MIXED":
                score.trend_score = 60  # 涓€т篃缁欒緝楂樺垎鏁?
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
        logger.warning(f"澶氭椂闂存鏋跺垎鏋愬け璐ワ細{e}")
        score.trend_score = 60  # 鏁版嵁涓嶈冻鏃剁粰涓€у垎鏁?
    
    # 2. 鎴愪氦閲忓垎鏋?
    try:
        if not klines:
            klines = get_klines(symbol, interval="1h", limit=50)
        if klines:
            vol_analysis = analyze_volume(klines)
            score.volume_score = vol_analysis.get("score", 50)
            # 涓嶅啀鍥犳垚浜ら噺涓嶈冻娣诲姞鍚﹀喅淇″彿
        else:
            score.volume_score = 50
    except Exception as e:
        logger.warning(f"鎴愪氦閲忓垎鏋愬け璐ワ細{e}")
        score.volume_score = 50
    
    # 3. 鍔ㄩ噺鍒嗘瀽
    try:
        if klines:
            momentum = analyze_momentum(klines)
            score.momentum_score = momentum.get("momentum_score", 50)
            # 涓嶅啀鍥?RSI 瓒呬拱/瓒呭崠娣诲姞鍚﹀喅淇″彿锛堝甯佸彲浠ユ寔缁秴涔帮級
    except Exception as e:
        logger.warning(f"鍔ㄩ噺鍒嗘瀽澶辫触锛歿e}")
        score.momentum_score = 50
    
    # 4. 绐佺牬璐ㄩ噺鍒嗘瀽
    try:
        breakout = analyze_breakout(symbol, stage, metrics)
        score.breakout_score = breakout.get("breakout_strength", 50)
        # 涓嶅啀鍥犵獊鐮存棤鏁堟坊鍔犲惁鍐充俊鍙?
    except Exception as e:
        logger.warning(f"绐佺牬鍒嗘瀽澶辫触锛歿e}")
        score.breakout_score = 50
    
    # 5. 甯傚満鐜璇勫垎
    try:
        market = score_market_environment()
        score.market_score = market.get("score", 50)
    except Exception as e:
        logger.warning(f"甯傚満鐜璇勫垎澶辫触锛歿e}")
        score.market_score = 50
    
    # 閲嶆柊璁＄畻鎬诲垎锛堜細鑷姩璋冪敤 _calculate_total锛?
    score._calculate_total()
    
    return score


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# 馃彟 搴勫闆疯揪璇勫垎闆嗘垚
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def enhance_with_radar_score(score: SignalScore, metrics: Dict[str, Any]) -> SignalScore:
    """Compatibility no-op: radar module entry has been removed."""
    _ = metrics
    return score


# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
# CLI 娴嬭瘯鍏ュ彛
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="馃幆 绁炲湥淇″彿澧炲己绯荤粺")
    parser.add_argument("--symbol", type=str, required=True, help="甯佺绗﹀彿")
    parser.add_argument("--stage", type=str, default="pre_break", help="绐佺牬闃舵")
    parser.add_argument("--direction", type=str, default="LONG", choices=["LONG", "SHORT"], help="鏂瑰悜")
    
    args = parser.parse_args()
    
    print("\n" + "鈺? * 70)
    print(f"馃幆 淇″彿璐ㄩ噺璇勫垎 - {args.symbol}")
    print("鈺? * 70)
    
    # 妯℃嫙鎸囨爣鏁版嵁
    metrics = {
        "change_24h_pct": 25.0,
        "volume_24h_mult": 3.5,
        "oi_24h_pct": 30.0,
    }
    
    score = score_signal(args.symbol, args.stage, args.direction, metrics)
    
    result = score.to_dict()
    
    print(f"\n鏂瑰悜锛歿result['direction']}")
    print(f"闃舵锛歿result['stage']}")
    print(f"\n鍚勯」璇勫垎:")
    print(f"  瓒嬪娍寮哄害锛歿result['scores']['trend']:.1f}/100")
    print(f"  鎴愪氦閲忕‘璁わ細{result['scores']['volume']:.1f}/100")
    print(f"  鍔ㄩ噺寮哄害锛歿result['scores']['momentum']:.1f}/100")
    print(f"  绐佺牬璐ㄩ噺锛歿result['scores']['breakout']:.1f}/100")
    print(f"  甯傚満鐜锛歿result['scores']['market']:.1f}/100")
    print(f"\n缁煎悎璇勫垎锛歿result['total_score']:.1f}/100")
    print(f"缃俊搴︼細{result['confidence']}")
    
    if result['veto_signals']:
        print(f"\n鈿狅笍  鍚﹀喅淇″彿:")
        for v in result['veto_signals']:
            print(f"  - {v}")
    else:
        print(f"\n鉁?鏃犲惁鍐充俊鍙?)
    
    print("\n" + "鈺? * 70 + "\n")


if __name__ == "__main__":
    main()

