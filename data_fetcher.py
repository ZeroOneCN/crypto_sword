#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         📡 DATA FETCHER - 统一数据获取模块 📡                 ║
║                                                               ║
║    统一 K 线/行情/OI 获取逻辑，集中管理缓存与 API 限流         ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

功能：
- 统一缓存机制（TTLCache）
- 统一 API 调用（binance-cli / 原生 REST）
- 动态节流（替代硬编码延迟）
- 错误重试与降级

使用方式：
from data_fetcher import get_klines, get_ticker, get_open_interest
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# TTLCache - 统一缓存机制
# ═══════════════════════════════════════════════════════════════

class TTLCache:
    """带 TTL 和 LRU 清理的缓存类"""
    def __init__(self, ttl_sec: float = 30, max_size: int = 200):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl_sec
        self._max_size = max_size
    
    def get(self, key: str) -> Any:
        if key in self._cache:
            value, expires_at = self._cache[key]
            if time.time() < expires_at:
                return value
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any):
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
        self._cache[key] = (value, time.time() + self._ttl)
    
    def clear(self):
        self._cache.clear()
    
    def __len__(self) -> int:
        return len(self._cache)


# 全局缓存实例
_klines_cache = TTLCache(ttl_sec=30, max_size=200)
_ticker_cache = TTLCache(ttl_sec=30, max_size=100)
_oi_cache = TTLCache(ttl_sec=15, max_size=100)


# ═══════════════════════════════════════════════════════════════
# 动态节流机制 - 替代硬编码延迟
# ═══════════════════════════════════════════════════════════════

class DynamicThrottle:
    """动态 API 节流器
    
    根据最近 API 响应时间自动调整延迟：
    - 响应快 (<200ms) → 延迟 0.1s
    - 响应中 (200-500ms) → 延迟 0.3s
    - 响应慢 (>500ms) → 延迟 0.5s
    """
    def __init__(self):
        self._last_call_time = 0.0
        self._recent_latencies: List[float] = []
        self._max_history = 10
    
    def wait(self):
        """根据历史延迟计算等待时间"""
        now = time.time()
        avg_latency = sum(self._recent_latencies) / len(self._recent_latencies) if self._recent_latencies else 0.3
        
        if avg_latency < 0.2:
            delay = 0.1
        elif avg_latency < 0.5:
            delay = 0.3
        else:
            delay = 0.5
        
        elapsed = now - self._last_call_time
        if elapsed < delay:
            time.sleep(delay - elapsed)
        
        self._last_call_time = time.time()
    
    def record_latency(self, latency: float):
        """记录 API 响应时间"""
        self._recent_latencies.append(latency)
        if len(self._recent_latencies) > self._max_history:
            self._recent_latencies.pop(0)


_throttle = DynamicThrottle()


# ═══════════════════════════════════════════════════════════════
# 统一 API 调用
# ═══════════════════════════════════════════════════════════════

def run_binance_cli(args: List[str], max_retries: int = 3) -> Dict[str, Any] | List[Any]:
    """运行 binance-cli 命令（带动态节流与重试）"""
    cmd = ["binance-cli", "futures-usds"] + args
    
    for attempt in range(max_retries + 1):
        try:
            _throttle.wait()
            start_time = time.perf_counter()
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            latency = time.perf_counter() - start_time
            _throttle.record_latency(latency)
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() or f"Exit code {result.returncode}"
                if "rate limit" in error_msg.lower() or "too many requests" in error_msg.lower():
                    if attempt < max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"API 限流，等待 {wait_time}s 后重试 ({attempt + 1}/{max_retries + 1})")
                        time.sleep(wait_time)
                        continue
                raise RuntimeError(f"binance-cli 错误：{error_msg}")
            
            stdout = result.stdout.strip()
            if not stdout:
                if attempt < max_retries:
                    time.sleep(0.5)
                    continue
                raise RuntimeError("binance-cli 返回空响应")
            
            return json.loads(stdout)
            
        except subprocess.TimeoutExpired:
            if attempt < max_retries:
                time.sleep(0.5)
                continue
            raise RuntimeError("binance-cli 超时")
        except json.JSONDecodeError as e:
            if attempt < max_retries:
                time.sleep(0.5)
                continue
            raise RuntimeError(f"JSON 解析失败：{e}")
    
    raise RuntimeError("binance-cli 失败（已达最大重试次数）")


# ═══════════════════════════════════════════════════════════════
# 数据获取接口
# ═══════════════════════════════════════════════════════════════

def get_klines(symbol: str, interval: str = "1h", limit: int = 50) -> List[Dict[str, Any]]:
    """获取 K 线数据（带缓存）"""
    cache_key = f"{symbol}_{interval}_{limit}"
    cached = _klines_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        data = run_binance_cli([
            "klines", "--symbol", symbol, "--interval", interval, "--limit", str(limit)
        ])
        if isinstance(data, list):
            _klines_cache.set(cache_key, data)
            return data
        return []
    except Exception as e:
        logger.warning(f"获取 K 线失败 {symbol}: {e}")
        return []


def get_ticker(symbol: str = None) -> Dict[str, Any] | List[Any]:
    """获取 24h 行情数据（带缓存）"""
    cache_key = symbol or "ALL"
    cached = _ticker_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        args = ["ticker-24hr"]
        if symbol:
            args.extend(["--symbol", symbol])
        
        data = run_binance_cli(args)
        if isinstance(data, (dict, list)):
            _ticker_cache.set(cache_key, data)
            return data
        return {}
    except Exception as e:
        logger.warning(f"获取行情失败 {symbol}: {e}")
        return {}


def get_open_interest(symbol: str, period: str = "1h", limit: int = 24) -> List[Dict[str, Any]]:
    """获取 OI 历史数据（带缓存）"""
    cache_key = f"{symbol}_{period}_{limit}"
    cached = _oi_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        data = run_binance_cli([
            "open-interest-hist", "--symbol", symbol, "--period", period, "--limit", str(limit)
        ])
        if isinstance(data, list):
            _oi_cache.set(cache_key, data)
            return data
        return []
    except Exception as e:
        logger.warning(f"获取 OI 失败 {symbol}: {e}")
        return []


def clear_caches():
    """清空所有缓存"""
    _klines_cache.clear()
    _ticker_cache.clear()
    _oi_cache.clear()
    logger.info("📡 数据缓存已清空")
