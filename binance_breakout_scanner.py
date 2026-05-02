"""Binance breakout scanner (MVP).

This module will grow into a full scanner that:
- pulls Binance USDT-margined futures public data
- computes breakout-style metrics
- classifies stage via token_anomaly_radar.classify_breakout_stage

For now, we start with small, unit-tested metric helpers.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Tuple

from hermes_paths import hermes_scripts_dir

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.RLock()
_CACHE: dict[tuple[Any, ...], tuple[float, Any]] = {}


def _cache_get(key: tuple[Any, ...], ttl_sec: float) -> Any | None:
    if ttl_sec <= 0:
        return None
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        expires_at, value = cached
        if expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return value


def _cache_set(key: tuple[Any, ...], value: Any, ttl_sec: float) -> Any:
    if ttl_sec <= 0:
        return value
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + ttl_sec, value)
    return value

try:
    from binance_api_client import get_native_binance_client
except Exception:
    get_native_binance_client = None


def _tradifi_symbol_set(ttl_sec: float = 300.0) -> set[str]:
    allow_tradifi = str(os.environ.get("HERMES_ALLOW_TRADIFI_PERPS", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    allow_tradifi = allow_tradifi or str(os.environ.get("BINANCE_ALLOW_TRADIFI_PERPS", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    if allow_tradifi:
        return set()

    cache_key = ("tradifi_symbol_set",)
    cached = _cache_get(cache_key, ttl_sec)
    if cached is not None:
        return set(cached)

    symbols: set[str] = set()
    if not get_native_binance_client:
        return symbols
    try:
        info = get_native_binance_client().exchange_info()
        for item in info.get("symbols", []) if isinstance(info, dict) else []:
            symbol = str(item.get("symbol", "") or "").upper()
            if not symbol:
                continue
            contract_type = str(item.get("contractType", "") or "").upper()
            subtypes = {str(x or "").strip().upper() for x in (item.get("underlyingSubType", []) or [])}
            if contract_type == "TRADIFI_PERPETUAL" or "TRADIFI" in subtypes or "TRADFI" in subtypes:
                symbols.add(symbol)
    except Exception as e:
        logger.debug(f"tradifi symbol refresh failed: {e}")
    return _cache_set(cache_key, symbols, ttl_sec)


def compute_change_pct(closes: list[float], lookback_candles: int) -> float:
    """Return percent change between the latest close and the close `lookback_candles` ago."""
    if lookback_candles <= 0:
        raise ValueError("lookback_candles must be > 0")
    if len(closes) < lookback_candles + 1:
        raise ValueError("not enough closes")

    old = float(closes[-(lookback_candles + 1)])
    new = float(closes[-1])
    if old == 0:
        raise ValueError("old close is zero")
    return (new / old - 1.0) * 100.0


def compute_volume_mult(
    quote_volumes: list[float],
    *,
    window_candles: int,
    baseline_candles: int,
) -> float:
    """Compute volume expansion multiple.

    ratio = sum(last window_candles) / avg(sum(per-window) over the previous baseline_candles)

    Example (as tested):
      baseline_candles=10, window_candles=5
      baseline split into two 5-candle windows; take their average sum.
    """
    if window_candles <= 0 or baseline_candles <= 0:
        raise ValueError("window_candles and baseline_candles must be > 0")
    if baseline_candles % window_candles != 0:
        raise ValueError("baseline_candles must be a multiple of window_candles")
    if len(quote_volumes) < window_candles + baseline_candles:
        raise ValueError("not enough volume candles")

    last_sum = sum(float(x) for x in quote_volumes[-window_candles:])
    baseline = [float(x) for x in quote_volumes[-(window_candles + baseline_candles) : -window_candles]]

    # Split baseline into equal chunks of size window_candles.
    chunk_sums = []
    for i in range(0, len(baseline), window_candles):
        chunk_sums.append(sum(baseline[i : i + window_candles]))

    baseline_avg = sum(chunk_sums) / len(chunk_sums)
    if baseline_avg == 0:
        return float("inf") if last_sum > 0 else 1.0
    return last_sum / baseline_avg


def compute_drawdown_from_high_pct(highs: list[float], last_close: float) -> float:
    """Compute drawdown percentage from the highest high to the last close."""
    if not highs:
        raise ValueError("highs list cannot be empty")
    highest = max(float(h) for h in highs)
    if highest == 0:
        raise ValueError("highest high is zero")
    return (highest - float(last_close)) / highest * 100.0


def derive_venues_events(
    *,
    max_abs_return_pct_180m: float,
    volume_mult_180m: float,
    oi_change_pct_180m: float,
    ls_ratio_delta: float,
    funding_rate: float,
) -> Tuple[int, int]:
    """Approximate the 'venues/events' concept from the X design using independent signal families.

    We don't have true multi-venue / multi-source counts in the Binance-only MVP.
    Instead:
      venues = number of signal families triggered
      events = a conservative total trigger count (>= venues)

    Families:
      - price impulse
      - volume expansion
      - open interest expansion
      - positioning shift (long/short ratio)
      - extreme funding (optional)
    """

    venues = 0
    events = 0

    # Price family
    if abs(max_abs_return_pct_180m) >= 1.5:
        venues += 1
        events += 1

    # Volume family
    if volume_mult_180m >= 2.5:
        venues += 1
        events += 1

    # OI family
    if oi_change_pct_180m >= 8.0:
        venues += 1
        events += 1

    # Positioning family
    if abs(ls_ratio_delta) >= 0.25:
        venues += 1
        events += 1

    # Funding family (treat as an extra event if extreme)
    if abs(funding_rate) >= 0.01:
        venues += 1
        events += 1

    return venues, events


_radar_mod = None


def _load_radar_module():
    """Load token_anomaly_radar module.

    Prefer regular imports (repo-local). If not importable, fall back to
    `$HERMES_HOME/scripts/token_anomaly_radar.py` for legacy deployments.
    """
    global _radar_mod
    if _radar_mod is not None:
        return _radar_mod

    try:
        import token_anomaly_radar as mod  # type: ignore

        _radar_mod = mod
        return mod
    except Exception:
        pass

    module_path = hermes_scripts_dir() / "token_anomaly_radar.py"
    if not module_path.exists():
        raise FileNotFoundError(f"token_anomaly_radar.py not found: {module_path}")

    spec = importlib.util.spec_from_file_location("token_anomaly_radar", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load spec for token_anomaly_radar from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    _radar_mod = mod
    return mod


def decide_direction(stage: str, metrics: dict) -> str:
    """Map (stage, metrics) -> trade direction label.
    
    支持双向交易 - 宽松版（更容易开单）：
    - LONG: 做多信号
    - SHORT: 做空信号
    - WATCH: 观望
    - AVOID_CHASE: 避免追高/追空
    - RISK_OFF: 风险规避
    - NO_TRADE: 不交易
    
    放宽策略：
    - 主要看价格动量和阶段判断
    - OI/LS/资金费率作为辅助确认，不是硬性条件
    """
    change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
    change_72h = float(metrics.get("change_72h_pct", 0.0) or 0.0)
    funding = float(metrics.get("funding_rate", 0.0) or 0.0)
    ls_now = float(metrics.get("ls_ratio_now", 0.0) or 0.0)
    ls_prev = float(metrics.get("ls_ratio_prev_24h", 0.0) or 0.0)
    oi_24h = float(metrics.get("oi_24h_pct", 0.0) or 0.0)
    volume_mult = float(metrics.get("volume_24h_mult", 0.0) or 0.0)
    
    ls_rising = ls_now > ls_prev
    ls_falling = ls_now < ls_prev
    
    # pre_break / confirmed_breakout: 宽松双向交易
    if stage == "pre_break" or stage == "confirmed_breakout":
        # 做多条件：价格上涨为主，其他辅助（放宽到 2 个条件满足即可）
        long_signals = sum([
            change_24h > 0,           # 价格上涨
            funding >= -0.001,        # 资金费率不太负
            ls_rising,                # 多空比上升
            oi_24h > -5,              # OI 不太降
        ])
        if long_signals >= 2:
            return "LONG"
        
        # 做空条件：价格下跌为主，其他辅助
        short_signals = sum([
            change_24h < 0,           # 价格下跌
            funding <= 0.001,         # 资金费率不太正
            ls_falling,               # 多空比下降
            oi_24h > -5,              # OI 不太降
        ])
        if short_signals >= 2:
            return "SHORT"
        
        return "WATCH"
    
    # mania: 极端行情 - 反向交易
    if stage == "mania":
        # 多头过热 → 做空
        if change_24h >= 30 or ls_now >= 2.0:
            return "SHORT"
        # 空头过热 → 做多
        if change_24h <= -25 or ls_now <= 0.6:
            return "LONG"
        return "AVOID_CHASE"
    
    # exhaustion: 衰竭 - 反转交易（放宽阈值）
    if stage == "exhaustion":
        # 高位衰竭 → 做空（更宽松）
        if change_72h >= 30 and change_24h < 20:
            return "SHORT"
        # 低位衰竭 → 做多（更宽松）
        if change_72h <= -25 and change_24h > -20:
            return "LONG"
        return "RISK_OFF"
    
    return "NO_TRADE"


def classify_and_direction(metrics: dict) -> tuple[str, str, str, str]:
    """Return (stage, direction, trigger, risk)."""
    radar = _load_radar_module()
    stage, trigger, risk = radar.classify_breakout_stage(metrics)
    direction = decide_direction(stage, metrics)
    return stage, direction, trigger, risk


@dataclass
class SymbolBreakoutResult:
    """Result for a single symbol after scanning."""
    symbol: str
    stage: str
    direction: str
    trigger: str
    risk: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "stage": self.stage,
            "direction": self.direction,
            "trigger": self.trigger,
            "risk": self.risk,
            "metrics": self.metrics,
        }


def _run_native_binance_compat(args: list[str], max_retries: int = 3) -> dict[str, Any] | list[Any]:
    """Compatibility wrapper backed by native Binance REST.
    
    Added retry logic and empty response handling to prevent JSON parse errors.
    Increased retries to 5 with exponential backoff for rate limit handling.
    Added API call throttling to avoid rate limiting.
    """
    if get_native_binance_client is None:
        raise RuntimeError("原生 Binance API 客户端不可用")

    for attempt in range(max_retries + 1):
        try:
            throttle_sec = float(os.getenv("HERMES_BINANCE_PUBLIC_THROTTLE_SEC", "0.02"))
            if throttle_sec > 0:
                time.sleep(throttle_sec)
            return get_native_binance_client().command_compat(args)  # type: ignore
        except Exception as e:
            if attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))  # Faster backoff: 0.5s, 1s
                continue
            raise RuntimeError(f"原生 Binance API 调用失败：{e}")

    raise RuntimeError("原生 Binance API 失败（已达最大重试次数）")


def fetch_ticker_24hr(symbol: str | None = None) -> dict[str, Any]:
    """Fetch 24hr ticker statistics. If symbol is None, fetch all."""
    cache_key = ("ticker_24hr", symbol or "ALL")
    # 优化：全市场 ticker 缓存从 5s 延长至 30s，减少 API 调用频率
    cached = _cache_get(cache_key, 30 if symbol is None else 30)
    if cached is not None:
        return cached
    if symbol:
        all_tickers = _cache_get(("ticker_24hr", "ALL"), 30)
        if isinstance(all_tickers, list):
            for ticker in all_tickers:
                if ticker.get("symbol") == symbol:
                    return _cache_set(cache_key, ticker, 30)
    # Binance API ticker24hr-price-change-statistics does not support --symbol filter
    # Always fetch all tickers and extract the target symbol
    all_data = _run_native_binance_compat(["ticker24hr-price-change-statistics"])
    ttl = 30
    if isinstance(all_data, list):
        # Cache the full list for future lookups
        _cache_set(("ticker_24hr", "ALL"), all_data, ttl)
        if symbol:
            for ticker in all_data:
                if ticker.get("symbol") == symbol:
                    return _cache_set(cache_key, ticker, ttl)
            logger.warning(f"Symbol {symbol} not found in ticker list")
            return {}
    return _cache_set(cache_key, all_data, ttl)  # type: ignore


def fetch_open_interest(symbol: str) -> dict[str, Any]:
    """Fetch current open interest for a symbol."""
    cache_key = ("open_interest", symbol)
    cached = _cache_get(cache_key, 15)
    if cached is not None:
        return cached
    return _cache_set(cache_key, _run_native_binance_compat(["open-interest", "--symbol", symbol]), 15)  # type: ignore


def fetch_oi_statistics(symbol: str, period: str = "1h", limit: int = 24) -> list[dict[str, Any]]:
    """Fetch open interest statistics history.
    
    Note: New coins (like volatile meme coins) may have no OI history.
    Returns empty list [] if no data available - caller should handle gracefully.
    """
    cache_key = ("oi_statistics", symbol, period, limit)
    cached = _cache_get(cache_key, 120)
    if cached is not None:
        return cached
    try:
        data = _run_native_binance_compat(["open-interest-statistics", "--symbol", symbol, "--period", period, "--limit", str(limit)])  # type: ignore
        return _cache_set(cache_key, data, 120)
    except RuntimeError:
        return []  # Handle new coins with no OI history


def fetch_long_short_ratio(symbol: str, period: str = "1h", limit: int = 24) -> list[dict[str, Any]]:
    """Fetch long/short ratio history."""
    cache_key = ("long_short_ratio", symbol, period, limit)
    cached = _cache_get(cache_key, 120)
    if cached is not None:
        return cached
    data = _run_native_binance_compat(["long-short-ratio", "--symbol", symbol, "--period", period, "--limit", str(limit)])  # type: ignore
    return _cache_set(cache_key, data, 120)


def fetch_funding_rate(symbol: str, limit: int = 3) -> list[dict[str, Any]]:
    """Fetch funding rate history."""
    cache_key = ("funding_rate", symbol, limit)
    cached = _cache_get(cache_key, 180)
    if cached is not None:
        return cached
    data = _run_native_binance_compat(["get-funding-rate-history", "--symbol", symbol, "--limit", str(limit)])  # type: ignore
    return _cache_set(cache_key, data, 180)


def fetch_klines(symbol: str, interval: str = "1h", limit: int = 24) -> list[Any]:
    """Fetch and cache kline data for scanner-only volume expansion."""
    cache_key = ("klines", symbol, interval, limit)
    cached = _cache_get(cache_key, 60)
    if cached is not None:
        return cached
    data = _run_native_binance_compat([
        "kline-candlestick-data",
        "--symbol", symbol,
        "--interval", interval,
        "--limit", str(limit),
    ])
    return _cache_set(cache_key, data, 60)  # type: ignore


def build_symbol_metrics(symbol: str) -> dict[str, Any] | None:
    """Build complete metrics dict for a single symbol from Binance data.
    
    Returns None if symbol is not tradable (delisted or delivering).
    """
    try:
        ticker = fetch_ticker_24hr(symbol)
        oi_current = fetch_open_interest(symbol)
    except Exception as e:
        # Symbol not tradable (delisted, delivering, or testnet unavailable)
        logger.debug(f"Skipping {symbol}: {e}")
        return None
    
    # OI stats with error handling (new symbols may have no history)
    try:
        oi_stats = fetch_oi_statistics(symbol, period="1h", limit=24)
    except Exception:
        oi_stats = []
    
    # LS ratio with error handling
    try:
        ls_ratio = fetch_long_short_ratio(symbol, period="1h", limit=24)
    except Exception:
        ls_ratio = []
    
    # Funding rate with error handling
    try:
        funding = fetch_funding_rate(symbol, limit=3)
    except Exception:
        funding = []

    # Parse ticker fields
    price_change_pct = float(ticker.get("priceChangePercent", 0))
    volume = float(ticker.get("volume", 0))
    quote_volume = float(ticker.get("quoteVolume", 0))
    high = float(ticker.get("highPrice", 0))
    low = float(ticker.get("lowPrice", 0))
    open_price = float(ticker.get("openPrice", 0))
    last_price = float(ticker.get("lastPrice", 0))

    # OI current
    oi_value = float(oi_current.get("openInterest", 0))

    # OI change (approximate from stats - compare latest vs oldest)
    oi_24h_pct = 0.0
    if len(oi_stats) >= 2:
        oldest_oi = float(oi_stats[0].get("sumOpenInterest", 0))
        latest_oi = float(oi_stats[-1].get("sumOpenInterest", 0))
        if oldest_oi > 0:
            oi_24h_pct = (latest_oi - oldest_oi) / oldest_oi * 100

    # Long/short ratio
    ls_now = 0.0
    ls_prev = 0.0
    if len(ls_ratio) >= 1:
        ls_now = float(ls_ratio[-1].get("longShortRatio", 0))
    if len(ls_ratio) >= 2:
        ls_prev = float(ls_ratio[-2].get("longShortRatio", 0))

    # Funding rate (latest)
    funding_rate = 0.0
    if len(funding) >= 1:
        funding_rate = float(funding[-1].get("fundingRate", 0))

    # 计算成交量倍数（修复：使用 K 线数据计算真实倍数）
    volume_24h_mult = 1.0
    normalized_klines_1h: list[dict[str, Any]] = []
    try:
        # 获取 1h K 线数据（最近 24 根）
        klines_data = fetch_klines(symbol, interval="1h", limit=24)
        if klines_data and isinstance(klines_data, list):
            for k in klines_data:
                if isinstance(k, list) and len(k) >= 8:
                    normalized_klines_1h.append({
                        "open_time": k[0],
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                        "close_time": k[6],
                        "quote_volume": float(k[7]),
                    })
        
        if klines_data and isinstance(klines_data, list) and len(klines_data) >= 12:
            # 计算最近 12 小时的平均成交量
            recent_volumes = []
            for k in klines_data[-12:]:
                if isinstance(k, list) and len(k) >= 7:
                    recent_volumes.append(float(k[7]))  # quoteVolume 是第 7 个字段（索引 7）
            
            # 计算之前 12 小时的平均成交量作为基准
            baseline_volumes = []
            for k in klines_data[:12]:
                if isinstance(k, list) and len(k) >= 7:
                    baseline_volumes.append(float(k[7]))
            
            if baseline_volumes and sum(baseline_volumes) > 0:
                recent_avg = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
                baseline_avg = sum(baseline_volumes) / len(baseline_volumes)
                volume_24h_mult = recent_avg / baseline_avg if baseline_avg > 0 else 1.0
    except Exception as e:
        logger.debug(f"计算 {symbol} 成交量倍数失败：{e}，使用默认值 1.0")

    # Drawdown approximation (high vs last)
    drawdown = 0.0
    if high > 0:
        drawdown = (high - last_price) / high * 100

    # Venues/events approximation (we only have Binance, so use signal families)
    venues, events = derive_venues_events(
        max_abs_return_pct_180m=abs(price_change_pct),
        volume_mult_180m=volume_24h_mult,
        oi_change_pct_180m=oi_24h_pct,
        ls_ratio_delta=ls_now - ls_prev,
        funding_rate=funding_rate,
    )

    return {
        "change_24h_pct": price_change_pct,
        "change_72h_pct": price_change_pct * 1.5,  # approximation
        "change_7d_pct": price_change_pct * 3.0,  # approximation
        "volume_24h_mult": volume_24h_mult,
        "volume_72h_mult": volume_24h_mult,
        "oi_24h_pct": oi_24h_pct,
        "oi_72h_pct": oi_24h_pct * 1.5,
        "funding_rate": funding_rate,
        "ls_ratio_now": ls_now,
        "ls_ratio_prev_24h": ls_prev,
        "venues_180m": venues,
        "events_180m": events,
        "drawdown_from_24h_high_pct": drawdown,
        "range_position_24h_pct": ((last_price - low) / (high - low) * 100) if high > low else 50.0,
        "high_24h": high,
        "low_24h": low,
        "oi_value": oi_value,
        "quote_volume_24h": quote_volume,
        "last_price": last_price,
        "market_cap_usd": quote_volume,  # 使用24h交易量作为市值代理，避免UNKNOWN
        "klines_1h": normalized_klines_1h,
    }


def scan_symbols(symbols: list[str], min_stage: str | None = None, max_workers: int = 10) -> list[SymbolBreakoutResult]:
    """Scan multiple symbols and return breakout results.
    
    Two-stage scanning for efficiency:
    1. Fast filter: Single API call gets all 24h tickers
    2. Deep scan: Parallel API calls for OI/LS/Funding on candidates only
    
    Args:
        symbols: List of symbols to scan (e.g., ['BTCUSDT', 'ETHUSDT'])
        min_stage: If set, filter to only return results at or above this stage
                   Order: neutral < pre_break < confirmed_breakout < mania/exhaustion
        max_workers: Max concurrent threads for API calls (default 3 to avoid rate limits)
    
    Returns:
        List of SymbolBreakoutResult for symbols matching criteria.
    """
    blocked_symbols = _tradifi_symbol_set()
    symbols = [symbol for symbol in symbols if str(symbol or "").upper() not in blocked_symbols]

    stage_priority = {
        "neutral": 0,
        "pre_break": 1,
        "confirmed_breakout": 2,
        "exhaustion": 3,
        "mania": 3,
    }

    results = []
    
    def scan_single(symbol: str) -> SymbolBreakoutResult | None:
        """Scan a single symbol with retry logic."""
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                metrics = build_symbol_metrics(symbol)
                if metrics is None:
                    # Symbol not tradable (delisted/unavailable)
                    return None
                
                stage, direction, trigger, risk = classify_and_direction(metrics)

                if min_stage and stage_priority.get(stage, 0) < stage_priority.get(min_stage, 0):
                    return None  # Filtered out by stage

                return SymbolBreakoutResult(
                    symbol=symbol,
                    stage=stage,
                    direction=direction,
                    trigger=trigger,
                    risk=risk,
                    metrics=metrics,
                )
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(0.2 * (attempt + 1))  # Fast retry
                    continue
                logger.debug(f"Scan failed for {symbol}: {e}")
                return None  # Skip problematic symbols silently
        return None

    # Parallel scan with thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(scan_single, symbol): symbol for symbol in symbols}
        
        for future in as_completed(future_to_symbol):
            result = future.result()
            if result is not None:  # Skip filtered-out symbols
                results.append(result)

    # Sort by stage priority (highest first) then by |change|%
    results.sort(
        key=lambda r: (
            -stage_priority.get(r.stage, 0),
            -abs(r.metrics.get('change_24h_pct', 0)),
        )
    )

    return results


def get_top_symbols_by_volume(limit: int = 20) -> list[str]:
    """Get top N symbols by 24h quote volume."""
    ticker = fetch_ticker_24hr()  # all symbols
    if isinstance(ticker, dict):
        ticker = [ticker]
    blocked_symbols = _tradifi_symbol_set()

    sorted_tickers = sorted(
        [item for item in ticker if str(item.get("symbol", "") or "").upper() not in blocked_symbols],
        key=lambda x: float(x.get("quoteVolume", 0)),
        reverse=True,
    )[:limit]

    return [t["symbol"] for t in sorted_tickers if t.get("symbol")]


def get_top_symbols_by_change(limit: int | None = None, min_change: float = 3.0) -> list[str]:
    """Get top N symbols by 24h price change (gainers + losers).
    
    This is the key function for finding 妖币 (anomaly coins) - 
    sorted by absolute price change to catch breakouts early.
    
    Two-stage filtering for efficiency:
    1. Fast filter: Get all symbols via single API call, filter by |change|%
    2. Deep scan: Only scan filtered candidates with OI/LS/Funding APIs
    
    Args:
        limit: Max symbols to return (None = no limit, return all matching min_change)
        min_change: Minimum absolute change % to filter (default 3%)
    
    Returns:
        List of symbols sorted by |change|% descending
    """
    ticker = fetch_ticker_24hr()  # all symbols in ONE API call
    if isinstance(ticker, dict):
        ticker = [ticker]
    blocked_symbols = _tradifi_symbol_set()
    
    # Filter: USDT perpetuals only, exclude stablecoins and leveraged tokens
    filtered = []
    exclude_patterns = ['USDC', 'FDUSD', 'TUSD', 'UP', 'DOWN', 'BULL', 'BEAR']
    for t in ticker:
        symbol = t.get("symbol", "")
        if not symbol.endswith('USDT'):
            continue
        if symbol.upper() in blocked_symbols:
            continue
        if any(p in symbol for p in exclude_patterns):
            continue
        change = abs(float(t.get("priceChangePercent", 0)))
        if change >= min_change:
            filtered.append(t)
    
    # Sort by absolute change descending
    sorted_tickers = sorted(
        filtered,
        key=lambda x: abs(float(x.get("priceChangePercent", 0))),
        reverse=True,
    )
    
    # Apply limit if specified (None = return all matching)
    if limit is not None:
        sorted_tickers = sorted_tickers[:limit]
    
    return [t["symbol"] for t in sorted_tickers if t.get("symbol")]


def main():
    """CLI entrypoint for binance-scanner."""
    import argparse

    parser = argparse.ArgumentParser(description="Binance breakout scanner")
    parser.add_argument("--symbols", "-s", nargs="+", help="Specific symbols to scan")
    parser.add_argument("--top", "-t", type=int, default=10, help="Scan top N symbols by volume")
    parser.add_argument("--by-change", action="store_true", help="Scan top N symbols by 24h price change (妖币 mode)")
    parser.add_argument("--min-change", type=float, default=3.0, help="Minimum absolute change % for --by-change mode")
    parser.add_argument("--max-workers", type=int, default=6, help="Deep scan concurrency (default: 6)")
    parser.add_argument("--min-stage", choices=["pre_break", "confirmed_breakout", "mania", "exhaustion"],
                        help="Filter to minimum stage")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    elif args.by_change:
        symbols = get_top_symbols_by_change(args.top, min_change=args.min_change)
    else:
        symbols = get_top_symbols_by_volume(args.top)

    results = scan_symbols(symbols, min_stage=args.min_stage, max_workers=max(1, args.max_workers))

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        for r in results:
            print(f"\n{'='*60}")
            print(f"Symbol: {r.symbol}")
            print(f"Stage: {r.stage}")
            print(f"Direction: {r.direction}")
            print(f"Trigger: {r.trigger}")
            print(f"Risk: {r.risk}")
            if r.metrics:
                print(f"Price: ${r.metrics.get('last_price', 'N/A')}")
                print(f"24h Change: {r.metrics.get('change_24h_pct', 0):.2f}%")
                print(f"OI 24h: {r.metrics.get('oi_24h_pct', 0):.2f}%")
                print(f"Funding: {r.metrics.get('funding_rate', 0):.6f}")
                print(f"L/S Ratio: {r.metrics.get('ls_ratio_now', 0):.2f}")


if __name__ == "__main__":
    main()
