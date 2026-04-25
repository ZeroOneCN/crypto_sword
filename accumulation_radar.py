#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║       🏦 ACCUMULATION RADAR - 庄家收筹雷达 🏦                 ║
║                                                               ║
║    OI异动检测 + 横盘收筹分析 + 空头燃料扫描 + 三策略评分       ║
║                                                               ║
║    整合自：https://github.com/connectfarm1/accumulation-radar ║
║    已集成到 crypto_sword.py 主程序，独立运行仅用于调试         ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

核心功能：
- scan_accumulation_pool(): 全市场扫描收筹标的池
- scan_oi_changes(): OI异动监控 + 暗流信号检测
- scan_short_fuel(): 空头燃料（负费率+价格上涨）
- calculate_strategy_scores(): 三策略独立评分

使用方式：
1. 独立运行：python accumulation_radar.py pool  # 每日更新收筹池
2. 独立运行：python accumulation_radar.py oi    # 每小时OI监控
3. 被 crypto_sword.py 调用：提供额外评分维度（自动集成）
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 数据结构 - 雷达信号
# ═══════════════════════════════════════════════════════════════

@dataclass
class AccumulationSignal:
    """收筹信号"""
    symbol: str
    sideways_days: int = 0          # 横盘天数
    price_range_pct: float = 0.0    # 价格波动范围 %
    avg_volume_usd: float = 0.0     # 日均成交量
    market_cap_usd: float = 0.0     # 真实流通市值
    in_pool: bool = False           # 是否在收筹池
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "sideways_days": self.sideways_days,
            "price_range_pct": self.price_range_pct,
            "avg_volume_usd": self.avg_volume_usd,
            "market_cap_usd": self.market_cap_usd,
            "in_pool": self.in_pool,
        }


@dataclass
class OIChangeSignal:
    """OI异动信号"""
    symbol: str
    oi_current: float = 0.0         # 当前OI
    oi_previous: float = 0.0        # 前期OI
    oi_change_pct: float = 0.0      # OI变化 %
    price_change_pct: float = 0.0   # 价格变化 %
    funding_rate: float = 0.0       # 资金费率
    
    # 暗流信号：OI变但价没动（最佳埋伏时机）
    is_dark_flow: bool = False
    dark_flow_score: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "oi_current": self.oi_current,
            "oi_previous": self.oi_previous,
            "oi_change_pct": self.oi_change_pct,
            "price_change_pct": self.price_change_pct,
            "funding_rate": self.funding_rate,
            "is_dark_flow": self.is_dark_flow,
            "dark_flow_score": self.dark_flow_score,
        }


@dataclass  
class StrategyScore:
    """三策略评分"""
    symbol: str
    
    # 🔥 追多策略（短线轧空）
    chase_score: float = 0.0
    chase_rank: int = 0
    
    # 📊 综合策略（四维均衡）
    composite_score: float = 0.0
    composite_rank: int = 0
    
    # 🎯 埯伏策略（中长线布局）
    ambush_score: float = 0.0
    ambush_rank: int = 0
    
    # 各维度分数
    funding_score: float = 0.0      # 费率分
    market_cap_score: float = 0.0   # 市值分
    sideways_score: float = 0.0     # 横盘分
    oi_score: float = 0.0           # OI分
    
    # 特殊标记
    has_negative_funding: bool = False
    is_accelerating: bool = False   # 费率加速恶化
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "chase_score": self.chase_score,
            "chase_rank": self.chase_rank,
            "composite_score": self.composite_score,
            "composite_rank": self.composite_rank,
            "ambush_score": self.ambush_score,
            "ambush_rank": self.ambush_rank,
            "funding_score": self.funding_score,
            "market_cap_score": self.market_cap_score,
            "sideways_score": self.sideways_score,
            "oi_score": self.oi_score,
            "has_negative_funding": self.has_negative_funding,
            "is_accelerating": self.is_accelerating,
        }


# ═══════════════════════════════════════════════════════════════
# API 封装 - 币安合约数据获取
# ═══════════════════════════════════════════════════════════════

def _run_binance_cli(args: List[str]) -> Dict[str, Any] | List[Any]:
    """运行 binance-cli 获取合约数据"""
    cmd = ["binance-cli", "futures-usds"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.warning(f"binance-cli 失败: {result.stderr}")
            return {}
        return json.loads(result.stdout.strip() or "{}")
    except Exception as e:
        logger.error(f"API 请求失败: {e}")
        return {}


def get_all_perp_symbols() -> List[str]:
    """获取所有 USDT 永续合约列表"""
    data = _run_binance_cli(["exchange-info"])
    if isinstance(data, dict) and "symbols" in data:
        return [s["symbol"] for s in data["symbols"] if s.get("contractType") == "PERPETUAL"]
    return []


def get_market_caps() -> Dict[str, float]:
    """获取真实流通市值（三级 fallback）
    
    1. 币安现货 API（最准确）
    2. 合约 OI 接口的 CMC 流通量 × 价格
    3. 粗估公式
    """
    market_caps = {}
    
    # 尝试币安现货 API（需要单独请求）
    try:
        import urllib.request
        url = "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("success"):
                for item in data.get("data", []):
                    symbol = item.get("s", "")
                    cap = float(item.get("cs", 0)) * float(item.get("p", 0))  # 流通量 × 价格
                    if symbol and cap > 0:
                        # 转换为合约格式 (BTC -> BTCUSDT)
                        perp_symbol = symbol + "USDT"
                        market_caps[perp_symbol] = cap
                logger.info(f"✅ 获取市值数据: {len(market_caps)} 个币种")
    except Exception as e:
        logger.warning(f"市值 API 失败，使用估算值: {e}")
    
    return market_caps


def get_oi_history(symbol: str, days: int = 7) -> List[Dict[str, Any]]:
    """获取 OI 历史数据"""
    data = _run_binance_cli([
        "open-interest-hist",
        "--symbol", symbol,
        "--period", "1d",
        "--limit", str(days)
    ])
    if isinstance(data, list):
        return data
    return []


def get_funding_rates() -> Dict[str, float]:
    """获取所有币种资金费率"""
    data = _run_binance_cli(["premium-index"])
    rates = {}
    if isinstance(data, list):
        for item in data:
            symbol = item.get("symbol", "")
            rate = float(item.get("lastFundingRate", 0))
            rates[symbol] = rate
    return rates


def get_ticker_24hr(symbol: str = None) -> Dict[str, Any] | List[Any]:
    """获取 24h 行情数据"""
    if symbol:
        data = _run_binance_cli(["ticker-24hr", "--symbol", symbol])
    else:
        data = _run_binance_cli(["ticker-24hr"])
    return data or {}


def get_klines(symbol: str, interval: str = "1d", limit: int = 120) -> List[Dict[str, Any]]:
    """获取历史 K 线"""
    data = _run_binance_cli(["klines", "--symbol", symbol, "--interval", interval, "--limit", str(limit)])
    if isinstance(data, list):
        return data
    return []


# ═══════════════════════════════════════════════════════════════
# 收筹池扫描 - 横盘分析
# ═══════════════════════════════════════════════════════════════

# 收筹池参数
MIN_SIDEWAYS_DAYS = 45           # 最少横盘天数
MAX_RANGE_PCT = 80.0             # 价格波动阈值 %
MAX_AVG_VOL_USD = 20_000_000     # 日均成交量上限


def calculate_sideways_days(symbol: str, klines: List[Dict[str, Any]]) -> Tuple[int, float, float]:
    """计算横盘天数
    
    返回: (横盘天数, 价格波动范围%, 日均成交量USD)
    """
    if not klines or len(klines) < 30:
        return 0, 0.0, 0.0
    
    # 解析 K 线数据
    highs = []
    lows = []
    volumes = []
    
    for k in klines:
        try:
            high = float(k.get("high", 0) or k[2] if isinstance(k, list) else 0)
            low = float(k.get("low", 0) or k[3] if isinstance(k, list) else 0)
            vol = float(k.get("volume", 0) or k[5] if isinstance(k, list) else 0)
            if high > 0:
                highs.append(high)
                lows.append(low)
                volumes.append(vol)
        except:
            continue
    
    if not highs:
        return 0, 0.0, 0.0
    
    # 计算价格范围
    max_high = max(highs)
    min_low = min(lows)
    avg_price = (max_high + min_low) / 2
    range_pct = (max_high - min_low) / avg_price * 100 if avg_price > 0 else 0
    
    # 计算日均成交量（USD）
    avg_vol = sum(volumes) / len(volumes) * avg_price if volumes else 0
    
    # 计算横盘天数（连续在范围内）
    sideways_days = 0
    threshold_pct = MAX_RANGE_PCT / 100
    
    for i in range(len(highs) - 1, -1, -1):
        local_range = (highs[i] - lows[i]) / avg_price if avg_price > 0 else 0
        if local_range < threshold_pct:
            sideways_days += 1
        else:
            break
    
    return sideways_days, range_pct, avg_vol


def scan_accumulation_pool(symbols: List[str] = None) -> List[AccumulationSignal]:
    """全市场扫描收筹标的池
    
    条件：
    - 横盘天数 >= 45天
    - 价格波动 < 80%
    - 日均成交量 < $20M（低调收筹）
    """
    if not symbols:
        symbols = get_all_perp_symbols()
    
    logger.info(f"🔍 开始扫描收筹池: {len(symbols)} 个币种")
    
    # 获取市值数据
    market_caps = get_market_caps()
    
    pool = []
    
    for symbol in symbols[:100]:  # 限制数量避免API限流
        try:
            klines = get_klines(symbol, "1d", 120)
            sideways_days, range_pct, avg_vol = calculate_sideways_days(symbol, klines)
            
            if sideways_days >= MIN_SIDEWAYS_DAYS and range_pct <= MAX_RANGE_PCT:
                signal = AccumulationSignal(
                    symbol=symbol,
                    sideways_days=sideways_days,
                    price_range_pct=range_pct,
                    avg_volume_usd=avg_vol,
                    market_cap_usd=market_caps.get(symbol, 0),
                    in_pool=True,
                )
                pool.append(signal)
                logger.info(f"✅ {symbol} 收筹: {sideways_days}天, 波动{range_pct:.1f}%, 日均${avg_vol/1e6:.2f}M")
            
            time.sleep(0.3)  # API限流
            
        except Exception as e:
            logger.debug(f"{symbol} 扫描失败: {e}")
            continue
    
    logger.info(f"🏦 收筹池更新完成: {len(pool)} 个标的")
    return pool


# ═══════════════════════════════════════════════════════════════
# OI 异动监控 - 暗流信号检测
# ═══════════════════════════════════════════════════════════════

MIN_OI_DELTA_PCT = 3.0           # OI变化阈值 %
MIN_OI_USD = 2_000_000           # 最低OI门槛


def detect_dark_flow(oi_change_pct: float, price_change_pct: float) -> Tuple[bool, float]:
    """检测暗流信号
    
    暗流定义：OI 变化 >= 3%，但价格变化 < 1%
    这是最经典的庄家收筹信号：大资金进场但价格不动
    
    返回: (是否暗流, 暗流评分)
    """
    is_dark = abs(oi_change_pct) >= MIN_OI_DELTA_PCT and abs(price_change_pct) < 1.0
    
    if is_dark:
        # 暗流评分：OI 变化越大，评分越高
        score = min(abs(oi_change_pct) / 10 * 100, 100)  # 10% OI变化 = 100分
    else:
        score = 0.0
    
    return is_dark, score


def scan_oi_changes(pool_symbols: List[str] = None) -> List[OIChangeSignal]:
    """扫描 OI 异动
    
    参数：
    - pool_symbols: 仅监控收筹池内标的（可选）
    
    返回：OI 异动信号列表，按变化幅度排序
    """
    if not pool_symbols:
        pool_symbols = get_all_perp_symbols()
    
    logger.info(f"⚡ 开始 OI 异动扫描: {len(pool_symbols)} 个币种")
    
    # 获取资金费率
    funding_rates = get_funding_rates()
    
    signals = []
    
    for symbol in pool_symbols[:50]:  # 限制数量
        try:
            # 获取 OI 历史
            oi_hist = get_oi_history(symbol, 2)
            if len(oi_hist) < 2:
                continue
            
            # 计算 OI 变化
            oi_current = float(oi_hist[-1].get("openInterest", 0) or oi_hist[-1].get("sumOpenInterest", 0))
            oi_previous = float(oi_hist[-2].get("openInterest", 0) or oi_hist[-2].get("sumOpenInterest", 0))
            
            if oi_current < MIN_OI_USD:
                continue
            
            oi_change_pct = (oi_current - oi_previous) / oi_previous * 100 if oi_previous > 0 else 0
            
            # 获取价格变化
            ticker = get_ticker_24hr(symbol)
            price_change_pct = float(ticker.get("priceChangePercent", 0)) if isinstance(ticker, dict) else 0
            
            # 检测暗流信号
            is_dark, dark_score = detect_dark_flow(oi_change_pct, price_change_pct)
            
            if abs(oi_change_pct) >= MIN_OI_DELTA_PCT:
                signal = OIChangeSignal(
                    symbol=symbol,
                    oi_current=oi_current,
                    oi_previous=oi_previous,
                    oi_change_pct=oi_change_pct,
                    price_change_pct=price_change_pct,
                    funding_rate=funding_rates.get(symbol, 0),
                    is_dark_flow=is_dark,
                    dark_flow_score=dark_score,
                )
                signals.append(signal)
                
                if is_dark:
                    logger.info(f"🎯 {symbol} 暗流信号！OI {oi_change_pct:+.1f}% 但价格 {price_change_pct:+.1f}%")
            
            time.sleep(0.5)
            
        except Exception as e:
            logger.debug(f"{symbol} OI扫描失败: {e}")
            continue
    
    # 按OI变化排序
    signals.sort(key=lambda s: abs(s.oi_change_pct), reverse=True)
    
    logger.info(f"⚡ OI异动检测完成: {len(signals)} 个信号")
    return signals


# ═══════════════════════════════════════════════════════════════
# 空头燃料扫描 - 负费率 + 价格上涨
# ═══════════════════════════════════════════════════════════════

MIN_NEGATIVE_FUNDING = -0.005    # 最低负费率阈值
MIN_PRICE_CHANGE_PCT = 3.0       # 最低价格涨幅
MIN_VOLUME_USD = 1_000_000       # 最低成交额


def scan_short_fuel(symbols: List[str] = None) -> List[Dict[str, Any]]:
    """扫描空头燃料
    
    条件：
    - 资金费率 < -0.005%
    - 价格涨幅 > 3%
    - 成交额 > $1M
    
    空头燃料理论：
    涨完必须有人做空才有燃料继续拉
    费率越负 = 做空人越多 = 空头燃料越多
    """
    if not symbols:
        symbols = get_all_perp_symbols()
    
    logger.info(f"🔥 开始空头燃料扫描")
    
    funding_rates = get_funding_rates()
    tickers = get_ticker_24hr()
    
    if not isinstance(tickers, list):
        tickers = []
    
    fuel_signals = []
    
    for ticker in tickers:
        try:
            symbol = ticker.get("symbol", "")
            if symbol not in funding_rates:
                continue
            
            rate = funding_rates[symbol]
            price_change = float(ticker.get("priceChangePercent", 0))
            volume = float(ticker.get("volume", 0)) * float(ticker.get("lastPrice", 0))
            
            # 检查空头燃料条件
            if rate < MIN_NEGATIVE_FUNDING and price_change > MIN_PRICE_CHANGE_PCT and volume > MIN_VOLUME_USD:
                fuel_signals.append({
                    "symbol": symbol,
                    "funding_rate": rate,
                    "price_change_pct": price_change,
                    "volume_usd": volume,
                    "rank": len(fuel_signals) + 1,
                })
                
        except Exception as e:
            continue
    
    # 按费率排序（越负排名越高）
    fuel_signals.sort(key=lambda s: s["funding_rate"])
    
    for i, s in enumerate(fuel_signals):
        s["rank"] = i + 1
    
    logger.info(f"🔥 空头燃料: {len(fuel_signals)} 个标的")
    return fuel_signals


# ═══════════════════════════════════════════════════════════════
# 三策略评分体系
# ═══════════════════════════════════════════════════════════════

def calculate_strategy_scores(
    symbol: str,
    funding_rate: float,
    market_cap: float,
    sideways_days: int,
    oi_change_pct: float,
    price_change_pct: float,
    volume_usd: float,
    in_pool: bool = False,
) -> StrategyScore:
    """计算三策略评分
    
    🔥 追多策略（短线轧空）：
    - 前提：涨>3% + 费率< -0.005% + Vol>$1M
    - 评分：按费率负值排名
    
    📊 综合策略（四维均衡，各25分=100分）：
    - 费率分(25): 越负越好
    - 市值分(25): <$50M满分
    - 横盘分(25): ≥120天满分
    - OI分(25): ≥15%变化满分
    
    🎯 埯伏策略（市值>OI>横盘>费率）：
    - 前提：必须在收筹池内 + 涨幅<50%
    - 市值(35分): <50M满分
    - OI异动(30分): ≥10%满分
    - 横盘(20分): ≥120天满分
    - 负费率(15分): bonus加分
    """
    score = StrategyScore(symbol=symbol)
    
    # 1. 🔥 追多策略评分
    if price_change_pct > MIN_PRICE_CHANGE_PCT and funding_rate < MIN_NEGATIVE_FUNDING and volume_usd > MIN_VOLUME_USD:
        # 费率越负，分数越高（-0.1% = 100分）
        score.chase_score = min(abs(funding_rate) * 1000, 100)
        score.has_negative_funding = True
    
    # 2. 各维度分数（用于综合和埋伏）
    # 费率分（越负越好）
    if funding_rate < 0:
        score.funding_score = min(abs(funding_rate) * 100, 25)  # -0.25% = 25分
        score.has_negative_funding = True
    
    # 市值分（越低越好）
    if market_cap > 0:
        if market_cap < 50_000_000:
            score.market_cap_score = 25  # <50M满分
        elif market_cap < 100_000_000:
            score.market_cap_score = 15
        else:
            score.market_cap_score = 5
    
    # 横盘分（越久越好）
    if sideways_days >= 120:
        score.sideways_score = 25
    elif sideways_days >= 45:
        score.sideways_score = 15
    else:
        score.sideways_score = min(sideways_days / 120 * 25, 25)
    
    # OI分（变化越大越好）
    if abs(oi_change_pct) >= 15:
        score.oi_score = 25
    elif abs(oi_change_pct) >= 3:
        score.oi_score = min(abs(oi_change_pct) / 15 * 25, 25)
    else:
        score.oi_score = 0
    
    # 3. 📊 综合策略评分（四维均衡）
    score.composite_score = score.funding_score + score.market_cap_score + score.sideways_score + score.oi_score
    
    # 4. 🎯 埯伏策略评分（仅限收筹池内）
    if in_pool and price_change_pct < 50:
        # 市值权重35%
        ambush_cap = score.market_cap_score * 35 / 25 if score.market_cap_score > 0 else 0
        
        # OI权重30%
        ambush_oi = min(abs(oi_change_pct) / 10 * 30, 30) if abs(oi_change_pct) >= 3 else 0
        
        # 横盘权重20%
        ambush_sideways = min(sideways_days / 120 * 20, 20)
        
        # 费率权重15%（bonus）
        ambush_funding = score.funding_score * 15 / 25 if score.funding_score > 0 else 0
        
        score.ambush_score = ambush_cap + ambush_oi + ambush_sideways + ambush_funding
    
    return score


# ═══════════════════════════════════════════════════════════════
# 主函数 - CLI 入口
# ═══════════════════════════════════════════════════════════════

def main():
    """CLI 入口
    
    命令：
    - pool: 全市场扫描收筹池（每日1次）
    - oi: OI异动监控（每小时1次）
    - full: 全量运行
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="🏦 庄家收筹雷达")
    parser.add_argument("command", choices=["pool", "oi", "full"], help="运行模式")
    parser.add_argument("--notify", action="store_true", help="发送 Telegram 通知")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    
    if args.command == "pool":
        pool = scan_accumulation_pool()
        print(f"\n🏦 收筹池更新完成: {len(pool)} 个标的")
        for p in pool[:10]:
            print(f"  {p.symbol}: {p.sideways_days}天, 波动{p.price_range_pct:.1f}%, 市值${p.market_cap_usd/1e6:.1f}M")
    
    elif args.command == "oi":
        signals = scan_oi_changes()
        print(f"\n⚡ OI异动检测完成: {len(signals)} 个信号")
        for s in signals[:10]:
            dark_mark = "🎯 暗流！" if s.is_dark_flow else ""
            print(f"  {s.symbol}: OI {s.oi_change_pct:+.1f}%, 价 {s.price_change_pct:+.1f}%, 费率{s.funding_rate:.4f}% {dark_mark}")
    
    elif args.command == "full":
        pool = scan_accumulation_pool()
        oi_signals = scan_oi_changes([p.symbol for p in pool])
        fuel = scan_short_fuel()
        
        print(f"\n🏦 收筹池: {len(pool)} 个")
        print(f"⚡ OI异动: {len(oi_signals)} 个")
        print(f"🔥 空头燃料: {len(fuel)} 个")


if __name__ == "__main__":
    main()