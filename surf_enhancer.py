#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║         🌊 DATA ENHANCER - Surf 数据增强模块 🌊               ║
║                                                               ║
║    集成 Surf 技能：链上数据 + 社交情绪 + 巨鲸动向              ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""

import subprocess
import json
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Surf CLI 封装 - 赫尔墨斯的信使
# ═══════════════════════════════════════════════════════════════

def run_surf_command(args: List[str], timeout: int = 30) -> Optional[Dict[str, Any]]:
    """运行 Surf 命令并解析 JSON 输出"""
    try:
        cmd = ["surf"] + args + ["--json"]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode != 0:
            logger.warning(f"Surf 命令失败：{' '.join(cmd)} - {result.stderr[:200]}")
            return None
        
        # 解析 JSON
        try:
            data = json.loads(result.stdout)
            if "error" in data:
                logger.warning(f"Surf API 错误：{data['error'].get('message', 'Unknown')}")
                return None
            return data.get("data")
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败：{e}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error(f"Surf 命令超时：{' '.join(cmd)}")
        return None
    except Exception as e:
        logger.error(f"Surf 命令异常：{e}")
        return None


# ═══════════════════════════════════════════════════════════════
# 市场数据 - 波塞冬的海底宝藏
# ═══════════════════════════════════════════════════════════════

def get_market_ranking(limit: int = 50, sort_by: str = "volume_24h") -> List[Dict[str, Any]]:
    """
    获取市场排名数据
    
    Args:
        limit: 返回数量 (默认 50)
        sort_by: 排序方式 (market_cap / change_24h / volume_24h)
    
    Returns:
        币种排名列表
    """
    data = run_surf_command([
        "market-ranking",
        "--sort-by", sort_by,
        "--limit", str(limit)
    ])
    
    if not data:
        return []
    
    # 标准化字段
    results = []
    for item in data if isinstance(data, list) else []:
        results.append({
            "symbol": item.get("symbol", ""),
            "name": item.get("name", ""),
            "price_usd": item.get("price_usd", 0),
            "market_cap_usd": item.get("market_cap_usd", 0),
            "volume_24h_usd": item.get("volume_24h_usd", 0),
            "change_24h_pct": item.get("change_24h_pct", 0),
            "rank": item.get("rank", 0),
        })
    
    return results


def _fg_classification_zh(classification: str) -> str:
    """将恐惧贪婪指数的英文分类转换为中文"""
    mapping = {
        "Extreme Greed": "极度贪婪",
        "Greed": "贪婪",
        "Neutral": "中性",
        "Fear": "恐惧",
        "Extreme Fear": "极度恐惧",
    }
    return mapping.get(classification, classification)


def get_fear_greed_index() -> Optional[Dict[str, Any]]:
    """
    获取恐慌贪婪指数
    
    Returns:
        {
            "value": 50,
            "classification": "中性",
            "timestamp": "2024-01-01"
        }
    """
    # 尝试 Surf API（优先）
    try:
        data = run_surf_command(["market-fear-greed"])
        
        if data and isinstance(data, list) and len(data) > 0:
            latest = data[0]
            return {
                "value": latest.get("value", 0),
                "classification": _fg_classification_zh(latest.get("classification", "中性")),
                "timestamp": latest.get("timestamp", ""),
            }
    except Exception:
        pass
    
    # 备用：使用 alternative.me 免费 API
    try:
        import urllib.request
        url = "https://api.alternative.me/fng/"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            # API 返回格式：{'name': 'Fear and Greed Index', 'data': [...], 'metadata': {...}}
            if data.get("data") and len(data["data"]) > 0:
                latest = data["data"][0]
                value = int(latest.get("value", 50))
                
                # 分类映射
                if value >= 75:
                    classification = "极度贪婪"
                elif value >= 56:
                    classification = "贪婪"
                elif value >= 45:
                    classification = "中性"
                elif value >= 25:
                    classification = "恐惧"
                else:
                    classification = "极度恐惧"
                
                return {
                    "value": value,
                    "classification": classification,
                    "timestamp": latest.get("timestamp", ""),
                }
    except Exception:
        pass
    
    # 最后返回默认值
    return {
        "value": 50,
        "classification": "中性",
        "timestamp": "",
    }


def get_liquidation_data(symbol: str = "BTC", limit: int = 10) -> List[Dict[str, Any]]:
    """
    获取清算数据
    
    Args:
        symbol: 币种符号
        limit: 返回数量
    
    Returns:
        清算记录列表
    """
    data = run_surf_command([
        "market-liquidation-chart",
        "--symbol", symbol,
        "--limit", str(limit)
    ])
    
    if not data:
        return []
    
    return data if isinstance(data, list) else []


# ═══════════════════════════════════════════════════════════════
# 社交情绪 - 阿波罗的预言
# ═══════════════════════════════════════════════════════════════

def get_social_mindshare(limit: int = 20) -> List[Dict[str, Any]]:
    """
    获取社交媒体关注度排名
    
    Returns:
        币种社交热度排名
    """
    data = run_surf_command([
        "social-mindshare-ranking",
        "--limit", str(limit)
    ])
    
    if not data:
        return []
    
    results = []
    for item in data if isinstance(data, list) else []:
        results.append({
            "symbol": item.get("symbol", ""),
            "name": item.get("name", ""),
            "mindshare_volume": item.get("mindshare_volume", 0),
            "mindshare_change_pct": item.get("mindshare_change_pct", 0),
            "sentiment_score": item.get("sentiment_score", 0),
            "rank": item.get("rank", 0),
        })
    
    return results


def get_social_sentiment(symbol: str) -> Optional[Dict[str, Any]]:
    """
    获取特定币种的情绪分析
    
    Args:
        symbol: 币种符号
    
    Returns:
        情绪指标
    """
    # 尝试获取该币种的社交数据
    data = run_surf_command([
        "social-token",
        "--symbol", symbol
    ])
    
    if not data:
        return None
    
    return {
        "symbol": symbol,
        "sentiment_score": data.get("sentiment_score", 0),
        "social_volume": data.get("social_volume", 0),
        "social_dominance": data.get("social_dominance", 0),
    }


# ═══════════════════════════════════════════════════════════════
# 链上数据 - 雅典娜的智慧
# ═══════════════════════════════════════════════════════════════

def get_onchain_indicator(symbol: str = "BTC", metric: str = "nupl") -> Optional[Dict[str, Any]]:
    """
    获取链上指标
    
    Args:
        symbol: 币种符号（目前主要支持 BTC）
        metric: 指标类型 (nupl / mvrv / sopr / puell-multiple)
    
    Returns:
        链上指标数据
    """
    data = run_surf_command([
        "market-onchain-indicator",
        "--symbol", symbol,
        "--metric", metric
    ])
    
    if not data:
        return None
    
    return {
        "symbol": symbol,
        "metric": metric,
        "value": data.get("value", 0) if isinstance(data, dict) else None,
        "timestamp": data.get("timestamp", ""),
    }


def get_whale_activity(symbol: str = "BTC", limit: int = 10) -> List[Dict[str, Any]]:
    """
    获取巨鲸活动数据（大额转账）
    
    注意：这需要通过 onchain-sql 查询，较为复杂
    这里使用简化的方法
    """
    # TODO: 实现 onchain-sql 查询巨鲸转账
    # 需要查询 catalog 找到合适的表
    return []


# ═══════════════════════════════════════════════════════════════
# 预测市场 - 摩伊拉的命运
# ═══════════════════════════════════════════════════════════════

def get_polymarket_crypto_sentiment() -> Optional[Dict[str, Any]]:
    """
    获取 Polymarket 加密市场情绪
    
    Returns:
        预测市场数据
    """
    data = run_surf_command([
        "polymarket-markets",
        "--limit", "10"
    ])
    
    if not data:
        return None
    
    # 筛选加密相关市场
    crypto_markets = []
    for market in data if isinstance(data, list) else []:
        title = market.get("title", "").lower()
        if any(kw in title for kw in ["bitcoin", "btc", "eth", "crypto", "price"]):
            crypto_markets.append({
                "title": market.get("title", ""),
                "yes_bid": market.get("yes_bid", 0),
                "no_bid": market.get("no_bid", 0),
                "volume": market.get("volume", 0),
            })
    
    return {
        "markets": crypto_markets[:5],
        "timestamp": "",
    }


# ═══════════════════════════════════════════════════════════════
# 综合数据增强 - 众神的恩赐
# ═══════════════════════════════════════════════════════════════

def enhance_symbol_data(symbol: str) -> Dict[str, Any]:
    """
    为单个币种获取增强数据
    
    Args:
        symbol: 币种符号
    
    Returns:
        综合数据字典
    """
    enhanced = {
        "symbol": symbol,
        "market_data": {},
        "social_data": {},
        "onchain_data": {},
        "prediction_data": {},
    }
    
    # 1. 市场数据
    ranking_data = get_market_ranking(limit=100, sort_by="volume_24h")
    for item in ranking_data:
        if item.get("symbol") == symbol:
            enhanced["market_data"] = item
            break
    
    # 2. 社交情绪
    social_data = get_social_sentiment(symbol)
    if social_data:
        enhanced["social_data"] = social_data
    
    # 3. 链上数据（仅 BTC 支持较好）
    if symbol == "BTC":
        nupl = get_onchain_indicator("BTC", "nupl")
        if nupl:
            enhanced["onchain_data"]["nupl"] = nupl
        
        mvrv = get_onchain_indicator("BTC", "mvrv")
        if mvrv:
            enhanced["onchain_data"]["mvrv"] = mvrv
    
    # 4. 预测市场
    pred_data = get_polymarket_crypto_sentiment()
    if pred_data:
        enhanced["prediction_data"] = pred_data
    
    return enhanced


def get_market_overview() -> Dict[str, Any]:
    """
    获取市场概览 - 用于扫描前快速判断市场环境
    
    Returns:
        市场环境指标
    """
    overview = {
        "fear_greed": None,
        "top_gainers": [],
        "top_losers": [],
        "liquidation_risk": "低",
        "market_sentiment": "中性",
    }
    
    # 恐慌贪婪指数
    fg = get_fear_greed_index()
    if fg:
        overview["fear_greed"] = fg
        
        # 判断市场情绪
        value = fg.get("value", 50)
        if value >= 75:
            overview["market_sentiment"] = "贪婪"
        elif value <= 25:
            overview["market_sentiment"] = "恐惧"
    
    # 涨跌幅排名
    gainers = get_market_ranking(limit=20, sort_by="change_24h")
    if gainers:
        overview["top_gainers"] = gainers[:10]
        overview["top_losers"] = list(reversed(gainers[:10]))
    
    # 清算风险评估（简化）
    if overview["fear_greed"]:
        fg_value = overview["fear_greed"].get("value", 50)
        if fg_value >= 80 or fg_value <= 20:
            overview["liquidation_risk"] = "高"
        elif fg_value >= 70 or fg_value <= 30:
            overview["liquidation_risk"] = "中"
    
    return overview


# ═══════════════════════════════════════════════════════════════
# CLI 测试入口
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="🌊 Surf 数据增强模块")
    parser.add_argument("--overview", action="store_true", help="获取市场概览")
    parser.add_argument("--symbol", type=str, help="查询特定币种")
    parser.add_argument("--fear-greed", action="store_true", help="获取恐慌贪婪指数")
    parser.add_argument("--ranking", action="store_true", help="获取市场排名")
    
    args = parser.parse_args()
    
    print("\n" + "═" * 60)
    print("🌊 Surf 数据增强系统")
    print("═" * 60)
    
    if args.overview:
        overview = get_market_overview()
        print("\n📊 市场概览:")
        print(f"  恐慌贪婪：{overview.get('fear_greed', {})}")
        print(f"  市场情绪：{overview.get('market_sentiment', 'NEUTRAL')}")
        print(f"  清算风险：{overview.get('liquidation_risk', 'LOW')}")
        
        if overview.get('top_gainers'):
            print("\n📈 涨幅榜 (Top 5):")
            for g in overview['top_gainers'][:5]:
                print(f"  {g['symbol']}: +{g['change_24h_pct']:.2f}%")
    
    elif args.fear_greed:
        fg = get_fear_greed_index()
        print(f"\n恐惧贪婪指数：{fg}")
    
    elif args.ranking:
        ranking = get_market_ranking(limit=10)
        print("\n📊 市场排名 (按成交量):")
        for r in ranking:
            print(f"  #{r['rank']} {r['symbol']}: ${r['price_usd']:.4f} ({r['change_24h_pct']:.2f}%)")
    
    elif args.symbol:
        data = enhance_symbol_data(args.symbol)
        print(f"\n📊 {args.symbol} 增强数据:")
        print(json.dumps(data, indent=2, default=str))
    
    else:
        print("\n用法:")
        print("  surf-enhancer --overview          # 市场概览")
        print("  surf-enhancer --fear-greed        # 恐慌贪婪指数")
        print("  surf-enhancer --ranking           # 市场排名")
        print("  surf-enhancer --symbol BTC        # 特定币种")
    
    print("\n" + "═" * 60 + "\n")


if __name__ == "__main__":
    main()
