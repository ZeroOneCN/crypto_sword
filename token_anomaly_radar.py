#!/usr/bin/env python3
"""Generate long/short token anomaly signals from Surf data.

Standalone usage:
  python3 ~/.hermes/scripts/token_anomaly_radar.py --top-n 10 --exchange binance
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable


POSITIVE_KEYWORDS = {
    "bullish",
    "breakout",
    "surge",
    "strength",
    "momentum",
    "expansion",
    "accumulation",
    "squeeze",
    "rebound",
    "rally",
    "uptrend",
    "upgrade",
}
NEGATIVE_KEYWORDS = {
    "bearish",
    "breakdown",
    "panic",
    "selloff",
    "dump",
    "exploit",
    "hack",
    "liquidation",
    "weakness",
    "downtrend",
    "rug",
    "fear",
}
DEFAULT_STABLES = {"USDT", "USDC", "FDUSD", "DAI", "TUSD", "USDE", "BUSD"}


def classify_breakout_stage(metrics: dict[str, Any]) -> tuple[str, str, str]:
    """分类突破阶段 - 妖币识别优化版 v3
    
    进一步降低阈值，更容易开单（测试网验证后调整）：
    - 24h 涨幅 > 5% 即视为潜在妖币（之前 8%）
    - 量能放大 > 1.0x 即视为异动（之前 1.2x）
    - OI 变化 > 10% 视为资金关注（之前 15%）
    """
    change_24h = float(metrics.get("change_24h_pct", 0.0) or 0.0)
    change_72h = float(metrics.get("change_72h_pct", 0.0) or 0.0)
    volume_24h = float(metrics.get("volume_24h_mult", 0.0) or 0.0)
    oi_24h = float(metrics.get("oi_24h_pct", 0.0) or 0.0)
    oi_72h = float(metrics.get("oi_72h_pct", 0.0) or 0.0)
    funding = float(metrics.get("funding_rate", 0.0) or 0.0)
    ls_now = float(metrics.get("ls_ratio_now", 0.0) or 0.0)
    ls_prev = float(metrics.get("ls_ratio_prev_24h", 0.0) or 0.0)
    venues = int(metrics.get("venues_180m", 0) or 0)
    events = int(metrics.get("events_180m", 0) or 0)
    drawdown = float(metrics.get("drawdown_from_24h_high_pct", 0.0) or 0.0)
    
    ls_rising = ls_now > ls_prev
    crowded_longs = ls_now >= 2.5 or (ls_now >= 2.0 and funding > 0.01)
    crowded_shorts = ls_now <= 0.6 or (ls_now <= 0.7 and funding < -0.0005)
    
    # 妖币特征：大幅波动 + 量能异常 + OI 剧变（阈值降低）
    extreme_move = abs(change_24h) >= 5 or abs(change_72h) >= 12
    volume_spike = volume_24h >= 1.0
    oi_surge = abs(oi_24h) >= 10
    
    # 多头突破（阈值降低）
    strong_confirmation = change_72h >= 12 and volume_24h >= 1.2 and oi_24h >= 20 and ls_rising
    early_break = change_24h >= 5 and volume_24h >= 1.0 and oi_24h >= 10
    
    # 空头突破（做空信号）（阈值降低）
    strong_breakdown = change_72h <= -10 and volume_24h >= 1.2 and oi_24h >= 15
    early_breakdown = change_24h <= -5 and volume_24h >= 1.0 and oi_24h >= 10
    
    # 过热/衰竭（阈值降低，更早预警）
    overheated_long = change_24h >= 25 or change_72h >= 50 or volume_24h >= 6
    overheated_short = change_24h <= -20 or change_72h <= -45
    failed_followthrough = drawdown >= 8 or (funding < 0 and ls_now < ls_prev and abs(change_24h) < 15)

    # 分类逻辑 - 妖币双向交易
    # 1. 多头过热 - 避免追高，可能做空机会
    if overheated_long and crowded_longs:
        return (
            "mania",
            "极端多头行情，量能价格严重超买",
            "严重过热 + 拥挤多头；极高回调/爆仓风险，考虑反向做空",
        )

    # 2. 空头过热 - 避免追空，可能反弹
    if overheated_short and crowded_shorts:
        return (
            "mania",
            "极端空头行情，严重超卖",
            "严重超卖 + 拥挤空头；极高反弹/逼空风险，避免追空",
        )

    # 3. 多头确认突破 - 做多信号
    if strong_confirmation:
        risk = "确认突破，但警惕拥挤多头导致回调" if crowded_longs else "确认突破，注意不要盲目追高"
        return (
            "confirmed_breakout",
            "72h 突破确认 + 放量 + OI 上升 + 多空比改善",
            risk,
        )

    # 4. 空头确认突破 - 做空信号
    if strong_breakdown:
        risk = "确认跌破，但警惕拥挤空头导致反弹" if crowded_shorts else "确认跌破，注意量能和 OI 变化"
        return (
            "confirmed_breakout",
            "72h 跌破确认 + 放量 + OI 上升 + 空头主导",
            risk,
        )

    # 5. 早期突破 - 提前布局
    if early_break:
        risk = "早期突破，需要持续放量确认" if crowded_longs else "早期信号，关注后续量能和 OI"
        return (
            "pre_break",
            "24h 异动 + 放量 + OI 上升",
            risk,
        )

    # 6. 早期跌破 - 提前布局做空
    if early_breakdown:
        risk = "早期跌破，需要持续放量确认" if crowded_shorts else "早期做空信号，关注后续动能"
        return (
            "pre_break",
            "24h 下跌 + 放量 + OI 上升",
            risk,
        )

    # 7. 衰竭信号 - 可能反转
    if failed_followthrough:
        return (
            "exhaustion",
            "突破动能衰竭，高位回落或低位反弹",
            "动能衰竭；警惕反转风险，已有持仓建议减仓",
        )

    # 8. 中性 - 无明确信号
    return (
        "neutral",
        "信号证据不足或相互矛盾",
        "无明确突破结构；建议观望",
    )


@dataclass
class TokenSignal:
    symbol: str
    long_score: float = 0.0
    short_score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    snapshots: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "long_score": round(self.long_score, 2),
            "short_score": round(self.short_score, 2),
            "net_score": round(self.long_score - self.short_score, 2),
            "reasons": self.reasons,
            "snapshots": self.snapshots,
        }


def infer_text_bias(text: str) -> float:
    punctuation = ".,:;!?()[]{}\"'`"
    words = {token.strip(punctuation).lower() for token in text.split() if token.strip()}
    positive_hits = len(words & POSITIVE_KEYWORDS)
    negative_hits = len(words & NEGATIVE_KEYWORDS)
    return float(positive_hits - negative_hits)


def _ensure_token(store: dict[str, TokenSignal], symbol: str) -> TokenSignal:
    if symbol not in store:
        store[symbol] = TokenSignal(symbol=symbol)
    return store[symbol]


def _rank_bonus(rank: int | None, ceiling: float, decay: float = 1.8) -> float:
    if not rank or rank <= 0:
        return 0.0
    return max(0.0, ceiling - decay * (rank - 1))


def _normalize_symbol(raw: str | None) -> str | None:
    if not raw:
        return None
    return raw.strip().upper()


def _pulse_bias(items: Iterable[dict[str, Any]]) -> float:
    total = 0.0
    for item in items:
        chunks = [item.get("title", "")]
        chunks.extend(item.get("tldr", []) or [])
        total += infer_text_bias(" ".join(chunks))
    return total


def generate_signals(
    *,
    gainers: list[dict[str, Any]],
    losers: list[dict[str, Any]],
    volume_leaders: list[dict[str, Any]],
    social_ranking: list[dict[str, Any]],
    pulses_by_symbol: dict[str, list[dict[str, Any]]],
    allowed_symbols: set[str] | None = None,
    top_n: int = 10,
    min_score: float = 15.0,
) -> dict[str, list[dict[str, Any]]]:
    tokens: dict[str, TokenSignal] = {}
    allowed = {_normalize_symbol(s) for s in allowed_symbols} if allowed_symbols else None

    for item in gainers:
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol or symbol in DEFAULT_STABLES:
            continue
        token = _ensure_token(tokens, symbol)
        bonus = _rank_bonus(item.get("rank"), ceiling=26)
        change = max(0.0, float(item.get("change_24h_pct", 0.0)))
        token.long_score += bonus + min(22.0, change * 0.8)
        token.reasons.append(f"gainers rank #{item.get('rank')} change {change:.2f}%")
        token.snapshots.setdefault("market", {}).update({"change_24h_pct": item.get("change_24h_pct"), "market_cap_usd": item.get("market_cap_usd")})

    for item in losers:
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol or symbol in DEFAULT_STABLES:
            continue
        token = _ensure_token(tokens, symbol)
        bonus = _rank_bonus(item.get("rank"), ceiling=26)
        change = abs(min(0.0, float(item.get("change_24h_pct", 0.0))))
        token.short_score += bonus + min(22.0, change * 0.8)
        token.reasons.append(f"losers rank #{item.get('rank')} change -{change:.2f}%")
        token.snapshots.setdefault("market", {}).update({"change_24h_pct": item.get("change_24h_pct"), "market_cap_usd": item.get("market_cap_usd")})

    for item in volume_leaders:
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol or symbol in DEFAULT_STABLES:
            continue
        token = _ensure_token(tokens, symbol)
        direction = float(item.get("change_24h_pct", token.snapshots.get("market", {}).get("change_24h_pct", 0.0) or 0.0))
        bonus = _rank_bonus(item.get("rank"), ceiling=18, decay=1.25)
        volume_usd = float(item.get("volume_24h_usd", 0.0) or 0.0)
        if direction >= 0:
            token.long_score += bonus
            token.reasons.append(f"volume leader rank #{item.get('rank')} volume ${volume_usd:,.0f}")
        else:
            token.short_score += bonus
            token.reasons.append(f"sell pressure volume rank #{item.get('rank')} volume ${volume_usd:,.0f}")
        token.snapshots.setdefault("market", {}).update({"volume_24h_usd": item.get("volume_24h_usd")})

    for item in social_ranking:
        symbol = _normalize_symbol((item.get("token") or {}).get("symbol"))
        if not symbol or symbol in DEFAULT_STABLES:
            continue
        token = _ensure_token(tokens, symbol)
        sentiment = (item.get("sentiment") or "").lower()
        sentiment_score = float(item.get("sentiment_score", 0.0) or 0.0)
        bonus = _rank_bonus(item.get("rank"), ceiling=16, decay=0.9)
        if sentiment == "positive" or sentiment_score > 0:
            token.long_score += bonus + min(8.0, max(0.0, sentiment_score) * 8)
            token.reasons.append(f"social positive rank #{item.get('rank')} sentiment {sentiment_score:.2f}")
        elif sentiment == "negative" or sentiment_score < 0:
            token.short_score += bonus + min(8.0, abs(min(0.0, sentiment_score)) * 8)
            token.reasons.append(f"social negative rank #{item.get('rank')} sentiment {sentiment_score:.2f}")
        token.snapshots.setdefault("social", {}).update({"rank": item.get("rank"), "sentiment": sentiment, "sentiment_score": sentiment_score})

    for symbol, pulse_items in pulses_by_symbol.items():
        norm_symbol = _normalize_symbol(symbol)
        if not norm_symbol:
            continue
        token = _ensure_token(tokens, norm_symbol)
        bias = _pulse_bias(pulse_items)
        if bias > 0:
            token.long_score += min(15.0, 6.0 + bias * 3)
            token.reasons.append(f"pulse/news positive bias {bias:.1f}")
        elif bias < 0:
            token.short_score += min(15.0, 6.0 + abs(bias) * 3)
            token.reasons.append(f"pulse/news negative bias {bias:.1f}")
        token.snapshots.setdefault("pulse", {})["count"] = len(pulse_items)

    long_candidates = []
    short_candidates = []
    for signal in tokens.values():
        if allowed is not None and signal.symbol not in allowed:
            continue
        if signal.long_score >= min_score and signal.long_score > signal.short_score:
            long_candidates.append(signal.to_dict())
        if signal.short_score >= min_score and signal.short_score > signal.long_score:
            short_candidates.append(signal.to_dict())

    long_candidates.sort(key=lambda item: (item["long_score"], item["net_score"]), reverse=True)
    short_candidates.sort(key=lambda item: (item["short_score"], -item["net_score"]), reverse=True)
    return {
        "long": long_candidates[:top_n],
        "short": short_candidates[:top_n],
    }


def run_surf_json(args: list[str]) -> dict[str, Any]:
    result = subprocess.run(["surf", *args, "--json"], capture_output=True, text=True)
    output = (result.stdout or result.stderr).strip()
    if not output:
        raise RuntimeError(f"surf {' '.join(args)} produced no output")
    payload = json.loads(output)
    if payload.get("error"):
        raise RuntimeError(f"surf {' '.join(args)} failed: {payload['error']['code']} {payload['error']['message']}")
    return payload


def fetch_allowed_symbols(exchange: str) -> set[str]:
    payload = run_surf_json(["exchange-markets", "--exchange", exchange, "--type", "spot", "--quote", "USDT", "--limit", "5000"])
    return {
        item["base"].upper()
        for item in payload.get("data", [])
        if item.get("active") and item.get("base")
    }


def fetch_pulses_for_symbols(symbols: Iterable[str], limit_per_symbol: int = 3) -> dict[str, list[dict[str, Any]]]:
    pulses: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        try:
            payload = run_surf_json(["project-pulse", "--q", symbol, "--limit", str(limit_per_symbol)])
        except RuntimeError:
            continue
        items = payload.get("data", []) or []
        if items:
            pulses[symbol] = items
    return pulses


def build_live_signals(limit: int, exchange: str | None, top_n: int, min_score: float) -> dict[str, Any]:
    gainers = run_surf_json(["market-ranking", "--sort-by", "change_24h", "--order", "desc", "--limit", str(limit)]).get("data", [])
    losers = run_surf_json(["market-ranking", "--sort-by", "change_24h", "--order", "asc", "--limit", str(limit)]).get("data", [])
    volume = run_surf_json(["market-ranking", "--sort-by", "volume_24h", "--order", "desc", "--limit", str(limit)]).get("data", [])
    social = run_surf_json(["social-ranking", "--time-range", "24h", "--limit", str(limit)]).get("data", [])

    candidate_symbols = {
        _normalize_symbol(item.get("symbol"))
        for item in [*gainers, *losers, *volume]
        if _normalize_symbol(item.get("symbol"))
    }
    candidate_symbols.update(
        _normalize_symbol((item.get("token") or {}).get("symbol"))
        for item in social
        if _normalize_symbol((item.get("token") or {}).get("symbol"))
    )
    candidate_symbols.discard(None)

    allowed = fetch_allowed_symbols(exchange) if exchange else None
    pulses = fetch_pulses_for_symbols(sorted(candidate_symbols))
    signals = generate_signals(
        gainers=gainers,
        losers=losers,
        volume_leaders=volume,
        social_ranking=social,
        pulses_by_symbol=pulses,
        allowed_symbols=allowed,
        top_n=top_n,
        min_score=min_score,
    )
    return {
        "exchange_filter": exchange,
        "candidate_symbol_count": len(candidate_symbols),
        "long": signals["long"],
        "short": signals["short"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate long/short token anomaly signals using Surf")
    parser.add_argument("--limit", type=int, default=20, help="source ranking fetch size")
    parser.add_argument("--top-n", type=int, default=10, help="number of long/short candidates to print")
    parser.add_argument("--min-score", type=float, default=15.0, help="minimum directional score threshold")
    parser.add_argument("--exchange", default="binance", help="filter to symbols listed on this exchange; use '' to disable")
    parser.add_argument("--pretty", action="store_true", help="print human-readable output")
    args = parser.parse_args()

    exchange = args.exchange or None
    try:
        signals = build_live_signals(limit=args.limit, exchange=exchange, top_n=args.top_n, min_score=args.min_score)
    except RuntimeError as exc:
        payload = {"error": str(exc)}
        if args.pretty:
            print(f"Signal generation failed: {exc}", file=sys.stderr)
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    if args.pretty:
        print("LONG SIGNALS")
        for item in signals["long"]:
            print(f"- {item['symbol']}: long={item['long_score']} short={item['short_score']} reasons={'; '.join(item['reasons'])}")
        print("\nSHORT SIGNALS")
        for item in signals["short"]:
            print(f"- {item['symbol']}: short={item['short_score']} long={item['long_score']} reasons={'; '.join(item['reasons'])}")
    else:
        print(json.dumps(signals, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
