"""Utilities for monitor message diffing and stable sorting."""

from __future__ import annotations

import json
from typing import Any


def message_signature(payload: Any) -> str:
    try:
        return json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return str(payload)


def monitor_item_signature(item: dict[str, Any]) -> str:
    return message_signature(
        {
            "direction": item.get("direction"),
            "score": round(float((item.get("score") or {}).get("total_score", 0) or 0), 1),
            "entry_status_text": item.get("entry_status_text", ""),
            "entry_note": item.get("entry_note", ""),
            "strategy_line": item.get("strategy_line", ""),
            "watch_stage": item.get("watch_stage", ""),
        }
    )


def stable_monitor_sort(items: list[dict[str, Any]], order_cache: dict[str, int]) -> list[dict[str, Any]]:
    def _key(item: dict[str, Any]) -> tuple[int, float, int]:
        symbol = str(item.get("symbol", ""))
        strategy_bonus = 1 if item.get("strategy_line") == "趋势突破线" else 0
        score_total = round(float((item.get("score") or {}).get("total_score", 0) or 0), 1)
        previous_rank = order_cache.get(symbol, 999)
        return strategy_bonus, score_total, -previous_rank

    sorted_items = sorted(items, key=_key, reverse=True)
    order_cache.clear()
    for index, item in enumerate(sorted_items[:10]):
        symbol = str(item.get("symbol", ""))
        if symbol:
            order_cache[symbol] = index
    return sorted_items


def build_monitor_delta(
    items: list[dict[str, Any]],
    previous_snapshot: dict[str, str],
    count_label: str,
    top_n: int = 5,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    current_items = items[:top_n]
    current_snapshot = {
        str(item.get("symbol", "")): monitor_item_signature(item) for item in current_items if item.get("symbol")
    }
    delta_items: list[dict[str, Any]] = []

    for item in current_items:
        symbol = str(item.get("symbol", ""))
        if not symbol:
            continue
        if previous_snapshot.get(symbol) != current_snapshot.get(symbol):
            delta_items.append(item)

    previous_symbols = set(previous_snapshot.keys())
    current_symbols = set(current_snapshot.keys())
    removed_symbols = sorted(previous_symbols - current_symbols)
    for symbol in removed_symbols[:3]:
        delta_items.append(
            {
                "symbol": symbol,
                "direction": "LONG",
                "price": 0,
                "metrics": {},
                "score": {"total_score": 0, "confidence": "状态变更"},
                "entry_status_text": "失效淘汰",
                "entry_note": f"已移出当前{count_label}前排监控",
                "strategy_line": "",
                "watch_stage": "淘汰",
            }
        )

    return delta_items[:top_n], current_snapshot

