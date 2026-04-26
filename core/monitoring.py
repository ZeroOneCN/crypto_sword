"""Utilities for monitor message diffing and stable sorting."""

from __future__ import annotations

import json
from datetime import datetime
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
                "direction": "N/A",
                "price": 0,
                "metrics": {},
                "score": {"total_score": 0, "confidence": "状态变更"},
                "entry_status_text": "移出监控",
                "entry_note": f"已移出当前{count_label}前排监控",
                "strategy_line": "",
                "watch_stage": "监控变更",
            }
        )

    return delta_items[:top_n], current_snapshot


def build_strategy_event(signal: dict[str, Any], source: str = "scan") -> dict[str, Any]:
    score = signal.get("score") or {}
    oi_funding = signal.get("oi_funding") or {}
    return {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type": "strategy_signal",
        "source": source,
        "symbol": str(signal.get("symbol", "")).upper(),
        "stage": signal.get("stage", ""),
        "direction": signal.get("direction", ""),
        "score": round(float(score.get("total_score", 0) or 0), 2),
        "confidence": score.get("confidence", ""),
        "entry_status": signal.get("entry_status", ""),
        "entry_status_text": signal.get("entry_status_text", ""),
        "strategy_line": signal.get("strategy_line", ""),
        "oi_funding_bonus": round(float(oi_funding.get("score_bonus", 0) or 0), 2),
        "oi_change_pct": round(float(oi_funding.get("oi_change_pct", 0) or 0), 2),
        "funding_current": float(oi_funding.get("funding_current", 0) or 0),
    }


def build_execution_event(
    event: str,
    symbol: str,
    direction: str,
    session_id: str,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type": "execution",
        "event": event,
        "symbol": str(symbol).upper(),
        "direction": direction,
        "session_id": session_id,
    }
    if metrics:
        payload["metrics"] = metrics
    return payload


def build_monitor_event(
    open_positions: int,
    max_positions: int,
    unrealized_pnl: float,
    realized_pnl: float,
    closed_today: int,
) -> dict[str, Any]:
    return {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type": "monitor_snapshot",
        "open_positions": int(open_positions),
        "max_positions": int(max_positions),
        "unrealized_pnl": round(float(unrealized_pnl), 2),
        "realized_pnl": round(float(realized_pnl), 2),
        "closed_today": int(closed_today),
    }
