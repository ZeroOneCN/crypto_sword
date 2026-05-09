"""Exchange-authoritative accounting helpers for Binance Futures."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from binance_api_client import get_native_binance_client, is_native_binance_configured
from services.time_basis import UTC, utc_cutoff_for_days, utc_day_window, utc_now

logger = logging.getLogger(__name__)

EXCLUDED_INCOME_TYPES = {
    "TRANSFER",
    "INTERNAL_TRANSFER",
    "ASSET_TRANSFER",
}

ACCOUNTING_INCOME_TYPES = (
    "REALIZED_PNL",
    "COMMISSION",
    "FUNDING_FEE",
)


def _ms(dt: datetime) -> int:
    return int(dt.astimezone(UTC).timestamp() * 1000)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _row_time(row: dict[str, Any]) -> int:
    try:
        return int(row.get("time", row.get("tranId", 0)) or 0)
    except Exception:
        return 0


def _fetch_income_rows_once(
    start: datetime,
    end: datetime,
    *,
    income_type: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch one Binance income stream between UTC datetimes."""
    if not is_native_binance_configured():
        return []

    client = get_native_binance_client()
    start_ms = _ms(start)
    end_ms = _ms(end)
    rows: list[dict[str, Any]] = []
    cursor = start_ms

    for _ in range(10):
        batch = client.income_history(
            income_type=income_type,
            start_time=cursor,
            end_time=end_ms,
            limit=limit,
        )  # type: ignore[union-attr]
        if not batch:
            break
        batch = [row for row in batch if isinstance(row, dict)]
        rows.extend(batch)
        max_time = max((_row_time(row) for row in batch), default=0)
        if len(batch) < limit or max_time <= cursor:
            break
        cursor = max_time + 1
        if cursor >= end_ms:
            break

    return rows


def fetch_income_rows(start: datetime, end: datetime, *, limit: int = 1000) -> list[dict[str, Any]]:
    """Fetch Binance income rows between UTC datetimes.

    Binance's income endpoint can mix realized PnL, commission and funding
    rows.  Pulling key accounting types separately makes fee and PnL summaries
    much less likely to miss rows when a window contains many fills.
    """
    if not is_native_binance_configured():
        return []

    rows: list[dict[str, Any]] = []
    for income_type in ACCOUNTING_INCOME_TYPES:
        rows.extend(_fetch_income_rows_once(start, end, income_type=income_type, limit=limit))

    seen: set[tuple[str, str, str, str, str, str]] = set()
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("tranId", "")),
            str(row.get("time", "")),
            str(row.get("incomeType", "")),
            str(row.get("symbol", "")),
            str(row.get("income", "")),
            str(row.get("asset", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def summarize_income_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, float] = {}
    by_type_count: dict[str, int] = {}
    transfer_total = 0.0
    net_pnl = 0.0

    for row in rows:
        income_type = str(row.get("incomeType", "UNKNOWN") or "UNKNOWN").upper()
        amount = _safe_float(row.get("income"))
        by_type[income_type] = round(by_type.get(income_type, 0.0) + amount, 8)
        by_type_count[income_type] = by_type_count.get(income_type, 0) + 1
        if income_type in EXCLUDED_INCOME_TYPES:
            transfer_total += amount
            continue
        net_pnl += amount

    return {
        "available": bool(rows),
        "rows": len(rows),
        "net_pnl": round(net_pnl, 8),
        "realized_pnl": round(by_type.get("REALIZED_PNL", 0.0), 8),
        "commission": round(by_type.get("COMMISSION", 0.0), 8),
        "commission_cost": round(abs(by_type.get("COMMISSION", 0.0)), 8),
        "funding_fee": round(by_type.get("FUNDING_FEE", 0.0), 8),
        "transfer_total": round(transfer_total, 8),
        "by_type": by_type,
        "by_type_count": by_type_count,
        "excluded_types": sorted(EXCLUDED_INCOME_TYPES),
    }


def fetch_income_summary(start: datetime, end: datetime) -> dict[str, Any]:
    """Return exchange income summary for a UTC window."""
    if not is_native_binance_configured():
        return {"available": False, "rows": 0, "net_pnl": 0.0, "reason": "native Binance not configured"}
    try:
        rows = fetch_income_rows(start, end)
        summary = summarize_income_rows(rows)
        summary["start_utc"] = start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        summary["end_utc"] = end.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return summary
    except Exception as exc:
        logger.warning(f"Binance income summary fetch failed: {exc}")
        return {"available": False, "rows": 0, "net_pnl": 0.0, "reason": str(exc)}


def fetch_daily_income_summary(date_key: str) -> dict[str, Any]:
    start, end = utc_day_window(date_key)
    return fetch_income_summary(start, end)


def fetch_period_income_summary(days: int) -> dict[str, Any]:
    return fetch_income_summary(utc_cutoff_for_days(days), utc_now())
