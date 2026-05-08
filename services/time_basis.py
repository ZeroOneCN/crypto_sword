"""Shared Binance-style time-basis helpers.

Binance futures performance pages are keyed by UTC natural days.  Runtime
notifications still show an UTC+8 hint for readability, but all trade stats
should use UTC as the authoritative accounting day.
"""

from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

UTC = timezone.utc
UTC8 = timezone(timedelta(hours=8))


def _legacy_naive_tz() -> timezone:
    """Timezone used to interpret old naive DB timestamps.

    Older records were stored with ``datetime.now().isoformat()`` and no
    timezone.  In this project those historical records were generated from
    the UTC+8 runtime, so default to +8 while keeping an env override for
    unusual deployments.
    """

    try:
        offset_hours = float(os.environ.get("HERMES_DB_NAIVE_TZ_OFFSET_HOURS", "8") or 8)
    except Exception:
        offset_hours = 8.0
    return timezone(timedelta(hours=offset_hours))


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_today_key() -> str:
    return utc_now().date().isoformat()


def utc_previous_date_key(days: int = 1) -> str:
    return (utc_now().date() - timedelta(days=max(int(days), 0))).isoformat()


def utc_hour_key() -> str:
    return utc_now().strftime("%Y-%m-%d %H")


def parse_db_datetime(value: Any) -> datetime | None:
    """Parse DB/API timestamp into timezone-aware UTC datetime.

    Aware timestamps are converted as-is.  Naive legacy timestamps are treated
    as UTC+8 by default so historical reports line up with Binance UTC days.
    """

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        try:
            parsed = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_legacy_naive_tz())
    return parsed.astimezone(UTC)


def utc_date_key(value: Any) -> str:
    parsed = parse_db_datetime(value)
    return parsed.date().isoformat() if parsed else ""


def is_utc_date(value: Any, date_key: str) -> bool:
    return utc_date_key(value) == str(date_key)


def utc_cutoff_for_days(days: int) -> datetime:
    return utc_now() - timedelta(days=max(int(days), 0))


def utc_day_window(date_key: str) -> tuple[datetime, datetime]:
    target = date.fromisoformat(str(date_key))
    start = datetime.combine(target, time.min, tzinfo=UTC)
    return start, start + timedelta(days=1)


def is_within_utc_day(value: Any, date_key: str) -> bool:
    parsed = parse_db_datetime(value)
    if not parsed:
        return False
    start, end = utc_day_window(date_key)
    return start <= parsed < end


def is_after_utc_cutoff(value: Any, cutoff: datetime) -> bool:
    parsed = parse_db_datetime(value)
    return bool(parsed and parsed >= cutoff.astimezone(UTC))


def utc8_window_label(date_key: str) -> str:
    """Return the UTC+8 display window for a UTC natural day."""

    try:
        start, end = utc_day_window(date_key)
    except Exception:
        return ""
    local_start = start.astimezone(UTC8)
    local_end = end.astimezone(UTC8)
    return f"{local_start:%Y-%m-%d %H:%M} ~ {local_end:%Y-%m-%d %H:%M}"


def report_clock_label() -> str:
    now = utc_now()
    return f"{now:%Y-%m-%d %H:%M:%S} UTC｜{now.astimezone(UTC8):%Y-%m-%d %H:%M:%S} UTC+8"
