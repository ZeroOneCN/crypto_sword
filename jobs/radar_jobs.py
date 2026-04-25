"""Radar-related periodic jobs."""

from __future__ import annotations

from typing import Any


def scan_oi_changes_job() -> list[Any]:
    from accumulation_radar import scan_oi_changes

    return scan_oi_changes()


def scan_accumulation_pool_job() -> list[Any]:
    from accumulation_radar import scan_accumulation_pool

    return scan_accumulation_pool()
