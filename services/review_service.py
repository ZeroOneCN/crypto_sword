# -*- coding: utf-8 -*-
"""Structured post-trade review service.

This service keeps training/replay review workflows out of the SQLite
repository facade while preserving the existing database schema.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from repositories.trade_repository import TradeDatabase


class ReviewService:
    """Generate, save and export structured trade reviews."""

    def __init__(self, db: TradeDatabase | None = None):
        self.db = db or TradeDatabase()

    def save_trade_review(
        self,
        review: dict[str, Any],
        *,
        trade_id: int | None = None,
        session_id: str = "",
        symbol: str = "",
        mode: str = "live",
    ) -> int:
        return self.db.save_trade_review(
            review,
            trade_id=trade_id,
            session_id=session_id,
            symbol=symbol,
            mode=mode,
        )

    def backfill_trade_reviews(self, days: int = 3650, mode: str | None = None) -> int:
        return self.db.backfill_trade_reviews(days=days, mode=mode)

    def export_reviews_jsonl(self, output_path: Path, days: int = 3650, mode: str | None = None) -> int:
        return self.db.export_reviews_jsonl(output_path, days=days, mode=mode)

