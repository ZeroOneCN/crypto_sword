#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backward-compatible trade logger facade.

SQLite repository implementation lives in ``repositories/trade_repository.py``.
Report aggregation lives in ``services/report_service.py`` and structured review
helpers live in ``services/review_service.py``.
"""

from __future__ import annotations

from pathlib import Path

from repositories.trade_repository import DB_PATH, TradeDatabase, TradeRecord
from repositories.trade_repository import print_recent_trades, print_statistics
from services.review_service import ReviewService

__all__ = [
    "DB_PATH",
    "TradeDatabase",
    "TradeRecord",
    "ReviewService",
    "print_statistics",
    "print_recent_trades",
    "main",
]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="?? ????????")
    parser.add_argument("--stats", action="store_true", help="??????")
    parser.add_argument("--recent", action="store_true", help="??????")
    parser.add_argument("--export", type=str, help="?? CSV ?????")
    parser.add_argument("--backfill-reviews", action="store_true", help="Backfill structured reviews for closed trades")
    parser.add_argument("--export-reviews", type=str, help="Export structured reviews JSONL")
    parser.add_argument("--days", type=int, default=7, help="???? (???7)")
    parser.add_argument("--mode", type=str, choices=["live"], help="??????")
    parser.add_argument("--limit", type=int, default=10, help="?????? (???10)")

    args = parser.parse_args()
    db = TradeDatabase()
    reviews = ReviewService(db)

    if args.stats:
        print_statistics(db, days=args.days, mode=args.mode)
    elif args.backfill_reviews:
        count = reviews.backfill_trade_reviews(days=args.days, mode=args.mode)
        print(f"backfilled_reviews={count}")
    elif args.export_reviews:
        count = reviews.export_reviews_jsonl(Path(args.export_reviews), days=args.days, mode=args.mode)
        print(f"exported_reviews={count} path={args.export_reviews}")
    elif args.export:
        db.export_to_csv(Path(args.export), days=args.days)
        print(f"? ?????{args.export}")
    else:
        print_statistics(db, days=args.days, mode=args.mode)
        print_recent_trades(db, limit=args.limit)


if __name__ == "__main__":
    main()
