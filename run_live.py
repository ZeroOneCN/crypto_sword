#!/usr/bin/env python3
"""Lightweight runtime entry for Crypto Sword.

For full parameter control, use crypto_sword.py directly. For most runs:
  python3 run_live.py

This entry provides sensible defaults for quick launch.
"""

from __future__ import annotations

import argparse

from core.models import TradingConfig
from crypto_sword import CryptoSword


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Crypto Sword runtime (simplified entry)")
    parser.add_argument("--mode", default="live", help="Runtime mode (only live supported)")
    parser.add_argument("--leverage", type=int, default=5, help="Leverage multiplier")
    parser.add_argument("--risk", type=float, default=0.6, help="Risk per trade (%)")
    parser.add_argument("--stop-loss", type=float, default=12.0, help="Stop loss (%)")
    parser.add_argument("--take-profit", type=float, default=30.0, help="Take profit (%%)")
    parser.add_argument("--trailing", type=float, default=5.0, help="Trailing stop (%%)")
    parser.add_argument("--max-positions", type=int, default=3, help="Max open positions")
    parser.add_argument("--max-position-pct", type=float, default=25.0, help="Max notional position size (%% of balance)")
    parser.add_argument("--max-total-exposure", type=float, default=120.0, help="Max total notional exposure (%% of balance)")
    parser.add_argument("--max-daily-entries", type=int, default=5, help="Max new entries per day")
    parser.add_argument("--max-entries-per-cycle", type=int, default=1, help="Max new entries per scan cycle")
    parser.add_argument("--weak-daily-entries", type=int, default=3, help="Soft cap when daily stats are weak")
    parser.add_argument("--hard-daily-entries", type=int, default=2, help="Soft cap in deep defensive mode")
    parser.add_argument("--daily-exception-entries", type=int, default=2, help="Max A+ override entries after soft cap")
    parser.add_argument("--scan-top-n", type=int, default=50, help="Top N symbols per deep scan")
    parser.add_argument("--scan-interval", type=int, default=300, help="Deep scan interval seconds")
    parser.add_argument("--fast-interval", type=int, default=60, help="Fast scan interval seconds")
    parser.add_argument("--disable-oi-funding", action="store_true", help="Disable OI/Funding scoring bonus")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = TradingConfig(
        mode=args.mode,
        leverage=args.leverage,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        trailing_stop_pct=args.trailing,
        max_position_pct=max(5.0, args.max_position_pct),
        max_total_exposure_pct=max(args.max_position_pct, args.max_total_exposure),
        max_open_positions=args.max_positions,
        max_daily_entries=args.max_daily_entries,
        max_entries_per_cycle=args.max_entries_per_cycle,
        weak_daily_entries=args.weak_daily_entries,
        hard_daily_entries=args.hard_daily_entries,
        daily_exception_entries=args.daily_exception_entries,
        scan_top_n=args.scan_top_n,
        scan_interval_sec=args.scan_interval,
        fast_scan_interval_sec=args.fast_interval,
        oi_funding_enabled=not args.disable_oi_funding,
    )
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()

