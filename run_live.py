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
    parser.add_argument("--risk", type=float, default=0.8, help="Risk per trade percent")
    parser.add_argument("--stop-loss", type=float, default=12.0, help="Stop loss percent")
    parser.add_argument("--take-profit", type=float, default=35.0, help="Take profit percent")
    parser.add_argument("--trailing", type=float, default=8.0, help="Trailing stop percent")
    parser.add_argument("--max-positions", type=int, default=99, help="Max open positions")
    parser.add_argument("--max-position-pct", type=float, default=15.0, help="Max notional position size percent of balance")
    parser.add_argument("--max-total-exposure", type=float, default=150.0, help="Max total notional exposure percent of balance")
    parser.add_argument("--daily-entry-limit", dest="daily_entry_limit", action="store_true", default=False, help="Enable daily entry count throttle")
    parser.add_argument("--no-daily-entry-limit", dest="daily_entry_limit", action="store_false", help="Disable daily entry count throttle")
    parser.add_argument("--max-daily-entries", type=int, default=12, help="Max new entries per day")
    parser.add_argument("--max-entries-per-cycle", type=int, default=2, help="Max new entries per scan cycle")
    parser.add_argument("--weak-daily-entries", type=int, default=4, help="Soft cap when daily stats are weak")
    parser.add_argument("--hard-daily-entries", type=int, default=2, help="Soft cap in deep defensive mode")
    parser.add_argument("--daily-exception-entries", type=int, default=1, help="Max A+ override entries after soft cap")
    parser.add_argument("--scan-top-n", type=int, default=30, help="Top N symbols per deep scan")
    parser.add_argument("--scan-interval", type=int, default=300, help="Deep scan interval seconds")
    parser.add_argument("--fast-interval", type=int, default=60, help="Fast scan interval seconds")
    parser.add_argument("--disable-oi-funding", action="store_true", help="Disable OI/Funding scoring bonus")
    return parser


def build_config(args: argparse.Namespace) -> TradingConfig:
    """Build the recommended runtime config from simplified CLI args."""
    return TradingConfig(
        mode=args.mode,
        leverage=args.leverage,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        trailing_stop_pct=args.trailing,
        max_position_pct=max(5.0, args.max_position_pct),
        max_total_exposure_pct=max(args.max_position_pct, args.max_total_exposure),
        max_open_positions=args.max_positions,
        daily_entry_limit_enabled=args.daily_entry_limit,
        max_daily_entries=args.max_daily_entries,
        max_entries_per_cycle=args.max_entries_per_cycle,
        weak_daily_entries=args.weak_daily_entries,
        hard_daily_entries=args.hard_daily_entries,
        daily_exception_entries=args.daily_exception_entries,
        scan_top_n=args.scan_top_n,
        scan_interval_sec=args.scan_interval,
        fast_scan_interval_sec=args.fast_interval,
        oi_funding_enabled=not args.disable_oi_funding,
        min_signal_score_for_entry=78.0,
        min_signal_score_defensive=88.0,
        god_direct_score=90.0,
        pre_break_direct_score=78.0,
        confirmed_breakout_direct_score=78.0,
        confirmed_breakout_max_change_pct=30.0,
        confirmed_breakout_max_range_position_pct=94.0,
        quality_guard_defensive_min_score=88.0,
        quality_guard_defensive_max_change_pct=26.0,
        quality_guard_defensive_max_oi_pct=90.0,
        quality_guard_defensive_max_range_position_pct=92.0,
        min_change_pct=0.8,
        max_chase_change_pct=30.0,
        min_pullback_pct=0.8,
        shallow_pullback_pct=0.6,
        reclaim_volume_ratio=0.75,
        max_range_position_pct=95.0,
        max_abs_funding_rate=0.0050,
        max_oi_change_pct=110.0,
        max_entry_slippage_pct=0.45,
        exception_entry_score=88.0,
        exception_entry_min_change_pct=5.0,
        exception_entry_min_oi_pct=12.0,
        exception_entry_max_change_pct=40.0,
        exception_entry_max_oi_pct=100.0,
        exception_entry_max_abs_funding_rate=0.0040,
        defensive_score_override_score=90.0,
        defensive_score_override_max_abs_funding_rate=0.0040,
        momentum_entry_score=80.0,
        momentum_entry_min_change_pct=5.0,
        momentum_entry_min_oi_pct=9.0,
        ma_reentry_score=58.0,
        ma_reentry_min_change_pct=2.0,
        ma_reentry_max_change_pct=32.0,
        ma_reentry_min_oi_pct=4.0,
        ma_reentry_min_pullback_pct=0.4,
        ma_reentry_max_pullback_pct=9.0,
        ma_reentry_ma_tolerance_pct=1.6,
        ma_reentry_max_extension_pct=9.0,
        ma_reentry_min_volume_ratio=0.60,
        accumulation_entry_score=74.0,
        accumulation_entry_min_oi_pct=7.0,
        accumulation_entry_max_change_pct=20.0,
        accumulation_entry_max_range_pct=88.0,
        accumulation_entry_min_volume_mult=0.70,
        capital_min_expected_rr=1.65,
        capital_aggressive_score=86.0,
    )


def main() -> None:
    args = build_parser().parse_args()
    config = build_config(args)
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()
