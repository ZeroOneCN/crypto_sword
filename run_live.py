#!/usr/bin/env python3
"""Official runtime entry for Crypto Sword live/dry execution."""

from __future__ import annotations

import argparse

from core.models import TradingConfig
from crypto_sword import CryptoSword


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Hermes Trader runtime")
    parser.add_argument("--mode", default="live", choices=["live", "dry_run"], help="Runtime mode")
    parser.add_argument("--leverage", type=int, default=5, help="Leverage multiplier")
    parser.add_argument("--risk", type=float, default=2.0, help="Risk per trade (%)")
    parser.add_argument("--max-positions", type=int, default=5, help="Max open positions")
    parser.add_argument("--scan-top-n", type=int, default=30, help="Top N symbols per deep scan")
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
        max_open_positions=args.max_positions,
        scan_top_n=args.scan_top_n,
        scan_interval_sec=args.scan_interval,
        fast_scan_interval_sec=args.fast_interval,
        oi_funding_enabled=not args.disable_oi_funding,
    )
    trader = CryptoSword(config)
    trader.run()


if __name__ == "__main__":
    main()
