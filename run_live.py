#!/usr/bin/env python3
"""Lightweight runtime entry for Crypto Sword.

For full parameter control, use crypto_sword.py directly:
  python3 crypto_sword.py --leverage 5 --risk 2 --stop-loss 5 --take-profit 10 ...

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
    parser.add_argument("--risk", type=float, default=TradingConfig.DEFAULT_RISK_PER_TRADE_PCT, help="Risk per trade (%%)")
    parser.add_argument("--stop-loss", type=float, default=8.0, help="Stop loss (%%)")
    parser.add_argument("--take-profit", type=float, default=20.0, help="Take profit (%%)")
    parser.add_argument("--trailing", type=float, default=5.0, help="Trailing stop (%%)")
    parser.add_argument("--max-positions", type=int, default=TradingConfig.DEFAULT_MAX_OPEN_POSITIONS, help="Max open positions")
    parser.add_argument("--max-position-pct", type=float, default=30.0, help="Max notional position size (%% of balance)")
    parser.add_argument("--scan-top-n", type=int, default=30, help="Top N symbols per deep scan")
    parser.add_argument("--scan-interval", type=int, default=180, help="Deep scan interval seconds")
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

