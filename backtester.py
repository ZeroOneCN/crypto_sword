"""Backtester for Binance breakout strategy.

Tests the breakout classification strategy against historical data.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

import sys
sys.path.insert(0, str(Path("/root/.hermes/scripts")))

from binance_breakout_scanner import classify_and_direction, derive_venues_events

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Backtest configuration."""
    initial_balance: float = 10000.0
    risk_per_trade_pct: float = 1.0
    stop_loss_pct: float = 5.0
    take_profit_pct: float = 10.0
    max_position_pct: float = 20.0
    commission_pct: float = 0.04  # Binance futures fee
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])


@dataclass
class Trade:
    """A backtested trade."""
    symbol: str
    entry_date: str
    entry_price: float
    side: str  # LONG or SHORT
    quantity: float
    stop_loss_price: float
    take_profit_price: float
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None  # TP, SL, or CLOSE
    pnl: float = 0.0
    pnl_pct: float = 0.0
    metrics_at_entry: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "entry_date": self.entry_date,
            "entry_price": self.entry_price,
            "side": self.side,
            "quantity": self.quantity,
            "stop_loss_price": self.stop_loss_price,
            "take_profit_price": self.take_profit_price,
            "exit_date": self.exit_date,
            "exit_price": self.exit_price,
            "exit_reason": self.exit_reason,
            "pnl": round(self.pnl, 2),
            "pnl_pct": round(self.pnl_pct, 2),
        }


@dataclass
class BacktestResult:
    """Backtest performance results."""
    config: BacktestConfig
    trades: list[Trade]
    final_balance: float
    total_return_pct: float
    win_rate: float
    profit_factor: float
    max_drawdown_pct: float
    sharpe_ratio: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_win_pct: float
    avg_loss_pct: float
    avg_trade_duration_days: float

    def to_dict(self) -> dict:
        return {
            "final_balance": round(self.final_balance, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "win_rate": round(self.win_rate * 100, 2),
            "profit_factor": round(self.profit_factor, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct * 100, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "avg_win_pct": round(self.avg_win_pct * 100, 2),
            "avg_loss_pct": round(self.avg_loss_pct * 100, 2),
            "avg_trade_duration_days": round(self.avg_trade_duration_days, 2),
        }


def load_historical_data(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str = "1d",
) -> list[dict[str, Any]]:
    """Load historical OHLCV data for a symbol.

    In production, fetch from Binance API or load from local cache.
    For now, generate synthetic data for testing.
    """
    # TODO: Implement real data fetching through the native Binance REST client.

    logger.warning("Using synthetic data for backtest - implement real data fetch")

    # Generate synthetic data
    import random
    random.seed(42)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    data = []
    price = 50000.0 if "BTC" in symbol else 3000.0

    current = start
    while current <= end:
        # Random walk with drift
        drift = random.gauss(0.0005, 0.02)  # Daily return
        price = price * (1 + drift)

        high = price * (1 + abs(random.gauss(0, 0.015)))
        low = price * (1 - abs(random.gauss(0, 0.015)))
        open_price = price * (1 + random.gauss(0, 0.01))
        close = price
        volume = random.uniform(10000, 100000)

        data.append({
            "timestamp": current.isoformat(),
            "date": current.strftime("%Y-%m-%d"),
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "volume": round(volume, 2),
        })

        current += timedelta(days=1)

    return data


def simulate_metrics(data_window: list[dict]) -> dict[str, Any]:
    """Simulate breakout metrics from price data."""
    if len(data_window) < 2:
        return {}

    recent = data_window[-1]
    older = data_window[-2] if len(data_window) >= 2 else data_window[0]
    week_ago = data_window[-7] if len(data_window) >= 7 else data_window[0]

    close = recent["close"]
    old_close = older["close"]
    week_close = week_ago["close"]

    change_24h = (close - old_close) / old_close * 100 if old_close else 0
    change_72h = (close - week_close) / week_close * 100 if week_close else 0

    # Simulate other metrics
    import random
    volume_mult = random.uniform(0.8, 3.0)
    oi_change = random.uniform(-20, 40)
    funding = random.uniform(-0.001, 0.001)
    ls_ratio = random.uniform(0.8, 2.5)
    ls_prev = random.uniform(0.8, 2.5)

    venues, events = derive_venues_events(
        max_abs_return_pct_180m=abs(change_24h),
        volume_mult_180m=volume_mult,
        oi_change_pct_180m=oi_change,
        ls_ratio_delta=ls_ratio - ls_prev,
        funding_rate=funding,
    )

    high = recent["high"]
    drawdown = (high - close) / high * 100 if high else 0

    return {
        "change_24h_pct": change_24h,
        "change_72h_pct": change_72h * 1.5,
        "change_7d_pct": change_72h * 3,
        "volume_24h_mult": volume_mult,
        "oi_24h_pct": oi_change,
        "funding_rate": funding,
        "ls_ratio_now": ls_ratio,
        "ls_ratio_prev_24h": ls_prev,
        "venues_180m": venues,
        "events_180m": events,
        "drawdown_from_24h_high_pct": drawdown,
        "last_price": close,
    }


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """Run backtest for given configuration.

    Strategy:
    - Scan daily for breakout signals
    - Enter on pre_break or confirmed_breakout with LONG/SHORT direction
    - Exit on TP, SL, or end of data
    """
    balance = config.initial_balance
    trades: list[Trade] = []
    open_positions: dict[str, Trade] = {}
    peak_balance = balance
    max_drawdown = 0.0

    # Load data for all symbols
    all_data: dict[str, list[dict]] = {}
    for symbol in config.symbols:
        all_data[symbol] = load_historical_data(
            symbol,
            config.start_date,
            config.end_date,
        )

    # Get all dates
    all_dates = set()
    for data in all_data.values():
        for d in data:
            all_dates.add(d["date"])
    all_dates = sorted(all_dates)

    logger.info(f"Backtesting {len(all_dates)} days, {len(config.symbols)} symbols")

    # Simulate daily
    for date in all_dates:
        for symbol in config.symbols:
            data = all_data[symbol]
            # Get data up to this date
            window = [d for d in data if d["date"] <= date]
            if len(window) < 7:
                continue  # Need enough data

            # Check if we have an open position
            if symbol in open_positions:
                position = open_positions[symbol]
                current_price = window[-1]["close"]
                current_high = window[-1]["high"]
                current_low = window[-1]["low"]

                # Check stop loss
                if position.side == "LONG":
                    if current_low <= position.stop_loss_price:  # SL hit
                        position.exit_date = date
                        position.exit_price = position.stop_loss_price
                        position.exit_reason = "SL"
                    elif current_high >= position.take_profit_price:  # TP hit
                        position.exit_date = date
                        position.exit_price = position.take_profit_price
                        position.exit_reason = "TP"
                else:  # SHORT
                    if current_high >= position.stop_loss_price:  # SL hit
                        position.exit_date = date
                        position.exit_price = position.stop_loss_price
                        position.exit_reason = "SL"
                    elif current_low <= position.take_profit_price:  # TP hit
                        position.exit_date = date
                        position.exit_price = position.take_profit_price
                        position.exit_reason = "TP"

                # Calculate PnL if exited
                if position.exit_date:
                    if position.side == "LONG":
                        pnl = (position.exit_price - position.entry_price) * position.quantity
                    else:
                        pnl = (position.entry_price - position.exit_price) * position.quantity

                    # Subtract commission
                    commission = (position.entry_price + position.exit_price) * position.quantity * config.commission_pct / 100
                    pnl -= commission

                    position.pnl = pnl
                    position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100
                    balance += pnl

                    trades.append(position)
                    del open_positions[symbol]

                    # Update max drawdown
                    if balance > peak_balance:
                        peak_balance = balance
                    drawdown = (peak_balance - balance) / peak_balance
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown

            # Check for new signals if no position
            if symbol not in open_positions:
                metrics = simulate_metrics(window)
                stage, direction, trigger, risk = classify_and_direction(metrics)

                # Check if we should trade
                if stage in {"pre_break", "confirmed_breakout"} and direction in {"LONG", "SHORT"}:
                    entry_price = window[-1]["close"]

                    # Calculate position size
                    risk_amount = balance * config.risk_per_trade_pct / 100
                    stop_price = entry_price * (1 - config.stop_loss_pct / 100) if direction == "LONG" else entry_price * (1 + config.stop_loss_pct / 100)
                    stop_pct = abs(entry_price - stop_price) / entry_price * 100
                    position_value = risk_amount / (stop_pct / 100) if stop_pct else 0
                    position_value = min(position_value, balance * config.max_position_pct / 100)
                    quantity = position_value / entry_price

                    if quantity > 0:
                        tp_price = entry_price * (1 + config.take_profit_pct / 100) if direction == "LONG" else entry_price * (1 - config.take_profit_pct / 100)

                        position = Trade(
                            symbol=symbol,
                            entry_date=date,
                            entry_price=entry_price,
                            side=direction,
                            quantity=quantity,
                            stop_loss_price=stop_price,
                            take_profit_price=tp_price,
                            metrics_at_entry=metrics,
                        )
                        open_positions[symbol] = position

    # Close any remaining positions at end price
    for symbol, position in open_positions.items():
        data = all_data[symbol]
        if data:
            final_price = data[-1]["close"]
            position.exit_date = data[-1]["date"]
            position.exit_price = final_price
            position.exit_reason = "CLOSE"

            if position.side == "LONG":
                pnl = (final_price - position.entry_price) * position.quantity
            else:
                pnl = (position.entry_price - final_price) * position.quantity

            commission = (position.entry_price + final_price) * position.quantity * config.commission_pct / 100
            pnl -= commission

            position.pnl = pnl
            position.pnl_pct = pnl / (position.entry_price * position.quantity) * 100
            balance += pnl
            trades.append(position)

    # Calculate statistics
    winning = [t for t in trades if t.pnl > 0]
    losing = [t for t in trades if t.pnl <= 0]

    total_return = (balance - config.initial_balance) / config.initial_balance * 100
    win_rate = len(winning) / len(trades) if trades else 0

    gross_profit = sum(t.pnl for t in winning)
    gross_loss = abs(sum(t.pnl for t in losing))
    profit_factor = gross_profit / gross_loss if gross_loss else float("inf")

    avg_win = sum(t.pnl_pct for t in winning) / len(winning) if winning else 0
    avg_loss = sum(t.pnl_pct for t in losing) / len(losing) if losing else 0

    # Sharpe ratio (simplified)
    daily_returns = []
    for i in range(1, len(trades)):
        if trades[i].exit_date and trades[i-1].exit_date:
            ret = (trades[i].pnl - trades[i-1].pnl) / config.initial_balance
            daily_returns.append(ret)

    import statistics
    sharpe = 0.0
    if daily_returns and statistics.stdev(daily_returns) > 0:
        sharpe = statistics.mean(daily_returns) / statistics.stdev(daily_returns) * (252 ** 0.5)

    # Avg duration
    durations = []
    for t in trades:
        if t.exit_date and t.entry_date:
            d1 = datetime.strptime(t.exit_date, "%Y-%m-%d")
            d2 = datetime.strptime(t.entry_date, "%Y-%m-%d")
            durations.append((d1 - d2).days)
    avg_duration = sum(durations) / len(durations) if durations else 0

    return BacktestResult(
        config=config,
        trades=trades,
        final_balance=balance,
        total_return_pct=total_return,
        win_rate=win_rate,
        profit_factor=profit_factor,
        max_drawdown_pct=max_drawdown,
        sharpe_ratio=sharpe,
        total_trades=len(trades),
        winning_trades=len(winning),
        losing_trades=len(losing),
        avg_win_pct=avg_win,
        avg_loss_pct=avg_loss,
        avg_trade_duration_days=avg_duration,
    )


def main():
    """Run backtest from CLI."""
    import argparse

    parser = argparse.ArgumentParser(description="Backtest breakout strategy")
    parser.add_argument("--symbols", "-s", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--balance", "-b", type=float, default=10000.0)
    parser.add_argument("--risk", "-r", type=float, default=1.0)
    parser.add_argument("--stop-loss", "-l", type=float, default=5.0)
    parser.add_argument("--take-profit", "-t", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    config = BacktestConfig(
        initial_balance=args.balance,
        risk_per_trade_pct=args.risk,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        start_date=args.start,
        end_date=args.end,
        symbols=args.symbols,
    )

    print(f"Running backtest: {config.start_date} to {config.end_date}")
    print(f"Symbols: {config.symbols}")
    print(f"Initial balance: ${config.initial_balance:,.2f}")

    result = run_backtest(config)

    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"Final Balance:      ${result.final_balance:,.2f}")
        print(f"Total Return:       {result.total_return_pct:+.2f}%")
        print(f"Win Rate:           {result.win_rate * 100:.2f}%")
        print(f"Profit Factor:      {result.profit_factor:.2f}")
        print(f"Max Drawdown:       {result.max_drawdown_pct * 100:.2f}%")
        print(f"Sharpe Ratio:       {result.sharpe_ratio:.2f}")
        print(f"Total Trades:       {result.total_trades}")
        print(f"Winning:            {result.winning_trades}")
        print(f"Losing:             {result.losing_trades}")
        print(f"Avg Win:            {result.avg_win_pct:.2f}%")
        print(f"Avg Loss:           {result.avg_loss_pct:.2f}%")
        print(f"Avg Duration:       {result.avg_trade_duration_days:.1f} days")
        print("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
