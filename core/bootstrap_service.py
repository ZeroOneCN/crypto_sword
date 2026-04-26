"""Startup/shutdown orchestration service for the runtime."""

from __future__ import annotations

from logging import Logger
from typing import Any

from telegram_notifier import (
    format_error_msg,
    format_shutdown_msg,
    format_startup_msg,
    send_telegram_message,
)


class BootstrapService:
    """Encapsulate startup checks, startup notify and shutdown cleanup."""

    def __init__(self, trader: Any, logger: Logger):
        self.trader = trader
        self.logger = logger

    def mode_text(self) -> str:
        return f"{self.trader.config.mode_emoji} {self.trader.config.mode_name} mode"

    def log_startup_banner(self):
        mode_text = self.mode_text()
        alt_only = bool(getattr(self.trader.config, "target_altcoins", False))
        major_list = ", ".join(getattr(self.trader.config, "major_symbols", [])[:8]) or "-"
        self.logger.info("=" * 60)
        self.logger.info(f"Runtime start | {mode_text}")
        self.logger.info(f"Leverage={self.trader.config.leverage}x | Risk={self.trader.config.risk_per_trade_pct}%")
        self.logger.info(
            f"StopLoss={self.trader.config.stop_loss_pct}% | TakeProfit={self.trader.config.take_profit_pct}% "
            f"({self.trader.config.take_profit_mode})"
        )
        self.logger.info(
            f"ScanTopN={self.trader.config.scan_top_n} | Interval={self.trader.config.scan_interval_sec}s"
        )
        self.logger.info(f"MaxOpenPositions={self.trader.config.max_open_positions}")
        self.logger.info(f"AltcoinOnly={alt_only} | MajorSymbols={major_list}")
        self.logger.info("=" * 60)

    def startup(self):
        try:
            self.trader.day_start_balance = self.trader._run_health_checks()
            self.trader._start_market_ticker_stream()
            self.trader._start_user_data_stream()
            self.trader._start_background_protection_audit(source="startup_audit")
            self.logger.info(f"Startup health checks passed | available balance=${self.trader.day_start_balance:.2f}")
        except Exception as e:
            self.logger.error(f"Startup health checks failed: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="startup health checks failed",
                    message=str(e),
                    component="startup_checks",
                )
            )
            raise

        startup_notified = send_telegram_message(
            format_startup_msg(
                mode_name=self.mode_text(),
                leverage=self.trader.config.leverage,
                risk_pct=self.trader.config.risk_per_trade_pct,
                stop_loss_pct=self.trader.config.stop_loss_pct,
                take_profit_pct=self.trader.config.take_profit_pct,
                scan_top_n=self.trader.config.scan_top_n,
                scan_interval_sec=self.trader.config.scan_interval_sec,
                max_positions=self.trader.config.max_open_positions,
                take_profit_mode=self.trader.config.take_profit_mode,
                trailing_stop_pct=self.trader.config.trailing_stop_pct,
                trailing_enabled=self.trader.config.trailing_stop_enabled,
            )
        )
        if not startup_notified:
            message = "Telegram notification unavailable: startup message not delivered"
            self.logger.error(message)
            require_notify = bool(getattr(self.trader.config, "require_telegram_notify", True))
            if require_notify and str(getattr(self.trader.config, "mode", "")).lower() == "live":
                raise RuntimeError(message)
        self.trader._refresh_market_profile()

    def shutdown(self, mode_text: str | None = None):
        final_mode = mode_text or self.mode_text()
        summary = self.trader._enrich_summary_with_db(self.trader.tracker.get_summary())
        shutdown_notified = send_telegram_message(
            format_shutdown_msg(
                mode_name=final_mode,
                closed_trades=summary["closed_today"],
                realized_pnl=summary["realized_pnl"],
                unrealized_pnl=summary["total_unrealized_pnl"],
            )
        )
        if not shutdown_notified:
            self.logger.error("Telegram notification unavailable: shutdown summary not delivered")

        for client_name in ("_ws_client", "_market_ws_client", "_user_ws_client"):
            client = getattr(self.trader, client_name, None)
            if client:
                try:
                    client.stop()
                except Exception:
                    pass
