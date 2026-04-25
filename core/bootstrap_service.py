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
        return f"{self.trader.config.mode_emoji} {self.trader.config.mode_name} 模式"

    def log_startup_banner(self):
        mode_text = self.mode_text()
        self.logger.info("=" * 60)
        self.logger.info(f"🗡️ {mode_text} 启动")
        self.logger.info(f"🔡 杠杆: {self.trader.config.leverage}x | 风险: {self.trader.config.risk_per_trade_pct}%")
        self.logger.info(
            f"🛡️ 止损: {self.trader.config.stop_loss_pct}% | 止盈: {self.trader.config.take_profit_pct}% "
            f"({self.trader.config.take_profit_mode})"
        )
        self.logger.info(
            f"👀 扫描: 前 {self.trader.config.scan_top_n} 币种 | 间隔: {self.trader.config.scan_interval_sec}s"
        )
        self.logger.info(f"📊 最大持仓: {self.trader.config.max_open_positions}")
        self.logger.info("=" * 60)

    def startup(self):
        try:
            self.trader.day_start_balance = self.trader._run_health_checks()
            self.trader._start_market_ticker_stream()
            self.trader._start_user_data_stream()
            self.trader._start_background_protection_audit(source="startup_audit")
            self.logger.info(f"🚀 启动健康检查通过 | 可用余额: ${self.trader.day_start_balance:.2f}")
        except Exception as e:
            self.logger.error(f"❌ 启动健康检查失败: {e}")
            send_telegram_message(
                format_error_msg(
                    error_type="启动健康检查失败",
                    message=str(e),
                    component="startup_checks",
                )
            )
            raise

        send_telegram_message(
            format_startup_msg(
                mode_name=self.mode_text(),
                leverage=self.trader.config.leverage,
                risk_pct=self.trader.config.risk_per_trade_pct,
                stop_loss_pct=self.trader.config.stop_loss_pct,
                take_profit_pct=self.trader.config.take_profit_pct,
                scan_top_n=self.trader.config.scan_top_n,
                scan_interval_sec=self.trader.config.scan_interval_sec,
                max_positions=self.trader.config.max_open_positions,
            )
        )
        self.trader._refresh_market_profile()

    def shutdown(self, mode_text: str | None = None):
        final_mode = mode_text or self.mode_text()
        summary = self.trader._enrich_summary_with_db(self.trader.tracker.get_summary())
        send_telegram_message(
            format_shutdown_msg(
                mode_name=final_mode,
                closed_trades=summary["closed_today"],
                realized_pnl=summary["realized_pnl"],
                unrealized_pnl=summary["total_unrealized_pnl"],
            )
        )

        for client_name in ("_ws_client", "_market_ws_client", "_user_ws_client"):
            client = getattr(self.trader, client_name, None)
            if client:
                try:
                    client.stop()
                except Exception:
                    pass
