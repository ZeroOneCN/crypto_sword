# -*- coding: utf-8 -*-
"""Notification package for Hermes Trader."""

from .telegram_sender import get_telegram_config, send_telegram_message
from .templates_trade import (
    format_close_position_msg,
    format_error_msg,
    format_latency_alert_msg,
    format_open_position_msg,
    format_partial_take_profit_msg,
    format_protection_status_msg,
    format_scan_monitor_msg,
    format_shutdown_msg,
    format_signal_message,
    format_startup_msg,
    format_summary_msg,
    send_signal_alert,
)
from .templates_report import (
    format_accumulation_pool_report,
    format_daily_report_msg,
    format_dark_flow_alert,
    format_period_report_msg,
    format_radar_summary,
    format_short_fuel_report,
)
from .labels import format_direction_label, format_entry_failure_detail, format_protection_failure_detail

__all__ = [name for name in globals() if not name.startswith("_")]
