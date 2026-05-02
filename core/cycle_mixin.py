"""Cycle orchestration mixin for the trading engine."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from adapters.rest_gateway import get_top_symbols_by_change_rest, get_top_symbols_by_volume_rest
from feature_store import feature_store
from telegram_notifier import (
    format_daily_report_msg,
    format_error_msg,
    format_scan_monitor_msg,
    format_summary_msg,
    send_telegram_message,
)

from .monitoring import build_monitor_delta, build_monitor_event, message_signature, stable_monitor_sort

logger = logging.getLogger(__name__)


class CycleMixin:
    """Runtime cycle scheduling and monitor notifications."""

    def _filter_altcoin_symbols(self, symbols: list[str]) -> list[str]:
        if not getattr(self.config, "target_altcoins", False):
            return symbols
        major_set = {symbol.upper() for symbol in self.config.major_symbols}
        return [symbol for symbol in symbols if symbol.upper() not in major_set]

    def _send_daily_report_if_due(self):
        if not self.config.daily_report_enabled or not self.config.daily_report_on_first_cycle:
            return

        today = datetime.now().date().isoformat()
        if self._last_daily_report_sent_for == today:
            return

        report_date = (datetime.now().date() - timedelta(days=1)).isoformat()
        report = self._enrich_daily_report_with_api({}, report_date)

        send_telegram_message(format_daily_report_msg(report))
        self._last_daily_report_sent_for = today

    def _select_deep_scan_symbols(self, candidates: list[str] | None = None) -> list[str]:
        """Pick symbols for expensive deep scan, preferring fresh fast-scan candidates."""
        major_symbols = list(self.config.major_symbols)
        prefer_major = self._market_style_mode in {"major", "balanced"}
        selected_candidates = candidates if candidates is not None else self._fast_scan_candidates()
        if selected_candidates:
            selected_candidates = self._filter_altcoin_symbols(selected_candidates)
        if selected_candidates:
            merged = major_symbols + selected_candidates if prefer_major else selected_candidates + major_symbols
            merged = list(dict.fromkeys(merged))[: self.config.scan_top_n]
            return self._filter_altcoin_symbols(merged)

        if self.config.scan_by_change:
            symbols = get_top_symbols_by_change_rest(
                self.config.scan_top_n,
                min_change=self.config.min_change_pct,
            )
            logger.info(f"🔥 妖币模式(REST) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
            merged = major_symbols + symbols if prefer_major else symbols + major_symbols
            merged = list(dict.fromkeys(merged))[: self.config.scan_top_n]
            return self._filter_altcoin_symbols(merged)

        symbols = get_top_symbols_by_volume_rest(self.config.scan_top_n)
        logger.info(f"📊 成交量模式 - 扫描 {len(symbols)} 个币种：{symbols[:5]}...")
        merged = major_symbols + symbols if prefer_major else symbols + major_symbols
        merged = list(dict.fromkeys(merged))[: self.config.scan_top_n]
        return self._filter_altcoin_symbols(merged)

    def _check_new_day(self):
        today = datetime.now().date().isoformat()
        if today != self._daily_marker:
            self._daily_marker = today
            self.daily_pnl = 0.0
            self.traded_symbols_today.clear()
            self.tracker.reset_daily_summary()
            self._daily_loss_alert_sent = False
            self._consecutive_losses = 0
            self._entry_watchlist.clear()
            if hasattr(self, "_entry_timestamps_today"):
                self._entry_timestamps_today.clear()
            if hasattr(self, "_entry_exception_timestamps_today"):
                self._entry_exception_timestamps_today.clear()
            try:
                balance_info = self._get_account_info_cached(ttl_sec=5.0, force=True)
                self.day_start_balance = float(balance_info.get("availableBalance", self.day_start_balance or 0))
            except Exception:
                pass

    def _is_daily_loss_limit_hit(self) -> bool:
        if self.config.max_daily_loss_pct <= 0:
            return False
        if self.day_start_balance <= 0:
            return False
        limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
        return self.daily_pnl <= -limit_amount

    @staticmethod
    def _signal_score_value(signal: dict[str, Any]) -> float:
        score_data = signal.get("score") or {}
        if isinstance(score_data, dict):
            return float(score_data.get("total_score", score_data.get("total", 0)) or 0)
        return float(score_data or 0)

    def _build_entry_gate_snapshot(self) -> dict[str, Any]:
        """Build daily throttle state before trying new entries."""
        today = datetime.now().date().isoformat()
        now = time.time()
        if hasattr(self, "_entry_timestamps_today"):
            self._entry_timestamps_today = [
                ts for ts in self._entry_timestamps_today if now - float(ts or 0) < 24 * 3600
            ]
            local_entries = len(self._entry_timestamps_today)
        else:
            local_entries = 0
        if hasattr(self, "_entry_exception_timestamps_today"):
            self._entry_exception_timestamps_today = [
                ts for ts in self._entry_exception_timestamps_today if now - float(ts or 0) < 24 * 3600
            ]
            local_exceptions = len(self._entry_exception_timestamps_today)
        else:
            local_exceptions = 0

        db_entries = 0
        try:
            db_entries = int(self.db.get_daily_entry_count(today, mode=self.config.mode) or 0)
        except Exception as exc:
            logger.debug(f"daily entry count skipped: {exc}")
        db_exceptions = 0
        try:
            db_exceptions = int(self.db.get_daily_exception_entry_count(today, mode=self.config.mode) or 0)
        except Exception as exc:
            logger.debug(f"daily exception entry count skipped: {exc}")

        report = {}
        try:
            report = self._get_daily_report_snapshot(ttl_sec=90.0)
        except Exception as exc:
            logger.debug(f"daily report for entry gate skipped: {exc}")

        closed = int(report.get("closed_trades", 0) or 0)
        total_pnl = float(report.get("total_pnl", 0) or 0)
        profit_factor = float(report.get("profit_factor", 0) or 0)
        payoff_ratio = float(report.get("payoff_ratio", 0) or 0)
        weak_day = closed >= 5 and (
            total_pnl < 0
            or (0 < profit_factor < 1.0)
            or (0 < payoff_ratio < 1.0)
        )
        hard_day = closed >= 3 and total_pnl < 0 and (profit_factor < 0.70 or payoff_ratio < 0.70)
        if hard_day:
            soft_cap = min(self.config.max_daily_entries, int(getattr(self.config, "hard_daily_entries", 2) or 2))
            cap_mode = "deep_defensive"
        elif weak_day:
            soft_cap = min(self.config.max_daily_entries, int(getattr(self.config, "weak_daily_entries", 4) or 4))
            cap_mode = "defensive"
        else:
            soft_cap = int(self.config.max_daily_entries)
            cap_mode = "standard"

        return {
            "today": today,
            "daily_entries": max(local_entries, db_entries),
            "exception_entries": max(local_exceptions, db_exceptions),
            "weak_day": weak_day,
            "hard_day": hard_day,
            "soft_cap": max(1, soft_cap),
            "cap_mode": cap_mode,
            "closed": closed,
            "total_pnl": total_pnl,
            "profit_factor": profit_factor,
            "payoff_ratio": payoff_ratio,
        }

    def _soft_cap_override_reason(self, signal: dict[str, Any], snapshot: dict[str, Any]) -> str:
        max_exceptions = int(getattr(self.config, "daily_exception_entries", 0) or 0)
        used_exceptions = int(snapshot.get("exception_entries", 0) or 0)
        if max_exceptions <= 0:
            return ""
        if used_exceptions >= max_exceptions:
            return ""

        score = self._signal_score_value(signal)
        if score < float(getattr(self.config, "exception_entry_score", 95.0) or 95.0):
            return ""

        strategy_line = str(signal.get("strategy_line", "") or "")
        if strategy_line != "趋势突破线":
            return ""

        metrics = signal.get("metrics") or {}
        oi_funding = signal.get("oi_funding") or {}

        def _metric(*keys: str) -> float:
            for key in keys:
                if key in metrics:
                    try:
                        return float(metrics.get(key, 0) or 0)
                    except Exception:
                        return 0.0
                if key in oi_funding:
                    try:
                        return float(oi_funding.get(key, 0) or 0)
                    except Exception:
                        return 0.0
            return 0.0

        change_24h = abs(_metric("change_24h_pct", "price_change_pct"))
        oi_change = abs(_metric("oi_24h_pct", "oi_change_pct"))
        funding = abs(_metric("funding_rate", "funding_current"))

        min_change = float(getattr(self.config, "exception_entry_min_change_pct", 8.0) or 8.0)
        max_change = float(getattr(self.config, "exception_entry_max_change_pct", 35.0) or 35.0)
        min_oi = float(getattr(self.config, "exception_entry_min_oi_pct", 20.0) or 20.0)
        max_oi = float(getattr(self.config, "exception_entry_max_oi_pct", 85.0) or 85.0)
        max_funding = float(getattr(self.config, "exception_entry_max_abs_funding_rate", 0.0025) or 0.0025)

        if not (min_change <= change_24h <= max_change):
            return ""
        if not (min_oi <= oi_change <= max_oi):
            return ""
        if funding > max_funding:
            return ""

        return (
            f"王炸破例 {used_exceptions + 1}/{max_exceptions} "
            f"评分={score:.1f} 24h={change_24h:.1f}% OI={oi_change:.1f}% Funding={funding:.4%}"
        )

    def _entry_throttle_reason(self, signal: dict[str, Any], snapshot: dict[str, Any]) -> str:
        """Reject marginal entries before order execution to stop over-trading."""
        daily_entries = int(snapshot.get("daily_entries", 0) or 0)
        max_daily_entries = int(getattr(self.config, "max_daily_entries", 8) or 8)

        if daily_entries >= max_daily_entries:
            return f"今日开仓数已达上限 {daily_entries}/{max_daily_entries}"

        soft_cap = int(snapshot.get("soft_cap", max_daily_entries) or max_daily_entries)
        if daily_entries >= soft_cap:
            override_reason = self._soft_cap_override_reason(signal, snapshot)
            if override_reason:
                signal["_entry_gate_override"] = "soft_cap_override"
                signal["_entry_gate_note"] = override_reason
                logger.info(f"Entry soft cap override {signal.get('symbol', '')}: {override_reason}")
                return ""
            return (
                f"日内{snapshot.get('cap_mode', 'standard')}限单 {daily_entries}/{soft_cap}，"
                f"仅王炸信号可破例 | PF={float(snapshot.get('profit_factor', 0) or 0):.2f} "
                f"盈亏比={float(snapshot.get('payoff_ratio', 0) or 0):.2f}"
            )

        score = self._signal_score_value(signal)
        min_score = float(getattr(self.config, "min_signal_score_for_entry", 82.0) or 82.0)
        if snapshot.get("weak_day"):
            min_score = max(min_score, float(getattr(self.config, "min_signal_score_defensive", 90.0) or 90.0))
        if score < min_score:
            return f"评分不足 {score:.1f} < {min_score:.1f}"

        return ""

    def _mark_entry_accepted(self, signal: dict[str, Any] | str, snapshot: dict[str, Any]) -> None:
        now = time.time()
        if hasattr(self, "_entry_timestamps_today"):
            self._entry_timestamps_today.append(now)
        if isinstance(signal, dict) and signal.get("_entry_gate_override") and hasattr(self, "_entry_exception_timestamps_today"):
            self._entry_exception_timestamps_today.append(now)
            snapshot["exception_entries"] = int(snapshot.get("exception_entries", 0) or 0) + 1
        snapshot["daily_entries"] = int(snapshot.get("daily_entries", 0) or 0) + 1

    def _send_position_summary(self, summary: dict):
        """发送持仓汇总通知"""
        total_balance = 0.0
        available_balance = 0.0
        daily_report = self._get_daily_report_snapshot()
        try:
            balance_info = self._get_account_info_cached(ttl_sec=10.0)
            available_balance = float(balance_info.get("availableBalance", 0) or 0)
            total_balance = float(balance_info.get("totalWalletBalance", balance_info.get("totalMarginBalance", 0)) or 0)
        except Exception as e:
            logger.debug(f"summary balance fetch skipped: {e}")
        signature = message_signature(
            {
                "positions": summary["positions"],
                "total_pnl": summary["total_unrealized_pnl"],
                "realized_pnl": summary["realized_pnl"],
                "closed_today": summary["closed_today"],
                "total_balance": round(total_balance, 2),
                "available_balance": round(available_balance, 2),
                "win_rate": round(float(daily_report.get("win_rate", 0) or 0), 2),
                "best_trade": daily_report.get("best_trade"),
                "worst_trade": daily_report.get("worst_trade"),
            }
        )
        if signature == self._last_summary_signature:
            logger.debug("summary notify skipped: no material changes")
            return
        self._last_summary_signature = signature
        msg = format_summary_msg(
            positions=summary["positions"],
            total_pnl=summary["total_unrealized_pnl"],
            realized_pnl=summary["realized_pnl"],
            total_balance=total_balance,
            available_balance=available_balance,
            daily_stats=daily_report,
        )
        win_rate = float(daily_report.get("win_rate", 0) or 0)
        avg_pnl = float(daily_report.get("avg_pnl", 0) or 0)
        msg += f"\n<b>胜率</b>  <code>{win_rate:.1f}%</code>"
        msg += f"\n<b>笔均</b>  <code>{avg_pnl:+,.2f} USDT</code>"
        best_trade = daily_report.get("best_trade") or {}
        if best_trade.get("symbol"):
            msg += (
                f"\n<b>最佳</b>  <code>{best_trade.get('symbol')}</code>"
                f"  <code>{float(best_trade.get('pnl', 0) or 0):+,.2f} USDT</code>"
            )
        worst_trade = daily_report.get("worst_trade") or {}
        if worst_trade.get("symbol"):
            msg += (
                f"\n<b>最差</b>  <code>{worst_trade.get('symbol')}</code>"
                f"  <code>{float(worst_trade.get('pnl', 0) or 0):+,.2f} USDT</code>"
            )
        msg += f"\n\n<b>已平仓</b>  <code>{summary['closed_today']}</code> 笔"
        send_telegram_message(msg)

    def _watchlist_monitor_items(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        now = time.time()
        for symbol, watch in self._entry_watchlist.items():
            if now - float(watch.get("last_seen_ts", now)) > max(1800, self._current_scan_interval * 4):
                continue
            items.append(
                {
                    "symbol": symbol,
                    "direction": watch.get("direction", ""),
                    "price": watch.get("price", 0),
                    "metrics": watch.get("metrics", {}) or {},
                    "score": watch.get("score"),
                    "entry_status": "watch",
                    "entry_status_text": "观察中",
                    "entry_note": watch.get("entry_note", ""),
                    "strategy_line": watch.get("strategy_line", ""),
                    "watch_stage": watch.get("watch_stage", ""),
                }
            )
        return stable_monitor_sort(items, self._last_watch_monitor_order)

    def _watch_monitor_interval(self, watch_items: list[dict[str, Any]]) -> int:
        has_breakout = any(item.get("strategy_line") == "趋势突破线" for item in watch_items)
        if has_breakout:
            return max(90, min(self._monitor_interval, max(120, self.config.fast_scan_interval_sec * 2)))
        return max(180, min(self._monitor_interval * 2, max(self._current_scan_interval, 600)))

    def _send_scan_monitor(self, signals: list[dict]):
        """Send a compact Telegram scanner monitor report."""
        now = time.time()
        interval = max(60, min(self._monitor_interval, self._current_scan_interval))
        if now - self._last_monitor_time < interval:
            return
        signals = stable_monitor_sort(list(signals), self._last_scan_monitor_order)
        delta_items, current_snapshot = build_monitor_delta(
            signals,
            self._last_scan_monitor_snapshot,
            "扫描",
            top_n=5,
        )
        signature = message_signature(delta_items)
        if signature == self._last_scan_monitor_signature:
            logger.debug("scan monitor skipped: no material changes")
            return
        if not delta_items:
            logger.debug("scan monitor skipped: delta empty")
            self._last_scan_monitor_snapshot = current_snapshot
            return
        self._last_monitor_time = now
        self._last_scan_monitor_signature = signature
        self._last_scan_monitor_snapshot = current_snapshot
        try:
            msg = format_scan_monitor_msg(
                signals=delta_items,
                scanned_count=self.config.scan_top_n,
                max_items=5,
                report_title="宙斯交易中枢 | 妖币扫描变化",
                count_label="扫描范围",
            )
            send_telegram_message(msg)
        except Exception as e:
            logger.debug(f"扫描监控通知发送失败：{e}")

    def _send_watchlist_monitor(self):
        """Send periodic candidate follow-up even when no deep scan entry is ready."""
        watch_items = self._watchlist_monitor_items()
        if not watch_items:
            self._last_watch_monitor_snapshot = {}
            return
        now = time.time()
        interval = self._watch_monitor_interval(watch_items)
        if now - self._last_watch_monitor_time < interval:
            return
        force_interval = max(900, interval * 3)
        delta_items, current_snapshot = build_monitor_delta(
            watch_items,
            self._last_watch_monitor_snapshot,
            "候选",
            top_n=5,
        )
        signature = message_signature(delta_items)
        if signature == self._last_watch_monitor_signature:
            logger.debug("watch monitor skipped: no material changes")
            return
        if not delta_items:
            if now - self._last_watch_monitor_time < force_interval:
                logger.debug("watch monitor skipped: delta empty")
                self._last_watch_monitor_snapshot = current_snapshot
                return
            logger.info("watch monitor heartbeat: send full snapshot after quiet period")
            delta_items = watch_items[:5]
            self._last_watch_monitor_snapshot = current_snapshot
        self._last_watch_monitor_time = now
        self._last_watch_monitor_signature = signature
        self._last_watch_monitor_snapshot = current_snapshot
        try:
            msg = format_scan_monitor_msg(
                signals=delta_items,
                scanned_count=len(watch_items),
                max_items=5,
                report_title="宙斯交易中枢 | 候选变化",
                count_label="候选范围",
            )
            send_telegram_message(msg)
        except Exception as e:
            logger.debug(f"候选跟踪通知发送失败：{e}")

    def run_scan_cycle(self):
        """Run one fast or deep trading cycle."""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        now = time.time()
        deep_due = self._last_deep_scan_time <= 0 or now - self._last_deep_scan_time >= self._current_scan_interval

        step_started = time.perf_counter()
        self._check_new_day()
        self._send_daily_report_if_due()
        if deep_due:
            self._refresh_market_profile()
            self._refresh_market_style()
        if self._should_sync_positions(now, deep_due):
            with self._state_lock:
                self._sync_positions_with_exchange()
            self._last_position_sync_time = now
        self._record_latency_step(latency_steps, "daily_market_position_sync", step_started)

        cycle_type = "deep" if deep_due else "fast"
        logger.info("=" * 60)
        logger.info(
            f"Scan cycle start | type={cycle_type} | "
            f"positions={self.tracker.get_open_count()}/{self.config.max_open_positions}"
        )

        open_symbols = list(self.tracker.positions.keys())
        if open_symbols:
            step_started = time.perf_counter()
            prices = self.get_current_prices(open_symbols)
            self.tracker.update_all_prices(prices, self.config.trailing_stop_pct)

            exits = self.tracker.check_all_exits(prices)
            for symbol, reason in exits.items():
                logger.info(f"Exit trigger {symbol}: {reason}")
                self.execute_exit(symbol, reason)
            self._record_latency_step(latency_steps, "manage_open_positions", step_started)

        step_started = time.perf_counter()
        self._run_radar_background_scan(now)
        self._record_latency_step(latency_steps, "radar_background_scan", step_started)

        step_started = time.perf_counter()
        candidates = self._fast_scan_candidates()
        self._record_latency_step(latency_steps, "fast_scan_candidates", step_started)
        if not deep_due and self._should_force_ws_deep_scan(now, candidates):
            deep_due = True
            cycle_type = "deep"

        signals = []
        if deep_due:
            step_started = time.perf_counter()
            deep_symbols = self._select_deep_scan_symbols(candidates)
            signals = self.scan_for_signals(deep_symbols, scan_source="merged_selector")
            self._last_deep_scan_time = now
            self._record_latency_step(latency_steps, "deep_scan_signals", step_started)
            ready_count = sum(1 for item in signals if item.get("entry_status") == "ready")
            watch_count = sum(1 for item in signals if item.get("entry_status") == "watch")
            logger.info(f"Trade signals found: {len(signals)} | ready={ready_count} | watch={watch_count}")
            self._send_scan_monitor(signals)
        else:
            next_deep_sec = max(0, int(self._current_scan_interval - (now - self._last_deep_scan_time)))
            logger.info(f"Fast scan only | candidates={len(candidates)} | next_deep={next_deep_sec}s")
            self._send_watchlist_monitor()

        if self._is_daily_loss_limit_hit():
            if not self._daily_loss_alert_sent:
                limit_amount = self.day_start_balance * (self.config.max_daily_loss_pct / 100.0)
                msg = format_error_msg(
                    error_type="日内熔断",
                    message=f"当日亏损已达到 {self.daily_pnl:.2f} USDT，超过限制 {-limit_amount:.2f} USDT，暂停新开仓",
                    component="risk_guard",
                )
                send_telegram_message(msg)
                self._daily_loss_alert_sent = True
            signals = []

        step_started = time.perf_counter()
        if deep_due:
            balance_hint = None
            entries_opened_this_cycle = 0
            entry_gate_snapshot = self._build_entry_gate_snapshot()
            try:
                balance_info = self._get_account_info_cached(ttl_sec=3.0)
                balance_hint = float(balance_info.get("availableBalance", 0) or 0)
            except Exception:
                balance_hint = None
            for signal in signals:
                if entries_opened_this_cycle >= int(getattr(self.config, "max_entries_per_cycle", 1) or 1):
                    logger.info(
                        f"Entry throttle: cycle entry cap reached "
                        f"{entries_opened_this_cycle}/{self.config.max_entries_per_cycle}"
                    )
                    break
                if self.tracker.get_open_count() >= self.config.max_open_positions:
                    logger.info(f"Max open positions reached ({self.config.max_open_positions})")
                    break
                if signal.get("entry_status") != "ready":
                    continue
                throttle_reason = self._entry_throttle_reason(signal, entry_gate_snapshot)
                if throttle_reason:
                    logger.info(f"Entry throttle skip {signal.get('symbol', '')}: {throttle_reason}")
                    continue
                signal_price = float(signal.get("price", 0) or 0)
                score_conf = str((signal.get("score") or {}).get("confidence", "") or "")
                status_text = str(signal.get("entry_status_text", "") or "")
                watch_stage = str(signal.get("watch_stage", "") or "")
                entry_note = str(signal.get("entry_note", "") or "")
                guard_text = f"{status_text}|{watch_stage}|{entry_note}|{score_conf}"
                if signal_price <= 0:
                    logger.warning(f"entry guard skip {signal.get('symbol', '')}: invalid signal price={signal_price}")
                    continue
                if any(token in guard_text for token in ("失效", "淘汰", "移出监控", "状态变更")):
                    logger.warning(
                        f"entry guard skip {signal.get('symbol', '')}: blocked by monitor state [{guard_text}]"
                    )
                    continue
                if balance_hint is not None:
                    signal["_balance_hint"] = balance_hint

                position = self.execute_entry(signal)
                if position:
                    with self._state_lock:
                        self.tracker.add_position(position)
                        self.traded_symbols_today.add(signal["symbol"])
                        entries_opened_this_cycle += 1
                        self._mark_entry_accepted(signal, entry_gate_snapshot)
                        self._mark_watch_in_position(
                            signal["symbol"],
                            getattr(position, "strategy_line", signal.get("strategy_line", "")),
                            note="已开仓，继续跟踪回撤与再入场机会",
                        )
        self._record_latency_step(latency_steps, "execute_entries", step_started)

        step_started = time.perf_counter()
        if self._positions_need_summary_refresh():
            with self._state_lock:
                self._sync_positions_with_exchange()
        summary = self._enrich_summary_with_db(self.tracker.get_summary())
        daily_report = self._get_daily_report_snapshot()
        monitor_event = build_monitor_event(
            open_positions=summary["open_positions"],
            max_positions=self.config.max_open_positions,
            unrealized_pnl=summary["total_unrealized_pnl"],
            realized_pnl=summary["realized_pnl"],
            closed_today=summary["closed_today"],
            entry_protection=daily_report.get("entry_protection") if isinstance(daily_report, dict) else None,
        )
        logger.info(f"monitor_event {message_signature(monitor_event)}")
        feature_store.append_event(monitor_event)
        logger.info(
            f"Position summary: {summary['open_positions']} open | "
            f"unrealized PnL=${summary['total_unrealized_pnl']:.2f} | "
            f"realized today=${summary['realized_pnl']:.2f} | "
            f"closed today={summary['closed_today']}"
        )

        current_time = time.time()
        if current_time - self._last_summary_time >= self._summary_interval:
            self._send_position_summary(summary)
            self._last_summary_time = current_time
        self._record_latency_step(latency_steps, "summary_notify", step_started)

        self.last_scan_time = datetime.now()
        self._emit_latency_trace(f"run_scan_cycle_{cycle_type}", trace_started, latency_steps)
