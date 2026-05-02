"""Entry confirmation and watchlist state machine mixin."""

from __future__ import annotations

import logging
import time
from typing import Any

from signal_enhancer import analyze_trend, analyze_volume, get_klines

logger = logging.getLogger(__name__)


class ConfirmationMixin:
    def _prune_entry_watchlist(self):
        """Drop expired observation candidates so stale breakouts do not auto-fire later."""
        if not self._entry_watchlist:
            return
        now = time.time()
        timeout_sec = max(300, int(self.config.entry_confirmation_timeout_sec))
        expired_symbols = [
            symbol
            for symbol, item in self._entry_watchlist.items()
            if now - float(item.get("first_seen_ts", now)) >= timeout_sec
        ]
        for symbol in expired_symbols:
            self._entry_watchlist.pop(symbol, None)
            logger.info(f"🗑️ {symbol} 候选观察超时，已淘汰")

    def _load_confirmation_trend(self, symbol: str) -> dict[str, Any]:
        """Reuse cached klines to confirm 1h trend and 15m reclaim."""
        klines_1h = get_klines(symbol, interval="1h", limit=50) or []
        klines_15m = get_klines(symbol, interval="15m", limit=50) or []
        klines_5m = get_klines(symbol, interval="5m", limit=50) or []
        trend_1h = analyze_trend(klines_1h)
        trend_15m = analyze_trend(klines_15m)
        trend_5m = analyze_trend(klines_5m)
        volume_5m = analyze_volume(klines_5m) if klines_5m else {"score": 0, "volume_ratio": 0, "volume_trend": "UNKNOWN"}
        return {
            "1h": trend_1h,
            "15m": trend_15m,
            "5m": trend_5m,
            "5m_volume": volume_5m,
            "15m_klines": klines_15m,
            "5m_klines": klines_5m,
        }

    def _short_tf_breakout_ready(self, trend: dict[str, Any], direction: str, current_price: float) -> bool:
        trend_5m = trend.get("5m", {}) or {}
        ma5 = float(trend_5m.get("ma5", 0) or 0)
        ma_alignment = str(trend_5m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        if direction == "LONG":
            return ma_alignment == "BULLISH" and current_price >= ma5 > 0
        return ma_alignment == "BEARISH" and 0 < current_price <= ma5

    def _volume_reclaim_ready(self, trend: dict[str, Any]) -> bool:
        volume_5m = trend.get("5m_volume", {}) or {}
        volume_ratio = float(volume_5m.get("volume_ratio", 0) or 0)
        volume_trend = str(volume_5m.get("volume_trend", "UNKNOWN") or "UNKNOWN")
        return volume_ratio >= self.config.reclaim_volume_ratio and volume_trend in {"INCREASING", "FLAT"}

    def _required_pullback_pct(self, metrics: dict[str, Any], score_total: float = 0.0) -> float:
        """Use a shallower pullback for strong momentum leaders and a deeper one for ordinary setups."""
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))

        if score_total >= 82 and change_24h >= 22 and oi_change >= 50:
            return max(1.5, self.config.shallow_pullback_pct)
        if score_total >= self.config.momentum_entry_score and change_24h >= 15 and oi_change >= self.config.momentum_entry_min_oi_pct:
            return max(2.0, self.config.shallow_pullback_pct + 0.5)
        if (
            getattr(self.config, "ma_reentry_enabled", True)
            and score_total >= getattr(self.config, "ma_reentry_score", 58.0)
            and change_24h >= getattr(self.config, "ma_reentry_min_change_pct", 4.0)
            and oi_change >= getattr(self.config, "ma_reentry_min_oi_pct", 5.0)
        ):
            return max(0.5, float(getattr(self.config, "ma_reentry_min_pullback_pct", 0.8)))
        if change_24h >= 15 and oi_change >= 35:
            return max(2.0, self.config.shallow_pullback_pct + 0.4)
        return self.config.min_pullback_pct

    def _soft_breakout_candidate(self, metrics: dict[str, Any], score_total: float) -> bool:
        """Allow elite movers with moderate OI expansion to join the breakout line."""
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        return (
            score_total >= 70.0
            and change_24h >= 15.0
            and oi_change >= 20.0
            and abs(funding) < self.config.max_abs_funding_rate
        )

    def _early_trend_alignment_ok(
        self,
        direction: str,
        higher_alignment: str,
        *,
        score_total: float,
        change_24h: float,
        oi_change: float = 0.0,
    ) -> bool:
        """Let elite setups enter before the 1h MA fully flips, but never against it."""
        higher_alignment = str(higher_alignment or "NEUTRAL").upper()
        strong_hotspot = score_total >= 82.0 and abs(change_24h) >= 7.0
        strong_flow = score_total >= 78.0 and abs(oi_change) >= 18.0 and abs(change_24h) >= 6.0
        if direction == "LONG":
            if strong_hotspot or strong_flow:
                return higher_alignment != "BEARISH"
            return higher_alignment == "BULLISH"
        if strong_hotspot or strong_flow:
            return higher_alignment != "BULLISH"
        return higher_alignment == "BEARISH"

    def _is_accumulation_candidate(self, metrics: dict[str, Any], score_total: float) -> bool:
        """Detect early accumulation before a full breakout extension prints."""
        if not self.config.accumulation_entry_enabled:
            return False
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        volume_mult = float(metrics.get("volume_24h_mult", 0) or 0)
        range_position = float(metrics.get("range_position_24h_pct", 50) or 50)
        return (
            score_total >= self.config.accumulation_entry_score
            and 0 < change_24h <= self.config.accumulation_entry_max_change_pct
            and oi_change >= self.config.accumulation_entry_min_oi_pct
            and volume_mult >= self.config.accumulation_entry_min_volume_mult
            and range_position <= self.config.accumulation_entry_max_range_pct
            and abs(funding) < self.config.max_abs_funding_rate
        )

    def _is_ma_reentry_candidate(self, metrics: dict[str, Any], score_total: float) -> bool:
        """Detect a second-launch setup after price reclaims moving averages."""
        if not getattr(self.config, "ma_reentry_enabled", True):
            return False
        change_24h = abs(float(metrics.get("change_24h_pct", 0) or 0))
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = abs(float(metrics.get("funding_rate", 0) or 0))
        range_position = float(metrics.get("range_position_24h_pct", 50) or 50)
        return (
            score_total >= float(getattr(self.config, "ma_reentry_score", 58.0))
            and float(getattr(self.config, "ma_reentry_min_change_pct", 4.0))
            <= change_24h
            <= float(getattr(self.config, "ma_reentry_max_change_pct", 28.0))
            and oi_change >= float(getattr(self.config, "ma_reentry_min_oi_pct", 5.0))
            and funding < float(getattr(self.config, "max_abs_funding_rate", 0.004))
            and range_position <= float(getattr(self.config, "max_range_position_pct", 95.0))
        )

    def _strategy_line_for_signal(self, signal: dict[str, Any]) -> str:
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        raw_change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        change_24h = abs(raw_change_24h)
        oi_change = abs(float(metrics.get("oi_24h_pct", 0) or 0))
        funding = float(metrics.get("funding_rate", 0) or 0)
        direction = str(signal.get("direction", "") or "")
        if (
            self.config.momentum_entry_enabled
            and score_total >= self.config.momentum_entry_score
            and change_24h >= self.config.momentum_entry_min_change_pct
            and oi_change >= self.config.momentum_entry_min_oi_pct
        ):
            return "趋势突破线"
        if score_total >= 70.0 and change_24h >= 15.0 and oi_change >= 28.0 and funding <= 0:
            return "趋势突破线"
        if self._is_accumulation_candidate(metrics, score_total):
            return "趋势突破线"
        if self._is_ma_reentry_candidate(metrics, score_total) and (
            (direction == "LONG" and raw_change_24h > 0) or (direction == "SHORT" and raw_change_24h < 0)
        ):
            return "均线二启线"
        if self.config.momentum_entry_enabled and self._soft_breakout_candidate(metrics, score_total):
            return "趋势突破线"
        return "回踩确认线"

    def _current_pullback_pct(self, watch: dict[str, Any], direction: str, current_price: float) -> float:
        if direction == "LONG":
            anchor_price = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            return ((anchor_price - current_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0
        anchor_price = min(float(watch.get("lowest_price", current_price) or current_price), current_price)
        return ((current_price - anchor_price) / anchor_price * 100.0) if anchor_price > 0 else 0.0

    def _update_watch_state(
        self,
        watch: dict[str, Any],
        *,
        strategy_line: str,
        stage_name: str,
        entry_note: str,
        required_pullback: float,
        current_pullback: float,
        trend: dict[str, Any] | None = None,
    ):
        watch["strategy_line"] = strategy_line
        watch["watch_stage"] = stage_name
        watch["entry_note"] = entry_note
        watch["required_pullback_pct"] = required_pullback
        watch["current_pullback_pct"] = current_pullback
        if trend is not None:
            watch["confirmation_trend"] = trend

    def _mark_watch_in_position(self, symbol: str, strategy_line: str, note: str = ""):
        now = time.time()
        watch = self._entry_watchlist.get(symbol)
        if not watch:
            watch = {
                "symbol": symbol,
                "direction": "",
                "stage": "in_position",
                "first_seen_ts": now,
                "last_seen_ts": now,
                "first_price": 0.0,
                "highest_price": 0.0,
                "lowest_price": 0.0,
                "score_total": 0.0,
                "pullback_seen": True,
                "required_pullback_pct": 0.0,
                "current_pullback_pct": 0.0,
                "metrics": {},
                "score": {},
            }
            self._entry_watchlist[symbol] = watch
        watch["strategy_line"] = strategy_line
        watch["watch_stage"] = "持仓中"
        watch["entry_note"] = note or "已开仓，继续跟踪后续再入场机会"
        watch["last_seen_ts"] = now

    def _is_momentum_entry_ready(self, signal: dict[str, Any], trend: dict[str, Any], current_price: float) -> tuple[bool, str]:
        if not self.config.momentum_entry_enabled:
            return False, ""
        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        funding = abs(float(metrics.get("funding_rate", 0) or 0))
        if score_total < self.config.momentum_entry_score and not self._soft_breakout_candidate(metrics, score_total):
            return False, ""
        if abs(change_24h) < self.config.momentum_entry_min_change_pct:
            return False, ""
        if oi_change < self.config.momentum_entry_min_oi_pct and not self._soft_breakout_candidate(metrics, score_total):
            return False, ""
        if funding >= self.config.max_abs_funding_rate:
            return False, ""
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5 = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        higher_alignment = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)
        if direction == "LONG":
            ready = higher_alignment == "BULLISH" and ma_alignment == "BULLISH" and current_price >= ma5 > 0 and short_tf_ok and change_24h > 0
        else:
            ready = higher_alignment == "BEARISH" and ma_alignment == "BEARISH" and 0 < current_price <= ma5 and short_tf_ok and change_24h < 0
        if not ready:
            return False, ""
        return True, f"强趋势动量确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%，OI {oi_change:+.1f}%"

    def _is_trend_continuation_ready(self, signal: dict[str, Any], trend: dict[str, Any], current_price: float) -> tuple[bool, str]:
        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        funding = abs(float(metrics.get("funding_rate", 0) or 0))
        if score_total < 65 or abs(change_24h) < 10.0 or funding >= self.config.max_abs_funding_rate * 0.95:
            return False, ""
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5_15m = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment_15m = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)
        if direction == "LONG":
            ready = (
                change_24h > 0
                and self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=change_24h,
                    oi_change=oi_change,
                )
                and ma_alignment_15m == "BULLISH"
                and current_price >= ma5_15m > 0
                and short_tf_ok
            )
        else:
            ready = (
                change_24h < 0
                and self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=change_24h,
                    oi_change=oi_change,
                )
                and ma_alignment_15m == "BEARISH"
                and 0 < current_price <= ma5_15m
                and short_tf_ok
            )
        if not ready:
            return False, ""
        return True, f"热点延续确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%"

    def _is_flow_reclaim_ready(
        self, signal: dict[str, Any], trend: dict[str, Any], current_price: float, pullback_pct: float
    ) -> tuple[bool, str]:
        direction = signal.get("direction", "")
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        funding = float(metrics.get("funding_rate", 0) or 0)
        trend_1h = trend.get("1h", {}) or {}
        trend_5m = trend.get("5m", {}) or {}
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma5_5m = float(trend_5m.get("ma5", 0) or 0)
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)
        if score_total < 70 or oi_change < 28:
            return False, ""
        if direction == "LONG":
            ready = (
                funding <= self.config.max_abs_funding_rate
                and self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=float(metrics.get("change_24h_pct", 0) or 0),
                    oi_change=oi_change,
                )
                and current_price >= ma5_5m > 0
                and short_tf_ok
                and pullback_pct >= max(0.6, self.config.min_pullback_pct * 0.5)
            )
        else:
            ready = (
                funding >= -self.config.max_abs_funding_rate
                and self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=float(metrics.get("change_24h_pct", 0) or 0),
                    oi_change=oi_change,
                )
                and 0 < current_price <= ma5_5m
                and short_tf_ok
                and pullback_pct >= max(0.6, self.config.min_pullback_pct * 0.5)
            )
        if not ready:
            return False, ""
        funding_text = f"{funding:+.4%}" if abs(funding) < 1 else f"{funding:+.2f}"
        return True, f"资金/OI快线入场：评分 {score_total:.1f}，OI {oi_change:+.1f}%，费率 {funding_text}"

    def _is_accumulation_entry_ready(self, signal: dict[str, Any], trend: dict[str, Any], current_price: float) -> tuple[bool, str]:
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        if not self._is_accumulation_candidate(metrics, score_total):
            return False, ""
        direction = signal.get("direction", "")
        trend_1h = trend.get("1h", {}) or {}
        trend_15m = trend.get("15m", {}) or {}
        ma5_15m = float(trend_15m.get("ma5", 0) or 0)
        ma_alignment_1h = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        ma_alignment_15m = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        if direction == "LONG":
            ready = (
                self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=change_24h,
                    oi_change=oi_change,
                )
                and ma_alignment_15m == "BULLISH"
                and current_price >= ma5_15m > 0
                and short_tf_ok
            )
        else:
            ready = (
                self._early_trend_alignment_ok(
                    direction,
                    ma_alignment_1h,
                    score_total=score_total,
                    change_24h=change_24h,
                    oi_change=oi_change,
                )
                and ma_alignment_15m == "BEARISH"
                and 0 < current_price <= ma5_15m
                and short_tf_ok
            )
        if not ready:
            return False, ""
        return True, f"吸筹暗流确认：评分 {score_total:.1f}，24h {change_24h:+.1f}%，OI {oi_change:+.1f}%"

    def _is_ma_reentry_ready(self, signal: dict[str, Any], trend: dict[str, Any], current_price: float) -> tuple[bool, str]:
        """Confirm the MA second-launch shape: pullback holds MA20, then reclaims MA5."""
        metrics = signal.get("metrics", {}) or {}
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        if not self._is_ma_reentry_candidate(metrics, score_total):
            return False, ""

        klines_15m = trend.get("15m_klines") or []
        if len(klines_15m) < 24 or current_price <= 0:
            return False, ""

        closes = [float(item.get("close", 0) or 0) for item in klines_15m]
        highs = [float(item.get("high", 0) or 0) for item in klines_15m]
        lows = [float(item.get("low", 0) or 0) for item in klines_15m]
        if min(closes[-20:] or [0]) <= 0 or min(lows[-20:] or [0]) <= 0:
            return False, ""

        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes[-20:]) / 20
        prev_ma5 = sum(closes[-6:-1]) / 5
        prev_ma10 = sum(closes[-11:-1]) / 10
        if ma20 <= 0:
            return False, ""

        direction = signal.get("direction", "")
        change_24h = float(metrics.get("change_24h_pct", 0) or 0)
        min_pullback = max(0.3, float(getattr(self.config, "ma_reentry_min_pullback_pct", 0.8)))
        max_pullback = max(min_pullback, float(getattr(self.config, "ma_reentry_max_pullback_pct", 7.5)))
        tolerance = max(0.0, float(getattr(self.config, "ma_reentry_ma_tolerance_pct", 1.2))) / 100.0
        max_extension = max(0.5, float(getattr(self.config, "ma_reentry_max_extension_pct", 7.0)))

        volume_5m = trend.get("5m_volume", {}) or {}
        volume_ratio = float(volume_5m.get("volume_ratio", 0) or 0)
        min_volume_ratio = float(getattr(self.config, "ma_reentry_min_volume_ratio", 0.85))
        volume_ok = volume_ratio <= 0 or volume_ratio >= min_volume_ratio or self._volume_reclaim_ready(trend)
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price)

        if direction == "LONG":
            if change_24h <= 0:
                return False, ""
            recent_high = max(highs[-14:-2] or highs[-14:])
            recent_low = min(lows[-8:])
            pullback_pct = (recent_high - recent_low) / recent_high * 100.0 if recent_high > 0 else 0.0
            touched_ma_zone = recent_low <= ma10 * (1 + tolerance) or recent_low <= ma20 * (1 + tolerance)
            held_ma20 = recent_low >= ma20 * (1 - tolerance)
            reclaimed_ma5 = current_price >= ma5 and closes[-1] >= ma5
            ma_stack_ok = ma5 >= ma10 * 0.998 and ma10 >= ma20 * 0.995
            slope_ok = ma5 >= prev_ma5 or ma10 >= prev_ma10
            extension_pct = (current_price - ma20) / ma20 * 100.0
            ready = (
                min_pullback <= pullback_pct <= max_pullback
                and touched_ma_zone
                and held_ma20
                and reclaimed_ma5
                and ma_stack_ok
                and slope_ok
                and short_tf_ok
                and extension_pct <= max_extension
                and volume_ok
            )
            action_text = "重站MA5"
        else:
            if change_24h >= 0:
                return False, ""
            recent_low = min(lows[-14:-2] or lows[-14:])
            recent_high = max(highs[-8:])
            pullback_pct = (recent_high - recent_low) / recent_low * 100.0 if recent_low > 0 else 0.0
            touched_ma_zone = recent_high >= ma10 * (1 - tolerance) or recent_high >= ma20 * (1 - tolerance)
            held_ma20 = recent_high <= ma20 * (1 + tolerance)
            reclaimed_ma5 = current_price <= ma5 and closes[-1] <= ma5
            ma_stack_ok = ma5 <= ma10 * 1.002 and ma10 <= ma20 * 1.005
            slope_ok = ma5 <= prev_ma5 or ma10 <= prev_ma10
            extension_pct = (ma20 - current_price) / ma20 * 100.0
            ready = (
                min_pullback <= pullback_pct <= max_pullback
                and touched_ma_zone
                and held_ma20
                and reclaimed_ma5
                and ma_stack_ok
                and slope_ok
                and short_tf_ok
                and extension_pct <= max_extension
                and volume_ok
            )
            action_text = "跌回MA5"

        if not ready:
            return False, ""
        oi_change = float(metrics.get("oi_24h_pct", 0) or 0)
        return (
            True,
            f"均线二启确认：回踩 {pullback_pct:.2f}%，守住MA20，{action_text}，"
            f"评分 {score_total:.1f}，OI {oi_change:+.1f}%，量比 {volume_ratio:.2f}",
        )

    # Keep the original confirmation state-machine body unchanged for behavior stability.
    def _apply_entry_confirmation(self, signal: dict[str, Any]) -> dict[str, Any]:
        signal["entry_status"] = "ready"
        signal["entry_status_text"] = "确认入场"
        signal["entry_note"] = ""
        if not self.config.entry_confirmation_enabled:
            return signal
        symbol = signal["symbol"]
        direction = signal["direction"]
        current_price = float(signal.get("price", 0) or 0)
        score_total = float((signal.get("score") or {}).get("total_score", 0) or 0)
        required_pullback = self._required_pullback_pct(signal.get("metrics", {}) or {}, score_total)
        now = time.time()
        watch = self._entry_watchlist.get(symbol)
        if watch and watch.get("direction") != direction:
            self._entry_watchlist.pop(symbol, None)
            signal["entry_status"] = "invalid"; signal["entry_status_text"] = "失效淘汰"; signal["entry_note"] = "方向反转"; return signal
        if watch and score_total > 0:
            previous_score = float(watch.get("score_total", 0) or 0)
            threshold = max(40.0, previous_score * 0.7) if previous_score > 0 else 40.0
            if score_total < threshold:
                self._entry_watchlist.pop(symbol, None)
                signal["entry_status"] = "invalid"; signal["entry_status_text"] = "失效淘汰"; signal["entry_note"] = f"评分回落至 {score_total:.1f}"; return signal
        if not watch:
            strategy_line = self._strategy_line_for_signal(signal)
            initial_note = "首次发现，等待回踩确认"
            trend = self._load_confirmation_trend(symbol)
            continuation_ready, continuation_note = self._is_trend_continuation_ready(signal, trend, current_price)
            momentum_ready, momentum_note = self._is_momentum_entry_ready(signal, trend, current_price)
            accumulation_ready, accumulation_note = self._is_accumulation_entry_ready(signal, trend, current_price)
            ma_reentry_ready, ma_reentry_note = self._is_ma_reentry_ready(signal, trend, current_price)
            if strategy_line == "趋势突破线":
                initial_note = "首次发现，等待趋势延续确认"
            elif strategy_line == "均线二启线":
                initial_note = "首次发现，等待回踩守住均线后二次启动"
            if continuation_ready or momentum_ready or accumulation_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "突破确认入场"; signal["strategy_line"] = "趋势突破线"; signal["watch_stage"] = "首发现直通"; signal["entry_note"] = accumulation_note or momentum_note or continuation_note; signal["confirmation_trend"] = trend; return signal
            if strategy_line == "均线二启线" and ma_reentry_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "二启确认入场"; signal["strategy_line"] = "均线二启线"; signal["watch_stage"] = "均线二启"; signal["entry_note"] = ma_reentry_note; signal["confirmation_trend"] = trend; return signal
            self._entry_watchlist[symbol] = {"symbol": symbol,"direction": direction,"stage": signal.get("stage", ""),"first_seen_ts": now,"last_seen_ts": now,"first_price": current_price,"highest_price": current_price,"lowest_price": current_price,"score_total": score_total,"pullback_seen": False,"strategy_line": strategy_line,"watch_stage": "首发现","required_pullback_pct": required_pullback,"current_pullback_pct": 0.0,"entry_note": initial_note,"price": current_price,"metrics": signal.get("metrics", {}),"score": signal.get("score"),}
            signal["entry_status"] = "watch"; signal["entry_status_text"] = "观察中"; signal["strategy_line"] = self._entry_watchlist[symbol]["strategy_line"]; signal["watch_stage"] = "首发现"; signal["entry_note"] = initial_note; return signal
        watch["last_seen_ts"] = now; watch["stage"] = signal.get("stage", watch.get("stage", "")); watch["score_total"] = score_total or float(watch.get("score_total", 0) or 0); watch["price"] = current_price; watch["metrics"] = signal.get("metrics", {}); watch["score"] = signal.get("score"); watch["strategy_line"] = self._strategy_line_for_signal(signal)
        if current_price > 0:
            watch["highest_price"] = max(float(watch.get("highest_price", current_price) or current_price), current_price)
            low_seed = float(watch.get("lowest_price", current_price) or current_price)
            watch["lowest_price"] = current_price if low_seed <= 0 else min(low_seed, current_price)
        pullback_pct = self._current_pullback_pct(watch, direction, current_price)
        if pullback_pct >= required_pullback:
            watch["pullback_seen"] = True
        if not watch.get("pullback_seen"):
            trend = self._load_confirmation_trend(symbol)
            momentum_ready, momentum_note = self._is_momentum_entry_ready(signal, trend, current_price)
            accumulation_ready, accumulation_note = self._is_accumulation_entry_ready(signal, trend, current_price)
            ma_reentry_ready, ma_reentry_note = self._is_ma_reentry_ready(signal, trend, current_price)
            if momentum_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "动量确认入场"; signal["strategy_line"] = "趋势突破线"; signal["watch_stage"] = "动量突破"; signal["entry_note"] = momentum_note; signal["confirmation_trend"] = trend; return signal
            if accumulation_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "吸筹暗流入场"; signal["strategy_line"] = "趋势突破线"; signal["watch_stage"] = "吸筹启动"; signal["entry_note"] = accumulation_note; signal["confirmation_trend"] = trend; return signal
            if watch.get("strategy_line") == "均线二启线" and ma_reentry_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "二启确认入场"; signal["strategy_line"] = "均线二启线"; signal["watch_stage"] = "均线二启"; signal["entry_note"] = ma_reentry_note; signal["confirmation_trend"] = trend; return signal
            if watch.get("strategy_line") == "趋势突破线":
                continuation_ready, continuation_note = self._is_trend_continuation_ready(signal, trend, current_price)
                if continuation_ready:
                    signal["entry_status"] = "ready"; signal["entry_status_text"] = "突破确认入场"; signal["strategy_line"] = "趋势突破线"; signal["watch_stage"] = "趋势延续"; signal["entry_note"] = continuation_note; signal["confirmation_trend"] = trend; return signal
            if watch.get("strategy_line") == "均线二启线":
                stage_name = "二启待命"
                entry_note = "等待回踩均线后重新站上短均线"
            else:
                stage_name = "趋势待命" if watch.get("strategy_line") == "趋势突破线" else "回踩等待"
                entry_note = "等待趋势延续确认" if watch.get("strategy_line") == "趋势突破线" else f"等待至少 {required_pullback:.1f}% 回踩"
            self._update_watch_state(watch, strategy_line=watch.get("strategy_line", "回踩确认线"), stage_name=stage_name, entry_note=entry_note, required_pullback=required_pullback, current_pullback=pullback_pct, trend=trend)
            signal["entry_status"] = "watch"; signal["entry_status_text"] = "观察中"; signal["strategy_line"] = watch.get("strategy_line", "回踩确认线"); signal["watch_stage"] = stage_name; signal["entry_note"] = entry_note; return signal
        trend = self._load_confirmation_trend(symbol)
        trend_1h = trend.get("1h", {}) or {}; trend_15m = trend.get("15m", {}) or {}
        ma5 = float(trend_15m.get("ma5", 0) or 0); ma_alignment = str(trend_15m.get("ma_alignment", "NEUTRAL") or "NEUTRAL"); higher_alignment = str(trend_1h.get("ma_alignment", "NEUTRAL") or "NEUTRAL")
        short_tf_ok = self._short_tf_breakout_ready(trend, direction, current_price); volume_ok = self._volume_reclaim_ready(trend)
        if direction == "LONG":
            trend_ok = higher_alignment == "BULLISH" and ma_alignment == "BULLISH" and current_price >= ma5 > 0 and short_tf_ok
        else:
            trend_ok = higher_alignment == "BEARISH" and ma_alignment == "BEARISH" and 0 < current_price <= ma5 and short_tf_ok
        ma_reentry_ready, ma_reentry_note = self._is_ma_reentry_ready(signal, trend, current_price)
        if watch.get("strategy_line") == "均线二启线":
            if ma_reentry_ready:
                signal["entry_status"] = "ready"; signal["entry_status_text"] = "二启确认入场"; signal["strategy_line"] = "均线二启线"; signal["watch_stage"] = "均线二启"; signal["entry_note"] = ma_reentry_note; signal["confirmation_trend"] = trend; return signal
            self._update_watch_state(watch, strategy_line="均线二启线", stage_name="二启均线确认", entry_note="已回踩，等待守住MA20并重新站上MA5", required_pullback=required_pullback, current_pullback=pullback_pct, trend=trend)
            signal["entry_status"] = "watch"; signal["entry_status_text"] = "观察中"; signal["strategy_line"] = "均线二启线"; signal["watch_stage"] = "二启均线确认"; signal["entry_note"] = "已回踩，等待守住MA20并重新站上MA5"; return signal
        flow_ready, flow_note = self._is_flow_reclaim_ready(signal, trend, current_price, pullback_pct)
        if flow_ready:
            signal["entry_status"] = "ready"; signal["entry_status_text"] = "快线确认入场"; signal["strategy_line"] = watch.get("strategy_line", "回踩确认线"); signal["watch_stage"] = "资金OI快线"; signal["entry_note"] = flow_note; signal["confirmation_trend"] = trend; return signal
        if not trend_ok:
            self._update_watch_state(watch, strategy_line=watch.get("strategy_line", "回踩确认线"), stage_name="均线确认", entry_note="已回踩，等待 15m 重站均线", required_pullback=required_pullback, current_pullback=pullback_pct, trend=trend)
            signal["entry_status"] = "watch"; signal["entry_status_text"] = "观察中"; signal["strategy_line"] = watch.get("strategy_line", "回踩确认线"); signal["watch_stage"] = "均线确认"; signal["entry_note"] = "已回踩，等待 15m 重站均线"; return signal
        if watch.get("strategy_line") == "回踩确认线" and not volume_ok:
            self._update_watch_state(watch, strategy_line=watch.get("strategy_line", "回踩确认线"), stage_name="量能回归", entry_note=f"已回踩，等待 5m 量能回归 ≥ {self.config.reclaim_volume_ratio:.2f}", required_pullback=required_pullback, current_pullback=pullback_pct, trend=trend)
            signal["entry_status"] = "watch"; signal["entry_status_text"] = "观察中"; signal["strategy_line"] = watch.get("strategy_line", "回踩确认线"); signal["watch_stage"] = "量能回归"; signal["entry_note"] = f"已回踩，等待 5m 量能回归 ≥ {self.config.reclaim_volume_ratio:.2f}"; return signal
        signal["entry_status"] = "ready"; signal["entry_status_text"] = "确认入场"; signal["strategy_line"] = watch.get("strategy_line", "回踩确认线"); signal["watch_stage"] = "触发入场"; signal["entry_note"] = f"回踩 {pullback_pct:.2f}% 后重站 15m 均线"; signal["confirmation_trend"] = trend
        return signal
