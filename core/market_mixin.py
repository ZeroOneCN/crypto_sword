"""Market profile and all-market websocket mixin."""

from __future__ import annotations

import logging
import time
from typing import Any

from adapters.rest_gateway import load_market_overview
from adapters.ws_gateway import get_all_market_ticker_client_class
from telegram_notifier import format_dark_flow_alert, format_radar_summary, send_telegram_message

logger = logging.getLogger(__name__)


class MarketMixin:
    def _run_radar_background_scan(self, now: float):
        """Send lightweight radar notifications from the in-memory watchlist.

        This intentionally reuses symbols already scanned by the strategy loop so
        radar alerts do not add another REST-heavy path or slow down entries.
        """
        interval = max(900, int(getattr(self, "_radar_scan_interval", 3600) or 3600))
        if now - float(getattr(self, "_last_radar_scan_time", 0.0) or 0.0) < interval:
            return

        watchlist = getattr(self, "_entry_watchlist", {}) or {}
        if not watchlist:
            return

        active_items: list[dict[str, Any]] = []
        max_age = max(1800, int(getattr(self, "_current_scan_interval", 300) or 300) * 8)
        for symbol, item in watchlist.items():
            last_seen = float(item.get("last_seen_ts", now) or now)
            if now - last_seen > max_age:
                continue
            metrics = item.get("metrics", {}) or {}
            score = item.get("score", {}) or {}
            score_parts = score.get("scores", {}) if isinstance(score, dict) else {}
            active_items.append(
                {
                    "symbol": str(symbol).upper(),
                    "direction": str(item.get("direction", "") or ""),
                    "stage": str(item.get("stage", "") or ""),
                    "price": float(item.get("price", 0) or 0),
                    "score_total": float((score or {}).get("total_score", item.get("score_total", 0)) or 0),
                    "dark_flow": float(score_parts.get("dark_flow", 0) or 0),
                    "ambush": float(score_parts.get("ambush", 0) or 0),
                    "oi_change_pct": float(metrics.get("oi_24h_pct", metrics.get("oi_change_pct", 0)) or 0),
                    "price_change_pct": float(metrics.get("change_24h_pct", metrics.get("price_change_pct", 0)) or 0),
                    "funding_rate": float(metrics.get("funding_rate", metrics.get("funding_current", 0)) or 0),
                    "market_cap": float((score or {}).get("market_cap_usd", metrics.get("market_cap_usd", 0)) or 0),
                }
            )

        if not active_items:
            return
        self._last_radar_scan_time = now

        oi_items = [item for item in active_items if abs(item["oi_change_pct"]) >= 15.0]
        dark_items = [
            item
            for item in active_items
            if item["dark_flow"] >= 45.0
            or (
                item["oi_change_pct"] >= 15.0
                and abs(item["price_change_pct"]) <= 10.0
                and item["score_total"] >= 55.0
            )
        ]
        pool_items = [
            item
            for item in active_items
            if item["ambush"] >= 40.0
            or (
                item["score_total"] >= 60.0
                and item["oi_change_pct"] >= 8.0
                and abs(item["price_change_pct"]) <= 20.0
            )
        ]
        short_fuel_items = [
            item
            for item in active_items
            if item["direction"] == "LONG" and item["funding_rate"] <= -0.0003 and item["oi_change_pct"] >= 10.0
        ]

        top_dark = max(dark_items, key=lambda item: (item["dark_flow"], item["score_total"]), default=None)
        summary_signature = "|".join(
            [
                str(len(pool_items)),
                str(len(oi_items)),
                str(len(dark_items)),
                str(len(short_fuel_items)),
                str(top_dark["symbol"] if top_dark else ""),
                f"{top_dark['score_total']:.1f}" if top_dark else "",
            ]
        )
        if summary_signature != getattr(self, "_last_radar_summary_signature", ""):
            self._last_radar_summary_signature = summary_signature
            if pool_items or oi_items or dark_items or short_fuel_items:
                top_text = None
                if top_dark:
                    top_text = (
                        f"{top_dark['symbol']} 评分 {top_dark['score_total']:.1f} | "
                        f"暗流 {top_dark['dark_flow']:.1f} | OI {top_dark['oi_change_pct']:+.1f}%"
                    )
                send_telegram_message(
                    format_radar_summary(
                        pool_count=len(pool_items),
                        oi_signals=len(oi_items),
                        dark_flows=len(dark_items),
                        short_fuel=len(short_fuel_items),
                        top_dark_flow=top_text,
                    )
                )

        if top_dark and top_dark["dark_flow"] >= 60.0 and top_dark["score_total"] >= 65.0:
            alert_signature = f"{top_dark['symbol']}:{int(top_dark['dark_flow'] // 5)}:{int(top_dark['oi_change_pct'] // 5)}"
            if alert_signature != getattr(self, "_last_radar_alert_signature", ""):
                self._last_radar_alert_signature = alert_signature
                send_telegram_message(
                    format_dark_flow_alert(
                        symbol=top_dark["symbol"],
                        oi_change_pct=top_dark["oi_change_pct"],
                        price_change_pct=top_dark["price_change_pct"],
                        funding_rate=top_dark["funding_rate"],
                        market_cap=top_dark["market_cap"],
                    )
                )

    def _refresh_market_profile(self):
        """Update market-aware scan interval and TP multiplier."""
        try:
            self._market_overview = load_market_overview()
        except Exception as e:
            logger.warning(f"🌪 市场环境刷新失败：{e}")
            return

        fear_greed = self._market_overview.get("fear_greed", {})
        fear_greed_value = 50
        if isinstance(fear_greed, dict):
            fear_greed_value = int(float(fear_greed.get("value", fear_greed.get("score", 50)) or 50))
        liquidation_risk = str(self._market_overview.get("liquidation_risk", "LOW")).upper()
        sentiment = str(self._market_overview.get("market_sentiment", "NEUTRAL")).upper()

        interval = self._base_scan_interval
        tp_multiplier = 1.0

        if liquidation_risk in {"HIGH", "EXTREME"}:
            interval = max(60, min(interval, 120))
            tp_multiplier = 0.75
        elif sentiment in {"BULLISH", "RISK_ON"} and fear_greed_value >= 55:
            interval = max(90, min(interval, 180))
            tp_multiplier = 1.2
        elif fear_greed_value <= 30:
            interval = max(90, min(interval, 180))
            tp_multiplier = 0.8
        else:
            interval = max(120, interval)

        self._current_scan_interval = int(interval)
        self.config.scan_interval_sec = int(interval)
        self._tp_multiplier = tp_multiplier
        logger.info(
            f"🌪 动态参数：扫描间隔={self._current_scan_interval}s, TP倍率={self._tp_multiplier:.2f}, "
            f"情绪={sentiment}, 恐贪={fear_greed_value}, 清算={liquidation_risk}"
        )

    def _refresh_market_style(self, force: bool = False):
        """Auto-detect whether recent profits favor majors or hot alts."""
        now = time.time()
        if not force and now - self._last_market_style_refresh < self.config.market_style_refresh_sec:
            return
        self._last_market_style_refresh = now

        try:
            recent_closed = self.db.get_closed_trades(days=30, mode=self.config.mode)[: self.config.market_style_lookback_trades]
        except Exception as e:
            logger.warning(f"市场风格刷新失败：{e}")
            return

        marker = (
            len(recent_closed),
            str(recent_closed[0].exit_time) if recent_closed else "",
        )
        if not force and marker == self._market_style_trade_marker:
            return
        self._market_style_trade_marker = marker

        major_trades = [trade for trade in recent_closed if trade.symbol.upper() in self.config.major_symbols]
        alt_trades = [trade for trade in recent_closed if trade.symbol.upper() not in self.config.major_symbols]

        def _avg_pnl_pct(trades: list[Any]) -> float:
            if not trades:
                return 0.0
            return sum(float(trade.pnl_pct or 0.0) for trade in trades) / len(trades)

        major_avg = _avg_pnl_pct(major_trades)
        alt_avg = _avg_pnl_pct(alt_trades)
        major_win = sum(1 for trade in major_trades if float(trade.pnl or 0.0) > 0)
        alt_win = sum(1 for trade in alt_trades if float(trade.pnl or 0.0) > 0)

        style_mode = "balanced"
        if len(major_trades) >= 3 and (not alt_trades or major_avg >= alt_avg + 0.30):
            style_mode = "major"
        elif len(alt_trades) >= 3 and (not major_trades or alt_avg >= major_avg + 0.30):
            style_mode = "alt"

        self._market_style_mode = style_mode
        self._market_style_stats = {
            "lookback": len(recent_closed),
            "major_count": len(major_trades),
            "alt_count": len(alt_trades),
            "major_avg_pnl_pct": round(major_avg, 2),
            "alt_avg_pnl_pct": round(alt_avg, 2),
            "major_win": major_win,
            "alt_win": alt_win,
        }
        logger.info(
            f"📫 市场风格切换：mode={style_mode} | "
            f"major={len(major_trades)} avg={major_avg:+.2f}% | "
            f"alt={len(alt_trades)} avg={alt_avg:+.2f}%"
        )

    def _start_market_ticker_stream(self):
        """Start all-market mini ticker stream for fast anomaly ranking."""
        ws_client_cls = get_all_market_ticker_client_class()
        if ws_client_cls is None:
            logger.warning("All-market WebSocket unavailable; scanner will use REST ranking")
            return
        if self._market_ws_client:
            return

        try:
            self._market_ws_client = ws_client_cls()
            self._market_ws_client.start()
            logger.info("All-market WebSocket started: realtime anomaly ranking enabled")
        except Exception as e:
            self._market_ws_client = None
            logger.warning(f"All-market WebSocket start failed; scanner will use REST ranking: {e}")

    def _get_ws_top_symbols_by_change(self, limit: int, min_change: float) -> list[str]:
        if not self._market_ws_client:
            return []
        try:
            ws_limit = min(limit, int(getattr(self.config, "ws_deep_scan_candidate_limit", limit) or limit))
            hot_min_change = max(0.2, float(getattr(self.config, "ws_hot_min_change_pct", min_change) or min_change))
            if hasattr(self._market_ws_client, "get_top_symbols_by_hotness"):
                symbols = self._market_ws_client.get_top_symbols_by_hotness(
                    limit=ws_limit,
                    min_change=hot_min_change,
                    max_age_sec=max(20, min(90, self._current_scan_interval)),
                )
                rank_name = "WS热度榜"
            else:
                symbols = self._market_ws_client.get_top_symbols_by_change(
                    limit=ws_limit,
                    min_change=min_change,
                    max_age_sec=max(180, self._current_scan_interval * 2),
                )
                rank_name = "WS异动榜"
            if symbols:
                logger.info(
                    f"📋 {rank_name}命中 {len(symbols)} 个币种 "
                    f"(缓存新鲜币种 {self._market_ws_client.size()}): {symbols[:5]}..."
                )
            return symbols
        except Exception as e:
            logger.debug(f"WS异动榜不可用，回退REST：{e}")
            return []

    def _fast_scan_candidates(self) -> list[str]:
        """Refresh lightweight candidate pool from all-market WS."""
        now = time.time()
        ws_enabled = bool(self._market_ws_client)
        if ws_enabled:
            min_gap = max(3, int(getattr(self.config, "ws_fast_scan_interval_sec", self.config.fast_scan_interval_sec)))
        else:
            min_gap = max(10, int(self.config.fast_scan_interval_sec))
        if self._fast_candidates and now - self._last_fast_scan_time < min_gap:
            return self._fast_candidates

        candidates: list[str] = []
        if self.config.scan_by_change:
            candidates = self._get_ws_top_symbols_by_change(
                self.config.scan_top_n,
                self.config.min_change_pct,
            )

        if candidates:
            self._fast_candidates = candidates
            self._last_fast_scan_time = now
            logger.info(f"⚡ Fast scan candidates: {len(candidates)} symbols | {candidates[:5]}...")
        elif self._fast_candidates:
            logger.info(f"⚡ Fast scan keeps previous candidates: {self._fast_candidates[:5]}...")

        return self._fast_candidates

    def _should_force_ws_deep_scan(self, now: float, candidates: list[str]) -> bool:
        """Trigger an early deep scan when the WS hot list materially changes."""
        if not self._market_ws_client or not candidates:
            return False
        if self._last_deep_scan_time <= 0:
            return False
        min_gap = max(15, int(getattr(self.config, "ws_hot_deep_scan_min_gap_sec", 45) or 45))
        if now - self._last_deep_scan_time < min_gap:
            return False

        signature_size = max(3, int(getattr(self.config, "ws_hot_signature_size", 5) or 5))
        signature = "|".join(candidates[:signature_size])
        if not signature or signature == getattr(self, "_last_fast_candidate_signature", ""):
            return False

        self._last_fast_candidate_signature = signature
        self._last_ws_hot_deep_scan_at = now
        logger.info(
            f"⚡ WS热榜变化触发提前深扫：top{signature_size}={candidates[:signature_size]} "
            f"| 距上次深扫 {int(now - self._last_deep_scan_time)}s"
        )
        return True
