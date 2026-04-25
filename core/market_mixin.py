"""Market profile, radar jobs and all-market websocket mixin."""

from __future__ import annotations

import logging
import time
from typing import Any

from adapters.rest_gateway import load_market_overview
from adapters.ws_gateway import get_all_market_ticker_client_class
from jobs.radar_jobs import scan_accumulation_pool_job, scan_oi_changes_job
from telegram_notifier import (
    format_accumulation_pool_report,
    format_dark_flow_alert,
    format_radar_summary,
    send_telegram_message,
)

logger = logging.getLogger(__name__)


class MarketMixin:
    def _run_radar_background_scan(self, now: float):
        """Run periodic OI anomaly and accumulation pool background scans."""
        try:
            if now - self._last_radar_scan_time >= self._radar_scan_interval:
                logger.info("🛰 开始 OI 异动扫描...")
                oi_signals = scan_oi_changes_job()
                self._last_radar_scan_time = now

                dark_flows = [signal for signal in oi_signals if getattr(signal, "is_dark_flow", False)]
                if dark_flows:
                    for dark_flow in dark_flows[:3]:
                        msg = format_dark_flow_alert(
                            symbol=dark_flow.symbol,
                            oi_change_pct=dark_flow.oi_change_pct,
                            price_change_pct=dark_flow.price_change_pct,
                            funding_rate=dark_flow.funding_rate,
                            market_cap=0,
                        )
                        send_telegram_message(msg)
                        logger.info(f"📣 暗流信号已推送：{dark_flow.symbol}")

                if oi_signals:
                    summary = format_radar_summary(
                        pool_count=0,
                        oi_signals=len(oi_signals),
                        dark_flows=len(dark_flows),
                        short_fuel=0,
                        top_dark_flow=dark_flows[0].symbol if dark_flows else None,
                    )
                    send_telegram_message(summary)

            if now - self._last_pool_scan_time >= self._pool_scan_interval:
                logger.info("🛰 开始吸筹池扫描...")
                pool = scan_accumulation_pool_job()
                self._last_pool_scan_time = now

                if pool:
                    send_telegram_message(format_accumulation_pool_report(pool))
                    logger.info(f"🛰 吸筹池已更新：{len(pool)} 个标的")

        except ImportError:
            logger.debug("accumulation_radar 模块不可用，跳过雷达扫描")
        except Exception as e:
            logger.warning(f"雷达后台扫描失败：{e}")

    def _refresh_market_profile(self):
        """Update market-aware scan interval and TP multiplier."""
        try:
            self._market_overview = load_market_overview()
        except Exception as e:
            logger.warning(f"🌐 市场环境刷新失败：{e}")
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
            f"🌐 动态参数：扫描间隔={self._current_scan_interval}s, TP倍率={self._tp_multiplier:.2f}, "
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
            f"📱 市场风格切换：mode={style_mode} | "
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
            symbols = self._market_ws_client.get_top_symbols_by_change(
                limit=limit,
                min_change=min_change,
                max_age_sec=max(180, self._current_scan_interval * 2),
            )
            if symbols:
                logger.info(
                    f"📗 WS异动榜命中 {len(symbols)} 个币种"
                    f"(缓存新鲜币种 {self._market_ws_client.size()}): {symbols[:5]}..."
                )
            return symbols
        except Exception as e:
            logger.debug(f"WS异动榜不可用，回退REST：{e}")
            return []

    def _fast_scan_candidates(self) -> list[str]:
        """Refresh lightweight candidate pool from all-market WS."""
        now = time.time()
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
