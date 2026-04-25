"""Signal scanning mixin for the trading engine."""

from __future__ import annotations

import logging
import time
from typing import Any, List, Optional

from adapters.rest_gateway import (
    get_top_symbols_by_change_rest,
    get_top_symbols_by_volume_rest,
    scan_symbols_rest,
)
from services.signal_service import signal_service

logger = logging.getLogger(__name__)


class ScannerMixin:
    """Signal discovery and ranking pipeline."""

    def scan_for_signals(self, symbols: Optional[List[str]] = None, scan_source: str = "deep") -> List[dict]:
        """扫描交易信号 - 弗丽嘉的鹰眼"""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []
        self._prune_entry_watchlist()
        step_started = time.perf_counter()
        if symbols is not None:
            symbols = list(dict.fromkeys(symbols))[: self.config.scan_top_n]
            if getattr(self.config, "target_altcoins", False):
                major_set = {symbol.upper() for symbol in self.config.major_symbols}
                symbols = [symbol for symbol in symbols if symbol.upper() not in major_set]
            logger.info(f"⚡ Deep scan from {scan_source}: {len(symbols)} symbols | {symbols[:5]}...")
        elif self.config.scan_by_change:
            symbols = self._get_ws_top_symbols_by_change(
                self.config.scan_top_n,
                self.config.min_change_pct,
            )
            if not symbols:
                symbols = get_top_symbols_by_change_rest(
                    self.config.scan_top_n,
                    min_change=self.config.min_change_pct,
                )
                logger.info(f"🔥 妖币模式(REST) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
            else:
                logger.info(f"🔥 妖币模式(WS) - 扫描 {len(symbols)} 个异动币种：{symbols[:5]}...")
        else:
            symbols = get_top_symbols_by_volume_rest(self.config.scan_top_n)
            logger.info(f"📊 成交量模式 - 扫描 {len(symbols)} 个币种：{symbols[:5]}...")
        self._record_latency_step(latency_steps, "select_symbols", step_started)

        step_started = time.perf_counter()
        results = scan_symbols_rest(
            symbols,
            min_stage=self.config.min_stage,
            max_workers=self.config.scan_workers,
        )
        self._record_latency_step(latency_steps, "deep_scan", step_started)

        signals = []
        step_started = time.perf_counter()
        for r in results:
            if r.stage in {"neutral", "error"}:
                continue
            if r.direction not in {"LONG", "SHORT"}:
                continue
            if r.symbol in self.tracker.positions:
                continue
            rejection_reason = self._entry_rejection_reason(r.symbol, r.direction, r.metrics)
            if rejection_reason:
                logger.info(f"🧊 {r.symbol} 入场过滤：{rejection_reason}")
                continue

            try:
                signal_score = signal_service.score(
                    symbol=r.symbol,
                    stage=r.stage,
                    direction=r.direction,
                    metrics=r.metrics,
                )
                if signal_score.confidence in {"低"}:
                    logger.info(f"🎯 {r.symbol} 信号质量过低 ({signal_score.total_score:.1f})，跳过")
                    continue

                logger.info(f"🎯 {r.symbol} 信号评分：{signal_score.total_score:.1f}/100 ({signal_score.confidence})")

            except Exception as e:
                logger.warning(f"信号评分失败 {r.symbol}: {e}")
                signal_score = None

            signal_data = {
                "symbol": r.symbol,
                "stage": r.stage,
                "direction": r.direction,
                "price": r.metrics.get("last_price", 0),
                "metrics": r.metrics,
                "trigger": r.trigger,
                "risk": r.risk,
                "score": signal_score.to_dict() if signal_score else None,
            }
            signals.append(self._apply_entry_confirmation(signal_data))

        def _signal_priority(item: dict[str, Any]) -> tuple[int, float]:
            symbol = str(item.get("symbol", "")).upper()
            if self._market_style_mode == "major":
                major_bonus = 2 if symbol in self.config.major_symbols else 0
            elif self._market_style_mode == "alt":
                major_bonus = -1 if symbol in self.config.major_symbols else 1
            else:
                major_bonus = 1 if symbol in self.config.major_symbols else 0
            score_total = float((item.get("score") or {}).get("total_score", 0) or 0)
            return major_bonus, score_total

        signals.sort(key=_signal_priority, reverse=True)
        self._record_latency_step(latency_steps, "score_filter", step_started)

        logger.info(f"📡 发现 {len(signals)} 个有效信号（已按质量排序）")
        self._emit_latency_trace("scan_for_signals", trace_started, latency_steps)

        return signals
