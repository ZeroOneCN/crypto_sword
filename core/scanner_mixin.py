"""Signal scanning mixin for the trading engine."""

from __future__ import annotations

import logging
import time
from typing import Any, List, Optional

from binance_breakout_scanner import (
    get_top_symbols_by_change,
    get_top_symbols_by_volume,
    scan_symbols,
)
from feature_store import feature_store
from core.monitoring import build_strategy_event, message_signature
from services.oi_funding_service import oi_funding_service
from signal_enhancer import score_signal

logger = logging.getLogger(__name__)


class ScannerMixin:
    """Signal discovery and ranking pipeline."""

    def scan_for_signals(self, symbols: Optional[List[str]] = None, scan_source: str = "deep") -> List[dict]:
        """鎵弿浜ゆ槗淇″彿骞舵寜璐ㄩ噺鎺掑簭銆?""
        trace_started = time.perf_counter()
        latency_steps: list[tuple[str, float]] = []

        self._prune_entry_watchlist()

        step_started = time.perf_counter()
        if symbols is not None:
            symbols = list(dict.fromkeys(symbols))[: self.config.scan_top_n]
            if getattr(self.config, "target_altcoins", False):
                major_set = {symbol.upper() for symbol in self.config.major_symbols}
                symbols = [symbol for symbol in symbols if symbol.upper() not in major_set]
            logger.info(f"馃攷 Deep scan from {scan_source}: {len(symbols)} symbols | {symbols[:5]}...")
        elif self.config.scan_by_change:
            symbols = self._get_ws_top_symbols_by_change(
                self.config.scan_top_n,
                self.config.min_change_pct,
            )
            if not symbols:
                symbols = get_top_symbols_by_change(
                    self.config.scan_top_n,
                    min_change=self.config.min_change_pct,
                )
                logger.info(f"馃敟 灞卞甯佹ā寮?REST) - 鎵弿 {len(symbols)} 涓紓鍔ㄥ竵绉嶏細{symbols[:5]}...")
            else:
                logger.info(f"馃敟 灞卞甯佹ā寮?WS) - 鎵弿 {len(symbols)} 涓紓鍔ㄥ竵绉嶏細{symbols[:5]}...")
        else:
            symbols = get_top_symbols_by_volume(self.config.scan_top_n)
            logger.info(f"馃摮 鎴愪氦閲忔ā寮?- 鎵弿 {len(symbols)} 涓竵绉嶏細{symbols[:5]}...")
        self._record_latency_step(latency_steps, "select_symbols", step_started)

        step_started = time.perf_counter()
        results = scan_symbols(
            symbols,
            min_stage=self.config.min_stage,
            max_workers=self.config.scan_workers,
        )
        self._record_latency_step(latency_steps, "deep_scan", step_started)

        step_started = time.perf_counter()
        oi_funding_map = oi_funding_service.analyze_symbols(
            [item.symbol for item in results if getattr(item, "symbol", None)],
            self.config,
        )
        self._record_latency_step(latency_steps, "oi_funding_scan", step_started)

        signals = []
        step_started = time.perf_counter()
        for result in results:
            if result.stage in {"neutral", "error"}:
                continue
            if result.direction not in {"LONG", "SHORT"}:
                continue
            if result.symbol in self.tracker.positions:
                continue

            rejection_reason = self._entry_rejection_reason(result.symbol, result.direction, result.metrics)
            if rejection_reason:
                logger.info(f"馃 {result.symbol} 鍏ュ満杩囨护锛歿rejection_reason}")
                continue

            oi_funding = oi_funding_map.get(result.symbol.upper(), {})
            try:
                signal_score = score_signal(
                    symbol=result.symbol,
                    stage=result.stage,
                    direction=result.direction,
                    metrics=result.metrics,
                )

                added_bonus = oi_funding_service.apply_bonus(signal_score, oi_funding)
                if added_bonus > 0:
                    logger.info(
                        f"馃И {result.symbol} OI/Funding 鍔犲垎 +{added_bonus:.1f} "
                        f"(OI={oi_funding.get('oi_change_pct', 0.0):.1f}%, "
                        f"funding={oi_funding.get('funding_current', 0.0):.4%})"
                    )

                if signal_score.confidence in {"浣?}:
                    logger.info(f"馃幆 {result.symbol} 淇″彿璐ㄩ噺杩囦綆 ({signal_score.total_score:.1f})锛岃烦杩?)
                    continue

                logger.info(f"馃幆 {result.symbol} 淇″彿璇勫垎锛歿signal_score.total_score:.1f}/100 ({signal_score.confidence})")
            except Exception as exc:
                logger.warning(f"淇″彿璇勫垎澶辫触 {result.symbol}: {exc}")
                signal_score = None

            signal_data = {
                "symbol": result.symbol,
                "stage": result.stage,
                "direction": result.direction,
                "price": result.metrics.get("last_price", 0),
                "metrics": result.metrics,
                "trigger": result.trigger,
                "risk": result.risk,
                "score": signal_score.to_dict() if signal_score else None,
                "oi_funding": oi_funding,
            }
            confirmed_signal = self._apply_entry_confirmation(signal_data)
            strategy_event = build_strategy_event(confirmed_signal)
            logger.info(f"strategy_event {message_signature(strategy_event)}")
            feature_store.append_event(strategy_event)
            signals.append(confirmed_signal)

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

        logger.info(f"馃摋 鍙戠幇 {len(signals)} 涓湁鏁堜俊鍙凤紙宸叉寜璐ㄩ噺鎺掑簭锛?)
        self._emit_latency_trace("scan_for_signals", trace_started, latency_steps)
        return signals

