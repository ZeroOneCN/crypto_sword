"""Signal scoring service wrapper."""

from __future__ import annotations

from typing import Any

from signal_enhancer import score_signal


class SignalService:
    """Encapsulate scoring and optional radar enhancement."""

    @staticmethod
    def score(symbol: str, stage: str, direction: str, metrics: dict[str, Any]):
        signal_score = score_signal(
            symbol=symbol,
            stage=stage,
            direction=direction,
            metrics=metrics,
            klines_1h=metrics.get("klines_1h"),
        )

        try:
            from signal_enhancer import enhance_with_radar_score

            signal_score = enhance_with_radar_score(signal_score, metrics)
        except ImportError:
            pass
        return signal_score


signal_service = SignalService()
