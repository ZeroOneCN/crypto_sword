"""Signal scoring service wrapper."""

from __future__ import annotations

from typing import Any

from signal_enhancer import score_signal
try:
    from signal_enhancer import enhance_with_radar_score as _enhance_with_radar_score
except Exception:
    _enhance_with_radar_score = None


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

        if _enhance_with_radar_score is not None:
            signal_score = _enhance_with_radar_score(signal_score, metrics)
        return signal_score


signal_service = SignalService()
