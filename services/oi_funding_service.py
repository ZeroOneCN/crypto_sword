"""OI + funding regime service for altcoin opportunity scoring."""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover - optional runtime dependency fallback
    requests = None


class OiFundingService:
    """Batch-evaluate funding turn + OI expansion and return score bonuses."""

    def __init__(self):
        self._lock = threading.Lock()
        self._funding_snapshot: dict[str, float] = {}
        self._funding_cache: tuple[float, dict[str, float]] | None = None
        self._oi_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._session_local = threading.local()

    def _session(self):
        session = getattr(self._session_local, "session", None)
        if session is None and requests is not None:
            session = requests.Session()
            session.headers.update({"User-Agent": "HermesTrader/1.0"})
            self._session_local.session = session
        return session

    def _http_json(self, url: str, params: dict[str, Any] | None = None, timeout: float = 8.0) -> Any:
        if requests is not None:
            resp = self._session().get(url, params=params or {}, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "HermesTrader/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _fetch_funding_map(self, cache_sec: float) -> dict[str, float]:
        now = time.time()
        with self._lock:
            if self._funding_cache and now - self._funding_cache[0] < max(5.0, cache_sec):
                return dict(self._funding_cache[1])

        raw = self._http_json("https://fapi.binance.com/fapi/v1/premiumIndex")
        if not isinstance(raw, list):
            return {}

        funding_map: dict[str, float] = {}
        for item in raw:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            try:
                funding_map[symbol] = float(item.get("lastFundingRate", 0) or 0)
            except Exception:
                continue

        with self._lock:
            self._funding_cache = (now, dict(funding_map))
        return funding_map

    def _fetch_oi_profile(self, symbol: str, min_oi_change_pct: float, cache_sec: float) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            cached = self._oi_cache.get(symbol)
            if cached and now - cached[0] < max(30.0, cache_sec):
                return dict(cached[1])

        raw = self._http_json(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "1h", "limit": 24},
        )

        values: list[float] = []
        if isinstance(raw, list):
            for item in raw:
                try:
                    values.append(float(item.get("sumOpenInterestValue", 0) or 0))
                except Exception:
                    continue

        if len(values) < 8:
            profile = {
                "oi_change_pct": 0.0,
                "oi_rising": False,
                "oi_monotonic_up": False,
                "segments": [],
                "oi_signal": False,
            }
        else:
            seg_len = max(2, len(values) // 4)
            segments = [
                sum(values[i * seg_len : (i + 1) * seg_len]) / len(values[i * seg_len : (i + 1) * seg_len])
                for i in range(3)
            ]
            tail = values[3 * seg_len :]
            segments.append(sum(tail) / max(1, len(tail)))

            first = segments[0] if segments and segments[0] > 0 else 0.0
            oi_change_pct = ((segments[-1] - first) / first * 100.0) if first > 0 else 0.0
            oi_rising = segments[-1] > segments[0]
            oi_monotonic_up = all(segments[i + 1] >= segments[i] for i in range(len(segments) - 1))
            oi_signal = oi_change_pct >= min_oi_change_pct and oi_rising

            profile = {
                "oi_change_pct": oi_change_pct,
                "oi_rising": oi_rising,
                "oi_monotonic_up": oi_monotonic_up,
                "segments": [round(seg, 2) for seg in segments],
                "oi_signal": oi_signal,
            }

        with self._lock:
            self._oi_cache[symbol] = (now, dict(profile))
        return profile

    def analyze_symbols(self, symbols: list[str], config: Any) -> dict[str, dict[str, Any]]:
        if not getattr(config, "oi_funding_enabled", True):
            return {}

        unique_symbols = [symbol.upper() for symbol in dict.fromkeys(symbols) if symbol]
        if not unique_symbols:
            return {}

        min_oi_change = float(getattr(config, "oi_funding_min_oi_change_pct", 8.0))
        turn_bonus = float(getattr(config, "oi_funding_turn_bonus", 4.0))
        rising_bonus = float(getattr(config, "oi_funding_rising_bonus", 8.0))
        bonus_cap = float(getattr(config, "oi_funding_bonus_cap", 12.0))
        cache_sec = float(getattr(config, "oi_funding_cache_sec", 120.0))

        funding_now = self._fetch_funding_map(cache_sec=max(cache_sec, 300.0))
        with self._lock:
            funding_prev = dict(self._funding_snapshot)

        result: dict[str, dict[str, Any]] = {}
        for symbol in unique_symbols:
            current_funding = funding_now.get(symbol)
            if current_funding is None:
                continue

            previous_funding = funding_prev.get(symbol)
            turned_negative = previous_funding is not None and previous_funding >= 0 and current_funding < 0

            oi_profile = self._fetch_oi_profile(
                symbol=symbol,
                min_oi_change_pct=min_oi_change,
                cache_sec=cache_sec,
            )

            score_bonus = 0.0
            if turned_negative:
                score_bonus += turn_bonus
            if oi_profile.get("oi_signal"):
                score_bonus += rising_bonus
            score_bonus = min(max(0.0, score_bonus), max(0.0, bonus_cap))

            result[symbol] = {
                "symbol": symbol,
                "funding_current": current_funding,
                "funding_previous": previous_funding,
                "turned_negative": turned_negative,
                "oi_change_pct": float(oi_profile.get("oi_change_pct", 0.0) or 0.0),
                "oi_rising": bool(oi_profile.get("oi_rising", False)),
                "oi_monotonic_up": bool(oi_profile.get("oi_monotonic_up", False)),
                "oi_signal": bool(oi_profile.get("oi_signal", False)),
                "score_bonus": score_bonus,
            }

        with self._lock:
            for symbol in unique_symbols:
                if symbol in funding_now:
                    self._funding_snapshot[symbol] = funding_now[symbol]

        return result

    @staticmethod
    def apply_bonus(signal_score: Any, oi_funding: dict[str, Any]) -> float:
        bonus = float((oi_funding or {}).get("score_bonus", 0.0) or 0.0)
        if bonus <= 0 or signal_score is None:
            return 0.0

        base = float(getattr(signal_score, "total_score", 0.0) or 0.0)
        total = min(100.0, base + bonus)
        signal_score.total_score = total

        if total >= 70:
            signal_score.confidence = "\u6781\u9ad8"
        elif total >= 50:
            signal_score.confidence = "\u9ad8"
        elif total >= 30:
            signal_score.confidence = "\u4e2d"
        else:
            signal_score.confidence = "\u4f4e"
        return bonus


oi_funding_service = OiFundingService()
