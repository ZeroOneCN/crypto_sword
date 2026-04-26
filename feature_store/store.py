"""Simple JSONL feature/event store for replay and training."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hermes_paths import hermes_logs_dir


class FeatureStore:
    """Append-only event/review storage with day-partitioned JSONL files."""

    def __init__(self, base_dir: Path | None = None):
        root = base_dir or (hermes_logs_dir() / "feature_store")
        self.base_dir = Path(root)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _day_dir(self, dt: datetime | None = None) -> Path:
        day = (dt or datetime.utcnow()).strftime("%Y-%m-%d")
        target = self.base_dir / day
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _append_jsonl(self, filename: str, payload: dict[str, Any]) -> None:
        with self._lock:
            path = self._day_dir() / filename
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
                f.write("\n")

    def append_event(self, payload: dict[str, Any]) -> None:
        body = dict(payload or {})
        body.setdefault("schema_version", 1)
        body.setdefault("record_type", "event")
        body.setdefault("stored_at", datetime.utcnow().isoformat(timespec="seconds") + "Z")
        self._append_jsonl("events.ndjson", body)

    def append_review(self, payload: dict[str, Any]) -> None:
        body = dict(payload or {})
        body.setdefault("schema_version", 1)
        body.setdefault("record_type", "trade_review")
        body.setdefault("stored_at", datetime.utcnow().isoformat(timespec="seconds") + "Z")
        self._append_jsonl("reviews.ndjson", body)

    def summarize_entry_protection(self, report_date: str, tz_offset_hours: int = 8) -> dict[str, Any]:
        """Summarize entry protection pass/fail events for a local-calendar day."""
        summary: dict[str, Any] = {
            "attempts": 0,
            "ok": 0,
            "failed": 0,
            "ok_rate": 0.0,
            "failed_by_symbol": {},
            "failed_by_direction": {},
            "failed_by_detail": {},
        }
        try:
            target_day = datetime.fromisoformat(report_date).date()
        except Exception:
            return summary

        tz_local = timezone(timedelta(hours=tz_offset_hours))
        candidate_days = {
            target_day - timedelta(days=1),
            target_day,
            target_day + timedelta(days=1),
        }
        candidate_paths = [self.base_dir / day.isoformat() / "events.ndjson" for day in candidate_days]

        def _is_target_day(ts_text: str) -> bool:
            if not ts_text:
                return False
            try:
                parsed = datetime.fromisoformat(str(ts_text).replace("Z", "+00:00"))
                return parsed.astimezone(tz_local).date() == target_day
            except Exception:
                return False

        failed_by_symbol: dict[str, int] = {}
        failed_by_direction: dict[str, int] = {}
        failed_by_detail: dict[str, int] = {}

        for events_path in candidate_paths:
            if not events_path.exists():
                continue
            try:
                with events_path.open("r", encoding="utf-8") as f:
                    for raw in f:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                        except Exception:
                            continue
                        if payload.get("type") != "execution":
                            continue
                        event_name = str(payload.get("event", "") or "")
                        if event_name not in {"entry_protection_ok", "entry_protection_failed"}:
                            continue
                        if not _is_target_day(str(payload.get("ts", "") or "")):
                            continue

                        summary["attempts"] += 1
                        if event_name == "entry_protection_ok":
                            summary["ok"] += 1
                            continue

                        summary["failed"] += 1
                        symbol = str(payload.get("symbol", "") or "").upper() or "UNKNOWN"
                        direction = str(payload.get("direction", "") or "").upper() or "UNKNOWN"
                        failed_by_symbol[symbol] = failed_by_symbol.get(symbol, 0) + 1
                        failed_by_direction[direction] = failed_by_direction.get(direction, 0) + 1
                        detail_text = ""
                        metrics = payload.get("metrics")
                        if isinstance(metrics, dict):
                            detail_text = str(metrics.get("detail", "") or "").strip()
                        if detail_text:
                            detail_items = [item.strip() for item in detail_text.split(";") if item.strip()]
                            for item in detail_items:
                                failed_by_detail[item] = failed_by_detail.get(item, 0) + 1
            except Exception:
                continue

        attempts = int(summary["attempts"] or 0)
        summary["ok_rate"] = round((float(summary["ok"]) / attempts * 100.0), 2) if attempts > 0 else 0.0
        summary["failed_by_symbol"] = dict(
            sorted(failed_by_symbol.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))
        )
        summary["failed_by_direction"] = dict(
            sorted(failed_by_direction.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))
        )
        summary["failed_by_detail"] = dict(
            sorted(failed_by_detail.items(), key=lambda item: (-int(item[1] or 0), str(item[0])))
        )
        return summary


feature_store = FeatureStore()
