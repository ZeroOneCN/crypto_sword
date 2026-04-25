"""Simple JSONL feature/event store for replay and training."""

from __future__ import annotations

import json
import threading
from datetime import datetime
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


feature_store = FeatureStore()
