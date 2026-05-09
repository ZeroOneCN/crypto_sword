# -*- coding: utf-8 -*-
"""Telegram config, queue and sending transport."""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import threading
import time
from html import unescape
from pathlib import Path
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover - urllib fallback is kept for minimal hosts
    requests = None

from .labels import (
    _looks_like_placeholder,
    _normalize_telegram_value,
    _sanitize_token_preview,
    _strip_html,
    _is_valid_bot_token,
)

logger = logging.getLogger(__name__)

_telegram_lock = threading.Lock()
_last_message_time = 0.0
_min_message_interval = 0.5
_telegram_session_local = threading.local()

def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return default

_telegram_queue: "queue.Queue[tuple[str, str | None]]" = queue.Queue(
    maxsize=max(10, _int_env("TELEGRAM_QUEUE_SIZE", 1000))
)
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()

_ERROR_TIME_RE = re.compile(r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b")
_ERROR_SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,24}USDT)\b")
_ERROR_SESSION_RE = re.compile(r"\b([A-Z0-9]{2,24}USDT-\d{14}-[a-f0-9]{8})\b")

def _telegram_timeout() -> float:
    try:
        return max(1.0, float(os.environ.get("TELEGRAM_TIMEOUT_SEC", "4") or 4))
    except Exception:
        return 4.0

def _telegram_session():
    if requests is None:
        return None
    session = getattr(_telegram_session_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "HermesTraderTelegram/1.0"})
        _telegram_session_local.session = session
    return session

def _hermes_home() -> Path:
    """Return Hermes home dir (cross-platform).

    Priority:
      1) $HERMES_HOME
      2) ~/.hermes
    """
    env = os.environ.get("HERMES_HOME")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.home() / ".hermes").resolve()

def get_telegram_config() -> dict[str, Any]:
    """Load Telegram config from environment or json file.

    Supported:
      - env: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
      - repo: ./config/telegram.json (recommended for local dev)
      - user: ~/.hermes/config/telegram.json (recommended for prod)
    """
    config: dict[str, Any] = {
        "bot_token": _normalize_telegram_value(os.environ.get("TELEGRAM_BOT_TOKEN", "")),
        "chat_id": _normalize_telegram_value(os.environ.get("TELEGRAM_CHAT_ID", "")),
    }

    if config["bot_token"] and config["chat_id"]:
        return config

    candidates = [
        (Path(__file__).resolve().parent / "config" / "telegram.json"),
        (_hermes_home() / "config" / "telegram.json"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                file_config = json.load(f) or {}
            config["bot_token"] = config["bot_token"] or _normalize_telegram_value(file_config.get("bot_token", ""))
            config["chat_id"] = config["chat_id"] or _normalize_telegram_value(file_config.get("chat_id", ""))
            break
        except Exception as e:
            logger.warning(f"Failed to load Telegram config from {path}: {e}")

    if config["bot_token"].startswith("bot"):
        config["bot_token"] = config["bot_token"][3:]

    return config

def send_telegram_message(message: str, parse_mode: str | None = "HTML", async_send: bool | None = None) -> bool:
    """Queue a Telegram message without blocking trading flows."""
    _record_transaction_error_message(message)
    if async_send is None:
        async_send = os.environ.get("TELEGRAM_ASYNC_SEND", "1").strip().lower() not in {"0", "false", "no"}

    if not async_send:
        return _send_telegram_message_sync(message, parse_mode=parse_mode)

    config = get_telegram_config()
    token = _normalize_telegram_value(config.get("bot_token", ""))
    chat_id = _normalize_telegram_value(config.get("chat_id", ""))
    if not _telegram_config_usable(token, chat_id):
        logger.info(f"[TG] {message}")
        return False

    _ensure_telegram_worker()
    try:
        _telegram_queue.put_nowait((message, parse_mode))
        return True
    except queue.Full:
        logger.error("Telegram queue full - dropping notification to keep trading loop non-blocking")
        return False

def _record_transaction_error_message(message: str) -> None:
    """Persist formatted trading-exception Telegram messages for the dashboard."""
    if "交易异常" not in str(message or "") and "浜ゆ槗寮傚父" not in str(message or ""):
        return
    try:
        text = unescape(_strip_html(str(message or "")))
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        joined = "\n".join(lines)

        def _after_label(label: str) -> str:
            for line in lines:
                if line.startswith(label):
                    return line[len(label):].strip()
            return ""

        event_time = ""
        time_match = _ERROR_TIME_RE.search(joined)
        if time_match:
            event_time = time_match.group(0)
        else:
            from services.time_basis import utc_now_iso

            event_time = utc_now_iso()

        error_type = _after_label("类型")
        component = _after_label("组件")
        symbol = _after_label("标的")
        session_id = _after_label("流水号")

        if not symbol:
            symbol_match = _ERROR_SYMBOL_RE.search(joined)
            symbol = symbol_match.group(1) if symbol_match else ""
        if not session_id:
            session_match = _ERROR_SESSION_RE.search(joined)
            session_id = session_match.group(1) if session_match else ""

        detail_lines: list[str] = []
        in_detail = False
        for line in lines:
            if line == "详情":
                in_detail = True
                continue
            if in_detail and (line.startswith("⚠") or set(line) <= {"━", "-"}):
                break
            if in_detail:
                detail_lines.append(line)
        detail = "\n".join(detail_lines) or joined
        summary = detail_lines[0] if detail_lines else (error_type or joined[:120])

        from repositories.trade_repository import TradeDatabase

        TradeDatabase().add_transaction_error(
            event_time=event_time,
            error_type=error_type or "交易异常",
            component=component,
            symbol=symbol,
            session_id=session_id,
            summary=summary,
            detail=detail,
            source="telegram",
            raw_text=joined,
        )
    except Exception as exc:
        logger.debug(f"transaction error capture skipped: {exc}")

def _ensure_telegram_worker():
    global _telegram_worker_started
    if _telegram_worker_started:
        return
    with _telegram_worker_lock:
        if _telegram_worker_started:
            return
        thread = threading.Thread(target=_telegram_worker_loop, name="telegram-notifier", daemon=True)
        thread.start()
        _telegram_worker_started = True

def _telegram_worker_loop():
    while True:
        message, parse_mode = _telegram_queue.get()
        try:
            _send_telegram_message_sync(message, parse_mode=parse_mode)
        except Exception as exc:
            logger.error(f"Telegram worker unexpected failure: {exc}")
        finally:
            _telegram_queue.task_done()

def _telegram_config_usable(token: str, chat_id: str) -> bool:
    if not token or not chat_id:
        logger.warning("Telegram not configured - skipping notification")
        return False
    if _looks_like_placeholder(token) or _looks_like_placeholder(chat_id):
        logger.error(
            "Telegram config uses placeholder values. "
            "Please set real TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID or config/telegram.json."
        )
        return False
    if not _is_valid_bot_token(token):
        logger.error(
            f"Telegram bot token format invalid: {_sanitize_token_preview(token)} "
            "(expected <digits>:<secret>)"
        )
        return False
    return True

def _send_telegram_message_sync(message: str, parse_mode: str | None = "HTML") -> bool:
    """Send a message to Telegram (thread-safe with simple rate limiting)."""
    global _last_message_time

    config = get_telegram_config()
    token = _normalize_telegram_value(config.get("bot_token", ""))
    chat_id = _normalize_telegram_value(config.get("chat_id", ""))

    if not _telegram_config_usable(token, chat_id):
        logger.info(f"[TG] {message}")
        return False

    with _telegram_lock:
        elapsed = time.time() - _last_message_time
        if elapsed < _min_message_interval:
            time.sleep(_min_message_interval - elapsed)

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        try:
            attempts: list[tuple[str, str | None]] = [(message, parse_mode)]
            if parse_mode is not None:
                attempts.append((_strip_html(message), None))

            for index, (attempt_message, attempt_parse_mode) in enumerate(attempts):
                payload: dict[str, Any] = {"chat_id": chat_id, "text": attempt_message}
                if attempt_parse_mode:
                    payload["parse_mode"] = attempt_parse_mode

                if requests is not None:
                    session = _telegram_session()
                    response = session.post(url, json=payload, timeout=_telegram_timeout())  # type: ignore[union-attr]
                    raw = response.text or ""
                    if response.status_code >= 400:
                        logger.error(f"Telegram HTTP {response.status_code}: {raw[:300]}")
                        if response.status_code == 404:
                            logger.error(
                                "Telegram endpoint 404, usually caused by invalid bot token. "
                                f"token={_sanitize_token_preview(token)}"
                            )
                        if response.status_code == 400 and index + 1 < len(attempts):
                            continue
                        response.raise_for_status()
                    result = response.json() if raw else {}
                else:
                    import urllib.error
                    import urllib.request

                    data = json.dumps(payload).encode("utf-8")
                    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                    try:
                        with urllib.request.urlopen(req, timeout=_telegram_timeout()) as response:
                            result = json.loads(response.read().decode("utf-8"))
                    except urllib.error.HTTPError as e:
                        body = e.read().decode("utf-8", errors="replace")
                        logger.error(f"Telegram HTTP {e.code}: {body[:300]}")
                        if e.code == 400 and index + 1 < len(attempts):
                            continue
                        raise

                _last_message_time = time.time()
                if result.get("ok"):
                    if index > 0:
                        logger.warning("Telegram HTML failed; plain-text fallback sent successfully")
                    else:
                        logger.info("Telegram message sent successfully")
                    return True
                logger.error(f"Telegram API error: {result}")

            return False
        except Exception as e:
            _last_message_time = time.time()
            logger.error(f"Failed to send Telegram message: {e}")
            return False

