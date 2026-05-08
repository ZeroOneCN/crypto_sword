# -*- coding: utf-8 -*-
"""Backward-compatible Telegram notification facade.

Implementation lives in ``notifiers/`` so templates, labels and sending queue can
evolve independently without touching trading code imports.
"""

from __future__ import annotations

from notifiers import *  # noqa: F401,F403
from notifiers.labels import _fmt_usdt  # Backward compatibility for legacy mixins.


def main() -> int:
    import argparse
    from notifiers.telegram_sender import send_telegram_message

    parser = argparse.ArgumentParser(description="Send a Telegram test message")
    parser.add_argument("message", nargs="?", default="Hermes Telegram notifier test")
    args = parser.parse_args()
    success = send_telegram_message(args.message)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
