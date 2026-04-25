# -*- coding: utf-8 -*-
"""Cross-platform paths for Hermes Trader.

Historically this project used Linux-only paths like `/root/.hermes/...`.
This helper centralizes path resolution so the code runs on Windows/macOS/Linux.
"""

from __future__ import annotations

import os
from pathlib import Path


def hermes_home() -> Path:
    """Return Hermes home dir.

    Priority:
      1) $HERMES_HOME
      2) ~/.hermes
    """
    env = os.environ.get("HERMES_HOME")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.home() / ".hermes").resolve()


def hermes_config_dir() -> Path:
    return hermes_home() / "config"


def hermes_logs_dir() -> Path:
    return hermes_home() / "logs"


def hermes_scripts_dir() -> Path:
    return hermes_home() / "scripts"

