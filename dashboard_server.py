#!/usr/bin/env python3
"""Backward-compatible dashboard entrypoint.

HTTP routing lives in ``dashboard/api.py``; aggregation lives in
``dashboard/data_service.py``; UI lives in ``dashboard/static/index.html``.
"""

from __future__ import annotations

from dashboard.api import DashboardHandler, main
from dashboard.data_service import DashboardData

__all__ = ["DashboardHandler", "DashboardData", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
