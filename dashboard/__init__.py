# -*- coding: utf-8 -*-
"""Read-only dashboard package."""

from .api import DashboardHandler, main
from .data_service import DashboardData

__all__ = ["DashboardHandler", "DashboardData", "main"]
