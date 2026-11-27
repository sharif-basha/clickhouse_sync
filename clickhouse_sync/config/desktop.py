# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Desktop icon definition for the ClickHouse Sync module."""

from __future__ import annotations

from frappe import _


def get_data() -> list[dict]:
    """Return the module's desktop icon configuration."""
    return [
        {
            "module_name": "ClickHouse Sync",
            "category": "Modules",
            "label": _("ClickHouse Sync"),
            "color": "#1B4332",
            "icon": "octicon octicon-database",
            "type": "module",
            "description": _("Synchronise Frappe data into ClickHouse for reporting."),
        }
    ]
