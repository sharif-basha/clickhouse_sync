# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Failed Syncs report: recent failures with their error messages."""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _


def execute(filters: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Return ``(columns, data)`` for the Failed Syncs report."""
    filters = filters or {}
    columns = [
        {"label": _("Log"), "fieldname": "name", "fieldtype": "Link", "options": "ClickHouse Sync Log", "width": 180},
        {"label": _("Configuration"), "fieldname": "configuration", "fieldtype": "Link", "options": "ClickHouse Sync Configuration", "width": 200},
        {"label": _("Trigger"), "fieldname": "trigger", "fieldtype": "Data", "width": 90},
        {"label": _("Host"), "fieldname": "execution_host", "fieldtype": "Data", "width": 130},
        {"label": _("When"), "fieldname": "start_time", "fieldtype": "Datetime", "width": 170},
        {"label": _("Message"), "fieldname": "message", "fieldtype": "Small Text", "width": 360},
    ]

    conditions: dict[str, Any] = {"status": "Failed"}
    if filters.get("configuration"):
        conditions["configuration"] = filters["configuration"]

    data = frappe.get_all(
        "ClickHouse Sync Log",
        filters=conditions,
        fields=["name", "configuration", "trigger", "execution_host", "start_time", "message"],
        order_by="creation desc",
        limit=200,
    )
    return columns, data
