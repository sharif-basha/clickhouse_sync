# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Slowest Syncs report: completed runs ranked by duration and throughput."""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _


def execute(filters: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Return ``(columns, data)`` for the Slowest Syncs report."""
    filters = filters or {}
    columns = [
        {"label": _("Log"), "fieldname": "name", "fieldtype": "Link", "options": "ClickHouse Sync Log", "width": 180},
        {"label": _("Configuration"), "fieldname": "configuration", "fieldtype": "Link", "options": "ClickHouse Sync Configuration", "width": 200},
        {"label": _("Duration (s)"), "fieldname": "duration", "fieldtype": "Float", "precision": "2", "width": 120},
        {"label": _("Rows Written"), "fieldname": "rows_written", "fieldtype": "Int", "width": 120},
        {"label": _("Rows/sec"), "fieldname": "throughput", "fieldtype": "Float", "precision": "1", "width": 110},
        {"label": _("When"), "fieldname": "start_time", "fieldtype": "Datetime", "width": 170},
    ]

    limit = int(filters.get("limit") or 50)
    rows: list[dict[str, Any]] = frappe.get_all(
        "ClickHouse Sync Log",
        filters={"status": "Completed", "duration": (">", 0)},
        fields=["name", "configuration", "duration", "rows_written", "start_time"],
        order_by="duration desc",
        limit=limit,
    )

    for row in rows:
        duration = row.get("duration") or 0
        row["throughput"] = round((row.get("rows_written") or 0) / duration, 1) if duration else 0.0

    return columns, rows
