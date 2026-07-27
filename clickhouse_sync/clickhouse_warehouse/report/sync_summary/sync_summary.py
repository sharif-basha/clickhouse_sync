# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Sync Summary report: per-configuration roll-up of sync activity."""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _


def execute(filters: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Return ``(columns, data)`` for the Sync Summary report."""
    filters = filters or {}
    columns = _get_columns()
    data = _get_data(filters)
    return columns, data


def _get_columns() -> list[dict[str, Any]]:
    return [
        {"label": _("Configuration"), "fieldname": "configuration", "fieldtype": "Link", "options": "ClickHouse Sync Configuration", "width": 220},
        {"label": _("Runs"), "fieldname": "runs", "fieldtype": "Int", "width": 80},
        {"label": _("Completed"), "fieldname": "completed", "fieldtype": "Int", "width": 100},
        {"label": _("Failed"), "fieldname": "failed", "fieldtype": "Int", "width": 90},
        {"label": _("Rows Written"), "fieldname": "rows_written", "fieldtype": "Int", "width": 130},
        {"label": _("Avg Duration (s)"), "fieldname": "avg_duration", "fieldtype": "Float", "precision": "2", "width": 140},
        {"label": _("Last Run"), "fieldname": "last_run", "fieldtype": "Datetime", "width": 180},
    ]


def _get_data(filters: dict) -> list[dict[str, Any]]:
    conditions = ""
    params: dict[str, Any] = {}
    if filters.get("from_date"):
        conditions += " AND creation >= %(from_date)s"
        params["from_date"] = filters["from_date"]
    if filters.get("to_date"):
        conditions += " AND creation <= %(to_date)s"
        params["to_date"] = filters["to_date"]

    return frappe.db.sql(
        f"""
        SELECT
            configuration,
            COUNT(*) AS runs,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed,
            COALESCE(SUM(rows_written), 0) AS rows_written,
            COALESCE(AVG(duration), 0) AS avg_duration,
            MAX(creation) AS last_run
        FROM `tabClickHouse Sync Log`
        WHERE 1 = 1 {conditions}
        GROUP BY configuration
        ORDER BY rows_written DESC
        """,
        params,
        as_dict=True,
    )
