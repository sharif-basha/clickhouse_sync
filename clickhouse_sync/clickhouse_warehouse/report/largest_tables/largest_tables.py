# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Largest Tables report: live row counts and on-disk sizes from ClickHouse."""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse.warehouse_stats import readable_size, table_sizes


def execute(filters: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Return ``(columns, data)`` for the Largest Tables report.

    Queries ``system.parts`` on the live ClickHouse server. If ClickHouse is
    unreachable the report degrades to an empty result with a message rather
    than raising, so the Desk page still renders.
    """
    columns = [
        {"label": _("Table"), "fieldname": "table", "fieldtype": "Data", "width": 260},
        {"label": _("Rows (physical)"), "fieldname": "rows", "fieldtype": "Int", "width": 150},
        {"label": _("Size"), "fieldname": "size", "fieldtype": "Data", "width": 120},
        {"label": _("Bytes"), "fieldname": "bytes", "fieldtype": "Int", "width": 160},
        {"label": _("Parts"), "fieldname": "parts", "fieldtype": "Int", "width": 90},
    ]

    try:
        client = ch_connection.get_client()
        database = ch_connection.get_connection_params().database
    except Exception as exc:  # noqa: BLE001
        frappe.msgprint(_("ClickHouse unreachable: {0}").format(str(exc)))
        return columns, []

    data: list[dict[str, Any]] = [
        {**t, "size": readable_size(t["bytes"])}
        for t in table_sizes(client, database, limit=100)
    ]
    return columns, data
