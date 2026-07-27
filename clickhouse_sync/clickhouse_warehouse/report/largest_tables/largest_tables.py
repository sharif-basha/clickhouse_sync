# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Largest Tables report: live row counts and on-disk sizes from ClickHouse."""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _

from clickhouse_sync.clickhouse import connection as ch_connection


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

    result = client.query(
        """
        SELECT
            table,
            sum(rows) AS rows,
            formatReadableSize(sum(bytes_on_disk)) AS size,
            sum(bytes_on_disk) AS bytes,
            count() AS parts
        FROM system.parts
        WHERE database = {db:String} AND active
        GROUP BY table
        ORDER BY bytes DESC
        LIMIT 100
        """,
        parameters={"db": database},
    )

    data: list[dict[str, Any]] = [
        {"table": r[0], "rows": r[1], "size": r[2], "bytes": r[3], "parts": r[4]}
        for r in result.result_rows
    ]
    return columns, data
