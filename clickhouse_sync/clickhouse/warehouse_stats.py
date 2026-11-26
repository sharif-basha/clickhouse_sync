# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Read-only facts about the warehouse tables, shared by every caller.

The dashboard, the older ``api`` module and the Largest Tables report used to
carry their own copies of this ``system.parts`` query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from clickhouse_connect.driver.client import Client


def table_sizes(
    client: "Client",
    database: str,
    *,
    limit: int | None = None,
    time_limit_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """Per-table physical row count, bytes on disk and part count, largest first.

    ``rows`` counts physical rows: re-synced records that have not been merged
    yet are counted more than once (query with ``FINAL`` for record counts).
    """
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    settings = {"max_execution_time": time_limit_seconds} if time_limit_seconds else None
    result = client.query(
        f"""
        SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS parts
        FROM system.parts
        WHERE database = {{db:String}} AND active
        GROUP BY table
        ORDER BY bytes DESC
        {limit_sql}
        """,
        parameters={"db": database},
        settings=settings,
    )
    return [
        {"table": r[0], "rows": int(r[1]), "bytes": int(r[2]), "parts": int(r[3])}
        for r in result.result_rows
    ]


def readable_size(num_bytes: int) -> str:
    """Bytes as text, matching ClickHouse's ``formatReadableSize``."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TiB"  # pragma: no cover
