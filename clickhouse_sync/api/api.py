# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Whitelisted API surface for clickhouse_sync.

Every endpoint here is:

* **CSRF-safe** - it relies on Frappe's built-in CSRF handling for session
  requests; token-authenticated API calls bypass CSRF as usual.
* **Permission-checked** - each method calls :func:`_guard` which enforces the
  ``System Manager`` / ``ClickHouse Sync Manager`` roles.
* **Side-effect aware** - anything that launches work uses
  :func:`frappe.enqueue` so HTTP requests return immediately.

These are the endpoints referenced by the desk-wide client script
(``public/js/clickhouse_sync.js``), the configuration form, the workspace
number cards and other apps, so their names and response shapes are kept
stable. The monitoring dashboard uses :mod:`clickhouse_sync.api.dashboard`;
where both need the same data, the logic lives in one place
(:mod:`clickhouse_sync.tasks`, :mod:`clickhouse_sync.clickhouse.warehouse_stats`,
:mod:`clickhouse_sync.api.dashboard`) and this module only adapts the shape.
"""

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import add_days, cint, get_datetime, getdate, now_datetime

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse import live_status
from clickhouse_sync.clickhouse import schema as ch_schema
from clickhouse_sync.clickhouse import sync_engine
from clickhouse_sync.clickhouse.warehouse_stats import readable_size, table_sizes
from clickhouse_sync.tasks import enqueue_config_sync

ALLOWED_ROLES = ("System Manager", "ClickHouse Sync Manager")


def _guard() -> None:
    """Enforce role-based access for every API call."""
    frappe.only_for(ALLOWED_ROLES)


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #
@frappe.whitelist()
def test_connection() -> dict[str, Any]:
    """Ping ClickHouse and return a health report."""
    _guard()
    return ch_connection.health_check()


# --------------------------------------------------------------------------- #
# Sync control
# --------------------------------------------------------------------------- #
@frappe.whitelist(methods=["POST"])
def sync_now(configuration: str, trigger: str = "Manual") -> dict[str, Any]:
    """Enqueue a background sync for a single configuration.

    Args:
        configuration: Name of the ClickHouse Sync Configuration.
        trigger: ``"Manual"`` (default) or ``"Scheduler"``.
    """
    _guard()
    if not frappe.db.exists("ClickHouse Sync Configuration", configuration):
        frappe.throw(_("Unknown configuration: {0}").format(configuration))

    outcome = enqueue_config_sync(configuration, trigger=trigger)
    return {
        "queued": outcome["queued"],
        "job_id": outcome["job_id"],
        "reason": outcome["reason"],
        "configuration": configuration,
    }


@frappe.whitelist(methods=["POST"])
def sync_all(trigger: str = "Manual") -> dict[str, Any]:
    """Enqueue syncs for every enabled configuration, in priority order."""
    _guard()
    configs = frappe.get_all(
        "ClickHouse Sync Configuration",
        filters={"enabled": 1},
        fields=["name"],
        order_by="priority asc",
    )
    queued: list[str] = []
    for config in configs:
        if enqueue_config_sync(config.name, trigger=trigger)["queued"]:
            queued.append(config.name)
    return {"queued": queued, "count": len(queued)}


def count_running_syncs(configuration: str | None = None) -> int:
    """Number of syncs running right now, from the workers' live records.

    Sync Logs are written when a run ENDS, so counting logs with status
    "Running" (as this module and the number card used to) always gave 0.
    """
    names = (
        [configuration]
        if configuration
        else frappe.get_all("ClickHouse Sync Configuration", pluck="name")
    )
    running = 0
    for name in names:
        state = live_status.get_live(name)
        if state and state.get("status") == "Running":
            running += 1
    return running


def _age_text(seconds: float) -> str:
    minutes = int(seconds // 60)
    if minutes < 1:
        return _("just now")
    if minutes < 60:
        return _("{0} min ago").format(minutes)
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return _("{0} h {1} min ago").format(hours, minutes)
    days, hours = divmod(hours, 24)
    return _("{0} d {1} h ago").format(days, hours)


def freshness_summary(configs: list[dict], now: Any) -> str:
    """How out of date the warehouse is, as one line for a number card.

    Names the enabled DocType whose last successful sync is the oldest
    (its sync position, *Last Sync Timestamp*), or one that has never synced.
    """
    if not configs:
        return _("No DocTypes enabled")
    never = [c["source_doctype"] for c in configs if not c.get("last_sync_timestamp")]
    if never:
        more = _(" (+{0} more)").format(len(never) - 1) if len(never) > 1 else ""
        return _("Never synced: {0}{1}").format(never[0], more)
    oldest = min(configs, key=lambda c: get_datetime(c["last_sync_timestamp"]))
    age = (get_datetime(now) - get_datetime(oldest["last_sync_timestamp"])).total_seconds()
    return f"{_age_text(max(age, 0))} · {oldest['source_doctype']}"


@frappe.whitelist()
def data_freshness_card() -> dict[str, Any]:
    """Data source for the "Oldest Sync" number card (Custom card)."""
    _guard()
    configs = frappe.get_all(
        "ClickHouse Sync Configuration",
        filters={"enabled": 1},
        fields=["source_doctype", "last_sync_timestamp"],
    )
    return {"value": freshness_summary(configs, now_datetime()), "fieldtype": "Data"}


@frappe.whitelist()
def running_syncs_card() -> dict[str, Any]:
    """Data source for the "Running Syncs" number card (Custom card)."""
    _guard()
    return {"value": count_running_syncs(), "fieldtype": "Int"}


@frappe.whitelist(methods=["POST"])
def cancel_sync(configuration: str) -> dict[str, Any]:
    """Request cancellation of a running sync for a configuration."""
    _guard()
    sync_engine.request_cancel(configuration)
    return {"cancelled": True, "configuration": configuration}


@frappe.whitelist()
def get_progress(configuration: str) -> dict[str, Any]:
    """Return the latest known progress for a configuration.

    Progress is primarily pushed over realtime; this polling endpoint is a
    fallback for clients that miss the socket events. It reports the newest
    log row and whether a run appears to be active.
    """
    _guard()
    latest = frappe.get_all(
        "ClickHouse Sync Log",
        filters={"configuration": configuration},
        fields=["name", "status", "rows_read", "rows_written", "rows_failed", "duration"],
        order_by="creation desc",
        limit=1,
    )
    running = count_running_syncs(configuration)
    return {
        "configuration": configuration,
        "running": bool(running),
        "latest": latest[0] if latest else None,
    }


# --------------------------------------------------------------------------- #
# Schema / table status
# --------------------------------------------------------------------------- #
@frappe.whitelist()
def schema_status(configuration: str) -> dict[str, Any]:
    """Return the diff between the desired and live ClickHouse schema."""
    _guard()
    config = frappe.get_doc("ClickHouse Sync Configuration", configuration)
    client = ch_connection.get_client()
    database = ch_connection.get_connection_params().database
    table_schema = ch_schema.build_table_schema(
        config.source_doctype,
        include_soft_delete=(config.delete_strategy == "Soft Delete"),
        exclude_fields=set(
            sync_engine.parse_excluded_fields(config.get("excluded_fields"))
        ),
    )
    exists = ch_schema.table_exists(client, database, table_schema.table_name)
    diff = (
        ch_schema.diff_schema(client, table_schema, database)
        if exists
        else ch_schema.SchemaDiff(added=list(table_schema.columns))
    )
    return {
        "configuration": configuration,
        "table_name": table_schema.table_name,
        "table_exists": exists,
        "missing_columns": [c.name for c in diff.added],
        "widenable_columns": [w[0] for w in diff.widened],
        "incompatible_columns": [i[0] for i in diff.ignored],
        "in_sync": exists and not diff.has_changes,
    }


@frappe.whitelist()
def table_status() -> list[dict[str, Any]]:
    """Return per-table row counts and sizes from ClickHouse ``system.parts``."""
    _guard()
    client = ch_connection.get_client()
    database = ch_connection.get_connection_params().database
    return [
        {"table": t["table"], "rows": t["rows"], "size": readable_size(t["bytes"]), "bytes": t["bytes"]}
        for t in table_sizes(client, database)
    ]


# --------------------------------------------------------------------------- #
# Dashboard / statistics
# --------------------------------------------------------------------------- #
@frappe.whitelist()
def dashboard() -> dict[str, Any]:
    """Deprecated alias of :func:`statistics`, kept for existing callers."""
    _guard()
    return statistics()


@frappe.whitelist()
def statistics() -> dict[str, Any]:
    """Compute headline statistics across all sync activity."""
    _guard()
    today = frappe.utils.today()

    running = count_running_syncs()
    failed_today = frappe.db.count(
        "ClickHouse Sync Log",
        {"status": "Failed", "creation": (">=", today)},
    )

    # A range on ``creation`` can use its index; ``DATE(creation) = ...``
    # forced a scan of the whole log table.
    rows_today = (
        frappe.db.sql(
            """
            SELECT COALESCE(SUM(rows_written), 0)
            FROM `tabClickHouse Sync Log`
            WHERE creation >= %(today)s AND creation < %(tomorrow)s
            """,
            {"today": today, "tomorrow": add_days(getdate(today), 1)},
        )[0][0]
        or 0
    )
    rows_total = (
        frappe.db.sql(
            "SELECT COALESCE(SUM(rows_written), 0) FROM `tabClickHouse Sync Log`"
        )[0][0]
        or 0
    )
    avg_duration = (
        frappe.db.sql(
            """
            SELECT COALESCE(AVG(duration), 0)
            FROM `tabClickHouse Sync Log`
            WHERE status = 'Completed'
            """
        )[0][0]
        or 0.0
    )

    largest_table = None
    try:
        tables = table_status()
        if tables:
            largest_table = {"table": tables[0]["table"], "rows": tables[0]["rows"]}
    except Exception:  # noqa: BLE001 - ClickHouse may be unreachable; degrade
        largest_table = None

    recent_logs = frappe.get_all(
        "ClickHouse Sync Log",
        fields=[
            "name",
            "configuration",
            "status",
            "rows_written",
            "duration",
            "creation",
        ],
        order_by="creation desc",
        limit=10,
    )

    return {
        "running_syncs": running,
        "failed_syncs": failed_today,
        "rows_today": int(rows_today),
        "rows_total": int(rows_total),
        "average_duration": round(float(avg_duration), 3),
        "largest_table": largest_table,
        "recent_logs": recent_logs,
        "as_of": get_datetime(now_datetime()).isoformat(),
    }


@frappe.whitelist()
def analytics(days: int = 30) -> dict[str, Any]:
    """Aggregate everything the analytics dashboard page renders in one call.

    Combines sync-performance metrics (from the ClickHouse Sync Log table in
    MariaDB) with live warehouse metrics (from ClickHouse ``system.parts``).
    Every ClickHouse-dependent section degrades gracefully to empty when the
    server is unreachable, so the dashboard still renders from log data alone.

    Args:
        days: Size of the trailing window for the time-series, in days.

    Returns:
        A dict with ``headline``, ``rows_per_day``, ``status_breakdown``,
        ``rows_per_doctype``, ``recent_logs`` and ``clickhouse_tables`` keys.
    """
    _guard()
    window = max(1, min(cint(days) or 30, 365))

    # --- Headline numbers -------------------------------------------------- #
    base = statistics()
    total_runs = frappe.db.count("ClickHouse Sync Log")
    completed_runs = frappe.db.count("ClickHouse Sync Log", {"status": "Completed"})
    success_rate = round((completed_runs / total_runs) * 100, 1) if total_runs else 0.0

    headline = {
        "rows_total": base["rows_total"],
        "rows_today": base["rows_today"],
        "running_syncs": base["running_syncs"],
        "failed_today": base["failed_syncs"],
        "average_duration": base["average_duration"],
        "success_rate": success_rate,
        "total_runs": total_runs,
        "largest_table": base["largest_table"],
    }

    # --- Rows synced per day (time series) --------------------------------- #
    # Same per-day series as the monitoring dashboard (one implementation),
    # including days with no runs.
    from clickhouse_sync.api import dashboard as monitoring

    since = add_days(getdate(frappe.utils.today()), -window)
    series = monitoring._analytics(get_datetime(since), window)
    rows_per_day = {"labels": series["labels"], "values": series["rows_written"]}

    # --- Status breakdown -------------------------------------------------- #
    status_raw = frappe.db.sql(
        """
        SELECT status, COUNT(*) AS count
        FROM `tabClickHouse Sync Log`
        GROUP BY status
        ORDER BY count DESC
        """,
        as_dict=True,
    )
    status_breakdown = {
        "labels": [r["status"] for r in status_raw],
        "values": [int(r["count"]) for r in status_raw],
    }

    # --- Rows per configuration (DocType) ---------------------------------- #
    per_doctype_raw = frappe.db.sql(
        """
        SELECT configuration, COALESCE(SUM(rows_written), 0) AS rows_written
        FROM `tabClickHouse Sync Log`
        GROUP BY configuration
        ORDER BY rows_written DESC
        LIMIT 10
        """,
        as_dict=True,
    )
    rows_per_doctype = {
        "labels": [r["configuration"] for r in per_doctype_raw],
        "values": [int(r["rows_written"]) for r in per_doctype_raw],
    }

    # --- Live ClickHouse table footprint ----------------------------------- #
    clickhouse_tables: list[dict[str, Any]] = []
    try:
        clickhouse_tables = table_status()
    except Exception:  # noqa: BLE001 - warehouse may be unreachable; degrade
        clickhouse_tables = []

    return {
        "headline": headline,
        "rows_per_day": rows_per_day,
        "status_breakdown": status_breakdown,
        "rows_per_doctype": rows_per_doctype,
        "recent_logs": base["recent_logs"],
        "clickhouse_tables": clickhouse_tables,
        "window_days": window,
        "as_of": base["as_of"],
    }
