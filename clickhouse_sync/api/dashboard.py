# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""API behind the ClickHouse Sync monitoring dashboard.

Every number returned here comes from one of three backend sources:

* **MariaDB** - the source tables (record counts) and ClickHouse Sync Log
  (run history, failures, throughput).
* **ClickHouse** - the warehouse tables (record counts, table sizes).
* **Redis** - the live record each sync worker keeps up to date while it runs
  (:mod:`clickhouse_sync.clickhouse.live_status`).

Nothing is estimated in the browser. Values that cannot be known are returned
as ``None`` and shown as a dash, with the reason in a tooltip.

Record counts on large tables are not free, so they are cached in Redis for
:data:`COUNTS_FRESH_SECONDS` and recomputed on demand with :func:`recount`.
Source counts run under a MariaDB statement time limit and fall back to the
table-statistics estimate (flagged ``approximate``) if the limit is hit.

Read endpoints accept GET; every endpoint that starts or stops work is
POST-only. All endpoints require the System Manager or ClickHouse Sync
Manager role.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import frappe
from frappe import _
from frappe.utils import cint, get_datetime, now_datetime

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse import live_status
from clickhouse_sync.clickhouse.exceptions import categorize_text
from clickhouse_sync.clickhouse.warehouse_stats import table_sizes
from clickhouse_sync.clickhouse.schema import (
    SOFT_DELETE_COLUMN,
    table_exists,
    table_name_for,
)
from clickhouse_sync.clickhouse.sync_engine import (
    VALID_SYNC_MODES,
    _cancel_requested_at,
    request_cancel,
)

ALLOWED_ROLES = ("System Manager", "ClickHouse Sync Manager")
CONFIG_DOCTYPE = "ClickHouse Sync Configuration"
JOB_TIMEOUT_SECONDS = 21600

COUNTS_FRESH_SECONDS = 600
# Pending counts change with every save, so they are refreshed far more often
# than the full counts - but not on every 30-second dashboard refresh, which
# ran one COUNT per configuration per request and could time the page out.
PENDING_FRESH_SECONDS = 60
# Failed runs whose category is worked out for the "by category" breakdown.
ERROR_CATEGORY_SAMPLE = 500
COUNTS_STORE_SECONDS = 3600
SOURCE_COUNT_TIME_LIMIT_SECONDS = 10
WAREHOUSE_COUNT_TIME_LIMIT_SECONDS = 20

# A queued record whose job has disappeared from the queue for this long is
# removed (the queue de-duplicated it, or a worker discarded it).
QUEUED_ORPHAN_SECONDS = 120

MAX_WINDOW_DAYS = 90


def _guard() -> None:
    frappe.only_for(ALLOWED_ROLES)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _configs() -> list[dict[str, Any]]:
    return frappe.db.sql(
        f"""
        SELECT name, source_doctype, enabled, scheduler_enabled, sync_mode,
               delete_strategy, priority, last_sync_timestamp, sync_interval_minutes
        FROM `tab{CONFIG_DOCTYPE}`
        ORDER BY priority ASC, name ASC
        """,
        as_dict=True,
    )


def _job_id(config_name: str) -> str:
    return f"clickhouse_sync::{config_name}"


def _job_active(job_id: str) -> bool | None:
    """``True``/``False`` if the queue knows the job, ``None`` if unknown."""
    try:
        from frappe.utils.background_jobs import is_job_enqueued
    except Exception:  # noqa: BLE001 - older Frappe without the helper
        return None
    try:
        return bool(is_job_enqueued(job_id))
    except Exception:  # noqa: BLE001 - Redis queue unreachable
        return None


def _age_seconds(value: Any, now: Any) -> float | None:
    if not value:
        return None
    return (now - get_datetime(value)).total_seconds()


def _window(days: Any) -> int:
    return max(1, min(cint(days) or 7, MAX_WINDOW_DAYS))


def _enqueue(config_name: str, doctype: str | None, mode: str | None, trigger: str = "Manual") -> dict[str, Any]:
    from clickhouse_sync.tasks import enqueue_config_sync

    return enqueue_config_sync(
        config_name, trigger=trigger, mode=mode, doctype=doctype, job_active=_job_active
    )


def _validate_mode(mode: str | None) -> str | None:
    if mode in (None, "", "Default"):
        return None
    if mode not in VALID_SYNC_MODES:
        frappe.throw(_("Sync mode must be Full or Incremental."))
    return mode


def _config_or_throw(configuration: str) -> dict[str, Any]:
    for config in _configs():
        if config["name"] == configuration:
            return config
    frappe.throw(_("Unknown configuration: {0}").format(configuration))
    return {}  # unreachable


# --------------------------------------------------------------------------- #
# Live runs
# --------------------------------------------------------------------------- #
def _live_runs(configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Queued and running syncs, with server-derived speed, progress and ETA."""
    now = now_datetime()
    runs: list[dict[str, Any]] = []
    for config in configs:
        state = live_status.get_live(config["name"])
        if not state:
            continue
        active = _job_active(state.get("job_id") or _job_id(config["name"]))

        if state.get("status") == "Queued":
            age = _age_seconds(state.get("queued_at"), now)
            if active is False and age is not None and age > QUEUED_ORPHAN_SECONDS:
                live_status.clear(config["name"])
                continue

        run = live_status.derive_metrics(state, now)
        run["doctype"] = run.get("doctype") or config["source_doctype"]
        run["stop_requested"] = bool(_cancel_requested_at(config["name"]))
        # A running record whose job is gone from the queue and has stopped
        # reporting means the worker died: show it, but as lost.
        run["worker_lost"] = bool(run.get("stalled") and active is False)
        if run["stalled"]:
            run["display_status"] = "Stalled"
        else:
            run["display_status"] = run.get("status")
        runs.append(run)

    order = {"Running": 0, "Stalled": 1, "Queued": 2}
    runs.sort(key=lambda r: (order.get(r["display_status"], 3), r.get("started_at") or r.get("queued_at") or ""))
    return runs


# --------------------------------------------------------------------------- #
# Record counts
# --------------------------------------------------------------------------- #
def _source_count(doctype: str) -> tuple[int | None, bool]:
    """Exact ``COUNT(*)`` under a time limit; table estimate as a fallback."""
    table = f"tab{doctype}"
    try:
        rows = frappe.db.sql(
            f"SET STATEMENT max_statement_time={SOURCE_COUNT_TIME_LIMIT_SECONDS} FOR "
            f"SELECT COUNT(*) FROM `{table}`"
        )
        return int(rows[0][0]), False
    except Exception:  # noqa: BLE001 - timeout or no SET STATEMENT support
        pass
    try:
        rows = frappe.db.sql(
            "SELECT table_rows FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %(t)s",
            {"t": table},
        )
        if rows and rows[0][0] is not None:
            return int(rows[0][0]), True
    except Exception:  # noqa: BLE001
        pass
    return None, False


def _pending_count(doctype: str, cursor: Any, source: int | None) -> int | None:
    """Rows changed since the last successful sync position."""
    if not cursor:
        return source
    try:
        rows = frappe.db.sql(
            f"SET STATEMENT max_statement_time={SOURCE_COUNT_TIME_LIMIT_SECONDS} FOR "
            f"SELECT COUNT(*) FROM `tab{doctype}` WHERE modified > %(cursor)s",
            {"cursor": get_datetime(cursor)},
        )
        return int(rows[0][0])
    except Exception:  # noqa: BLE001
        return None


class _Warehouse:
    """One ClickHouse connection attempt per request, shared by all counts."""

    def __init__(self) -> None:
        self._client: Any = None
        self._tried = False
        self.ok: bool | None = None
        self.message = ""
        self.database = ""

    def client(self) -> Any:
        if not self._tried:
            self._tried = True
            try:
                self.database = ch_connection.get_connection_params().database
                self._client = ch_connection.get_client(retries=1)
                self.ok = True
                self.message = _("Connected")
            except Exception as exc:  # noqa: BLE001
                self.ok = False
                self.message = str(exc)[:300]
        return self._client

    def count(self, doctype: str, soft_delete: bool) -> tuple[int | None, str | None]:
        client = self.client()
        if client is None:
            return None, "unreachable"
        table = table_name_for(doctype)
        try:
            if not table_exists(client, self.database, table):
                return 0, "no table"
            where = f" WHERE `{SOFT_DELETE_COLUMN}` = 0" if soft_delete else ""
            result = client.query(
                f"SELECT count() FROM `{self.database}`.`{table}` FINAL{where}",
                settings={"max_execution_time": WAREHOUSE_COUNT_TIME_LIMIT_SECONDS},
            )
            return int(result.result_rows[0][0]), None
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)[:200]


def _counts_for(config: dict[str, Any], warehouse: _Warehouse, force: bool) -> dict[str, Any]:
    now = now_datetime()
    cached = None if force else live_status.get_counts(config["name"])
    if cached and (_age_seconds(cached.get("counted_at"), now) or 0) < COUNTS_FRESH_SECONDS:
        cached = dict(cached)
        pending_fresh = (
            "pending" in cached
            and cached.get("pending_cursor") == str(config.get("last_sync_timestamp"))
            and (_age_seconds(cached.get("pending_at"), now) or PENDING_FRESH_SECONDS)
            < PENDING_FRESH_SECONDS
        )
        if not pending_fresh:
            cached["pending"] = _pending_count(
                config["source_doctype"], config.get("last_sync_timestamp"), cached.get("source")
            )
            cached["pending_at"] = now.isoformat(sep=" ")
            cached["pending_cursor"] = str(config.get("last_sync_timestamp"))
            live_status.set_counts(config["name"], cached, COUNTS_STORE_SECONDS)
        return cached

    source, approximate = _source_count(config["source_doctype"])
    wh_count, wh_note = warehouse.count(
        config["source_doctype"], config.get("delete_strategy") == "Soft Delete"
    )
    counts = {
        "source": source,
        "source_approximate": approximate,
        "warehouse": wh_count,
        "warehouse_note": wh_note,
        "counted_at": now.isoformat(sep=" "),
    }
    counts["pending"] = _pending_count(
        config["source_doctype"], config.get("last_sync_timestamp"), source
    )
    counts["pending_at"] = now.isoformat(sep=" ")
    counts["pending_cursor"] = str(config.get("last_sync_timestamp"))
    # Do not cache a warehouse failure: the next request should try again.
    if wh_count is not None or wh_note == "no table":
        live_status.set_counts(config["name"], counts, COUNTS_STORE_SECONDS)
    return counts


def _sync_percent(source: int | None, pending: int | None) -> float | None:
    if source is None or pending is None:
        return None
    if source == 0:
        return 100.0
    return round(max(0.0, min(1.0, (source - pending) / source)) * 100, 1)


# --------------------------------------------------------------------------- #
# Log-derived facts
# --------------------------------------------------------------------------- #
def _per_config_history() -> dict[str, dict[str, Any]]:
    rows = frappe.db.sql(
        """
        SELECT configuration,
               MAX(CASE WHEN status = 'Completed' THEN end_time END) AS last_success,
               MAX(CASE WHEN status = 'Failed' THEN COALESCE(end_time, creation) END) AS last_failure
        FROM `tabClickHouse Sync Log`
        GROUP BY configuration
        """,
        as_dict=True,
    )
    history = {r["configuration"]: dict(r) for r in rows}

    latest = frappe.db.sql(
        """
        SELECT l.configuration, l.name, l.status, l.message, l.creation, l.rows_written, l.duration
        FROM `tabClickHouse Sync Log` l
        JOIN (
            SELECT configuration, MAX(creation) AS latest
            FROM `tabClickHouse Sync Log`
            WHERE status IN ('Completed', 'Failed', 'Cancelled')
            GROUP BY configuration
        ) x ON x.configuration = l.configuration AND x.latest = l.creation
        """,
        as_dict=True,
    )
    for row in latest:
        entry = history.setdefault(row["configuration"], {"configuration": row["configuration"]})
        entry["last_log"] = row["name"]
        entry["last_status"] = row["status"]
        entry["last_message"] = row["message"]
        entry["last_run_at"] = row["creation"]
        entry["last_rows_written"] = row["rows_written"]
        entry["last_duration"] = row["duration"]
    return history


def _errors(since: Any, limit: int = 25) -> dict[str, Any]:
    # The category breakdown used to count only the 25 rows listed, while the
    # totals counted every failure in the window. Categorise a much larger
    # sample (newest first) and say how many were included.
    rows = frappe.db.sql(
        """
        SELECT name, configuration, creation, end_time, message, exception,
               rows_failed, rows_read, `trigger`
        FROM `tabClickHouse Sync Log`
        WHERE status = 'Failed' AND creation >= %(since)s
        ORDER BY creation DESC
        LIMIT %(limit)s
        """,
        {"since": since, "limit": max(limit, ERROR_CATEGORY_SAMPLE)},
        as_dict=True,
    )
    recent = []
    categories: dict[str, int] = {}
    for index, row in enumerate(rows):
        detail = live_status.get_run_detail(row["name"]) or {}
        category = detail.get("category") or categorize_text(row.get("message"), row.get("exception"))
        categories[category] = categories.get(category, 0) + 1
        if index >= limit:
            continue
        recent.append(
            {
                "log": row["name"],
                "configuration": row["configuration"],
                "at": row["end_time"] or row["creation"],
                "message": row["message"],
                "category": category,
                "rows_failed": row["rows_failed"],
                "trigger": row["trigger"],
                "batch": detail.get("batch"),
                "retries": detail.get("retries"),
                "phase": detail.get("phase"),
                "cursor_modified": detail.get("cursor_modified"),
                "detail_available": bool(detail),
            }
        )

    totals = frappe.db.sql(
        """
        SELECT COUNT(*) AS runs, COALESCE(SUM(rows_failed), 0) AS rows_failed
        FROM `tabClickHouse Sync Log`
        WHERE status = 'Failed' AND creation >= %(since)s
        """,
        {"since": since},
        as_dict=True,
    )[0]
    return {
        "recent": recent,
        "failed_runs": int(totals["runs"] or 0),
        "failed_rows": int(totals["rows_failed"] or 0),
        "by_category": [
            {"category": k, "count": v} for k, v in sorted(categories.items(), key=lambda kv: -kv[1])
        ],
        "categorised_runs": len(rows),
    }


def _analytics(since: Any, days: int) -> dict[str, Any]:
    per_day = frappe.db.sql(
        """
        SELECT DATE(creation) AS day,
               COALESCE(SUM(rows_written), 0) AS rows_written,
               SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed,
               SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled
        FROM `tabClickHouse Sync Log`
        WHERE creation >= %(since)s
        GROUP BY DATE(creation)
        ORDER BY day ASC
        """,
        {"since": since},
        as_dict=True,
    )
    by_day = {str(r["day"]): r for r in per_day}
    labels, rows_written, completed, failed, cancelled = [], [], [], [], []
    start = get_datetime(since).date()
    for offset in range(days + 1):
        day = str(start + timedelta(days=offset))
        if day > str(now_datetime().date()):
            break
        r = by_day.get(day, {})
        labels.append(day)
        rows_written.append(int(r.get("rows_written") or 0))
        completed.append(int(r.get("completed") or 0))
        failed.append(int(r.get("failed") or 0))
        cancelled.append(int(r.get("cancelled") or 0))

    window_totals = frappe.db.sql(
        """
        SELECT COUNT(*) AS runs,
               SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed,
               COALESCE(SUM(rows_written), 0) AS rows_written,
               AVG(CASE WHEN status = 'Completed' THEN duration END) AS avg_duration
        FROM `tabClickHouse Sync Log`
        WHERE creation >= %(since)s AND status IN ('Completed', 'Failed', 'Cancelled')
        """,
        {"since": since},
        as_dict=True,
    )[0]
    runs = int(window_totals["runs"] or 0)

    recent = frappe.db.sql(
        """
        SELECT name, configuration, status, `trigger`, rows_read, rows_written,
               rows_failed, duration, creation
        FROM `tabClickHouse Sync Log`
        ORDER BY creation DESC
        LIMIT 15
        """,
        as_dict=True,
    )
    return {
        "labels": labels,
        "rows_written": rows_written,
        "runs_completed": completed,
        "runs_failed": failed,
        "runs_cancelled": cancelled,
        "runs": runs,
        "rows_written_total": int(window_totals["rows_written"] or 0),
        "success_rate": round((int(window_totals["completed"] or 0) / runs) * 100, 1) if runs else None,
        "avg_duration": round(float(window_totals["avg_duration"]), 1) if window_totals["avg_duration"] is not None else None,
        "recent_runs": recent,
    }


def _warehouse_tables(warehouse: _Warehouse) -> list[dict[str, Any]]:
    client = warehouse.client()
    if client is None:
        return []
    try:
        return table_sizes(
            client, warehouse.database, limit=15,
            time_limit_seconds=WAREHOUSE_COUNT_TIME_LIMIT_SECONDS,
        )
    except Exception:  # noqa: BLE001
        return []


# --------------------------------------------------------------------------- #
# Read endpoints
# --------------------------------------------------------------------------- #
@frappe.whitelist()
def overview(days: int = 7) -> dict[str, Any]:
    """Everything the dashboard renders, in one call.

    Args:
        days: Trailing window for errors and analytics (1-90). Record counts
            and live runs are always current.
    """
    _guard()
    window = _window(days)
    now = now_datetime()
    since = (now - timedelta(days=window)).replace(hour=0, minute=0, second=0, microsecond=0)

    from clickhouse_sync.tasks import _last_started, next_run_at

    configs = _configs()
    warehouse = _Warehouse()
    history = _per_config_history()
    last_started = _last_started()
    live_runs = _live_runs(configs)
    live_by_config = {r["configuration"]: r for r in live_runs}

    doctypes: list[dict[str, Any]] = []
    totals = {"source": 0, "warehouse": 0, "pending": 0}
    complete = {"source": True, "warehouse": True, "pending": True}
    approximate = False
    oldest_count = None

    for config in configs:
        counts = _counts_for(config, warehouse, force=False) if config["enabled"] else {}
        hist = history.get(config["name"], {})
        last_run = live_status.get_last_run(config["name"]) or {}
        source, wh, pending = counts.get("source"), counts.get("warehouse"), counts.get("pending")
        if config["enabled"]:
            for key, value in (("source", source), ("warehouse", wh), ("pending", pending)):
                if value is None:
                    complete[key] = False
                else:
                    totals[key] += value
            approximate = approximate or bool(counts.get("source_approximate"))
            counted_at = counts.get("counted_at")
            if counted_at and (oldest_count is None or counted_at < oldest_count):
                oldest_count = counted_at

        live = live_by_config.get(config["name"])
        scheduled = bool(config["enabled"] and config["scheduler_enabled"])
        next_run = (
            next_run_at(config.get("sync_interval_minutes"), last_started.get(config["name"]), now)
            if scheduled
            else None
        )
        doctypes.append(
            {
                "configuration": config["name"],
                "doctype": config["source_doctype"],
                "table": table_name_for(config["source_doctype"]),
                "enabled": bool(config["enabled"]),
                "scheduled": bool(config["scheduler_enabled"]),
                "paused": bool(config["enabled"] and not config["scheduler_enabled"]),
                "sync_interval": cint(config.get("sync_interval_minutes")) or 15,
                "next_run": next_run.isoformat(sep=" ") if next_run else None,
                "mode": config["sync_mode"] or "Full",
                "delete_strategy": config["delete_strategy"] or "Ignore",
                "cursor": config["last_sync_timestamp"],
                "source": source,
                "source_approximate": bool(counts.get("source_approximate")),
                "warehouse": wh,
                "warehouse_note": counts.get("warehouse_note"),
                "difference": (wh - source) if (wh is not None and source is not None) else None,
                "pending": pending,
                "sync_percent": _sync_percent(source, pending),
                "counted_at": counts.get("counted_at"),
                "last_success": hist.get("last_success"),
                "last_failure": hist.get("last_failure"),
                "last_status": hist.get("last_status"),
                "last_log": hist.get("last_log"),
                "last_message": hist.get("last_message"),
                "last_run_at": hist.get("last_run_at"),
                "last_retries": last_run.get("retries"),
                "last_category": last_run.get("category") if hist.get("last_status") == "Failed" else None,
                "live_status": live["display_status"] if live else None,
                "live_percent": live.get("percent") if live else None,
            }
        )

    errors = _errors(since)
    analytics = _analytics(since, window)
    last_success = max(
        (d["last_success"] for d in doctypes if d["last_success"]), default=None,
        key=lambda v: get_datetime(v),
    )
    enabled = [d for d in doctypes if d["enabled"]]
    failed_doctypes = [d["configuration"] for d in enabled if d["last_status"] == "Failed"]

    # Fetch table sizes (which also connects, if the counts above all came
    # from the cache) BEFORE reporting the connection state.
    warehouse_tables = _warehouse_tables(warehouse)

    overall = {
        "configured": len(doctypes),
        "enabled": len(enabled),
        "source_records": totals["source"] if complete["source"] else None,
        "warehouse_records": totals["warehouse"] if complete["warehouse"] else None,
        "pending_records": totals["pending"] if complete["pending"] else None,
        "source_partial": not complete["source"],
        "warehouse_partial": not complete["warehouse"],
        "pending_partial": not complete["pending"],
        "failed_records": errors["failed_rows"],
        "failed_runs": errors["failed_runs"],
        "failed_doctypes": failed_doctypes,
        "sync_percent": _sync_percent(
            totals["source"] if complete["source"] else None,
            totals["pending"] if complete["pending"] else None,
        ),
        "last_success": last_success,
        "counts_as_of": oldest_count,
        "counts_approximate": approximate,
        "running": sum(1 for r in live_runs if r["display_status"] == "Running"),
        "queued": sum(1 for r in live_runs if r["display_status"] == "Queued"),
        "stalled": sum(1 for r in live_runs if r["display_status"] == "Stalled"),
    }

    return {
        "as_of": now.isoformat(sep=" "),
        "window_days": window,
        "clickhouse": {"ok": warehouse.ok, "message": warehouse.message, "database": warehouse.database},
        "overall": overall,
        "live": live_runs,
        "doctypes": doctypes,
        "errors": errors,
        "analytics": analytics,
        "warehouse_tables": warehouse_tables,
    }


@frappe.whitelist()
def live() -> dict[str, Any]:
    """Lightweight poll for the live monitor (no counts, no ClickHouse)."""
    _guard()
    return {"as_of": now_datetime().isoformat(sep=" "), "live": _live_runs(_configs())}


@frappe.whitelist()
def error_detail(log: str) -> dict[str, Any]:
    """Full error report for one failed run: message, traceback, run detail."""
    _guard()
    rows = frappe.db.sql(
        """
        SELECT name, configuration, status, `trigger`, start_time, end_time, duration,
               rows_read, rows_written, rows_failed, message, exception, execution_host
        FROM `tabClickHouse Sync Log` WHERE name = %(name)s
        """,
        {"name": log},
        as_dict=True,
    )
    if not rows:
        frappe.throw(_("Unknown log: {0}").format(log))
    row = dict(rows[0])
    detail = live_status.get_run_detail(log) or {}
    row["category"] = detail.get("category") or categorize_text(row.get("message"), row.get("exception"))
    row["detail"] = detail
    return row


# --------------------------------------------------------------------------- #
# Control endpoints (POST only)
# --------------------------------------------------------------------------- #
@frappe.whitelist(methods=["POST"])
def start_sync(configuration: str, mode: str | None = None) -> dict[str, Any]:
    """Queue one configuration. ``mode`` = Full/Incremental for a one-off run."""
    _guard()
    config = _config_or_throw(configuration)
    if not config["enabled"]:
        frappe.throw(_("Configuration {0} is disabled.").format(configuration))
    run_mode = _validate_mode(mode)
    outcome = _enqueue(configuration, config["source_doctype"], run_mode)
    if not outcome["queued"]:
        # Tell the user instead of pretending: the job already waiting or
        # running keeps its own mode.
        return {
            "queued": [],
            "skipped": [configuration],
            "job_id": outcome["job_id"],
            "mode": run_mode,
            "message": _("A sync for {0} is already queued or running.").format(configuration),
        }
    return {"queued": [configuration], "job_id": outcome["job_id"], "mode": run_mode}


@frappe.whitelist(methods=["POST"])
def start_all(mode: str | None = None) -> dict[str, Any]:
    """Queue every enabled configuration, in priority order."""
    _guard()
    run_mode = _validate_mode(mode)
    queued, skipped = [], []
    for config in _configs():
        if config["enabled"]:
            if _enqueue(config["name"], config["source_doctype"], run_mode)["queued"]:
                queued.append(config["name"])
            else:
                skipped.append(config["name"])
    return {"queued": queued, "skipped": skipped, "mode": run_mode}


@frappe.whitelist(methods=["POST"])
def retry_failed() -> dict[str, Any]:
    """Queue every enabled configuration whose latest finished run failed."""
    _guard()
    history = _per_config_history()
    queued, skipped = [], []
    for config in _configs():
        if config["enabled"] and history.get(config["name"], {}).get("last_status") == "Failed":
            if _enqueue(config["name"], config["source_doctype"], None)["queued"]:
                queued.append(config["name"])
            else:
                skipped.append(config["name"])
    return {"queued": queued, "skipped": skipped}


@frappe.whitelist(methods=["POST"])
def set_paused(configuration: str, paused: Any = 1) -> dict[str, Any]:
    """Pause or resume one configuration's scheduled syncs.

    Pausing turns off *Scheduler Enabled*: the scheduler skips the DocType
    until it is resumed. Manual runs from the dashboard still work, and a
    run already in progress is not stopped (use Stop for that).
    """
    _guard()
    _config_or_throw(configuration)
    if not frappe.has_permission(CONFIG_DOCTYPE, "write", configuration):
        frappe.throw(_("You are not allowed to change {0}.").format(configuration), frappe.PermissionError)
    pause = cint(paused) == 1
    frappe.db.set_value(CONFIG_DOCTYPE, configuration, "scheduler_enabled", 0 if pause else 1)
    frappe.db.commit()
    return {"configuration": configuration, "paused": pause}


@frappe.whitelist(methods=["POST"])
def stop_sync(configuration: str) -> dict[str, Any]:
    """Ask a running sync to stop after its current batch."""
    _guard()
    _config_or_throw(configuration)
    request_cancel(configuration)
    return {"stopping": [configuration]}


@frappe.whitelist(methods=["POST"])
def stop_all() -> dict[str, Any]:
    """Ask every queued or running sync to stop after its current batch."""
    _guard()
    stopping = []
    for run in _live_runs(_configs()):
        request_cancel(run["configuration"])
        stopping.append(run["configuration"])
    return {"stopping": stopping}


@frappe.whitelist(methods=["POST"])
def recount(configuration: str | None = None) -> dict[str, Any]:
    """Recompute source and warehouse record counts now (bypasses the cache)."""
    _guard()
    warehouse = _Warehouse()
    done = []
    for config in _configs():
        if configuration and config["name"] != configuration:
            continue
        if config["enabled"]:
            _counts_for(config, warehouse, force=True)
            done.append(config["name"])
    return {"recounted": done, "clickhouse_ok": warehouse.ok}
