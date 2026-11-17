# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Scheduler and background-job entry points for clickhouse_sync.

These functions are referenced from ``hooks.py`` (``scheduler_events``) and
from :func:`frappe.enqueue` calls. They are deliberately thin: all real work
lives in :mod:`clickhouse_sync.clickhouse.sync_engine`. Keeping the scheduler
surface small makes it easy to reason about *what* runs *when* without
re-reading the engine.
"""

from __future__ import annotations

import frappe
from frappe.utils import add_to_date, cint, get_datetime, now_datetime

from clickhouse_sync.clickhouse import live_status, sync_engine
from clickhouse_sync.clickhouse.sync_engine import propagate_delete  # re-exported

__all__ = [
    "run_configuration_sync",
    "run_scheduled_syncs",
    "run_hourly_syncs",
    "run_daily_syncs",
    "check_sync_staleness",
    "propagate_doc_delete",
]


def run_configuration_sync(
    config_name: str, trigger: str = "Manual", mode: str | None = None
) -> dict:
    """Background-job target: run one configuration end to end.

    Args:
        config_name: Name of the ClickHouse Sync Configuration.
        trigger: ``"Manual"`` or ``"Scheduler"`` - recorded on the log.
        mode: Optional one-off sync mode (``"Full"`` or ``"Incremental"``)
            chosen on the dashboard; ``None`` uses the configuration's mode.

    Returns:
        A compact result dict (status + row counts) for job introspection.
    """
    result = sync_engine.run_sync(config_name, trigger=trigger, mode=mode)
    return {
        "configuration": config_name,
        "status": result.status,
        "rows_written": result.rows_written,
        "rows_failed": result.rows_failed,
        "duration": result.duration,
        "log": result.log_name,
    }


def _enqueue_enabled_configs(trigger: str = "Scheduler") -> list[str]:
    """Enqueue a long-queue job for every scheduler-enabled configuration.

    Configurations run in ``priority`` order (ascending). Each is enqueued as
    a deduplicated job so overlapping scheduler ticks cannot double-run one.

    Returns:
        The list of configuration names that were enqueued.
    """
    configs = frappe.get_all(
        "ClickHouse Sync Configuration",
        filters={"enabled": 1, "scheduler_enabled": 1},
        fields=["name"],
        order_by="priority asc",
    )

    enqueued: list[str] = []
    for config in configs:
        job_id = f"clickhouse_sync::{config.name}"
        frappe.enqueue(
            "clickhouse_sync.tasks.run_configuration_sync",
            queue="long",
            timeout=21600,
            job_id=job_id,
            deduplicate=True,
            config_name=config.name,
            trigger=trigger,
        )
        live_status.mark_queued(config.name, None, trigger, None, job_id)
        enqueued.append(config.name)

    if enqueued:
        frappe.logger("clickhouse_sync").info(
            f"Scheduler enqueued {len(enqueued)} sync job(s): {', '.join(enqueued)}"
        )
    return enqueued


def run_scheduled_syncs() -> list[str]:
    """Cron-triggered entry point (see ``scheduler_events['cron']``)."""
    return _enqueue_enabled_configs(trigger="Scheduler")


def run_hourly_syncs() -> list[str]:
    """Hourly scheduler entry point."""
    return _enqueue_enabled_configs(trigger="Scheduler")


def run_daily_syncs() -> list[str]:
    """Daily scheduler entry point."""
    return _enqueue_enabled_configs(trigger="Scheduler")


def check_sync_staleness() -> list[str]:
    """Flag enabled configurations that have not synced recently.

    Reads the ``Staleness Alert Hours`` threshold from ClickHouse Settings
    (0 disables the check). For each enabled configuration, finds the most
    recent *completed* sync log; if none exists within the threshold, raises a
    notification to System Managers and records a warning in the log.

    Returns:
        The names of configurations flagged as stale.
    """
    threshold = cint(
        frappe.db.get_single_value("ClickHouse Settings", "staleness_alert_hours")
    )
    if threshold <= 0:
        return []

    cutoff = add_to_date(now_datetime(), hours=-threshold)
    stale: list[str] = []

    for config in frappe.get_all(
        "ClickHouse Sync Configuration",
        filters={"enabled": 1},
        fields=["name"],
    ):
        last_ok = frappe.db.get_value(
            "ClickHouse Sync Log",
            {"configuration": config.name, "status": "Completed"},
            "MAX(end_time)",
        )
        if last_ok is None or get_datetime(last_ok) < cutoff:
            stale.append(config.name)

    if stale:
        _raise_staleness_alert(stale, threshold)
    return stale


def _raise_staleness_alert(stale: list[str], threshold: int) -> None:
    """Notify System Managers about stale configurations."""
    names = ", ".join(stale)
    message = (
        f"ClickHouse Sync: {len(stale)} configuration(s) have not completed a "
        f"successful sync in over {threshold}h: {names}"
    )
    frappe.logger("clickhouse_sync").warning(message)

    try:
        managers = frappe.get_all(
            "Has Role",
            filters={"role": "System Manager", "parenttype": "User"},
            pluck="parent",
        )
        for user in set(managers):
            if user in ("Administrator", "Guest"):
                continue
            notification = frappe.new_doc("Notification Log")
            notification.subject = "ClickHouse Sync: stale configurations"
            notification.email_content = message
            notification.for_user = user
            notification.type = "Alert"
            notification.document_type = "ClickHouse Sync Configuration"
            notification.document_name = stale[0]
            notification.insert(ignore_permissions=True)
        frappe.db.commit()
    except Exception as exc:  # noqa: BLE001 - alerting must never break the scheduler
        frappe.logger("clickhouse_sync").warning(
            f"Failed to raise staleness notification: {exc}"
        )


def propagate_doc_delete(doc, method: str | None = None) -> None:  # noqa: ANN001
    """``on_trash`` doc-event handler: propagate a deletion to ClickHouse.

    Registered against ``*`` in ``hooks.py``; it filters to configured
    DocTypes inside :func:`propagate_delete`, so the overhead for
    non-configured DocTypes is a single cached lookup.
    """
    try:
        propagate_delete(doc.doctype, doc.name)
    except Exception as exc:  # noqa: BLE001 - never block a Frappe delete
        frappe.logger("clickhouse_sync").warning(
            f"on_trash propagation error for {doc.doctype}/{doc.name}: {exc}"
        )


def send_batch_digest() -> None:
    """Scheduler target: send the consolidated batch digest (Consolidated mode).

    Runs a few minutes after each sync tick so the batch's runs have landed in
    the log. Sends one message only if the window had failures; silent when
    healthy. Delegates to the alerts module, which never raises.
    """
    from clickhouse_sync.clickhouse.alerts import send_batch_digest as _digest

    _digest(window_minutes=20)


def send_daily_heartbeat() -> None:
    """Scheduler target: send the once-daily 'pipeline alive' summary."""
    from clickhouse_sync.clickhouse.alerts import send_daily_heartbeat as _hb

    _hb()
