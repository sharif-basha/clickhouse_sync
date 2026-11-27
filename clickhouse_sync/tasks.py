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

from datetime import timedelta

import frappe
from frappe.utils import add_to_date, cint, get_datetime, now_datetime

from clickhouse_sync.clickhouse import live_status, sync_engine
from clickhouse_sync.clickhouse.sync_engine import propagate_delete  # noqa: F401 - re-exported

__all__ = [
    "enqueue_config_sync",
    "run_configuration_sync",
    "run_scheduled_syncs",
    "run_hourly_syncs",
    "run_daily_syncs",
    "check_sync_staleness",
    "propagate_doc_delete",
    "flush_pending_deletes",
    "purge_old_sync_logs",
]

JOB_TIMEOUT_SECONDS = 21600  # 6 hours - large full syncs
VALID_TRIGGERS = ("Manual", "Scheduler")
DEFAULT_LOG_RETENTION_DAYS = 90


def job_id_for(config_name: str) -> str:
    return f"clickhouse_sync::{config_name}"


def _job_is_active(job_id: str) -> bool | None:
    """``True``/``False`` if the queue knows the job, ``None`` if unknown."""
    try:
        from frappe.utils.background_jobs import is_job_enqueued

        return bool(is_job_enqueued(job_id))
    except Exception:  # noqa: BLE001 - older Frappe or Redis queue unreachable
        return None


def enqueue_config_sync(
    config_name: str,
    trigger: str = "Manual",
    mode: str | None = None,
    doctype: str | None = None,
    job_active=None,  # noqa: ANN001 - injectable for tests
) -> dict:
    """Queue one configuration's sync job; the single place that does so.

    A job for the configuration that is already queued or running is left
    alone and reported as not queued. Previously the queue silently dropped
    the duplicate while the dashboard's live record was overwritten - a
    "Full" request then showed as queued while the earlier Incremental job
    ran, and the API answered ``queued: true``.

    Returns:
        ``{"queued": bool, "job_id": str, "reason": str | None}``
    """
    if trigger not in VALID_TRIGGERS:
        trigger = "Manual"
    job_id = job_id_for(config_name)
    active = (job_active or _job_is_active)(job_id)
    if active:
        return {"queued": False, "job_id": job_id, "reason": "already queued or running"}
    frappe.enqueue(
        "clickhouse_sync.tasks.run_configuration_sync",
        queue="long",
        timeout=JOB_TIMEOUT_SECONDS,
        job_id=job_id,
        deduplicate=True,
        config_name=config_name,
        trigger=trigger,
        mode=mode,
    )
    live_status.mark_queued(config_name, doctype, trigger, mode, job_id)
    return {"queued": True, "job_id": job_id, "reason": None}


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


SCHEDULER_TICK_MINUTES = 15
# A run that started slightly less than one interval ago still counts as due,
# so a 60-minute configuration is not pushed to every 75 minutes by jitter in
# the scheduler's timing.
SCHEDULE_SLACK_MINUTES = 2


def _last_started() -> dict[str, object]:
    """Start time of each configuration's most recent run, from the Sync Log."""
    rows = frappe.db.sql(
        """
        SELECT configuration, MAX(start_time) AS last_start
        FROM `tabClickHouse Sync Log`
        GROUP BY configuration
        """,
        as_dict=True,
    )
    return {r["configuration"]: r["last_start"] for r in rows}


def is_due(interval_minutes: int, last_start: object, now: object) -> bool:
    """Whether a configuration with ``interval_minutes`` should run now."""
    if interval_minutes <= SCHEDULER_TICK_MINUTES or not last_start:
        return True
    elapsed = (get_datetime(now) - get_datetime(last_start)).total_seconds() / 60
    return elapsed >= interval_minutes - SCHEDULE_SLACK_MINUTES


def next_run_at(interval_minutes: int, last_start: object, now: object):
    """When the scheduler will next queue a configuration (a datetime).

    The scheduler ticks every 15 minutes, at :00, :15, :30 and :45. A
    configuration runs at the first tick at which :func:`is_due` is true.
    """
    now = get_datetime(now)
    interval = max(cint(interval_minutes) or SCHEDULER_TICK_MINUTES, SCHEDULER_TICK_MINUTES)
    earliest = now
    if last_start and interval > SCHEDULER_TICK_MINUTES:
        due = get_datetime(last_start) + timedelta(minutes=interval - SCHEDULE_SLACK_MINUTES)
        earliest = max(now, due)
    tick = earliest.replace(second=0, microsecond=0)
    step = (-tick.minute) % SCHEDULER_TICK_MINUTES
    if step == 0 and tick < earliest:
        step = SCHEDULER_TICK_MINUTES
    return tick + timedelta(minutes=step)


def _enqueue_enabled_configs(trigger: str = "Scheduler") -> list[str]:
    """Enqueue a long-queue job for every scheduler-enabled configuration that is due.

    Configurations run in ``priority`` order (ascending). Each has its own
    ``Sync Interval (Minutes)``: the scheduler ticks every 15 minutes and
    skips a configuration whose last run started less than its interval ago,
    so a large Full-mode DocType can run hourly or daily. Each job is
    de-duplicated, so overlapping ticks cannot double-run one.

    Returns:
        The list of configuration names that were enqueued.
    """
    configs = frappe.get_all(
        "ClickHouse Sync Configuration",
        filters={"enabled": 1, "scheduler_enabled": 1},
        fields=["name", "sync_interval_minutes"],
        order_by="priority asc",
    )
    last_started = _last_started()
    now = now_datetime()

    enqueued: list[str] = []
    for config in configs:
        interval = cint(config.get("sync_interval_minutes")) or SCHEDULER_TICK_MINUTES
        if not is_due(interval, last_started.get(config.name), now):
            continue
        if enqueue_config_sync(config.name, trigger=trigger)["queued"]:
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
    """Kept for compatibility; no longer scheduled (the 15-minute cron covers it)."""
    return _enqueue_enabled_configs(trigger="Scheduler")


def run_daily_syncs() -> list[str]:
    """Kept for compatibility; no longer scheduled (the 15-minute cron covers it)."""
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
    """``after_delete`` doc-event handler: queue a deletion for ClickHouse.

    Registered against ``*`` in ``hooks.py``. ``after_delete`` runs once
    Frappe's link checks have passed, and the name is only queued when the
    transaction commits, so a refused or rolled-back delete never reaches
    ClickHouse. No ClickHouse call happens inside the user's request; for
    non-configured DocTypes the cost is one cached lookup.
    """
    try:
        sync_engine.queue_delete(doc.doctype, doc.name)
    except Exception as exc:  # noqa: BLE001 - never block a Frappe delete
        frappe.logger("clickhouse_sync").warning(
            f"delete propagation error for {doc.doctype}/{doc.name}: {exc}"
        )


def flush_pending_deletes() -> int:
    """Background job / cron target: apply queued deletes in batches."""
    return sync_engine.flush_pending_deletes()


def purge_old_sync_logs() -> int:
    """Daily: delete Sync Logs older than ``Log Retention Days`` (0 keeps all).

    Without this the log table grew by ~96 rows per configuration per day
    and every dashboard refresh aggregated all of it.
    """
    raw = frappe.db.get_single_value("ClickHouse Settings", "log_retention_days")
    days = DEFAULT_LOG_RETENTION_DAYS if raw is None else cint(raw)
    if days <= 0:
        return 0
    cutoff = add_to_date(now_datetime(), days=-days)
    count = frappe.db.count("ClickHouse Sync Log", {"creation": ("<", cutoff)})
    if count:
        frappe.db.delete("ClickHouse Sync Log", {"creation": ("<", cutoff)})
        frappe.db.commit()
        frappe.logger("clickhouse_sync").info(f"Purged {count} Sync Log(s) older than {days} days")
    return count


def send_batch_digest() -> None:
    """Scheduler target: send the consolidated batch digest (Consolidated mode).

    Runs a few minutes after each sync tick so the batch's runs have landed in
    the log. Sends one message only if the window had failures; silent when
    healthy. Delegates to the alerts module, which never raises.
    """
    from clickhouse_sync.clickhouse.alerts import send_batch_digest as _digest

    _digest()


def send_daily_heartbeat() -> None:
    """Scheduler target: send the once-daily 'pipeline alive' summary."""
    from clickhouse_sync.clickhouse.alerts import send_daily_heartbeat as _hb

    _hb()
