# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Live run state for the monitoring dashboard, kept in Redis.

The sync engine writes here while it works; the dashboard API reads it. Every
value the dashboard shows for a running sync therefore comes from the worker
that is doing the work, not from the browser.

Three records are kept per configuration:

* **live** - the run that is queued or in progress: phase, counters, batch
  number, cursor position and a heartbeat. Removed when the run ends.
* **last run** - a summary of the most recent finished run (status, error
  category, the batch it stopped in, retries). Kept for 30 days.
* **run detail** - the same summary keyed by Sync Log name, so the error list
  can show batch and retry details next to each failed log. Kept for 30 days.

Design rules:

* **Monitoring must never break a sync.** Every write is wrapped; a Redis
  problem is logged and ignored.
* **Reads bypass Frappe's per-request cache copy** (``expires=True`` plus
  dropping any local entry), so a value written by a worker is seen at once.
* **No database writes.** Nothing here needs a migration, and a busy sync does
  not add load to MariaDB. Durable history stays in ClickHouse Sync Log.

Redis is the Frappe *cache* instance, which is not persistent: after a Redis
restart the live and last-run records are gone until the next run, while the
Sync Logs remain. The dashboard falls back to log data in that case.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import frappe
from frappe.utils import get_datetime, now_datetime

LIVE_PREFIX = "clickhouse_sync:live:"
LAST_RUN_PREFIX = "clickhouse_sync:last_run:"
RUN_DETAIL_PREFIX = "clickhouse_sync:run_detail:"
COUNTS_PREFIX = "clickhouse_sync:counts:"

# A queued/running record outlives the longest allowed job (6 hours) slightly.
LIVE_TTL_SECONDS = 7 * 3600
HISTORY_TTL_SECONDS = 30 * 24 * 3600

# Write live progress at most this often (the engine also throttles).
WRITE_THROTTLE_SECONDS = 1.0

# A running sync with no heartbeat for this long is shown as "Stalled".
STALE_AFTER_SECONDS = 600
# Phases that are one long ClickHouse statement (no batch to report between)
# get a longer allowance before they count as stalled.
LONG_PHASE_PREFIXES = ("optimizing", "reconciling")
STALE_AFTER_LONG_PHASE_SECONDS = 3 * 3600

# Rate and ETA are only shown once this much work has been observed, so a
# single fast first batch does not produce a misleading estimate.
MIN_ELAPSED_FOR_RATE_SECONDS = 10.0
MIN_BATCHES_FOR_ETA = 2


# --------------------------------------------------------------------------- #
# Low-level helpers
# --------------------------------------------------------------------------- #
class _SilentLogger:
    def __getattr__(self, name: str) -> Any:
        return lambda *args, **kwargs: None


def _logger() -> Any:
    """Return the app logger, or a silent one if logging itself is unavailable."""
    try:
        return frappe.logger("clickhouse_sync")
    except Exception:  # noqa: BLE001 - monitoring must never break a sync
        return _SilentLogger()


def _forget_local(cache: Any, key: str) -> None:
    local_cache = getattr(getattr(frappe, "local", None), "cache", None)
    make_key = getattr(cache, "make_key", None)
    if isinstance(local_cache, dict) and make_key is not None:
        try:
            local_cache.pop(make_key(key), None)
        except Exception:  # noqa: BLE001
            pass


def read_fresh(key: str) -> Any:
    """Read a fresh value from Redis (never from the per-request copy).

    Returns ``None`` (and logs) when Redis cannot be read.
    """
    try:
        cache = frappe.cache()
        _forget_local(cache, key)
        try:
            return cache.get_value(key, expires=True)
        except TypeError:
            value = cache.get_value(key)
            _forget_local(cache, key)
            return value
    except Exception as exc:  # noqa: BLE001
        _logger().warning(f"live_status read failed for {key}: {exc}")
        return None


def _write(key: str, value: Any, ttl: int) -> None:
    try:
        cache = frappe.cache()
        cache.set_value(key, value, expires_in_sec=ttl)
        _forget_local(cache, key)
    except Exception as exc:  # noqa: BLE001 - monitoring must never break a sync
        _logger().warning(f"live_status write failed for {key}: {exc}")


def _delete(key: str) -> None:
    try:
        cache = frappe.cache()
        cache.delete_value(key)
        _forget_local(cache, key)
    except Exception as exc:  # noqa: BLE001
        _logger().warning(f"live_status delete failed for {key}: {exc}")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


# --------------------------------------------------------------------------- #
# Writes (called by the API and the engine)
# --------------------------------------------------------------------------- #
def mark_queued(config_name: str, doctype: str | None, trigger: str, mode: str | None, job_id: str) -> None:
    """Record that a job was enqueued and is waiting for a worker.

    An existing Running record is left alone: a duplicate enqueue of a running
    configuration is dropped by the queue's de-duplication.
    """
    current = read_fresh(LIVE_PREFIX + config_name)
    if current and current.get("status") == "Running":
        return
    # Re-queueing a job that is still waiting keeps its original queue time:
    # a stop pressed while it waited is honoured only if it came after
    # ``queued_at``, and a scheduler tick must not push that time forward.
    queued_at = (
        current.get("queued_at")
        if current and current.get("status") == "Queued" and current.get("queued_at")
        else _iso(now_datetime())
    )
    _write(
        LIVE_PREFIX + config_name,
        {
            "configuration": config_name,
            "doctype": doctype,
            "status": "Queued",
            "trigger": trigger,
            "mode": mode,
            "job_id": job_id,
            "queued_at": queued_at,
        },
        LIVE_TTL_SECONDS,
    )


class LiveRun:
    """Writer for one run's live record. One instance per ``run_sync`` call."""

    def __init__(self, config_name: str, doctype: str | None, trigger: str, mode: str | None, max_attempts: int) -> None:
        self.key = LIVE_PREFIX + config_name
        self._last_write = 0.0
        previous = read_fresh(self.key) or {}
        now = now_datetime()
        self.state: dict[str, Any] = {
            "configuration": config_name,
            "doctype": doctype,
            "status": "Running",
            "trigger": trigger,
            "mode": mode,
            "job_id": previous.get("job_id") or f"clickhouse_sync::{config_name}",
            "queued_at": previous.get("queued_at"),
            "started_at": _iso(now),
            "heartbeat": _iso(now),
            "phase": "starting",
            "attempt": 1,
            "max_attempts": max_attempts,
            "total": 0,
            "rows_read": 0,
            "rows_written": 0,
            "rows_failed": 0,
            "rows_new": None,
            "rows_updated": None,
            "rows_rechecked": None,
            "rows_skipped": 0,
            "child_rows_written": 0,
            "batch": 0,
            "cursor_modified": None,
            "cursor_name": None,
            "last_error": None,
        }
        self.flush()

    def update(self, force: bool = False, **fields: Any) -> None:
        """Merge ``fields`` into the record; write if forced or throttle elapsed."""
        try:
            for key, value in fields.items():
                self.state[key] = _iso(value) if isinstance(value, datetime) else value
            now = time.monotonic()
            if force or (now - self._last_write) >= WRITE_THROTTLE_SECONDS:
                self.flush()
        except Exception as exc:  # noqa: BLE001 - monitoring must never break a sync
            _logger().warning(f"live_status update failed: {exc}")

    def flush(self) -> None:
        try:
            self.state["heartbeat"] = _iso(now_datetime())
            self._last_write = time.monotonic()
            _write(self.key, dict(self.state), LIVE_TTL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            _logger().warning(f"live_status flush failed: {exc}")

    def finish(self, status: str, log_name: str | None, message: str, category: str | None) -> None:
        """Remove the live record and store the run summary."""
        try:
            self._finish(status, log_name, message, category)
        except Exception as exc:  # noqa: BLE001
            _logger().warning(f"live_status finish failed: {exc}")

    def _finish(self, status: str, log_name: str | None, message: str, category: str | None) -> None:
        summary = {
            "configuration": self.state["configuration"],
            "status": status,
            "log": log_name,
            "message": (message or "")[:500],
            "category": category,
            "mode": self.state.get("mode"),
            "trigger": self.state.get("trigger"),
            "started_at": self.state.get("started_at"),
            "finished_at": _iso(now_datetime()),
            "phase": self.state.get("phase"),
            "attempts": self.state.get("attempt"),
            "retries": max(0, int(self.state.get("attempt") or 1) - 1),
            "batch": self.state.get("batch"),
            "cursor_modified": self.state.get("cursor_modified"),
            "cursor_name": self.state.get("cursor_name"),
            "rows_read": self.state.get("rows_read"),
            "rows_written": self.state.get("rows_written"),
            "rows_failed": self.state.get("rows_failed"),
            "rows_new": self.state.get("rows_new"),
            "rows_updated": self.state.get("rows_updated"),
            "rows_rechecked": self.state.get("rows_rechecked"),
            "rows_skipped": self.state.get("rows_skipped"),
            "failed_batch_first": self.state.get("failed_batch_first"),
            "failed_batch_last": self.state.get("failed_batch_last"),
        }
        _write(LAST_RUN_PREFIX + summary["configuration"], summary, HISTORY_TTL_SECONDS)
        if log_name:
            _write(RUN_DETAIL_PREFIX + log_name, summary, HISTORY_TTL_SECONDS)
        _delete(self.key)


class NullLiveRun(LiveRun):
    """Stand-in used when the live record could not be created."""

    def __init__(self) -> None:  # noqa: D401 - intentionally does nothing
        self.state = {}

    def update(self, force: bool = False, **fields: Any) -> None:
        pass

    def flush(self) -> None:
        pass

    def finish(self, status: str, log_name: str | None, message: str, category: str | None) -> None:
        pass


def start_run(config_name: str, doctype: str | None, trigger: str, mode: str | None, max_attempts: int) -> LiveRun:
    """Create the live record for a run, or a no-op writer if that fails."""
    try:
        return LiveRun(config_name, doctype, trigger, mode, max_attempts)
    except Exception as exc:  # noqa: BLE001
        _logger().warning(f"live_status start failed for {config_name}: {exc}")
        return NullLiveRun()


def clear(config_name: str) -> None:
    """Remove a live record (used for queued jobs that no longer exist)."""
    _delete(LIVE_PREFIX + config_name)


# --------------------------------------------------------------------------- #
# Reads (called by the dashboard API)
# --------------------------------------------------------------------------- #
def get_live(config_name: str) -> dict[str, Any] | None:
    return read_fresh(LIVE_PREFIX + config_name)


def get_last_run(config_name: str) -> dict[str, Any] | None:
    return read_fresh(LAST_RUN_PREFIX + config_name)


def get_run_detail(log_name: str) -> dict[str, Any] | None:
    return read_fresh(RUN_DETAIL_PREFIX + log_name)


def get_counts(config_name: str) -> dict[str, Any] | None:
    return read_fresh(COUNTS_PREFIX + config_name)


def set_counts(config_name: str, counts: dict[str, Any], ttl: int) -> None:
    _write(COUNTS_PREFIX + config_name, counts, ttl)


def derive_metrics(state: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Add elapsed time, speed, progress, ETA and stall flag to a live record.

    All values are computed from what the worker reported; nothing is guessed
    in the browser. ``eta_seconds`` is ``None`` until enough work has been seen
    to make an estimate meaningful.
    """
    out = dict(state)
    now = now or now_datetime()
    started = get_datetime(state.get("started_at")) if state.get("started_at") else None
    heartbeat = get_datetime(state.get("heartbeat")) if state.get("heartbeat") else None

    elapsed = (now - started).total_seconds() if started else 0.0
    out["elapsed_seconds"] = round(max(elapsed, 0.0), 1)
    out["heartbeat_age_seconds"] = round((now - heartbeat).total_seconds(), 1) if heartbeat else None
    phase = str(state.get("phase") or "")
    allowance = (
        STALE_AFTER_LONG_PHASE_SECONDS
        if phase.startswith(LONG_PHASE_PREFIXES)
        else STALE_AFTER_SECONDS
    )
    out["stalled"] = bool(
        state.get("status") == "Running"
        and out["heartbeat_age_seconds"] is not None
        and out["heartbeat_age_seconds"] > allowance
    )

    read = int(state.get("rows_read") or 0)
    total = int(state.get("total") or 0)
    # Speed is measured over the time the worker has reported, so a stalled
    # run does not show a falling rate that looks like progress.
    measured = (heartbeat - started).total_seconds() if (started and heartbeat) else 0.0
    rate = read / measured if measured >= MIN_ELAPSED_FOR_RATE_SECONDS and read else None
    out["rows_per_second"] = round(rate, 1) if rate else None

    if total > 0:
        out["percent"] = round(min(read / total, 1.0) * 100, 1)
    else:
        out["percent"] = None

    eta = None
    if (
        rate
        and total > read
        and int(state.get("batch") or 0) >= MIN_BATCHES_FOR_ETA
        and not out["stalled"]
    ):
        eta = (total - read) / rate
    out["eta_seconds"] = round(eta) if eta is not None else None
    return out
