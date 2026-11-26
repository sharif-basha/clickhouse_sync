"""Sync service layer: orchestrates DocType -> ClickHouse synchronisation.

This is the heart of the app. A single public entry point,
:func:`run_sync`, drives the whole pipeline for one configuration:

    settings/config -> connect -> ensure table -> reconcile schema
        -> sync child tables (before the parent cursor can move)
        -> stream rows from MariaDB (keyset paginated)
        -> coerce + batch insert into ClickHouse
        -> reconcile deletes
        -> update cursor + write a ClickHouse Sync Log

Design notes
------------
* **Memory efficient**: rows are read from MariaDB in keyset-paginated
  windows and inserted in column-oriented batches, so peak memory is bounded
  by ``batch_size`` regardless of table size (millions of rows are fine).
* **Resumable**: incremental sync reads rows in ``(modified, name)`` order and
  persists the ``modified`` position after each successful batch. Because the
  read order and the cursor are the same quantity, everything before the
  cursor is known to be delivered, so an interrupted run resumes from the last
  successful batch without skipping rows.
* **No lost edits**: each run only reads rows modified up to its start time
  (the *watermark*); the next run starts a safety lag before that, so edits
  made while a run was in progress - or committed slightly late - are picked
  up by the following run. Re-read rows are absorbed by ReplacingMergeTree.
* **Cancelable**: a cache flag is polled between batches; setting it makes the
  loop exit cleanly and mark the log ``Cancelled``.
* **Retryable**: transient failures are retried with exponential backoff; the
  retry count/interval come from the configuration.
* **Observable**: progress is published over Frappe realtime and written to
  a live record in Redis (:mod:`live_status`) that the monitoring dashboard
  reads; every run is recorded in a ClickHouse Sync Log document.
"""

from __future__ import annotations

import json
import random
import socket
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterator

import frappe
from frappe.utils import cint, get_datetime, now_datetime

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse import live_status
from clickhouse_sync.clickhouse.exceptions import (
    ClickHouseSyncError,
    SyncCancelledError,
    categorize_error,
    is_transient_error,
)
from clickhouse_sync.clickhouse.schema import (
    SOFT_DELETE_COLUMN,
    SYNC_TIMESTAMP_COLUMN,
    TableSchema,
    build_table_schema,
    ensure_table,
    get_child_doctypes,
    insert_columns,
    is_creation_partitioned,
    reconcile_schema,
    table_name_for,
)
from clickhouse_sync.clickhouse.type_mapping import (
    attach_timezone,
    coerce_value,
    column_timezone,
    empty_value_for,
)

if TYPE_CHECKING:  # pragma: no cover
    from clickhouse_connect.driver.client import Client


CANCEL_CACHE_PREFIX = "clickhouse_sync::cancel::"
PROGRESS_EVENT = "clickhouse_sync_progress"
DEFAULT_BATCH_SIZE = 25000
PROGRESS_THROTTLE_SECONDS = 1.0
VALID_SYNC_MODES = ("Full", "Incremental")

# Default for how long *before* the saved cursor incremental runs start (each
# configuration can set its own, see ``Cursor Safety Lag``). Frappe stamps
# ``modified`` when a document is saved, not when its transaction commits, so a
# row can become visible slightly after a later-stamped row was already read.
# Re-reading a short overlap closes that gap; duplicates are harmless because
# the target table is a ReplacingMergeTree keyed on ``name``.
CURSOR_SAFETY_LAG_SECONDS = 300
MAX_CURSOR_SAFETY_LAG_SECONDS = 7 * 24 * 3600

# Rows stamped further in the future than this (relative to the run watermark)
# are reported as unreachable by incremental sync until that time arrives.
FUTURE_DATED_GRACE_SECONDS = 3600


# --------------------------------------------------------------------------- #
# Value objects
# --------------------------------------------------------------------------- #
@dataclass
class SyncContext:
    """Resolved, validated parameters for a single sync run."""

    config_name: str
    doctype: str
    table_name: str
    sync_mode: str  # "Full" | "Incremental"
    batch_size: int
    auto_create: bool
    auto_update: bool
    delete_strategy: str  # "Soft Delete" | "Hard Delete" | "Ignore"
    retry_count: int
    retry_interval: int
    trigger: str  # "Scheduler" | "Manual"
    last_sync_timestamp: datetime | None
    exclude_fields: frozenset[str] = field(default_factory=frozenset)
    optimize_after_sync: bool = True
    # Default ON: tables are a single partition, so OPTIMIZE FINAL rewrites
    # the whole table - doing that on every 15-minute incremental tick is far
    # too expensive. Query with FINAL between full syncs instead.
    optimize_only_on_full: bool = True
    reconcile_deletes_incremental: bool = False
    sync_child_tables: bool = False
    # The cursor as it was when the run started. ``last_sync_timestamp`` moves
    # forward batch by batch; this one never changes during a run. Used to
    # tell new records (created after the previous sync) from updated ones.
    previous_cursor: datetime | None = None
    # ``True`` when this run's mode was chosen on the dashboard and differs
    # from the configuration's saved mode (e.g. a one-off full resync).
    mode_overridden: bool = False
    # Live progress writer for the dashboard (never ``None`` during a run).
    live: Any = None
    # How far before the saved cursor an incremental run starts reading.
    safety_lag_seconds: int = 300
    # Partition new tables by creation month (see schema.build_table_schema).
    partition_by_creation: bool = False
    # Values that could not be converted to their column type, per column.
    # They are written as NULL; the counts are reported on the run.
    unconverted: dict[str, int] = field(default_factory=dict)
    schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        self.schema = build_table_schema(
            self.doctype,
            include_soft_delete=(self.delete_strategy == "Soft Delete"),
            exclude_fields=set(self.exclude_fields),
            partition_by_creation=self.partition_by_creation,
        )


@dataclass
class SyncResult:
    """Outcome of a sync run, mirrored into a ClickHouse Sync Log."""

    status: str = "Queued"
    rows_read: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    message: str = ""
    exception: str = ""
    start_time: datetime | None = None
    end_time: datetime | None = None
    log_name: str | None = None

    @property
    def duration(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


# --------------------------------------------------------------------------- #
# Cancellation helpers
# --------------------------------------------------------------------------- #
def _cancel_key(config_name: str) -> str:
    return f"{CANCEL_CACHE_PREFIX}{config_name}"


def request_cancel(config_name: str) -> None:
    """Signal a sync for ``config_name`` to stop at the next batch.

    The flag holds the time it was set, so a sync that is still queued can tell
    a stop pressed while it waited (honoured) from a stale flag left by an
    earlier run (ignored).
    """
    frappe.cache().set_value(
        _cancel_key(config_name), now_datetime().isoformat(sep=" "), expires_in_sec=3600
    )


def _cancel_requested_at(config_name: str) -> datetime | None:
    """When a stop was requested for ``config_name``, if one is pending."""
    value = live_status.read_fresh(_cancel_key(config_name))
    if not value:
        return None
    try:
        return get_datetime(value) if value != "1" else None
    except Exception:  # noqa: BLE001 - unparseable legacy value
        return None


def _clear_cancel(config_name: str) -> None:
    try:
        frappe.cache().delete_value(_cancel_key(config_name))
    except Exception as exc:  # noqa: BLE001 - a Redis outage must not fail the sync
        frappe.logger("clickhouse_sync").warning(f"could not clear cancel flag for {config_name}: {exc}")


def _is_cancelled(config_name: str) -> bool:
    """Return ``True`` if a cancel has been requested, reading Redis directly.

    Frappe keeps a per-job copy of cache reads in ``frappe.local.cache``, so a
    plain ``get_value`` would keep returning the first value seen and a long
    sync would never notice the Cancel button. :func:`live_status.read_fresh`
    bypasses that copy. A Redis outage reads as "not cancelled".
    """
    return bool(live_status.read_fresh(_cancel_key(config_name)))


# --------------------------------------------------------------------------- #
# Progress reporting
# --------------------------------------------------------------------------- #
class ProgressReporter:
    """Throttled publisher of realtime progress events for the Desk UI."""

    def __init__(self, config_name: str, total: int, live: Any = None) -> None:
        self.config_name = config_name
        self.total = max(total, 0)
        self._last_emit = 0.0
        self.live = live
        if live is not None:
            live.update(force=True, total=self.total)

    def emit(self, read: int, written: int, phase: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_emit) < PROGRESS_THROTTLE_SECONDS:
            return
        self._last_emit = now
        percent = round((read / self.total) * 100, 1) if self.total else 0.0
        if self.live is not None:
            self.live.update(force=True, phase=phase, rows_read=read, rows_written=written)
        frappe.publish_realtime(
            PROGRESS_EVENT,
            {
                "configuration": self.config_name,
                "phase": phase,
                "rows_read": read,
                "rows_written": written,
                "total": self.total,
                "percent": percent,
            },
            user=frappe.session.user,
        )


# --------------------------------------------------------------------------- #
# Context construction
# --------------------------------------------------------------------------- #
def build_context(
    config_name: str, trigger: str = "Manual", mode: str | None = None
) -> SyncContext:
    """Load a ClickHouse Sync Configuration and build a :class:`SyncContext`.

    Args:
        config_name: The configuration to load.
        trigger: ``"Manual"`` or ``"Scheduler"``.
        mode: Optional one-off override of the configuration's sync mode
            (``"Full"`` or ``"Incremental"``). The saved configuration is not
            changed.
    """
    config = frappe.get_doc("ClickHouse Sync Configuration", config_name)

    if not config.enabled:
        raise ClickHouseSyncError(f"Configuration '{config_name}' is disabled.")
    if mode is not None and mode not in VALID_SYNC_MODES:
        raise ClickHouseSyncError(f"Unknown sync mode '{mode}'.")

    saved_mode = config.sync_mode or "Full"
    run_mode = mode or saved_mode
    last_ts = get_datetime(config.last_sync_timestamp) if config.last_sync_timestamp else None

    return SyncContext(
        config_name=config.name,
        doctype=config.source_doctype,
        table_name=table_name_for(config.source_doctype),
        sync_mode=run_mode,
        batch_size=_clamp_batch_size(cint(config.batch_size)),
        auto_create=bool(config.auto_create_table),
        auto_update=bool(config.auto_update_schema),
        delete_strategy=config.delete_strategy or "Ignore",
        retry_count=cint(config.retry_count) or 3,
        retry_interval=cint(config.retry_interval) or 5,
        trigger=trigger,
        last_sync_timestamp=last_ts,
        exclude_fields=parse_excluded_fields(config.get("excluded_fields")),
        optimize_after_sync=_coerce_flag(config.get("optimize_after_sync"), default=True),
        optimize_only_on_full=_coerce_flag(config.get("optimize_only_on_full"), default=True),
        reconcile_deletes_incremental=_coerce_flag(
            config.get("reconcile_deletes_incremental"), default=False
        ),
        sync_child_tables=_coerce_flag(config.get("sync_child_tables"), default=False),
        previous_cursor=last_ts,
        mode_overridden=run_mode != saved_mode,
        safety_lag_seconds=_safety_lag(config.get("cursor_safety_lag_seconds")),
        partition_by_creation=_coerce_flag(config.get("partition_by_creation"), default=False),
    )


def _safety_lag(value: object) -> int:
    """The configured overlap in seconds; unset means the default.

    Long-running transactions (bulk imports, slow submits) can commit rows
    stamped earlier than the default window covers; such sites raise it.
    """
    if value is None or value == "":
        return CURSOR_SAFETY_LAG_SECONDS
    return max(0, min(cint(value), MAX_CURSOR_SAFETY_LAG_SECONDS))


def _coerce_flag(value: object, *, default: bool) -> bool:
    """Interpret a Frappe check value, treating unset (None) as ``default``.

    New boolean fields are None on documents created before the field existed;
    for the optimize flag we want those to behave as enabled.
    """
    if value is None:
        return default
    return bool(cint(value))


def parse_excluded_fields(raw: str | None) -> frozenset[str]:
    """Parse the configuration's excluded-fields text into a set of names.

    Accepts a comma- and/or newline-separated list of field names, tolerating
    blank lines and surrounding whitespace.
    """
    if not raw:
        return frozenset()
    tokens = raw.replace(",", "\n").splitlines()
    return frozenset(t.strip() for t in tokens if t.strip())


def _clamp_batch_size(value: int) -> int:
    """Keep the batch size within sane bounds (1k .. 100k)."""
    if value <= 0:
        return DEFAULT_BATCH_SIZE
    return max(1000, min(value, 100000))


# --------------------------------------------------------------------------- #
# MariaDB streaming reader (keyset pagination)
# --------------------------------------------------------------------------- #
def incremental_lower_bound(ctx: SyncContext) -> datetime | None:
    """Return the ``modified`` value an incremental run starts reading from.

    ``None`` means "no lower bound" (Full mode, or the very first incremental
    run). Otherwise it is the saved cursor minus the configuration's safety
    lag (default :data:`CURSOR_SAFETY_LAG_SECONDS`).
    """
    if ctx.sync_mode != "Incremental" or ctx.last_sync_timestamp is None:
        return None
    return ctx.last_sync_timestamp - timedelta(seconds=ctx.safety_lag_seconds)


def _select_fields(ctx: SyncContext, fields: list[str]) -> list[str]:
    """Source columns to read: requested ones that exist, plus name/modified."""
    # Only request source columns that actually exist on the DocType table to
    # avoid SQL errors on optional framework columns.
    available = set(frappe.get_meta(ctx.doctype).get_valid_columns())
    select_fields = [f for f in fields if f in available] or ["name"]
    if "name" not in select_fields:
        select_fields.insert(0, "name")
    if "modified" not in select_fields:
        select_fields.append("modified")
    return select_fields


def stream_source_rows(
    ctx: SyncContext,
    fields: list[str],
    upper: datetime | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield batches of source rows using keyset (seek) pagination.

    Keyset pagination avoids the O(n^2) cost of ``LIMIT/OFFSET`` on large
    tables. Two orderings are used:

    * **Full** mode pages on ``name`` (the primary key) and reads every row.
      The cursor is only written after the whole run succeeds, so the read
      order does not have to match it.
    * **Incremental** mode pages on ``(modified, name)`` and only reads rows
      with ``modified <= upper``. The last row of each batch is therefore a
      valid resume point: every row ordered before it has been yielded.

    Args:
        ctx: The run context.
        fields: Columns wanted, in ClickHouse insert order.
        upper: The run watermark (incremental only). Defaults to "now".

    Yields:
        Lists of row dicts, each no larger than ``ctx.batch_size``.
    """
    select_fields = _select_fields(ctx, fields)
    yield from keyset_batches(
        ctx,
        f"`tab{ctx.doctype}`",
        select_fields,
        upper=upper or now_datetime(),
    )


def keyset_batches(
    ctx: SyncContext,
    table_sql: str,
    select_fields: list[str],
    *,
    upper: datetime,
    conditions: list[str] | None = None,
    params: dict[str, Any] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Read ``table_sql`` in keyset-paginated batches - parent and child alike.

    Full mode pages on ``name``. Incremental mode pages on
    ``(modified, name)`` up to ``upper``: the first window starts at
    :func:`incremental_lower_bound` (inclusive, so rows sharing the boundary
    timestamp are never skipped) and every later window continues strictly
    after the last row yielded, using ``name`` to break ties between rows
    with an identical ``modified`` - correct even when more rows than one
    batch share a timestamp (bulk imports). The ordering rides the
    ``modified`` index, which InnoDB extends with the primary key.

    ``conditions``/``params`` add fixed filters (e.g. a child table's
    ``parenttype``). ``select_fields`` must include ``name`` and, for
    incremental mode, ``modified``.
    """
    col_sql = ", ".join(f"`{c}`" for c in select_fields)
    incremental = ctx.sync_mode == "Incremental"
    lower = incremental_lower_bound(ctx)
    position: tuple[datetime | None, Any] | None = None  # (modified, name) of the last row

    while True:
        where = list(conditions or [])
        values: dict[str, Any] = dict(params or {})
        values["limit"] = ctx.batch_size
        if incremental:
            where.append("modified <= %(upper)s")
            values["upper"] = upper
            if position is not None:
                where.append(
                    "(modified > %(pos_modified)s "
                    "OR (modified = %(pos_modified)s AND name > %(pos_name)s))"
                )
                values["pos_modified"], values["pos_name"] = position
            elif lower is not None:
                where.append("modified >= %(lower)s")
                values["lower"] = lower
            order_by = "modified ASC, name ASC"
        else:
            where.append("name > %(last_name)s")
            values["last_name"] = position[1] if position is not None else ""
            order_by = "name ASC"

        rows: list[dict[str, Any]] = frappe.db.sql(
            f"SELECT {col_sql} FROM {table_sql} "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY {order_by} LIMIT %(limit)s",
            values,
            as_dict=True,
        )
        if not rows:
            break

        yield rows
        # ``modified`` cannot be NULL in incremental mode: the
        # ``modified <= upper`` predicate already excludes NULLs.
        position = (
            get_datetime(rows[-1]["modified"]) if incremental else None,
            rows[-1]["name"],
        )
        if len(rows) < ctx.batch_size:
            break


def count_source_rows(ctx: SyncContext, upper: datetime | None = None) -> int:
    """Return the number of source rows a run will process (for progress)."""
    filters: dict[str, Any] = {}
    if ctx.sync_mode == "Incremental":
        lower = incremental_lower_bound(ctx)
        upper = upper or now_datetime()
        if lower is not None:
            filters["modified"] = ("between", [lower, upper])
        else:
            filters["modified"] = ("<=", upper)
    try:
        return frappe.db.count(ctx.doctype, filters=filters)
    except Exception:  # noqa: BLE001 - counting must never break a sync
        return 0


def count_unreachable_rows(ctx: SyncContext, upper: datetime) -> int:
    """Count rows an incremental run can never reach through ``modified``.

    Rows whose ``modified`` is NULL, or stamped well into the future, fall
    outside every ``lower <= modified <= upper`` window. They are rare (direct
    SQL inserts, clock problems) but must be reported rather than silently
    left behind. Returns 0 if the check itself fails - it must not break a run.
    """
    horizon = upper + timedelta(seconds=FUTURE_DATED_GRACE_SECONDS)
    try:
        result = frappe.db.sql(
            f"SELECT COUNT(*) FROM `tab{ctx.doctype}` "
            f"WHERE modified IS NULL OR modified > %(horizon)s",
            {"horizon": horizon},
        )
        return cint(result[0][0]) if result else 0
    except Exception:  # noqa: BLE001
        return 0


# --------------------------------------------------------------------------- #
# Batch insert
# --------------------------------------------------------------------------- #
def _prepare_batch(
    ctx: SyncContext,
    rows: list[dict[str, Any]],
    column_defs: list[Any],
    synced_at: datetime,
) -> tuple[list[str], list[list[Any]]]:
    """Coerce a batch of source rows into column-oriented insert data.

    Returns:
        ``(column_names, data_matrix)`` where ``data_matrix`` is a list of
        row lists aligned to ``column_names`` - exactly the shape
        ``clickhouse-connect``'s :meth:`Client.insert` expects.
    """
    columns = [c.name for c in column_defs]
    # Per column: (fieldtype used for coercion, value to use for NULL). The
    # NULL substitute is only set for non-nullable columns, i.e. the storage
    # key and columns created non-nullable by older versions.
    # A column with an explicit time zone gets aware datetimes: the driver
    # converts naive ones with the WORKER's OS time zone, which need not be
    # Frappe's system time zone.
    plan = [
        (
            c.name,
            c.frappe_fieldtype,
            None if c.nullable else empty_value_for(c.clickhouse_type),
            column_timezone(c.clickhouse_type),
        )
        for c in column_defs
    ]
    unconverted = ctx.unconverted

    data: list[list[Any]] = []
    for row in rows:
        record: list[Any] = []
        for col, fieldtype, empty, tz in plan:
            if col == SYNC_TIMESTAMP_COLUMN:
                record.append(attach_timezone(synced_at, tz) if tz else synced_at)
                continue
            if col == SOFT_DELETE_COLUMN:
                record.append(0)
                continue
            raw = row.get(col)
            value = coerce_value(fieldtype or "Data", raw)
            if value is None and raw not in (None, ""):
                # Silently NULLing bad values hid data problems: count them.
                unconverted[col] = unconverted.get(col, 0) + 1
            if value is None and empty is not None:
                value = empty
            elif tz and value is not None:
                value = attach_timezone(value, tz)
            record.append(value)
        data.append(record)
    return columns, data


def _creation_months(rows: list[dict[str, Any]]) -> set[str]:
    """``YYYYMM`` partition ids of the rows' ``creation`` (for OPTIMIZE)."""
    months: set[str] = set()
    for row in rows:
        created = row.get("creation")
        if created:
            created = get_datetime(created)
            months.add(f"{created.year:04d}{created.month:02d}")
        else:
            months.add("197001")  # NULL creation lands in the epoch partition
    return months


def _unconverted_summary(ctx: SyncContext) -> str:
    if not ctx.unconverted:
        return ""
    total = sum(ctx.unconverted.values())
    detail = ", ".join(f"{col}: {n}" for col, n in sorted(ctx.unconverted.items()))
    return f" WARNING: {total} value(s) could not be converted and were written as NULL ({detail})."


def _insert_batch(
    client: "Client",
    database: str,
    table_name: str,
    columns: list[str],
    data: list[list[Any]],
) -> None:
    """Insert one prepared batch, letting driver-level errors propagate."""
    client.insert(
        table=table_name,
        data=data,
        column_names=columns,
        database=database,
    )


# --------------------------------------------------------------------------- #
# Delete reconciliation
# --------------------------------------------------------------------------- #
def reconcile_deletes(client: "Client", ctx: SyncContext, database: str) -> int:
    """Apply the configured delete strategy for rows removed from the source.

    Compares names in bounded ClickHouse pages against the source and:

    * **Soft Delete** - marks vanished rows with ``_ch_is_deleted = 1``.
    * **Hard Delete** - issues a lightweight ``DELETE`` for vanished rows.
    * **Ignore** - does nothing.

    Runs on full syncs, and on incremental syncs when the configuration has
    ``reconcile_deletes_incremental`` enabled. This is the reliable safety net
    for deletions that the real-time ``on_trash`` hook can miss (rows deleted
    while a sync was down, bulk/direct-DB deletes). It performs a full
    full name comparison, so it is opt-in for incremental to avoid that scan
    cost on every scheduled tick.

    Returns:
        The number of rows affected by the delete reconciliation.
    """
    if ctx.delete_strategy == "Ignore":
        return 0
    if ctx.sync_mode != "Full" and not ctx.reconcile_deletes_incremental:
        return 0

    return _reconcile_missing_rows(
        client,
        ctx.doctype,
        ctx.table_name,
        database,
        ctx.delete_strategy,
    )


# Names compared per round trip. Larger pages mean fewer round trips and fewer
# delete statements; 5,000 keeps the MariaDB IN-list comfortably small.
DELETE_RECONCILE_BATCH_SIZE = 5000


def _reconcile_missing_rows(
    client: "Client",
    source_doctype: str,
    table_name: str,
    database: str,
    strategy: str,
    *,
    source_parenttype: str | None = None,
) -> int:
    """Reconcile deleted or reparented rows with bounded, parameterized batches.

    Rows already flagged deleted (Soft Delete) are skipped: re-flagging them on
    every run queued one ClickHouse mutation per page for every historical
    delete, forever.
    """
    affected = 0
    last_name = ""

    while True:
        clickhouse_conditions = ["`name` > {last_name:String}"]
        if strategy == "Soft Delete":
            clickhouse_conditions.append(f"`{SOFT_DELETE_COLUMN}` = 0")
        query_parameters: dict[str, Any] = {
            "last_name": last_name,
            "limit": DELETE_RECONCILE_BATCH_SIZE,
        }
        if source_parenttype is not None:
            clickhouse_conditions.append("`parenttype` = {source_parenttype:String}")
            query_parameters["source_parenttype"] = source_parenttype
        result = client.query(
            f"SELECT DISTINCT `name` FROM `{database}`.`{table_name}` "
            f"WHERE {' AND '.join(clickhouse_conditions)} "
            "ORDER BY `name` ASC LIMIT {limit:UInt32}",
            parameters=query_parameters,
        )
        names = [row[0] for row in result.result_rows]
        if not names:
            break
        last_name = names[-1]

        placeholders = ", ".join(f"%(name_{index})s" for index in range(len(names)))
        source_conditions = [f"name IN ({placeholders})"]
        params: dict[str, Any] = {
            f"name_{index}": name for index, name in enumerate(names)
        }
        if source_parenttype is not None:
            source_conditions.append("parenttype = %(parenttype)s")
            params["parenttype"] = source_parenttype
        rows = frappe.db.sql(
            f"SELECT name FROM `tab{source_doctype}` "
            f"WHERE {' AND '.join(source_conditions)}",
            params,
            as_dict=True,
        )
        live_names = {row["name"] for row in rows}
        vanished = [name for name in names if name not in live_names]
        if vanished:
            _apply_delete(
                client, database, table_name, strategy, vanished,
                source_parenttype=source_parenttype,
            )
            affected += len(vanished)

        if len(names) < DELETE_RECONCILE_BATCH_SIZE:
            break

    return affected


def _apply_delete(
    client: "Client",
    database: str,
    table_name: str,
    strategy: str,
    names: list[str],
    *,
    source_parenttype: str | None = None,
) -> None:
    """Soft- or hard-delete ``names`` with ONE statement.

    *Soft Delete* inserts a new version of each live row with the flag set
    (``INSERT ... SELECT ... FINAL``), one millisecond newer than the row it
    replaces, so ReplacingMergeTree keeps the flagged copy. Unlike the former
    ``ALTER TABLE ... UPDATE`` this is an ordinary insert - no mutation that
    rewrites data parts, no "too many mutations" limit, and the result is
    visible at once. *Hard Delete* uses a lightweight ``DELETE``.
    """
    if not names or strategy not in ("Soft Delete", "Hard Delete"):
        return
    conditions = []
    parameters: dict[str, Any] = {"names": list(names)}
    if source_parenttype is not None:
        conditions.append("`parenttype` = {source_parenttype:String}")
        parameters["source_parenttype"] = source_parenttype
    conditions.append("`name` IN {names:Array(String)}")
    if strategy == "Soft Delete":
        conditions.append(f"`{SOFT_DELETE_COLUMN}` = 0")
        client.command(
            f"INSERT INTO `{database}`.`{table_name}` "
            f"SELECT * REPLACE ("
            f"toUInt8(1) AS `{SOFT_DELETE_COLUMN}`, "
            f"`{SYNC_TIMESTAMP_COLUMN}` + toIntervalMillisecond(1) AS `{SYNC_TIMESTAMP_COLUMN}`"
            f") FROM `{database}`.`{table_name}` FINAL "
            f"WHERE {' AND '.join(conditions)}",
            parameters=parameters,
        )
    else:
        client.command(
            f"DELETE FROM `{database}`.`{table_name}` "
            f"WHERE {' AND '.join(conditions)}",
            parameters=parameters,
        )


def _reconcile_children_of_parents(
    client: "Client",
    child_doctype: str,
    table_name: str,
    database: str,
    strategy: str,
    parenttype: str,
    parents: list[str],
) -> int:
    """Delete-check only the child rows of ``parents`` (incremental runs).

    Removing a grid row, or moving it to another document, always saves the
    parent document it came from, so the parents changed in this run's window
    are exactly the ones whose child rows can have vanished. Comparing just
    those keeps the cost proportional to the changes instead of scanning the
    whole child table on every tick.
    """
    affected = 0
    unique = sorted(set(p for p in parents if p))
    for start in range(0, len(unique), DELETE_RECONCILE_BATCH_SIZE):
        chunk = unique[start : start + DELETE_RECONCILE_BATCH_SIZE]
        conditions = [
            "`parenttype` = {source_parenttype:String}",
            "`parent` IN {parents:Array(String)}",
        ]
        if strategy == "Soft Delete":
            conditions.append(f"`{SOFT_DELETE_COLUMN}` = 0")
        result = client.query(
            f"SELECT DISTINCT `name` FROM `{database}`.`{table_name}` "
            f"WHERE {' AND '.join(conditions)}",
            parameters={"source_parenttype": parenttype, "parents": chunk},
        )
        warehouse_names = [row[0] for row in result.result_rows]
        if not warehouse_names:
            continue
        # Look the rows up by NAME (not by parent): an unmerged older copy
        # of a row that moved to another document of the same type still
        # matches ``parent IN (...)`` in ClickHouse, and must not be deleted.
        for w_start in range(0, len(warehouse_names), DELETE_RECONCILE_BATCH_SIZE):
            names = warehouse_names[w_start : w_start + DELETE_RECONCILE_BATCH_SIZE]
            placeholders = ", ".join(f"%(name_{i})s" for i in range(len(names)))
            params: dict[str, Any] = {f"name_{i}": name for i, name in enumerate(names)}
            params["parenttype"] = parenttype
            rows = frappe.db.sql(
                f"SELECT name FROM `tab{child_doctype}` "
                f"WHERE parenttype = %(parenttype)s AND name IN ({placeholders})",
                params,
                as_dict=True,
            )
            live_names = {row["name"] for row in rows}
            vanished = [name for name in names if name not in live_names]
            _apply_delete(
                client, database, table_name, strategy, vanished,
                source_parenttype=parenttype,
            )
            affected += len(vanished)
    return affected


def _parents_changed_in_window(ctx: SyncContext, upper: datetime) -> list[str]:
    """Names of parent documents modified inside this incremental window."""
    lower = incremental_lower_bound(ctx)
    params: dict[str, Any] = {"upper": upper}
    condition = "modified <= %(upper)s"
    if lower is not None:
        condition += " AND modified >= %(lower)s"
        params["lower"] = lower
    rows = frappe.db.sql(
        f"SELECT name FROM `tab{ctx.doctype}` WHERE {condition}",
        params,
        as_dict=True,
    )
    return [row["name"] for row in rows]


# --------------------------------------------------------------------------- #
# Core run (single attempt)
# --------------------------------------------------------------------------- #
def _run_once(ctx: SyncContext, result: SyncResult) -> None:
    """Execute exactly one sync attempt, mutating ``result`` in place.

    Raises on failure so the retry wrapper can decide whether to retry.
    """
    live = ctx.live or live_status.NullLiveRun()
    ctx.unconverted.clear()  # counted per attempt, child tables included
    live.update(force=True, phase="connecting")
    client = ch_connection.get_client()
    params = ch_connection.get_connection_params()
    database = params.database

    # 1. Table + schema reconciliation.
    live.update(force=True, phase="checking schema")
    created = ensure_table(client, ctx.schema, database, auto_create=ctx.auto_create)
    reconcile_schema(client, ctx.schema, database, auto_update=ctx.auto_update)
    partition_note = ""
    if ctx.partition_by_creation and not created and not is_creation_partitioned(
        client, database, ctx.table_name
    ):
        # The partition key is fixed when a table is created.
        partition_note = (
            " NOTE: 'Partition By Creation Month' applies to new tables; "
            f"drop {ctx.table_name} and run a Full sync to apply it."
        )
    # Insert into the columns the live table actually has, converted for
    # their live types (an unmigratable type change must not fail inserts).
    column_defs = insert_columns(client, ctx.schema, database)

    # ``watermark`` is the single point in time this run is "as of". It is the
    # upper bound for incremental reads, the version stamped on every row
    # written, and (on success) the next cursor.
    watermark = now_datetime()
    synced_at = watermark

    # 2. Child tables first. They share the parent's cursor, so they must be
    #    fully synced BEFORE that cursor is allowed to move. If a child table
    #    fails, the run fails here with the cursor untouched and the next run
    #    covers the same window again. (Previously children ran last, their
    #    errors were swallowed, and the already-advanced cursor meant the
    #    missed child rows were never retried.)
    child_written = 0
    if ctx.sync_child_tables:
        live.update(force=True, phase="child tables")
        child_written = _sync_child_tables(client, ctx, database, synced_at)
        live.update(force=True, child_rows_written=child_written)

    # 3. Stream + insert parent rows.
    fields = [c.name for c in column_defs if c.name not in (SYNC_TIMESTAMP_COLUMN, SOFT_DELETE_COLUMN)]
    live.update(force=True, phase="counting")
    total = count_source_rows(ctx, watermark)
    reporter = ProgressReporter(ctx.config_name, total, live)
    reporter.emit(0, 0, "starting", force=True)

    incremental = ctx.sync_mode == "Incremental"
    # The new / updated / re-checked split is only knowable for incremental
    # runs that have a previous cursor:
    #   new        - created after the previous sync position
    #   updated    - modified after it (but created before)
    #   re-checked - unchanged since then; re-read only because each run
    #                starts a safety lag before the cursor
    split_known = incremental and ctx.previous_cursor is not None
    touched_months: set[str] = set()
    rows_new = 0 if split_known else None
    rows_updated = 0 if split_known else None
    rows_rechecked = 0 if split_known else None
    batch_number = 0
    # The cursor never moves backwards: the first window deliberately starts a
    # safety lag before the saved cursor, and those re-read rows must not pull
    # the cursor back with them.
    cursor_floor = ctx.last_sync_timestamp

    for batch in stream_source_rows(ctx, fields, watermark):
        if _is_cancelled(ctx.config_name):
            raise SyncCancelledError(f"Sync cancelled for {ctx.config_name}.")

        batch_number += 1
        result.rows_read += len(batch)
        touched_months.update(_creation_months(batch))
        columns, data = _prepare_batch(ctx, batch, column_defs, synced_at)
        try:
            _insert_batch(client, database, ctx.table_name, columns, data)
            result.rows_written += len(data)
        except Exception:  # noqa: BLE001 - count and re-raise for retry
            result.rows_failed += len(data)
            live.update(
                force=True,
                batch=batch_number,
                rows_read=result.rows_read,
                rows_failed=result.rows_failed,
                failed_batch_first=batch[0].get("name"),
                failed_batch_last=batch[-1].get("name"),
            )
            raise

        if split_known:
            for row in batch:
                created = row.get("creation")
                modified = row.get("modified")
                if created is not None and get_datetime(created) > ctx.previous_cursor:
                    rows_new += 1
                elif modified is not None and get_datetime(modified) > ctx.previous_cursor:
                    rows_updated += 1
                else:
                    rows_rechecked += 1
        last_row = batch[-1]
        # One Redis write per batch (batches are thousands of rows), so the
        # dashboard always shows the batch that was actually completed.
        live.update(
            force=True,
            phase="syncing",
            batch=batch_number,
            rows_read=result.rows_read,
            rows_written=result.rows_written,
            rows_new=rows_new,
            rows_updated=rows_updated,
            rows_rechecked=rows_rechecked,
            cursor_modified=last_row.get("modified"),
            cursor_name=last_row.get("name"),
        )

        # Persist the position after every successful batch => resumable.
        # Incremental batches arrive in (modified, name) order, so every row
        # with an earlier ``modified`` than the last row of this batch has
        # been delivered. Rows sharing that exact timestamp may not all have
        # been read yet; the next run re-reads them because it starts at
        # ``cursor - lag`` with an inclusive comparison.
        if incremental and batch[-1].get("modified") is not None:
            position = get_datetime(batch[-1]["modified"])
            if cursor_floor is None or position > cursor_floor:
                _persist_cursor(ctx.config_name, position)
                cursor_floor = position
                # Keep the in-memory context in step, so a retry of this run
                # resumes from here instead of from the original cursor.
                ctx.last_sync_timestamp = position

        reporter.emit(result.rows_read, result.rows_written, "syncing")

    # 4. Delete reconciliation (full sync, or incremental when opted in).
    live.update(force=True, phase="reconciling deletes", rows_read=result.rows_read, rows_written=result.rows_written)
    deleted = reconcile_deletes(client, ctx, database)
    if deleted:
        result.message = f"{result.rows_written} rows synced, {deleted} deletes reconciled."
    else:
        result.message = f"{result.rows_written} rows synced."

    if child_written:
        result.rows_written += child_written
        result.message += f" {child_written} child rows synced."

    unconverted_note = _unconverted_summary(ctx)
    if unconverted_note:
        result.message += unconverted_note
        live.update(force=True, rows_unconverted=sum(ctx.unconverted.values()))
        frappe.logger("clickhouse_sync").warning(f"{ctx.doctype}:{unconverted_note}")

    # Rows incremental sync can never see (NULL / far-future ``modified``).
    if incremental:
        unreachable = count_unreachable_rows(ctx, watermark)
        live.update(force=True, rows_skipped=unreachable)
        if unreachable:
            result.message += f" WARNING: {unreachable} row(s) skipped (NULL/future modified)."
            frappe.logger("clickhouse_sync").warning(
                f"{ctx.doctype}: {unreachable} row(s) have a NULL or future-dated "
                f"`modified` and cannot be reached by incremental sync. "
                f"Fix the source rows or run a Full sync."
            )

    # 4b. The whole run succeeded: advance the cursor to the watermark. Every
    #     row with ``modified <= watermark`` that was visible has been written.
    #     Anything saved during the run carries a later timestamp (or falls in
    #     the safety lag) and is read by the next run.
    _persist_cursor(ctx.config_name, watermark)

    # 5. Optionally compact the table so it holds exactly one physical row per
    #    record (latest version wins). Without this the ReplacingMergeTree table
    #    keeps prior versions until ClickHouse's background merge runs, so a raw
    #    SELECT (without FINAL) can transiently show duplicates. Running OPTIMIZE
    #    FINAL here makes the table single-row-per-record immediately after each
    #    sync. It rewrites the affected parts, so it is gated behind per-config
    #    flags: `optimize_after_sync` enables it at all, and
    #    `optimize_only_on_full` restricts it to Full syncs so frequent
    #    incremental ticks on a large table don't trigger a full rewrite each
    #    time (query those with FINAL between periodic full compactions).
    should_optimize = (
        ctx.optimize_after_sync
        and result.rows_written
        and (ctx.sync_mode == "Full" or not ctx.optimize_only_on_full)
    )
    if should_optimize:
        live.update(force=True, phase="optimizing")
        _optimize_table(
            client, database, ctx.table_name,
            months=None if ctx.sync_mode == "Full" else touched_months,
        )

    result.message += partition_note
    if ctx.mode_overridden:
        result.message = f"[{ctx.sync_mode} sync requested] " + result.message

    reporter.emit(result.rows_read, result.rows_written, "completed", force=True)
    result.status = "Completed"


def _optimize_table(
    client: "Client",
    database: str,
    table_name: str,
    months: set[str] | None = None,
) -> None:
    """Run ``OPTIMIZE TABLE ... FINAL`` to physically deduplicate the table.

    On a table partitioned by creation month, an incremental run passes the
    months it wrote to (``months``) and only those partitions are rewritten.
    Every other table is a single partition and is optimized whole.

    Failures are logged but never raise - a successful sync must not be marked
    failed just because the post-sync compaction hit a transient issue.
    """
    try:
        if months and is_creation_partitioned(client, database, table_name):
            for month in sorted(months):
                client.command(
                    f"OPTIMIZE TABLE `{database}`.`{table_name}` PARTITION ID {{pid:String}} FINAL",
                    parameters={"pid": month},
                )
            frappe.logger("clickhouse_sync").info(
                f"Optimized {len(months)} partition(s) of {table_name}."
            )
            return
        client.command(f"OPTIMIZE TABLE `{database}`.`{table_name}` FINAL")
        frappe.logger("clickhouse_sync").info(f"Optimized {table_name} (deduplicated).")
    except Exception as exc:  # noqa: BLE001
        frappe.logger("clickhouse_sync").warning(
            f"OPTIMIZE FINAL failed for {table_name} (rows remain deduplicated "
            f"logically; query with FINAL): {exc}"
        )


def _sync_child_tables(
    client: "Client",
    ctx: SyncContext,
    database: str,
    synced_at: datetime,
) -> int:
    """Sync a parent DocType's child tables (grid rows) into ClickHouse.

    Each child DocType becomes its own ClickHouse table carrying the child's
    own fields plus ``parent``/``parenttype``/``parentfield`` link columns, so
    grid data (invoice line items, education rows, etc.) can be joined back to
    the parent in reporting.

    Child tables are filtered to ``parenttype = <this DocType>`` so a child
    DocType shared by several parents only receives the relevant rows. Sync
    mode and delete strategy follow the parent configuration.

    A failure in one child table does not stop the remaining child tables from
    being attempted, but it is never swallowed: once every child has been
    tried, the first error is re-raised so the run is retried or marked
    Failed. The original exception is re-raised unchanged so the retry layer
    can still tell transient from permanent failures.

    Returns:
        The total number of child rows written across all child tables.

    Raises:
        Exception: The first error raised by any child table sync.
    """
    total_written = 0
    first_error: Exception | None = None
    seen: set[str] = set()
    for child_doctype, _parentfield in get_child_doctypes(ctx.doctype):
        # A child DocType used by two Table fields of the same parent is one
        # ClickHouse table and one ``parenttype`` filter: sync it once.
        if child_doctype in seen:
            continue
        seen.add(child_doctype)
        try:
            total_written += _sync_one_child(
                client, ctx, child_doctype, database, synced_at
            )
        except Exception as exc:  # noqa: BLE001 - try the others, then fail the run
            frappe.logger("clickhouse_sync").error(
                f"Child table sync failed for {ctx.doctype} -> {child_doctype}: {exc}"
            )
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error
    return total_written


def _sync_one_child(
    client: "Client",
    ctx: SyncContext,
    child_doctype: str,
    database: str,
    synced_at: datetime,
) -> int:
    """Sync one child DocType within the parent's watermark and cursor window.

    Incremental reads keyset-page by ``(modified, name)``. Afterward, bounded
    reconciliation removes or marks child rows that were deleted or moved to
    another parent DocType.
    """
    child_schema = build_table_schema(
        child_doctype,
        include_soft_delete=(ctx.delete_strategy == "Soft Delete"),
        exclude_fields=set(ctx.exclude_fields),
        is_child=True,
        partition_by_creation=ctx.partition_by_creation,
    )
    ensure_table(client, child_schema, database, auto_create=ctx.auto_create)
    reconcile_schema(client, child_schema, database, auto_update=ctx.auto_update)
    column_defs = insert_columns(client, child_schema, database)

    managed = {SYNC_TIMESTAMP_COLUMN, SOFT_DELETE_COLUMN}
    valid = set(frappe.get_meta(child_doctype).get_valid_columns())
    fields = [c.name for c in column_defs if c.name not in managed and c.name in valid]
    if "name" not in fields:
        fields.insert(0, "name")
    # ``parent`` and ``modified`` drive the keyset and the delete check.
    for required in ("modified", "parent"):
        if required in valid and required not in fields:
            fields.append(required)
    insert_names = {c.name for c in column_defs}

    written = 0
    batch_number = 0
    touched_parents: set[str] = set()
    touched_months: set[str] = set()
    live = ctx.live or live_status.NullLiveRun()
    lower = incremental_lower_bound(ctx)

    for rows in keyset_batches(
        ctx,
        f"`tab{child_doctype}`",
        fields,
        upper=synced_at,
        conditions=["parenttype = %(ptype)s"],
        params={"ptype": ctx.doctype},
    ):
        if _is_cancelled(ctx.config_name):
            raise SyncCancelledError(f"Sync cancelled for {ctx.config_name}.")
        columns, data = _prepare_batch(ctx, rows, column_defs, synced_at)
        client.insert(
            table=child_schema.table_name,
            data=data,
            column_names=columns,
            database=database,
        )
        written += len(data)
        batch_number += 1
        touched_parents.update(row.get("parent") for row in rows if row.get("parent"))
        touched_months.update(_creation_months(rows))
        # Keep the dashboard heartbeat alive: a large child table can take
        # longer than the stall threshold.
        live.update(
            force=True,
            phase=f"child table {child_doctype}",
            child_rows_written=written,
            child_batch=batch_number,
        )

    if ctx.delete_strategy != "Ignore":
        live.update(force=True, phase=f"reconciling child table {child_doctype}")
        if ctx.sync_mode == "Full" or ctx.reconcile_deletes_incremental or lower is None:
            # Full comparison: full syncs, opted-in incremental syncs, and the
            # first incremental run (no window to scope the check to).
            _reconcile_missing_rows(
                client,
                child_doctype,
                child_schema.table_name,
                database,
                ctx.delete_strategy,
                source_parenttype=ctx.doctype,
            )
        else:
            # Incremental: only the children of parents changed in this
            # window (plus parents of child rows read in it) can have gone.
            parents = set(_parents_changed_in_window(ctx, synced_at)) | touched_parents
            if parents and "parent" in insert_names:
                _reconcile_children_of_parents(
                    client,
                    child_doctype,
                    child_schema.table_name,
                    database,
                    ctx.delete_strategy,
                    ctx.doctype,
                    sorted(parents),
                )

    if written and ctx.optimize_after_sync and (
        ctx.sync_mode == "Full" or not ctx.optimize_only_on_full
    ):
        live.update(force=True, phase=f"optimizing {child_schema.table_name}")
        _optimize_table(
            client, database, child_schema.table_name,
            months=None if ctx.sync_mode == "Full" else touched_months,
        )

    return written


def _persist_cursor(config_name: str, cursor: datetime) -> None:
    """Persist the incremental cursor without tripping the modified-check."""
    frappe.db.set_value(
        "ClickHouse Sync Configuration",
        config_name,
        "last_sync_timestamp",
        cursor,
        update_modified=False,
    )
    frappe.db.commit()


# --------------------------------------------------------------------------- #
# Public entry point (retry + logging wrapper)
# --------------------------------------------------------------------------- #
def run_sync(
    config_name: str, trigger: str = "Manual", mode: str | None = None
) -> SyncResult:
    """Run a full sync pipeline for one configuration, with retry + logging.

    This is the single public entry point used by the scheduler, the API and
    the background job. Failures - including ClickHouse being unreachable for
    every attempt - are captured, logged to a ClickHouse Sync Log, and
    returned inside the :class:`SyncResult`; it does not raise for them. Only
    a worker shutdown signal propagates, after a Failed log has been written.

    Args:
        config_name: Name of the ClickHouse Sync Configuration to run.
        trigger: ``"Scheduler"`` or ``"Manual"`` - recorded on the log.
        mode: Optional one-off sync mode (``"Full"``/``"Incremental"``)
            overriding the configuration for this run only.

    Returns:
        A populated :class:`SyncResult`.
    """
    result = SyncResult(start_time=now_datetime(), status="Running")
    logger = frappe.logger("clickhouse_sync")

    # A stop pressed while this job was still waiting in the queue is honoured
    # here; any older flag (left by a previous run) is discarded.
    queued = live_status.get_live(config_name) or {}
    stop_at = _cancel_requested_at(config_name)
    queued_at = get_datetime(queued["queued_at"]) if queued.get("queued_at") else None
    _clear_cancel(config_name)
    live = live_status.start_run(config_name, None, trigger, mode, max_attempts=1)
    if stop_at and queued_at and stop_at >= queued_at:
        result.status = "Cancelled"
        result.message = "Sync cancelled before it started."
        result.end_time = now_datetime()
        _write_log(config_name, trigger, result)
        live.finish("Cancelled", result.log_name, result.message, "Cancelled")
        return result

    try:
        ctx = build_context(config_name, trigger=trigger, mode=mode)
    except Exception as exc:  # noqa: BLE001 - config errors are terminal
        _safe_rollback()
        result.status = "Failed"
        result.end_time = now_datetime()
        result.exception = traceback.format_exc()
        result.message = str(exc)
        _write_log(config_name, trigger, result)
        live.finish("Failed", result.log_name, result.message, "Configuration")
        logger.error(f"Sync config load failed for {config_name}: {exc}")
        return result

    ctx.live = live
    live.update(force=True, doctype=ctx.doctype, mode=ctx.sync_mode, max_attempts=ctx.retry_count)
    category: str | None = None

    attempt = 0
    try:
        while True:
            attempt += 1
            live.update(force=True, attempt=attempt)
            try:
                if attempt > 1:
                    # Force a fresh connection for the retry. This sits INSIDE
                    # the try on purpose: if ClickHouse is still down the
                    # reconnect itself fails, and that failure must be counted
                    # as another attempt - not escape and skip the Sync Log.
                    ch_connection.get_client(force_new=True)
                _run_once(ctx, result)
                break
            except SyncCancelledError as exc:
                result.status = "Cancelled"
                result.message = str(exc)
                category = "Cancelled"
                break
            except Exception as exc:  # noqa: BLE001
                transient = is_transient_error(exc)
                logger.warning(
                    f"Sync attempt {attempt}/{ctx.retry_count} failed for "
                    f"{config_name} (transient={transient}): {exc}"
                )
                live.update(force=True, last_error=str(exc)[:300])
                if attempt >= ctx.retry_count or not transient:
                    _safe_rollback()
                    result.status = "Failed"
                    result.exception = traceback.format_exc()
                    result.message = str(exc)
                    category = categorize_error(exc)
                    break
                # Only now is it certain another attempt follows; the phase the
                # failure happened in stays recorded until this point.
                live.update(force=True, phase="retrying")
                # A transient database error (deadlock, lost connection) can
                # leave the transaction unusable for the next attempt.
                _safe_rollback()
                # Reset per-attempt counters before the next attempt.
                result.rows_read = 0
                result.rows_written = 0
                result.rows_failed = 0
                _backoff_sleep(ctx.retry_interval, attempt)
    except BaseException as exc:
        # Not an ordinary error: the worker is being stopped (SystemExit,
        # KeyboardInterrupt) or something escaped the handlers above. Record
        # the run as Failed so it is visible, then let the signal propagate.
        _safe_rollback()
        result.status = "Failed"
        result.exception = traceback.format_exc()
        result.message = f"Sync interrupted: {type(exc).__name__}: {exc}"
        result.end_time = now_datetime()
        _write_log(config_name, trigger, result)
        live.finish("Failed", result.log_name, result.message, "Interrupted")
        raise

    result.end_time = now_datetime()
    _write_log(config_name, trigger, result)
    live.finish(result.status, result.log_name, result.message, category)

    # Fire an optional Telegram alert (governed by ClickHouse Settings.alert_on).
    # This never raises - alerting problems must not affect the sync outcome.
    try:
        from clickhouse_sync.clickhouse.alerts import send_run_alert

        send_run_alert(config_name, trigger, result)
    except Exception:  # noqa: BLE001
        logger.warning(f"alert dispatch failed: {traceback.format_exc()}")

    return result


MAX_BACKOFF_SECONDS = 300


def _backoff_delay(base_interval: int, attempt: int) -> float:
    """Exponential backoff, capped, with up to 20% jitter.

    The cap keeps a long retry series from sleeping for most of an hour (and
    stays under the dashboard's stall threshold); the jitter stops many
    configurations that failed together from retrying in lock-step.
    """
    delay = min(float(base_interval) * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)
    return delay * (1 + random.uniform(0, 0.2))


def _backoff_sleep(base_interval: int, attempt: int) -> None:
    """Sleep with capped exponential backoff between retry attempts."""
    time.sleep(_backoff_delay(base_interval, attempt))


def _safe_rollback() -> None:
    """Roll back a possibly broken transaction before writing the Sync Log.

    After a database error (lock wait timeout, lost connection) the open
    transaction can refuse further writes, which used to lose the log of the
    very failure it should record. Cursor positions were committed batch by
    batch, so nothing that must be kept is rolled back.
    """
    try:
        frappe.db.rollback()
    except Exception:  # noqa: BLE001
        pass


def _write_log(config_name: str, trigger: str, result: SyncResult) -> str:
    """Persist a ClickHouse Sync Log document for the run and return its name."""
    try:
        log = frappe.new_doc("ClickHouse Sync Log")
        log.configuration = config_name
        log.start_time = result.start_time
        log.end_time = result.end_time
        log.duration = result.duration
        log.rows_read = result.rows_read
        log.rows_written = result.rows_written
        log.rows_failed = result.rows_failed
        log.status = result.status
        log.message = (result.message or "")[:140]
        log.exception = result.exception
        log.execution_host = socket.gethostname()
        log.trigger = trigger
        log.insert(ignore_permissions=True)
        frappe.db.commit()
        result.log_name = log.name
        return log.name
    except Exception as exc:  # noqa: BLE001 - logging must never mask the result
        frappe.logger("clickhouse_sync").error(f"Failed to write sync log: {exc}")
        return ""


# --------------------------------------------------------------------------- #
# Delete propagation (queued from doc_events after_delete, applied in batches)
# --------------------------------------------------------------------------- #
# Deletes used to be sent from ``on_trash``: inside the user's request, BEFORE
# Frappe checked links and before the transaction committed. A delete refused
# because other documents link to it ("Cannot delete ... linked with ...")
# had already removed the row from ClickHouse, and a ClickHouse outage made
# every delete in the ERP hang through the connection retries. Now the
# document name is queued in Redis only once the delete has COMMITTED, and a
# short background job applies queued deletes as one mutation per DocType per
# batch. Anything the queue misses (Redis restart, worker crash) is still
# caught by delete reconciliation.
PENDING_DELETES_KEY = "clickhouse_sync:pending_deletes"
DELETE_STRATEGIES_KEY = "clickhouse_sync:delete_strategies"
DELETE_STRATEGIES_TTL_SECONDS = 300
FLUSH_DELETES_JOB_ID = "clickhouse_sync::flush_pending_deletes"
FLUSH_MAX_ITEMS_PER_RUN = 50000


def _load_delete_strategies() -> dict[str, str]:
    rows = frappe.db.sql(
        "SELECT source_doctype, delete_strategy FROM `tabClickHouse Sync Configuration` "
        "WHERE enabled = 1",
        as_dict=True,
    )
    return {r["source_doctype"]: (r["delete_strategy"] or "Ignore") for r in rows}


def delete_strategy_for(doctype: str) -> str | None:
    """Delete strategy of the enabled configuration for ``doctype``, if any.

    Cached in Redis because the ``after_delete`` hook runs for every delete of
    every DocType; the cache is cleared whenever a configuration changes.
    """
    try:
        cache = frappe.cache()
        mapping = cache.get_value(DELETE_STRATEGIES_KEY)
        if mapping is None:
            mapping = _load_delete_strategies()
            cache.set_value(
                DELETE_STRATEGIES_KEY, mapping, expires_in_sec=DELETE_STRATEGIES_TTL_SECONDS
            )
    except Exception:  # noqa: BLE001 - Redis down: ask the database
        mapping = _load_delete_strategies()
    return mapping.get(doctype)


def clear_delete_strategy_cache() -> None:
    try:
        frappe.cache().delete_value(DELETE_STRATEGIES_KEY)
    except Exception:  # noqa: BLE001
        pass


def queue_delete(doctype: str, docname: str) -> bool:
    """Queue ``doctype``/``docname`` for deletion in ClickHouse after commit.

    Returns ``True`` if the delete will be propagated. Does no ClickHouse I/O,
    so it adds nothing noticeable to the user's delete request.
    """
    strategy = delete_strategy_for(doctype)
    if strategy not in ("Soft Delete", "Hard Delete"):
        return False
    payload = json.dumps([doctype, docname])

    def _push() -> None:
        try:
            frappe.cache().rpush(PENDING_DELETES_KEY, payload)
            frappe.enqueue(
                "clickhouse_sync.tasks.flush_pending_deletes",
                queue="short",
                job_id=FLUSH_DELETES_JOB_ID,
                deduplicate=True,
            )
        except Exception as exc:  # noqa: BLE001 - reconciliation is the safety net
            frappe.logger("clickhouse_sync").warning(
                f"could not queue ClickHouse delete for {doctype}/{docname}: {exc}"
            )

    after_commit = getattr(frappe.db, "after_commit", None)
    if after_commit is not None:
        after_commit.add(_push)
    else:  # pragma: no cover - very old Frappe without commit callbacks
        _push()
    return True


def _decode(raw: Any) -> tuple[str, str] | None:
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        doctype, name = json.loads(raw)
        return str(doctype), str(name)
    except Exception:  # noqa: BLE001 - drop malformed entries
        return None


def flush_pending_deletes(max_items: int = FLUSH_MAX_ITEMS_PER_RUN) -> int:
    """Apply queued deletes, one ClickHouse mutation per DocType per batch.

    Transient failures put the batch back on the queue for the next flush;
    permanent ones are logged and dropped (reconciliation repairs them).

    Returns:
        The number of queued entries processed.
    """
    cache = frappe.cache()
    logger = frappe.logger("clickhouse_sync")
    processed = 0
    while processed < max_items:
        raw_items = cache.lrange(PENDING_DELETES_KEY, 0, DELETE_RECONCILE_BATCH_SIZE - 1) or []
        if not raw_items:
            break
        cache.ltrim(PENDING_DELETES_KEY, len(raw_items), -1)
        processed += len(raw_items)

        by_doctype: dict[str, list[str]] = {}
        for raw in raw_items:
            item = _decode(raw)
            if item:
                by_doctype.setdefault(item[0], []).append(item[1])

        for doctype, names in by_doctype.items():
            try:
                propagate_delete(doctype, names)
            except Exception as exc:  # noqa: BLE001
                if is_transient_error(exc):
                    for name in names:
                        cache.rpush(PENDING_DELETES_KEY, json.dumps([doctype, name]))
                    logger.warning(
                        f"ClickHouse delete for {len(names)} {doctype} row(s) re-queued: {exc}"
                    )
                    return processed  # ClickHouse is struggling: try again later
                logger.error(
                    f"ClickHouse delete for {len(names)} {doctype} row(s) dropped "
                    f"(reconciliation will retry): {exc}"
                )
    return processed


def propagate_delete(doctype: str, docnames: str | list[str]) -> int:
    """Apply a deletion of ``docnames`` to ClickHouse now, in one mutation.

    Honours the configuration's delete strategy and is a no-op when the
    DocType is not configured or its strategy is ``Ignore``. Names that exist
    in the source again (restored documents) are skipped. Errors propagate so
    the caller can retry; :func:`flush_pending_deletes` handles that.

    Returns:
        The number of names sent to ClickHouse.
    """
    names = [docnames] if isinstance(docnames, str) else list(dict.fromkeys(docnames))
    if not names:
        return 0
    strategy = frappe.db.get_value(
        "ClickHouse Sync Configuration",
        {"source_doctype": doctype, "enabled": 1},
        "delete_strategy",
    )
    if strategy not in ("Soft Delete", "Hard Delete"):
        return 0

    placeholders = ", ".join(f"%(name_{i})s" for i in range(len(names)))
    existing = frappe.db.sql(
        f"SELECT name FROM `tab{doctype}` WHERE name IN ({placeholders})",
        {f"name_{i}": name for i, name in enumerate(names)},
        as_dict=True,
    )
    still_there = {row["name"] for row in existing}
    names = [name for name in names if name not in still_there]
    if not names:
        return 0

    client = ch_connection.get_client()
    database = ch_connection.get_connection_params().database
    table = table_name_for(doctype)
    for start in range(0, len(names), DELETE_RECONCILE_BATCH_SIZE):
        _apply_delete(client, database, table, strategy, names[start : start + DELETE_RECONCILE_BATCH_SIZE])
    return len(names)
