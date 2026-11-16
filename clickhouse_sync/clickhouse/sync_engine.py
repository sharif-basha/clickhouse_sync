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
* **Observable**: progress is published over Frappe realtime, and every run
  (and every retry) is recorded in a ClickHouse Sync Log document.
"""

from __future__ import annotations

import socket
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterator

import frappe
from frappe.utils import cint, get_datetime, now_datetime

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse.exceptions import (
    ClickHouseSyncError,
    SyncCancelledError,
    is_transient_error,
)
from clickhouse_sync.clickhouse.schema import (
    SOFT_DELETE_COLUMN,
    SYNC_TIMESTAMP_COLUMN,
    TableSchema,
    build_table_schema,
    ensure_table,
    get_child_doctypes,
    reconcile_schema,
    table_name_for,
)
from clickhouse_sync.clickhouse.type_mapping import coerce_value

if TYPE_CHECKING:  # pragma: no cover
    from clickhouse_connect.driver.client import Client


CANCEL_CACHE_PREFIX = "clickhouse_sync::cancel::"
PROGRESS_EVENT = "clickhouse_sync_progress"
DEFAULT_BATCH_SIZE = 25000
PROGRESS_THROTTLE_SECONDS = 1.0

# Incremental runs start this long *before* the saved cursor. Frappe stamps
# ``modified`` when a document is saved, not when its transaction commits, so a
# row can become visible slightly after a later-stamped row was already read.
# Re-reading a short overlap closes that gap; duplicates are harmless because
# the target table is a ReplacingMergeTree keyed on ``name``.
CURSOR_SAFETY_LAG_SECONDS = 300

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
    optimize_only_on_full: bool = False
    reconcile_deletes_incremental: bool = False
    sync_child_tables: bool = False
    schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        self.schema = build_table_schema(
            self.doctype,
            include_soft_delete=(self.delete_strategy == "Soft Delete"),
            exclude_fields=set(self.exclude_fields),
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
    """Signal a running sync for ``config_name`` to stop at the next batch."""
    frappe.cache().set_value(_cancel_key(config_name), "1", expires_in_sec=3600)


def _clear_cancel(config_name: str) -> None:
    frappe.cache().delete_value(_cancel_key(config_name))


def _is_cancelled(config_name: str) -> bool:
    """Return ``True`` if a cancel has been requested, reading Redis directly.

    Frappe keeps a per-request/per-job copy of cache reads in
    ``frappe.local.cache``. A plain ``get_value`` therefore returns the value
    seen on the *first* check for the rest of the job, so a long-running sync
    would never notice the Cancel button being pressed in another process.
    Drop any local copy before each read, and read with ``expires=True`` so
    the result is not stored locally again.
    """
    cache = frappe.cache()
    key = _cancel_key(config_name)
    _forget_local_cache_entry(cache, key)
    try:
        value = cache.get_value(key, expires=True)
    except TypeError:  # very old wrappers without the ``expires`` argument
        _forget_local_cache_entry(cache, key)
        value = cache.get_value(key)
        _forget_local_cache_entry(cache, key)
    return bool(value)


def _forget_local_cache_entry(cache: Any, key: str) -> None:
    """Remove ``key`` from Frappe's in-process cache copy, if there is one."""
    local_cache = getattr(getattr(frappe, "local", None), "cache", None)
    make_key = getattr(cache, "make_key", None)
    if not isinstance(local_cache, dict) or make_key is None:
        return
    try:
        local_cache.pop(make_key(key), None)
    except Exception:  # noqa: BLE001 - a cache quirk must never break a sync
        pass


# --------------------------------------------------------------------------- #
# Progress reporting
# --------------------------------------------------------------------------- #
class ProgressReporter:
    """Throttled publisher of realtime progress events for the Desk UI."""

    def __init__(self, config_name: str, total: int) -> None:
        self.config_name = config_name
        self.total = max(total, 0)
        self._last_emit = 0.0

    def emit(self, read: int, written: int, phase: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_emit) < PROGRESS_THROTTLE_SECONDS:
            return
        self._last_emit = now
        percent = round((read / self.total) * 100, 1) if self.total else 0.0
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
def build_context(config_name: str, trigger: str = "Manual") -> SyncContext:
    """Load a ClickHouse Sync Configuration and build a :class:`SyncContext`."""
    config = frappe.get_doc("ClickHouse Sync Configuration", config_name)

    if not config.enabled:
        raise ClickHouseSyncError(f"Configuration '{config_name}' is disabled.")

    last_ts = get_datetime(config.last_sync_timestamp) if config.last_sync_timestamp else None

    return SyncContext(
        config_name=config.name,
        doctype=config.source_doctype,
        table_name=table_name_for(config.source_doctype),
        sync_mode=config.sync_mode or "Full",
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
        optimize_only_on_full=_coerce_flag(config.get("optimize_only_on_full"), default=False),
        reconcile_deletes_incremental=_coerce_flag(
            config.get("reconcile_deletes_incremental"), default=False
        ),
        sync_child_tables=_coerce_flag(config.get("sync_child_tables"), default=False),
    )


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
def _source_field_order(schema: TableSchema) -> list[str]:
    """Columns to read from MariaDB, in the exact ClickHouse insert order.

    Engine-managed columns (sync timestamp, soft-delete flag) are excluded -
    they are synthesised at insert time, not read from the source.
    """
    managed = {SYNC_TIMESTAMP_COLUMN, SOFT_DELETE_COLUMN}
    return [c.name for c in schema.columns if c.name not in managed]


def incremental_lower_bound(ctx: SyncContext) -> datetime | None:
    """Return the ``modified`` value an incremental run starts reading from.

    ``None`` means "no lower bound" (Full mode, or the very first incremental
    run). Otherwise it is the saved cursor minus :data:`CURSOR_SAFETY_LAG_SECONDS`.
    """
    if ctx.sync_mode != "Incremental" or ctx.last_sync_timestamp is None:
        return None
    return ctx.last_sync_timestamp - timedelta(seconds=CURSOR_SAFETY_LAG_SECONDS)


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
    if ctx.sync_mode == "Incremental":
        yield from _stream_by_modified(ctx, select_fields, upper or now_datetime())
    else:
        yield from _stream_by_name(ctx, select_fields)


def _stream_by_name(
    ctx: SyncContext, select_fields: list[str]
) -> Iterator[list[dict[str, Any]]]:
    """Full-table read ordered by primary key (``WHERE name > :last``)."""
    table = f"`tab{ctx.doctype}`"
    col_sql = ", ".join(f"`{c}`" for c in select_fields)
    last_name: Any = ""

    while True:
        params: dict[str, Any] = {"last_name": last_name, "limit": ctx.batch_size}
        query = (
            f"SELECT {col_sql} FROM {table} "
            f"WHERE name > %(last_name)s "
            f"ORDER BY name ASC LIMIT %(limit)s"
        )
        rows: list[dict[str, Any]] = frappe.db.sql(query, params, as_dict=True)
        if not rows:
            break

        yield rows
        last_name = rows[-1]["name"]
        if len(rows) < ctx.batch_size:
            break


def _stream_by_modified(
    ctx: SyncContext,
    select_fields: list[str],
    upper: datetime,
) -> Iterator[list[dict[str, Any]]]:
    """Changed-row read ordered by ``(modified, name)``, bounded by ``upper``.

    The first window starts at :func:`incremental_lower_bound` (inclusive, so
    rows sharing the boundary timestamp are never skipped). Every later window
    continues strictly after the last row yielded, using ``name`` to break ties
    between rows with an identical ``modified`` - this keeps the loop correct
    even when more rows than one batch share the same timestamp (bulk imports).

    The ordering rides the ``modified`` index, which InnoDB extends with the
    primary key, so only changed rows are touched.
    """
    table = f"`tab{ctx.doctype}`"
    col_sql = ", ".join(f"`{c}`" for c in select_fields)
    lower = incremental_lower_bound(ctx)

    position: tuple[datetime, Any] | None = None  # (modified, name) of last row yielded

    while True:
        conditions = ["modified <= %(upper)s"]
        params: dict[str, Any] = {"upper": upper, "limit": ctx.batch_size}
        if position is not None:
            conditions.append(
                "(modified > %(pos_modified)s "
                "OR (modified = %(pos_modified)s AND name > %(pos_name)s))"
            )
            params["pos_modified"], params["pos_name"] = position
        elif lower is not None:
            conditions.append("modified >= %(lower)s")
            params["lower"] = lower

        query = (
            f"SELECT {col_sql} FROM {table} "
            f"WHERE {' AND '.join(conditions)} "
            f"ORDER BY modified ASC, name ASC LIMIT %(limit)s"
        )
        rows: list[dict[str, Any]] = frappe.db.sql(query, params, as_dict=True)
        if not rows:
            break

        yield rows
        # ``modified`` cannot be NULL here: the ``modified <= upper`` predicate
        # already excludes NULLs.
        position = (get_datetime(rows[-1]["modified"]), rows[-1]["name"])
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
    type_by_name = {c.name: c.frappe_fieldtype for c in column_defs}

    data: list[list[Any]] = []
    for row in rows:
        record: list[Any] = []
        for col in columns:
            if col == SYNC_TIMESTAMP_COLUMN:
                record.append(synced_at)
            elif col == SOFT_DELETE_COLUMN:
                record.append(0)
            else:
                record.append(coerce_value(type_by_name.get(col, "Data"), row.get(col)))
        data.append(record)
    return columns, data


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

    Compares the set of ``name`` values present in ClickHouse against the
    source and:

    * **Soft Delete** - marks vanished rows with ``_ch_is_deleted = 1``.
    * **Hard Delete** - issues a lightweight ``DELETE`` for vanished rows.
    * **Ignore** - does nothing.

    Runs on full syncs, and on incremental syncs when the configuration has
    ``reconcile_deletes_incremental`` enabled. This is the reliable safety net
    for deletions that the real-time ``on_trash`` hook can miss (rows deleted
    while a sync was down, bulk/direct-DB deletes). It performs a full
    ``name``-set comparison, so it is opt-in for incremental to avoid that scan
    cost on every scheduled tick.

    Returns:
        The number of rows affected by the delete reconciliation.
    """
    if ctx.delete_strategy == "Ignore":
        return 0
    if ctx.sync_mode != "Full" and not ctx.reconcile_deletes_incremental:
        return 0

    source_names: set[str] = set(
        frappe.get_all(ctx.doctype, pluck="name", limit_page_length=0)
    )

    result = client.query(
        f"SELECT DISTINCT `name` FROM `{database}`.`{ctx.table_name}`"
    )
    ch_names: set[str] = {row[0] for row in result.result_rows}

    vanished = list(ch_names - source_names)
    if not vanished:
        return 0

    affected = 0
    # Chunk to keep the IN() list and query size bounded.
    for start in range(0, len(vanished), 10000):
        chunk = vanished[start : start + 10000]
        placeholders = ", ".join(f"'{_escape(n)}'" for n in chunk)
        if ctx.delete_strategy == "Soft Delete":
            client.command(
                f"ALTER TABLE `{database}`.`{ctx.table_name}` "
                f"UPDATE `{SOFT_DELETE_COLUMN}` = 1 "
                f"WHERE `name` IN ({placeholders})"
            )
        elif ctx.delete_strategy == "Hard Delete":
            client.command(
                f"DELETE FROM `{database}`.`{ctx.table_name}` "
                f"WHERE `name` IN ({placeholders})"
            )
        affected += len(chunk)

    return affected


def _escape(value: str) -> str:
    """Minimal single-quote escaping for identifiers used in IN() lists."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


# --------------------------------------------------------------------------- #
# Core run (single attempt)
# --------------------------------------------------------------------------- #
def _run_once(ctx: SyncContext, result: SyncResult) -> None:
    """Execute exactly one sync attempt, mutating ``result`` in place.

    Raises on failure so the retry wrapper can decide whether to retry.
    """
    client = ch_connection.get_client()
    params = ch_connection.get_connection_params()
    database = params.database

    # 1. Table + schema reconciliation.
    ensure_table(client, ctx.schema, database, auto_create=ctx.auto_create)
    reconcile_schema(client, ctx.schema, database, auto_update=ctx.auto_update)

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
        child_written = _sync_child_tables(client, ctx, database, synced_at)

    # 3. Stream + insert parent rows.
    fields = _source_field_order(ctx.schema)
    column_defs = ctx.schema.columns
    total = count_source_rows(ctx, watermark)
    reporter = ProgressReporter(ctx.config_name, total)
    reporter.emit(0, 0, "starting", force=True)

    incremental = ctx.sync_mode == "Incremental"
    # The cursor never moves backwards: the first window deliberately starts a
    # safety lag before the saved cursor, and those re-read rows must not pull
    # the cursor back with them.
    cursor_floor = ctx.last_sync_timestamp

    for batch in stream_source_rows(ctx, fields, watermark):
        if _is_cancelled(ctx.config_name):
            raise SyncCancelledError(f"Sync cancelled for {ctx.config_name}.")

        result.rows_read += len(batch)
        columns, data = _prepare_batch(ctx, batch, column_defs, synced_at)
        try:
            _insert_batch(client, database, ctx.table_name, columns, data)
            result.rows_written += len(data)
        except Exception:  # noqa: BLE001 - count and re-raise for retry
            result.rows_failed += len(data)
            raise

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
    deleted = reconcile_deletes(client, ctx, database)
    if deleted:
        result.message = f"{result.rows_written} rows synced, {deleted} deletes reconciled."
    else:
        result.message = f"{result.rows_written} rows synced."

    if child_written:
        result.rows_written += child_written
        result.message += f" {child_written} child rows synced."

    # Rows incremental sync can never see (NULL / far-future ``modified``).
    if incremental:
        unreachable = count_unreachable_rows(ctx, watermark)
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
        _optimize_table(client, database, ctx.table_name)

    reporter.emit(result.rows_read, result.rows_written, "completed", force=True)
    result.status = "Completed"


def _optimize_table(client: "Client", database: str, table_name: str) -> None:
    """Run ``OPTIMIZE TABLE ... FINAL`` to physically deduplicate the table.

    Failures are logged but never raise - a successful sync must not be marked
    failed just because the post-sync compaction hit a transient issue.
    """
    try:
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
    """Sync a single child DocType filtered to the parent DocType's rows."""
    child_schema = build_table_schema(
        child_doctype,
        include_soft_delete=(ctx.delete_strategy == "Soft Delete"),
        exclude_fields=set(ctx.exclude_fields),
        is_child=True,
    )
    ensure_table(client, child_schema, database, auto_create=ctx.auto_create)
    reconcile_schema(client, child_schema, database, auto_update=ctx.auto_update)

    managed = {SYNC_TIMESTAMP_COLUMN, SOFT_DELETE_COLUMN}
    valid = set(frappe.get_meta(child_doctype).get_valid_columns())
    fields = [
        c.name for c in child_schema.columns if c.name not in managed and c.name in valid
    ]
    if "name" not in fields:
        fields.insert(0, "name")

    col_sql = ", ".join(f"`{c}`" for c in fields)
    src_table = f"`tab{child_doctype}`"
    written = 0
    last_name = ""

    # Same window start as the parent (saved cursor minus the safety lag).
    # Child rows are read all-or-nothing per run and the shared cursor only
    # moves after they succeed, so primary-key paging is safe here.
    lower = incremental_lower_bound(ctx)

    while True:
        conditions = ["parenttype = %(ptype)s", "name > %(last)s"]
        params: dict[str, Any] = {
            "ptype": ctx.doctype,
            "last": last_name,
            "limit": ctx.batch_size,
        }
        if lower is not None:
            conditions.append("modified >= %(cursor)s")
            params["cursor"] = lower

        where = " AND ".join(conditions)
        rows: list[dict[str, Any]] = frappe.db.sql(
            f"SELECT {col_sql} FROM {src_table} WHERE {where} "
            f"ORDER BY name ASC LIMIT %(limit)s",
            params,
            as_dict=True,
        )
        if not rows:
            break

        columns, data = _prepare_batch(ctx, rows, child_schema.columns, synced_at)
        client.insert(
            table=child_schema.table_name,
            data=data,
            column_names=columns,
            database=database,
        )
        written += len(data)
        last_name = rows[-1]["name"]
        if len(rows) < ctx.batch_size:
            break

    if written and ctx.optimize_after_sync and (
        ctx.sync_mode == "Full" or not ctx.optimize_only_on_full
    ):
        _optimize_table(client, database, child_schema.table_name)

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
def run_sync(config_name: str, trigger: str = "Manual") -> SyncResult:
    """Run a full sync pipeline for one configuration, with retry + logging.

    This is the single public entry point used by the scheduler, the API and
    the background job. Failures - including ClickHouse being unreachable for
    every attempt - are captured, logged to a ClickHouse Sync Log, and
    returned inside the :class:`SyncResult`; it does not raise for them. Only
    a worker shutdown signal propagates, after a Failed log has been written.

    Args:
        config_name: Name of the ClickHouse Sync Configuration to run.
        trigger: ``"Scheduler"`` or ``"Manual"`` - recorded on the log.

    Returns:
        A populated :class:`SyncResult`.
    """
    result = SyncResult(start_time=now_datetime(), status="Running")
    logger = frappe.logger("clickhouse_sync")
    _clear_cancel(config_name)

    try:
        ctx = build_context(config_name, trigger=trigger)
    except Exception as exc:  # noqa: BLE001 - config errors are terminal
        result.status = "Failed"
        result.end_time = now_datetime()
        result.exception = traceback.format_exc()
        result.message = str(exc)
        _write_log(config_name, trigger, result)
        logger.error(f"Sync config load failed for {config_name}: {exc}")
        return result

    attempt = 0
    try:
        while True:
            attempt += 1
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
                break
            except Exception as exc:  # noqa: BLE001
                transient = is_transient_error(exc)
                logger.warning(
                    f"Sync attempt {attempt}/{ctx.retry_count} failed for "
                    f"{config_name} (transient={transient}): {exc}"
                )
                if attempt >= ctx.retry_count or not transient:
                    result.status = "Failed"
                    result.exception = traceback.format_exc()
                    result.message = str(exc)
                    break
                # Reset per-attempt counters before the next attempt.
                result.rows_read = 0
                result.rows_written = 0
                result.rows_failed = 0
                _backoff_sleep(ctx.retry_interval, attempt)
    except BaseException as exc:
        # Not an ordinary error: the worker is being stopped (SystemExit,
        # KeyboardInterrupt) or something escaped the handlers above. Record
        # the run as Failed so it is visible, then let the signal propagate.
        result.status = "Failed"
        result.exception = traceback.format_exc()
        result.message = f"Sync interrupted: {type(exc).__name__}: {exc}"
        result.end_time = now_datetime()
        _write_log(config_name, trigger, result)
        raise

    result.end_time = now_datetime()
    _write_log(config_name, trigger, result)

    # Fire an optional Telegram alert (governed by ClickHouse Settings.alert_on).
    # This never raises - alerting problems must not affect the sync outcome.
    try:
        from clickhouse_sync.clickhouse.alerts import send_run_alert

        send_run_alert(config_name, trigger, result)
    except Exception:  # noqa: BLE001
        logger.warning(f"alert dispatch failed: {traceback.format_exc()}")

    return result


def _backoff_sleep(base_interval: int, attempt: int) -> None:
    """Sleep with exponential backoff between retry attempts."""
    time.sleep(base_interval * (2 ** (attempt - 1)))


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
# Real-time delete propagation (called from doc_events on_trash)
# --------------------------------------------------------------------------- #
def propagate_delete(doctype: str, docname: str) -> None:
    """Propagate a single Frappe deletion to ClickHouse in real time.

    Wired to the framework ``on_trash`` event for every configured DocType.
    Honours the configuration's delete strategy. Silently no-ops when the
    DocType is not configured or its strategy is ``Ignore``.
    """
    config_name = frappe.db.get_value(
        "ClickHouse Sync Configuration",
        {"source_doctype": doctype, "enabled": 1},
        "name",
    )
    if not config_name:
        return

    strategy = frappe.db.get_value(
        "ClickHouse Sync Configuration", config_name, "delete_strategy"
    )
    if strategy == "Ignore":
        return

    try:
        client = ch_connection.get_client()
        database = ch_connection.get_connection_params().database
        table = table_name_for(doctype)
        escaped = _escape(docname)
        if strategy == "Soft Delete":
            client.command(
                f"ALTER TABLE `{database}`.`{table}` "
                f"UPDATE `{SOFT_DELETE_COLUMN}` = 1 WHERE `name` = '{escaped}'"
            )
        elif strategy == "Hard Delete":
            client.command(
                f"DELETE FROM `{database}`.`{table}` WHERE `name` = '{escaped}'"
            )
    except Exception as exc:  # noqa: BLE001 - never block the Frappe delete
        frappe.logger("clickhouse_sync").warning(
            f"Real-time delete propagation failed for {doctype}/{docname}: {exc}"
        )
