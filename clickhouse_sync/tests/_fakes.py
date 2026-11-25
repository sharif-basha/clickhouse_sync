# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""In-memory stand-ins for Frappe, MariaDB and ClickHouse used by unit tests.

These let the sync engine's real code run end to end without a bench, a
database server or a ClickHouse server:

* :class:`FakeFrappe` replaces the ``frappe`` name inside the engine modules
  (patched per test, so a real Frappe installation is never disturbed).
* :class:`FakeDB` executes the engine's *actual SQL text* against an in-memory
  SQLite database, so pagination and cursor predicates are genuinely evaluated
  rather than imitated.
* :class:`FakeClickHouse` records every call and applies ReplacingMergeTree
  semantics (last write per ``name`` wins) to inserts.

Use :class:`EngineHarness` to wire everything together.
"""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
import sys
import types
from typing import Any, Callable
from unittest import mock

_DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_DATETIME_COLUMNS = {"modified", "creation", "start_time", "end_time", "last_sync_timestamp", "queued_at"}

LOG_TABLE_COLUMNS = [
    "name", "creation", "configuration", "status", "trigger", "start_time", "end_time",
    "duration", "rows_read", "rows_written", "rows_failed", "message", "exception",
    "execution_host",
]
CONFIG_TABLE_COLUMNS = [
    "name", "source_doctype", "enabled", "scheduler_enabled", "sync_mode",
    "delete_strategy", "priority", "last_sync_timestamp",
]


def ensure_frappe_importable() -> None:
    """Make ``import frappe`` succeed when the tests run outside a bench.

    Inside a bench the real package is importable and nothing is changed.
    """
    try:
        import frappe  # noqa: F401
        import frappe.utils  # noqa: F401

        return
    except Exception:  # noqa: BLE001 - not installed: provide the bare minimum
        pass

    frappe_stub = types.ModuleType("frappe")
    utils_stub = types.ModuleType("frappe.utils")
    utils_stub.cint = _cint
    utils_stub.get_datetime = _get_datetime
    utils_stub.now_datetime = dt.datetime.now
    frappe_stub.utils = utils_stub
    def whitelist(*args: Any, **kwargs: Any) -> Any:
        def decorate(fn: Any) -> Any:
            fn.is_whitelisted = True
            fn.allowed_http_methods = kwargs.get("methods")
            return fn

        return decorate

    frappe_stub.whitelist = whitelist
    frappe_stub.only_for = lambda *a, **k: None
    frappe_stub._ = lambda s: s
    sys.modules["frappe"] = frappe_stub
    sys.modules["frappe.utils"] = utils_stub


def _cint(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _get_datetime(value: Any = None) -> Any:
    """Like ``frappe.utils.get_datetime``: accepts any ISO-style string."""
    if isinstance(value, str):
        return dt.datetime.fromisoformat(value)
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return dt.datetime.combine(value, dt.time())
    return value


class Clock:
    """A controllable 'now' so tests decide exactly when things happen."""

    def __init__(self) -> None:
        self.now = dt.datetime(2026, 1, 1, 10, 0, 0)

    def __call__(self) -> dt.datetime:
        return self.now

    def advance(self, seconds: float) -> dt.datetime:
        self.now = self.now + dt.timedelta(seconds=seconds)
        return self.now


class FakeDB:
    """Runs the engine's MariaDB-style SQL on SQLite."""

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.cursors: dict[str, Any] = {}
        self.cursor_history: list[Any] = []
        self.queries: list[str] = []
        self.query_log: list[tuple[str, dict[str, Any]]] = []
        self.before_query: Callable[[str, dict], None] | None = None

    # -- table helpers ----------------------------------------------------- #
    def create_table(self, doctype: str, columns: list[str]) -> None:
        standard = ["name", "owner", "creation", "modified", "modified_by", "docstatus", "idx"]
        columns = standard + [c for c in columns if c not in standard]
        cols = ", ".join(f"`{c}`" for c in columns)
        self.conn.execute(f"CREATE TABLE `tab{doctype}` ({cols})")

    def create_raw_table(self, table: str, columns: list[str]) -> None:
        cols = ", ".join(f"`{c}`" for c in columns)
        self.conn.execute(f"CREATE TABLE `{table}` ({cols})")

    def has_table(self, table: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None

    def insert_raw(self, table: str, **row: Any) -> None:
        cols = ", ".join(f"`{c}`" for c in row)
        marks = ", ".join("?" for _ in row)
        self.conn.execute(
            f"INSERT INTO `{table}` ({cols}) VALUES ({marks})",
            [self._to_db(v) for v in row.values()],
        )

    def upsert(self, doctype: str, **row: Any) -> None:
        self.conn.execute(f"DELETE FROM `tab{doctype}` WHERE name = ?", (row["name"],))
        cols = ", ".join(f"`{c}`" for c in row)
        marks = ", ".join("?" for _ in row)
        self.conn.execute(
            f"INSERT INTO `tab{doctype}` ({cols}) VALUES ({marks})",
            [self._to_db(v) for v in row.values()],
        )

    def delete(self, doctype: str, name: str) -> None:
        self.conn.execute(f"DELETE FROM `tab{doctype}` WHERE name = ?", (name,))

    def rows(self, doctype: str) -> dict[str, dict[str, Any]]:
        cur = self.conn.execute(f"SELECT * FROM `tab{doctype}`")
        return {r["name"]: self._from_db(r) for r in cur.fetchall()}

    # -- frappe.db API ----------------------------------------------------- #
    def sql(self, query: str, params: dict | None = None, as_dict: bool = False) -> list:
        params = dict(params or {})
        if self.before_query:
            self.before_query(query, params)
        self.queries.append(query)
        self.query_log.append((query, params))
        # MariaDB-only statement time limits: SQLite has no equivalent.
        query = re.sub(r"^\s*SET STATEMENT max_statement_time=\d+ FOR\s+", "", query)
        if "information_schema" in query:
            raise sqlite3.OperationalError("information_schema is not available in tests")
        translated = re.sub(r"%\((\w+)\)s", r":\1", query)
        cur = self.conn.execute(translated, {k: self._to_db(v) for k, v in params.items()})
        fetched = cur.fetchall()
        if as_dict:
            return [self._from_db(r) for r in fetched]
        return [tuple(r) for r in fetched]

    def count(self, doctype: str, filters: dict | None = None) -> int:
        where, params = ["1 = 1"], {}
        condition = (filters or {}).get("modified")
        if condition:
            op, value = condition
            if op == "between":
                where.append("modified >= :lo AND modified <= :hi")
                params = {"lo": self._to_db(value[0]), "hi": self._to_db(value[1])}
            else:
                where.append(f"modified {op} :v")
                params = {"v": self._to_db(value)}
        cur = self.conn.execute(
            f"SELECT COUNT(*) FROM `tab{doctype}` WHERE {' AND '.join(where)}", params
        )
        return cur.fetchone()[0]

    def set_value(self, doctype: str, name: str, field: str, value: Any, **kwargs: Any) -> None:
        self.cursors[name] = value
        self.cursor_history.append(value)
        table = f"tab{doctype}"
        if field == "last_sync_timestamp" and self.has_table(table):
            self.conn.execute(
                f"UPDATE `{table}` SET last_sync_timestamp = ? WHERE name = ?",
                (self._to_db(value), name),
            )

    def commit(self) -> None:
        pass

    def get_value(self, *args: Any, **kwargs: Any) -> Any:
        if not args:
            return None
        doctype = args[0]
        filters = args[1] if len(args) > 1 else None
        fieldname = args[2] if len(args) > 2 else kwargs.get("fieldname")
        if doctype != "ClickHouse Sync Configuration" or not fieldname:
            return None
        if isinstance(filters, dict):
            where = " AND ".join(f"`{field}` = ?" for field in filters)
            values = tuple(filters.values())
        else:
            where, values = "`name` = ?", (filters,)
        row = self.conn.execute(
            f"SELECT `{fieldname}` FROM `tabClickHouse Sync Configuration` WHERE {where}",
            values,
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _to_db(value: Any) -> Any:
        if isinstance(value, dt.datetime):
            return value.strftime(_DT_FORMAT)
        return value

    @staticmethod
    def _from_db(row: sqlite3.Row) -> dict[str, Any]:
        out = dict(row)
        for key in _DATETIME_COLUMNS & out.keys():
            if isinstance(out[key], str):
                out[key] = dt.datetime.fromisoformat(out[key])
        return out


class FakeMeta:
    def __init__(self, fields: list[Any], istable: bool = False) -> None:
        self.fields = fields
        self.istable = istable

    def get_valid_columns(self) -> list[str]:
        cols = ["name", "owner", "creation", "modified", "modified_by", "docstatus", "idx"]
        if self.istable:
            cols += ["parent", "parenttype", "parentfield"]
        return cols + [f.fieldname for f in self.fields if f.fieldtype not in ("Table", "Table MultiSelect")]


def docfield(fieldname: str, fieldtype: str = "Data", options: str | None = None, reqd: int = 0) -> Any:
    return types.SimpleNamespace(
        fieldname=fieldname, fieldtype=fieldtype, options=options, reqd=reqd, precision=None
    )


class FakeClickHouse:
    """Records calls; inserts follow ReplacingMergeTree(name) semantics."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, dict[str, Any]]] = {}
        self.commands: list[str] = []
        self.command_calls: list[tuple[str, dict[str, Any]]] = []
        self.insert_log: list[tuple[str, int]] = []
        self.insert_attempts = 0
        self.fail_queries = False
        # Called before each insert with (insert_number, table); raise to fail.
        self.on_insert: Callable[[int, str], None] | None = None

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        pass

    def insert(self, table: str, data: list, column_names: list[str], database: str | None = None) -> None:
        self.insert_attempts += 1  # counts failed attempts too
        if self.on_insert:
            self.on_insert(self.insert_attempts, table)
        self.insert_log.append((table, len(data)))
        target = self.tables.setdefault(table, {})
        key = column_names.index("name")
        for row in data:
            target[row[key]] = dict(zip(column_names, row))

    def command(self, sql: str, parameters: dict | None = None) -> None:
        self.commands.append(sql)
        params = dict(parameters or {})
        self.command_calls.append((sql, params))
        match = re.search(r"CREATE TABLE IF NOT EXISTS `[^`]+`\.`([^`]+)`", sql)
        if match:
            self.tables.setdefault(match.group(1), {})
            return
        match = re.search(r"(?:ALTER TABLE|DELETE FROM) `[^`]+`\.`([^`]+)`", sql)
        if not match:
            return
        table = self.tables.get(match.group(1), {})
        names = params.get("names")
        if names is None:
            names = [params.get("name")]
        parenttype = params.get("source_parenttype")
        if parenttype is not None:
            names = [
                name for name in names
                if name in table and table[name].get("parenttype") == parenttype
            ]
        if "UPDATE `_ch_is_deleted` = 1" in sql:
            for name in names:
                if name in table:
                    table[name]["_ch_is_deleted"] = 1
        else:
            for name in names:
                table.pop(name, None)

    def query(self, sql: str, parameters: dict | None = None, settings: dict | None = None) -> Any:
        if self.fail_queries:
            raise ConnectionRefusedError("connection refused")
        if "system.tables" in sql:
            table = (parameters or {}).get("tbl")
            if table is None:
                return types.SimpleNamespace(result_rows=[[1]])
            return types.SimpleNamespace(result_rows=[[1 if table in self.tables else 0]])
        if "FROM system.parts" in sql:
            return types.SimpleNamespace(
                result_rows=[[t, len(rows), 1000 * len(rows), 1] for t, rows in self.tables.items()]
            )
        if "ORDER BY `name` ASC LIMIT {limit:UInt32}" in sql:
            table = sql.split("`.`")[1].split("`")[0]
            params = parameters or {}
            parenttype = params.get("source_parenttype")
            names = sorted(
                name
                for name, row in self.tables.get(table, {}).items()
                if name > params.get("last_name", "")
                and (parenttype is None or row.get("parenttype") == parenttype)
            )
            names = names[: params.get("limit", len(names))]
            return types.SimpleNamespace(result_rows=[[name] for name in names])
        match = re.search(r"SELECT count\(\) FROM `[^`]+`\.`([^`]+)` FINAL(.*)", sql)
        if match:
            rows = self.tables.get(match.group(1), {}).values()
            if "_ch_is_deleted" in match.group(2):
                rows = [r for r in rows if not r.get("_ch_is_deleted")]
            return types.SimpleNamespace(result_rows=[[len(list(rows))]])
        if "system.columns" in sql:
            return types.SimpleNamespace(result_rows=[])
        if "SELECT DISTINCT `name`" in sql:
            table = sql.split("`.`")[1].split("`")[0]
            return types.SimpleNamespace(result_rows=[[n] for n in self.tables.get(table, {})])
        return types.SimpleNamespace(result_rows=[])


class FakeRedisCache:
    """Mimics Frappe's RedisWrapper, including its per-job local copy.

    ``get_value`` stores what it read in ``local`` (unless ``expires=True``)
    and serves later reads from there; ``set_value`` with ``expires_in_sec``
    writes only to the shared store. This reproduces the stale-read behaviour
    that made mid-run cancellation impossible.
    """

    def __init__(self, shared: dict[str, Any], local: dict[str, Any]) -> None:
        self.shared = shared
        self.local = local

    def make_key(self, key: str, user: Any = None, shared: bool = False) -> str:
        return f"test.site|{key}"

    def set_value(self, key: str, val: Any, user: Any = None, expires_in_sec: Any = None, shared: bool = False) -> None:
        full = self.make_key(key)
        if not expires_in_sec:
            self.local[full] = val
        self.shared[full] = val

    def get_value(self, key: str, generator: Any = None, user: Any = None, expires: bool = False, shared: bool = False) -> Any:
        full = self.make_key(key)
        if full in self.local:
            return self.local[full]
        val = self.shared.get(full)
        if not expires:
            self.local[full] = val
        return val

    def delete_value(self, key: str, user: Any = None, shared: bool = False) -> None:
        full = self.make_key(key)
        self.local.pop(full, None)
        self.shared.pop(full, None)


class FakeFrappe:
    """The subset of the ``frappe`` API the engine modules touch."""

    def __init__(self, db: FakeDB, clock: Clock) -> None:
        self.db = db
        self.metas: dict[str, FakeMeta] = {}
        self.configs: dict[str, Any] = {}
        self.logs: list[Any] = []
        self.clock = clock
        self.enqueued: list[dict[str, Any]] = []
        self.role_checks: list[tuple] = []
        self.deny_roles = False
        self.warnings: list[str] = []
        self._cache: dict[str, Any] = {}
        self.session = types.SimpleNamespace(user="Administrator")
        # ``cache`` is the per-job copy Frappe keeps in front of Redis.
        self.local = types.SimpleNamespace(site="test.site", cache={})
        self.utils = types.SimpleNamespace(
            escape_html=lambda s: s, now_datetime=clock, cint=_cint, get_datetime=_get_datetime
        )

    def get_meta(self, doctype: str) -> FakeMeta:
        return self.metas[doctype]

    def get_doc(self, doctype: str, name: str) -> Any:
        config = self.configs[name]
        # Mirror what a real reload does: the cursor comes from the database.
        config.last_sync_timestamp = self.db.cursors.get(name)
        return config

    def get_all(self, doctype: str, **kwargs: Any) -> list[str]:
        return list(self.db.rows(doctype))

    def new_doc(self, doctype: str) -> Any:
        doc = types.SimpleNamespace(name=f"LOG-{len(self.logs) + 1:04d}")

        def insert(**kwargs: Any) -> None:
            self.logs.append(doc)
            if self.db.has_table("tabClickHouse Sync Log"):
                row = {c: getattr(doc, c, None) for c in LOG_TABLE_COLUMNS}
                row["creation"] = self.clock()
                self.db.insert_raw("tabClickHouse Sync Log", **row)

        doc.insert = insert
        return doc

    def enqueue(self, method: str, **kwargs: Any) -> None:
        self.enqueued.append(kwargs)

    def only_for(self, roles: Any, message: bool = False) -> None:
        self.role_checks.append(tuple(roles) if not isinstance(roles, str) else (roles,))
        if self.deny_roles:
            raise PermissionError("Not permitted")

    def throw(self, message: str, *args: Any, **kwargs: Any) -> None:
        raise ValueError(message)

    def whitelist(self, *args: Any, **kwargs: Any) -> Any:
        def decorate(fn: Any) -> Any:
            fn.is_whitelisted = True
            fn.allowed_http_methods = kwargs.get("methods")
            return fn

        return decorate

    def cache(self) -> "FakeRedisCache":
        return FakeRedisCache(self._cache, self.local.cache)

    def logger(self, *args: Any, **kwargs: Any) -> Any:
        record = lambda message, *a, **k: self.warnings.append(str(message))  # noqa: E731
        return types.SimpleNamespace(info=record, warning=record, error=record, debug=record)

    def publish_realtime(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get_traceback(self) -> str:
        import traceback

        return traceback.format_exc()


class EngineHarness:
    """Wires the fakes into the engine modules for the duration of one test."""

    DOCTYPE = "Doc"

    def __init__(self, testcase: Any, batch_size: int = 2) -> None:
        from clickhouse_sync.api import dashboard
        from clickhouse_sync.clickhouse import alerts, connection, live_status, schema, sync_engine

        self.dashboard = dashboard

        self.engine = sync_engine
        self.clock = Clock()
        self.db = FakeDB()
        self.frappe = FakeFrappe(self.db, self.clock)
        self.clickhouse = FakeClickHouse()
        self.sleeps: list[tuple[int, int]] = []
        self.batch_size = batch_size

        self.db.create_table(self.DOCTYPE, ["name", "modified", "title"])
        self.db.create_raw_table("tabClickHouse Sync Log", LOG_TABLE_COLUMNS)
        self.db.create_raw_table("tabClickHouse Sync Configuration", CONFIG_TABLE_COLUMNS)
        self.frappe.metas[self.DOCTYPE] = FakeMeta([docfield("title")])
        self.job_active: bool | None = None
        self.set_config()

        schema._SCHEMA_CACHE.clear()
        patches = [
            mock.patch.object(sync_engine, "frappe", self.frappe),
            mock.patch.object(schema, "frappe", self.frappe),
            mock.patch.object(alerts, "frappe", self.frappe),
            mock.patch.object(live_status, "frappe", self.frappe),
            mock.patch.object(dashboard, "frappe", self.frappe),
            mock.patch.object(dashboard, "now_datetime", self.clock),
            mock.patch.object(dashboard, "get_datetime", _get_datetime),
            mock.patch.object(dashboard, "cint", _cint),
            mock.patch.object(dashboard, "_", lambda text: text),
            mock.patch.object(dashboard, "_job_active", lambda job_id: self.job_active),
            mock.patch.object(live_status, "now_datetime", self.clock),
            mock.patch.object(live_status, "get_datetime", _get_datetime),
            mock.patch.object(sync_engine, "now_datetime", self.clock),
            mock.patch.object(sync_engine, "get_datetime", _get_datetime),
            mock.patch.object(sync_engine, "cint", _cint),
            # Tests use tiny batches so multi-batch behaviour is visible.
            mock.patch.object(sync_engine, "_clamp_batch_size", lambda value: self.batch_size),
            mock.patch.object(
                sync_engine, "_backoff_sleep", lambda base, attempt: self.sleeps.append((base, attempt))
            ),
            mock.patch.object(connection, "get_client", self.get_client),
            mock.patch.object(
                connection, "get_connection_params", lambda: types.SimpleNamespace(database="wh")
            ),
            mock.patch.object(alerts, "send_run_alert", lambda *a, **k: None),
        ]
        for patch in patches:
            patch.start()
            testcase.addCleanup(patch.stop)

        self.connect_calls: list[bool] = []
        self.connect_error: Callable[[int], Exception | None] | None = None

    # -- configuration ----------------------------------------------------- #
    def set_config(self, **overrides: Any) -> None:
        values = dict(
            name=self.DOCTYPE, source_doctype=self.DOCTYPE, enabled=1, sync_mode="Incremental",
            batch_size=1000, auto_create_table=1, auto_update_schema=1, delete_strategy="Ignore",
            retry_count=3, retry_interval=1, last_sync_timestamp=None, optimize_after_sync=0,
        )
        values.update(overrides)
        config = types.SimpleNamespace(**values)
        config.get = lambda key, default=None: getattr(config, key, default)
        self.frappe.configs[values["name"]] = config
        self.db.conn.execute("DELETE FROM `tabClickHouse Sync Configuration` WHERE name = ?", (values["name"],))
        self.db.insert_raw(
            "tabClickHouse Sync Configuration",
            name=values["name"], source_doctype=values["source_doctype"], enabled=values["enabled"],
            scheduler_enabled=values.get("scheduler_enabled", 1), sync_mode=values["sync_mode"],
            delete_strategy=values["delete_strategy"], priority=values.get("priority", 0),
            last_sync_timestamp=self.db.cursors.get(values["name"]),
        )

    def add_child_table(self, child_doctype: str = "Doc Item") -> str:
        self.db.create_table(child_doctype, ["name", "modified", "parent", "parenttype", "parentfield", "qty"])
        self.frappe.metas[child_doctype] = FakeMeta([docfield("qty", "Int")], istable=True)
        self.frappe.metas[self.DOCTYPE] = FakeMeta(
            [docfield("title"), docfield("items", "Table", options=child_doctype)]
        )
        return child_doctype

    # -- connection -------------------------------------------------------- #
    def get_client(self, force_new: bool = False, retries: int | None = None) -> FakeClickHouse:
        self.connect_calls.append(force_new)
        if self.connect_error:
            error = self.connect_error(len(self.connect_calls))
            if error:
                raise error
        return self.clickhouse

    # -- source data ------------------------------------------------------- #
    def save(self, name: str, title: str = "v1", modified: dt.datetime | None = None) -> None:
        """Insert or update a source document, stamped 'now' unless given."""
        self.db.upsert(self.DOCTYPE, name=name, modified=modified or self.clock.now, title=title)

    def seed(self, count: int, spacing: float = 1.0) -> None:
        for i in range(count):
            self.save(f"DOC-{i:02d}")
            self.clock.advance(spacing)

    # -- running + assertions --------------------------------------------- #
    def run(self) -> Any:
        return self.engine.run_sync(self.DOCTYPE, trigger="Manual")

    @property
    def cursor(self) -> Any:
        return self.db.cursors.get(self.DOCTYPE)

    def warehouse(self, table: str = "tab_doc") -> dict[str, dict[str, Any]]:
        return self.clickhouse.tables.get(table, {})

    def out_of_sync(self) -> list[str]:
        """Source documents missing from, or stale in, the warehouse."""
        target = self.warehouse()
        return sorted(
            name
            for name, row in self.db.rows(self.DOCTYPE).items()
            if name not in target or target[name]["title"] != row["title"]
        )
