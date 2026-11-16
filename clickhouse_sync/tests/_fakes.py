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
_DATETIME_COLUMNS = {"modified", "creation"}


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
    frappe_stub.whitelist = lambda *a, **k: (lambda fn: fn)
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
    if isinstance(value, str):
        return dt.datetime.strptime(value, _DT_FORMAT)
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
        self.before_query: Callable[[str, dict], None] | None = None

    # -- table helpers ----------------------------------------------------- #
    def create_table(self, doctype: str, columns: list[str]) -> None:
        standard = ["name", "owner", "creation", "modified", "modified_by", "docstatus", "idx"]
        columns = standard + [c for c in columns if c not in standard]
        cols = ", ".join(f"`{c}`" for c in columns)
        self.conn.execute(f"CREATE TABLE `tab{doctype}` ({cols})")

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

    def commit(self) -> None:
        pass

    def get_value(self, *args: Any, **kwargs: Any) -> None:
        return None

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
                out[key] = dt.datetime.strptime(out[key], _DT_FORMAT)
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
        self.insert_log: list[tuple[str, int]] = []
        self.insert_attempts = 0
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

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str, parameters: dict | None = None) -> Any:
        if "system.tables" in sql:
            return types.SimpleNamespace(result_rows=[[1]])
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
        doc = types.SimpleNamespace(name=f"LOG-{len(self.logs) + 1}")
        doc.insert = lambda **kwargs: self.logs.append(doc)
        return doc

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
        from clickhouse_sync.clickhouse import alerts, connection, schema, sync_engine

        self.engine = sync_engine
        self.clock = Clock()
        self.db = FakeDB()
        self.frappe = FakeFrappe(self.db, self.clock)
        self.clickhouse = FakeClickHouse()
        self.sleeps: list[tuple[int, int]] = []
        self.batch_size = batch_size

        self.db.create_table(self.DOCTYPE, ["name", "modified", "title"])
        self.frappe.metas[self.DOCTYPE] = FakeMeta([docfield("title")])
        self.set_config()

        schema._SCHEMA_CACHE.clear()
        patches = [
            mock.patch.object(sync_engine, "frappe", self.frappe),
            mock.patch.object(schema, "frappe", self.frappe),
            mock.patch.object(alerts, "frappe", self.frappe),
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
        self.frappe.configs[self.DOCTYPE] = config

    def add_child_table(self, child_doctype: str = "Doc Item") -> str:
        self.db.create_table(child_doctype, ["name", "modified", "parent", "parenttype", "parentfield", "qty"])
        self.frappe.metas[child_doctype] = FakeMeta([docfield("qty", "Int")], istable=True)
        self.frappe.metas[self.DOCTYPE] = FakeMeta(
            [docfield("title"), docfield("items", "Table", options=child_doctype)]
        )
        return child_doctype

    # -- connection -------------------------------------------------------- #
    def get_client(self, force_new: bool = False) -> FakeClickHouse:
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
