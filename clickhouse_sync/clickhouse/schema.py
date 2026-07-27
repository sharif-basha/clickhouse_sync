"""ClickHouse schema generation and reconciliation.

Responsibilities:

* Read a Frappe DocType's metadata and translate it into a set of
  :class:`~clickhouse_sync.clickhouse.type_mapping.ColumnDefinition` objects.
* Generate a ``CREATE TABLE`` statement using the ``MergeTree`` engine,
  ``ORDER BY name`` and ``PARTITION BY toYYYYMM(modified)`` (configurable
  engine).
* Compare the desired schema against the live ClickHouse table and reconcile:
  add missing columns, widen compatible types, and ignore unsupported
  changes rather than failing the whole sync.
* Maintain a per-table metadata cache so repeated syncs skip redundant
  ``DESCRIBE`` round-trips.

The reconciliation is intentionally *additive and non-destructive*: columns
are never dropped and incompatible narrowing is refused, because ClickHouse
column mutations are expensive and destructive changes should be an explicit
operator decision.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import frappe

from clickhouse_sync.clickhouse.exceptions import (
    ClickHouseQueryError,
    ClickHouseSchemaError,
)
from clickhouse_sync.clickhouse.type_mapping import (
    KEY_COLUMNS,
    ColumnDefinition,
    build_column_definition,
    is_stored_field,
    map_fieldtype,
)

if TYPE_CHECKING:  # pragma: no cover
    from clickhouse_connect.driver.client import Client


# Metadata columns that Frappe stores on every document. They are added
# explicitly so the ClickHouse table always carries them even if they are not
# present as docfields.
STANDARD_DOC_COLUMNS: tuple[tuple[str, str, str], ...] = (
    # (fieldname, frappe_fieldtype, clickhouse_base_type)
    ("name", "Data", "String"),
    ("owner", "Data", "String"),
    ("creation", "Datetime", "DateTime64(6)"),
    ("modified", "Datetime", "DateTime64(6)"),
    ("modified_by", "Data", "String"),
    ("docstatus", "Int", "Int8"),
    ("idx", "Int", "Int64"),
    ("_user_tags", "Data", "String"),
    ("_comments", "Long Text", "String"),
    ("_assign", "Data", "String"),
    ("_liked_by", "Data", "String"),
)

# Extra framework columns present on child-table records. They let a synced
# child table be joined back to its parent document in ClickHouse.
CHILD_LINK_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("parent", "Data", "String"),
    ("parenttype", "Data", "String"),
    ("parentfield", "Data", "String"),
)

# Column appended when the delete strategy is "Soft Delete".
SOFT_DELETE_COLUMN = "_ch_is_deleted"
SYNC_TIMESTAMP_COLUMN = "_ch_synced_at"

# Rank of ClickHouse numeric families for compatible widening decisions.
# A migration is only allowed when the target rank >= source rank *within*
# the same family (int->int, float->float, string is universal sink).
_WIDENING_RANK: dict[str, int] = {
    "Int8": 1,
    "Int16": 2,
    "Int32": 3,
    "Int64": 4,
    "UInt8": 1,
    "UInt16": 2,
    "UInt32": 3,
    "UInt64": 4,
    "Float32": 5,
    "Float64": 6,
}

# Process-local metadata cache: table name -> (timestamp, live column map).
_SCHEMA_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
_SCHEMA_CACHE_TTL = 300  # seconds


# Default engine. ReplacingMergeTree deduplicates rows that share the same
# ORDER BY key during background merges, keeping the row with the greatest
# value of the version column (here, _ch_synced_at). This makes a Full sync
# idempotent: re-running it replaces each row's prior copy instead of stacking
# duplicates. Queries can add FINAL to force deduplication at read time.
DEFAULT_ENGINE = "ReplacingMergeTree"


@dataclass
class TableSchema:
    """The desired ClickHouse schema for one Frappe DocType."""

    doctype: str
    table_name: str
    columns: list[ColumnDefinition] = field(default_factory=list)
    engine: str = DEFAULT_ENGINE
    order_by: tuple[str, ...] = KEY_COLUMNS
    partition_by: str = "toYYYYMM(modified)"
    # Version column used by ReplacingMergeTree to pick the surviving row.
    version_column: str = SYNC_TIMESTAMP_COLUMN

    @property
    def engine_clause(self) -> str:
        """Render the ENGINE clause, wiring the version column for Replacing*."""
        if self.engine.startswith("ReplacingMergeTree"):
            return f"ReplacingMergeTree(`{self.version_column}`)"
        return self.engine

    @property
    def column_map(self) -> dict[str, ColumnDefinition]:
        """Return a ``{name: ColumnDefinition}`` view of the columns."""
        return {c.name: c for c in self.columns}


def table_name_for(doctype: str) -> str:
    """Return the ClickHouse table name for a DocType.

    Mirrors Frappe's own convention (``tab<DocType>``) but lower-cased and
    space-stripped, which keeps ClickHouse identifiers tidy and predictable.
    """
    normalized = doctype.strip().lower().replace(" ", "_").replace("-", "_")
    return f"tab_{normalized}"


def get_child_doctypes(parent_doctype: str) -> list[tuple[str, str]]:
    """Return the child DocTypes referenced by a parent's Table fields.

    Frappe grid rows (e.g. an invoice's line items) live in separate
    ``tab<Child>`` tables linked by ``parent``/``parenttype``/``parentfield``.
    This returns ``[(child_doctype, parentfield), ...]`` for every
    ``Table`` / ``Table MultiSelect`` field on the parent, de-duplicated.

    Returns:
        A list of ``(child_doctype, parentfield)`` tuples. Empty when the
        parent has no child tables.
    """
    meta = frappe.get_meta(parent_doctype)
    children: list[tuple[str, str]] = []
    seen: set[str] = set()
    for df in meta.fields:
        if df.fieldtype in ("Table", "Table MultiSelect") and df.options:
            key = f"{df.options}:{df.fieldname}"
            if key in seen:
                continue
            seen.add(key)
            children.append((df.options, df.fieldname))
    return children


# Framework columns that must never be excluded - they are the storage key,
# the version column source, or required for delete reconciliation.
PROTECTED_COLUMNS: frozenset[str] = frozenset({"name", "modified", "creation"})

# Fields that are ALWAYS excluded from every table, regardless of per-config
# settings, as a safety net so credentials cannot leak into the reporting
# store if an operator forgets to list them. Matching is by exact fieldname
# and by suffix for the *_secret / *_password / *_token families (see
# _is_blocked_field). This list is deliberately conservative - it targets
# secrets and auth material, not merely "personal" data (which is the
# operator's per-config decision via exclude_fields).
CREDENTIAL_BLOCKLIST: frozenset[str] = frozenset(
    {
        "password",
        "new_password",
        "api_key",
        "api_secret",
        "secret",
        "access_token",
        "refresh_token",
        "reset_password_key",
        "encryption_key",
        "webhook_secret",
        "private_key",
        "ssh_key",
        "salt",
        "otp_secret",
    }
)

# Suffixes that mark a field as secret-bearing regardless of its exact name.
_BLOCKED_SUFFIXES: tuple[str, ...] = ("_password", "_secret", "_token", "_api_key", "_private_key")


def _is_blocked_field(fieldname: str) -> bool:
    """Return ``True`` if a field is always excluded as a credential.

    Protected framework columns are never blocked (they cannot contain
    secrets and are required by the engine).
    """
    if fieldname in PROTECTED_COLUMNS:
        return False
    if fieldname in CREDENTIAL_BLOCKLIST:
        return True
    return fieldname.endswith(_BLOCKED_SUFFIXES)


def build_table_schema(
    doctype: str,
    *,
    engine: str = DEFAULT_ENGINE,
    include_soft_delete: bool = False,
    exclude_fields: "set[str] | frozenset[str] | None" = None,
    is_child: bool = False,
) -> TableSchema:
    """Build the desired :class:`TableSchema` from a DocType's metadata.

    Args:
        doctype: Name of the source Frappe DocType.
        engine: ClickHouse table engine (defaults to ``ReplacingMergeTree``,
            which makes full syncs idempotent).
        include_soft_delete: When ``True`` the soft-delete marker column is
            appended to the schema.
        exclude_fields: Field names to omit from the table entirely. Used to
            keep sensitive columns (bank details, government IDs, salary,
            health data, etc.) out of the reporting store. Protected framework
            columns (name/modified/creation) are never excluded even if listed.
            Credential fields (see :data:`CREDENTIAL_BLOCKLIST`) are ALWAYS
            excluded regardless of this argument.
        is_child: When ``True`` the parent/parenttype/parentfield link columns
            are added so the synced child table can be joined to its parent.

    Returns:
        A populated :class:`TableSchema`.
    """
    meta = frappe.get_meta(doctype)
    columns: list[ColumnDefinition] = []
    seen: set[str] = set()

    # Normalise the exclusion set; protected columns can never be dropped.
    excluded = {f.strip() for f in (exclude_fields or set()) if f and f.strip()}
    excluded -= PROTECTED_COLUMNS

    standard = STANDARD_DOC_COLUMNS + (CHILD_LINK_COLUMNS if is_child else ())

    # 1. Standard framework columns first (they own the storage key).
    for fieldname, frappe_type, ch_type in standard:
        if fieldname in excluded or _is_blocked_field(fieldname):
            continue
        nullable = fieldname not in KEY_COLUMNS
        columns.append(
            ColumnDefinition(
                name=fieldname,
                clickhouse_type=ch_type,
                frappe_fieldtype=frappe_type,
                nullable=nullable,
            )
        )
        seen.add(fieldname)

    # 2. User-defined docfields (excluded fields are skipped entirely, so they
    #    are never even read from MariaDB by the streaming reader).
    for df in meta.fields:
        if not is_stored_field(df):
            continue
        if df.fieldname in seen or df.fieldname in excluded:
            continue
        if _is_blocked_field(df.fieldname):
            continue
        # Password fieldtype always holds secret material - never mirror it.
        if getattr(df, "fieldtype", None) == "Password":
            continue
        columns.append(build_column_definition(df))
        seen.add(df.fieldname)

    # 3. Engine-managed markers.
    columns.append(
        ColumnDefinition(
            name=SYNC_TIMESTAMP_COLUMN,
            clickhouse_type="DateTime64(3)",
            frappe_fieldtype="Datetime",
            nullable=False,
        )
    )
    if include_soft_delete:
        columns.append(
            ColumnDefinition(
                name=SOFT_DELETE_COLUMN,
                clickhouse_type="UInt8",
                frappe_fieldtype="Check",
                nullable=False,
            )
        )

    resolved_engine = engine or DEFAULT_ENGINE

    # For ReplacingMergeTree the ORDER BY key IS the dedup key, so it must be
    # exactly `name` (the immutable document identity) - not (name, modified),
    # which would treat every edit as a distinct row and defeat deduplication.
    #
    # Partitioning is deliberately disabled (tuple() = single partition) for the
    # deduplicating engine: ReplacingMergeTree only collapses duplicates WITHIN a
    # partition, so any partition key derived from a mutable column (like
    # `modified`) would let an edited row leave a ghost copy behind in its old
    # partition. A single partition guarantees global deduplication, which is
    # what a faithful mirror of the source table needs. Very large tables can be
    # re-partitioned on an immutable column later as a performance optimisation.
    # Plain MergeTree keeps the original composite key and month partitioning.
    if resolved_engine.startswith("ReplacingMergeTree"):
        order_by: tuple[str, ...] = ("name",)
        partition_by = "tuple()"
    else:
        order_by = KEY_COLUMNS
        partition_by = "toYYYYMM(modified)"

    return TableSchema(
        doctype=doctype,
        table_name=table_name_for(doctype),
        columns=columns,
        engine=resolved_engine,
        order_by=order_by,
        partition_by=partition_by,
    )


def generate_create_table_sql(schema: TableSchema, database: str) -> str:
    """Render a ``CREATE TABLE IF NOT EXISTS`` statement for ``schema``."""
    column_ddl = ",\n    ".join(col.ddl for col in schema.columns)
    order_by = ", ".join(f"`{c}`" for c in schema.order_by)
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{schema.table_name}`\n"
        f"(\n    {column_ddl}\n)\n"
        f"ENGINE = {schema.engine_clause}\n"
        f"PARTITION BY {schema.partition_by}\n"
        f"ORDER BY ({order_by})\n"
        f"SETTINGS index_granularity = 8192"
    )
    return ddl


def _live_columns(client: "Client", database: str, table_name: str) -> dict[str, str]:
    """Return the live ``{column: type}`` map for a table, using the cache."""
    cache_key = f"{database}.{table_name}"
    cached = _SCHEMA_CACHE.get(cache_key)
    now = time.time()
    if cached and (now - cached[0]) < _SCHEMA_CACHE_TTL:
        return cached[1]

    try:
        result = client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String}",
            parameters={"db": database, "tbl": table_name},
        )
    except Exception as exc:  # noqa: BLE001
        raise ClickHouseQueryError(f"Failed to read live schema for {cache_key}: {exc}") from exc

    columns = {row[0]: row[1] for row in result.result_rows}
    _SCHEMA_CACHE[cache_key] = (now, columns)
    return columns


def invalidate_schema_cache(table_name: str | None = None) -> None:
    """Drop cached schema metadata (all tables, or a single one)."""
    if table_name is None:
        _SCHEMA_CACHE.clear()
        return
    for key in [k for k in _SCHEMA_CACHE if k.endswith(f".{table_name}")]:
        _SCHEMA_CACHE.pop(key, None)


def table_exists(client: "Client", database: str, table_name: str) -> bool:
    """Return ``True`` if the physical table already exists in ClickHouse."""
    result = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": database, "tbl": table_name},
    )
    return bool(result.result_rows and result.result_rows[0][0])


def ensure_table(
    client: "Client",
    schema: TableSchema,
    database: str,
    *,
    auto_create: bool,
) -> bool:
    """Create the target table when missing (if ``auto_create``).

    Returns:
        ``True`` if a table was created, ``False`` if it already existed.

    Raises:
        ClickHouseSchemaError: If the table is missing and ``auto_create`` is
            ``False``.
    """
    if table_exists(client, database, schema.table_name):
        return False

    if not auto_create:
        raise ClickHouseSchemaError(
            f"Table `{database}`.`{schema.table_name}` does not exist and "
            f"'Auto Create Table' is disabled for {schema.doctype}."
        )

    ddl = generate_create_table_sql(schema, database)
    try:
        client.command(ddl)
    except Exception as exc:  # noqa: BLE001
        raise ClickHouseQueryError(
            f"CREATE TABLE failed for {schema.table_name}: {exc}\nSQL:\n{ddl}"
        ) from exc

    invalidate_schema_cache(schema.table_name)
    frappe.logger("clickhouse_sync").info(f"Created ClickHouse table {schema.table_name}")
    return True


def _base_type(clickhouse_type: str) -> str:
    """Strip a ``Nullable(...)`` wrapper to get the underlying base type."""
    t = clickhouse_type.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[len("Nullable(") : -1]
    return t


def _can_widen(current: str, desired: str) -> bool:
    """Return ``True`` if migrating ``current`` -> ``desired`` is a safe widen.

    Widening to ``String`` is always allowed (universal sink). Numeric
    widening is allowed within the same broad family and only upward in rank.
    """
    cur = _base_type(current)
    des = _base_type(desired)
    if cur == des:
        return False  # nothing to do
    if des == "String":
        return True
    cur_rank = _WIDENING_RANK.get(cur)
    des_rank = _WIDENING_RANK.get(des)
    if cur_rank is not None and des_rank is not None:
        return des_rank >= cur_rank
    return False


@dataclass
class SchemaDiff:
    """Result of comparing the desired schema against the live table."""

    added: list[ColumnDefinition] = field(default_factory=list)
    widened: list[tuple[str, str, str]] = field(default_factory=list)  # (col, from, to)
    ignored: list[tuple[str, str, str]] = field(default_factory=list)  # incompatible

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.widened)


def diff_schema(
    client: "Client",
    schema: TableSchema,
    database: str,
) -> SchemaDiff:
    """Compute the additive/widening diff between desired and live schema."""
    live = _live_columns(client, database, schema.table_name)
    diff = SchemaDiff()

    for col in schema.columns:
        live_type = live.get(col.name)
        if live_type is None:
            diff.added.append(col)
            continue

        desired_type = col.ddl.split(" ", 1)[1]  # strip the `name` prefix
        if _base_type(live_type) == _base_type(desired_type):
            continue
        if _can_widen(live_type, desired_type):
            diff.widened.append((col.name, live_type, desired_type))
        else:
            diff.ignored.append((col.name, live_type, desired_type))

    return diff


def reconcile_schema(
    client: "Client",
    schema: TableSchema,
    database: str,
    *,
    auto_update: bool,
) -> SchemaDiff:
    """Reconcile the live table with the desired schema.

    Adds missing columns and widens compatible types when ``auto_update`` is
    enabled. Incompatible changes are recorded in :attr:`SchemaDiff.ignored`
    and logged, but never raise - the sync proceeds with the columns that do
    line up.
    """
    diff = diff_schema(client, schema, database)
    if not auto_update or not diff.has_changes:
        _log_ignored(schema, diff)
        return diff

    logger = frappe.logger("clickhouse_sync")

    for col in diff.added:
        alter = (
            f"ALTER TABLE `{database}`.`{schema.table_name}` "
            f"ADD COLUMN IF NOT EXISTS {col.ddl}"
        )
        try:
            client.command(alter)
            logger.info(f"Added column {schema.table_name}.{col.name}")
        except Exception as exc:  # noqa: BLE001
            raise ClickHouseQueryError(
                f"ADD COLUMN failed for {schema.table_name}.{col.name}: {exc}"
            ) from exc

    for col_name, _from, to_type in diff.widened:
        alter = (
            f"ALTER TABLE `{database}`.`{schema.table_name}` "
            f"MODIFY COLUMN `{col_name}` {_base_type(to_type)}"
        )
        try:
            client.command(alter)
            logger.info(f"Widened column {schema.table_name}.{col_name} -> {to_type}")
        except Exception as exc:  # noqa: BLE001
            # A failed widen is non-fatal: log and continue.
            logger.warning(f"MODIFY COLUMN failed for {col_name}: {exc}")

    _log_ignored(schema, diff)
    invalidate_schema_cache(schema.table_name)
    return diff


def _log_ignored(schema: TableSchema, diff: SchemaDiff) -> None:
    """Emit a warning for each schema change the engine chose to ignore."""
    if not diff.ignored:
        return
    logger = frappe.logger("clickhouse_sync")
    for col_name, from_type, to_type in diff.ignored:
        logger.warning(
            f"Ignoring incompatible schema change for "
            f"{schema.table_name}.{col_name}: {from_type} !-> {to_type}"
        )
