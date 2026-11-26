"""Frappe field type -> ClickHouse column type mapping.

Every Frappe field type listed in the specification is handled here. The
mapping is intentionally conservative: when a Frappe type has no natural
ClickHouse equivalent (rich text, HTML, JSON, geo, ...) it is stored as
``String`` so that no information is lost, and downstream reporting can parse
it as needed.

Nullability: every docfield column is ``Nullable(...)`` so that empty Frappe
values round-trip as SQL ``NULL`` rather than as type-specific zero values.
That includes mandatory fields: ``reqd`` is a form rule, not a database
constraint, and older rows (or rows written before a field became mandatory)
regularly hold NULL - a non-nullable column made such a batch fail. Only the
storage-key columns (``name`` and ``modified``) are non-nullable, because
ClickHouse forbids nullable sorting-key columns in the general case.
"""

from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

# --------------------------------------------------------------------------- #
# Base (non-nullable) ClickHouse type for every Frappe field type.
# --------------------------------------------------------------------------- #
FRAPPE_TO_CLICKHOUSE: dict[str, str] = {
    # --- textual -----------------------------------------------------------
    "Data": "String",
    "Small Text": "String",
    "Long Text": "String",
    "Text": "String",
    "Text Editor": "String",
    "Code": "String",
    "HTML": "String",
    "HTML Editor": "String",
    "Markdown": "String",
    "Password": "String",
    "Link": "String",
    "Dynamic Link": "String",
    "Select": "String",
    "Autocomplete": "String",
    "Phone": "String",
    "Email": "String",
    "Color": "String",
    "Barcode": "String",
    "Signature": "String",
    "Geolocation": "String",
    "Attach": "String",
    "Attach Image": "String",
    "Read Only": "String",
    "Icon": "String",
    "JSON": "String",
    # --- numeric -----------------------------------------------------------
    "Int": "Int64",
    "Long Int": "Int64",
    "Check": "UInt8",
    # Frappe stores Currency as decimal(21, 9); keep the same scale so no
    # digits are lost (see CURRENCY_SCALE / build_column_definition).
    "Currency": "Decimal(21, 9)",
    "Float": "Float64",
    "Percent": "Float64",
    "Rating": "Float32",
    "Duration": "Float64",  # stored as seconds
    # --- temporal ----------------------------------------------------------
    "Date": "Date32",
    "Datetime": "DateTime64(6)",
    "Time": "String",  # ClickHouse has no standalone time type
    # --- structural (materialised as strings) ------------------------------
    "Table": "String",
    "Table MultiSelect": "String",
    "Image": "String",
}

# Frappe field types that never produce a stored column (purely presentational
# or layout elements). They are skipped entirely during schema generation.
NON_STORED_FIELDTYPES: frozenset[str] = frozenset(
    {
        "Section Break",
        "Column Break",
        "Tab Break",
        "Fold",
        "Heading",
        "Button",
        "HTML",  # display-only HTML block - Frappe never stores it
        "Image",  # displays another field's attachment; no column of its own
        # Child-table fields live in their own tables, not as a column here.
        "Table",
        "Table MultiSelect",
    }
)

# Columns that are always present on every synced table and always take part
# in the storage key. Order matters for ORDER BY.
KEY_COLUMNS: tuple[str, ...] = ("name", "modified")

DEFAULT_CLICKHOUSE_TYPE = "String"


@dataclass(frozen=True)
class ColumnDefinition:
    """A single resolved ClickHouse column."""

    name: str
    clickhouse_type: str
    frappe_fieldtype: str
    nullable: bool

    @property
    def ddl(self) -> str:
        """Return the ```col` Type`` fragment for a CREATE/ALTER statement."""
        rendered = self.clickhouse_type
        if self.nullable and not rendered.startswith("Nullable("):
            rendered = f"Nullable({rendered})"
        return f"`{self.name}` {rendered}"


def map_fieldtype(fieldtype: str) -> str:
    """Return the base (non-nullable) ClickHouse type for a Frappe field type.

    Unknown field types degrade gracefully to ``String`` so that a schema can
    always be produced even for custom field types shipped by other apps.
    """
    return FRAPPE_TO_CLICKHOUSE.get(fieldtype, DEFAULT_CLICKHOUSE_TYPE)


def is_stored_field(df: Any) -> bool:
    """Return ``True`` if a docfield should become a ClickHouse column.

    Args:
        df: A Frappe docfield-like object exposing ``fieldtype`` and
            ``fieldname`` attributes.
    """
    fieldtype = getattr(df, "fieldtype", None)
    fieldname = getattr(df, "fieldname", None)
    if not fieldtype or not fieldname:
        return False
    if fieldtype in NON_STORED_FIELDTYPES:
        return False
    return True


def _is_nullable(df: Any) -> bool:
    """Decide whether a field's column should be ``Nullable``.

    Only the storage-key columns are non-nullable. Mandatory (``reqd``) fields
    are nullable too: the flag is enforced by forms, not by MariaDB, so source
    rows can and do hold NULL in them.
    """
    return getattr(df, "fieldname", None) not in KEY_COLUMNS


def build_column_definition(df: Any) -> ColumnDefinition:
    """Build a :class:`ColumnDefinition` from a Frappe docfield.

    For ``Currency`` fields the docfield's configured precision (if any) is
    used as the Decimal scale, and the precision is widened so the column
    always keeps at least :data:`CURRENCY_INTEGER_DIGITS` digits before the
    decimal point (``Decimal(21, 18)`` would otherwise overflow at 1,000).
    """
    fieldname = df.fieldname
    fieldtype = df.fieldtype
    clickhouse_type = map_fieldtype(fieldtype)

    if fieldtype == "Currency":
        precision = _docfield_precision(df)
        if precision is not None:
            clickhouse_type = currency_type(precision)

    return ColumnDefinition(
        name=fieldname,
        clickhouse_type=clickhouse_type,
        frappe_fieldtype=fieldtype,
        nullable=_is_nullable(df),
    )


def _docfield_precision(df: Any) -> int | None:
    """Return a valid Decimal scale from a docfield's precision, or ``None``.

    Frappe stores precision as a string like ``"2"``; invalid or out-of-range
    values fall back to ``None`` so the default mapping is used.
    """
    raw = getattr(df, "precision", None)
    if raw in (None, ""):
        return None
    try:
        precision = int(raw)
    except (TypeError, ValueError):
        return None
    if 0 <= precision <= 18:
        return precision
    return None


# Digits Frappe keeps before the decimal point in a decimal(21, 9) column.
CURRENCY_INTEGER_DIGITS = 12


def currency_type(scale: int) -> str:
    """ClickHouse Decimal type for a Currency field with ``scale`` decimals."""
    return f"Decimal({max(21, CURRENCY_INTEGER_DIGITS + scale)}, {scale})"


# --------------------------------------------------------------------------- #
# Value coercion: Frappe/Python value -> value acceptable to clickhouse-connect
# --------------------------------------------------------------------------- #
def _coerce_check(value: Any) -> int:
    return 1 if value in (1, "1", True, "true", "True") else 0


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_decimal(value: Any) -> Decimal | None:
    """Keep money exact: MariaDB returns ``Decimal``; never go through float."""
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    try:
        # str() first: Decimal(0.1) would carry the binary float error.
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, default=str)
    return str(value)


def _coerce_passthrough(value: Any) -> Any:
    # Date/Datetime values are handled natively by clickhouse-connect when
    # passed as datetime.date / datetime.datetime; empty strings become NULL.
    if value in (None, ""):
        return None
    return value


# Per-fieldtype coercion callables. Anything not listed falls back to string.
_COERCERS: dict[str, Callable[[Any], Any]] = {
    "Check": _coerce_check,
    "Int": _coerce_int,
    "Long Int": _coerce_int,
    "Currency": _coerce_decimal,
    "Float": _coerce_float,
    "Percent": _coerce_float,
    "Rating": _coerce_float,
    "Duration": _coerce_float,
    "Date": _coerce_passthrough,
    "Datetime": _coerce_passthrough,
}


def coerce_value(fieldtype: str, value: Any) -> Any:
    """Coerce a raw Frappe value into a ClickHouse-insertable Python value.

    Args:
        fieldtype: The Frappe field type driving the coercion.
        value: The raw value read from MariaDB.

    Returns:
        A value acceptable to ``clickhouse-connect``'s column-oriented insert.
    """
    coercer = _COERCERS.get(fieldtype, _coerce_str)
    return coercer(value)


# --------------------------------------------------------------------------- #
# Live-column awareness
# --------------------------------------------------------------------------- #
# When a field's type changes in Frappe in a way the schema layer cannot
# migrate (e.g. Data -> Int on an existing String column), the live column
# keeps its old type. Values must then be converted for the column that
# actually exists, or every insert would fail. These helpers derive the
# conversion and the "empty" value from a live ClickHouse type string.
_LOW_CARDINALITY_RE = re.compile(r"^LowCardinality\((.*)\)$")


def _strip_wrappers(clickhouse_type: str) -> tuple[str, bool]:
    """Return ``(base_type, nullable)`` for a live ClickHouse type string."""
    t = clickhouse_type.strip()
    nullable = False
    changed = True
    while changed:
        changed = False
        match = _LOW_CARDINALITY_RE.match(t)
        if match:
            t, changed = match.group(1).strip(), True
        if t.startswith("Nullable(") and t.endswith(")"):
            t, nullable, changed = t[len("Nullable("):-1].strip(), True, True
    return t, nullable


def fieldtype_for_clickhouse_type(clickhouse_type: str) -> str:
    """Return the Frappe field type whose coercion suits a live column type."""
    base, _nullable = _strip_wrappers(clickhouse_type)
    if base.startswith("Decimal"):
        return "Currency"
    if base == "UInt8":
        return "Check"
    if re.match(r"^U?Int\d+$", base):
        return "Int"
    if base.startswith("Float"):
        return "Float"
    if base.startswith("DateTime"):
        return "Datetime"
    if base.startswith("Date"):
        return "Date"
    return "Data"


def is_nullable_type(clickhouse_type: str) -> bool:
    """Return ``True`` if a live column accepts NULL."""
    return _strip_wrappers(clickhouse_type)[1]


def empty_value_for(clickhouse_type: str) -> Any:
    """The value to write for NULL into a *non-nullable* live column.

    ClickHouse itself uses these defaults for a missing value. Writing them is
    better than failing the whole batch on a column created by an older
    version of this app (which made mandatory fields non-nullable).
    """
    base, _nullable = _strip_wrappers(clickhouse_type)
    if base.startswith("Decimal"):
        return Decimal(0)
    if re.match(r"^U?Int\d+$", base):
        return 0
    if base.startswith("Float"):
        return 0.0
    if base.startswith("DateTime"):
        return _dt.datetime(1970, 1, 1)
    if base.startswith("Date"):
        return _dt.date(1970, 1, 1)
    return ""
