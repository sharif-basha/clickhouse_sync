"""Frappe field type -> ClickHouse column type mapping.

Every Frappe field type listed in the specification is handled here. The
mapping is intentionally conservative: when a Frappe type has no natural
ClickHouse equivalent (rich text, HTML, JSON, geo, ...) it is stored as
``String`` so that no information is lost, and downstream reporting can parse
it as needed.

Nullability: optional fields are wrapped in ``Nullable(...)`` so that empty
Frappe values round-trip as SQL ``NULL`` rather than as type-specific zero
values. Fields that participate in the ``ORDER BY`` / ``PARTITION BY`` keys
(``name`` and ``modified``) are always emitted as non-nullable because
ClickHouse forbids nullable primary-key / partition-key columns in the general
case and it keeps the MergeTree layout efficient.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    "Currency": "Decimal(21, 6)",
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
        "HTML",  # display-only HTML block (see note below)
    }
)

# NB: "HTML" is ambiguous in Frappe - as a *field* it can hold stored markup,
# but the layout element "HTML" is display-only. We treat an HTML field as
# stored ONLY when it carries a fieldname; the schema builder enforces that.

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
        """Return the ``\`col\` Type`` fragment for a CREATE/ALTER statement."""
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

    Mandatory fields are non-nullable; everything else is nullable so that
    unset optional values round-trip as SQL NULL.
    """
    if getattr(df, "fieldname", None) in KEY_COLUMNS:
        return False
    return not int(getattr(df, "reqd", 0) or 0)


def build_column_definition(df: Any) -> ColumnDefinition:
    """Build a :class:`ColumnDefinition` from a Frappe docfield.

    For ``Currency`` fields the docfield's configured precision (if any) is
    used to build a ``Decimal(21, precision)`` type so monetary values keep
    their intended scale in ClickHouse rather than a fixed default.
    """
    fieldname = df.fieldname
    fieldtype = df.fieldtype
    clickhouse_type = map_fieldtype(fieldtype)

    if fieldtype == "Currency":
        precision = _docfield_precision(df)
        if precision is not None:
            clickhouse_type = f"Decimal(21, {precision})"

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
    # Decimal(21, s) requires 0 <= s <= 21; clamp defensively.
    if 0 <= precision <= 18:
        return precision
    return None


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
    "Currency": _coerce_float,
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
