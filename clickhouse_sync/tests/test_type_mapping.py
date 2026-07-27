# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Unit tests for the Frappe -> ClickHouse type mapping."""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from clickhouse_sync.clickhouse import type_mapping as tm


class TestTypeMapping(unittest.TestCase):
    """Validate field-type mapping, nullability and value coercion."""

    def test_every_spec_fieldtype_maps(self) -> None:
        spec_types = [
            "Data", "Small Text", "Long Text", "Text Editor", "Link",
            "Dynamic Link", "Password", "Date", "Datetime", "Time",
            "Currency", "Float", "Percent", "Int", "Check", "JSON", "Table",
            "Attach", "Attach Image", "Signature", "Code", "Duration",
            "Rating", "Color", "Select", "Phone", "Email", "Geolocation",
            "Markdown", "Autocomplete", "Barcode", "Read Only", "Icon",
        ]
        for ft in spec_types:
            with self.subTest(fieldtype=ft):
                self.assertTrue(tm.map_fieldtype(ft))

    def test_unknown_fieldtype_defaults_to_string(self) -> None:
        self.assertEqual(tm.map_fieldtype("SomethingCustom"), "String")

    def test_check_maps_to_uint8(self) -> None:
        self.assertEqual(tm.map_fieldtype("Check"), "UInt8")

    def test_numeric_mappings(self) -> None:
        self.assertEqual(tm.map_fieldtype("Int"), "Int64")
        self.assertEqual(tm.map_fieldtype("Float"), "Float64")
        self.assertEqual(tm.map_fieldtype("Currency"), "Decimal(21, 6)")

    def test_non_stored_fieldtypes_skipped(self) -> None:
        df = SimpleNamespace(fieldtype="Section Break", fieldname=None)
        self.assertFalse(tm.is_stored_field(df))
        df2 = SimpleNamespace(fieldtype="Button", fieldname="do_it")
        self.assertFalse(tm.is_stored_field(df2))

    def test_stored_field_true(self) -> None:
        df = SimpleNamespace(fieldtype="Data", fieldname="title")
        self.assertTrue(tm.is_stored_field(df))

    def test_nullable_wrapping(self) -> None:
        df = SimpleNamespace(fieldtype="Data", fieldname="title", reqd=0)
        col = tm.build_column_definition(df)
        self.assertTrue(col.nullable)
        self.assertIn("Nullable(String)", col.ddl)

    def test_mandatory_field_not_nullable(self) -> None:
        df = SimpleNamespace(fieldtype="Data", fieldname="title", reqd=1)
        col = tm.build_column_definition(df)
        self.assertFalse(col.nullable)
        self.assertNotIn("Nullable", col.ddl)

    def test_key_columns_never_nullable(self) -> None:
        df = SimpleNamespace(fieldtype="Data", fieldname="name", reqd=0)
        col = tm.build_column_definition(df)
        self.assertFalse(col.nullable)

    def test_coerce_check(self) -> None:
        self.assertEqual(tm.coerce_value("Check", "1"), 1)
        self.assertEqual(tm.coerce_value("Check", 0), 0)
        self.assertEqual(tm.coerce_value("Check", True), 1)

    def test_coerce_int_handles_blank(self) -> None:
        self.assertIsNone(tm.coerce_value("Int", ""))
        self.assertEqual(tm.coerce_value("Int", "42"), 42)

    def test_coerce_float(self) -> None:
        self.assertEqual(tm.coerce_value("Currency", "3.50"), 3.5)
        self.assertIsNone(tm.coerce_value("Float", ""))

    def test_coerce_json_value_serialised(self) -> None:
        out = tm.coerce_value("JSON", {"a": 1})
        self.assertIn('"a"', out)


    def test_currency_uses_docfield_precision(self) -> None:
        df = SimpleNamespace(fieldtype="Currency", fieldname="amount", reqd=0, precision="2")
        col = tm.build_column_definition(df)
        self.assertIn("Decimal(21, 2)", col.ddl)

    def test_currency_default_precision_when_unset(self) -> None:
        df = SimpleNamespace(fieldtype="Currency", fieldname="amount", reqd=0, precision="")
        col = tm.build_column_definition(df)
        self.assertIn("Decimal(21, 6)", col.ddl)

    def test_currency_invalid_precision_falls_back(self) -> None:
        df = SimpleNamespace(fieldtype="Currency", fieldname="amount", reqd=0, precision="xx")
        col = tm.build_column_definition(df)
        self.assertIn("Decimal(21, 6)", col.ddl)


if __name__ == "__main__":
    unittest.main()
