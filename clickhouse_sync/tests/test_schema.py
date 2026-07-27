# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Unit tests for DDL generation and widening logic.

These tests exercise the pure-logic parts of the schema module that do not
require a live ClickHouse server or a Frappe site.
"""

from __future__ import annotations

import unittest

from clickhouse_sync.clickhouse import schema as sch
from clickhouse_sync.clickhouse.type_mapping import ColumnDefinition


class TestSchemaDDL(unittest.TestCase):
    def _sample_schema(self, engine: str = "ReplacingMergeTree") -> sch.TableSchema:
        cols = [
            ColumnDefinition("name", "String", "Data", nullable=False),
            ColumnDefinition("modified", "DateTime64(6)", "Datetime", nullable=False),
            ColumnDefinition("title", "String", "Data", nullable=True),
            ColumnDefinition("amount", "Float64", "Currency", nullable=True),
            ColumnDefinition("_ch_synced_at", "DateTime64(3)", "Datetime", nullable=False),
        ]
        order_by = ("name",) if engine.startswith("ReplacingMergeTree") else ("name", "modified")
        partition = "tuple()" if engine.startswith("ReplacingMergeTree") else "toYYYYMM(modified)"
        return sch.TableSchema(
            doctype="Sales Invoice",
            table_name="tab_sales_invoice",
            columns=cols,
            engine=engine,
            order_by=order_by,
            partition_by=partition,
        )

    def test_table_name_normalisation(self) -> None:
        self.assertEqual(sch.table_name_for("Sales Invoice"), "tab_sales_invoice")
        self.assertEqual(sch.table_name_for("Item-Price"), "tab_item_price")

    def test_create_table_uses_replacingmergetree_and_version(self) -> None:
        ddl = sch.generate_create_table_sql(self._sample_schema(), "reporting")
        self.assertIn("CREATE TABLE IF NOT EXISTS `reporting`.`tab_sales_invoice`", ddl)
        # ReplacingMergeTree must carry the version column so re-runs dedupe.
        self.assertIn("ENGINE = ReplacingMergeTree(`_ch_synced_at`)", ddl)
        # Dedup key must be `name` alone, not (name, modified).
        self.assertIn("ORDER BY (`name`)", ddl)
        # Single partition so dedup is global, not per-month.
        self.assertIn("PARTITION BY tuple()", ddl)

    def test_plain_mergetree_keeps_composite_key(self) -> None:
        ddl = sch.generate_create_table_sql(self._sample_schema(engine="MergeTree"), "reporting")
        self.assertIn("ENGINE = MergeTree", ddl)
        self.assertNotIn("ReplacingMergeTree", ddl)
        self.assertIn("ORDER BY (`name`, `modified`)", ddl)

    def test_nullable_columns_rendered(self) -> None:
        ddl = sch.generate_create_table_sql(self._sample_schema(), "reporting")
        self.assertIn("`title` Nullable(String)", ddl)
        self.assertIn("`amount` Nullable(Float64)", ddl)
        self.assertIn("`name` String", ddl)

    def test_widening_string_is_universal_sink(self) -> None:
        self.assertTrue(sch._can_widen("Int64", "String"))
        self.assertTrue(sch._can_widen("Float32", "String"))

    def test_widening_numeric_upward_allowed(self) -> None:
        self.assertTrue(sch._can_widen("Int32", "Int64"))
        self.assertTrue(sch._can_widen("Float32", "Float64"))

    def test_widening_downward_refused(self) -> None:
        self.assertFalse(sch._can_widen("Int64", "Int32"))
        self.assertFalse(sch._can_widen("Float64", "Float32"))

    def test_same_type_is_noop(self) -> None:
        self.assertFalse(sch._can_widen("String", "String"))

    def test_base_type_strips_nullable(self) -> None:
        self.assertEqual(sch._base_type("Nullable(String)"), "String")
        self.assertEqual(sch._base_type("Int64"), "Int64")

    def test_credential_blocklist_exact_names(self) -> None:
        for f in ("api_key", "api_secret", "password", "new_password", "reset_password_key"):
            self.assertTrue(sch._is_blocked_field(f), f)

    def test_credential_blocklist_suffixes(self) -> None:
        self.assertTrue(sch._is_blocked_field("stripe_secret"))
        self.assertTrue(sch._is_blocked_field("admin_password"))
        self.assertTrue(sch._is_blocked_field("oauth_token"))

    def test_blocklist_does_not_touch_ordinary_fields(self) -> None:
        for f in ("first_name", "email", "designation", "company", "status"):
            self.assertFalse(sch._is_blocked_field(f), f)

    def test_protected_columns_never_blocked(self) -> None:
        for f in ("name", "modified", "creation"):
            self.assertFalse(sch._is_blocked_field(f), f)


if __name__ == "__main__":
    unittest.main()
