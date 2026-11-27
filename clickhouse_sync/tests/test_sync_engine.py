# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Integration tests for the sync engine, run against a live Frappe site.

These extend ``frappe.tests.utils.FrappeTestCase`` so they require ``bench
run-tests``. They validate batch-size clamping, context construction, and a
round-trip sync against an ephemeral configuration. Tests that need a real
ClickHouse server are skipped automatically when one is not reachable.
"""

from __future__ import annotations

import unittest

from clickhouse_sync.tests._fakes import ensure_frappe_importable

ensure_frappe_importable()

import frappe  # noqa: E402

try:
    from frappe.tests.utils import FrappeTestCase

    IN_BENCH = True
except Exception:  # pragma: no cover - allows import outside a bench
    FrappeTestCase = unittest.TestCase  # type: ignore[assignment,misc]
    IN_BENCH = False

from clickhouse_sync.clickhouse import connection as ch_connection  # noqa: E402
from clickhouse_sync.clickhouse import sync_engine  # noqa: E402


def _clickhouse_available() -> bool:
    try:
        return ch_connection.health_check().get("ok", False)
    except Exception:  # noqa: BLE001
        return False


class TestSyncEngineHelpers(FrappeTestCase):
    """Pure-logic tests that do not require ClickHouse."""

    def test_batch_size_clamp_lower_bound(self) -> None:
        self.assertEqual(sync_engine._clamp_batch_size(0), sync_engine.DEFAULT_BATCH_SIZE)
        self.assertEqual(sync_engine._clamp_batch_size(10), 1000)

    def test_batch_size_clamp_upper_bound(self) -> None:
        self.assertEqual(sync_engine._clamp_batch_size(10_000_000), 100000)

    def test_batch_size_passthrough(self) -> None:
        self.assertEqual(sync_engine._clamp_batch_size(25000), 25000)

    @unittest.skipUnless(IN_BENCH, "needs a Frappe site with Redis (bench run-tests)")
    def test_cancel_flag_roundtrip(self) -> None:
        cfg = "unit-test-cancel"
        self.assertFalse(sync_engine._is_cancelled(cfg))
        sync_engine.request_cancel(cfg)
        self.assertTrue(sync_engine._is_cancelled(cfg))
        sync_engine._clear_cancel(cfg)
        self.assertFalse(sync_engine._is_cancelled(cfg))


@unittest.skipUnless(_clickhouse_available(), "ClickHouse server not reachable")
class TestSyncEngineRoundTrip(FrappeTestCase):
    """Full round-trip sync against a live ClickHouse instance."""

    CONFIG = "ToDo"

    def setUp(self) -> None:
        if not frappe.db.exists("ClickHouse Sync Configuration", self.CONFIG):
            doc = frappe.new_doc("ClickHouse Sync Configuration")
            doc.source_doctype = "ToDo"
            doc.enabled = 1
            doc.sync_mode = "Full"
            doc.batch_size = 1000
            doc.auto_create_table = 1
            doc.auto_update_schema = 1
            doc.delete_strategy = "Ignore"
            doc.insert(ignore_permissions=True)

    def test_full_sync_completes(self) -> None:
        result = sync_engine.run_sync(self.CONFIG, trigger="Manual")
        self.assertIn(result.status, ("Completed", "Cancelled"))
        self.assertIsNotNone(result.log_name)


if __name__ == "__main__":
    unittest.main()
