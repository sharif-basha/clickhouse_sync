# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Regression tests for the second round of review fixes.

Covers the configurable safety lag (M10), time zones on new tables (M11),
counting values that could not be converted (M12), soft deletes as row
versions, creation-month partitioning, per-configuration schedules, dashboard
count caching (M8), the stall allowance for long phases (M4) and the error
category sample.
"""

from __future__ import annotations

import datetime as dt
import types
import unittest
from unittest import mock

from clickhouse_sync.tests._fakes import (
    LOG_TABLE_COLUMNS,
    EngineHarness,
    FakeMeta,
    docfield,
    ensure_frappe_importable,
)

ensure_frappe_importable()

import clickhouse_sync  # noqa: E402
from clickhouse_sync import hooks, tasks  # noqa: E402
from clickhouse_sync.clickhouse import live_status, schema, sync_engine  # noqa: E402
from clickhouse_sync.clickhouse import type_mapping as tm  # noqa: E402
from clickhouse_sync.clickhouse.warehouse_stats import readable_size  # noqa: E402

KOLKATA = "Asia/Kolkata"


class TestM10SafetyLag(unittest.TestCase):
    def _first_window_lower(self, **config) -> dt.timedelta:
        h = EngineHarness(self)
        h.set_config(**config)
        h.seed(3)
        self.assertEqual(h.run().status, "Completed")
        cursor = h.cursor
        h.clock.advance(7200)
        h.db.query_log.clear()
        self.assertEqual(h.run().status, "Completed")
        lower = next(p["lower"] for q, p in h.db.query_log if "modified >= %(lower)s" in q)
        return cursor - lower

    def test_default_lag_is_five_minutes(self) -> None:
        self.assertEqual(self._first_window_lower(), dt.timedelta(seconds=300))

    def test_configured_lag_is_used(self) -> None:
        self.assertEqual(
            self._first_window_lower(cursor_safety_lag_seconds=3600), dt.timedelta(hours=1)
        )

    def test_lag_is_bounded(self) -> None:
        self.assertEqual(sync_engine._safety_lag(-5), 0)
        self.assertEqual(sync_engine._safety_lag(10**9), sync_engine.MAX_CURSOR_SAFETY_LAG_SECONDS)
        self.assertEqual(sync_engine._safety_lag(None), sync_engine.CURSOR_SAFETY_LAG_SECONDS)


class TestM11TimeZones(unittest.TestCase):
    def test_new_tables_declare_the_system_time_zone(self) -> None:
        h = EngineHarness(self)
        h.frappe.metas[h.DOCTYPE] = FakeMeta([docfield("title"), docfield("due", "Datetime")])
        table = schema.build_table_schema(h.DOCTYPE, timezone=KOLKATA)
        ddl = schema.generate_create_table_sql(table, "wh")
        self.assertIn("`modified` DateTime64(6, 'Asia/Kolkata')", ddl)
        self.assertIn("`creation` Nullable(DateTime64(6, 'Asia/Kolkata'))", ddl)
        self.assertIn("`due` Nullable(DateTime64(6, 'Asia/Kolkata'))", ddl)
        self.assertIn("`_ch_synced_at` DateTime64(3, 'Asia/Kolkata')", ddl)

    def test_values_for_zoned_columns_are_sent_as_aware_datetimes(self) -> None:
        h = EngineHarness(self)
        with mock.patch.object(schema, "system_timezone", lambda: KOLKATA):
            h.save("DOC-1", modified=dt.datetime(2025, 12, 1, 12, 0, 0))
            self.assertEqual(h.run().status, "Completed")
        modified = h.warehouse()["DOC-1"]["modified"]
        self.assertEqual(str(modified.tzinfo), KOLKATA)
        # 12:00 in Kolkata is 06:30 UTC: the instant is right whatever the
        # worker's OS time zone is.
        self.assertEqual(modified.astimezone(dt.timezone.utc).hour, 6)

    def test_existing_columns_without_a_zone_are_left_alone(self) -> None:
        h = EngineHarness(self)
        h.seed(1)
        self.assertEqual(h.run().status, "Completed")  # created without a zone
        h.clock.advance(3600)
        h.save("DOC-00", title="v2")
        with mock.patch.object(schema, "system_timezone", lambda: KOLKATA):
            self.assertEqual(h.run().status, "Completed")
        self.assertFalse([c for c in h.clickhouse.commands if "MODIFY COLUMN" in c])
        self.assertIsNone(h.warehouse()["DOC-00"]["modified"].tzinfo)

    def test_type_helpers(self) -> None:
        self.assertEqual(tm.column_timezone("Nullable(DateTime64(6, 'Asia/Kolkata'))"), KOLKATA)
        self.assertIsNone(tm.column_timezone("DateTime64(6)"))
        self.assertEqual(tm.strip_timezone("DateTime64(6, 'UTC')"), "DateTime64(6)")
        self.assertIsNone(tm.valid_timezone("Not/A_Zone"))
        self.assertIsNone(tm.valid_timezone("x'); DROP TABLE t; --"))


class TestM12UnconvertedValues(unittest.TestCase):
    def test_values_that_cannot_be_converted_are_counted(self) -> None:
        h = EngineHarness(self)
        h.frappe.metas[h.DOCTYPE] = FakeMeta([docfield("title", "Int")])
        h.save("DOC-1", title="12")
        h.save("DOC-2", title="not a number")
        result = h.run()
        self.assertEqual(result.status, "Completed")
        self.assertIn("1 value(s) could not be converted", result.message)
        self.assertIn("title: 1", result.message)
        self.assertIsNone(h.warehouse()["DOC-2"]["title"])
        self.assertEqual(h.warehouse()["DOC-1"]["title"], 12)


    def test_child_table_values_are_counted_too(self) -> None:
        h = EngineHarness(self)
        child = h.add_child_table()
        h.set_config(sync_child_tables=1)
        h.save("DOC-1")
        h.db.upsert(
            child, name="row-1", modified=h.clock.now, parent="DOC-1",
            parenttype=h.DOCTYPE, parentfield="items", qty="lots",
        )
        h.clock.advance(5)
        result = h.run()
        self.assertEqual(result.status, "Completed")
        self.assertIn("qty: 1", result.message)


class TestSoftDeleteAsNewVersion(unittest.TestCase):
    def test_soft_delete_inserts_a_flagged_version_instead_of_a_mutation(self) -> None:
        h = EngineHarness(self)
        h.set_config(delete_strategy="Soft Delete", sync_mode="Full")
        h.save("A")
        h.save("B")
        self.assertEqual(h.run().status, "Completed")
        h.db.delete(h.DOCTYPE, "A")
        self.assertEqual(h.run().status, "Completed")
        self.assertEqual(h.warehouse()["A"]["_ch_is_deleted"], 1)
        self.assertEqual(h.warehouse()["B"]["_ch_is_deleted"], 0)
        self.assertFalse([c for c in h.clickhouse.commands if c.startswith("ALTER TABLE") and "UPDATE" in c])
        insert = next(c for c in h.clickhouse.commands if c.startswith("INSERT INTO"))
        self.assertIn("FINAL", insert)
        self.assertIn("toIntervalMillisecond(1) AS `_ch_synced_at`", insert)
        self.assertIn("`_ch_is_deleted` = 0", insert)


class TestCreationPartitioning(unittest.TestCase):
    def _harness(self, **config) -> EngineHarness:
        h = EngineHarness(self)
        h.set_config(optimize_after_sync=1, optimize_only_on_full=0, **config)
        h.save("A", modified=dt.datetime(2025, 12, 15))
        h.db.conn.execute("UPDATE `tabDoc` SET creation = '2025-11-02 10:00:00.000000'")
        return h

    def test_new_table_is_partitioned_and_only_touched_months_are_optimized(self) -> None:
        h = self._harness(partition_by_creation=1)
        self.assertEqual(h.run().status, "Completed")
        create = next(c for c in h.clickhouse.commands if c.startswith("CREATE TABLE"))
        self.assertIn("PARTITION BY toYYYYMM(assumeNotNull(`creation`))", create)
        optimizes = [(c, p) for c, p in h.clickhouse.command_calls if c.startswith("OPTIMIZE")]
        self.assertEqual(len(optimizes), 1)
        self.assertIn("PARTITION ID {pid:String} FINAL", optimizes[0][0])
        self.assertEqual(optimizes[0][1], {"pid": "202511"})

    def test_unpartitioned_tables_are_optimized_whole(self) -> None:
        h = self._harness()
        self.assertEqual(h.run().status, "Completed")
        self.assertIn("OPTIMIZE TABLE `wh`.`tab_doc` FINAL", h.clickhouse.commands)

    def test_flag_on_an_existing_table_is_explained(self) -> None:
        h = self._harness()
        self.assertEqual(h.run().status, "Completed")
        h.set_config(optimize_after_sync=1, optimize_only_on_full=0, partition_by_creation=1)
        h.clock.advance(60)
        result = h.run()
        self.assertIn("applies to new tables", result.message)


class TestPerConfigurationSchedule(unittest.TestCase):
    NOW = dt.datetime(2026, 6, 1, 12, 0)

    def test_is_due(self) -> None:
        self.assertTrue(tasks.is_due(15, self.NOW, self.NOW))
        self.assertTrue(tasks.is_due(60, None, self.NOW))
        self.assertFalse(tasks.is_due(60, self.NOW - dt.timedelta(minutes=30), self.NOW))
        # Started 59 minutes ago: due (scheduler timing jitter).
        self.assertTrue(tasks.is_due(60, self.NOW - dt.timedelta(minutes=59), self.NOW))
        self.assertFalse(tasks.is_due(1440, self.NOW - dt.timedelta(hours=5), self.NOW))

    def test_scheduler_skips_configurations_that_are_not_due(self) -> None:
        fake = mock.MagicMock()
        fake.get_all.return_value = [
            types.SimpleNamespace(name="Often", sync_interval_minutes=15, get=lambda k, d=None: 15),
            types.SimpleNamespace(name="Daily", sync_interval_minutes=1440, get=lambda k, d=None: 1440),
        ]
        fake.db.sql.return_value = [
            {"configuration": "Often", "last_start": self.NOW - dt.timedelta(minutes=15)},
            {"configuration": "Daily", "last_start": self.NOW - dt.timedelta(hours=3)},
        ]
        queued = []
        with mock.patch.object(tasks, "frappe", fake), mock.patch.object(
            tasks, "now_datetime", lambda: self.NOW
        ), mock.patch.object(tasks, "get_datetime", lambda v: v), mock.patch.object(
            tasks, "enqueue_config_sync",
            lambda name, trigger: queued.append(name) or {"queued": True},
        ):
            tasks._enqueue_enabled_configs()
        self.assertEqual(queued, ["Often"])


class TestM8PendingCountCache(unittest.TestCase):
    def test_pending_count_is_not_recounted_on_every_refresh(self) -> None:
        h = EngineHarness(self)
        h.seed(3)
        self.assertEqual(h.run().status, "Completed")
        h.dashboard.overview()
        pending_queries = lambda: [q for q, _ in h.db.query_log if "WHERE modified > %(cursor)s" in q]  # noqa: E731
        before = len(pending_queries())
        h.clock.advance(20)
        h.dashboard.overview()
        self.assertEqual(len(pending_queries()), before)
        h.clock.advance(h.dashboard.PENDING_FRESH_SECONDS)
        h.dashboard.overview()
        self.assertEqual(len(pending_queries()), before + 1)


class TestM4LongPhases(unittest.TestCase):
    def _state(self, phase: str) -> dict:
        now = dt.datetime(2026, 6, 1, 12, 0)
        return {
            "status": "Running", "phase": phase,
            "started_at": (now - dt.timedelta(hours=2)).isoformat(sep=" "),
            "heartbeat": (now - dt.timedelta(hours=1)).isoformat(sep=" "),
        }, now

    def test_long_statement_phases_get_a_longer_allowance(self) -> None:
        for phase, stalled in (("optimizing", False), ("reconciling deletes", False), ("syncing", True)):
            with self.subTest(phase=phase):
                state, now = self._state(phase)
                with mock.patch.object(live_status, "get_datetime", dt.datetime.fromisoformat):
                    self.assertEqual(live_status.derive_metrics(state, now)["stalled"], stalled)


class TestErrorCategorySample(unittest.TestCase):
    def test_categories_cover_more_than_the_listed_rows(self) -> None:
        h = EngineHarness(self)
        for i in range(30):
            row = {c: None for c in LOG_TABLE_COLUMNS}
            row.update(
                name=f"LOG-{i:03d}", configuration="Doc", status="Failed",
                creation=h.clock.now, message="Code: 241. DB::Exception: Memory limit",
            )
            h.db.insert_raw("tabClickHouse Sync Log", **row)
        errors = h.dashboard._errors(h.clock.now - dt.timedelta(days=1))
        self.assertEqual(len(errors["recent"]), 25)
        self.assertEqual(errors["categorised_runs"], 30)
        self.assertEqual(sum(c["count"] for c in errors["by_category"]), 30)


class TestHousekeeping(unittest.TestCase):
    def test_single_version_source(self) -> None:
        self.assertEqual(hooks.app_version, clickhouse_sync.__version__)

    def test_readable_size(self) -> None:
        self.assertEqual(readable_size(512), "512.00 B")
        self.assertEqual(readable_size(3 * 1024**3), "3.00 GiB")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
