# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Tests for the monitoring dashboard API and the live run record.

The engine runs for real against the in-memory fakes, and the dashboard API
is called while it runs and after it finishes, so every figure checked here
is the figure the page would show.
"""

from __future__ import annotations

import datetime as dt
import unittest

from clickhouse_sync.tests._fakes import EngineHarness, ensure_frappe_importable

ensure_frappe_importable()

from clickhouse_sync.clickhouse import live_status, sync_engine  # noqa: E402
from clickhouse_sync.clickhouse.exceptions import (  # noqa: E402
    ClickHouseConnectionError,
    categorize_error,
    categorize_text,
)


class DashboardTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)
        self.api = self.h.dashboard

    def overview(self, days: int = 7) -> dict:
        return self.api.overview(days=days)

    def row(self, data: dict, name: str = "Doc") -> dict:
        return next(r for r in data["doctypes"] if r["configuration"] == name)


class TestLiveMonitor(DashboardTestCase):
    def test_running_sync_reports_backend_progress(self) -> None:
        self.h.seed(10)
        self.h.clock.advance(5)
        seen: list[dict] = []

        def watch(n: int, table: str) -> None:
            self.h.clock.advance(6)  # each batch takes 6 seconds
            if n == 4:
                seen.extend(self.api.live()["live"])

        self.h.clickhouse.on_insert = watch
        self.assertEqual(self.h.run().status, "Completed")

        self.assertEqual(len(seen), 1)
        run = seen[0]
        self.assertEqual(run["display_status"], "Running")
        self.assertEqual(run["doctype"], "Doc")
        self.assertEqual(run["total"], 10)
        self.assertEqual(run["batch"], 3)  # batch 4 is being inserted
        self.assertEqual(run["rows_read"], 6)
        self.assertEqual(run["cursor_name"], "DOC-05")
        self.assertEqual(run["percent"], 60.0)
        self.assertEqual(run["attempt"], 1)
        self.assertIsNotNone(run["rows_per_second"])
        self.assertIsNotNone(run["eta_seconds"])
        self.assertFalse(run["stalled"])

    def test_live_record_is_removed_and_summary_kept_after_the_run(self) -> None:
        self.h.seed(4)
        self.h.run()
        self.assertEqual(self.api.live()["live"], [])
        last = live_status.get_last_run("Doc")
        self.assertEqual(last["status"], "Completed")
        self.assertEqual(last["rows_written"], 4)

    def test_queued_job_shows_and_orphan_is_cleared(self) -> None:
        self.api.start_sync("Doc", "Incremental")
        self.assertEqual(self.h.frappe.enqueued[-1]["mode"], "Incremental")
        live = self.api.live()["live"]
        self.assertEqual([r["display_status"] for r in live], ["Queued"])

        # The queue no longer knows the job and it has waited too long.
        self.h.job_active = False
        self.h.clock.advance(self.api.QUEUED_ORPHAN_SECONDS + 1)
        self.assertEqual(self.api.live()["live"], [])

    def test_stalled_and_lost_worker(self) -> None:
        self.h.seed(4)

        def die(n: int, table: str) -> None:
            raise SystemExit("worker killed")

        self.h.clickhouse.on_insert = die
        with self.assertRaises(SystemExit):
            self.h.run()
        # A killed worker normally leaves no live record (the Failed log path
        # removes it). Simulate a hard kill that skipped cleanup.
        live_status._write(
            live_status.LIVE_PREFIX + "Doc",
            {"configuration": "Doc", "status": "Running", "started_at": str(self.h.clock.now),
             "heartbeat": str(self.h.clock.now), "rows_read": 2, "batch": 1},
            3600,
        )
        self.h.clock.advance(live_status.STALE_AFTER_SECONDS + 5)
        self.h.job_active = False
        run = self.api.live()["live"][0]
        self.assertEqual(run["display_status"], "Stalled")
        self.assertTrue(run["worker_lost"])
        self.assertIsNone(run["eta_seconds"])

    def test_stop_pressed_while_queued_cancels_before_start(self) -> None:
        self.h.seed(4)
        self.api.start_sync("Doc")
        self.h.clock.advance(5)
        self.api.stop_sync("Doc")
        self.h.clock.advance(5)
        result = self.h.run()
        self.assertEqual(result.status, "Cancelled")
        self.assertEqual(result.rows_written, 0)
        self.assertIn("before it started", result.message)

    def test_stale_stop_flag_from_an_earlier_run_is_ignored(self) -> None:
        self.h.seed(4)
        self.api.stop_sync("Doc")  # nothing running: flag left behind
        self.h.clock.advance(60)
        self.api.start_sync("Doc")
        self.h.clock.advance(5)
        self.assertEqual(self.h.run().status, "Completed")

    def test_new_and_updated_split_for_incremental_runs(self) -> None:
        self.h.seed(6)
        self.h.run()
        self.h.clock.advance(sync_engine.CURSOR_SAFETY_LAG_SECONDS * 2)
        self.h.save("DOC-02", title="v2")  # existing record edited
        self.h.db.upsert("Doc", name="DOC-NEW", modified=self.h.clock.now,
                         creation=self.h.clock.now, title="v1")
        self.h.clock.advance(5)
        self.h.run()
        last = live_status.get_last_run("Doc")
        self.assertEqual(last["rows_new"], 1)
        self.assertEqual(last["rows_updated"], 1)
        self.assertEqual(last["rows_rechecked"], 5)  # unchanged, inside the safety lag

    def test_split_unknown_for_first_and_full_runs(self) -> None:
        self.h.seed(3)
        self.h.run()
        self.assertIsNone(live_status.get_last_run("Doc")["rows_new"])

    def test_derived_metrics_wait_for_enough_evidence(self) -> None:
        now = dt.datetime(2026, 1, 1, 10, 0, 0)
        early = live_status.derive_metrics(
            {"status": "Running", "started_at": "2026-01-01 09:59:57", "heartbeat": "2026-01-01 10:00:00",
             "rows_read": 500, "total": 1000, "batch": 1},
            now,
        )
        self.assertIsNone(early["rows_per_second"])
        self.assertIsNone(early["eta_seconds"])
        later = live_status.derive_metrics(
            {"status": "Running", "started_at": "2026-01-01 09:59:40", "heartbeat": "2026-01-01 10:00:00",
             "rows_read": 400, "total": 1000, "batch": 4},
            now,
        )
        self.assertEqual(later["rows_per_second"], 20.0)
        self.assertEqual(later["eta_seconds"], 30)
        self.assertEqual(later["percent"], 40.0)


class TestOverview(DashboardTestCase):
    def test_counts_pending_and_history(self) -> None:
        self.h.seed(5)
        self.h.run()
        data = self.overview()
        row = self.row(data)
        self.assertEqual((row["source"], row["warehouse"], row["pending"]), (5, 5, 0))
        self.assertEqual(row["difference"], 0)
        self.assertEqual(row["sync_percent"], 100.0)
        self.assertEqual(row["last_status"], "Completed")
        self.assertIsNotNone(row["last_success"])
        o = data["overall"]
        self.assertEqual((o["configured"], o["enabled"]), (1, 1))
        self.assertEqual((o["source_records"], o["warehouse_records"]), (5, 5))
        self.assertEqual(o["sync_percent"], 100.0)
        self.assertTrue(data["clickhouse"]["ok"])

        self.h.clock.advance(60)
        self.h.save("DOC-01", title="v2")
        self.h.save("DOC-09", title="v1")
        row = self.row(self.overview())
        self.assertEqual(row["pending"], 2)  # pending is always fresh
        self.assertEqual(row["source"], 5)  # source count comes from the cache
        self.assertEqual(row["sync_percent"], 60.0)

    def test_connection_state_is_reported_when_counts_come_from_cache(self) -> None:
        self.h.seed(3)
        self.h.run()
        self.overview()  # fills the count cache
        data = self.overview()  # every count now cached
        self.assertTrue(data["clickhouse"]["ok"])

    def test_recount_bypasses_the_cache(self) -> None:
        self.h.seed(3)
        self.h.run()
        self.overview()
        self.h.save("DOC-77")
        self.assertEqual(self.row(self.overview())["source"], 3)
        self.api.recount("Doc")
        self.assertEqual(self.row(self.overview())["source"], 4)

    def test_counts_expire_after_ten_minutes(self) -> None:
        self.h.seed(3)
        self.h.run()
        self.overview()
        self.h.save("DOC-77")
        self.h.clock.advance(self.api.COUNTS_FRESH_SECONDS + 1)
        self.assertEqual(self.row(self.overview())["source"], 4)

    def test_soft_deleted_rows_are_not_counted_in_the_warehouse(self) -> None:
        self.h.set_config(sync_mode="Full", delete_strategy="Soft Delete")
        self.h.seed(4)
        self.h.run()
        self.h.clickhouse.tables["tab_doc"]["DOC-01"]["_ch_is_deleted"] = 1
        self.api.recount()
        self.assertEqual(self.row(self.overview())["warehouse"], 3)

    def test_clickhouse_down_still_renders_and_is_not_cached(self) -> None:
        self.h.seed(3)
        self.h.run()
        self.h.connect_error = lambda n: ClickHouseConnectionError("refused")
        self.api.recount()
        data = self.overview()
        self.assertFalse(data["clickhouse"]["ok"])
        self.assertIsNone(self.row(data)["warehouse"])
        self.assertTrue(data["overall"]["warehouse_partial"])
        self.assertIsNone(data["overall"]["warehouse_records"])
        self.assertEqual(data["warehouse_tables"], [])
        self.assertEqual(self.row(data)["source"], 3)

        self.h.connect_error = None
        self.assertEqual(self.row(self.overview())["warehouse"], 3)

    def test_table_not_created_yet_counts_as_zero(self) -> None:
        self.h.seed(2)
        row = self.row(self.overview())
        self.assertEqual(row["warehouse"], 0)
        self.assertEqual(row["warehouse_note"], "no table")
        self.assertIsNone(row["last_status"])

    def test_disabled_configurations_are_listed_but_not_totalled(self) -> None:
        self.h.seed(2)
        self.h.set_config(enabled=0)
        data = self.overview()
        self.assertEqual(data["overall"]["enabled"], 0)
        self.assertIsNone(self.row(data)["source"])
        self.assertEqual(data["overall"]["source_records"], 0)

    def test_analytics_cover_every_day_in_the_window(self) -> None:
        self.h.seed(3)
        self.h.run()
        data = self.overview(days=7)
        a = data["analytics"]
        self.assertEqual(len(a["labels"]), 8)  # today plus the 7 days before
        self.assertEqual(sum(a["rows_written"]), 3)
        self.assertEqual(sum(a["runs_completed"]), 1)
        self.assertEqual(a["success_rate"], 100.0)
        self.assertEqual([t["table"] for t in data["warehouse_tables"]], ["tab_doc"])


class TestErrors(DashboardTestCase):
    def test_failed_run_appears_with_category_batch_and_retries(self) -> None:
        self.h.seed(6)
        self.h.clock.advance(5)

        def fail_third(n: int, table: str) -> None:
            if n == 3:
                raise ValueError("Cannot parse 'abc' as Int64")

        self.h.clickhouse.on_insert = fail_third
        self.assertEqual(self.h.run().status, "Failed")

        data = self.overview()
        errors = data["errors"]
        self.assertEqual(errors["failed_runs"], 1)
        self.assertEqual(errors["failed_rows"], 2)
        first = errors["recent"][0]
        self.assertEqual(first["category"], "Data")
        self.assertEqual(first["batch"], 3)
        self.assertEqual(first["retries"], 0)
        self.assertEqual(data["overall"]["failed_doctypes"], ["Doc"])
        self.assertEqual(self.row(data)["last_status"], "Failed")

        detail = self.api.error_detail(first["log"])
        self.assertEqual(detail["category"], "Data")
        self.assertIn("ValueError", detail["exception"])
        self.assertEqual(detail["detail"]["phase"], "syncing")  # where it failed, not "retrying"
        self.assertEqual(detail["detail"]["failed_batch_first"], "DOC-04")
        self.assertEqual(detail["detail"]["failed_batch_last"], "DOC-05")

    def test_retries_are_recorded(self) -> None:
        self.h.seed(4)
        self.h.connect_error = lambda n: ClickHouseConnectionError("refused")
        self.assertEqual(self.h.run().status, "Failed")
        first = self.overview()["errors"]["recent"][0]
        self.assertEqual(first["category"], "Connection")
        self.assertEqual(first["retries"], 2)

    def test_retry_failed_queues_only_failing_doctypes(self) -> None:
        self.h.seed(2)
        self.h.clickhouse.on_insert = lambda n, t: (_ for _ in ()).throw(ValueError("bad"))
        self.h.run()
        self.assertEqual(self.api.retry_failed()["queued"], ["Doc"])
        self.h.clickhouse.on_insert = None
        self.h.clock.advance(10)
        self.h.run()
        self.assertEqual(self.api.retry_failed()["queued"], [])

    def test_category_of_old_logs_is_inferred_from_text(self) -> None:
        self.assertEqual(categorize_text("Code: 241. DB::Exception: Memory limit"), "Server capacity")
        self.assertEqual(categorize_text("x", "Traceback...\nValueError: bad"), "Data")
        self.assertEqual(categorize_text("Failed to connect to ClickHouse", "ClickHouseConnectionError: x"), "Connection")
        self.assertEqual(categorize_text("Sync cancelled for Doc."), "Cancelled")
        self.assertEqual(categorize_error(TimeoutError()), "Timeout")
        self.assertEqual(categorize_error(Exception("Code: 60. DB::Exception: Table missing")), "Schema")


class TestControls(DashboardTestCase):
    def test_full_override_runs_full_once_without_changing_the_configuration(self) -> None:
        self.h.seed(4)
        self.api.start_sync("Doc", "Full")
        self.assertEqual(self.h.frappe.enqueued[-1]["mode"], "Full")
        result = sync_engine.run_sync("Doc", trigger="Manual", mode="Full")
        self.assertEqual(result.status, "Completed")
        self.assertTrue(result.message.startswith("[Full sync requested]"))
        self.assertTrue(any("ORDER BY name ASC" in q for q in self.h.db.queries))
        self.assertEqual(self.h.frappe.configs["Doc"].sync_mode, "Incremental")

    def test_invalid_mode_and_unknown_configuration_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.api.start_sync("Doc", "Everything")
        with self.assertRaises(ValueError):
            self.api.start_sync("Nope")
        self.assertEqual(self.h.frappe.enqueued, [])

    def test_disabled_configuration_cannot_be_started(self) -> None:
        self.h.set_config(enabled=0)
        with self.assertRaises(ValueError):
            self.api.start_sync("Doc")

    def test_start_all_and_stop_all(self) -> None:
        self.h.set_config(name="Doc 2", source_doctype="Doc", priority=1)
        queued = self.api.start_all("Incremental")["queued"]
        self.assertEqual(sorted(queued), ["Doc", "Doc 2"])
        stopping = self.api.stop_all()["stopping"]
        self.assertEqual(sorted(stopping), ["Doc", "Doc 2"])
        self.assertTrue(all(r["stop_requested"] for r in self.api.live()["live"]))

    def test_every_endpoint_checks_roles(self) -> None:
        self.h.frappe.deny_roles = True
        calls = [
            lambda: self.api.overview(),
            lambda: self.api.live(),
            lambda: self.api.error_detail("LOG-0001"),
            lambda: self.api.start_sync("Doc"),
            lambda: self.api.start_all(),
            lambda: self.api.retry_failed(),
            lambda: self.api.stop_sync("Doc"),
            lambda: self.api.stop_all(),
            lambda: self.api.recount(),
        ]
        for call in calls:
            with self.assertRaises(PermissionError):
                call()
        self.assertEqual(self.h.frappe.enqueued, [])


class TestMonitoringNeverBreaksASync(DashboardTestCase):
    def test_sync_completes_when_redis_writes_fail(self) -> None:
        self.h.seed(4)

        def broken(*args, **kwargs):
            raise ConnectionError("redis down")

        original = self.h.frappe.cache
        self.h.frappe.cache = broken
        try:
            result = self.h.run()
        finally:
            self.h.frappe.cache = original
        # The cancel check also uses Redis; a Redis outage must not fail syncs.
        self.assertEqual(result.status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])
        self.assertEqual(len(self.h.frappe.logs), 1)


if __name__ == "__main__":
    unittest.main()
