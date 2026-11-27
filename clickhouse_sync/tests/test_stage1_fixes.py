# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Regression tests for the Stage 1 audit fixes.

Each class covers one audit finding and fails on the pre-fix code:

* Findings 1 and 2 - incremental cursor / pagination data loss.
* Finding 3        - ClickHouse outage escaping ``run_sync`` with no log.
* Finding 4        - child-table failures swallowed.
* Finding 10       - job timeout retried (classification itself is covered
                     in ``test_exceptions.py``).
* Finding 34       - alert duration line.

They run without a bench (``python -m unittest``) and also under
``bench run-tests``; see ``_fakes.py`` for how.
"""

from __future__ import annotations

import datetime as dt
import random
import unittest

from clickhouse_sync.tests._fakes import EngineHarness, ensure_frappe_importable

ensure_frappe_importable()

from clickhouse_sync.api import api as whitelist_api  # noqa: E402
from clickhouse_sync.clickhouse import alerts, sync_engine  # noqa: E402
from clickhouse_sync.clickhouse.exceptions import ClickHouseConnectionError  # noqa: E402

LAG = sync_engine.CURSOR_SAFETY_LAG_SECONDS


class TestWhitelistedApiMethods(unittest.TestCase):
    def test_state_changing_endpoints_are_post_only(self) -> None:
        for endpoint in (whitelist_api.sync_now, whitelist_api.sync_all, whitelist_api.cancel_sync):
            with self.subTest(endpoint=endpoint.__name__):
                self.assertEqual(endpoint.allowed_http_methods, ["POST"])

    def test_read_only_endpoints_keep_default_methods(self) -> None:
        for endpoint in (whitelist_api.test_connection, whitelist_api.get_progress):
            with self.subTest(endpoint=endpoint.__name__):
                self.assertIsNone(endpoint.allowed_http_methods)


def fail_on_insert(number: int, error: Exception, table: str | None = None):
    """Return an ``on_insert`` hook that raises on the given insert number."""

    def hook(n: int, tbl: str) -> None:
        if n == number and (table is None or tbl == table):
            raise error

    return hook


class TestIncrementalCursor(unittest.TestCase):
    """Findings 1 and 2: no row may be skipped, whatever interrupts a run."""

    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)

    def test_initial_load_completes_and_cursor_is_run_start(self) -> None:
        self.h.seed(7)
        started = self.h.clock.now
        result = self.h.run()
        self.assertEqual(result.status, "Completed")
        self.assertEqual(result.rows_written, 7)
        self.assertEqual(self.h.out_of_sync(), [])
        self.assertEqual(self.h.cursor, started)

    def test_failed_initial_load_resumes_without_losing_rows(self) -> None:
        # The exact audit reproduction: an early-named row carries the newest
        # timestamp, and the run dies on its third batch.
        self.h.seed(10)
        self.h.save("DOC-01", modified=self.h.clock.advance(500))
        self.h.clock.advance(10)
        self.h.clickhouse.on_insert = fail_on_insert(3, ValueError("bad value"))

        first = self.h.run()
        self.assertEqual(first.status, "Failed")
        self.assertEqual(first.rows_failed, 2)

        self.h.clickhouse.on_insert = None
        self.h.clock.advance(60)
        second = self.h.run()
        self.assertEqual(second.status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])
        self.assertEqual(len(self.h.warehouse()), 10)

    def test_failure_at_any_batch_never_loses_rows(self) -> None:
        for failing_batch in range(1, 7):
            with self.subTest(failing_batch=failing_batch):
                h = EngineHarness(self, batch_size=2)
                h.seed(10)
                h.save("DOC-03", modified=h.clock.advance(300))
                h.clock.advance(5)
                h.clickhouse.on_insert = fail_on_insert(failing_batch, ValueError("boom"))
                h.run()
                h.clickhouse.on_insert = None
                h.clock.advance(30)
                self.assertEqual(h.run().status, "Completed")
                self.assertEqual(h.out_of_sync(), [])

    def test_resume_does_not_restart_from_the_beginning(self) -> None:
        self.h.seed(10, spacing=LAG)  # rows far apart, so the lag re-reads little
        self.h.clickhouse.on_insert = fail_on_insert(4, ValueError("boom"))
        self.h.run()
        self.h.clickhouse.on_insert = None
        self.h.clickhouse.insert_log.clear()
        self.h.clock.advance(1)
        resumed = self.h.run()
        self.assertEqual(resumed.status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])
        # 6 rows were delivered before the failure; far fewer than 10 are re-read.
        self.assertLessEqual(resumed.rows_read, 6)

    def test_edit_during_a_successful_run_is_synced_by_the_next_run(self) -> None:
        self.h.seed(10)
        self.h.run()
        self.h.clock.advance(LAG * 2)
        for i in range(5, 10):
            self.h.save(f"DOC-{i:02d}", title="v2")
            self.h.clock.advance(1)

        selects = {"count": 0}

        def edit_mid_run(query: str, params: dict) -> None:
            # A user saves DOC-05 again after the run has already read it.
            if "ORDER BY modified" in query:
                selects["count"] += 1
                if selects["count"] == 3:
                    self.h.clock.advance(2)
                    self.h.save("DOC-05", title="v3")

        self.h.db.before_query = edit_mid_run
        self.assertEqual(self.h.run().status, "Completed")
        self.h.db.before_query = None

        self.h.clock.advance(LAG * 2)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])
        self.assertEqual(self.h.warehouse()["DOC-05"]["title"], "v3")

    def test_row_committed_late_is_covered_by_the_safety_lag(self) -> None:
        self.h.seed(4)
        stamped = self.h.clock.now  # saved now, but its transaction is still open
        self.h.clock.advance(30)
        self.h.run()
        # The late transaction commits after the run: the row appears with an
        # older ``modified`` than the cursor.
        self.h.save("DOC-LATE", modified=stamped)
        self.h.clock.advance(60)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])

    def test_many_rows_sharing_one_timestamp_are_all_read_once_per_run(self) -> None:
        same = self.h.clock.now
        for i in range(9):
            self.h.save(f"DOC-{i:02d}", modified=same)
        self.h.clock.advance(10)
        result = self.h.run()
        self.assertEqual(result.status, "Completed")
        self.assertEqual(result.rows_read, 9)
        self.assertEqual(self.h.out_of_sync(), [])

    def test_cursor_never_moves_backwards(self) -> None:
        self.h.seed(6)
        self.h.run()
        self.h.clock.advance(60)
        self.h.save("DOC-02", title="v2")
        self.h.clock.advance(60)
        self.h.run()
        history = [c for c in self.h.db.cursor_history if c is not None]
        self.assertEqual(history, sorted(history))

    def test_cancelled_run_leaves_a_safe_cursor(self) -> None:
        self.h.seed(10)
        self.h.save("DOC-00", modified=self.h.clock.advance(100))
        self.h.clock.advance(5)

        def cancel_after_two(n: int, table: str) -> None:
            if n == 2:
                sync_engine.request_cancel(self.h.DOCTYPE)

        self.h.clickhouse.on_insert = cancel_after_two
        self.assertEqual(self.h.run().status, "Cancelled")
        self.h.clickhouse.on_insert = None
        self.h.clock.advance(30)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])

    def test_rows_unreachable_by_modified_are_reported(self) -> None:
        self.h.seed(3)
        self.h.db.upsert(self.h.DOCTYPE, name="DOC-NULL", modified=None, title="v1")
        result = self.h.run()
        self.assertIn("WARNING: 1 row(s) skipped", result.message)

    def test_full_mode_still_reads_every_row_by_primary_key(self) -> None:
        self.h.set_config(sync_mode="Full")
        self.h.seed(7)
        started = self.h.clock.now
        result = self.h.run()
        self.assertEqual(result.status, "Completed")
        self.assertEqual(result.rows_written, 7)
        self.assertEqual(self.h.cursor, started)
        self.assertTrue(any("ORDER BY name ASC" in q for q in self.h.db.queries))
        self.assertFalse(any("ORDER BY modified" in q for q in self.h.db.queries))

    def test_full_mode_failure_does_not_touch_the_cursor(self) -> None:
        self.h.set_config(sync_mode="Full")
        self.h.seed(7)
        self.h.clickhouse.on_insert = fail_on_insert(2, ValueError("boom"))
        self.assertEqual(self.h.run().status, "Failed")
        self.assertIsNone(self.h.cursor)


class TestRandomisedInterleavings(unittest.TestCase):
    """Findings 1 and 2 under arbitrary interleavings of edits and failures.

    Each seed plays a different random history: documents are saved (sometimes
    several with the same timestamp), transactions commit late, runs fail on a
    random batch, are cancelled, or have documents edited underneath them.
    Whatever happened, two quiet runs at the end must leave the warehouse
    identical to the source.
    """

    def _play(self, seed: int) -> None:
        rng = random.Random(seed)
        h = EngineHarness(self, batch_size=rng.choice([1, 2, 3, 5]))
        h.seed(rng.randint(0, 8))
        version = {"n": 0}
        uncommitted: list[tuple[str, dt.datetime, str]] = []  # (name, stamped, title)

        def save_random() -> None:
            version["n"] += 1
            name = f"DOC-{rng.randint(0, 14):02d}"
            title = f"v{version['n']}"
            if rng.random() < 0.2:
                # Stamped now, becomes visible later (an open transaction).
                uncommitted.append((name, h.clock.now, title))
            else:
                h.save(name, title=title)

        def commit_due(force: bool = False) -> None:
            for entry in list(uncommitted):
                name, stamped, title = entry
                age = (h.clock.now - stamped).total_seconds()
                if force or age > 100 or rng.random() < 0.3:
                    current = h.db.rows(h.DOCTYPE).get(name)
                    # A later save of the same document wins, as in a real database.
                    if current is None or current["modified"] <= stamped:
                        h.save(name, title=title, modified=stamped)
                    uncommitted.remove(entry)

        for _ in range(40):
            h.clock.advance(rng.choice([0, 0, 1, 5, 20]))
            commit_due()
            action = rng.random()
            if action < 0.55:
                save_random()
                continue

            attempts_before = h.clickhouse.insert_attempts
            fail_at = attempts_before + rng.randint(1, 4) if rng.random() < 0.4 else None
            cancel_at = attempts_before + rng.randint(1, 4) if rng.random() < 0.15 else None

            def on_insert(n: int, table: str) -> None:
                if n == fail_at:
                    raise ValueError("injected failure")
                if n == cancel_at:
                    sync_engine.request_cancel(h.DOCTYPE)

            def during_run(query: str, params: dict) -> None:
                if rng.random() < 0.3:
                    h.clock.advance(rng.choice([0, 1]))
                    save_random()

            h.clickhouse.on_insert = on_insert
            h.db.before_query = during_run
            h.run()
            h.clickhouse.on_insert = None
            h.db.before_query = None

        commit_due(force=True)
        for _ in range(2):
            h.clock.advance(30)
            self.assertEqual(h.run().status, "Completed")
        self.assertEqual(h.out_of_sync(), [], f"seed {seed}")
        self.assertEqual(len(h.warehouse()), len(h.db.rows(h.DOCTYPE)), f"seed {seed}")

    def test_warehouse_always_converges_to_the_source(self) -> None:
        for seed in range(200):
            with self.subTest(seed=seed):
                self._play(seed)


class TestCancellation(unittest.TestCase):
    """A cancel pressed while a job is running must be seen by that job.

    Frappe caches cache reads per job, so the job's first "not cancelled"
    answer used to be reused for the rest of the run.
    """

    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)

    def test_flag_set_after_a_first_check_is_seen(self) -> None:
        name = self.h.DOCTYPE
        self.assertFalse(sync_engine._is_cancelled(name))
        # Another process (the web request behind the Cancel button) sets it.
        other_process = self.h.frappe.cache()
        other_process.local = {}
        other_process.set_value(sync_engine._cancel_key(name), "1", expires_in_sec=3600)
        self.assertTrue(sync_engine._is_cancelled(name))
        sync_engine._clear_cancel(name)
        self.assertFalse(sync_engine._is_cancelled(name))

    def test_cancel_pressed_mid_run_stops_the_run(self) -> None:
        self.h.seed(10)
        self.h.clock.advance(5)

        def press_cancel_after_second_batch(n: int, table: str) -> None:
            if n == 2:
                web_request = self.h.frappe.cache()
                web_request.local = {}  # a different process: its own local copy
                web_request.set_value(
                    sync_engine._cancel_key(self.h.DOCTYPE), "1", expires_in_sec=3600
                )

        self.h.clickhouse.on_insert = press_cancel_after_second_batch
        result = self.h.run()
        self.assertEqual(result.status, "Cancelled")
        self.assertLess(result.rows_written, 10)


class TestChildTables(unittest.TestCase):
    """Finding 4: a child-table failure must fail the run and be retried."""

    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)
        self.child = self.h.add_child_table()
        self.h.set_config(sync_child_tables=1)
        self.h.seed(3)
        for i in range(3):
            self.h.db.upsert(
                self.child, name=f"row-{i}", modified=self.h.clock.now,
                parent="DOC-00", parenttype=self.h.DOCTYPE, parentfield="items", qty=i,
            )
        self.h.clock.advance(5)

    def test_child_rows_are_written_before_the_parent(self) -> None:
        self.assertEqual(self.h.run().status, "Completed")
        tables = [table for table, _ in self.h.clickhouse.insert_log]
        self.assertEqual(tables[0], "tab_doc_item")
        self.assertLess(tables.index("tab_doc_item"), tables.index("tab_doc"))
        self.assertEqual(len(self.h.warehouse("tab_doc_item")), 3)

    def test_child_failure_fails_the_run_and_keeps_the_cursor(self) -> None:
        self.h.clickhouse.on_insert = fail_on_insert(1, ValueError("child rejected"), "tab_doc_item")
        result = self.h.run()
        self.assertEqual(result.status, "Failed")
        self.assertIn("child rejected", result.message)
        self.assertIsNone(self.h.cursor)

        self.h.clickhouse.on_insert = None
        self.h.clock.advance(60)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(len(self.h.warehouse("tab_doc_item")), 3)
        self.assertEqual(self.h.out_of_sync(), [])

    def test_transient_child_failure_is_retried_within_the_run(self) -> None:
        self.h.clickhouse.on_insert = fail_on_insert(
            1, ConnectionResetError("connection reset by peer"), "tab_doc_item"
        )
        result = self.h.run()
        self.assertEqual(result.status, "Completed")
        self.assertEqual(len(self.h.sleeps), 1)
        self.assertEqual(len(self.h.warehouse("tab_doc_item")), 3)

    def test_child_rows_changed_after_a_failed_parent_run_are_not_lost(self) -> None:
        # Children succeed, the parent then fails part-way; a child edit made
        # afterwards must still be picked up by the next run.
        self.h.seed(6)
        parent_inserts = {"count": 0}

        def fail_second_parent_batch(n: int, table: str) -> None:
            if table == "tab_doc":
                parent_inserts["count"] += 1
                if parent_inserts["count"] == 2:
                    raise ValueError("boom")

        self.h.clickhouse.on_insert = fail_second_parent_batch
        self.assertEqual(self.h.run().status, "Failed")
        self.assertIsNotNone(self.h.cursor)  # the parent got one batch through
        self.h.clickhouse.on_insert = None
        self.h.clock.advance(20)
        self.h.db.upsert(
            self.child, name="row-new", modified=self.h.clock.now,
            parent="DOC-01", parenttype=self.h.DOCTYPE, parentfield="items", qty=9,
        )
        self.h.clock.advance(20)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertIn("row-new", self.h.warehouse("tab_doc_item"))

    def test_incremental_child_pages_share_parent_watermark_and_keyset(self) -> None:
        self.assertEqual(self.h.run().status, "Completed")
        child_queries = [
            (query, params)
            for query, params in self.h.db.query_log
            if "FROM `tabDoc Item`" in query
        ]
        self.assertTrue(child_queries)
        query, params = child_queries[0]
        self.assertIn("modified <= %(upper)s", query)
        self.assertIn("ORDER BY modified ASC, name ASC", query)
        parent_watermarks = [
            params["upper"]
            for query, params in self.h.db.query_log
            if "ORDER BY modified ASC, name ASC" in query
            and "FROM `tabDoc`" in query
        ]
        self.assertIn(params["upper"], parent_watermarks)

    def test_child_edit_after_its_batch_is_read_is_picked_up_next_run(self) -> None:
        self.assertEqual(self.h.run().status, "Completed")
        self.h.clock.advance(LAG * 2)
        first_insert = self.h.clickhouse.insert_attempts + 1

        def edit_child_during_insert(n: int, table: str) -> None:
            if table == "tab_doc_item" and n == first_insert:
                modified = self.h.clock.advance(1)
                self.h.db.upsert(
                    self.child, name="row-0", modified=modified,
                    parent="DOC-00", parenttype=self.h.DOCTYPE, parentfield="items", qty=99,
                )

        self.h.clickhouse.on_insert = edit_child_during_insert
        self.assertEqual(self.h.run().status, "Completed")
        self.h.clickhouse.on_insert = None
        self.assertNotEqual(self.h.warehouse("tab_doc_item")["row-0"]["qty"], 99)

        self.h.clock.advance(1)
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.warehouse("tab_doc_item")["row-0"]["qty"], 99)

    def test_removed_and_reparented_child_rows_are_reconciled(self) -> None:
        self.h.set_config(sync_child_tables=1, delete_strategy="Hard Delete")
        self.assertEqual(self.h.run().status, "Completed")
        self.h.db.delete(self.child, "row-0")
        self.h.db.upsert(
            self.child, name="row-1", modified=self.h.clock.advance(1),
            parent="DOC-02", parenttype="Other DocType", parentfield="items", qty=1,
        )
        self.h.db.upsert(
            self.child, name="row-2", modified=self.h.clock.advance(1),
            parent="DOC-02", parenttype=self.h.DOCTYPE, parentfield="items", qty=2,
        )

        self.assertEqual(self.h.run().status, "Completed")
        rows = self.h.warehouse("tab_doc_item")
        self.assertNotIn("row-0", rows)
        self.assertNotIn("row-1", rows)
        self.assertEqual(rows["row-2"]["parent"], "DOC-02")

    def test_reconciling_shared_child_preserves_other_parent_rows(self) -> None:
        other_parent = "Other DocType"
        self.h.set_config(sync_child_tables=1, delete_strategy="Hard Delete")
        self.h.db.create_table(other_parent, ["name", "modified", "title"])
        self.h.frappe.metas[other_parent] = self.h.frappe.metas[self.h.DOCTYPE]
        self.h.set_config(
            name=other_parent,
            source_doctype=other_parent,
            sync_child_tables=1,
            delete_strategy="Hard Delete",
        )
        self.h.db.upsert(
            self.child, name="deleted-for-doc", modified=self.h.clock.now,
            parent="DOC-00", parenttype=self.h.DOCTYPE, parentfield="items", qty=10,
        )
        self.h.db.upsert(
            self.child, name="reparented-for-doc", modified=self.h.clock.now,
            parent="DOC-00", parenttype=self.h.DOCTYPE, parentfield="items", qty=20,
        )
        self.h.db.upsert(
            self.child, name="other-parent-row", modified=self.h.clock.now,
            parent="OTHER-00", parenttype=other_parent, parentfield="items", qty=30,
        )
        self.h.clock.advance(5)

        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.engine.run_sync(other_parent).status, "Completed")
        self.h.db.delete(self.child, "deleted-for-doc")
        self.h.db.upsert(
            self.child, name="reparented-for-doc", modified=self.h.clock.advance(1),
            parent="OTHER-00", parenttype=other_parent, parentfield="items", qty=20,
        )

        self.assertEqual(self.h.run().status, "Completed")
        rows = self.h.warehouse("tab_doc_item")
        self.assertNotIn("deleted-for-doc", rows)
        self.assertNotIn("reparented-for-doc", rows)
        self.assertIn("other-parent-row", rows)
        reconcile_calls = [
            (sql, params)
            for sql, params in self.h.clickhouse.command_calls
            if "DELETE FROM `wh`.`tab_doc_item`" in sql
        ]
        self.assertTrue(reconcile_calls)
        self.assertTrue(
            all(call[1]["source_parenttype"] == self.h.DOCTYPE for call in reconcile_calls)
        )


class TestParameterizedDeleteNames(unittest.TestCase):
    def test_quotes_and_backslashes_are_bound_for_delete_paths(self) -> None:
        h = EngineHarness(self)
        h.set_config(delete_strategy="Hard Delete", sync_mode="Full")
        name = "quote' and slash\\name"
        h.save(name)
        self.assertEqual(h.run().status, "Completed")
        h.db.delete(h.DOCTYPE, name)
        self.assertEqual(h.run().status, "Completed")
        self.assertNotIn(name, h.warehouse())
        reconcile_call = next(
            (sql, params)
            for sql, params in h.clickhouse.command_calls
            if "DELETE FROM" in sql and params.get("names") == [name]
        )
        self.assertIn("{names:Array(String)}", reconcile_call[0])

        h.clickhouse.tables["tab_doc"][name] = {"name": name, "_ch_is_deleted": 0}
        sync_engine.propagate_delete(h.DOCTYPE, [name])
        propagation_call = h.clickhouse.command_calls[-1]
        self.assertIn("{names:Array(String)}", propagation_call[0])
        self.assertEqual(propagation_call[1], {"names": [name]})
        self.assertNotIn(name, h.warehouse())


class TestRetryAndLogging(unittest.TestCase):
    """Findings 3 and 10: every run ends with a Sync Log; retries are sane."""

    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)
        self.h.seed(4)

    def test_outage_is_recorded_as_failed_and_does_not_raise(self) -> None:
        self.h.connect_error = lambda n: ClickHouseConnectionError("Failed to connect")
        result = self.h.run()  # must not raise
        self.assertEqual(result.status, "Failed")
        self.assertIn("Failed to connect", result.message)
        self.assertEqual(len(self.h.frappe.logs), 1)
        self.assertEqual(self.h.frappe.logs[0].status, "Failed")
        self.assertEqual(len(self.h.sleeps), 2)  # 3 attempts, 2 waits between them
        self.assertIsNone(self.h.cursor)

    def test_recovers_when_clickhouse_comes_back_during_retries(self) -> None:
        self.h.connect_error = lambda n: ClickHouseConnectionError("down") if n <= 2 else None
        result = self.h.run()
        self.assertEqual(result.status, "Completed")
        self.assertEqual(self.h.out_of_sync(), [])
        self.assertEqual(len(self.h.frappe.logs), 1)

    def test_retry_uses_a_fresh_connection(self) -> None:
        self.h.clickhouse.on_insert = fail_on_insert(1, ConnectionResetError("connection reset"))
        self.assertEqual(self.h.run().status, "Completed")
        self.assertIn(True, self.h.connect_calls)  # force_new=True was requested

    def test_job_timeout_ends_the_run_instead_of_restarting_it(self) -> None:
        JobTimeoutException = type("JobTimeoutException", (Exception,), {"__module__": "rq.timeouts"})
        self.h.clickhouse.on_insert = fail_on_insert(
            1, JobTimeoutException("Task exceeded maximum timeout value (21600 seconds)")
        )
        result = self.h.run()
        self.assertEqual(result.status, "Failed")
        self.assertEqual(self.h.sleeps, [])  # no retry
        self.assertEqual(len(self.h.frappe.logs), 1)

    def test_data_error_mentioning_a_gateway_number_is_not_retried(self) -> None:
        self.h.clickhouse.on_insert = fail_on_insert(1, ValueError("Cannot parse 'INV-5034' as Int64"))
        self.assertEqual(self.h.run().status, "Failed")
        self.assertEqual(self.h.sleeps, [])

    def test_server_memory_limit_is_retried(self) -> None:
        self.h.clickhouse.on_insert = fail_on_insert(
            1, Exception("Code: 241. DB::Exception: Memory limit (total) exceeded")
        )
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(len(self.h.sleeps), 1)

    def test_worker_shutdown_writes_a_failed_log_then_propagates(self) -> None:
        def stop(n: int, table: str) -> None:
            raise SystemExit("worker stopping")

        self.h.clickhouse.on_insert = stop
        with self.assertRaises(SystemExit):
            self.h.run()
        self.assertEqual(len(self.h.frappe.logs), 1)
        self.assertEqual(self.h.frappe.logs[0].status, "Failed")
        self.assertIn("interrupted", self.h.frappe.logs[0].message)


class TestAlertMessage(unittest.TestCase):
    """Finding 34: the duration line must appear in alerts."""

    def test_duration_line_is_present(self) -> None:
        h = EngineHarness(self)
        start = dt.datetime(2026, 1, 1, 10, 0, 0)
        result = sync_engine.SyncResult(
            status="Failed", start_time=start, end_time=start + dt.timedelta(seconds=42.5), message="boom"
        )
        text = alerts._format_message(h.DOCTYPE, "Manual", result)
        self.assertIn("Duration: 42.5s", text)

    def test_duration_line_when_times_are_missing(self) -> None:
        EngineHarness(self)
        text = alerts._format_message("Doc", "Manual", sync_engine.SyncResult(status="Failed"))
        self.assertIn("Duration: 0.0s", text)


if __name__ == "__main__":
    unittest.main()
