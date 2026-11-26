# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Regression tests for the fixes from the November 2026 code review.

Each test is named after the finding it covers (C = critical, H = high,
M = medium) and runs the engine's real code against the in-memory fakes.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import types
import unittest
from decimal import Decimal
from unittest import mock

from clickhouse_sync.tests._fakes import EngineHarness, FakeMeta, docfield, ensure_frappe_importable

ensure_frappe_importable()

from clickhouse_sync import hooks, tasks  # noqa: E402
from clickhouse_sync.api import api as whitelist_api  # noqa: E402
from clickhouse_sync.clickhouse import alerts, connection, live_status, schema, sync_engine  # noqa: E402
from clickhouse_sync.clickhouse import type_mapping as tm  # noqa: E402
from clickhouse_sync.clickhouse.exceptions import (  # noqa: E402
    ClickHouseConfigurationError,
    ClickHouseConnectionError,
    categorize_error,
    is_transient_error,
)

LIVE_TABLE = {
    "name": "String",
    "owner": "Nullable(String)",
    "creation": "Nullable(DateTime64(6))",
    "modified": "DateTime64(6)",
    "modified_by": "Nullable(String)",
    "docstatus": "Nullable(Int8)",
    "idx": "Nullable(Int64)",
    "_user_tags": "Nullable(String)",
    "_comments": "Nullable(String)",
    "_assign": "Nullable(String)",
    "_liked_by": "Nullable(String)",
    "_ch_synced_at": "DateTime64(3)",
}


def _doc(doctype: str, name: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(doctype=doctype, name=name)


# --------------------------------------------------------------------------- #
# C1 - deletes reach ClickHouse only after commit, in batches
# --------------------------------------------------------------------------- #
class TestC1DeletePropagation(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.h.set_config(delete_strategy="Hard Delete", sync_mode="Full")
        for name in ("A", "B", "C"):
            self.h.save(name)
        self.assertEqual(self.h.run().status, "Completed")
        self.h.clickhouse.command_calls.clear()

    def _delete_in_source(self, *names: str) -> None:
        for name in names:
            self.h.db.delete(self.h.DOCTYPE, name)
            tasks.propagate_doc_delete(_doc(self.h.DOCTYPE, name))

    def test_hook_is_after_delete_not_on_trash(self) -> None:
        events = hooks.doc_events["*"]
        self.assertIn("after_delete", events)
        self.assertNotIn("on_trash", events)

    def test_nothing_reaches_clickhouse_inside_the_request(self) -> None:
        self._delete_in_source("A")
        self.assertEqual(self.h.clickhouse.command_calls, [])
        self.assertIn("A", self.h.warehouse())

    def test_rolled_back_delete_is_never_sent(self) -> None:
        self._delete_in_source("A")
        self.h.db.rollback()  # e.g. Frappe refused it: "linked with ..."
        self.assertEqual(sync_engine.flush_pending_deletes(), 0)
        self.assertIn("A", self.h.warehouse())

    def test_committed_deletes_are_applied_as_one_mutation(self) -> None:
        self._delete_in_source("A", "B")
        self.h.db.commit()
        self.assertTrue(
            any(job.get("job_id") == sync_engine.FLUSH_DELETES_JOB_ID for job in self.h.frappe.enqueued)
        )
        self.assertEqual(sync_engine.flush_pending_deletes(), 2)
        deletes = [c for c in self.h.clickhouse.command_calls if c[0].startswith("DELETE FROM")]
        self.assertEqual(len(deletes), 1)
        self.assertEqual(sorted(deletes[0][1]["names"]), ["A", "B"])
        self.assertNotIn("A", self.h.warehouse())
        self.assertIn("C", self.h.warehouse())

    def test_document_restored_before_the_flush_is_kept(self) -> None:
        self._delete_in_source("A")
        self.h.db.commit()
        self.h.save("A")  # restored from Deleted Documents
        sync_engine.flush_pending_deletes()
        self.assertIn("A", self.h.warehouse())

    def test_transient_failure_requeues_the_batch(self) -> None:
        self._delete_in_source("A")
        self.h.db.commit()
        with mock.patch.object(
            self.h.clickhouse, "command", side_effect=ConnectionResetError("connection reset")
        ):
            sync_engine.flush_pending_deletes()
        self.assertIn("A", self.h.warehouse())
        sync_engine.flush_pending_deletes()
        self.assertNotIn("A", self.h.warehouse())

    def test_transient_failure_requeues_every_untried_doctype(self) -> None:
        # Review finding: only the failing group went back on the queue.
        cache = self.h.frappe.cache()
        for doctype, name in (("Alpha", "A1"), ("Beta", "B1"), ("Beta", "B2"), ("Gamma", "G1")):
            cache.rpush(sync_engine.PENDING_DELETES_KEY, json.dumps([doctype, name]))
        tried = []

        def fail_on_beta(doctype, names):
            tried.append(doctype)
            if doctype == "Beta":
                raise ConnectionResetError("connection reset")
            return len(names)

        with mock.patch.object(sync_engine, "propagate_delete", fail_on_beta):
            sync_engine.flush_pending_deletes()
        self.assertEqual(tried, ["Alpha", "Beta"])
        left = sorted(
            tuple(json.loads(raw)) for raw in cache.lrange(sync_engine.PENDING_DELETES_KEY, 0, -1)
        )
        self.assertEqual(left, [("Beta", "B1"), ("Beta", "B2"), ("Gamma", "G1")])

    def test_unconfigured_doctype_costs_no_queue_entry(self) -> None:
        self.assertFalse(sync_engine.queue_delete("Some Other DocType", "X"))
        self.assertEqual(self.h.db.after_commit.callbacks, [])


# --------------------------------------------------------------------------- #
# C2 - child delete checks are bounded; soft deletes are not re-applied
# --------------------------------------------------------------------------- #
class TestC2ChildReconciliation(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self, batch_size=2)
        self.child = self.h.add_child_table()
        self.h.seed(3)
        for i in range(3):
            self.h.db.upsert(
                self.child, name=f"row-{i}", modified=self.h.clock.now,
                parent="DOC-00", parenttype=self.h.DOCTYPE, parentfield="items", qty=i,
            )
        self.h.clock.advance(5)

    def _full_scans_of_child_table(self) -> list:
        return [
            q for q, _ in self.h.clickhouse.queries
            if "`tab_doc_item`" in q and "ORDER BY `name` ASC LIMIT" in q
        ]

    def test_incremental_run_checks_only_changed_parents(self) -> None:
        self.h.set_config(sync_child_tables=1, delete_strategy="Hard Delete")
        self.assertEqual(self.h.run().status, "Completed")
        self.h.clock.advance(3600)  # well past the safety lag
        self.h.clickhouse.queries.clear()

        # Removing a grid row saves its parent.
        self.h.db.delete(self.child, "row-1")
        self.h.save("DOC-00", modified=self.h.clock.advance(1))
        self.h.clock.advance(5)

        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self._full_scans_of_child_table(), [])
        rows = self.h.warehouse("tab_doc_item")
        self.assertNotIn("row-1", rows)
        self.assertIn("row-0", rows)

    def test_opt_in_keeps_the_full_comparison(self) -> None:
        self.h.set_config(
            sync_child_tables=1, delete_strategy="Hard Delete", reconcile_deletes_incremental=1
        )
        self.assertEqual(self.h.run().status, "Completed")
        self.h.clock.advance(3600)
        self.h.clickhouse.queries.clear()
        self.assertEqual(self.h.run().status, "Completed")
        self.assertTrue(self._full_scans_of_child_table())

    def test_soft_deleted_rows_are_not_flagged_again(self) -> None:
        self.h.set_config(sync_child_tables=1, delete_strategy="Soft Delete", sync_mode="Full")
        self.assertEqual(self.h.run().status, "Completed")
        self.h.db.delete(self.child, "row-1")
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.warehouse("tab_doc_item")["row-1"]["_ch_is_deleted"], 1)

        self.h.clickhouse.command_calls.clear()
        self.assertEqual(self.h.run().status, "Completed")
        updates = [c for c, _ in self.h.clickhouse.command_calls if "UPDATE `_ch_is_deleted` = 1" in c]
        self.assertEqual(updates, [])


# --------------------------------------------------------------------------- #
# C3 - OPTIMIZE FINAL is not run on incremental ticks by default
# --------------------------------------------------------------------------- #
class TestC3Optimize(unittest.TestCase):
    def _optimizes(self, h: EngineHarness) -> list[str]:
        return [c for c in h.clickhouse.commands if c.startswith("OPTIMIZE")]

    def test_incremental_run_skips_optimize_by_default(self) -> None:
        h = EngineHarness(self)
        h.set_config(optimize_after_sync=1)  # optimize_only_on_full left unset
        h.seed(3)
        self.assertEqual(h.run().status, "Completed")
        self.assertEqual(self._optimizes(h), [])

    def test_full_run_still_optimizes(self) -> None:
        h = EngineHarness(self)
        h.set_config(optimize_after_sync=1, sync_mode="Full")
        h.seed(3)
        self.assertEqual(h.run().status, "Completed")
        self.assertEqual(len(self._optimizes(h)), 1)


# --------------------------------------------------------------------------- #
# C4 - money stays exact
# --------------------------------------------------------------------------- #
class TestC4Currency(unittest.TestCase):
    def test_decimal_values_are_not_routed_through_float(self) -> None:
        amount = Decimal("123456789012.123456789")
        self.assertEqual(tm.coerce_value("Currency", amount), amount)
        self.assertEqual(tm.coerce_value("Currency", 0.1), Decimal("0.1"))
        self.assertEqual(tm.coerce_value("Currency", "42.50"), Decimal("42.50"))
        self.assertIsNone(tm.coerce_value("Currency", ""))

    def test_high_precision_keeps_twelve_integer_digits(self) -> None:
        df = types.SimpleNamespace(fieldtype="Currency", fieldname="amount", reqd=0, precision="18")
        self.assertIn("Decimal(30, 18)", tm.build_column_definition(df).ddl)


# --------------------------------------------------------------------------- #
# H1 / H2 / H3 - schema migration and live-type aware inserts
# --------------------------------------------------------------------------- #
class TestSchemaFixes(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.h.seed(2)

    def _existing_table(self, **extra: str) -> None:
        self.h.clickhouse.tables["tab_doc"] = {}
        self.h.clickhouse.columns["tab_doc"] = {**LIVE_TABLE, **extra}

    def test_h1_widening_keeps_the_column_nullable(self) -> None:
        self.h.frappe.metas[self.h.DOCTYPE] = FakeMeta([docfield("title", "Int")])
        self._existing_table(title="Nullable(Int32)")
        self.h.db.conn.execute("UPDATE `tabDoc` SET title = 5")
        self.assertEqual(self.h.run().status, "Completed")
        modify = [c for c in self.h.clickhouse.commands if "MODIFY COLUMN `title`" in c]
        self.assertEqual(modify, ["ALTER TABLE `wh`.`tab_doc` MODIFY COLUMN `title` Nullable(Int64)"])

    def test_h2_unsafe_conversions_are_refused(self) -> None:
        self.assertFalse(schema._can_widen("UInt64", "Int64"))
        self.assertFalse(schema._can_widen("UInt8", "Int8"))
        self.assertFalse(schema._can_widen("Int64", "Float32"))
        self.assertFalse(schema._can_widen("Int64", "Float64"))
        self.assertTrue(schema._can_widen("UInt32", "Int64"))
        self.assertTrue(schema._can_widen("Int32", "Float64"))
        self.assertTrue(schema._can_widen("Decimal(18, 2)", "Decimal(21, 4)"))
        self.assertFalse(schema._can_widen("Decimal(21, 6)", "Decimal(21, 9)"))

    def test_h3_unmigratable_type_change_converts_for_the_live_column(self) -> None:
        # The field became Int in Frappe, but the live column is still String.
        self.h.frappe.metas[self.h.DOCTYPE] = FakeMeta([docfield("title", "Int")])
        self._existing_table(title="Nullable(String)")
        self.h.db.conn.execute("UPDATE `tabDoc` SET title = 7")
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.warehouse()["DOC-00"]["title"], "7")

    def test_h3_missing_column_with_auto_update_off_is_left_out(self) -> None:
        self.h.set_config(auto_update_schema=0)
        self._existing_table()  # no `title` column
        self.assertEqual(self.h.run().status, "Completed")
        self.assertNotIn("title", self.h.warehouse()["DOC-00"])

    def test_integer_values_survive_an_unmigrated_uint8_column(self) -> None:
        # Review finding: a Check field turned Int over a UInt8 column had
        # its values collapsed to 0/1.
        self.h.set_config(auto_update_schema=0)
        self.h.frappe.metas[self.h.DOCTYPE] = FakeMeta([docfield("title", "Int")])
        self._existing_table(title="Nullable(UInt8)")
        self.h.db.conn.execute("UPDATE `tabDoc` SET title = 2")
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.warehouse()["DOC-00"]["title"], 2)

    def test_null_in_an_old_non_nullable_column_uses_its_empty_value(self) -> None:
        self._existing_table(title="String")  # created by an older version for a reqd field
        self.h.db.conn.execute("UPDATE `tabDoc` SET title = NULL")
        self.assertEqual(self.h.run().status, "Completed")
        self.assertEqual(self.h.warehouse()["DOC-00"]["title"], "")


# --------------------------------------------------------------------------- #
# H5 / H6 - connection cache and credential errors
# --------------------------------------------------------------------------- #
def _params(password: str = "secret", **kw) -> connection.ClickHouseConnectionParams:
    values = dict(host="ch", port=8123, database="wh", username="u", password=password)
    values.update(kw)
    return connection.ClickHouseConnectionParams(**values)


class TestConnectionFixes(unittest.TestCase):
    def test_h5_password_change_changes_the_cache_key(self) -> None:
        self.assertNotEqual(_params("old").fingerprint, _params("new").fingerprint)
        self.assertNotEqual(_params().fingerprint, _params(send_receive_timeout=10).fingerprint)
        self.assertNotIn("secret", _params("secret").fingerprint)

    def test_h6_rejected_credentials_are_a_configuration_error(self) -> None:
        calls = []

        def get_client(**kwargs):
            calls.append(kwargs)
            raise Exception("Code: 516. DB::Exception: u: Authentication failed")

        fake_module = types.SimpleNamespace(get_client=get_client)
        with mock.patch.dict(sys.modules, {"clickhouse_connect": fake_module}), mock.patch.object(
            connection.time, "sleep"
        ):
            with self.assertRaises(ClickHouseConfigurationError):
                connection._connect_with_retry(_params(), retries=3)
        self.assertEqual(len(calls), 1)  # not retried

    def test_h6_server_code_wins_over_connection_error_type(self) -> None:
        exc = ClickHouseConnectionError("Code: 516. DB::Exception: Authentication failed")
        self.assertFalse(is_transient_error(exc))
        self.assertEqual(categorize_error(exc), "Configuration")
        self.assertTrue(is_transient_error(ClickHouseConnectionError("connection refused")))


# --------------------------------------------------------------------------- #
# M6 - MariaDB errors are judged by error number
# --------------------------------------------------------------------------- #
class OperationalError(Exception):
    """Same class name as pymysql.err.OperationalError."""


class TestM6MariaDBErrors(unittest.TestCase):
    def test_permanent_errnos_are_not_retried(self) -> None:
        self.assertFalse(is_transient_error(OperationalError(1054, "Unknown column 'x'")))
        self.assertFalse(is_transient_error(OperationalError(1045, "Access denied")))

    def test_transient_errnos_are_retried(self) -> None:
        self.assertTrue(is_transient_error(OperationalError(1205, "Lock wait timeout exceeded")))
        self.assertTrue(is_transient_error(OperationalError(2013, "Lost connection")))

    def test_driver_operational_error_without_errno_is_still_transient(self) -> None:
        self.assertTrue(is_transient_error(OperationalError("HTTP driver received HTTP code 502")))


# --------------------------------------------------------------------------- #
# H7 / M9 - alerts
# --------------------------------------------------------------------------- #
TOKEN = "123456:ABC-def_SECRET"


class TestAlertFixes(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.sent: list[str] = []

    def test_h7_token_never_appears_in_errors(self) -> None:
        def boom(url, **kwargs):
            raise ConnectionError(f"Max retries exceeded with url: /bot{TOKEN}/sendMessage")

        with mock.patch("requests.post", boom):
            with self.assertRaises(alerts.TelegramError) as ctx:
                alerts._post(TOKEN, "chat", "hi")
        self.assertNotIn(TOKEN, str(ctx.exception))
        self.assertNotIn("SECRET", alerts._redact(f"https://api.telegram.org/bot{TOKEN}/x"))

    def _digest_setup(self, logs: list[dict]) -> None:
        def get_all(doctype, filters=None, **kwargs):
            cutoff = filters["creation"][1]
            return [dict(r) for r in logs if r["creation"] > cutoff]

        for patch in (
            mock.patch.object(self.h.frappe, "get_all", get_all),
            mock.patch.object(alerts, "_alert_mode", lambda: "Consolidated"),
            mock.patch.object(alerts, "_resolve_credentials", lambda: (TOKEN, "chat")),
            mock.patch.object(alerts, "_post", self._fake_post),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    post_error: Exception | None = None

    def _fake_post(self, token, chat, msg) -> bool:
        if self.post_error is not None:
            raise self.post_error
        self.sent.append(msg)
        return True

    def test_failed_delivery_is_retried_by_the_next_digest(self) -> None:
        # Review finding: the window moved on before Telegram had the message.
        self._digest_setup([
            {"configuration": "Doc", "status": "Failed", "rows_written": 0, "rows_failed": 2,
             "creation": self.h.clock.now + dt.timedelta(minutes=1)},
        ])
        self.h.clock.advance(5 * 60)
        self.post_error = alerts.TelegramError("Telegram request failed: timeout")
        alerts.send_batch_digest()
        self.assertEqual(self.sent, [])
        self.post_error = None
        self.h.clock.advance(15 * 60)
        alerts.send_batch_digest()
        self.assertEqual(len(self.sent), 1)

    def test_m9_a_failure_is_reported_once(self) -> None:
        start = self.h.clock.now
        self._digest_setup([
            {"configuration": "Doc", "status": "Failed", "rows_written": 0, "rows_failed": 2,
             "creation": start + dt.timedelta(minutes=1)},
        ])
        self.h.clock.advance(5 * 60)
        alerts.send_batch_digest()
        self.h.clock.advance(15 * 60)  # next tick: old 20-minute window still covered it
        alerts.send_batch_digest()
        self.assertEqual(len(self.sent), 1)

    def test_m9_cancelled_runs_are_not_failures(self) -> None:
        self._digest_setup([
            {"configuration": "Doc", "status": "Cancelled", "rows_written": 0, "rows_failed": 0,
             "creation": self.h.clock.now + dt.timedelta(minutes=1)},
        ])
        self.h.clock.advance(5 * 60)
        alerts.send_batch_digest()
        self.assertEqual(self.sent, [])


# --------------------------------------------------------------------------- #
# H8 / M2 / M3 - live state, queueing
# --------------------------------------------------------------------------- #
class TestQueueAndLiveState(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        patch = mock.patch.object(whitelist_api, "frappe", self.h.frappe)
        patch.start()
        self.addCleanup(patch.stop)

    def test_h8_running_count_comes_from_live_state(self) -> None:
        self.assertEqual(whitelist_api.count_running_syncs(), 0)
        live_status.start_run(self.h.DOCTYPE, self.h.DOCTYPE, "Manual", None, 1)
        self.assertEqual(whitelist_api.count_running_syncs(), 1)

    def test_m2_request_for_an_already_queued_job_is_reported_not_faked(self) -> None:
        live_status.mark_queued(self.h.DOCTYPE, self.h.DOCTYPE, "Scheduler", None, "job")
        self.h.job_active = True
        result = self.h.dashboard.start_sync(self.h.DOCTYPE, "Full")
        self.assertEqual(result["queued"], [])
        self.assertEqual(result["skipped"], [self.h.DOCTYPE])
        self.assertEqual(self.h.frappe.enqueued, [])
        self.assertIsNone(live_status.get_live(self.h.DOCTYPE)["mode"])  # not overwritten

    def test_m3_requeue_keeps_the_original_queue_time(self) -> None:
        live_status.mark_queued(self.h.DOCTYPE, None, "Scheduler", None, "job")
        first = live_status.get_live(self.h.DOCTYPE)["queued_at"]
        self.h.clock.advance(900)
        live_status.mark_queued(self.h.DOCTYPE, None, "Scheduler", None, "job")
        self.assertEqual(live_status.get_live(self.h.DOCTYPE)["queued_at"], first)


# --------------------------------------------------------------------------- #
# M5 / M7 - retries
# --------------------------------------------------------------------------- #
class TestRetryFixes(unittest.TestCase):
    def test_m5_backoff_is_capped(self) -> None:
        for attempt in range(1, 15):
            self.assertLessEqual(sync_engine._backoff_delay(5, attempt), 300 * 1.2)
        self.assertGreaterEqual(sync_engine._backoff_delay(5, 1), 5)

    def test_m7_failed_run_rolls_back_before_writing_its_log(self) -> None:
        h = EngineHarness(self)
        h.seed(2)
        h.clickhouse.on_insert = lambda n, t: (_ for _ in ()).throw(ValueError("bad row"))
        result = h.run()
        self.assertEqual(result.status, "Failed")
        self.assertGreaterEqual(h.db.rollbacks, 1)
        self.assertTrue(result.log_name)


# --------------------------------------------------------------------------- #
# H9 - log retention
# --------------------------------------------------------------------------- #
class TestH9LogRetention(unittest.TestCase):
    def _run(self, retention):
        fake = mock.MagicMock()
        fake.db.get_single_value.return_value = retention
        fake.db.count.return_value = 4
        with mock.patch.object(tasks, "frappe", fake), mock.patch.object(
            tasks, "now_datetime", lambda: dt.datetime(2026, 6, 1)
        ), mock.patch.object(tasks, "add_to_date", lambda d, **kw: d + dt.timedelta(**kw)):
            return tasks.purge_old_sync_logs(), fake

    def test_old_logs_are_deleted(self) -> None:
        count, fake = self._run(30)
        self.assertEqual(count, 4)
        args = fake.db.delete.call_args[0]
        self.assertEqual(args[1], {"creation": ("<", dt.datetime(2026, 5, 2))})

    def test_zero_keeps_everything(self) -> None:
        count, fake = self._run(0)
        self.assertEqual(count, 0)
        fake.db.delete.assert_not_called()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
