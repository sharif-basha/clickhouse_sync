# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Tests for the 1.8.0 enhancements: next-run times, pause/resume, the data
freshness card, reporting views, email alerts and the onboarding checklist."""

from __future__ import annotations

import datetime as dt
import json
import types
import unittest
from pathlib import Path
from unittest import mock

from clickhouse_sync.tests._fakes import EngineHarness, ensure_frappe_importable

ensure_frappe_importable()

from clickhouse_sync import tasks  # noqa: E402
from clickhouse_sync.api import api as whitelist_api  # noqa: E402
from clickhouse_sync.clickhouse import alerts, schema  # noqa: E402

MODULE = Path(__file__).resolve().parents[1] / "clickhouse_warehouse"


class TestNextRun(unittest.TestCase):
    NOW = dt.datetime(2026, 6, 1, 12, 7, 30)

    def test_every_tick_configurations_run_at_the_next_quarter_hour(self) -> None:
        self.assertEqual(tasks.next_run_at(15, self.NOW, self.NOW), dt.datetime(2026, 6, 1, 12, 15))

    def test_longer_interval_waits_until_due_then_the_next_tick(self) -> None:
        last = dt.datetime(2026, 6, 1, 11, 30)
        # Due at 12:28 (60 min minus 2 min slack) -> first tick after is 12:30.
        self.assertEqual(tasks.next_run_at(60, last, self.NOW), dt.datetime(2026, 6, 1, 12, 30))

    def test_overdue_or_never_run_means_the_next_tick(self) -> None:
        self.assertEqual(tasks.next_run_at(1440, None, self.NOW), dt.datetime(2026, 6, 1, 12, 15))
        long_ago = dt.datetime(2026, 5, 1)
        self.assertEqual(tasks.next_run_at(60, long_ago, self.NOW), dt.datetime(2026, 6, 1, 12, 15))

    def test_matches_is_due(self) -> None:
        last = dt.datetime(2026, 6, 1, 8, 0)  # due at 13:58 -> next tick 14:00
        nxt = tasks.next_run_at(360, last, self.NOW)
        self.assertEqual(nxt, dt.datetime(2026, 6, 1, 14, 0))
        self.assertTrue(tasks.is_due(360, last, nxt))
        self.assertFalse(tasks.is_due(360, last, nxt - dt.timedelta(minutes=15)))


class TestDashboardScheduleAndPause(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.h.seed(2)
        self.assertEqual(self.h.run().status, "Completed")
        tasks_patch = mock.patch.object(tasks, "now_datetime", self.h.clock)
        tasks_patch.start()
        self.addCleanup(tasks_patch.stop)
        get_dt = mock.patch.object(tasks, "get_datetime", lambda v: dt.datetime.fromisoformat(v) if isinstance(v, str) else v)
        get_dt.start()
        self.addCleanup(get_dt.stop)

    def _row(self) -> dict:
        return self.h.dashboard.overview()["doctypes"][0]

    def test_overview_reports_the_next_run(self) -> None:
        row = self._row()
        self.assertFalse(row["paused"])
        self.assertEqual(row["sync_interval"], 15)
        self.assertIsNotNone(row["next_run"])

    def test_pause_and_resume(self) -> None:
        self.assertEqual(self.h.dashboard.set_paused(self.h.DOCTYPE, 1), {"configuration": self.h.DOCTYPE, "paused": True})
        row = self._row()
        self.assertTrue(row["paused"])
        self.assertIsNone(row["next_run"])
        self.h.dashboard.set_paused(self.h.DOCTYPE, 0)
        self.assertFalse(self._row()["paused"])

    def test_pause_is_post_only_and_role_checked(self) -> None:
        self.assertEqual(self.h.dashboard.set_paused.allowed_http_methods, ["POST"])
        self.h.frappe.deny_roles = True
        with self.assertRaises(PermissionError):
            self.h.dashboard.set_paused(self.h.DOCTYPE, 1)


class TestFreshness(unittest.TestCase):
    NOW = dt.datetime(2026, 6, 1, 12, 0)

    def _summary(self, configs):
        with mock.patch.object(whitelist_api, "_", lambda s: s), mock.patch.object(
            whitelist_api, "get_datetime", lambda v: v
        ):
            return whitelist_api.freshness_summary(configs, self.NOW)

    def test_names_the_least_recently_synced_doctype(self) -> None:
        text = self._summary([
            {"source_doctype": "Sales Invoice", "last_sync_timestamp": self.NOW - dt.timedelta(minutes=12)},
            {"source_doctype": "Item", "last_sync_timestamp": self.NOW - dt.timedelta(hours=2, minutes=5)},
        ])
        self.assertEqual(text, "2 h 5 min ago · Item")

    def test_never_synced_doctypes_come_first(self) -> None:
        text = self._summary([
            {"source_doctype": "Item", "last_sync_timestamp": self.NOW},
            {"source_doctype": "Customer", "last_sync_timestamp": None},
            {"source_doctype": "Supplier", "last_sync_timestamp": None},
        ])
        self.assertEqual(text, "Never synced: Customer (+1 more)")

    def test_nothing_enabled(self) -> None:
        self.assertEqual(self._summary([]), "No DocTypes enabled")


class TestReportingViews(unittest.TestCase):
    def test_view_applies_final_and_hides_soft_deletes(self) -> None:
        h = EngineHarness(self)
        h.set_config(delete_strategy="Soft Delete")
        h.seed(2)
        self.assertEqual(h.run().status, "Completed")
        view = h.clickhouse.views["v_doc"]
        self.assertIn("FROM `wh`.`tab_doc` FINAL", view)
        self.assertIn("WHERE `_ch_is_deleted` = 0", view)

    def test_view_without_soft_delete_has_no_filter_and_is_not_recreated_every_run(self) -> None:
        h = EngineHarness(self)
        h.seed(2)
        self.assertEqual(h.run().status, "Completed")
        self.assertNotIn("WHERE", h.clickhouse.views["v_doc"])
        h.clickhouse.commands.clear()
        h.clock.advance(3600)
        self.assertEqual(h.run().status, "Completed")
        self.assertFalse([c for c in h.clickhouse.commands if "VIEW" in c])

    def test_a_failing_view_never_fails_the_sync(self) -> None:
        h = EngineHarness(self)
        h.seed(1)
        original = h.clickhouse.command

        def no_views(sql, parameters=None):
            if "VIEW" in sql:
                raise RuntimeError("Code: 497. DB::Exception: Not enough privileges")
            return original(sql, parameters)

        h.clickhouse.command = no_views
        self.assertEqual(h.run().status, "Completed")

    def test_view_name(self) -> None:
        self.assertEqual(schema.view_name_for("tab_sales_invoice"), "v_sales_invoice")


class TestEmailAlerts(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.sent_mail: list[dict] = []
        self.h.frappe.sendmail = lambda **kw: self.sent_mail.append(kw)

    def _settings(self, **values):
        doc = types.SimpleNamespace(**values)
        doc.get = lambda key, default=None: getattr(doc, key, default)
        return mock.patch.object(alerts, "_settings", lambda: doc)

    def test_email_only_channel_delivers(self) -> None:
        with self._settings(alert_email_recipients="ops@example.com,\nlead@example.com"), mock.patch.object(
            alerts, "_resolve_credentials", lambda: None
        ):
            channels = alerts._channels()
            self.assertTrue(alerts._deliver(channels, "<b>Hi</b>\nthere", "Subject"))
        self.assertEqual(self.sent_mail[0]["recipients"], ["ops@example.com", "lead@example.com"])
        self.assertEqual(self.sent_mail[0]["message"], "<b>Hi</b><br>there")

    def test_email_still_delivers_when_telegram_fails(self) -> None:
        def broken(*a, **k):
            raise alerts.TelegramError("Telegram request failed")

        with self._settings(alert_email_recipients="ops@example.com"), mock.patch.object(
            alerts, "_resolve_credentials", lambda: ("1:abc", "chat")
        ), mock.patch.object(alerts, "_post", broken):
            self.assertTrue(alerts._deliver(alerts._channels(), "msg", "Subject"))
        self.assertEqual(len(self.sent_mail), 1)

    def test_no_channel_means_alerting_is_off(self) -> None:
        with self._settings(alert_email_recipients=""), mock.patch.object(alerts, "_resolve_credentials", lambda: None):
            self.assertIsNone(alerts._channels())


class TestOnboarding(unittest.TestCase):
    def _load(self, folder: str) -> dict[str, dict]:
        return {json.loads(p.read_text())["name"]: json.loads(p.read_text()) for p in (MODULE / folder).glob("*/*.json")}

    def test_workspace_onboarding_block_points_at_real_steps(self) -> None:
        ws = json.loads((MODULE / "workspace" / "clickhouse_warehouse" / "clickhouse_warehouse.json").read_text())
        names = [b["data"]["onboarding_name"] for b in json.loads(ws["content"]) if b["type"] == "onboarding"]
        onboardings = self._load("module_onboarding")
        steps = self._load("onboarding_step")
        self.assertEqual(names, ["ClickHouse Sync"])
        for step in onboardings["ClickHouse Sync"]["steps"]:
            with self.subTest(step=step["step"]):
                self.assertIn(step["step"], steps)

    def test_steps_refer_to_real_doctypes_fields_and_pages(self) -> None:
        settings = json.loads((MODULE / "doctype" / "clickhouse_settings" / "clickhouse_settings.json").read_text())
        settings_fields = {f["fieldname"] for f in settings["fields"]}
        doctypes = {json.loads(p.read_text())["name"] for p in (MODULE / "doctype").glob("*/*.json") if p.stem == p.parent.name}
        pages = {json.loads(p.read_text())["name"] for p in (MODULE / "page").glob("*/*.json")}
        for name, step in self._load("onboarding_step").items():
            with self.subTest(step=name):
                if step.get("reference_document"):
                    self.assertIn(step["reference_document"], doctypes)
                if step.get("field"):
                    self.assertIn(step["field"], settings_fields)
                if step["action"] == "Go to Page":
                    self.assertIn(step["path"], pages)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
