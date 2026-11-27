# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Licensing, links and data-protection safeguards that should not regress."""

from __future__ import annotations

import ast
import re
import types
import unittest
from pathlib import Path
from unittest import mock

from clickhouse_sync.tests._fakes import EngineHarness, ensure_frappe_importable

ensure_frappe_importable()

from clickhouse_sync.clickhouse import alerts, connection  # noqa: E402
from clickhouse_sync.clickhouse.sync_engine import SyncResult  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "clickhouse_sync"


class TestLicensing(unittest.TestCase):
    def test_license_file_exists_and_is_mit(self) -> None:
        # Every source file header points to license.txt.
        text = (ROOT / "license.txt").read_text()
        self.assertIn("MIT License", text)
        self.assertIn("Sharif Basha", text)

    def test_package_metadata_declares_the_license(self) -> None:
        pyproject = (ROOT / "pyproject.toml").read_text()
        self.assertIn('license = { file = "license.txt" }', pyproject)
        self.assertIn("License :: OSI Approved :: MIT License", pyproject)

    def test_readme_carries_the_trademark_notice(self) -> None:
        readme = (ROOT / "README.md").read_text()
        self.assertIn("ClickHouse is a trademark of ClickHouse, Inc.", readme)


class TestLinks(unittest.TestCase):
    def test_no_links_to_accounts_or_domains_we_do_not_own(self) -> None:
        for path in APP.rglob("*.py"):
            if "tests" in path.parts:
                continue
            for url in re.findall(r"https?://[^\s\"')]+", path.read_text()):
                with self.subTest(file=str(path.relative_to(ROOT)), url=url):
                    self.assertNotIn("github.com/ryss/", url)
                    self.assertNotIn("ryss.example.org", url)


class TestAlertDetails(unittest.TestCase):
    def setUp(self) -> None:
        self.h = EngineHarness(self)
        self.result = SyncResult(status="Failed", message="Cannot parse 'Ramesh Kumar' as Int64", log_name="CHSYNC-0001")

    def _message(self, include: int) -> str:
        doc = types.SimpleNamespace(alert_include_details=include)
        doc.get = lambda key, default=None: getattr(doc, key, default)
        with mock.patch.object(alerts, "_settings", lambda: doc):
            return alerts._format_message("Employee", "Scheduler", self.result)

    def test_error_text_is_left_out_by_default(self) -> None:
        text = self._message(0)
        self.assertNotIn("Ramesh Kumar", text)
        self.assertIn("Details: Sync Log CHSYNC-0001", text)

    def test_error_text_is_included_when_enabled(self) -> None:
        self.assertIn("Ramesh Kumar", self._message(1))


class TestEncryptionWarning(unittest.TestCase):
    def test_local_host_detection(self) -> None:
        for host in ("localhost", "127.0.0.1", "127.0.1.1", "::1", "[::1]", "LOCALHOST"):
            with self.subTest(host=host):
                self.assertTrue(connection.is_local_host(host))
        for host in ("10.0.0.5", "192.168.1.20", "clickhouse.internal", "ch.example.com", "", None):
            with self.subTest(host=host):
                self.assertFalse(connection.is_local_host(host))

    def test_settings_validate_warns_about_unencrypted_connections(self) -> None:
        controller = APP / "clickhouse_warehouse" / "doctype" / "clickhouse_settings" / "clickhouse_settings.py"
        tree = ast.parse(controller.read_text())
        validate = next(
            n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "validate"
        )
        calls = {ast.unparse(c.func) for c in ast.walk(validate) if isinstance(c, ast.Call)}
        self.assertIn("self._warn_if_unencrypted", calls)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
