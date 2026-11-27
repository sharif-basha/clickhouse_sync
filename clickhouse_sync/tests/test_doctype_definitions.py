# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Checks on the app's DocType definitions that Frappe will not catch itself."""

from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path

DOCTYPE_DIR = Path(__file__).resolve().parents[1] / "clickhouse_warehouse" / "doctype"


def _definitions() -> dict[str, dict]:
    return {
        path.parent.name: json.loads(path.read_text())
        for path in DOCTYPE_DIR.glob("*/*.json")
        if path.stem == path.parent.name
    }


class TestDocTypeDefinitions(unittest.TestCase):
    def test_read_only_doctypes_stay_reachable_in_desk(self) -> None:
        # Frappe leaves a DocType out of Desk's routes (``can_read``) when a
        # user can only read it AND it is flagged ``read_only`` ("User Cannot
        # Search"). The Sync Log is read-only for every role, so with the flag
        # set, /app/clickhouse-sync-log showed "Page not found" for everyone
        # except Administrator.
        for name, definition in _definitions().items():
            perms = definition.get("permissions", [])
            read_only_for_all = perms and not any(p.get("write") or p.get("create") for p in perms)
            with self.subTest(doctype=name):
                if read_only_for_all and not definition.get("istable") and not definition.get("issingle"):
                    self.assertFalse(
                        definition.get("read_only"),
                        f"{name} is read-only for every role, so 'User Cannot Search' would hide it in Desk",
                    )

    def test_sync_log_is_found_and_checked(self) -> None:
        self.assertIn("clickhouse_sync_log", _definitions())


class TestSettingsForm(unittest.TestCase):
    SETTINGS = DOCTYPE_DIR / "clickhouse_settings"

    def test_test_connection_button_calls_the_whitelisted_method(self) -> None:
        # The README and setup steps tell people to press Test Connection; the
        # button was missing even though the server method existed.
        script = (self.SETTINGS / "clickhouse_settings.js").read_text()
        self.assertIn('__("Test Connection")', script)
        self.assertIn('method: "test_connection"', script)

        tree = ast.parse((self.SETTINGS / "clickhouse_settings.py").read_text())
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        self.assertIn("test_connection", methods)
        decorators = [ast.unparse(d) for d in methods["test_connection"].decorator_list]
        self.assertTrue(any(d.startswith("frappe.whitelist") for d in decorators), decorators)

    def test_connection_status_field_exists_and_is_read_only(self) -> None:
        definition = json.loads((self.SETTINGS / "clickhouse_settings.json").read_text())
        field = next(f for f in definition["fields"] if f["fieldname"] == "connection_status")
        self.assertTrue(field.get("read_only"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
