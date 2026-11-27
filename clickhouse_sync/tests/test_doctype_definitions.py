# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Checks on the app's DocType definitions that Frappe will not catch itself."""

from __future__ import annotations

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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
