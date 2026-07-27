# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Unit tests for transient-vs-permanent error classification."""

from __future__ import annotations

import unittest

from clickhouse_sync.clickhouse import exceptions as exc


class TestErrorClassification(unittest.TestCase):
    def test_configuration_error_is_permanent(self) -> None:
        self.assertFalse(exc.is_transient_error(exc.ClickHouseConfigurationError("bad")))

    def test_schema_error_is_permanent(self) -> None:
        self.assertFalse(exc.is_transient_error(exc.ClickHouseSchemaError("nope")))

    def test_connection_error_is_transient(self) -> None:
        self.assertTrue(exc.is_transient_error(exc.ClickHouseConnectionError("down")))

    def test_timeout_message_is_transient(self) -> None:
        self.assertTrue(exc.is_transient_error(Exception("Read timed out after 30s")))

    def test_connection_refused_is_transient(self) -> None:
        self.assertTrue(exc.is_transient_error(OSError("Connection refused")))

    def test_generic_value_error_is_permanent(self) -> None:
        self.assertFalse(exc.is_transient_error(ValueError("bad column type")))

    def test_5xx_marker_is_transient(self) -> None:
        self.assertTrue(exc.is_transient_error(Exception("HTTP 503 Service Unavailable")))


if __name__ == "__main__":
    unittest.main()
