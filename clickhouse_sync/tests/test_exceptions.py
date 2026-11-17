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

    # --- Classification by ClickHouse error code (audit finding 10) -------- #
    def test_clickhouse_transient_codes_are_retried(self) -> None:
        for code, text in (
            (241, "Memory limit (total) exceeded"),
            (252, "Too many parts (600)"),
            (202, "Too many simultaneous queries"),
            (209, "Timeout exceeded while reading from socket"),
            (210, "Connection reset by peer"),
        ):
            with self.subTest(code=code):
                self.assertTrue(
                    exc.is_transient_error(Exception(f"Code: {code}. DB::Exception: {text}"))
                )

    def test_clickhouse_permanent_codes_are_not_retried(self) -> None:
        for code, text in (
            (60, "Table wh.tab_x does not exist"),
            (62, "Syntax error: failed at position 503"),
            (16, "No such column websocket_id"),
            (516, "Authentication failed"),
            (53, "Type mismatch, socket timeout mentioned in a value"),
        ):
            with self.subTest(code=code):
                self.assertFalse(
                    exc.is_transient_error(Exception(f"Code: {code}. DB::Exception: {text}"))
                )

    def test_error_code_is_found_through_wrapped_errors(self) -> None:
        try:
            try:
                raise RuntimeError("Code: 252. DB::Exception: Too many parts")
            except RuntimeError as inner:
                raise exc.ClickHouseQueryError("insert failed") from inner
        except exc.ClickHouseQueryError as wrapped:
            self.assertEqual(exc.clickhouse_error_code(wrapped), 252)
            self.assertTrue(exc.is_transient_error(wrapped))

    def test_ordinary_text_is_not_mistaken_for_an_error_code(self) -> None:
        self.assertIsNone(exc.clickhouse_error_code(Exception("Item Code: 252 not found")))

    def test_numbers_and_words_inside_data_are_not_transient(self) -> None:
        self.assertFalse(exc.is_transient_error(ValueError("Cannot parse 'INV-5034' as Int64")))
        self.assertFalse(exc.is_transient_error(Exception("Cannot parse value 'INV-5034'")))
        self.assertFalse(exc.is_transient_error(Exception("Unknown column 'websocket_id'")))
        self.assertFalse(exc.is_transient_error(KeyError("timeout")))

    def test_job_timeout_is_permanent(self) -> None:
        JobTimeoutException = type("JobTimeoutException", (Exception,), {"__module__": "rq.timeouts"})
        error = JobTimeoutException("Task exceeded maximum timeout value (21600 seconds)")
        self.assertTrue(exc.is_job_timeout(error))
        self.assertFalse(exc.is_transient_error(error))

    def test_builtin_network_errors_are_transient(self) -> None:
        self.assertTrue(exc.is_transient_error(ConnectionResetError("reset")))
        self.assertTrue(exc.is_transient_error(TimeoutError()))
        self.assertTrue(exc.is_transient_error(BrokenPipeError()))

    def test_driver_network_errors_are_transient_by_class_name(self) -> None:
        OperationalError = type("OperationalError", (Exception,), {})
        self.assertTrue(exc.is_transient_error(OperationalError("Error executing HTTP request")))

    def test_gateway_status_needs_context(self) -> None:
        self.assertTrue(exc.is_transient_error(Exception("returned response code 502")))
        self.assertFalse(exc.is_transient_error(Exception("row 502 rejected")))


if __name__ == "__main__":
    unittest.main()
