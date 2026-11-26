# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Controller for the single ``ClickHouse Settings`` DocType.

Holds the connection parameters for the target ClickHouse cluster. Changing
any value invalidates the cached driver clients so the new settings take
effect on the next operation.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.model.document import Document

from clickhouse_sync.clickhouse import connection as ch_connection


class ClickHouseSettings(Document):
    """Single DocType storing ClickHouse connection configuration."""

    def validate(self) -> None:
        """Validate port range and normalise host/database strings."""
        if self.host:
            self.host = self.host.strip()
        if self.database:
            self.database = self.database.strip()

        port = int(self.port or 0)
        if port <= 0 or port > 65535:
            frappe.throw(_("Port must be between 1 and 65535."))

        if int(self.connection_timeout or 0) <= 0:
            self.connection_timeout = 10
        if int(self.send_receive_timeout or 0) <= 0:
            self.send_receive_timeout = 300
        if not self.default_engine:
            self.default_engine = "ReplacingMergeTree"
        self._require_password_for_new_endpoint()

    def _require_password_for_new_endpoint(self) -> None:
        """Changing where credentials go requires typing the password again.

        Otherwise anyone who can edit these settings could point ``host`` at
        a server they control and receive the stored password with the next
        connection (HTTP basic auth).
        """
        if self.is_new():
            return
        endpoint_changed = any(
            self.has_value_changed(field) for field in ("host", "port", "username", "secure")
        )
        if not endpoint_changed:
            return
        # An unchanged Password field holds only a mask of asterisks.
        password = self.password or ""
        if not password or set(password) == {"*"}:
            if self.get_password("password", raise_exception=False):
                frappe.throw(
                    _("Re-enter the password when changing the host, port, user or TLS setting.")
                )

    def on_update(self) -> None:
        """Invalidate cached clients so credential/endpoint changes apply."""
        ch_connection.invalidate_cache()

    @frappe.whitelist()
    def test_connection(self) -> dict:
        """Run a live health check and persist the resulting status string.

        Returns:
            The health-check dict from
            :func:`clickhouse_sync.clickhouse.connection.health_check`.
        """
        frappe.only_for("System Manager")
        health = ch_connection.health_check()
        status = (
            f"OK - ClickHouse {health['server_version']} "
            f"({health['latency_ms']} ms)"
            if health["ok"]
            else f"ERROR - {health['message']}"
        )
        self.db_set("connection_status", status, update_modified=False)
        return health
