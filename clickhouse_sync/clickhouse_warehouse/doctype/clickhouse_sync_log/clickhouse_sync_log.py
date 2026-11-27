# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Controller for ``ClickHouse Sync Log``.

An immutable audit record written once per sync attempt by the sync engine.
It also carries the ``dashboard`` cards/number-charts shown on the linked
configuration and drives the ClickHouse Sync workspace dashboard.
"""

from __future__ import annotations

import frappe
from frappe.model.document import Document


class ClickHouseSyncLog(Document):
    """Immutable log of a single sync run (or retry sequence)."""

    def before_insert(self) -> None:
        """Recompute duration defensively in case it was not pre-populated."""
        if self.start_time and self.end_time and not self.duration:
            self.duration = (
                frappe.utils.get_datetime(self.end_time)
                - frappe.utils.get_datetime(self.start_time)
            ).total_seconds()

    def after_insert(self) -> None:
        """Mirror the terminal status onto the parent configuration."""
        if self.configuration:
            frappe.db.set_value(
                "ClickHouse Sync Configuration",
                self.configuration,
                "last_status",
                self.status,
                update_modified=False,
            )
