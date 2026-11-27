# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Controller for ``ClickHouse Sync Configuration``.

Each row maps one Frappe DocType to a ClickHouse table and captures how it
should be synchronised (mode, batching, delete strategy, retry, schedule).
The controller exposes whitelisted actions used by the Desk client script to
enqueue background syncs and preview the generated DDL.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import cint

from clickhouse_sync.clickhouse import connection as ch_connection
from clickhouse_sync.clickhouse import schema as ch_schema
from clickhouse_sync.clickhouse import sync_engine
from clickhouse_sync.clickhouse.sync_engine import parse_excluded_fields


class ClickHouseSyncConfiguration(Document):
    """One DocType-to-ClickHouse synchronisation configuration."""

    def validate(self) -> None:
        """Apply sensible defaults and guard against invalid numeric input."""
        if cint(self.batch_size) <= 0:
            self.batch_size = 25000
        if cint(self.retry_count) <= 0:
            self.retry_count = 3
        if cint(self.retry_interval) <= 0:
            self.retry_interval = 5
        if cint(self.priority) <= 0:
            self.priority = 10
        if not self.delete_strategy:
            self.delete_strategy = "Ignore"
        if not self.sync_mode:
            self.sync_mode = "Incremental"
        # The scheduler ticks every 15 minutes; shorter intervals mean "every tick".
        if cint(self.get("sync_interval_minutes")) < 15:
            self.sync_interval_minutes = 15
        lag = self.get("cursor_safety_lag_seconds")
        if lag is None or lag == "" or cint(lag) < 0:
            self.cursor_safety_lag_seconds = sync_engine.CURSOR_SAFETY_LAG_SECONDS
        elif cint(lag) > sync_engine.MAX_CURSOR_SAFETY_LAG_SECONDS:
            self.cursor_safety_lag_seconds = sync_engine.MAX_CURSOR_SAFETY_LAG_SECONDS
        self._validate_source_doctype()

    def _validate_source_doctype(self) -> None:
        """Only real, readable, table-backed DocTypes can be synced.

        * Single and virtual DocTypes have no ``tab<DocType>`` table.
        * Child DocTypes are synced through their parent's *Sync Child
          Tables* option (they need the parent's ``parenttype`` filter).
        * The person saving must be able to read the DocType: a
          configuration copies every record into the warehouse, so without
          this check the sync role could read any DocType there, bypassing
          Frappe's own permissions.
        * Two DocTypes must not map to the same ClickHouse table.
        """
        if not self.source_doctype:
            return
        meta = frappe.get_meta(self.source_doctype)
        if cint(getattr(meta, "issingle", 0)):
            frappe.throw(_("{0} is a Single DocType and has no table to sync.").format(self.source_doctype))
        if cint(getattr(meta, "is_virtual", 0)):
            frappe.throw(_("{0} is a virtual DocType and has no table to sync.").format(self.source_doctype))
        if cint(getattr(meta, "istable", 0)):
            frappe.throw(
                _("{0} is a child table. Sync its parent DocType with 'Sync Child Tables' enabled.").format(
                    self.source_doctype
                )
            )
        if not (frappe.flags.in_install or frappe.flags.in_migrate or frappe.flags.in_test):
            if not frappe.has_permission(self.source_doctype, "read"):
                frappe.throw(
                    _("You need read permission on {0} to sync it.").format(self.source_doctype),
                    frappe.PermissionError,
                )
        table = ch_schema.table_name_for(self.source_doctype)
        for other in frappe.get_all(
            "ClickHouse Sync Configuration",
            filters={"name": ("!=", self.name or "")},
            pluck="source_doctype",
        ):
            if other and ch_schema.table_name_for(other) == table:
                frappe.throw(
                    _("{0} and {1} would both be written to ClickHouse table {2}.").format(
                        self.source_doctype, other, table
                    )
                )

    def on_update(self) -> None:
        sync_engine.clear_delete_strategy_cache()

    def on_trash(self) -> None:
        sync_engine.clear_delete_strategy_cache()

    @frappe.whitelist()
    def enqueue_sync(self, trigger: str = "Manual") -> dict:
        """Queue a background sync job for this configuration.

        Returns:
            A dict with the enqueued ``job_id`` so the client can track it.
        """
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        if not self.enabled:
            frappe.throw(_("Enable this configuration before syncing."))

        from clickhouse_sync.tasks import enqueue_config_sync

        outcome = enqueue_config_sync(self.name, trigger=trigger, doctype=self.source_doctype)
        if not outcome["queued"]:
            frappe.throw(_("A sync for {0} is already queued or running.").format(self.name))
        return {"job_id": outcome["job_id"], "configuration": self.name}

    @frappe.whitelist()
    def cancel_sync(self) -> dict:
        """Request cancellation of a running sync for this configuration."""
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        sync_engine.request_cancel(self.name)
        return {"cancelled": True, "configuration": self.name}

    @frappe.whitelist()
    def preview_ddl(self) -> dict:
        """Return the CREATE TABLE DDL that would be generated for this config."""
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        table_schema = ch_schema.build_table_schema(
            self.source_doctype,
            include_soft_delete=(self.delete_strategy == "Soft Delete"),
            exclude_fields=set(parse_excluded_fields(self.get("excluded_fields"))),
        )
        database = ch_connection.get_connection_params().database
        return {
            "table_name": table_schema.table_name,
            "ddl": ch_schema.generate_create_table_sql(table_schema, database),
        }

    @frappe.whitelist()
    def refresh_schema(self) -> dict:
        """Ensure the table exists and reconcile its schema immediately."""
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        client = ch_connection.get_client()
        database = ch_connection.get_connection_params().database
        table_schema = ch_schema.build_table_schema(
            self.source_doctype,
            include_soft_delete=(self.delete_strategy == "Soft Delete"),
            exclude_fields=set(parse_excluded_fields(self.get("excluded_fields"))),
        )
        created = ch_schema.ensure_table(
            client, table_schema, database, auto_create=bool(self.auto_create_table)
        )
        diff = ch_schema.reconcile_schema(
            client, table_schema, database, auto_update=bool(self.auto_update_schema)
        )
        return {
            "created": created,
            "added": [c.name for c in diff.added],
            "widened": [w[0] for w in diff.widened],
            "ignored": [i[0] for i in diff.ignored],
        }

    @frappe.whitelist()
    def reset_cursor(self) -> dict:
        """Clear the incremental cursor so the next run performs a full pass."""
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        self.db_set("last_sync_timestamp", None, update_modified=False)
        return {"reset": True}

