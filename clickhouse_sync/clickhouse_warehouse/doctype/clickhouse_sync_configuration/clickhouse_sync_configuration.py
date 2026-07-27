# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
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

VALID_BATCH_SIZES = (10000, 25000, 50000)


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

    @frappe.whitelist()
    def enqueue_sync(self, trigger: str = "Manual") -> dict:
        """Queue a background sync job for this configuration.

        Returns:
            A dict with the enqueued ``job_id`` so the client can track it.
        """
        frappe.only_for(("System Manager", "ClickHouse Sync Manager"))
        if not self.enabled:
            frappe.throw(_("Enable this configuration before syncing."))

        job_id = f"clickhouse_sync::{self.name}"
        if _job_already_running(job_id):
            frappe.throw(_("A sync for {0} is already running.").format(self.name))

        frappe.enqueue(
            "clickhouse_sync.tasks.run_configuration_sync",
            queue="long",
            timeout=21600,  # 6 hours - large full syncs
            job_id=job_id,
            deduplicate=True,
            config_name=self.name,
            trigger=trigger,
        )
        return {"job_id": job_id, "configuration": self.name}

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


def _job_already_running(job_id: str) -> bool:
    """Return ``True`` if a background job with ``job_id`` is queued/running."""
    try:
        from frappe.utils.background_jobs import get_jobs

        for jobs in get_jobs().values():
            if job_id in jobs:
                return True
    except Exception:  # noqa: BLE001 - dedup check must never block the user
        return False
    return False
