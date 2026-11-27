# Copyright (c) 2026, Sharif Basha and contributors
"""Rename the module 'ClickHouse Sync' -> 'ClickHouse Warehouse'.

The old module name slugified to ``clickhouse-sync``, which shadowed the
``/app/clickhouse-sync-log`` route (the ClickHouse Sync Log list view). Renaming
the module frees that route. This patch repoints an existing site's records so
``bench migrate`` succeeds after the app's on-disk module folder was renamed to
``clickhouse_warehouse``.

Idempotent: safe to run repeatedly; does nothing once the rename is in place.
"""

from __future__ import annotations

import frappe

OLD = "ClickHouse Sync"
NEW = "ClickHouse Warehouse"


def execute() -> None:
    # If the new module already exists and the old one is gone, nothing to do.
    if frappe.db.exists("Module Def", NEW) and not frappe.db.exists("Module Def", OLD):
        return

    # Ensure the new Module Def exists (points at this app).
    if not frappe.db.exists("Module Def", NEW):
        if frappe.db.exists("Module Def", OLD):
            try:
                frappe.rename_doc("Module Def", OLD, NEW, force=True)
            except Exception:
                _create_new_module()
        else:
            _create_new_module()

    # Repoint every record that referenced the old module.
    doctypes_with_module = [
        "DocType", "Page", "Report", "Workspace",
        "Dashboard", "Dashboard Chart", "Number Card", "Web Form",
    ]
    for dt in doctypes_with_module:
        if not frappe.db.has_column(dt, "module"):
            continue
        for name in frappe.get_all(dt, filters={"module": OLD}, pluck="name"):
            frappe.db.set_value(dt, name, "module", NEW, update_modified=False)

    # Remove the stale old Module Def if it lingers.
    if frappe.db.exists("Module Def", OLD):
        try:
            frappe.delete_doc("Module Def", OLD, force=True, ignore_permissions=True)
        except Exception:
            pass

    frappe.clear_cache()


def _create_new_module() -> None:
    doc = frappe.new_doc("Module Def")
    doc.module_name = NEW
    doc.app_name = "clickhouse_sync"
    doc.insert(ignore_permissions=True)
