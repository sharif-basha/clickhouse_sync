# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Installation and migration lifecycle hooks for clickhouse_sync."""

from __future__ import annotations

import frappe

ROLE_NAME = "ClickHouse Sync Manager"


def after_install() -> None:
    """Run once after the app is installed on a site."""
    _ensure_role()
    _ensure_default_settings()
    frappe.db.commit()


def after_migrate() -> None:
    """Run after every ``bench migrate`` to keep invariants in place."""
    _ensure_role()
    _ensure_default_settings()
    frappe.db.commit()


def before_tests() -> None:
    """Bootstrap the minimal state the test-suite relies on."""
    _ensure_role()
    _ensure_default_settings()
    frappe.db.commit()


def _ensure_role() -> None:
    """Create the ClickHouse Sync Manager role if it does not already exist."""
    if frappe.db.exists("Role", ROLE_NAME):
        return
    role = frappe.new_doc("Role")
    role.role_name = ROLE_NAME
    role.desk_access = 1
    role.insert(ignore_permissions=True)


def _ensure_default_settings() -> None:
    """Seed the single ClickHouse Settings doc with sane defaults."""
    settings = frappe.get_single("ClickHouse Settings")
    changed = False
    if not settings.port:
        settings.port = 8123
        changed = True
    if not settings.username:
        settings.username = "default"
        changed = True
    if settings.compression is None:
        settings.compression = 1
        changed = True
    if not settings.connection_timeout:
        settings.connection_timeout = 10
        changed = True
    if not settings.send_receive_timeout:
        settings.send_receive_timeout = 300
        changed = True
    if not settings.max_insert_block_size:
        settings.max_insert_block_size = 1048576
        changed = True
    if not settings.default_engine:
        settings.default_engine = "MergeTree"
        changed = True
    if changed:
        settings.flags.ignore_permissions = True
        settings.save(ignore_permissions=True)
