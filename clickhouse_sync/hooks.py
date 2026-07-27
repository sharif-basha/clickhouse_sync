# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Frappe app hooks for clickhouse_sync.

Declares app metadata, asset bundles, document events (real-time delete
propagation), scheduler events, and exported fixtures.
"""

from __future__ import annotations

app_name = "clickhouse_sync"
app_title = "ClickHouse Sync"
app_publisher = "RySS / Common Ground Initiative"
app_description = "Automatic, high-throughput synchronisation of Frappe (MariaDB) data into ClickHouse for high-performance reporting."
app_email = "data@ryss.example.org"
app_license = "MIT"
app_version = "1.7.0"
required_apps = ["frappe"]

# --------------------------------------------------------------------------- #
# Assets
# --------------------------------------------------------------------------- #
app_include_js = ["/assets/clickhouse_sync/js/clickhouse_sync.js"]
app_include_css = ["/assets/clickhouse_sync/css/clickhouse_sync.css"]

# --------------------------------------------------------------------------- #
# Desk / navigation
# --------------------------------------------------------------------------- #
# The workspace JSON under clickhouse_sync/workspace provides the desk landing
# page and its dashboard cards/charts.

# --------------------------------------------------------------------------- #
# Document events
# --------------------------------------------------------------------------- #
# Propagate deletions in real time for every DocType; the handler filters to
# configured DocTypes internally, so the cost for others is a single lookup.
doc_events = {
    "*": {
        "on_trash": "clickhouse_sync.tasks.propagate_doc_delete",
    }
}

# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
scheduler_events = {
    "hourly_long": [
        "clickhouse_sync.tasks.run_hourly_syncs",
    ],
    "hourly": [
        "clickhouse_sync.tasks.check_sync_staleness",
    ],
    "daily_long": [
        "clickhouse_sync.tasks.run_daily_syncs",
    ],
    "daily": [
        "clickhouse_sync.tasks.send_daily_heartbeat",
    ],
    "cron": {
        # Every 15 minutes: catch incremental configurations promptly.
        "*/15 * * * *": [
            "clickhouse_sync.tasks.run_scheduled_syncs",
        ],
        # 5 minutes after each sync tick: send the consolidated batch digest
        # (only if that batch had failures; healthy batches stay silent).
        "5,20,35,50 * * * *": [
            "clickhouse_sync.tasks.send_batch_digest",
        ],
    },
}

# --------------------------------------------------------------------------- #
# Fixtures (exported with `bench export-fixtures`)
# --------------------------------------------------------------------------- #
fixtures = [
    {
        "dt": "Role",
        "filters": [["name", "in", ["ClickHouse Sync Manager"]]],
    },
    {
        "dt": "Custom DocPerm",
        "filters": [["role", "=", "ClickHouse Sync Manager"]],
    },
]

# --------------------------------------------------------------------------- #
# Installation lifecycle
# --------------------------------------------------------------------------- #
after_install = "clickhouse_sync.install.after_install"
after_migrate = "clickhouse_sync.install.after_migrate"

# --------------------------------------------------------------------------- #
# Jinja (optional helpers available in print formats / web pages)
# --------------------------------------------------------------------------- #
jinja = {
    "methods": [],
}

# --------------------------------------------------------------------------- #
# Testing
# --------------------------------------------------------------------------- #
# before_tests bootstraps a minimal fixture set for CI runs.
before_tests = "clickhouse_sync.install.before_tests"
