# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Frappe app hooks for clickhouse_sync.

Declares app metadata, asset bundles, document events (real-time delete
propagation), scheduler events, and exported fixtures.
"""

from __future__ import annotations

from clickhouse_sync import __version__

app_name = "clickhouse_sync"
app_title = "ClickHouse Sync"
app_publisher = "RySS / Common Ground Initiative"
app_description = "Automatic, high-throughput synchronisation of Frappe (MariaDB) data into ClickHouse for high-performance reporting."
app_email = "data@ryss.example.org"
app_license = "MIT"
app_version = __version__
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
# ``after_delete`` (not ``on_trash``): it runs after Frappe's link checks, so a
# delete Frappe refuses never reaches ClickHouse; the handler only queues the
# name, after commit, and a background job applies it.
doc_events = {
    "*": {
        "after_delete": "clickhouse_sync.tasks.propagate_doc_delete",
    }
}

# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
# The 15-minute cron already queues every scheduler-enabled configuration;
# the former hourly_long / daily_long entries only re-queued the same jobs.
scheduler_events = {
    "hourly": [
        "clickhouse_sync.tasks.check_sync_staleness",
    ],
    "daily": [
        "clickhouse_sync.tasks.send_daily_heartbeat",
        "clickhouse_sync.tasks.purge_old_sync_logs",
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
        # Safety net for queued deletes whose flush job was de-duplicated
        # away while another flush was running.
        "*/5 * * * *": [
            "clickhouse_sync.tasks.flush_pending_deletes",
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
