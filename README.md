# ClickHouse Sync

Sync ERPNext / Frappe DocTypes into [ClickHouse](https://clickhouse.com/) for
fast analytical queries, with retries, batching, Telegram alerting and an
operational dashboard.

Built for RySS (Rythu Sadhikara Samstha) / APCNF to move operational data out of
MariaDB into a columnar warehouse that Superset and the `ai_insights` app can
query without loading the production database.

**Version:** 1.7.0 · **Frappe:** v15 · **License:** MIT

---

## What it does

- **Scheduled sync** of any DocType into ClickHouse, on a per-configuration
  frequency (15-minute incremental tier, hourly, or daily).
- **Incremental sync** via a `modified`-timestamp cursor — only changed rows are
  read each run, so large tables stay cheap. A **Reset Cursor** action forces a
  full re-scan when needed.
- **Deduplication** using `ReplacingMergeTree` keyed on `_ch_synced_at`; always
  query with `FINAL` to get the latest version of each row.
- **Delete reconciliation** — deletions in Frappe propagate to ClickHouse via a
  separate mechanism (edits ride the cursor; deletes are reconciled).
- **Resilience** — configurable retry count/interval with exponential backoff on
  transient ClickHouse errors; memory-bounded batched inserts via keyset
  pagination that scale to millions of rows; resumable and cancellable runs.
- **Force Sync** — trigger any configuration manually from its form.

## Telegram alerts

Alerts fire after sync runs, configurable in **ClickHouse Settings → Alert Mode**:

- **Consolidated** (default) — one digest per sync batch, sent **only when a run
  failed**; healthy batches stay silent. Plus a once-daily heartbeat summary so
  you know the pipeline is alive.
- **Per Run** — a message after every run (gated to Failures / All / Never).
- **Never** — no alerts.

Credentials live in ClickHouse Settings, or fall back to `Zoom Telegram Settings`
if the `zoom_meeting` app is installed, so one bot can serve both. A **Send Test
Digest** / **Send Test Telegram Alert** button verifies wiring from the settings
form.

## Dashboard

A dashboard page (`/app/clickhouse-sync-dashboard`) shows:

- A **health banner** — one-glance ✅ / ⚠️ / ❌ verdict from success rate.
- A **time-range selector** (7 / 30 / 90 days).
- Metric cards, rows-synced-per-day, status breakdown, rows by DocType, live
  ClickHouse table sizes, and recent runs.

## Install

```bash
bench get-app clickhouse_sync https://github.com/sharif-basha/clickhouse_sync.git
bench --site <sitename> install-app clickhouse_sync
```

Requires a reachable ClickHouse server (HTTP interface). Configure the connection
in **ClickHouse Settings**.

## Configuration

1. **ClickHouse Settings** — connection host/port/database, retry policy, and
   Telegram alert mode.
2. **ClickHouse Sync Configuration** — one per DocType you want synced: the
   source DocType, frequency, and an optional **field exclusion** list (use the
   *Select Fields to Exclude* picker to drop personal/sensitive columns from the
   warehouse).
3. **ClickHouse Sync Log** — a record per run, with status, row counts and
   errors. Reports: Sync Summary, Failed Syncs, Slowest Syncs.

## Extending

Add a new synced source by creating a **ClickHouse Sync Configuration** — no code
required. The public entry point for programmatic use is
`clickhouse_sync.clickhouse.sync_engine.run_sync(config_name, trigger)`. The
connection helper (`clickhouse_sync.clickhouse.connection.get_client`) is reused
by `ai_insights`.

## Operational notes

- ClickHouse tables created before v1.1.0 may be plain `MergeTree` — drop and
  re-sync them to get `ReplacingMergeTree` dedup.
- Always query with `FINAL` to resolve the latest row version; without it you may
  see pre-merge duplicates immediately after an edit syncs.
- The module is named **ClickHouse Warehouse** (from v1.6.0) to keep the module
  route from shadowing the sync-log list route.
