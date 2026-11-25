# ClickHouse Sync

Copy ERPNext / Frappe DocTypes from MariaDB into [ClickHouse](https://clickhouse.com/)
for fast analytical queries, with incremental and full sync, resumable runs,
retries, delete handling, Telegram alerts and a live monitoring dashboard.

Built for organisation to move operational data out of
MariaDB into a columnar warehouse that Superset and the `ai_insights` app can
query without loading the production database.

**Version:** 1.7.0 (plus the unreleased changes below) · **Frappe:** v15 ·
**ClickHouse:** any version with the HTTP interface (tested against 26.6) ·
**License:** MIT

---

## Contents

- [What it does](#what-it-does)
- [How a sync works](#how-a-sync-works)
- [Monitoring dashboard](#monitoring-dashboard)
- [Telegram alerts](#telegram-alerts)
- [Install](#install)
- [Configuration](#configuration)
- [Day-to-day operation](#day-to-day-operation)
- [Querying the warehouse](#querying-the-warehouse)
- [Security](#security)
- [Testing](#testing)
- [Code layout](#code-layout)
- [API](#api)
- [Known limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)

---

## What it does

- **Any DocType, no code.** Create a *ClickHouse Sync Configuration* per
  DocType. The ClickHouse table is created and kept in step with the DocType's
  fields automatically.
- **Incremental sync** reads only records changed since the last successful
  sync position. **Full sync** re-reads the whole table. Either can be run on
  demand from the dashboard, whatever the configuration's saved mode.
- **Resumable and loss-free.** Incremental runs read records in
  `(modified, name)` order and save their position after every batch, so a
  failed, cancelled or timed-out run resumes from the last successful batch
  without skipping anything.
- **Duplicate-safe.** Tables use `ReplacingMergeTree` keyed on `name`;
  re-sending a record replaces the earlier version.
- **Deletes.** Choose per DocType: *Ignore*, *Soft Delete* (flag the row) or
  *Hard Delete* (remove it). Deletes are sent when a document is trashed and
  reconciled on full syncs (optionally on incremental syncs too).
- **Child tables** can be synced alongside their parent (one ClickHouse table
  per child DocType).
- **Resilient.** Transient failures (connection drops, ClickHouse memory
  limits, too many parts, timeouts) are retried with exponential backoff;
  permanent ones (bad data, schema problems) fail fast with a clear category.
  Every run, including failed ones, writes a *ClickHouse Sync Log*.
- **Cancellable.** Stop a running or queued sync from the dashboard; it stops
  after the current batch and keeps its progress.
- **Observable.** A live dashboard, four reports and Telegram alerts.

## How a sync works

```
scheduler / dashboard / form button
        │  enqueue (long queue, de-duplicated per configuration)
        ▼
run_sync(config)
        ├─ ensure ClickHouse table, add new columns, widen types
        ├─ child tables first (if enabled)
        ├─ read source in batches  ──►  insert batch into ClickHouse
        │        └─ after each batch: save position, update live progress
        ├─ reconcile deletes (full mode, or if enabled for incremental)
        ├─ on success: move cursor to the run's start time
        └─ write ClickHouse Sync Log, send alert if configured
```

**The cursor.** `last_sync_timestamp` on the configuration is the sync
position. Each incremental run reads records with
`cursor − 5 minutes ≤ modified ≤ run start`, ordered by `modified, name`.

- The **5-minute overlap** catches records whose transaction committed slightly
  after they were stamped. Re-reading them is harmless, because ClickHouse
  keeps one version per record. The dashboard shows these as *Re-checked*.
- Capping each run at its **start time** means an edit made while a run is in
  progress is picked up by the next run, never skipped.
- The cursor only moves forward, and a failed run leaves it at the last batch
  that succeeded.

Records with an empty or future `modified` cannot be reached by incremental
sync. They are counted and reported in the run message; a full sync picks
them up.

**Full mode** reads the table in primary-key order and only moves the cursor
after the whole run succeeds.

## Monitoring dashboard

Open **`/app/clickhouse-sync-dashboard`**. It requires the System Manager or
ClickHouse Sync Manager role.

| Section | What it shows |
| --- | --- |
| Health | One verdict (Healthy / Attention / Action needed) with the reason, and whether ClickHouse is reachable |
| Overall statistics | DocTypes configured, source records (MariaDB), warehouse records (ClickHouse), pending records, failed records and runs, synchronized %, last successful sync |
| Live synchronization | One card per queued or running sync: phase, progress, records processed / written / inserted / updated / re-checked / skipped / failed, batch, cursor, speed, elapsed, time remaining, retry attempt, Stop |
| DocTypes | Per DocType: status, source and warehouse counts, difference, pending, synced %, last success and failure, with Incremental / Full / Stop / Logs buttons; sortable and filterable |
| Errors | Recent failed runs with category, batch, retries and failed records; a full report per run (message, traceback, records in the failed batch); DocTypes failing now, with a retry button |
| Analytics | Records written per day, runs per day by outcome, warehouse table sizes, recent runs, for 7 / 30 / 90 days |

Page buttons: **Incremental Sync (All)**, **Start → Full Sync (All) / Retry
Failed**, **Stop All**, **Refresh**; the menu has **Recount Records** and links
to configurations, logs, reports and settings.

**Where the numbers come from.** Nothing is estimated in the browser.

- **Live progress** is written by the sync worker to Redis after every batch
  (`clickhouse_sync.clickhouse.live_status`).
- **Source records** are `COUNT(*)` in MariaDB, limited to 10 seconds per
  table. Very large tables fall back to the database's row estimate, marked ≈.
- **Warehouse records** are `SELECT count() … FINAL` in ClickHouse, excluding
  soft-deleted rows.
- **Pending** counts records with `modified` after the DocType's cursor. It is
  always current; the other counts are cached for 10 minutes (menu → Recount
  Records forces a recount).
- **Synchronized %** is (source − pending) ÷ source.
- **Failed records** are the records in batches ClickHouse rejected, summed
  over failed runs in the selected window.
- **Inserted / Updated / Re-checked** are only known for incremental runs that
  already have a previous sync position; otherwise the card shows a dash.

The live monitor refreshes every 3 seconds while a sync is queued or running;
the whole page every 30 seconds. Polling pauses while the page or browser tab
is hidden. A running sync that reports no progress for 10 minutes is flagged
*Stalled*, and as a lost worker if its job has left the queue.

## Telegram alerts

Configure in **ClickHouse Settings → Alert Mode**:

- **Consolidated** (default): one digest per scheduler tick, sent only when a
  run failed, plus a once-daily heartbeat so you know the pipeline is alive.
- **Per Run**: a message after every run, filtered by **Alert On**
  (Failures / All / Never).
- **Never**: no alerts.

Credentials live in ClickHouse Settings, or fall back to *Zoom Telegram
Settings* if the `zoom_meeting` app is installed, so one bot can serve both.
**Send Test Digest** / **Send Test Telegram Alert** on the settings form
verify the wiring.

An hourly check also notifies System Managers when a configuration has not
completed a sync within **Staleness Alert Hours** (0 disables it).

## Install

```bash
cd ~/frappe-bench
bench get-app clickhouse_sync https://github.com/sharif-basha/clickhouse_sync.git
bench --site <site> install-app clickhouse_sync
bench restart        # or restart `bench start` in development
```

Python dependencies (`clickhouse-connect`, `requests`) install with the app.
You need a ClickHouse server reachable over its HTTP interface (default port
8123, or 8443 with TLS) and a ClickHouse user that can create tables in the
target database.

**Updating an existing install**

```bash
cd ~/frappe-bench/apps/clickhouse_sync && git pull
cd ~/frappe-bench
bench --site <site> migrate
bench --site <site> clear-cache
bench restart
```

Then reload Desk from the avatar menu → **Reload**, so the browser drops its
cached copy of the dashboard script.

## Configuration

### 1. ClickHouse Settings

| Field | Notes |
| --- | --- |
| Host, Port, Database, Username, Password | Connection. The password is stored encrypted. |
| Secure | Use HTTPS. Turn this on for any server not on the same machine or private network. |
| Compression | Compress inserts (recommended). |
| Connection Timeout, Send/Receive Timeout | Seconds. |
| Max Threads, Max Insert Block Size | Passed to ClickHouse as query settings. |
| Staleness Alert Hours | See Telegram alerts. |
| Alert Mode, Alert On, Telegram Bot Token, Telegram Chat ID | See Telegram alerts. |

### 2. ClickHouse Sync Configuration (one per DocType)

| Field | Notes |
| --- | --- |
| Source DocType | The DocType to copy. The table is named `tab_` + the DocType in lower case with spaces as `_` (for example `tab_sales_invoice`). |
| Enabled / Scheduler Enabled / Priority | Disabled configurations are listed on the dashboard but not synced or totalled. Lower priority numbers are queued first. |
| Sync Mode | **Incremental** (recommended) or **Full**. |
| Batch Size | Records per batch, kept between 1,000 and 100,000 (default 25,000). Use 5,000–10,000 for DocTypes with large text fields. |
| Auto Create Table / Auto Update Schema | Create the table and add new columns automatically. |
| Optimize After Sync / Optimize Only On Full | Run `OPTIMIZE TABLE … FINAL` after a run. This rewrites the table, so on large tables enable *Optimize Only On Full* and query with `FINAL`. |
| Delete Strategy | Ignore, Soft Delete (`_ch_is_deleted = 1`) or Hard Delete. |
| Reconcile Deletes (Incremental) | Also compare full key sets on incremental runs. Costly on large tables. |
| Sync Child Tables | Copy the DocType's child tables too. |
| Excluded Fields | Fields never copied (use the *Select Fields to Exclude* picker). Password fields and names such as `api_key`, `*_secret` and `*_token` are always excluded. |
| Retry Count / Retry Interval | Attempts per run and the base wait in seconds, doubled per attempt. |
| Last Sync Timestamp | The cursor; see [How a sync works](#how-a-sync-works). |

### 3. ClickHouse Sync Log

One record per run, with status, trigger, timings, record counts, message and
traceback. Reports: **Sync Summary**, **Failed Syncs**, **Slowest Syncs**,
**Largest Tables**.

### Roles

**ClickHouse Sync Manager** (shipped as a fixture) and **System Manager** can
use the dashboard, start and stop syncs and read logs.

## Day-to-day operation

| Task | How |
| --- | --- |
| Sync one DocType now | Dashboard → row → **Incremental** or **Full**, or **Sync Now** on the configuration form |
| Sync everything | Dashboard → **Incremental Sync (All)** or **Start → Full Sync (All)** |
| Stop a sync | Dashboard → **Stop** on its card or row, or **Stop All** |
| Retry what failed | Dashboard → **Start → Retry Failed**, or **Retry** in an error report |
| Re-copy a DocType from scratch | Configuration form → **Reset Cursor**, then Sync Now (or run a Full sync from the dashboard) |
| Check source and warehouse agree | Dashboard → DocTypes → *Difference* column; menu → **Recount Records** for fresh counts |
| Investigate a failure | Dashboard → Errors → **Details**, or the Failed Syncs report |

**After upgrading from 1.7.0:** run **Reset Cursor** and then **Sync Now** on
each incremental configuration once. Earlier versions could skip records
after an interrupted run (see the changelog); this re-copies them. Run large
DocTypes outside working hours.

## Querying the warehouse

Always query with `FINAL` to get the latest version of each record. Without it
you may briefly see an older and a newer version of a record that was just
re-synced.

```sql
SELECT customer, sum(grand_total)
FROM erp_warehouse.tab_sales_invoice FINAL
WHERE docstatus = 1
GROUP BY customer;
```

With *Soft Delete*, also add `WHERE _ch_is_deleted = 0`. `_ch_synced_at`
records when each row was written.

## Security

- Every API method checks for the System Manager or ClickHouse Sync Manager
  role. The dashboard's start and stop endpoints accept POST only.
- ClickHouse and Telegram credentials are Password fields (encrypted at rest)
  and never written to logs by this app.
- Sensitive fields are excluded by name pattern and by the Excluded Fields
  list. The sync itself does not apply Frappe user permissions, so anyone who
  can query the ClickHouse database can read every synced DocType. Give BI
  tools a read-only ClickHouse user limited to the warehouse database.
- Use **Secure** (TLS) when ClickHouse is on another host.

## Testing

```bash
# On a bench (includes tests that need Frappe):
bench --site <site> set-config allow_tests true      # test/staging sites only
bench --site <site> run-tests --app clickhouse_sync

# Without a bench, from the app folder (Python 3.10+):
python -m unittest clickhouse_sync.tests.test_stage1_fixes \
  clickhouse_sync.tests.test_dashboard clickhouse_sync.tests.test_exceptions \
  clickhouse_sync.tests.test_schema clickhouse_sync.tests.test_type_mapping
```

The engine and dashboard tests run the real sync code against in-memory
stand-ins (`tests/_fakes.py`): the source database is SQLite executing the
engine's actual SQL, and ClickHouse and Redis are simulated. They cover
cursor resume after failure at every batch, edits during a run, late commits,
200 randomised histories of edits / failures / cancels, retries and error
classification, child tables, cancellation, and every dashboard endpoint
including role checks. No ClickHouse server is needed. Don't run tests on a
production site.

## Code layout

```
clickhouse_sync/
├── clickhouse/
│   ├── sync_engine.py     # run_sync: read, insert, cursor, retries, deletes, logging
│   ├── live_status.py     # live run state in Redis for the dashboard
│   ├── connection.py      # cached clickhouse-connect client, health check
│   ├── schema.py          # table DDL, schema reconciliation, field exclusion
│   ├── type_mapping.py    # Frappe field type -> ClickHouse type, value coercion
│   ├── exceptions.py      # error types, transient/permanent classification, categories
│   └── alerts.py          # Telegram messages
├── api/
│   ├── api.py             # configuration-form and legacy endpoints
│   └── dashboard.py       # dashboard endpoints
├── tasks.py               # scheduler and background-job entry points
├── hooks.py               # scheduler events, on_trash hook, assets, fixtures
├── clickhouse_warehouse/  # module: DocTypes, dashboard page, reports, workspace
└── tests/
```

## API

All methods are whitelisted under `clickhouse_sync.api` and need one of the
two roles.

| Method | HTTP | Purpose |
| --- | --- | --- |
| `dashboard.overview(days=7)` | GET | Everything the dashboard shows |
| `dashboard.live()` | GET | Queued and running syncs only |
| `dashboard.error_detail(log)` | GET | Full report for one failed run |
| `dashboard.start_sync(configuration, mode=None)` | POST | Queue one sync; `mode` = `Full` or `Incremental` for a one-off run |
| `dashboard.start_all(mode=None)` | POST | Queue every enabled configuration |
| `dashboard.retry_failed()` | POST | Queue configurations whose latest run failed |
| `dashboard.stop_sync(configuration)` / `dashboard.stop_all()` | POST | Stop after the current batch |
| `dashboard.recount(configuration=None)` | POST | Recompute record counts now |
| `api.sync_now(configuration)`, `api.sync_all()`, `api.cancel_sync(configuration)`, `api.test_connection()`, `api.schema_status(configuration)`, `api.table_status()`, `api.analytics(days)` | any | Used by the configuration form and the earlier dashboard |

From Python, `clickhouse_sync.clickhouse.sync_engine.run_sync(config_name,
trigger="Manual", mode=None)` runs a sync in the current process and returns
the result. `clickhouse_sync.clickhouse.connection.get_client()` returns a
ClickHouse client and is reused by `ai_insights`.

## Known limitations

These are documented in the audit report and scheduled for the next stages:

- **Schedule.** Every scheduler-enabled configuration is queued every 15
  minutes, and again hourly and daily; there is no per-configuration
  frequency yet. Full-mode configurations therefore reload their whole table
  on every tick: prefer Incremental, or turn off *Scheduler Enabled* for large
  Full-mode DocTypes and run them on demand.
- **Deletes** are sent to ClickHouse from inside the user's delete
  transaction, one ClickHouse mutation per document. Child rows removed from
  a parent are not deleted in ClickHouse, and renamed documents leave their
  old name behind.
- **Mandatory fields** become non-nullable ClickHouse columns; a NULL in such
  a field in old data can make a batch fail.
- **Per-run OPTIMIZE** rewrites the whole table when enabled.
- **Live dashboard state** lives in the Redis cache; a Redis restart clears
  it until the next run. Sync Logs are unaffected. A running sync's log is
  only written when it ends.
- **Failed records** are tracked per batch, not per record. There is no
  dead-letter list of individual records yet.
- Very large initial loads in Incremental mode read in `modified` order,
  which is slower than a Full-mode pass, but they can resume.

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| Dashboard looks unstyled or shows stray `<div …>` text | The browser is running a cached old script. Avatar menu → **Reload**, or open the page in a private window. Check the code is deployed: `grep -c chsync-dash-style clickhouse_sync/clickhouse_warehouse/page/clickhouse_sync_dashboard/clickhouse_sync_dashboard.js` prints 1 or more. |
| "ClickHouse unreachable" | Check host, port and Secure in ClickHouse Settings, and that the server's HTTP interface is reachable from the bench host. |
| Syncs stay *Queued* | No background worker is running the `long` queue. In development keep `bench start` running; in production check `sudo supervisorctl status`. |
| Stop does nothing | The Redis cache isn't reachable (check `redis-cli -p <port> ping` using the `redis_cache` port in `sites/common_site_config.json`). |
| Warehouse count higher than source | Deletes not propagated: the Delete Strategy is *Ignore*, or documents were renamed. Run a Full sync with Soft or Hard Delete. |
| Warehouse count lower than source | Records not yet copied (see *Pending*), or lost by a version before this release: Reset Cursor and Sync Now. |
| Run fails with category *Data* | A value ClickHouse can't store in its column type. **Details** shows the message and the records in the failed batch. |
| `git push` says password authentication is not supported | Use a GitHub personal access token as the password, or SSH. |

## Changelog

### Unreleased (branch `stage1-fixes`)

**Data correctness**

- Incremental sync no longer loses records. Previously a failed, cancelled or
  timed-out run could move the cursor past records it had not read, and
  edits made during a successful run could be skipped. Records are now read
  in `(modified, name)` order up to the run's start time, with a 5-minute
  overlap.
- Child-table failures now fail the run, and child tables sync before the
  parent's cursor moves. Previously failures were ignored and the run
  reported success.
- A ClickHouse outage now produces a Failed log and an alert. Previously the
  job crashed without writing either.
- Errors are classified by type and ClickHouse error code. Job time limits
  are no longer retried, and server memory-limit and too-many-parts errors
  are.
- The Cancel button now reaches a running sync. Previously Frappe's per-job
  cache hid the request. Stopping a queued sync now works too.
- Alert messages include the run duration again.

**Monitoring**

- New monitoring dashboard with live progress from the sync worker, record
  counts from both databases, error categories and per-run reports, and
  start / stop / retry controls.
- One-off Full or Incremental runs from the dashboard, without changing the
  configuration.
- A Redis outage can no longer fail a sync.

**Tests**

- 101 unit tests that run with or without a bench (106 on a bench).

No DocType changes and no migration needed. After deploying, run Reset Cursor
and Sync Now once per incremental configuration (see
[Day-to-day operation](#day-to-day-operation)).

### 1.7.0 and earlier

See the git history.
