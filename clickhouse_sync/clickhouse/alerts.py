# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""Telegram alerts for completed sync runs.

After every sync run (scheduled or manual) an optional Telegram message is sent
summarising the outcome. Whether an alert fires is governed by the per-run
status and the ``alert_on`` setting in **ClickHouse Settings**:

* ``All``      - alert on every run (noisy; useful while validating).
* ``Failures`` - alert only on Failed / partial runs (default; low noise).
* ``Never``    - alerting off.

Credentials come from ClickHouse Settings' own Telegram fields; if those are
blank the module falls back to **Zoom Telegram Settings** (if the zoom_meeting
app is installed) so a single bot can serve both apps. All failures here are
logged and swallowed - an alerting problem must never fail or delay a sync.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import frappe

if TYPE_CHECKING:
    from clickhouse_sync.clickhouse.sync_engine import SyncResult

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 15

# Emoji per status for an at-a-glance read on a phone.
_STATUS_ICON = {
    "Completed": "✅",
    "Success": "✅",
    "Failed": "❌",
    "Cancelled": "⚠️",
    "Partial": "⚠️",
    "Running": "⏳",
}


def _settings():
    return frappe.get_cached_doc("ClickHouse Settings")


def _resolve_credentials() -> "tuple[str, str] | None":
    """Return (bot_token, chat_id), falling back to Zoom Telegram Settings."""
    settings = _settings()
    token = settings.get_password("telegram_bot_token") if settings.get("telegram_bot_token") else None
    chat_id = settings.get("telegram_chat_id")

    if not token and frappe.db.exists("DocType", "Zoom Telegram Settings"):
        try:
            zoom = frappe.get_cached_doc("Zoom Telegram Settings")
            token = zoom.get_password("telegram_bot_token")
            chat_id = chat_id or zoom.get("default_chat_id") or zoom.get("telegram_chat_id")
        except Exception:  # noqa: BLE001
            pass

    if token and chat_id:
        return token, chat_id
    return None


def _should_alert(status: str) -> bool:
    mode = (_settings().get("alert_on") or "Failures").strip()
    if mode == "Never":
        return False
    if mode == "All":
        return True
    # Failures (default): anything that isn't a clean success.
    return status not in ("Completed", "Success")


def _format_message(config_name: str, trigger: str, result: "SyncResult") -> str:
    icon = _STATUS_ICON.get(result.status, "•")
    lines = [
        f"{icon} <b>ClickHouse Sync {result.status}</b>",
        f"Config: <b>{frappe.utils.escape_html(config_name)}</b>",
        f"Trigger: {trigger}",
        f"Rows read: {result.rows_read:,}  |  written: {result.rows_written:,}",
    ]
    if result.rows_failed:
        lines.append(f"Rows failed: {result.rows_failed:,}")
    try:
        lines.append(f"Duration: {result.duration():.1f}s")
    except Exception:  # noqa: BLE001
        pass
    if result.status not in ("Completed", "Success") and result.message:
        msg = frappe.utils.escape_html(str(result.message)[:300])
        lines.append(f"\n<code>{msg}</code>")
    site = frappe.local.site or ""
    if site:
        lines.append(f"\n<i>{site}</i>")
    return "\n".join(lines)


def send_run_alert(config_name: str, trigger: str, result: "SyncResult") -> None:
    """Send a Telegram alert for a finished run, if configured. Never raises.

    In ``Consolidated`` mode, per-run sends are suppressed for scheduled runs
    (the batch digest covers them). Manual runs still alert immediately, since a
    person is waiting on that specific result.
    """
    try:
        mode = _alert_mode()
        if mode == "Never":
            return
        if mode == "Consolidated" and trigger != "Manual":
            return  # the batch digest handles scheduled runs
        # Per Run mode, or a manual run: use the failure/all gate.
        if not _should_alert(result.status):
            return
        creds = _resolve_credentials()
        if not creds:
            return  # alerting simply not configured
        token, chat_id = creds
        message = _format_message(config_name, trigger, result)
        _post(token, chat_id, message)
    except Exception:  # noqa: BLE001 - alerting must never disrupt a sync
        frappe.logger("clickhouse_sync").warning(
            f"Telegram alert failed: {frappe.get_traceback()}"
        )


def _post(token: str, chat_id: str, message: str) -> None:
    import requests

    resp = requests.post(
        _TELEGRAM_API.format(token=token),
        data={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
        timeout=_TIMEOUT,
    )
    if resp.status_code != 200:
        frappe.logger("clickhouse_sync").warning(
            f"Telegram returned {resp.status_code}: {resp.text[:200]}"
        )


@frappe.whitelist()
def send_test_alert() -> dict:
    """Send a test message to verify Telegram wiring from the settings form."""
    frappe.only_for("System Manager")
    creds = _resolve_credentials()
    if not creds:
        return {"ok": False, "message": "No Telegram bot token / chat id configured."}
    token, chat_id = creds
    try:
        _post(token, chat_id, "✅ <b>ClickHouse Sync</b>\nTest alert - Telegram is configured correctly.")
        return {"ok": True, "message": f"Test message sent to chat {chat_id}."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


# --------------------------------------------------------------------------- #
# Consolidated (batch) alerts
# --------------------------------------------------------------------------- #
def _alert_mode() -> str:
    """Return the configured alert mode.

    * ``Consolidated`` (default) - one digest per scheduler batch, sent only
      when the batch had failures; healthy batches stay silent. A daily
      heartbeat confirms the pipeline is alive.
    * ``Per Run`` - one message at the end of every run (legacy behaviour).
    * ``Never`` - no alerts.
    """
    return (_settings().get("alert_mode") or "Consolidated").strip()


def send_batch_digest(window_minutes: int = 20) -> "dict | None":
    """Summarise all runs in the last ``window_minutes`` into one message.

    Called shortly after each scheduler tick. In Consolidated mode it sends a
    single digest covering every configuration that ran, but only when at least
    one failed - so a fully healthy cycle produces no message. Never raises.

    Returns a small summary dict (for logging/tests), or ``None`` if nothing
    was sent.
    """
    try:
        if _alert_mode() != "Consolidated":
            return None
        creds = _resolve_credentials()
        if not creds:
            return None

        cutoff = frappe.utils.add_to_date(frappe.utils.now_datetime(), minutes=-abs(window_minutes))
        logs = frappe.get_all(
            "ClickHouse Sync Log",
            filters={"creation": [">=", cutoff]},
            fields=["configuration", "status", "rows_written", "rows_failed"],
            order_by="creation asc",
        )
        if not logs:
            return None

        # Deduplicate to the latest row per configuration in the window.
        latest: dict[str, dict] = {}
        for row in logs:
            latest[row["configuration"]] = row
        runs = list(latest.values())

        failures = [r for r in runs if r["status"] not in ("Completed", "Success")]
        if not failures:
            # Healthy cycle - stay silent by design.
            return {"sent": False, "reason": "no failures", "runs": len(runs)}

        token, chat_id = creds
        _post(token, chat_id, _format_batch_message(runs, failures))
        return {"sent": True, "runs": len(runs), "failures": len(failures)}
    except Exception:  # noqa: BLE001
        frappe.logger("clickhouse_sync").warning(
            f"batch digest failed: {frappe.get_traceback()}"
        )
        return None


def send_daily_heartbeat() -> "dict | None":
    """Send one summary of the last 24h, regardless of success/failure.

    Confirms the pipeline is alive even on quiet days. Only active in
    Consolidated mode. Never raises.
    """
    try:
        if _alert_mode() != "Consolidated":
            return None
        creds = _resolve_credentials()
        if not creds:
            return None

        cutoff = frappe.utils.add_to_date(frappe.utils.now_datetime(), hours=-24)
        logs = frappe.get_all(
            "ClickHouse Sync Log",
            filters={"creation": [">=", cutoff]},
            fields=["configuration", "status", "rows_written"],
        )
        total = len(logs)
        ok = sum(1 for l in logs if l["status"] in ("Completed", "Success"))
        failed = total - ok
        rows = sum(l["rows_written"] or 0 for l in logs)
        rate = round(ok / total * 100, 1) if total else 100.0
        icon = "✅" if failed == 0 else ("⚠️" if rate >= 90 else "❌")

        lines = [
            f"{icon} <b>ClickHouse Sync - Daily Summary</b>",
            f"Runs (24h): {total}",
            f"Succeeded: {ok}  |  Failed: {failed}",
            f"Success rate: {rate}%",
            f"Rows written: {rows:,}",
        ]
        if failed:
            bad = {}
            for l in logs:
                if l["status"] not in ("Completed", "Success"):
                    bad[l["configuration"]] = bad.get(l["configuration"], 0) + 1
            named = ", ".join(f"{k} ({v})" for k, v in sorted(bad.items()))
            lines.append(f"\nFailing: {frappe.utils.escape_html(named)}")
        site = frappe.local.site or ""
        if site:
            lines.append(f"\n<i>{site}</i>")

        token, chat_id = creds
        _post(token, chat_id, "\n".join(lines))
        return {"sent": True, "total": total, "failed": failed}
    except Exception:  # noqa: BLE001
        frappe.logger("clickhouse_sync").warning(
            f"daily heartbeat failed: {frappe.get_traceback()}"
        )
        return None


def _format_batch_message(runs: list[dict], failures: list[dict]) -> str:
    """Build the consolidated digest message for a batch with failures."""
    ok = [r for r in runs if r["status"] in ("Completed", "Success")]
    total_rows = sum(r["rows_written"] or 0 for r in runs)

    lines = [
        "⚠️ <b>ClickHouse Sync - Batch Report</b>",
        f"{len(ok)} succeeded, {len(failures)} failed of {len(runs)} configs",
        f"Rows written: {total_rows:,}",
        "",
        "<b>Failed:</b>",
    ]
    for r in failures:
        name = frappe.utils.escape_html(r["configuration"])
        lines.append(f"❌ {name}")
    # List the healthy ones compactly so you can see what did work.
    if ok:
        names = ", ".join(frappe.utils.escape_html(r["configuration"]) for r in ok)
        lines.append(f"\n✅ OK: {names}")
    site = frappe.local.site or ""
    if site:
        lines.append(f"\n<i>{site}</i>")
    return "\n".join(lines)


@frappe.whitelist()
def send_test_digest() -> dict:
    """Force-send a batch digest over the last 24h, ignoring the failure gate.

    Lets an operator preview the consolidated message format from the settings
    form without waiting for a real failure.
    """
    frappe.only_for("System Manager")
    creds = _resolve_credentials()
    if not creds:
        return {"ok": False, "message": "No Telegram bot token / chat id configured."}

    cutoff = frappe.utils.add_to_date(frappe.utils.now_datetime(), hours=-24)
    logs = frappe.get_all(
        "ClickHouse Sync Log",
        filters={"creation": [">=", cutoff]},
        fields=["configuration", "status", "rows_written", "rows_failed"],
        order_by="creation asc",
    )
    if not logs:
        return {"ok": False, "message": "No sync runs in the last 24h to summarise."}

    latest: dict = {}
    for row in logs:
        latest[row["configuration"]] = row
    runs = list(latest.values())
    failures = [r for r in runs if r["status"] not in ("Completed", "Success")]

    token, chat_id = creds
    try:
        _post(token, chat_id, _format_batch_message(runs, failures or []))
        return {"ok": True, "message": f"Test digest sent ({len(runs)} configs, {len(failures)} failing)."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}
