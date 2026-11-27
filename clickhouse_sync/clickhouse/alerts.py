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

import re
from typing import TYPE_CHECKING

import frappe

if TYPE_CHECKING:
    from clickhouse_sync.clickhouse.sync_engine import SyncResult

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 15
# Telegram rejects messages longer than 4096 characters.
_MAX_MESSAGE_CHARS = 4000
_SUCCESS_STATUSES = ("Completed", "Success")
# Runs a person stopped on purpose are not failures worth paging about.
_NOT_FAILURES = _SUCCESS_STATUSES + ("Cancelled",)
LAST_DIGEST_KEY = "clickhouse_sync:last_digest_at"
_MAX_DIGEST_WINDOW_MINUTES = 60
_TOKEN_RE = re.compile(r"bot\d+:[A-Za-z0-9_-]+")


class TelegramError(Exception):
    """A Telegram request failed. The message never contains the bot token."""


def _redact(text: str, token: str | None = None) -> str:
    """Remove a bot token from ``text`` (it travels in the request URL)."""
    if token:
        text = text.replace(token, "<redacted>")
    return _TOKEN_RE.sub("bot<redacted>", text)

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


def _email_recipients() -> list[str]:
    """Addresses in ClickHouse Settings > Alert Email Recipients."""
    try:
        raw = _settings().get("alert_email_recipients") or ""
    except Exception:  # noqa: BLE001 - settings unavailable: no email channel
        return []
    return [e.strip() for e in re.split(r"[,;\n]", raw) if e.strip()]


def _channels() -> "tuple[tuple[str, str] | None, list[str]] | None":
    """The configured alert channels: (Telegram credentials, email recipients).

    ``None`` when neither is configured, i.e. alerting is simply off.
    """
    creds = _resolve_credentials()
    emails = _email_recipients()
    if not creds and not emails:
        return None
    return creds, emails


def _as_html(message: str) -> str:
    return message.replace("\n", "<br>")


def _deliver(channels, message: str, subject: str) -> bool:
    """Send ``message`` to every configured channel.

    Returns ``True`` if at least one channel accepted it. Raises the first
    error only when every channel failed.
    """
    creds, emails = channels
    delivered = False
    errors: list[Exception] = []
    if creds:
        token, chat_id = creds
        try:
            delivered = bool(_post(token, chat_id, message)) or delivered
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    if emails:
        try:
            frappe.sendmail(recipients=emails, subject=subject, message=_as_html(message))
            delivered = True
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    if errors and not delivered:
        raise errors[0]
    return delivered


def _include_details() -> bool:
    """Whether alerts may carry error text (Include Error Details in Alerts)."""
    try:
        return bool(int(_settings().get("alert_include_details") or 0))
    except Exception:  # noqa: BLE001 - unknown: keep details out
        return False


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
        f"Trigger: {frappe.utils.escape_html(str(trigger))}",
        f"Rows read: {result.rows_read:,}  |  written: {result.rows_written:,}",
    ]
    if result.rows_failed:
        lines.append(f"Rows failed: {result.rows_failed:,}")
    # ``duration`` is a property on SyncResult (seconds as a float).
    lines.append(f"Duration: {float(result.duration or 0):.1f}s")
    if result.status not in ("Completed", "Success"):
        # The error text can quote record names or field values, and these
        # channels are outside the site, so it is only included when the
        # operator opts in. The Sync Log always has the full details.
        if _include_details() and result.message:
            msg = frappe.utils.escape_html(str(result.message)[:300])
            lines.append(f"\n<code>{msg}</code>")
        elif result.log_name:
            lines.append(f"Details: Sync Log {frappe.utils.escape_html(result.log_name)}")
    site = frappe.local.site or ""
    if site:
        lines.append(f"\n<i>{site}</i>")
    return "\n".join(lines)


def send_run_alert(config_name: str, trigger: str, result: "SyncResult") -> None:
    """Send a Telegram and/or email alert for a finished run. Never raises.

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
        channels = _channels()
        if not channels:
            return  # alerting simply not configured
        message = _format_message(config_name, trigger, result)
        _deliver(channels, message, f"ClickHouse Sync {result.status}: {config_name}")
    except Exception as exc:  # noqa: BLE001 - alerting must never disrupt a sync
        # Never log the traceback: a requests error carries the request URL,
        # and the bot token is part of that URL.
        frappe.logger("clickhouse_sync").warning(f"Sync alert failed: {_redact(str(exc))}")


def _truncate(message: str) -> str:
    if len(message) <= _MAX_MESSAGE_CHARS:
        return message
    return message[: _MAX_MESSAGE_CHARS - 20].rsplit("\n", 1)[0] + "\n… (truncated)"


def _post(token: str, chat_id: str, message: str) -> bool:
    """Send one message; ``True`` once Telegram accepted it.

    Raises :class:`TelegramError` (token removed) when the request fails.
    """
    import requests

    try:
        resp = requests.post(
            _TELEGRAM_API.format(token=token),
            data={"chat_id": chat_id, "text": _truncate(message), "parse_mode": "HTML"},
            timeout=_TIMEOUT,
        )
    except Exception as exc:  # noqa: BLE001 - re-raise without the token
        raise TelegramError(
            f"Telegram request failed: {type(exc).__name__}: {_redact(str(exc), token)}"
        ) from None
    if resp.status_code != 200:
        frappe.logger("clickhouse_sync").warning(
            f"Telegram returned {resp.status_code}: {_redact(resp.text[:200], token)}"
        )
        return False
    return True


@frappe.whitelist()
def send_test_alert() -> dict:
    """Send a test message to every configured channel from the settings form."""
    frappe.only_for("System Manager")
    channels = _channels()
    if not channels:
        return {"ok": False, "message": "No alert channel configured: add a Telegram bot token and chat id, or email recipients."}
    creds, emails = channels
    try:
        delivered = _deliver(
            channels,
            "✅ <b>ClickHouse Sync</b>\nTest alert - alerts are configured correctly.",
            "ClickHouse Sync: test alert",
        )
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": _redact(str(exc), creds[0] if creds else None)}
    sent_to = ([f"Telegram chat {creds[1]}"] if creds else []) + ([", ".join(emails)] if emails else [])
    if not delivered:
        return {"ok": False, "message": "No channel accepted the test message; check the error log."}
    return {"ok": True, "message": "Test alert sent to " + " and ".join(sent_to) + "."}


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


def _digest_cutoff(window_minutes: int | None) -> "object":
    """Start of the digest window: the end of the previous digest's window.

    Fixed 20-minute windows on a 15-minute schedule overlapped, so a failure
    could be reported twice. The window now starts where the last one ended
    (at most an hour back, e.g. after a scheduler outage).
    """
    now = frappe.utils.now_datetime()
    if window_minutes is not None:
        return frappe.utils.add_to_date(now, minutes=-abs(window_minutes))
    floor = frappe.utils.add_to_date(now, minutes=-_MAX_DIGEST_WINDOW_MINUTES)
    try:
        from clickhouse_sync.clickhouse.live_status import read_fresh

        last = read_fresh(LAST_DIGEST_KEY)  # not the per-job cache copy
        if last:
            last = frappe.utils.get_datetime(last)
            if last > floor:
                return last
    except Exception:  # noqa: BLE001
        pass
    return frappe.utils.add_to_date(now, minutes=-15)


def _remember_digest(now: "object") -> None:
    try:
        frappe.cache().set_value(LAST_DIGEST_KEY, str(now), expires_in_sec=24 * 3600)
    except Exception:  # noqa: BLE001
        pass


def send_batch_digest(window_minutes: int | None = None) -> "dict | None":
    """Summarise the runs that ended since the previous digest into one message.

    Called shortly after each scheduler tick. In Consolidated mode it sends a
    single digest covering every configuration that ran, but only when at least
    one failed - so a fully healthy cycle produces no message. Never raises.

    Returns a small summary dict (for logging/tests), or ``None`` if nothing
    was sent.
    """
    try:
        if _alert_mode() != "Consolidated":
            return None
        channels = _channels()
        if not channels:
            return None

        now = frappe.utils.now_datetime()
        cutoff = _digest_cutoff(window_minutes)
        logs = frappe.get_all(
            "ClickHouse Sync Log",
            filters={"creation": [">", cutoff]},
            fields=["configuration", "status", "rows_written", "rows_failed"],
            order_by="creation asc",
        )
        if not logs:
            _remember_digest(now)  # nothing to report up to now
            return None

        # Deduplicate to the latest row per configuration in the window.
        latest: dict[str, dict] = {}
        for row in logs:
            latest[row["configuration"]] = row
        runs = list(latest.values())

        failures = [r for r in runs if r["status"] not in _NOT_FAILURES]
        if not failures:
            # Healthy cycle - stay silent by design.
            _remember_digest(now)
            return {"sent": False, "reason": "no failures", "runs": len(runs)}

        # Move the window on only once a channel has the message: a failed
        # send must leave these failures in the next digest's window.
        try:
            delivered = _deliver(
                channels,
                _format_batch_message(runs, failures),
                f"ClickHouse Sync: {len(failures)} configuration(s) failed",
            )
        except Exception:
            delivered = False
            raise
        finally:
            # Delivered: the next window starts now. Not delivered: it starts
            # where this one did, so these failures are reported next time.
            _remember_digest(now if delivered else cutoff)
        if not delivered:
            return {"sent": False, "reason": "no channel accepted the message"}
        return {"sent": True, "runs": len(runs), "failures": len(failures)}
    except Exception as exc:  # noqa: BLE001
        frappe.logger("clickhouse_sync").warning(f"batch digest failed: {_redact(str(exc))}")
        return None


def send_daily_heartbeat() -> "dict | None":
    """Send one summary of the last 24h, regardless of success/failure.

    Confirms the pipeline is alive even on quiet days. Only active in
    Consolidated mode. Never raises.
    """
    try:
        if _alert_mode() != "Consolidated":
            return None
        channels = _channels()
        if not channels:
            return None

        cutoff = frappe.utils.add_to_date(frappe.utils.now_datetime(), hours=-24)
        logs = frappe.get_all(
            "ClickHouse Sync Log",
            filters={"creation": [">=", cutoff]},
            fields=["configuration", "status", "rows_written"],
        )
        total = len(logs)
        ok = sum(1 for l in logs if l["status"] in _SUCCESS_STATUSES)
        failed = sum(1 for l in logs if l["status"] not in _NOT_FAILURES)
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
                if l["status"] not in _NOT_FAILURES:
                    bad[l["configuration"]] = bad.get(l["configuration"], 0) + 1
            named = ", ".join(f"{k} ({v})" for k, v in sorted(bad.items()))
            lines.append(f"\nFailing: {frappe.utils.escape_html(named)}")
        site = frappe.local.site or ""
        if site:
            lines.append(f"\n<i>{site}</i>")

        _deliver(channels, "\n".join(lines), "ClickHouse Sync: daily summary")
        return {"sent": True, "total": total, "failed": failed}
    except Exception as exc:  # noqa: BLE001
        frappe.logger("clickhouse_sync").warning(f"daily heartbeat failed: {_redact(str(exc))}")
        return None


def _format_batch_message(runs: list[dict], failures: list[dict]) -> str:
    """Build the consolidated digest message for a batch with failures."""
    ok = [r for r in runs if r["status"] in _SUCCESS_STATUSES]
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
    channels = _channels()
    if not channels:
        return {"ok": False, "message": "No alert channel configured: add a Telegram bot token and chat id, or email recipients."}
    creds = channels[0]

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
    failures = [r for r in runs if r["status"] not in _NOT_FAILURES]

    try:
        _deliver(channels, _format_batch_message(runs, failures or []), "ClickHouse Sync: test digest")
        return {"ok": True, "message": f"Test digest sent ({len(runs)} configs, {len(failures)} failing)."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": _redact(str(exc), creds[0] if creds else None)}
