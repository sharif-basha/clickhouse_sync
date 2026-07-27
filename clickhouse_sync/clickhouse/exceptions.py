"""Typed exception hierarchy for the ClickHouse sync engine.

Distinguishing *transient* failures (network blips, timeouts, ClickHouse
temporarily unavailable) from *permanent* ones (bad SQL, schema conflicts,
misconfiguration) is what lets the retry layer back off intelligently
instead of hammering a broken configuration.
"""

from __future__ import annotations


class ClickHouseSyncError(Exception):
    """Base class for every error raised by the engine."""


class ClickHouseConfigurationError(ClickHouseSyncError):
    """Raised when settings/configuration are missing or invalid.

    This is a *permanent* failure - retrying will not help until an
    operator fixes the configuration.
    """


class ClickHouseConnectionError(ClickHouseSyncError):
    """Raised when a connection to ClickHouse cannot be established.

    Treated as *transient* by the retry layer.
    """


class ClickHouseQueryError(ClickHouseSyncError):
    """Raised when a query/DDL/insert fails on the ClickHouse server."""


class ClickHouseSchemaError(ClickHouseSyncError):
    """Raised when schema reconciliation encounters an incompatible change.

    This is a *permanent* failure and must be surfaced to an operator.
    """


class SyncCancelledError(ClickHouseSyncError):
    """Raised (internally) when a running sync is cancelled by a user."""


# Substrings that identify a ClickHouse/network error as transient. Matching
# is intentionally done on the lowercased string representation because the
# underlying driver raises a fairly wide variety of exception classes.
TRANSIENT_ERROR_MARKERS: tuple[str, ...] = (
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "connection aborted",
    "connection closed",
    "broken pipe",
    "temporarily unavailable",
    "too many simultaneous queries",
    "no route to host",
    "network is unreachable",
    "socket",
    "eof occurred",
    "read timed out",
    "max retries exceeded",
    "service unavailable",
    "502",
    "503",
    "504",
)


def is_transient_error(exc: BaseException) -> bool:
    """Return ``True`` when ``exc`` looks like a retryable, transient failure.

    Args:
        exc: The exception instance to classify.

    Returns:
        ``True`` if the engine should retry the operation, ``False`` for
        errors that will not resolve on their own.
    """
    if isinstance(exc, (ClickHouseConfigurationError, ClickHouseSchemaError, SyncCancelledError)):
        return False
    if isinstance(exc, ClickHouseConnectionError):
        return True

    message = str(exc).lower()
    return any(marker in message for marker in TRANSIENT_ERROR_MARKERS)
