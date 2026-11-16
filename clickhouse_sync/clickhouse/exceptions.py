"""Typed exception hierarchy for the ClickHouse sync engine.

Distinguishing *transient* failures (network blips, timeouts, ClickHouse
temporarily unavailable) from *permanent* ones (bad SQL, schema conflicts,
misconfiguration) is what lets the retry layer back off intelligently
instead of hammering a broken configuration.
"""

from __future__ import annotations

import re


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


# --------------------------------------------------------------------------- #
# Transient-vs-permanent classification
# --------------------------------------------------------------------------- #
# Classification order (first match wins):
#
#   1. Our own permanent error types, and background-job timeouts -> permanent.
#   2. ClickHouseConnectionError                                  -> transient.
#   3. A ClickHouse server error code in the message              -> by code.
#   4. Python programming/data errors (ValueError, TypeError ...) -> permanent.
#   5. Network exception classes (builtin and driver/http ones)   -> transient.
#   6. Unambiguous network phrases in the message                 -> transient.
#
# Earlier versions matched bare substrings such as "timeout", "socket", "502",
# "503" and "504" anywhere in the message. That retried permanent data errors
# whose text merely contained those characters (a document name like
# "INV-5034", a column called "websocket_id"), retried the whole sync after the
# background-job time limit had already expired, and did NOT retry genuine
# server-side transient conditions such as a memory limit or "too many parts".

# ClickHouse server error codes that are worth retrying. Any other code found
# in a server error message is treated as permanent.
TRANSIENT_CLICKHOUSE_CODES: frozenset[int] = frozenset(
    {
        159,  # TIMEOUT_EXCEEDED
        198,  # DNS_ERROR
        202,  # TOO_MANY_SIMULTANEOUS_QUERIES
        203,  # NO_FREE_CONNECTION
        209,  # SOCKET_TIMEOUT
        210,  # NETWORK_ERROR
        225,  # NO_ZOOKEEPER
        241,  # MEMORY_LIMIT_EXCEEDED
        242,  # TABLE_IS_READ_ONLY
        252,  # TOO_MANY_PARTS
        279,  # ALL_CONNECTION_TRIES_FAILED
        999,  # KEEPER_EXCEPTION
    }
)

# Server errors are rendered as "Code: 241. DB::Exception: ...". Requiring the
# "DB::" suffix stops ordinary text such as "Item Code: 252" from matching.
_CLICKHOUSE_CODE_RE = re.compile(r"Code:\s*(\d+)\.\s*DB::")

# Python errors that signal a bug or bad data. Retrying cannot fix them.
_PERMANENT_EXCEPTION_TYPES: tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    LookupError,
    AttributeError,
    ArithmeticError,
    AssertionError,
    NotImplementedError,
)

# Class names (anywhere in the exception's MRO) raised by the HTTP stack and
# the database drivers for network-level failures. Matched by name so that
# none of those libraries has to be imported here.
_TRANSIENT_EXCEPTION_NAMES: frozenset[str] = frozenset(
    {
        "OperationalError",  # clickhouse-connect / pymysql transport failures
        "InterfaceError",
        "ConnectTimeout",
        "ReadTimeout",
        "Timeout",
        "ConnectTimeoutError",
        "ReadTimeoutError",
        "NewConnectionError",
        "MaxRetryError",
        "ProtocolError",
        "ChunkedEncodingError",
        "IncompleteRead",
        "RemoteDisconnected",
    }
)

# Phrases that only appear in network failures. Kept as a last resort for
# exceptions that arrive as plain ``Exception``/``OSError`` with no error code.
TRANSIENT_ERROR_MARKERS: tuple[str, ...] = (
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
    "eof occurred",
    "max retries exceeded",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "remote end closed connection",
)

# "HTTP 503", "status 502", "response code 504" - the number alone is not enough.
_HTTP_GATEWAY_RE = re.compile(
    r"\b(?:http|status|status code|response code)\W{0,3}50[234]\b", re.IGNORECASE
)


def clickhouse_error_code(exc: BaseException) -> int | None:
    """Return the ClickHouse server error code carried by ``exc``, if any.

    The exception chain is searched as well, because engine errors wrap the
    driver error (``raise ClickHouseQueryError(...) from exc``).
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        match = _CLICKHOUSE_CODE_RE.search(str(current))
        if match:
            return int(match.group(1))
        current = current.__cause__ or current.__context__
    return None


def is_job_timeout(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is the background worker's job time limit.

    RQ raises ``rq.timeouts.JobTimeoutException`` inside the running job when
    the job's timeout expires. The time budget is gone at that point, so the
    run must end rather than start again. Matched by module/name so RQ does
    not have to be importable.
    """
    for cls in type(exc).__mro__:
        module = getattr(cls, "__module__", "") or ""
        if module.split(".")[0] == "rq" and "Timeout" in cls.__name__:
            return True
    return False


def is_transient_error(exc: BaseException) -> bool:
    """Return ``True`` when ``exc`` looks like a retryable, transient failure.

    Args:
        exc: The exception instance to classify.

    Returns:
        ``True`` if the engine should retry the operation, ``False`` for
        errors that will not resolve on their own.
    """
    # 1. Explicitly permanent.
    if isinstance(exc, (ClickHouseConfigurationError, ClickHouseSchemaError, SyncCancelledError)):
        return False
    if is_job_timeout(exc):
        return False

    # 2. Explicitly transient.
    if isinstance(exc, ClickHouseConnectionError):
        return True

    # 3. The server told us exactly what went wrong.
    code = clickhouse_error_code(exc)
    if code is not None:
        return code in TRANSIENT_CLICKHOUSE_CODES

    # 4. Bugs and bad data never fix themselves.
    if isinstance(exc, _PERMANENT_EXCEPTION_TYPES):
        return False

    # 5. Network failures identified by type.
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    if any(cls.__name__ in _TRANSIENT_EXCEPTION_NAMES for cls in type(exc).__mro__):
        return True

    # 6. Last resort: unambiguous network wording.
    message = str(exc).lower()
    if any(marker in message for marker in TRANSIENT_ERROR_MARKERS):
        return True
    return bool(_HTTP_GATEWAY_RE.search(message))
