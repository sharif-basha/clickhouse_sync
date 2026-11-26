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
#   2. A ClickHouse server error code in the message              -> by code.
#   3. ClickHouseConnectionError                                  -> transient.
#      MariaDB DB-API errors                                      -> by errno.
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

# MariaDB / MySQL client error numbers worth retrying. PyMySQL raises
# ``OperationalError`` for *every* server error number >= 1000 that it has no
# specific mapping for - including "Unknown column" (1054) and "Access denied"
# (1045) - so the class name alone must not make an error transient.
TRANSIENT_MARIADB_ERRNOS: frozenset[int] = frozenset(
    {
        1040,  # ER_CON_COUNT_ERROR (too many connections)
        1053,  # ER_SERVER_SHUTDOWN
        1205,  # ER_LOCK_WAIT_TIMEOUT
        1213,  # ER_LOCK_DEADLOCK
        1927,  # ER_CONNECTION_KILLED
        2002,  # CR_CONNECTION_ERROR
        2003,  # CR_CONN_HOST_ERROR
        2006,  # CR_SERVER_GONE_ERROR
        2013,  # CR_SERVER_LOST
        2055,  # CR_SERVER_LOST_EXTENDED
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


def _mariadb_errno(exc: BaseException) -> int | None:
    """Return the MySQL/MariaDB error number of a DB-API error, if it has one.

    PyMySQL errors carry ``(errno, message)`` in ``args``. Only exceptions that
    look like DB-API errors (by class name) are inspected, so an arbitrary
    exception whose first argument is an int is not misread.
    """
    names = {cls.__name__ for cls in type(exc).__mro__}
    if not names & {"OperationalError", "InterfaceError", "InternalError", "ProgrammingError", "DatabaseError"}:
        return None
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int) and not isinstance(args[0], bool):
        return args[0]
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

    # 2. The server told us exactly what went wrong. Checked before the
    #    connection-error type, because a failed connect can still carry a
    #    permanent server code (wrong password, unknown database).
    code = clickhouse_error_code(exc)
    if code is not None:
        return code in TRANSIENT_CLICKHOUSE_CODES

    # 3. Explicitly transient.
    if isinstance(exc, ClickHouseConnectionError):
        return True

    # 3b. MariaDB errors: decided by error number, not by class name.
    errno = _mariadb_errno(exc)
    if errno is not None:
        return errno in TRANSIENT_MARIADB_ERRNOS

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


# --------------------------------------------------------------------------- #
# Error categories (for the dashboard and reports)
# --------------------------------------------------------------------------- #
ERROR_CATEGORIES: tuple[str, ...] = (
    "Connection",
    "Timeout",
    "Server capacity",
    "Schema",
    "Configuration",
    "Data",
    "ClickHouse error",
    "Job timeout",
    "Cancelled",
    "Interrupted",
    "Unexpected",
)

# Codes that mean the settings are wrong: unknown user, wrong password,
# authentication failure. Retrying cannot fix them.
CONFIGURATION_CLICKHOUSE_CODES: frozenset[int] = frozenset({192, 193, 194, 195, 516})

_CAPACITY_CODES = frozenset({202, 203, 241, 242, 252})
_TIMEOUT_CODES = frozenset({159, 209})
_CONNECTION_CODES = frozenset({198, 210, 225, 279, 999})


def _category_for_code(code: int) -> str:
    if code in _CAPACITY_CODES:
        return "Server capacity"
    if code in _TIMEOUT_CODES:
        return "Timeout"
    if code in _CONNECTION_CODES:
        return "Connection"
    if code in (16, 47, 53, 60, 81):  # unknown column/identifier/type, missing table/db
        return "Schema"
    if code in (6, 27, 38, 41, 69, 70, 72):  # value parse/convert errors
        return "Data"
    if code in CONFIGURATION_CLICKHOUSE_CODES:  # unknown user / wrong password / auth
        return "Configuration"
    return "ClickHouse error"


def categorize_error(exc: BaseException) -> str:
    """Return a short, stable category name for a sync failure."""
    if isinstance(exc, SyncCancelledError):
        return "Cancelled"
    if is_job_timeout(exc):
        return "Job timeout"
    if isinstance(exc, ClickHouseConfigurationError):
        return "Configuration"
    if isinstance(exc, ClickHouseSchemaError):
        return "Schema"
    code = clickhouse_error_code(exc)
    if code is not None:
        return _category_for_code(code)
    if isinstance(exc, ClickHouseConnectionError):
        return "Connection"
    if isinstance(exc, TimeoutError):
        return "Timeout"
    if isinstance(exc, _PERMANENT_EXCEPTION_TYPES):
        return "Data"
    if is_transient_error(exc):
        return "Connection"
    return "Unexpected"


_TRACEBACK_CLASS_RE = re.compile(r"^([A-Za-z_][\w.]*(?:Error|Exception|Timeout\w*|Exit))\b", re.MULTILINE)


def categorize_text(message: str | None, exception_text: str | None = None) -> str:
    """Best-effort category for a stored Sync Log (message + traceback text).

    Used for logs written before categories were recorded alongside runs.
    """
    text = f"{exception_text or ''}\n{message or ''}"
    if "Sync cancelled" in text or "SyncCancelledError" in text:
        return "Cancelled"
    if "Sync interrupted" in text:
        return "Interrupted"
    if "JobTimeoutException" in text or "maximum timeout value" in text:
        return "Job timeout"
    match = _CLICKHOUSE_CODE_RE.search(text)
    if match:
        return _category_for_code(int(match.group(1)))
    names = {m.split(".")[-1] for m in _TRACEBACK_CLASS_RE.findall(text)}
    if "ClickHouseConfigurationError" in names or "is disabled" in text:
        return "Configuration"
    if "ClickHouseSchemaError" in names:
        return "Schema"
    if "ClickHouseConnectionError" in names or names & {"ConnectionError", "ConnectionResetError", "ConnectionRefusedError", "OperationalError"}:
        return "Connection"
    if names & {"TimeoutError", "ReadTimeout", "ConnectTimeout"}:
        return "Timeout"
    if names & {"ValueError", "TypeError", "KeyError", "IndexError", "AttributeError", "ZeroDivisionError", "OverflowError"}:
        return "Data"
    lowered = text.lower()
    if any(marker in lowered for marker in TRANSIENT_ERROR_MARKERS):
        return "Connection"
    return "Unexpected"
