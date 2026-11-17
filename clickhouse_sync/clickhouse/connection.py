"""Connection management for ClickHouse.

Provides a small, process-local connection pool on top of the official
``clickhouse-connect`` driver. Clients are cached per (settings fingerprint,
worker process) so that repeated syncs inside the same worker reuse a live
HTTP session instead of re-establishing one on every batch.

Key behaviours:

* **Caching** - one client per configuration fingerprint per process.
* **Health check** - ``ping`` before handing a cached client back; a dead
  client is transparently discarded and rebuilt.
* **Automatic reconnect** - :func:`get_client` will rebuild on failure.
* **Timeout handling** - connect/send/receive timeouts come from settings.
* **Retry** - connection establishment is retried with exponential backoff.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import frappe

from clickhouse_sync.clickhouse.exceptions import (
    ClickHouseConfigurationError,
    ClickHouseConnectionError,
    is_transient_error,
)

if TYPE_CHECKING:  # pragma: no cover - import only for type checking
    from clickhouse_connect.driver.client import Client


# Process-local cache. The key is the settings fingerprint; the value is a
# live driver client. A lock guards concurrent access from gevent/threaded
# workers within the same process.
_CLIENT_CACHE: dict[str, "Client"] = {}
_CACHE_LOCK = threading.RLock()

DEFAULT_CONNECT_RETRIES = 3
DEFAULT_BACKOFF_BASE = 0.5  # seconds


@dataclass(frozen=True)
class ClickHouseConnectionParams:
    """Immutable, hashable snapshot of the connection parameters.

    The fingerprint derived from these fields keys the client cache, so that
    changing any setting (host, port, credentials, TLS, ...) transparently
    invalidates the cached client.
    """

    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)
    secure: bool = False
    compression: bool = True
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    max_threads: int = 0
    max_insert_block_size: int = 1048576

    @property
    def fingerprint(self) -> str:
        """Return a stable cache key that never leaks the password."""
        return (
            f"{self.host}:{self.port}/{self.database}"
            f"?user={self.username}&secure={int(self.secure)}"
            f"&compress={int(self.compression)}&pid={os.getpid()}"
        )

    def settings(self) -> dict[str, Any]:
        """Return per-session ClickHouse settings derived from the params."""
        settings: dict[str, Any] = {}
        if self.max_threads and self.max_threads > 0:
            settings["max_threads"] = self.max_threads
        if self.max_insert_block_size and self.max_insert_block_size > 0:
            settings["max_insert_block_size"] = self.max_insert_block_size
        return settings


def get_connection_params() -> ClickHouseConnectionParams:
    """Load and validate connection parameters from ClickHouse Settings.

    Returns:
        A populated :class:`ClickHouseConnectionParams`.

    Raises:
        ClickHouseConfigurationError: If the single-doctype settings row is
            missing mandatory fields.
    """
    settings = frappe.get_cached_doc("ClickHouse Settings")

    host = (settings.host or "").strip()
    database = (settings.database or "").strip()
    if not host:
        raise ClickHouseConfigurationError("ClickHouse Settings: 'Host' is required.")
    if not database:
        raise ClickHouseConfigurationError("ClickHouse Settings: 'Database' is required.")

    # get_password decrypts the value stored in the encrypted Password field.
    password = settings.get_password("password", raise_exception=False) or ""

    return ClickHouseConnectionParams(
        host=host,
        port=int(settings.port or (8443 if settings.secure else 8123)),
        database=database,
        username=(settings.username or "default").strip(),
        password=password,
        secure=bool(settings.secure),
        compression=bool(settings.compression) if settings.compression is not None else True,
        connect_timeout=int(settings.connection_timeout or DEFAULT_CONNECT_RETRIES * 10),
        send_receive_timeout=int(getattr(settings, "send_receive_timeout", 0) or 300),
        max_threads=int(settings.max_threads or 0),
        max_insert_block_size=int(settings.max_insert_block_size or 1048576),
    )


def _build_client(params: ClickHouseConnectionParams) -> "Client":
    """Construct a fresh ``clickhouse-connect`` client.

    Raises:
        ClickHouseConfigurationError: If the driver package is not installed.
        ClickHouseConnectionError: If the client cannot be built/connected.
    """
    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ClickHouseConfigurationError(
            "The 'clickhouse-connect' package is not installed. "
            "Run: bench pip install clickhouse-connect"
        ) from exc

    compress: Any = "lz4" if params.compression else False

    try:
        client = clickhouse_connect.get_client(
            host=params.host,
            port=params.port,
            username=params.username,
            password=params.password,
            database=params.database,
            secure=params.secure,
            compress=compress,
            connect_timeout=params.connect_timeout,
            send_receive_timeout=params.send_receive_timeout,
            settings=params.settings(),
            # Identify our sessions in system.query_log for observability.
            client_name="frappe-clickhouse-sync",
            query_limit=0,  # never truncate streaming query results
        )
    except Exception as exc:  # noqa: BLE001 - normalise to our hierarchy
        raise ClickHouseConnectionError(
            f"Could not connect to ClickHouse at {params.host}:{params.port} - {exc}"
        ) from exc

    return client


def _is_alive(client: "Client") -> bool:
    """Return ``True`` if a cached client still responds to ``ping``."""
    try:
        return bool(client.ping())
    except Exception:  # noqa: BLE001 - any failure means "not alive"
        return False


def get_client(force_new: bool = False, retries: int | None = None) -> "Client":
    """Return a healthy ClickHouse client, using the process cache when possible.

    Args:
        force_new: When ``True`` the cache entry is discarded and a brand new
            client is built. Use this after a settings change or a hard error.
        retries: Connection attempts before giving up. Defaults to
            ``DEFAULT_CONNECT_RETRIES``. Interactive callers such as the
            dashboard pass ``1`` so an outage is reported at once instead of
            after the backoff sequence.

    Returns:
        A live ``clickhouse-connect`` client.

    Raises:
        ClickHouseConnectionError: If a live client cannot be established after
            the configured number of retries.
    """
    params = get_connection_params()
    key = params.fingerprint

    with _CACHE_LOCK:
        cached = _CLIENT_CACHE.get(key)
        if cached is not None and not force_new:
            if _is_alive(cached):
                return cached
            # Stale/dead connection - drop it and rebuild below.
            _close_silently(cached)
            _CLIENT_CACHE.pop(key, None)

        client = _connect_with_retry(
            params, retries=retries if retries is not None else DEFAULT_CONNECT_RETRIES
        )
        _CLIENT_CACHE[key] = client
        return client


def _connect_with_retry(
    params: ClickHouseConnectionParams,
    retries: int = DEFAULT_CONNECT_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
) -> "Client":
    """Establish a connection, retrying transient failures with backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _build_client(params)
        except ClickHouseConfigurationError:
            raise  # never retry a configuration problem
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retries or not is_transient_error(exc):
                break
            sleep_for = backoff_base * (2 ** (attempt - 1))
            time.sleep(sleep_for)

    raise ClickHouseConnectionError(
        f"Failed to connect to ClickHouse after {retries} attempt(s): {last_exc}"
    )


def _close_silently(client: "Client") -> None:
    """Close a client, swallowing any error raised during teardown."""
    try:
        client.close()
    except Exception:  # noqa: BLE001 - teardown must never raise
        pass


def health_check() -> dict[str, Any]:
    """Perform a live health check against the configured ClickHouse server.

    Returns:
        A dict describing server reachability and version. Suitable for
        surfacing directly in the Desk UI / dashboard.
    """
    started = time.perf_counter()
    try:
        client = get_client(force_new=True)
        version = client.server_version
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": True,
            "server_version": version,
            "latency_ms": elapsed_ms,
            "message": f"Connected to ClickHouse {version} in {elapsed_ms} ms.",
        }
    except Exception as exc:  # noqa: BLE001 - report, do not raise, for UI use
        return {
            "ok": False,
            "server_version": None,
            "latency_ms": None,
            "message": str(exc),
        }


def invalidate_cache() -> None:
    """Close and forget every cached client in this process.

    Called from the ClickHouse Settings ``on_update`` hook so that credential
    or endpoint changes take effect immediately.
    """
    with _CACHE_LOCK:
        for client in list(_CLIENT_CACHE.values()):
            _close_silently(client)
        _CLIENT_CACHE.clear()
