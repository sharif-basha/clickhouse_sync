"""ClickHouse integration engine for the clickhouse_sync Frappe app.

This package contains the low-level building blocks used by the sync
service layer:

* :mod:`clickhouse_sync.clickhouse.exceptions` - typed error hierarchy.
* :mod:`clickhouse_sync.clickhouse.connection` - cached, health-checked
  connection pool built on top of ``clickhouse-connect``.
* :mod:`clickhouse_sync.clickhouse.type_mapping` - Frappe -> ClickHouse
  field type mapping.
* :mod:`clickhouse_sync.clickhouse.schema` - DDL generation and schema
  reconciliation.
* :mod:`clickhouse_sync.clickhouse.sync_engine` - the service layer that
  orchestrates full / incremental / delete synchronisation.
"""

from __future__ import annotations

__all__ = [
    "exceptions",
    "connection",
    "type_mapping",
    "schema",
    "sync_engine",
]
