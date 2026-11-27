# Copyright (c) 2026, Sharif Basha and contributors
"""Documentation site configuration for clickhouse_sync."""

source_link = "https://github.com/sharif-basha/clickhouse_sync"
docs_base_url = "https://github.com/sharif-basha/clickhouse_sync#readme"
headline = "Sync Frappe data into ClickHouse"
sub_heading = "Automatic, incremental, high-throughput replication for reporting"


def get_context(context):  # noqa: ANN001, ANN201
    """Populate the docs context (title/brand)."""
    context.brand_html = "ClickHouse Sync"
