# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
"""Documentation site configuration for clickhouse_sync."""

source_link = "https://github.com/ryss/clickhouse_sync"
docs_base_url = "https://ryss.example.org/clickhouse_sync"
headline = "Sync Frappe data into ClickHouse"
sub_heading = "Automatic, incremental, high-throughput replication for reporting"


def get_context(context):  # noqa: ANN001, ANN201
    """Populate the docs context (title/brand)."""
    context.brand_html = "ClickHouse Sync"
