# Copyright (c) 2026, Sharif Basha and contributors
# For license information, please see license.txt
"""Form dashboard for ``ClickHouse Sync Configuration``.

Surfaces linked ClickHouse Sync Log records directly on the configuration
form so operators can jump straight to the run history.
"""

from __future__ import annotations

from frappe import _


def get_data() -> dict:
    """Return the dashboard link configuration."""
    return {
        "fieldname": "configuration",
        "non_standard_fieldnames": {},
        "transactions": [
            {
                "label": _("Runs"),
                "items": ["ClickHouse Sync Log"],
            }
        ],
    }
