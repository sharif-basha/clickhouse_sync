// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
frappe.query_reports["Failed Syncs"] = {
	filters: [
		{
			fieldname: "configuration",
			label: __("Configuration"),
			fieldtype: "Link",
			options: "ClickHouse Sync Configuration",
		},
	],
};
