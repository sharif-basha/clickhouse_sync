// Copyright (c) 2026, Sharif Basha and contributors
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
