// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
frappe.query_reports["Slowest Syncs"] = {
	filters: [
		{ fieldname: "limit", label: __("Show Top N"), fieldtype: "Int", default: 50 },
	],
};
