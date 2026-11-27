// Copyright (c) 2026, Sharif Basha and contributors
frappe.query_reports["Slowest Syncs"] = {
	filters: [
		{ fieldname: "limit", label: __("Show Top N"), fieldtype: "Int", default: 50 },
	],
};
