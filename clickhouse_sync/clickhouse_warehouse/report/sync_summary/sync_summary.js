// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
frappe.query_reports["Sync Summary"] = {
	filters: [
		{ fieldname: "from_date", label: __("From Date"), fieldtype: "Datetime" },
		{ fieldname: "to_date", label: __("To Date"), fieldtype: "Datetime" },
	],
};
