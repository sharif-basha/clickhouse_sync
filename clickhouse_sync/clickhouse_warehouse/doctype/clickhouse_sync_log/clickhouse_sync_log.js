// Copyright (c) 2026, Sharif Basha and contributors
// For license information, please see license.txt

frappe.listview_settings["ClickHouse Sync Log"] = {
	add_fields: ["status", "rows_written", "duration"],
	get_indicator(doc) {
		const map = {
			Completed: "green",
			Running: "blue",
			Queued: "orange",
			Cancelled: "gray",
			Failed: "red",
		};
		return [__(doc.status), map[doc.status] || "gray", "status,=," + doc.status];
	},
};

frappe.ui.form.on("ClickHouse Sync Log", {
	refresh(frm) {
		if (frm.doc.status === "Failed" && frm.doc.exception) {
			frm.dashboard.add_comment(__("This sync failed. See the stack trace below."), "red", true);
		}
		if (frm.doc.configuration) {
			frm.add_custom_button(__("Open Configuration"), () => {
				frappe.set_route("Form", "ClickHouse Sync Configuration", frm.doc.configuration);
			});
		}
	},
});
