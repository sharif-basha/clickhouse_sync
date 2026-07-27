// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
frappe.ui.form.on("ClickHouse Settings", {
	refresh(frm) {
		frm.add_custom_button(__("Send Test Digest"), () => {
			frm.call({
				method: "clickhouse_sync.clickhouse.alerts.send_test_digest",
				freeze: true, freeze_message: __("Sending digest…"),
			}).then((r) => {
				const res = r.message || {};
				frappe.msgprint({
					title: res.ok ? __("Sent") : __("Not sent"),
					message: frappe.utils.escape_html(res.message || ""),
					indicator: res.ok ? "green" : "orange",
				});
			});
		}, __("Telegram"));

		frm.add_custom_button(__("Send Test Telegram Alert"), () => {
			frm.call({
				method: "clickhouse_sync.clickhouse.alerts.send_test_alert",
				freeze: true, freeze_message: __("Sending test alert…"),
			}).then((r) => {
				const res = r.message || {};
				frappe.msgprint({
					title: res.ok ? __("Sent") : __("Not sent"),
					message: frappe.utils.escape_html(res.message || ""),
					indicator: res.ok ? "green" : "orange",
				});
			});
		}, __("Telegram"));
	},
});
