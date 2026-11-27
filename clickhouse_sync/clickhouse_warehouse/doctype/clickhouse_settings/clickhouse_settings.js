// Copyright (c) 2026, Sharif Basha and contributors
frappe.ui.form.on("ClickHouse Settings", {
	refresh(frm) {
		frm.add_custom_button(__("Test Connection"), () => {
			// The check uses the saved settings, so unsaved edits would not be tested.
			if (frm.is_dirty()) {
				frappe.msgprint(__("Save the settings first, then test the connection."));
				return;
			}
			frm.call({
				doc: frm.doc,
				method: "test_connection",
				freeze: true,
				freeze_message: __("Connecting to ClickHouse…"),
			}).then((r) => {
				const res = r.message || {};
				frappe.msgprint({
					title: res.ok ? __("Connected") : __("Connection failed"),
					message: res.ok
						? __("ClickHouse {0} answered in {1} ms.", [
								frappe.utils.escape_html(String(res.server_version || "")),
								res.latency_ms,
						  ])
						: frappe.utils.escape_html(res.message || ""),
					indicator: res.ok ? "green" : "red",
				});
				// test_connection saves the result in Connection Status.
				frm.reload_doc();
			});
		}).addClass("btn-primary");

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
		}, __("Alerts"));

		frm.add_custom_button(__("Send Test Alert"), () => {
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
		}, __("Alerts"));
	},
});
