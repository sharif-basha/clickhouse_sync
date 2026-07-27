// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
// For license information, please see license.txt

/**
 * Global helpers for the ClickHouse Sync app, available across the Desk.
 * Namespaced under `frappe.provide("clickhouse_sync")`.
 */
frappe.provide("clickhouse_sync");

clickhouse_sync = {
	/**
	 * Call a whitelisted API method in the clickhouse_sync.api.api module.
	 * @param {string} method - Bare method name (e.g. "sync_all").
	 * @param {Object} args - Keyword arguments for the method.
	 * @returns {Promise<Object>} Resolves with the server message payload.
	 */
	api(method, args = {}) {
		return frappe
			.call({ method: `clickhouse_sync.api.api.${method}`, args })
			.then((r) => r.message);
	},

	/** Trigger a sync of every enabled configuration. */
	sync_all() {
		return clickhouse_sync.api("sync_all", { trigger: "Manual" }).then((res) => {
			frappe.show_alert(
				{
					message: __("Queued {0} sync job(s).", [res.count]),
					indicator: "green",
				},
				5
			);
			return res;
		});
	},

	/** Run a live connection test and toast the result. */
	test_connection() {
		return clickhouse_sync.api("test_connection").then((res) => {
			frappe.show_alert(
				{
					message: res.ok
						? __("ClickHouse {0} reachable ({1} ms).", [res.server_version, res.latency_ms])
						: __("ClickHouse unreachable: {0}", [res.message]),
					indicator: res.ok ? "green" : "red",
				},
				7
			);
			return res;
		});
	},
};
