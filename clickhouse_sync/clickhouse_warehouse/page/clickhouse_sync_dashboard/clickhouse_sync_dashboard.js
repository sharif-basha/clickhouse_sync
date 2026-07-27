// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
// For license information, please see license.txt

/**
 * ClickHouse Sync Dashboard - an analytical Desk page.
 *
 * Renders headline metric cards, four charts (rows/day, status breakdown,
 * rows per DocType, and a warehouse footprint table) and a recent-runs table.
 * All data comes from the single whitelisted endpoint
 * `clickhouse_sync.api.api.analytics`, which combines sync-log performance
 * metrics with live ClickHouse table sizes. Charts use frappe-charts
 * (`frappe.Chart`), which ships with Desk - no external libraries.
 */

frappe.pages["clickhouse-sync-dashboard"].on_page_load = function (wrapper) {
	const page = frappe.ui.make_app_page({
		parent: wrapper,
		title: __("ClickHouse Sync Dashboard"),
		single_column: true,
	});

	const dashboard = new ClickHouseSyncDashboard(page);
	dashboard.refresh();

	page.set_primary_action(__("Refresh"), () => dashboard.refresh(), "refresh");
	page.add_menu_item(__("Open Configurations"), () =>
		frappe.set_route("List", "ClickHouse Sync Configuration")
	);
	page.add_menu_item(__("Open Logs"), () =>
		frappe.set_route("List", "ClickHouse Sync Log")
	);
};

class ClickHouseSyncDashboard {
	constructor(page) {
		this.page = page;
		this.body = $('<div class="chsync-dashboard">').appendTo(page.main);
		this._build_skeleton();
	}

	_build_skeleton() {
		this.body.html(`
			<div class="chsync-health"></div>
			<div class="chsync-toolbar">
				<div class="chsync-range btn-group btn-group-sm" role="group">
					<button class="btn btn-default" data-days="7">${__("7 days")}</button>
					<button class="btn btn-default active" data-days="30">${__("30 days")}</button>
					<button class="btn btn-default" data-days="90">${__("90 days")}</button>
				</div>
			</div>
			<div class="chsync-cards row"></div>
			<div class="row">
				<div class="col-md-8">
					<div class="chsync-panel">
						<h6 class="chsync-panel-title">${__("Rows Synced Per Day")}</h6>
						<div class="chsync-panel-hint text-muted small">${__("How much data moved to ClickHouse each day.")}</div>
						<div class="chsync-rows-per-day"></div>
					</div>
				</div>
				<div class="col-md-4">
					<div class="chsync-panel">
						<h6 class="chsync-panel-title">${__("Sync Status")}</h6>
						<div class="chsync-panel-hint text-muted small">${__("Share of runs that succeeded vs failed.")}</div>
						<div class="chsync-status"></div>
					</div>
				</div>
			</div>
			<div class="row">
				<div class="col-md-6">
					<div class="chsync-panel">
						<h6 class="chsync-panel-title">${__("Rows by DocType")}</h6>
						<div class="chsync-panel-hint text-muted small">${__("Which sources contribute the most rows.")}</div>
						<div class="chsync-rows-per-doctype"></div>
					</div>
				</div>
				<div class="col-md-6">
					<div class="chsync-panel">
						<h6 class="chsync-panel-title">${__("ClickHouse Tables")}</h6>
						<div class="chsync-panel-hint text-muted small">${__("Live row counts and sizes in the warehouse.")}</div>
						<div class="chsync-tables"></div>
					</div>
				</div>
			</div>
			<div class="row">
				<div class="col-md-12">
					<div class="chsync-panel">
						<h6 class="chsync-panel-title">${__("Recent Runs")}</h6>
						<div class="chsync-recent"></div>
					</div>
				</div>
			</div>
		`);
		this.days = 30;
		this.body.find(".chsync-range button").on("click", (e) => {
			const $b = $(e.currentTarget);
			this.body.find(".chsync-range button").removeClass("active");
			$b.addClass("active");
			this.days = parseInt($b.data("days"), 10);
			this.refresh();
		});
	}

	_render_health(h) {
		// One-glance health verdict from success rate and failures today.
		let level, label, color;
		if (h.failed_today > 0 || h.success_rate < 90) {
			level = "bad"; color = "#B23A48";
			label = __("Attention needed — {0} failed today, {1}% success rate", [h.failed_today, h.success_rate]);
		} else if (h.success_rate < 99) {
			level = "warn"; color = "#B8860B";
			label = __("Mostly healthy — {0}% success rate over the period", [h.success_rate]);
		} else {
			level = "good"; color = "#2D6A4F";
			label = __("Healthy — {0}% of runs succeeded", [h.success_rate]);
		}
		const icon = { good: "✅", warn: "⚠️", bad: "❌" }[level];
		this.body.find(".chsync-health").html(
			`<div class="chsync-health-banner" style="border-left:5px solid ${color};">
				<span class="chsync-health-icon">${icon}</span>
				<span class="chsync-health-text">${label}</span>
			</div>`
		);
	}

	refresh() {
		this.page.set_indicator(__("Loading..."), "orange");
		frappe
			.call({ method: "clickhouse_sync.api.api.analytics", args: { days: this.days || 30 } })
			.then((r) => {
				const data = r.message;
				if (!data) {
					this.page.set_indicator(__("No data"), "red");
					return;
				}
				this._render_health(data.headline);
				this._render_cards(data.headline);
				this._render_rows_per_day(data.rows_per_day);
				this._render_status(data.status_breakdown);
				this._render_rows_per_doctype(data.rows_per_doctype);
				this._render_tables(data.clickhouse_tables);
				this._render_recent(data.recent_logs);
				this.page.set_indicator(
					__("Updated {0}", [frappe.datetime.now_time()]),
					"green"
				);
			})
			.catch(() => this.page.set_indicator(__("Error loading data"), "red"));
	}

	_card(label, value, color) {
		return `
			<div class="col-sm-4 col-md-2">
				<div class="chsync-card" style="border-top: 3px solid ${color};">
					<div class="chsync-card-value">${value}</div>
					<div class="chsync-card-label">${label}</div>
				</div>
			</div>`;
	}

	_render_cards(h) {
		const fmt = (n) => frappe.format(n, { fieldtype: "Int" });
		const largest = h.largest_table
			? `${h.largest_table.table} (${fmt(h.largest_table.rows)})`
			: "—";
		this.body.find(".chsync-cards").html(
			this._card(__("Rows Total"), fmt(h.rows_total), "#1B4332") +
				this._card(__("Rows Today"), fmt(h.rows_today), "#40916C") +
				this._card(__("Success Rate"), `${h.success_rate}%`, "#2D6A4F") +
				this._card(__("Failed Today"), fmt(h.failed_today), "#B23A48") +
				this._card(__("Avg Duration"), `${h.average_duration}s`, "#B8860B") +
				this._card(__("Total Runs"), fmt(h.total_runs), "#52796F")
		);
		if (h.largest_table) {
			this.body
				.find(".chsync-cards")
				.append(
					`<div class="col-12"><div class="text-muted small mt-1">${__(
						"Largest table"
					)}: ${largest}</div></div>`
				);
		}
	}

	_render_rows_per_day(series) {
		const el = this.body.find(".chsync-rows-per-day").get(0);
		if (!series.labels.length) {
			this.body.find(".chsync-rows-per-day").html(this._empty());
			return;
		}
		new frappe.Chart(el, {
			type: "bar",
			height: 260,
			colors: ["#1B4332"],
			data: {
				labels: series.labels,
				datasets: [{ name: __("Rows"), values: series.values }],
			},
			axisOptions: { xIsSeries: true },
		});
	}

	_render_status(series) {
		const el = this.body.find(".chsync-status").get(0);
		if (!series.labels.length) {
			this.body.find(".chsync-status").html(this._empty());
			return;
		}
		const colorFor = {
			Completed: "#2D6A4F",
			Failed: "#B23A48",
			Cancelled: "#6C757D",
			Running: "#0077B6",
			Queued: "#B8860B",
		};
		new frappe.Chart(el, {
			type: "donut",
			height: 260,
			colors: series.labels.map((l) => colorFor[l] || "#52796F"),
			data: {
				labels: series.labels,
				datasets: [{ values: series.values }],
			},
		});
	}

	_render_rows_per_doctype(series) {
		const el = this.body.find(".chsync-rows-per-doctype").get(0);
		if (!series.labels.length) {
			this.body.find(".chsync-rows-per-doctype").html(this._empty());
			return;
		}
		new frappe.Chart(el, {
			type: "bar",
			height: 260,
			colors: ["#40916C"],
			data: {
				labels: series.labels,
				datasets: [{ name: __("Rows"), values: series.values }],
			},
		});
	}

	_render_tables(tables) {
		if (!tables || !tables.length) {
			this.body
				.find(".chsync-tables")
				.html(
					`<div class="text-muted small p-3">${__(
						"No table data (ClickHouse unreachable or nothing synced yet)."
					)}</div>`
				);
			return;
		}
		let rows = tables
			.slice(0, 10)
			.map(
				(t) =>
					`<tr><td>${frappe.utils.escape_html(t.table)}</td>
					 <td class="text-right">${frappe.format(t.rows, { fieldtype: "Int" })}</td>
					 <td class="text-right">${frappe.utils.escape_html(t.size)}</td></tr>`
			)
			.join("");
		this.body.find(".chsync-tables").html(`
			<table class="table table-sm">
				<thead><tr><th>${__("Table")}</th><th class="text-right">${__(
			"Rows"
		)}</th><th class="text-right">${__("Size")}</th></tr></thead>
				<tbody>${rows}</tbody>
			</table>`);
	}

	_render_recent(logs) {
		if (!logs || !logs.length) {
			this.body.find(".chsync-recent").html(this._empty());
			return;
		}
		const indicator = {
			Completed: "green",
			Failed: "red",
			Cancelled: "gray",
			Running: "blue",
			Queued: "orange",
		};
		let rows = logs
			.map((l) => {
				const dot = indicator[l.status] || "gray";
				return `<tr>
					<td><a href="/app/clickhouse-sync-log/${encodeURIComponent(
						l.name
					)}">${l.name}</a></td>
					<td>${frappe.utils.escape_html(l.configuration || "")}</td>
					<td><span class="indicator ${dot}">${l.status}</span></td>
					<td class="text-right">${frappe.format(l.rows_written, { fieldtype: "Int" })}</td>
					<td class="text-right">${l.duration || 0}s</td>
					<td>${frappe.datetime.comment_when(l.creation)}</td>
				</tr>`;
			})
			.join("");
		this.body.find(".chsync-recent").html(`
			<table class="table table-sm">
				<thead><tr>
					<th>${__("Log")}</th><th>${__("Configuration")}</th><th>${__("Status")}</th>
					<th class="text-right">${__("Rows")}</th><th class="text-right">${__(
			"Duration"
		)}</th><th>${__("When")}</th>
				</tr></thead>
				<tbody>${rows}</tbody>
			</table>`);
	}

	_empty() {
		return `<div class="text-muted small p-3">${__("No data yet.")}</div>`;
	}
}
