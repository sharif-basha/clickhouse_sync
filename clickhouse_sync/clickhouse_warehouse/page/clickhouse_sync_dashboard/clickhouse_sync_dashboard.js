// Copyright (c) 2026, Sharif Basha and contributors
// For license information, please see license.txt

/**
 * ClickHouse Sync Dashboard - monitoring and control console.
 *
 * Sections (top to bottom):
 *   1. Health banner and ClickHouse connection status
 *   2. Overall statistics (record counts, pending, failures, sync %)
 *   3. Live synchronization monitor (one card per queued/running sync)
 *   4. DocType status table with per-DocType controls
 *   5. Error monitoring (recent failures, categories, failed DocTypes)
 *   6. Analytics (throughput, run outcomes, warehouse tables, recent runs)
 *
 * Every value comes from `clickhouse_sync.api.dashboard`. Live progress is
 * written by the sync worker itself (Redis), counts come from MariaDB and
 * ClickHouse, history from ClickHouse Sync Log. Nothing is estimated here:
 * unknown values are shown as a dash with the reason in a tooltip.
 *
 * Refresh: the live monitor polls every 3 s while a sync is queued or
 * running; the full overview refreshes every 30 s. Polling pauses while the
 * page or browser tab is hidden.
 */

const CHSYNC_API = "clickhouse_sync.api.dashboard";
const CHSYNC_LIVE_POLL_MS = 3000;
const CHSYNC_FULL_POLL_MS = 30000;

const METRIC_HELP = {
	"Records to process": "Source records in this run's window, counted when the run started.",
	Processed: "Records read from the source so far.",
	Written: "Records written to ClickHouse so far.",
	"Inserted (new)": "Created after the previous sync.",
	Updated: "Created earlier, modified after the previous sync.",
	"Re-checked": "Unchanged records re-read because each run starts 5 minutes before the last position. Harmless: ClickHouse keeps one version.",
	Skipped: "Records incremental sync cannot reach (empty or future 'modified' date).",
	Failed: "Records in batches ClickHouse rejected in this run.",
	Speed: "Records read per second since the run started.",
	Remaining: "Estimate from the current speed; shown after 2 batches.",
};

// Styles ship with the page script so they load whenever the page does,
// independent of asset builds and browser caching of the app stylesheet.
const CHSYNC_CSS = "/* ====================================================================== */\n/* Monitoring dashboard (v1.8)                                             */\n/* Colours: Frappe theme variables for surfaces and text; data colours     */\n/* follow a validated palette (series blue, status good/warning/critical). */\n/* ====================================================================== */\n.chsync-dash {\n\t--chsync-surface: var(--card-bg, #fff);\n\t--chsync-subtle: var(--subtle-fg, #f4f5f6);\n\t--chsync-border: var(--border-color, #e2e6e9);\n\t--chsync-text: var(--text-color, #1f272e);\n\t--chsync-muted: var(--text-muted, #6b7280);\n\t--chsync-series: #2a78d6;\n\t--chsync-good: #0ca30c;\n\t--chsync-good-text: #006300;\n\t--chsync-warning: #fab219;\n\t--chsync-warning-text: #8a5a00;\n\t--chsync-critical: #d03b3b;\n\t--chsync-critical-text: #b42323;\n\t--chsync-info-text: #1c5cab;\n\t--chsync-track: var(--gray-200, #e9ecef);\n\tcolor: var(--chsync-text);\n\tpadding-bottom: 32px;\n\ttransition: opacity 0.2s;\n}\n[data-theme=\"dark\"] .chsync-dash {\n\t--chsync-series: #3987e5;\n\t--chsync-good-text: #3fcf3f;\n\t--chsync-warning-text: #fab219;\n\t--chsync-critical-text: #f07a7a;\n\t--chsync-info-text: #86b6ef;\n\t--chsync-track: var(--gray-700, #383835);\n}\n.chsync-dash.chsync-refreshing [data-region=\"kpis\"],\n.chsync-dash.chsync-refreshing [data-region=\"doctypes\"] { opacity: 0.75; }\n\n.chsync-dash .chsync-section { margin-bottom: 24px; }\n.chsync-dash .chsync-section-head {\n\tdisplay: flex; align-items: center; justify-content: space-between;\n\tgap: 12px; margin-bottom: 10px; flex-wrap: wrap;\n}\n.chsync-dash .chsync-section-head h4 { margin: 0; font-size: 15px; font-weight: 600; color: var(--chsync-text); }\n.chsync-dash .chsync-mt-sm { margin-top: 10px; }\n.chsync-dash .chsync-table th, .chsync-dash .chsync-table td,\n.chsync-dash .chsync-metrics, .chsync-dash .chsync-card { text-align: left; }\n.chsync-dash .chsync-section-tools { display: flex; gap: 8px; align-items: center; }\n.chsync-dash .chsync-search { width: 200px; }\n.chsync-dash .chsync-card {\n\tbackground: var(--chsync-surface);\n\tborder: 1px solid var(--chsync-border);\n\tborder-radius: 10px;\n\tpadding: 14px 16px;\n}\n.chsync-dash .chsync-card-title {\n\tfont-size: 12px; font-weight: 600; color: var(--chsync-muted);\n\ttext-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 8px;\n}\n.chsync-dash .chsync-mt { margin-top: 16px; }\n.chsync-dash .chsync-empty { padding: 18px 4px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }\n.chsync-dash .chsync-grid-2-1 { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; align-items: start; }\n.chsync-dash .chsync-grid-1-1 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }\n@media (max-width: 991px) {\n\t.chsync-dash .chsync-grid-2-1, .chsync-dash .chsync-grid-1-1 { grid-template-columns: 1fr; }\n}\n\n/* Top bar */\n.chsync-dash .chsync-topbar {\n\tdisplay: flex; justify-content: space-between; align-items: center;\n\tgap: 12px; flex-wrap: wrap; margin: 4px 0 16px;\n}\n.chsync-dash .chsync-health { display: flex; align-items: center; gap: 10px; }\n.chsync-dash .chsync-health-text { font-weight: 500; }\n.chsync-dash .chsync-topbar-right { display: flex; align-items: center; gap: 12px; font-size: 12px; }\n\n/* Pills: colour + icon + word, never colour alone */\n.chsync-dash .chsync-pill {\n\tdisplay: inline-flex; align-items: center; gap: 4px;\n\tpadding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 500;\n\tborder: 1px solid currentColor; white-space: nowrap; line-height: 18px;\n}\n.chsync-dash .chsync-pill-lg { font-size: 13px; padding: 4px 10px; }\n.chsync-dash .chsync-good { color: var(--chsync-good-text); }\n.chsync-dash .chsync-warning { color: var(--chsync-warning-text); }\n.chsync-dash .chsync-critical { color: var(--chsync-critical-text); }\n.chsync-dash .chsync-info { color: var(--chsync-info-text); }\n.chsync-dash .chsync-neutral { color: var(--chsync-muted); }\n.chsync-dash .chsync-clickable { cursor: pointer; }\n.chsync-dash .chsync-icon { width: 12px; height: 12px; flex: none; }\n.chsync-dash .critical-text { color: var(--chsync-critical-text); }\n.chsync-dash .warning-text { color: var(--chsync-warning-text); }\n.chsync-dash .chsync-tag {\n\tfont-size: 11px; padding: 1px 6px; border-radius: 4px;\n\tbackground: var(--chsync-subtle); color: var(--chsync-text); white-space: nowrap;\n}\n\n/* KPI tiles */\n.chsync-dash .chsync-kpis {\n\tdisplay: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px;\n}\n.chsync-dash .chsync-kpi {\n\tbackground: var(--chsync-surface); border: 1px solid var(--chsync-border);\n\tborder-radius: 10px; padding: 12px 14px; cursor: default;\n}\n.chsync-dash .chsync-kpi-label { font-size: 12px; color: var(--chsync-muted); }\n.chsync-dash .chsync-kpi-value { font-size: 24px; font-weight: 600; margin: 4px 0 2px; line-height: 1.2; }\n.chsync-dash .chsync-kpi-value-sm { font-size: 18px; }\n.chsync-dash .chsync-kpi-sub { font-size: 11.5px; color: var(--chsync-muted); }\n.chsync-dash .chsync-kpi-note { font-size: 11.5px; margin-top: 8px; }\n\n/* Meters and progress */\n.chsync-dash .chsync-meter {\n\tdisplay: block; height: 4px; border-radius: 2px; background: var(--chsync-track);\n\toverflow: hidden; margin: 6px 0 4px;\n}\n.chsync-dash .chsync-meter > span { display: block; height: 100%; background: var(--chsync-series); border-radius: 2px; }\n.chsync-dash .chsync-meter-inline { display: inline-block; width: 48px; margin: 0 0 0 8px; vertical-align: middle; }\n.chsync-dash .chsync-progress {\n\tflex: 1; height: 8px; border-radius: 4px; background: var(--chsync-track); overflow: hidden;\n}\n.chsync-dash .chsync-progress > span {\n\tdisplay: block; height: 100%; background: var(--chsync-series); border-radius: 4px;\n\ttransition: width 0.6s ease;\n}\n.chsync-dash .chsync-progress-unknown > span {\n\twidth: 30%; animation: chsync-slide 1.4s ease-in-out infinite;\n}\n@keyframes chsync-slide { 0% { margin-left: -30%; } 100% { margin-left: 100%; } }\n@media (prefers-reduced-motion: reduce) {\n\t.chsync-dash .chsync-progress-unknown > span { animation: none; width: 100%; opacity: 0.35; }\n}\n\n/* Live monitor */\n.chsync-dash .chsync-live-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 12px; align-items: start; }\n.chsync-dash .chsync-live-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }\n.chsync-dash .chsync-live-title { font-size: 15px; font-weight: 600; }\n.chsync-dash .chsync-live-actions { display: flex; gap: 8px; align-items: center; }\n.chsync-dash .chsync-live-progress { display: flex; align-items: center; gap: 10px; margin: 12px 0; }\n.chsync-dash .chsync-live-pct { font-weight: 600; min-width: 48px; text-align: right; font-variant-numeric: tabular-nums; }\n.chsync-dash .chsync-live-foot { margin-top: 10px; display: grid; gap: 4px; }\n.chsync-dash .chsync-metrics {\n\tdisplay: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px 16px; margin: 0;\n}\n.chsync-dash .chsync-metrics-wide { grid-template-columns: repeat(3, minmax(0, 1fr)); margin-bottom: 16px; }\n.chsync-dash .chsync-metrics dt { font-size: 11.5px; font-weight: 400; color: var(--chsync-muted); }\n.chsync-dash .chsync-metrics dd { margin: 0; font-weight: 600; font-variant-numeric: tabular-nums; }\n@media (max-width: 600px) {\n\t.chsync-dash .chsync-live-grid { grid-template-columns: 1fr; }\n\t.chsync-dash .chsync-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n}\n\n/* Tables */\n.chsync-dash .chsync-table-wrap { overflow-x: auto; padding: 6px 8px; }\n.chsync-dash .chsync-table { margin: 0; font-size: 13px; }\n.chsync-dash .chsync-table th {\n\tfont-size: 11.5px; font-weight: 600; color: var(--chsync-muted);\n\tborder-top: none; white-space: nowrap;\n}\n.chsync-dash .chsync-table td { vertical-align: middle; border-color: var(--chsync-border); }\n.chsync-dash .chsync-table th, .chsync-dash .chsync-table td { background: transparent; color: inherit; }\n.chsync-dash .chsync-table th.num, .chsync-dash .chsync-table td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }\n.chsync-dash .chsync-sortable { cursor: pointer; user-select: none; }\n.chsync-dash .chsync-sortable:hover { color: var(--chsync-text); }\n.chsync-dash .chsync-table td.chsync-actions { white-space: nowrap; text-align: right; }\n.chsync-dash .chsync-disabled td { opacity: 0.55; }\n.chsync-dash .chsync-nowrap { white-space: nowrap; }\n.chsync-dash .chsync-msg { max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n\n/* Error summary */\n.chsync-dash .chsync-mini-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 16px; }\n.chsync-dash .chsync-mini-value { font-size: 22px; font-weight: 600; }\n.chsync-dash .chsync-mini-label { font-size: 12px; color: var(--chsync-muted); }\n.chsync-dash .chsync-list { padding-left: 18px; margin: 0 0 8px; }\n.chsync-dash .chsync-hbar { display: grid; grid-template-columns: 110px 1fr 36px; gap: 8px; align-items: center; font-size: 12.5px; margin-bottom: 6px; }\n.chsync-dash .chsync-hbar-track { height: 8px; }\n.chsync-dash .chsync-hbar-track > span { display: block; height: 100%; min-width: 2px; background: var(--chsync-series); border-radius: 0 4px 4px 0; }\n.chsync-dash .chsync-hbar-value { text-align: right; font-variant-numeric: tabular-nums; }\n\n/* Analytics */\n.chsync-dash .chsync-window-stats { display: flex; gap: 24px; flex-wrap: wrap; margin-bottom: 12px; font-size: 13px; }\n\n/* Error dialog */\n.chsync-error-detail .chsync-error-message {\n\tpadding: 10px 12px; border-radius: 6px; background: var(--chsync-subtle);\n\tmargin-bottom: 16px; white-space: pre-wrap; word-break: break-word;\n}\n.chsync-error-detail .chsync-trace {\n\tmax-height: 360px; overflow: auto; font-size: 12px; padding: 10px 12px;\n\tborder-radius: 6px; background: var(--chsync-subtle); color: var(--chsync-text);\n}\n";

function chsync_inject_styles() {
	if (document.getElementById("chsync-dash-style")) return;
	const style = document.createElement("style");
	style.id = "chsync-dash-style";
	style.textContent = CHSYNC_CSS;
	document.head.appendChild(style);
}

frappe.pages["clickhouse-sync-dashboard"].on_page_load = function (wrapper) {
	chsync_inject_styles();
	const page = frappe.ui.make_app_page({
		parent: wrapper,
		title: __("ClickHouse Sync Dashboard"),
		single_column: true,
	});
	wrapper.chsync_dashboard = new ClickHouseSyncDashboard(page, wrapper);
};

frappe.pages["clickhouse-sync-dashboard"].on_page_show = function (wrapper) {
	if (wrapper.chsync_dashboard) wrapper.chsync_dashboard.on_show();
};

class ClickHouseSyncDashboard {
	constructor(page, wrapper) {
		this.page = page;
		this.wrapper = wrapper;
		this.days = 7;
		this.data = null;
		this.sort = { key: "configuration", dir: 1 };
		this.filter = "";
		this.charts = {};
		this.loading = false;

		this.root = document.createElement("div");
		this.root.className = "chsync-dash";
		page.main.get(0).appendChild(this.root);

		this._setup_actions();
		this._build_skeleton();
		this._bind_events();
		this.refresh();
		this._start_timers();

		if (frappe.realtime && frappe.realtime.on) {
			// Progress events reach only the user who started a sync; use them
			// as a hint to poll sooner. Polling remains the source of truth.
			frappe.realtime.on("clickhouse_sync_progress", () => this._poll_live_soon());
		}
	}

	// ------------------------------------------------------------------ //
	// Page actions
	// ------------------------------------------------------------------ //
	_setup_actions() {
		const p = this.page;
		p.set_primary_action(__("Incremental Sync (All)"), () =>
			this._confirm_start_all("Incremental")
		, "refresh");
		p.add_inner_button(__("Full Sync (All)"), () => this._confirm_start_all("Full"), __("Start"));
		p.add_inner_button(__("Retry Failed"), () => this._retry_failed(), __("Start"));
		p.add_inner_button(__("Stop All"), () => this._stop_all());
		p.set_secondary_action(__("Refresh"), () => this.refresh(), "refresh");

		p.add_menu_item(__("Recount Records"), () => this._recount());
		p.add_menu_item(__("Sync Configurations"), () =>
			frappe.set_route("List", "ClickHouse Sync Configuration")
		);
		p.add_menu_item(__("Sync Logs"), () => frappe.set_route("List", "ClickHouse Sync Log"));
		p.add_menu_item(__("Failed Syncs Report"), () =>
			frappe.set_route("query-report", "Failed Syncs")
		);
		p.add_menu_item(__("ClickHouse Settings"), () =>
			frappe.set_route("Form", "ClickHouse Settings")
		);
	}

	// ------------------------------------------------------------------ //
	// Skeleton
	// ------------------------------------------------------------------ //
	_build_skeleton() {
		this.root.innerHTML = `
			<div class="chsync-topbar">
				<div class="chsync-health" data-region="health"></div>
				<div class="chsync-topbar-right">
					<span class="chsync-conn" data-region="conn"></span>
					<span class="chsync-updated text-muted" data-region="updated"></span>
				</div>
			</div>

			<section class="chsync-section">
				<div class="chsync-kpis" data-region="kpis"></div>
				<div class="chsync-kpi-note text-muted" data-region="kpi-note"></div>
			</section>

			<section class="chsync-section">
				<div class="chsync-section-head">
					<h4>${__("Live Synchronization")}</h4>
					<span class="chsync-live-count text-muted" data-region="live-count"></span>
				</div>
				<div data-region="live"></div>
			</section>

			<section class="chsync-section">
				<div class="chsync-section-head">
					<h4>${__("DocTypes")}</h4>
					<div class="chsync-section-tools">
						<input type="search" class="form-control input-xs chsync-search"
							placeholder="${__("Filter DocTypes")}" aria-label="${__("Filter DocTypes")}">
						<button class="btn btn-default btn-xs" data-action="recount-all">${__("Recount")}</button>
					</div>
				</div>
				<div class="chsync-card chsync-table-wrap" data-region="doctypes"></div>
			</section>

			<section class="chsync-section">
				<div class="chsync-section-head">
					<h4>${__("Errors")}</h4>
					<div class="chsync-range btn-group btn-group-sm" role="group" aria-label="${__("Time window")}">
						<button class="btn btn-default active" data-days="7">${__("7 days")}</button>
						<button class="btn btn-default" data-days="30">${__("30 days")}</button>
						<button class="btn btn-default" data-days="90">${__("90 days")}</button>
					</div>
				</div>
				<div class="chsync-grid-2-1">
					<div class="chsync-card chsync-table-wrap" data-region="errors"></div>
					<div class="chsync-card" data-region="error-summary"></div>
				</div>
			</section>

			<section class="chsync-section">
				<div class="chsync-section-head">
					<h4>${__("Analytics")}</h4>
					<span class="text-muted small" data-region="window-label"></span>
				</div>
				<div class="chsync-window-stats" data-region="window-stats"></div>
				<div class="chsync-grid-1-1">
					<div class="chsync-card">
						<div class="chsync-card-title">${__("Records written per day")}</div>
						<div data-region="chart-rows"></div>
					</div>
					<div class="chsync-card">
						<div class="chsync-card-title">${__("Runs per day by outcome")}</div>
						<div data-region="chart-runs"></div>
					</div>
				</div>
				<div class="chsync-grid-1-1">
					<div class="chsync-card chsync-table-wrap">
						<div class="chsync-card-title">${__("Warehouse tables")}</div>
						<div data-region="tables"></div>
					</div>
					<div class="chsync-card chsync-table-wrap">
						<div class="chsync-card-title">${__("Recent runs")}</div>
						<div data-region="recent"></div>
					</div>
				</div>
			</section>
		`;
	}

	_region(name) {
		return this.root.querySelector(`[data-region="${name}"]`);
	}

	_bind_events() {
		this.root.addEventListener("click", (e) => {
			const range = e.target.closest(".chsync-range button");
			if (range) {
				this.root.querySelectorAll(".chsync-range button").forEach((b) => b.classList.remove("active"));
				range.classList.add("active");
				this.days = parseInt(range.dataset.days, 10);
				this.refresh();
				return;
			}
			const th = e.target.closest("th[data-sort]");
			if (th) {
				const key = th.dataset.sort;
				this.sort = { key, dir: this.sort.key === key ? -this.sort.dir : 1 };
				this._render_doctypes();
				return;
			}
			const btn = e.target.closest("[data-action]");
			if (!btn) return;
			const cfg = btn.dataset.config;
			switch (btn.dataset.action) {
				case "sync":
					this._start(cfg, btn.dataset.mode);
					break;
				case "stop":
					this._stop(cfg);
					break;
				case "logs":
					frappe.set_route("List", "ClickHouse Sync Log", { configuration: cfg });
					break;
				case "pause":
					this._set_paused(cfg, btn.dataset.paused === "1");
					break;
				case "error":
					this._show_error(btn.dataset.log);
					break;
				case "recount":
					this._recount(cfg);
					break;
				case "recount-all":
					this._recount();
					break;
				case "retry-failed":
					this._retry_failed();
					break;
			}
		});
		this.root.querySelector(".chsync-search").addEventListener("input", (e) => {
			this.filter = (e.target.value || "").toLowerCase();
			this._render_doctypes();
		});
	}

	// ------------------------------------------------------------------ //
	// Data loading
	// ------------------------------------------------------------------ //
	_visible() {
		return document.visibilityState === "visible" && this.wrapper.offsetParent !== null;
	}

	_start_timers() {
		clearInterval(this._full_timer);
		clearInterval(this._live_timer);
		this._full_timer = setInterval(() => {
			if (this._visible()) this.refresh(true);
		}, CHSYNC_FULL_POLL_MS);
		this._live_timer = setInterval(() => {
			if (this._visible() && this._has_live()) this._poll_live();
		}, CHSYNC_LIVE_POLL_MS);
	}

	on_show() {
		if (this.data) this.refresh(true);
	}

	_has_live() {
		return !!(this.data && this.data.live && this.data.live.length);
	}

	refresh(quiet) {
		if (this.loading) return;
		this.loading = true;
		this.root.classList.add("chsync-refreshing");
		if (!quiet) this.page.set_indicator(__("Loading"), "orange");
		return frappe
			.xcall(`${CHSYNC_API}.overview`, { days: this.days })
			.then((data) => {
				this.data = data;
				this.render();
				this.page.set_indicator(__("Live"), "green");
			})
			.catch(() => this.page.set_indicator(__("Could not load"), "red"))
			.finally(() => {
				this.loading = false;
				this.root.classList.remove("chsync-refreshing");
			});
	}

	_poll_live() {
		if (this._live_inflight) return;
		this._live_inflight = true;
		frappe
			.xcall(`${CHSYNC_API}.live`)
			.then((r) => {
				if (!this.data) return;
				const was = this._live_keys();
				this.data.live = r.live;
				this.data.as_of = r.as_of;
				this._render_live();
				this._render_updated();
				// A run started or finished: refresh counts and history.
				if (was !== this._live_keys()) this.refresh(true);
			})
			.catch(() => {})
			.finally(() => (this._live_inflight = false));
	}

	_poll_live_soon() {
		clearTimeout(this._soon);
		this._soon = setTimeout(() => this._poll_live(), 500);
	}

	_live_keys() {
		return (this.data.live || []).map((r) => `${r.configuration}:${r.display_status}`).join("|");
	}

	// ------------------------------------------------------------------ //
	// Rendering
	// ------------------------------------------------------------------ //
	render() {
		this._render_health();
		this._render_updated();
		this._render_kpis();
		this._render_live();
		this._render_doctypes();
		this._render_errors();
		this._render_analytics();
	}

	_render_updated() {
		const d = this.data;
		const ch = d.clickhouse || {};
		let conn;
		if (ch.ok === true) {
			conn = `<span class="chsync-pill chsync-good">${icon("check")}${__("ClickHouse connected")}</span>`;
		} else if (ch.ok === false) {
			conn = `<span class="chsync-pill chsync-critical" title="${esc(ch.message)}">${icon("x")}${__("ClickHouse unreachable")}</span>`;
		} else {
			conn = `<span class="chsync-pill chsync-neutral">${__("ClickHouse not checked")}</span>`;
		}
		this._region("conn").innerHTML = conn;
		this._region("updated").textContent = __("Updated {0} · auto-refresh", [
			frappe.datetime.str_to_user(d.as_of).split(" ").pop(),
		]);
	}

	_render_health() {
		const o = this.data.overall;
		const ch = this.data.clickhouse || {};
		const issues = [];
		if (ch.ok === false) issues.push(__("ClickHouse is unreachable"));
		if (o.stalled) issues.push(__("{0} sync(s) stalled", [o.stalled]));
		if (o.failed_doctypes.length)
			issues.push(__("{0} DocType(s) failing", [o.failed_doctypes.length]));

		let level, text;
		if (issues.length) {
			level = "critical";
			text = issues.join(" · ");
		} else if (o.failed_runs) {
			level = "warning";
			text = __("All DocTypes healthy now · {0} failed run(s) in the last {1} days", [
				o.failed_runs,
				this.data.window_days,
			]);
		} else {
			level = "good";
			text = __("All DocTypes healthy");
		}
		const glyph = { good: "check", warning: "alert", critical: "x" }[level];
		const label = { good: __("Healthy"), warning: __("Attention"), critical: __("Action needed") }[level];
		this._region("health").innerHTML = `
			<span class="chsync-pill chsync-${level} chsync-pill-lg">${icon(glyph)}${label}</span>
			<span class="chsync-health-text">${esc(text)}</span>`;
	}

	_render_kpis() {
		const o = this.data.overall;
		const tiles = [
			{
				label: __("DocTypes"),
				value: num(o.configured),
				sub: __("{0} enabled", [num(o.enabled)]),
				help: __("Sync configurations defined; disabled ones are not counted below."),
			},
			{
				label: __("Source records"),
				value: o.source_records == null ? "—" : num(o.source_records),
				sub: o.counts_approximate ? __("includes estimates") : __("in MariaDB"),
				help: o.source_partial
					? __("Some DocTypes could not be counted.")
					: __("COUNT(*) of every enabled source table."),
			},
			{
				label: __("Warehouse records"),
				value: o.warehouse_records == null ? "—" : num(o.warehouse_records),
				sub: __("in ClickHouse"),
				help: o.warehouse_partial
					? __("ClickHouse could not be counted for some DocTypes.")
					: __("Current records in ClickHouse (latest version per record, excluding soft-deleted)."),
			},
			{
				label: __("Pending"),
				value: o.pending_records == null ? "—" : num(o.pending_records),
				sub: __("changed since last sync"),
				help: __("Source records modified after each DocType's last successful sync position. Normal between scheduled runs."),
			},
			{
				label: __("Failed records"),
				value: num(o.failed_records),
				sub: __("{0} failed run(s), {1} days", [num(o.failed_runs), this.data.window_days]),
				help: __("Records in the batches that failed, summed over failed runs in the window."),
				level: o.failed_records ? "critical-text" : "",
			},
			{
				label: __("Synchronized"),
				value: o.sync_percent == null ? "—" : `${o.sync_percent}%`,
				sub: __("of source records"),
				help: __("Share of source records not pending: (source − pending) ÷ source."),
				meter: o.sync_percent,
			},
			{
				label: __("Last successful sync"),
				value: o.last_success ? relative(o.last_success) : "—",
				sub: o.last_success ? frappe.datetime.str_to_user(o.last_success) : __("none yet"),
				help: __("End time of the most recent completed run of any DocType."),
				small: true,
			},
		];
		this._region("kpis").innerHTML = tiles
			.map(
				(t) => `
				<div class="chsync-kpi" title="${esc(t.help)}">
					<div class="chsync-kpi-label">${esc(t.label)}</div>
					<div class="chsync-kpi-value ${t.level || ""} ${t.small ? "chsync-kpi-value-sm" : ""}">${t.value}</div>
					${t.meter != null ? meter(t.meter) : ""}
					<div class="chsync-kpi-sub">${esc(t.sub)}</div>
				</div>`
			)
			.join("");
		const note = o.counts_as_of
			? __("Record counts as of {0} (cached up to 10 minutes; Pending is always current).", [relative(o.counts_as_of)])
			: "";
		this._region("kpi-note").innerHTML = `${note} ${
			o.counts_approximate ? esc(__("Very large tables use the database's row estimate.")) : ""
		}`;
	}

	_render_live() {
		const runs = this.data.live || [];
		const o = this.data.overall;
		const parts = [];
		const running = runs.filter((r) => r.display_status === "Running").length;
		const queued = runs.filter((r) => r.display_status === "Queued").length;
		if (running) parts.push(__("{0} running", [running]));
		if (queued) parts.push(__("{0} queued", [queued]));
		this._region("live-count").textContent = parts.join(" · ");

		if (!runs.length) {
			this._region("live").innerHTML = `
				<div class="chsync-card chsync-empty">
					${icon("pause")} ${__("No sync is running or queued.")}
					<span class="text-muted">${__("Scheduled syncs appear here as soon as they are queued.")}</span>
				</div>`;
			return;
		}
		this._region("live").innerHTML = `<div class="chsync-live-grid">${runs
			.map((r) => this._live_card(r))
			.join("")}</div>`;
	}

	_live_card(r) {
		const status = r.display_status;
		if (status === "Queued") {
			const stopq = r.stop_requested
				? `<button class="btn btn-default btn-xs" disabled>${__("Stopping…")}</button>`
				: `<button class="btn btn-default btn-xs" data-action="stop" data-config="${esc(r.configuration)}">${__("Cancel")}</button>`;
			return `
				<div class="chsync-card chsync-live-card chsync-live-queued">
					<div class="chsync-live-head">
						<div>
							<div class="chsync-live-title">${esc(r.doctype || r.configuration)}</div>
							<div class="text-muted small">${esc(r.mode || __("Configured mode"))} · ${esc(r.trigger || "")} · ${__("queued")} ${relative(r.queued_at)}</div>
						</div>
						<div class="chsync-live-actions">
							<span class="chsync-pill chsync-neutral">${icon("clock")}${__("Queued")}</span>
							${stopq}
						</div>
					</div>
					<div class="text-muted small chsync-mt-sm">${__("Waiting for a background worker. Progress appears here when it starts.")}</div>
				</div>`;
		}
		const level = { Running: "info", Queued: "neutral", Stalled: "warning" }[status] || "neutral";
		const pct = r.percent;
		const progress =
			pct == null
				? `<div class="chsync-progress chsync-progress-unknown" title="${__("Total not known yet")}"><span></span></div>`
				: `<div class="chsync-progress" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><span style="width:${pct}%"></span></div>`;

		const known = (v) => (v == null ? `<span title="${__("Only known for incremental runs after the first sync")}">—</span>` : num(v));
		const metrics = [
			[__("Records to process"), r.total ? num(r.total) : "—"],
			[__("Processed"), num(r.rows_read)],
			[__("Written"), num(r.rows_written)],
			[__("Inserted (new)"), known(r.rows_new)],
			[__("Updated"), known(r.rows_updated)],
			[__("Re-checked"), known(r.rows_rechecked)],
			[__("Skipped"), num(r.rows_skipped)],
			[__("Failed"), r.rows_failed ? `<span class="critical-text">${num(r.rows_failed)}</span>` : "0"],
			[__("Batch"), r.batch ? num(r.batch) : "—"],
			[__("Speed"), r.rows_per_second ? __("{0} rec/s", [num(Math.round(r.rows_per_second))]) : "—"],
			[__("Elapsed"), duration(r.elapsed_seconds)],
			[__("Remaining"), r.eta_seconds != null ? `≈ ${duration(r.eta_seconds)}` : "—"],
		];
		const attempt = r.attempt > 1 ? ` · <span class="warning-text">${__("attempt {0} of {1}", [r.attempt, r.max_attempts || r.attempt])}</span>` : "";
		const cursor = r.cursor_modified
			? `${esc(frappe.datetime.str_to_user(r.cursor_modified))} · ${esc(r.cursor_name || "")}`
			: "—";
		const notes = [];
		if (r.child_rows_written) notes.push(__("{0} child rows written", [num(r.child_rows_written)]));
		if (r.last_error) notes.push(`<span class="critical-text">${esc(__("Last error: {0}", [r.last_error]))}</span>`);
		if (r.worker_lost)
			notes.push(`<span class="warning-text">${esc(__("The background job is no longer in the queue; the worker may have stopped."))}</span>`);
		else if (r.stalled)
			notes.push(`<span class="warning-text">${esc(__("No progress reported for {0}.", [duration(r.heartbeat_age_seconds)]))}</span>`);

		const stop = r.stop_requested
			? `<button class="btn btn-default btn-xs" disabled>${__("Stopping…")}</button>`
			: `<button class="btn btn-danger btn-xs" data-action="stop" data-config="${esc(r.configuration)}">${__("Stop")}</button>`;

		return `
			<div class="chsync-card chsync-live-card">
				<div class="chsync-live-head">
					<div>
						<div class="chsync-live-title">${esc(r.doctype || r.configuration)}</div>
						<div class="text-muted small">${esc(r.mode || __("Configured mode"))} · ${esc(r.trigger || "")} · ${esc(phase_label(r.phase))}${attempt}</div>
					</div>
					<div class="chsync-live-actions">
						<span class="chsync-pill chsync-${level}">${icon(status === "Running" ? "play" : "alert")}${esc(__(status))}</span>
						${stop}
					</div>
				</div>
				<div class="chsync-live-progress">
					${progress}
					<span class="chsync-live-pct">${pct == null ? "" : `${pct}%`}</span>
				</div>
				<dl class="chsync-metrics">
					${metrics.map(([k, v]) => `<div title="${esc(METRIC_HELP[k] || "")}"><dt>${esc(k)}</dt><dd>${v}</dd></div>`).join("")}
				</dl>
				<div class="chsync-live-foot text-muted small">
					<span title="${__("Position of the last record written (modified · name)")}">${__("Cursor")}: ${cursor}</span>
					${notes.length ? `<div>${notes.join(" · ")}</div>` : ""}
				</div>
			</div>`;
	}

	_render_doctypes() {
		if (!this.data) return;
		let rows = this.data.doctypes.slice();
		if (this.filter) {
			rows = rows.filter((r) =>
				`${r.configuration} ${r.doctype} ${r.table}`.toLowerCase().includes(this.filter)
			);
		}
		const { key, dir } = this.sort;
		rows.sort((a, b) => compare(a[key], b[key]) * dir);

		const cols = [
			["configuration", __("DocType"), ""],
			["mode", __("Mode"), ""],
			["last_status", __("Status"), ""],
			["source", __("Source"), "num"],
			["warehouse", __("Warehouse"), "num"],
			["difference", __("Difference"), "num"],
			["pending", __("Pending"), "num"],
			["sync_percent", __("Synced"), "num"],
			["last_success", __("Last success"), ""],
			["last_failure", __("Last failure"), ""],
			["next_run", __("Next run"), ""],
		];
		const head = cols
			.map(([k, label, cls]) => {
				const arrow = this.sort.key === k ? (this.sort.dir > 0 ? " ▲" : " ▼") : "";
				return `<th data-sort="${k}" class="${cls} chsync-sortable" tabindex="0">${esc(label)}${arrow}</th>`;
			})
			.join("");

		const body = rows
			.map((r) => {
				const status = this._doctype_status(r);
				const diff =
					r.difference == null
						? "—"
						: r.difference === 0
						? `<span class="text-muted">0</span>`
						: `<span class="${r.difference < 0 ? "critical-text" : "warning-text"}" title="${
								r.difference < 0
									? __("Fewer records in ClickHouse than in the source")
									: __("More records in ClickHouse than in the source (for example deletes not propagated)")
						  }">${r.difference > 0 ? "+" : ""}${num(r.difference)}</span>`;
				const wh =
					r.warehouse == null
						? `<span title="${esc(r.warehouse_note || "")}">—</span>`
						: r.warehouse_note === "no table"
						? `<span class="text-muted" title="${__("Table not created yet")}">0</span>`
						: num(r.warehouse);
				const src =
					r.source == null ? "—" : `${num(r.source)}${r.source_approximate ? ` <span class="text-muted" title="${__("Estimate from table statistics")}">≈</span>` : ""}`;
				const live = r.live_status === "Running" || r.live_status === "Queued" || r.live_status === "Stalled";
				const actions = !r.enabled
					? `<span class="text-muted small">${__("Disabled")}</span>`
					: `
						<button class="btn btn-default btn-xs" data-action="sync" data-mode="Incremental" data-config="${esc(r.configuration)}" ${live ? "disabled" : ""} title="${__("Start incremental sync")}">${__("Incremental")}</button>
						<button class="btn btn-default btn-xs" data-action="sync" data-mode="Full" data-config="${esc(r.configuration)}" ${live ? "disabled" : ""} title="${__("Start full sync")}">${__("Full")}</button>
						${live ? `<button class="btn btn-default btn-xs" data-action="stop" data-config="${esc(r.configuration)}">${__("Stop")}</button>` : ""}
						<button class="btn btn-default btn-xs" data-action="pause" data-paused="${r.paused ? 0 : 1}" data-config="${esc(r.configuration)}" title="${
							r.paused
								? __("Resume scheduled syncs for this DocType")
								: __("Pause scheduled syncs for this DocType (manual runs still work)")
						}">${r.paused ? __("Resume") : __("Pause")}</button>
						<button class="btn btn-link btn-xs" data-action="logs" data-config="${esc(r.configuration)}">${__("Logs")}</button>`;
				return `<tr class="${r.enabled ? "" : "chsync-disabled"}">
					<td>
						<a href="/app/clickhouse-sync-configuration/${encodeURIComponent(r.configuration)}">${esc(r.doctype)}</a>
						<div class="text-muted small">${esc(r.table)}${r.scheduled ? "" : ` · ${__("not scheduled")}`}</div>
					</td>
					<td>${esc(__(r.mode))}</td>
					<td>${status}</td>
					<td class="num">${src}</td>
					<td class="num">${wh}</td>
					<td class="num">${diff}</td>
					<td class="num">${r.pending == null ? "—" : num(r.pending)}</td>
					<td class="num chsync-synced">${r.sync_percent == null ? "—" : `${r.sync_percent}%${meter(r.sync_percent, true)}`}</td>
					<td>${r.last_success ? relative(r.last_success) : `<span class="text-muted">${__("never")}</span>`}</td>
					<td>${r.last_failure ? relative(r.last_failure) : `<span class="text-muted">—</span>`}</td>
					<td>${next_run_cell(r)}</td>
					<td class="chsync-actions">${actions}</td>
				</tr>`;
			})
			.join("");

		this._region("doctypes").innerHTML = rows.length
			? `<table class="table table-sm chsync-table">
					<thead><tr>${head}<th></th></tr></thead>
					<tbody>${body}</tbody>
				</table>`
			: `<div class="chsync-empty text-muted">${
					this.data.doctypes.length ? __("No DocType matches the filter.") : __("No sync configurations yet.")
			  }</div>`;
	}

	_doctype_status(r) {
		if (r.live_status === "Running")
			return `<span class="chsync-pill chsync-info">${icon("play")}${__("Running")}${r.live_percent != null ? ` ${r.live_percent}%` : ""}</span>`;
		if (r.live_status === "Queued") return `<span class="chsync-pill chsync-neutral">${icon("clock")}${__("Queued")}</span>`;
		if (r.live_status === "Stalled") return `<span class="chsync-pill chsync-warning">${icon("alert")}${__("Stalled")}</span>`;
		if (!r.last_status) return `<span class="text-muted">${__("Never run")}</span>`;
		const map = {
			Completed: ["good", "check", __("Completed")],
			Failed: ["critical", "x", __("Failed")],
			Cancelled: ["neutral", "pause", __("Cancelled")],
		};
		const [level, glyph, label] = map[r.last_status] || ["neutral", "clock", r.last_status];
		const title = [r.last_category, r.last_message].filter(Boolean).join(": ");
		const link = r.last_status === "Failed" && r.last_log
			? ` data-action="error" data-log="${esc(r.last_log)}" role="button" tabindex="0"`
			: "";
		return `<span class="chsync-pill chsync-${level}${link ? " chsync-clickable" : ""}" title="${esc(title)}"${link}>${icon(glyph)}${label}</span>`;
	}

	_render_errors() {
		const e = this.data.errors;
		const o = this.data.overall;
		if (!e.recent.length) {
			this._region("errors").innerHTML = `<div class="chsync-empty text-muted">${icon("check")} ${__("No failed runs in the last {0} days.", [this.data.window_days])}</div>`;
		} else {
			const rows = e.recent
				.map(
					(x) => `<tr>
						<td class="chsync-nowrap">${relative(x.at)}</td>
						<td>${esc(x.configuration)}</td>
						<td><span class="chsync-tag">${esc(__(x.category))}</span></td>
						<td class="chsync-msg" title="${esc(x.message)}">${esc(x.message || "")}</td>
						<td class="num">${x.batch ? num(x.batch) : "—"}</td>
						<td class="num">${x.retries == null ? "—" : num(x.retries)}</td>
						<td class="num">${num(x.rows_failed || 0)}</td>
						<td><button class="btn btn-link btn-xs" data-action="error" data-log="${esc(x.log)}">${__("Details")}</button></td>
					</tr>`
				)
				.join("");
			this._region("errors").innerHTML = `
				<table class="table table-sm chsync-table">
					<thead><tr>
						<th>${__("When")}</th><th>${__("DocType")}</th><th>${__("Category")}</th>
						<th>${__("Message")}</th><th class="num">${__("Batch")}</th>
						<th class="num">${__("Retries")}</th><th class="num">${__("Failed records")}</th><th></th>
					</tr></thead>
					<tbody>${rows}</tbody>
				</table>`;
		}

		const max = Math.max(1, ...e.by_category.map((c) => c.count));
		const cats = e.by_category.length
			? e.by_category
					.map(
						(c) => `<div class="chsync-hbar">
							<span class="chsync-hbar-label">${esc(__(c.category))}</span>
							<span class="chsync-hbar-track"><span style="width:${(c.count / max) * 100}%"></span></span>
							<span class="chsync-hbar-value">${num(c.count)}</span>
						</div>`
					)
					.join("")
			: `<div class="text-muted small">${__("None")}</div>`;
		const failing = o.failed_doctypes.length
			? `<ul class="chsync-list">${o.failed_doctypes.map((d) => `<li>${esc(d)}</li>`).join("")}</ul>
			   <button class="btn btn-default btn-xs" data-action="retry-failed">${__("Retry failed DocTypes")}</button>`
			: `<div class="text-muted small">${__("None. Every DocType's latest run succeeded.")}</div>`;

		this._region("error-summary").innerHTML = `
			<div class="chsync-mini-stats">
				<div><div class="chsync-mini-value">${num(e.failed_runs)}</div><div class="chsync-mini-label">${__("Failed runs")}</div></div>
				<div><div class="chsync-mini-value">${num(e.failed_rows)}</div><div class="chsync-mini-label">${__("Failed records")}</div></div>
			</div>
			<div class="chsync-card-title">${__("Failing now")}</div>
			${failing}
			<div class="chsync-card-title chsync-mt">${__("Failures by category")}</div>
			${cats}`;
	}

	_render_analytics() {
		const a = this.data.analytics;
		this._region("window-label").textContent = __("Last {0} days", [this.data.window_days]);
		const stats = [
			[__("Runs"), num(a.runs)],
			[__("Success rate"), a.success_rate == null ? "—" : `${a.success_rate}%`],
			[__("Average duration"), a.avg_duration == null ? "—" : duration(a.avg_duration)],
			[__("Records written"), num(a.rows_written_total)],
		];
		this._region("window-stats").innerHTML = stats
			.map(([k, v]) => `<div class="chsync-wstat"><span class="text-muted">${esc(k)}</span> <strong>${v}</strong></div>`)
			.join("");

		const dark = document.documentElement.getAttribute("data-theme") === "dark";
		const blue = dark ? "#3987e5" : "#2a78d6";
		const red = "#d03b3b";
		const gray = "#8a8986";
		const labels = a.labels.map((d) => frappe.datetime.str_to_user(d));

		this._chart("chart-rows", {
			type: "bar",
			height: 220,
			colors: [blue],
			data: { labels, datasets: [{ name: __("Records written"), values: a.rows_written }] },
			axisOptions: { xIsSeries: 1, xAxisMode: "tick", yAxisMode: "span" },
			barOptions: { spaceRatio: 0.4 },
			tooltipOptions: { formatTooltipY: (v) => num(v) },
		}, a.rows_written.some((v) => v));

		this._chart("chart-runs", {
			type: "bar",
			height: 220,
			colors: [blue, red, gray],
			data: {
				labels,
				datasets: [
					{ name: __("Completed"), values: a.runs_completed },
					{ name: __("Failed"), values: a.runs_failed },
					{ name: __("Cancelled"), values: a.runs_cancelled },
				],
			},
			axisOptions: { xIsSeries: 1, xAxisMode: "tick", yAxisMode: "span" },
			barOptions: { stacked: 1, spaceRatio: 0.4 },
		}, a.runs > 0);

		const tables = this.data.warehouse_tables || [];
		this._region("tables").innerHTML = tables.length
			? `<table class="table table-sm chsync-table">
				<thead><tr><th>${__("Table")}</th><th class="num">${__("Rows (incl. old versions)")}</th><th class="num">${__("Size")}</th><th class="num">${__("Parts")}</th></tr></thead>
				<tbody>${tables
					.map(
						(t) => `<tr><td>${esc(t.table)}</td><td class="num">${num(t.rows)}</td>
							<td class="num">${bytes(t.bytes)}</td><td class="num">${num(t.parts)}</td></tr>`
					)
					.join("")}</tbody></table>`
			: `<div class="chsync-empty text-muted">${
					this.data.clickhouse.ok === false ? __("ClickHouse is unreachable.") : __("No tables yet.")
			  }</div>`;

		const status_level = { Completed: "good", Failed: "critical", Cancelled: "neutral", Running: "info", Queued: "neutral" };
		const recent = a.recent_runs || [];
		this._region("recent").innerHTML = recent.length
			? `<table class="table table-sm chsync-table">
				<thead><tr><th>${__("When")}</th><th>${__("DocType")}</th><th>${__("Status")}</th>
				<th class="num">${__("Written")}</th><th class="num">${__("Duration")}</th></tr></thead>
				<tbody>${recent
					.map(
						(l) => `<tr>
							<td class="chsync-nowrap"><a href="/app/clickhouse-sync-log/${encodeURIComponent(l.name)}">${relative(l.creation)}</a></td>
							<td>${esc(l.configuration || "")}</td>
							<td><span class="chsync-pill chsync-${status_level[l.status] || "neutral"}">${esc(__(l.status))}</span></td>
							<td class="num">${num(l.rows_written)}</td>
							<td class="num">${duration(l.duration)}</td>
						</tr>`
					)
					.join("")}</tbody></table>`
			: `<div class="chsync-empty text-muted">${__("No runs yet.")}</div>`;
	}

	_chart(region, options, has_data) {
		const el = this._region(region);
		if (!has_data) {
			el.innerHTML = `<div class="chsync-empty text-muted">${__("No runs in this period.")}</div>`;
			this.charts[region] = null;
			return;
		}
		const chart = this.charts[region];
		if (chart && chart.update && el.querySelector("svg")) {
			chart.update(options.data);
			return;
		}
		el.innerHTML = "";
		this.charts[region] = new frappe.Chart(el, options);
	}

	// ------------------------------------------------------------------ //
	// Controls
	// ------------------------------------------------------------------ //
	_after_action(message) {
		frappe.show_alert({ message, indicator: "green" });
		this.refresh(true);
	}

	_start(config, mode) {
		const go = () =>
			frappe
				.xcall(`${CHSYNC_API}.start_sync`, { configuration: config, mode })
				.then((r) =>
					r.queued && r.queued.length
						? this._after_action(__("{0} sync queued for {1}", [__(mode), config]))
						: frappe.show_alert({
								message: r.message || __("A sync for {0} is already queued or running.", [config]),
								indicator: "orange",
						  })
				);
		if (mode === "Full") {
			frappe.confirm(
				__("Run a full sync of {0}? Every record is read again; on a large table this takes a while and adds load to the database.", [`<b>${esc(config)}</b>`]),
				go
			);
		} else {
			go();
		}
	}

	_confirm_start_all(mode) {
		const enabled = this.data ? this.data.overall.enabled : 0;
		const text =
			mode === "Full"
				? __("Queue a full sync of all {0} enabled DocTypes? Every record is read again.", [enabled])
				: __("Queue an incremental sync of all {0} enabled DocTypes?", [enabled]);
		frappe.confirm(text, () =>
			frappe
				.xcall(`${CHSYNC_API}.start_all`, { mode })
				.then((r) => {
					const skipped = (r.skipped || []).length;
					this._after_action(
						skipped
							? __("{0} sync(s) queued; {1} already queued or running", [r.queued.length, skipped])
							: __("{0} sync(s) queued", [r.queued.length])
					);
				})
		);
	}

	_retry_failed() {
		frappe
			.xcall(`${CHSYNC_API}.retry_failed`)
			.then((r) =>
				r.queued.length
					? this._after_action(__("Retry queued for {0}", [r.queued.join(", ")]))
					: frappe.show_alert({ message: __("No failed DocTypes to retry"), indicator: "blue" })
			);
	}

	_set_paused(config, paused) {
		frappe
			.xcall(`${CHSYNC_API}.set_paused`, { configuration: config, paused: paused ? 1 : 0 })
			.then(() =>
				this._after_action(
					paused
						? __("Scheduled syncs paused for {0}", [config])
						: __("Scheduled syncs resumed for {0}", [config])
				)
			);
	}

	_stop(config) {
		frappe.confirm(
			__("Stop the sync of {0}? It stops after the current batch; progress so far is kept and the next run resumes from there.", [`<b>${esc(config)}</b>`]),
			() =>
				frappe
					.xcall(`${CHSYNC_API}.stop_sync`, { configuration: config })
					.then(() => this._after_action(__("Stop requested for {0}", [config])))
		);
	}

	_stop_all() {
		const n = this.data ? this.data.live.length : 0;
		if (!n) {
			frappe.show_alert({ message: __("Nothing is running or queued"), indicator: "blue" });
			return;
		}
		frappe.confirm(__("Stop all {0} running or queued syncs after their current batch?", [n]), () =>
			frappe
				.xcall(`${CHSYNC_API}.stop_all`)
				.then((r) => this._after_action(__("Stop requested for {0} sync(s)", [r.stopping.length])))
		);
	}

	_recount(config) {
		frappe.show_alert({ message: __("Counting records…"), indicator: "blue" });
		frappe
			.xcall(`${CHSYNC_API}.recount`, config ? { configuration: config } : {})
			.then(() => this._after_action(__("Record counts updated")));
	}

	_show_error(log) {
		frappe.xcall(`${CHSYNC_API}.error_detail`, { log }).then((e) => {
			const d = e.detail || {};
			const rows = [
				[__("DocType"), esc(e.configuration)],
				[__("Category"), esc(__(e.category))],
				[__("Started"), e.start_time ? esc(frappe.datetime.str_to_user(e.start_time)) : "—"],
				[__("Ended"), e.end_time ? esc(frappe.datetime.str_to_user(e.end_time)) : "—"],
				[__("Duration"), duration(e.duration)],
				[__("Trigger"), esc(e.trigger || "")],
				[__("Records read / written / failed"), `${num(e.rows_read)} / ${num(e.rows_written)} / ${num(e.rows_failed)}`],
				[__("Failed in batch"), d.batch ? num(d.batch) : "—"],
				[__("Records in failed batch"), d.failed_batch_first ? `${esc(d.failed_batch_first)} … ${esc(d.failed_batch_last || "")}` : "—"],
				[__("Phase"), d.phase ? esc(phase_label(d.phase)) : "—"],
				[__("Retries"), d.retries == null ? "—" : num(d.retries)],
				[__("Cursor at failure"), d.cursor_modified ? `${esc(frappe.datetime.str_to_user(d.cursor_modified))} · ${esc(d.cursor_name || "")}` : "—"],
				[__("Worker host"), esc(e.execution_host || "—")],
			];
			const dialog = new frappe.ui.Dialog({
				title: __("Failed run {0}", [e.name]),
				size: "extra-large",
				fields: [{ fieldtype: "HTML", fieldname: "body" }],
				primary_action_label: __("Retry this DocType"),
				primary_action: () => {
					dialog.hide();
					this._start(e.configuration, "Incremental");
				},
				secondary_action_label: __("Open log"),
				secondary_action: () => frappe.set_route("Form", "ClickHouse Sync Log", e.name),
			});
			dialog.fields_dict.body.$wrapper.html(`
				<div class="chsync-dash chsync-error-detail">
					<dl class="chsync-metrics chsync-metrics-wide">
						${rows.map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${v}</dd></div>`).join("")}
					</dl>
					<div class="chsync-card-title">${__("Message")}</div>
					<div class="chsync-error-message">${esc(e.message || "—")}</div>
					<div class="chsync-card-title">${__("Traceback")}</div>
					<pre class="chsync-trace">${esc(e.exception || __("No traceback recorded."))}</pre>
					${d.log ? "" : `<div class="text-muted small">${__("Batch and retry details are kept for 30 days and are not available for this run.")}</div>`}
				</div>`);
			dialog.show();
		});
	}
}

// ---------------------------------------------------------------------- //
// Formatting helpers
// ---------------------------------------------------------------------- //
function esc(value) {
	return frappe.utils.escape_html(value == null ? "" : String(value));
}

function num(value) {
	// Plain text in the system number format. (frappe.format returns HTML
	// for Int fields in recent versions, which must not be used here.)
	if (value == null || value === "") return "—";
	const n = Number(value);
	if (isNaN(n)) return esc(value);
	if (typeof window.format_number === "function") {
		return window.format_number(n, null, 0);
	}
	return n.toLocaleString();
}

function next_run_cell(r) {
	if (!r.enabled) return `<span class="text-muted">—</span>`;
	if (r.paused) return `<span class="chsync-pill chsync-neutral" title="${esc(__("Scheduled syncs are paused; manual runs still work"))}">${icon("pause")}${__("Paused")}</span>`;
	if (!r.next_run) return `<span class="text-muted">—</span>`;
	const when = frappe.datetime.str_to_obj(r.next_run);
	const now = frappe.datetime.str_to_obj(frappe.datetime.now_datetime());
	const minutes = Math.max(0, Math.round((when - now) / 60000));
	const label =
		minutes < 1
			? __("next tick")
			: minutes < 60
			? __("in {0} min", [minutes])
			: frappe.datetime.str_to_user(r.next_run);
	const every = r.sync_interval >= 60 && r.sync_interval % 60 === 0
		? __("every {0} h", [r.sync_interval / 60])
		: __("every {0} min", [r.sync_interval]);
	return `<span title="${esc(frappe.datetime.str_to_user(r.next_run))}">${esc(label)}</span>
		<div class="text-muted small">${esc(every)}</div>`;
}

function relative(dt) {
	if (!dt) return "—";
	return `<span title="${esc(frappe.datetime.str_to_user(dt))}">${frappe.datetime.comment_when(dt)}</span>`;
}

function duration(seconds) {
	if (seconds == null || isNaN(seconds)) return "—";
	let s = Math.max(0, Math.round(Number(seconds)));
	const h = Math.floor(s / 3600);
	const m = Math.floor((s % 3600) / 60);
	s = s % 60;
	if (h) return `${h}h ${String(m).padStart(2, "0")}m`;
	if (m) return `${m}m ${String(s).padStart(2, "0")}s`;
	return `${s}s`;
}

function bytes(n) {
	if (n == null) return "—";
	const units = ["B", "KB", "MB", "GB", "TB"];
	let i = 0;
	let v = Number(n);
	while (v >= 1024 && i < units.length - 1) {
		v /= 1024;
		i++;
	}
	return `${v.toFixed(v >= 100 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function meter(percent, inline) {
	const p = Math.max(0, Math.min(100, Number(percent) || 0));
	return `<span class="chsync-meter${inline ? " chsync-meter-inline" : ""}" aria-hidden="true"><span style="width:${p}%"></span></span>`;
}

function compare(a, b) {
	if (a == null && b == null) return 0;
	if (a == null) return 1;
	if (b == null) return -1;
	if (typeof a === "number" && typeof b === "number") return a - b;
	return String(a).localeCompare(String(b));
}

function phase_label(phase) {
	const labels = {
		starting: __("Starting"),
		connecting: __("Connecting to ClickHouse"),
		"checking schema": __("Checking table schema"),
		"child tables": __("Syncing child tables"),
		counting: __("Counting records"),
		syncing: __("Copying records"),
		retrying: __("Waiting to retry"),
		"reconciling deletes": __("Reconciling deletes"),
		optimizing: __("Compacting table"),
		completed: __("Finishing"),
	};
	return labels[phase] || phase || "";
}

function icon(name) {
	// Small inline glyphs so status never relies on colour alone.
	const paths = {
		check: "M3 8.5l3 3 7-7",
		x: "M4 4l8 8M12 4l-8 8",
		alert: "M8 3v6M8 12.5v.5",
		play: "M5 3.5v9l7-4.5z",
		pause: "M5 4v8M11 4v8",
		clock: "M8 4v4l3 2",
	};
	const fill = name === "play" ? "currentColor" : "none";
	return `<svg class="chsync-icon" width="12" height="12" viewBox="0 0 16 16" aria-hidden="true"><path d="${paths[name]}" fill="${fill}" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}
