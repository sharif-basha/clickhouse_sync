// Copyright (c) 2026, RySS / Common Ground Initiative and contributors
// For license information, please see license.txt

frappe.ui.form.on("ClickHouse Sync Configuration", {
	refresh(frm) {
		if (frm.is_new()) {
			return;
		}

		frm.add_custom_button(__("Sync Now"), () => start_sync(frm)).addClass("btn-primary");

		frm.add_custom_button(__("Refresh Schema"), () => {
			frm.call({ doc: frm.doc, method: "refresh_schema", freeze: true }).then((r) => {
				const res = r.message || {};
				frappe.msgprint({
					title: __("Schema Reconciled"),
					indicator: "green",
					message: __("Created: {0}<br>Added: {1}<br>Widened: {2}<br>Ignored: {3}", [
						res.created ? __("yes") : __("no"),
						(res.added || []).join(", ") || "-",
						(res.widened || []).join(", ") || "-",
						(res.ignored || []).join(", ") || "-",
					]),
				});
			});
		}, __("Schema"));

		frm.add_custom_button(__("Preview DDL"), () => {
			frm.call({ doc: frm.doc, method: "preview_ddl" }).then((r) => {
				const res = r.message || {};
				frappe.msgprint({
					title: __("CREATE TABLE for {0}", [res.table_name]),
					message: `<pre style="white-space:pre-wrap">${frappe.utils.escape_html(
						res.ddl || ""
					)}</pre>`,
					wide: true,
				});
			});
		}, __("Schema"));

		frm.add_custom_button(__("Reset Cursor"), () => {
			frappe.confirm(
				__("Clear the incremental cursor? The next run will re-scan everything."),
				() => frm.call({ doc: frm.doc, method: "reset_cursor" }).then(() => frm.reload_doc())
			);
		}, __("Schema"));

		frm.add_custom_button(__("View Logs"), () => {
			frappe.set_route("List", "ClickHouse Sync Log", { configuration: frm.doc.name });
		}, __("Logs"));

		if (frm.doc.source_doctype) {
			frm.add_custom_button(__("Select Fields to Exclude"), () =>
				open_exclude_dialog(frm), __("Privacy")
			);
		}
	},

	source_doctype(frm) {
		// Clearing the source doctype makes any excluded field names meaningless.
		if (!frm.doc.source_doctype && frm.doc.excluded_fields) {
			frm.set_value("excluded_fields", "");
		}
	},
});

function open_exclude_dialog(frm) {
	// Load the source DocType's fields and present them as checkboxes, so the
	// user picks fields to exclude instead of typing fieldnames by hand.
	frappe.model.with_doctype(frm.doc.source_doctype, () => {
		const meta = frappe.get_meta(frm.doc.source_doctype);
		const NO_DATA = ["Section Break", "Column Break", "Tab Break", "HTML", "Button"];
		const fields = (meta.fields || []).filter((f) => !NO_DATA.includes(f.fieldtype));

		// Currently-excluded set (parsed from the stored comma/newline text).
		const current = new Set(
			(frm.doc.excluded_fields || "")
				.split(/[\n,]/)
				.map((s) => s.trim())
				.filter(Boolean)
		);

		const options = fields.map((f) => ({
			label: `${__(f.label || f.fieldname)}  ·  ${f.fieldname}  (${f.fieldtype})`,
			value: f.fieldname,
			checked: current.has(f.fieldname),
		}));

		const d = new frappe.ui.Dialog({
			title: __("Exclude fields from {0}", [frm.doc.source_doctype]),
			size: "large",
			fields: [
				{
					fieldtype: "HTML",
					fieldname: "hint",
					options: `<p class="text-muted small">${__(
						"Ticked fields are removed from the ClickHouse table entirely — useful for personal or sensitive columns you don't want in the warehouse."
					)}</p>`,
				},
				{
					fieldtype: "MultiCheck",
					fieldname: "fields",
					label: __("Fields"),
					options: options,
					columns: 2,
				},
			],
			primary_action_label: __("Apply"),
			primary_action() {
				const selected = d.get_value("fields") || [];
				// Store newline-separated; parse_excluded_fields handles either.
				frm.set_value("excluded_fields", selected.join("\n"));
				d.hide();
				frappe.show_alert({
					message: __("{0} field(s) marked for exclusion. Save to apply.", [selected.length]),
					indicator: "blue",
				});
			},
		});

		// Quick select-all / clear helpers.
		d.$body.prepend(`
			<div class="mb-2">
				<button class="btn btn-xs btn-default tb-ex-all">${__("Select all")}</button>
				<button class="btn btn-xs btn-default tb-ex-none">${__("Clear all")}</button>
			</div>
		`);
		d.$body.find(".tb-ex-all").on("click", () => {
			d.$body.find('[data-fieldname="fields"] input[type=checkbox]').prop("checked", true).trigger("change");
		});
		d.$body.find(".tb-ex-none").on("click", () => {
			d.$body.find('[data-fieldname="fields"] input[type=checkbox]').prop("checked", false).trigger("change");
		});

		d.show();
	});
}

function start_sync(frm) {
	frm.call({
		doc: frm.doc,
		method: "enqueue_sync",
		args: { trigger: "Manual" },
		freeze: true,
		freeze_message: __("Queuing sync job..."),
	}).then((r) => {
		if (r.message && r.message.job_id) {
			open_progress_dialog(frm);
		}
	});
}

function open_progress_dialog(frm) {
	const d = new frappe.ui.Dialog({
		title: __("Syncing {0}", [frm.doc.name]),
		fields: [{ fieldtype: "HTML", fieldname: "progress_area" }],
		primary_action_label: __("Cancel Sync"),
		primary_action() {
			frm.call({ doc: frm.doc, method: "cancel_sync" }).then(() => {
				frappe.show_alert({ message: __("Cancellation requested."), indicator: "orange" });
			});
		},
	});

	const $body = $(d.fields_dict.progress_area.wrapper);
	$body.html(`
		<div class="clickhouse-sync-progress">
			<div class="progress" style="height: 18px;">
				<div class="progress-bar" role="progressbar" style="width: 0%;">0%</div>
			</div>
			<p class="text-muted small mt-2" data-role="phase">${__("Starting...")}</p>
			<p class="small" data-role="counts"></p>
		</div>
	`);

	const handler = (data) => {
		if (!data || data.configuration !== frm.doc.name) {
			return;
		}
		const pct = Math.min(100, data.percent || 0);
		$body.find(".progress-bar").css("width", pct + "%").text(pct + "%");
		$body.find('[data-role="phase"]').text(__("Phase: {0}", [data.phase]));
		$body.find('[data-role="counts"]').text(
			__("Read: {0} | Written: {1} | Total: {2}", [
				data.rows_read,
				data.rows_written,
				data.total,
			])
		);
		if (data.phase === "completed") {
			d.set_primary_action(__("Close"), () => d.hide());
			frm.reload_doc();
		}
	};

	frappe.realtime.on("clickhouse_sync_progress", handler);
	d.onhide = () => frappe.realtime.off("clickhouse_sync_progress", handler);
	d.show();
}
