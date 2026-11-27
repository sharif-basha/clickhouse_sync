# Copyright (c) 2026, RySS / Common Ground Initiative and contributors
# For license information, please see license.txt
"""The workspace only shows what its ``content`` places, and each placed block
must name a shortcut, card, number card or chart that exists. A broken name
renders as an empty gap on the live site, so check them here."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "clickhouse_warehouse"
WORKSPACE = MODULE / "workspace" / "clickhouse_warehouse" / "clickhouse_warehouse.json"


def _names(folder: str, key: str) -> set[str]:
    return {json.loads(p.read_text())[key] for p in (MODULE / folder).glob("*/*.json")}


class TestWorkspace(unittest.TestCase):
    def setUp(self) -> None:
        self.ws = json.loads(WORKSPACE.read_text())
        self.blocks = json.loads(self.ws["content"])

    def _placed(self, block_type: str, key: str) -> list[str]:
        return [b["data"][key] for b in self.blocks if b["type"] == block_type]

    def test_every_shortcut_card_and_widget_is_placed(self) -> None:
        # Defined but not placed = invisible (the previous version placed no
        # shortcuts or link cards at all).
        self.assertEqual(
            sorted(self._placed("shortcut", "shortcut_name")),
            sorted(s["label"] for s in self.ws["shortcuts"]),
        )
        self.assertEqual(
            sorted(self._placed("card", "card_name")),
            sorted(l["label"] for l in self.ws["links"] if l["type"] == "Card Break"),
        )
        self.assertEqual(
            sorted(self._placed("number_card", "number_card_name")),
            sorted(c["number_card_name"] for c in self.ws["number_cards"]),
        )
        self.assertEqual(
            sorted(self._placed("chart", "chart_name")),
            sorted(c["chart_name"] for c in self.ws["charts"]),
        )

    def test_placed_number_cards_and_charts_exist(self) -> None:
        self.assertLessEqual(set(self._placed("number_card", "number_card_name")), _names("number_card", "name"))
        self.assertLessEqual(set(self._placed("chart", "chart_name")), _names("dashboard_chart", "chart_name"))

    def test_links_point_at_real_targets(self) -> None:
        doctypes = _names("doctype", "name")
        reports = _names("report", "name")
        pages = _names("page", "name")
        targets = {"DocType": doctypes, "Report": reports, "Page": pages}
        for item in [l for l in self.ws["links"] if l["type"] == "Link"] + self.ws["shortcuts"]:
            kind = item.get("link_type") or item.get("type")
            with self.subTest(label=item["label"]):
                self.assertIn(item["link_to"], targets[kind])

    def test_card_break_counts_match_their_links(self) -> None:
        links = self.ws["links"]
        for i, item in enumerate(links):
            if item["type"] != "Card Break":
                continue
            following = []
            for nxt in links[i + 1 :]:
                if nxt["type"] == "Card Break":
                    break
                following.append(nxt)
            with self.subTest(card=item["label"]):
                self.assertEqual(item["link_count"], len(following))

    def test_shortcut_stats_filters_are_valid_json(self) -> None:
        for s in self.ws["shortcuts"]:
            if s.get("stats_filter"):
                with self.subTest(shortcut=s["label"]):
                    self.assertIsInstance(json.loads(s["stats_filter"]), dict)

    def test_block_ids_are_unique(self) -> None:
        ids = [b["id"] for b in self.blocks]
        self.assertEqual(len(ids), len(set(ids)))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
