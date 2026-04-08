from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import scripts.export_legacy_manual_pool_records as export_script


class ExportLegacyManualPoolRecordsTests(unittest.TestCase):
    def test_parser_accepts_linked_bitable_url_alias_and_task_name(self) -> None:
        parser = export_script._build_parser()
        args = parser.parse_args(
            [
                "--linked-bitable-url",
                "https://example.com/base/app?table=tbl&view=vew",
                "--task-name",
                "Duet",
            ]
        )
        self.assertEqual(args.url, "https://example.com/base/app?table=tbl&view=vew")
        self.assertEqual(args.task_name, "Duet")

    def test_matches_task_specific_manual_creator_id(self) -> None:
        self.assertTrue(export_script._matches_task_specific_manual_creator_id("Duet4/7转人工1", "Duet"))
        self.assertFalse(export_script._matches_task_specific_manual_creator_id("MINISO4/7转人工1", "Duet"))
        self.assertFalse(export_script._matches_task_specific_manual_creator_id("Duet_manual_1", "Duet"))

    def test_export_filters_to_task_specific_legacy_manual_pool_rows(self) -> None:
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="AI回信管理",
            view_id="vew",
            view_name="总视图",
        )
        existing_records = [
            (
                "rec_duet_manual_1",
                {
                    "达人ID": "Duet4/7转人工1",
                    "平台": "转人工",
                    "ai是否通过": "转人工",
                    "任务名": "Duet",
                    "主页链接": "",
                },
            ),
            (
                "rec_duet_manual_2",
                {
                    "达人ID": "Duet4/8转人工100",
                    "平台": "instagram",
                    "ai是否通过": "转人工",
                    "任务名": "Duet",
                    "主页链接": "",
                },
            ),
            (
                "rec_miniso_manual",
                {
                    "达人ID": "MINISO4/7转人工1",
                    "平台": "转人工",
                    "ai是否通过": "转人工",
                    "任务名": "MINISO",
                    "主页链接": "",
                },
            ),
            (
                "rec_normal",
                {
                    "达人ID": "alpha",
                    "平台": "instagram",
                    "ai是否通过": "是",
                    "任务名": "Duet",
                    "主页链接": "https://www.instagram.com/alpha",
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.export_legacy_manual_pool_records.resolve_bitable_view_from_url",
            return_value=resolved_view,
        ), patch(
            "scripts.export_legacy_manual_pool_records._canonicalize_target_url",
            return_value=resolved_view.source_url,
        ), patch(
            "scripts.export_legacy_manual_pool_records._fetch_field_schemas",
            return_value={},
        ), patch(
            "scripts.export_legacy_manual_pool_records._fetch_existing_records",
            return_value=existing_records,
        ), patch(
            "scripts.export_legacy_manual_pool_records._resolve_owner_scope_field_name",
            return_value="",
        ):
            result = export_script.export_legacy_manual_pool_records(
                client=object(),
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                task_name_filter="Duet",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["existing_record_count"], 4)
            self.assertEqual(result["candidate_record_count"], 2)
            self.assertEqual(result["task_filtered_candidate_count"], 2)
            self.assertEqual(result["strict_candidate_count"], 2)

            report = pd.read_excel(result["report_xlsx_path"]).fillna("")
            self.assertEqual(set(report["record_id"]), {"rec_duet_manual_1", "rec_duet_manual_2"})
            duet_manual_1 = report.loc[report["record_id"] == "rec_duet_manual_1"].iloc[0].to_dict()
            duet_manual_2 = report.loc[report["record_id"] == "rec_duet_manual_2"].iloc[0].to_dict()
            self.assertEqual(duet_manual_1["platform"], "转人工")
            self.assertEqual(duet_manual_1["recommended_action"], "review_then_delete")
            self.assertEqual(duet_manual_2["platform"], "instagram")
            self.assertEqual(duet_manual_2["recommended_action"], "review")


if __name__ == "__main__":
    unittest.main()
