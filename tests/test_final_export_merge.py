from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.final_export_merge import _compute_engagement_rate, build_all_platforms_final_review_artifacts


class FinalExportMergeTests(unittest.TestCase):
    def test_keep_lookup_falls_back_across_platforms_for_mail_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha/",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "@username": "alpha",
                        "URL": "https://www.tiktok.com/@alpha",
                        "last_mail_time": "2026-04-03T05:27:00+08:00",
                        "last_mail_snippet": "hello from mail thread",
                    }
                ]
            ).to_excel(keep_workbook, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "instagram": {
                        "final_review": str(instagram_export),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={"responsible_name": "陈俊仁"},
            )

            workbook = pd.read_excel(output_path).fillna("")
            self.assertEqual(workbook.loc[0, "full body"], "hello from mail thread")

    def test_fast_path_brand_message_fields_flow_into_export_and_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(tiktok_export, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            raw_dir = root / "upstream" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_mail_path = raw_dir / "alpha-brand-message.eml"
            raw_mail_path.write_text("Subject: alpha\n\nHi team,\n\nFull body rate is $300 per video.", encoding="utf-8")
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "@username": "alpha",
                        "URL": "https://www.tiktok.com/@alpha",
                        "brand_message_sent_at": "2026-03-30T21:55:31+00:00",
                        "brand_message_snippet": "Hi team, rate is $300 per video.",
                        "brand_message_raw_path": str(raw_mail_path),
                        "creator_emails": "alpha@example.com | manager@example.com",
                        "matched_contact_email": "alpha@example.com",
                        "matched_contact_name": "Alpha",
                    }
                ]
            ).to_excel(keep_workbook, index=False)
            positioning_review = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "适合家庭类合作",
                    }
                ]
            ).to_excel(positioning_review, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(positioning_review),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={"responsible_name": "陈俊仁"},
            )

            workbook = pd.read_excel(output_path)
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(workbook.loc[0, "当前网红报价"], "$300 per video")
            self.assertEqual(workbook.loc[0, "达人最后一次回复邮件时间"], "2026/03/31")
            self.assertEqual(workbook.loc[0, "full body"], "Hi team,\n\nFull body rate is $300 per video.")
            self.assertEqual(payload["rows"][0]["full body"], "Hi team,\n\nFull body rate is $300 per video.")
            self.assertEqual(payload["rows"][0]["达人回复的最后一封邮件内容"], "Hi team,\n\nFull body rate is $300 per video.")
            self.assertEqual(payload["rows"][0]["__feishu_update_mode"], "create_or_mail_only_update")
            self.assertEqual(payload["rows"][0]["creator_emails"], "alpha@example.com | manager@example.com")
            self.assertEqual(payload["rows"][0]["matched_contact_email"], "alpha@example.com")
            self.assertEqual(payload["rows"][0]["matched_contact_name"], "Alpha")
            self.assertEqual(payload["rows"][0]["__brand_message_raw_path"], str(raw_mail_path))
            self.assertEqual(payload["rows"][0]["__last_mail_raw_path"], str(raw_mail_path))
            self.assertEqual(payload["rows"][0]["__feishu_attachment_local_paths"], [str(raw_mail_path.resolve())])

    def test_mail_context_can_match_keep_row_by_creator_id_when_username_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "",
                        "profile_url": "",
                        "upload_handle": "",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "",
                        "profile_url": "",
                        "upload_handle": "",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "适合家庭类合作",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "达人ID": "alpha",
                        "URL": "https://www.tiktok.com/@not-alpha",
                        "brand_message_sent_at": "2026-03-30T21:55:31+00:00",
                        "brand_message_snippet": "My rate is $500 per video.",
                    }
                ]
            ).to_excel(keep_workbook, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(tiktok_positioning),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={"responsible_name": "陈俊仁"},
            )

            workbook = pd.read_excel(output_path).fillna("")
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(workbook.loc[0, "当前网红报价"], "$500 per video")
            self.assertEqual(workbook.loc[0, "达人最后一次回复邮件时间"], "2026/03/31")
            self.assertEqual(workbook.loc[0, "full body"], "My rate is $500 per video.")
            self.assertEqual(payload["rows"][0]["达人回复的最后一封邮件内容"], "My rate is $500 per video.")

    def test_quote_text_falls_back_to_resolved_full_body_when_snippet_has_no_quote(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "适合家庭类合作",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            raw_dir = root / "upstream" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_mail_path = raw_dir / "alpha-brand-message.eml"
            raw_mail_path.write_text(
                "Subject: alpha\n\nHi team,\n\nOur full package is $1,500 USD for one video.",
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "@username": "alpha",
                        "URL": "https://www.tiktok.com/@alpha",
                        "brand_message_sent_at": "2026-03-30T21:55:31+00:00",
                        "brand_message_snippet": "Hi team, thanks for reaching out.",
                        "brand_message_raw_path": str(raw_mail_path),
                    }
                ]
            ).to_excel(keep_workbook, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(tiktok_positioning),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={"responsible_name": "陈俊仁"},
            )

            workbook = pd.read_excel(output_path)
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(workbook.loc[0, "当前网红报价"], "$1,500 USD for one video")
            self.assertEqual(payload["rows"][0]["当前网红报价"], "$1,500 USD for one video")

    def test_payload_carries_row_mail_file_and_shared_workbook_attachment_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            positioning_review = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            raw_dir = root / "upstream" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_mail_path = raw_dir / "alpha-last.eml"
            raw_mail_path.write_text("Subject: alpha\n\nhello", encoding="utf-8")
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "适合家庭类合作",
                    }
                ]
            ).to_excel(positioning_review, index=False)
            pd.DataFrame(
                [
                    {
                        "Platform": "instagram",
                        "@username": "alpha",
                        "URL": "https://www.instagram.com/alpha",
                        "last_mail_time": "2026-03-31",
                        "last_mail_snippet": "hello",
                        "last_mail_raw_path": str(raw_mail_path),
                    }
                ]
            ).to_excel(keep_workbook, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            artifacts = build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "instagram": {
                        "final_review": str(instagram_export),
                        "positioning_card_review": str(positioning_review),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={"responsible_name": "陈俊仁"},
            )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["rows"][0]["__feishu_update_mode"], "create_or_mail_only_update")
            self.assertEqual(payload["rows"][0]["__last_mail_raw_path"], str(raw_mail_path))
            self.assertEqual(payload["rows"][0]["__feishu_attachment_local_paths"], [str(raw_mail_path.resolve())])
            self.assertEqual(payload["__feishu_shared_attachment_local_paths"], [str(output_path.resolve())])
            self.assertEqual(artifacts["all_platforms_upload_shared_attachment_local_paths"], [str(output_path.resolve())])

    def test_keep_workbook_row_owner_fields_override_task_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(tiktok_export, index=False)

            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            positioning_review = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "@username": "alpha",
                        "URL": "https://www.tiktok.com/@alpha",
                        "brand_message_sent_at": "2026-03-30T21:55:31+00:00",
                        "brand_message_snippet": "Hi Lilith, rate is $300 per video.",
                        "达人对接人": "Sherry97",
                        "达人对接人_employee_id": "ou_lilith",
                        "达人对接人_employee_record_id": "rec_lilith",
                        "达人对接人_employee_email": "lilith@amagency.biz",
                        "达人对接人_owner_name": "lilith@amagency.biz",
                        "任务名": "SKG",
                        "linked_bitable_url": "https://bitable.example/skg",
                    }
                ]
            ).to_excel(keep_workbook, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "适合家庭类合作",
                    }
                ]
            ).to_excel(positioning_review, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(positioning_review),
                    }
                },
                keep_workbook=keep_workbook,
                task_owner={
                    "responsible_name": "唐瑞霞",
                    "employee_id": "ou_rhea",
                    "employee_record_id": "rec_rhea",
                    "employee_email": "rhea@amagency.biz",
                    "owner_name": "rhea@amagency.biz",
                    "task_name": "SKG",
                    "linked_bitable_url": "https://bitable.example/skg",
                },
            )

            workbook = pd.read_excel(output_path)
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(workbook.loc[0, "达人对接人"], "Sherry97")
            self.assertEqual(payload["rows"][0]["__feishu_update_mode"], "create_or_mail_only_update")
            self.assertNotIn("达人对接人", payload["rows"][0])
            self.assertNotIn("达人对接人_employee_id", payload["rows"][0])
            self.assertEqual(payload["rows"][0]["linked_bitable_url"], "https://bitable.example/skg")

    def test_payload_skips_processing_failures_and_preserves_uploadable_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "good_creator",
                        "username": "good_creator",
                        "profile_url": "https://www.instagram.com/good_creator",
                        "upload_handle": "good_creator",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    },
                    {
                        "identifier": "bad_creator",
                        "username": "bad_creator",
                        "profile_url": "https://www.instagram.com/bad_creator",
                        "upload_handle": "bad_creator",
                        "final_status": "Error",
                        "final_reason": "视觉复核超时：bad_creator 超过 120 秒未完成",
                    },
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame([]).to_excel(tiktok_export, index=False)

            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            pd.DataFrame(
                [
                    {
                        "identifier": "good_creator",
                        "username": "good_creator",
                        "profile_url": "https://www.instagram.com/good_creator",
                        "upload_handle": "good_creator",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭博主",
                        "fit_summary": "适合家庭类合作",
                        "positioning_error": "",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            instagram_data_path = root / "data" / "instagram" / "instagram_data.json"
            instagram_data_path.parent.mkdir(parents=True, exist_ok=True)
            instagram_data_path.write_text(
                json.dumps(
                    [
                        {
                            "username": "good_creator",
                            "url": "https://www.instagram.com/good_creator",
                            "followersCount": 52300,
                            "followsCount": 1200,
                            "latestPosts": [{"videoViewCount": 22100, "likesCount": 1600}],
                        },
                        {
                            "username": "bad_creator",
                            "url": "https://www.instagram.com/bad_creator",
                            "followersCount": 48300,
                            "latestPosts": [{"videoViewCount": 11000, "likesCount": 400}],
                        },
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            artifacts = build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={
                    "instagram": {
                        "final_review": str(instagram_export),
                        "positioning_card_review": str(instagram_positioning),
                    },
                    "tiktok": {"final_review": str(tiktok_export)},
                },
                task_owner={"responsible_name": "陈俊仁"},
            )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            archive_json_path = exports_dir / "feishu_upload_local_archive" / "skipped_from_feishu_upload.json"
            archive_xlsx_path = exports_dir / "feishu_upload_local_archive" / "skipped_from_feishu_upload.xlsx"
            archive_payload = json.loads(archive_json_path.read_text(encoding="utf-8"))
            self.assertEqual(artifacts["source_row_count"], 2)
            self.assertEqual(artifacts["row_count"], 2)
            self.assertEqual(artifacts["skipped_row_count"], 0)
            self.assertEqual(artifacts["all_platforms_upload_local_archive_dir"], str((exports_dir / "feishu_upload_local_archive").resolve()))
            self.assertTrue(archive_json_path.exists())
            self.assertTrue(archive_xlsx_path.exists())
            self.assertEqual(payload["source_row_count"], 2)
            self.assertEqual(payload["row_count"], 2)
            self.assertEqual(payload["skipped_row_count"], 0)
            self.assertEqual(payload["rows"][0]["达人ID"], "good_creator")
            self.assertEqual(payload["rows"][0]["Following"], 1.2)
            self.assertEqual(payload["rows"][1]["达人ID"], "bad_creator")
            self.assertEqual(payload["rows"][1]["ai是否通过"], "转人工")
            self.assertEqual(archive_payload["skipped_row_count"], 0)

    def test_metric_notes_distinguish_missing_video_views_and_missing_scrape_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "ejay.cruzz",
                        "username": "ejay.cruzz",
                        "profile_url": "https://www.instagram.com/ejay.cruzz",
                        "upload_handle": "ejay.cruzz",
                        "final_status": "Pass",
                        "final_reason": "家庭内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "farrobear",
                        "username": "farrobear",
                        "profile_url": "https://tiktok.com/@farrobear",
                        "upload_handle": "farrobear",
                        "final_status": "Reject",
                        "final_reason": "抓取返回账号不存在或不可访问",
                    }
                ]
            ).to_excel(tiktok_export, index=False)

            instagram_data_path = root / "data" / "instagram" / "instagram_data.json"
            instagram_data_path.parent.mkdir(parents=True, exist_ok=True)
            instagram_data_path.write_text(
                json.dumps(
                    [
                        {
                            "username": "ejay.cruzz",
                            "url": "https://www.instagram.com/ejay.cruzz",
                            "followersCount": 290243,
                            "latestPosts": [
                                {
                                    "type": "Sidecar",
                                    "likesCount": 1200,
                                }
                            ],
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            tiktok_data_path = root / "data" / "tiktok" / "tiktok_data.json"
            tiktok_data_path.parent.mkdir(parents=True, exist_ok=True)
            tiktok_data_path.write_text("[]", encoding="utf-8")

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            artifacts = build_all_platforms_final_review_artifacts(
                output_path=output_path,
                final_exports={
                    "instagram": {"final_review": str(instagram_export)},
                    "tiktok": {"final_review": str(tiktok_export)},
                },
                task_owner={"responsible_name": "陈俊仁"},
            )

            self.assertEqual(artifacts["source_row_count"], 2)
            self.assertEqual(artifacts["row_count"], 2)
            self.assertEqual(artifacts["skipped_row_count"], 0)
            rows = pd.read_excel(output_path).fillna("")

            instagram_row = rows.loc[rows["达人ID"] == "ejay.cruzz"].iloc[0].to_dict()
            self.assertEqual(instagram_row["# Followers(K)#"], 290.2)
            self.assertEqual(instagram_row["Median Views (K)"], "")
            self.assertEqual(instagram_row["ai是否通过"], "转人工")
            self.assertEqual(instagram_row["标签(ai)"], "")
            self.assertIn("无视频播放数据", instagram_row["ai筛号反馈理由"])
            self.assertIn("缺少标签(ai)，已自动转人工", instagram_row["ai筛号反馈理由"])
            self.assertIn("无视频播放数据", instagram_row["ai评价"])
            self.assertIn("缺少标签(ai)，已自动转人工", instagram_row["ai评价"])

            tiktok_row = rows.loc[rows["达人ID"] == "farrobear"].iloc[0].to_dict()
            self.assertEqual(tiktok_row["# Followers(K)#"], "")
            self.assertEqual(tiktok_row["Median Views (K)"], "")
            self.assertIn("无抓取数据，需人工确认", tiktok_row["ai筛号反馈理由"])
            self.assertIn("无抓取数据，需人工确认", tiktok_row["ai评价"])

    def test_final_export_falls_back_to_creator_cache_for_metrics_when_run_data_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "andrealopez",
                        "username": "andrealopez",
                        "profile_url": "https://www.instagram.com/andrealopez",
                        "upload_handle": "andrealopez",
                        "final_status": "Reject",
                        "final_reason": "数据表现一般",
                    }
                ]
            ).to_excel(instagram_export, index=False)

            cached_rows = {
                "andrealopez": [
                    {
                        "username": "andrealopez",
                        "url": "https://www.instagram.com/andrealopez",
                        "followersCount": 1376339,
                        "followsCount": 2319,
                        "latestPosts": [
                            {"videoViewCount": 242700, "likesCount": 39100},
                            {"videoViewCount": 242700, "likesCount": 39100},
                        ],
                    }
                ]
            }

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            with patch(
                "backend.final_export_merge.creator_cache.load_scrape_cache_entries",
                return_value=cached_rows,
            ):
                build_all_platforms_final_review_artifacts(
                    output_path=output_path,
                    payload_json_path=payload_path,
                    final_exports={"instagram": {"final_review": str(instagram_export)}},
                    task_owner={"responsible_name": "陈俊仁"},
                )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            row = payload["rows"][0]
            self.assertEqual(row["# Followers(K)#"], 1376.3)
            self.assertEqual(row["Following"], 2.3)
            self.assertEqual(row["Median Views (K)"], 242.7)
            self.assertEqual(row["互动率"], "16.1%")

    def test_instagram_metrics_accept_play_count_without_video_view_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            cached_rows = {
                "alpha": [
                    {
                        "username": "alpha",
                        "url": "https://www.instagram.com/alpha",
                        "followersCount": 50000,
                        "followsCount": 1000,
                        "latestPosts": [
                            {
                                "url": "https://www.instagram.com/reel/first/",
                                "productType": "clips",
                                "videoPlayCount": 1200000,
                                "likesCount": 60000,
                            },
                            {
                                "url": "https://www.instagram.com/reel/second/",
                                "productType": "clips",
                                "videoPlayCount": 1800000,
                                "likesCount": 90000,
                            },
                        ],
                    }
                ]
            }

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            with patch(
                "backend.final_export_merge.creator_cache.load_scrape_cache_entries",
                return_value=cached_rows,
            ):
                build_all_platforms_final_review_artifacts(
                    output_path=output_path,
                    payload_json_path=payload_path,
                    final_exports={
                        "instagram": {
                            "final_review": str(instagram_export),
                            "positioning_card_review": str(instagram_positioning),
                        }
                    },
                    task_owner={"responsible_name": "陈俊仁"},
                )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            row = payload["rows"][0]
            self.assertEqual(row["Median Views (K)"], 1500)
            self.assertEqual(row["互动率"], "5.0%")

    def test_engagement_rate_falls_back_to_likes_over_views_from_upload_fields(self) -> None:
        self.assertEqual(
            _compute_engagement_rate(
                {
                    "upload_avg_likes": 100,
                    "upload_avg_views": 1000,
                }
            ),
            "10.0%",
        )

    def test_instagram_engagement_rate_uses_average_of_per_post_rates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            cached_rows = {
                "alpha": [
                    {
                        "username": "alpha",
                        "url": "https://www.instagram.com/alpha",
                        "followersCount": 50000,
                        "followsCount": 1000,
                        "latestPosts": [
                            {"videoViewCount": 100, "likesCount": 50},
                            {"videoViewCount": 10000, "likesCount": 1000},
                        ],
                    }
                ]
            }

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            with patch(
                "backend.final_export_merge.creator_cache.load_scrape_cache_entries",
                return_value=cached_rows,
            ):
                build_all_platforms_final_review_artifacts(
                    output_path=output_path,
                    payload_json_path=payload_path,
                    final_exports={
                        "instagram": {
                            "final_review": str(instagram_export),
                            "positioning_card_review": str(instagram_positioning),
                        }
                    },
                    task_owner={"responsible_name": "陈俊仁"},
                )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            row = payload["rows"][0]
            self.assertEqual(row["互动率"], "30.0%")

    def test_tiktok_engagement_rate_uses_average_of_per_post_rates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.tiktok.com/@alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            cached_rows = {
                "alpha": [
                    {
                        "authorMeta": {
                            "name": "alpha",
                            "profileUrl": "https://www.tiktok.com/@alpha",
                            "fans": 80000,
                            "following": 500,
                        },
                        "playCount": 100,
                        "diggCount": 50,
                    },
                    {
                        "authorMeta": {
                            "name": "alpha",
                            "profileUrl": "https://www.tiktok.com/@alpha",
                            "fans": 80000,
                            "following": 500,
                        },
                        "playCount": 10000,
                        "diggCount": 1000,
                    },
                ]
            }

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            with patch(
                "backend.final_export_merge.creator_cache.load_scrape_cache_entries",
                return_value=cached_rows,
            ):
                build_all_platforms_final_review_artifacts(
                    output_path=output_path,
                    payload_json_path=payload_path,
                    final_exports={
                        "tiktok": {
                            "final_review": str(tiktok_export),
                            "positioning_card_review": str(tiktok_positioning),
                        }
                    },
                    task_owner={"responsible_name": "陈俊仁"},
                )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            row = payload["rows"][0]
            self.assertEqual(row["互动率"], "30.0%")

    def test_instagram_metrics_prefer_reels_over_regular_video_posts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "final_status": "Pass",
                        "final_reason": "内容契合",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "alpha",
                        "username": "alpha",
                        "profile_url": "https://www.instagram.com/alpha",
                        "upload_handle": "alpha",
                        "positioning_stage_status": "Completed",
                        "positioning_labels": "家庭用品和家电-家庭博主",
                        "fit_summary": "内容契合",
                    }
                ]
            ).to_excel(instagram_positioning, index=False)

            cached_rows = {
                "alpha": [
                    {
                        "username": "alpha",
                        "url": "https://www.instagram.com/alpha",
                        "followersCount": 50000,
                        "followsCount": 1000,
                        "latestPosts": [
                            {
                                "url": "https://www.instagram.com/p/feed-video/",
                                "type": "Video",
                                "videoViewCount": 66000,
                                "likesCount": 6000,
                            },
                            {
                                "url": "https://www.instagram.com/reel/high-performing-reel/",
                                "productType": "clips",
                                "videoViewCount": 64000,
                                "videoPlayCount": 2400000,
                                "likesCount": 120000,
                            },
                            {
                                "url": "https://www.instagram.com/reel/second-high-performing-reel/",
                                "productType": "clips",
                                "videoViewCount": 71000,
                                "videoPlayCount": 2600000,
                                "likesCount": 130000,
                            },
                        ],
                    }
                ]
            }

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            with patch(
                "backend.final_export_merge.creator_cache.load_scrape_cache_entries",
                return_value=cached_rows,
            ):
                build_all_platforms_final_review_artifacts(
                    output_path=output_path,
                    payload_json_path=payload_path,
                    final_exports={
                        "instagram": {
                            "final_review": str(instagram_export),
                            "positioning_card_review": str(instagram_positioning),
                        }
                    },
                    task_owner={"responsible_name": "陈俊仁"},
                )

            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            row = payload["rows"][0]
            self.assertEqual(row["Median Views (K)"], 2500.0)
            self.assertEqual(row["互动率"], "5.0%")

    def test_processing_failures_and_positioning_errors_are_explicit_in_combined_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            instagram_export = exports_dir / "instagram" / "instagram_final_review.xlsx"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            instagram_positioning = exports_dir / "instagram" / "instagram_positioning_card_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            instagram_export.parent.mkdir(parents=True, exist_ok=True)
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "cmpmelody",
                        "username": "cmpmelody",
                        "profile_url": "https://www.instagram.com/cmpmelody",
                        "upload_handle": "cmpmelody",
                        "final_status": "Error",
                        "final_reason": "视觉复核超时：cmpmelody 超过 120 秒未完成",
                    }
                ]
            ).to_excel(instagram_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "aaroncarters",
                        "username": "aaroncarters",
                        "profile_url": "https://www.tiktok.com/@aaroncarters",
                        "upload_handle": "aaroncarters",
                        "runtime_avg_views": 3891028.9,
                        "final_status": "Pass",
                        "final_reason": "达人展示了户外场景、产品开箱及穿搭展示，符合内容合作标准。",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame([]).to_excel(instagram_positioning, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "aaroncarters",
                        "username": "aaroncarters",
                        "profile_url": "https://www.tiktok.com/@aaroncarters",
                        "upload_handle": "aaroncarters",
                        "positioning_stage_status": "Error",
                        "positioning_labels": "",
                        "fit_summary": "",
                        "positioning_error": "reelx: HTTP 401 认证失败；额度已用尽",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            instagram_data_path = root / "data" / "instagram" / "instagram_data.json"
            instagram_data_path.parent.mkdir(parents=True, exist_ok=True)
            instagram_data_path.write_text(
                json.dumps(
                    [
                        {
                            "username": "cmpmelody",
                            "url": "https://www.instagram.com/cmpmelody",
                            "followersCount": 129300,
                            "latestPosts": [{"videoViewCount": 25300, "likesCount": 21900}],
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            tiktok_data_path = root / "data" / "tiktok" / "tiktok_data.json"
            tiktok_data_path.parent.mkdir(parents=True, exist_ok=True)
            tiktok_data_path.write_text(
                json.dumps(
                    [
                        {
                            "authorMeta": {
                                "name": "aaroncarters",
                                "profileUrl": "https://www.tiktok.com/@aaroncarters",
                                "fans": 253400,
                            },
                            "playCount": 3891029,
                            "diggCount": 24100,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                final_exports={
                    "instagram": {
                        "final_review": str(instagram_export),
                        "positioning_card_review": str(instagram_positioning),
                    },
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(tiktok_positioning),
                    },
                },
                task_owner={"responsible_name": "陈俊仁"},
            )

            rows = pd.read_excel(output_path).fillna("")

            instagram_row = rows.loc[rows["达人ID"] == "cmpmelody"].iloc[0].to_dict()
            self.assertEqual(instagram_row["ai是否通过"], "转人工")
            self.assertIn("视觉复核超时", instagram_row["ai筛号反馈理由"])

            tiktok_row = rows.loc[rows["达人ID"] == "aaroncarters"].iloc[0].to_dict()
            self.assertEqual(tiktok_row["ai是否通过"], "处理失败")
            self.assertEqual(tiktok_row["标签(ai)"], "定位卡处理失败")
            self.assertIn("定位卡处理失败，需人工确认", tiktok_row["ai筛号反馈理由"])
            self.assertIn("定位卡处理失败，需人工确认", tiktok_row["ai评价"])

    def test_metric_reject_with_positioning_not_reviewed_stays_negative_not_manual(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "lowviews_creator",
                        "username": "lowviews_creator",
                        "profile_url": "https://www.tiktok.com/@lowviews_creator",
                        "upload_handle": "lowviews_creator",
                        "final_status": "Reject",
                        "final_reason": "播放量不达标（均值 2557/10000，中位数 2100/10000）",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "lowviews_creator",
                        "username": "lowviews_creator",
                        "profile_url": "https://www.tiktok.com/@lowviews_creator",
                        "upload_handle": "lowviews_creator",
                        "positioning_stage_status": "Not Reviewed",
                        "positioning_labels": "",
                        "fit_summary": "",
                        "positioning_error": "",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            tiktok_data_path = root / "data" / "tiktok" / "tiktok_data.json"
            tiktok_data_path.parent.mkdir(parents=True, exist_ok=True)
            tiktok_data_path.write_text("[]", encoding="utf-8")

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(tiktok_positioning),
                    },
                },
                task_owner={"responsible_name": "陈俊仁"},
            )

            rows = pd.read_excel(output_path).fillna("")
            tiktok_row = rows.loc[rows["达人ID"] == "lowviews_creator"].iloc[0].to_dict()
            self.assertEqual(tiktok_row["ai是否通过"], "否")
            self.assertIn("播放量不达标", tiktok_row["ai筛号反馈理由"])
            self.assertIn("定位卡未完成，需人工确认", tiktok_row["ai筛号反馈理由"])

    def test_visual_provider_timeout_is_sanitized_to_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            tiktok_export = exports_dir / "tiktok" / "tiktok_final_review.xlsx"
            tiktok_positioning = exports_dir / "tiktok" / "tiktok_positioning_card_review.xlsx"
            tiktok_export.parent.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                [
                    {
                        "identifier": "timeout_creator",
                        "username": "timeout_creator",
                        "profile_url": "https://www.tiktok.com/@timeout_creator",
                        "upload_handle": "timeout_creator",
                        "final_status": "Error",
                        "final_reason": "quan2go: HTTPSConnectionPool(host='capi.quan2go.com', port=443): Read timed out. (read timeout=30)",
                        "visual_reason": "quan2go: HTTPSConnectionPool(host='capi.quan2go.com', port=443): Read timed out. (read timeout=30)",
                    }
                ]
            ).to_excel(tiktok_export, index=False)
            pd.DataFrame(
                [
                    {
                        "identifier": "timeout_creator",
                        "username": "timeout_creator",
                        "profile_url": "https://www.tiktok.com/@timeout_creator",
                        "upload_handle": "timeout_creator",
                        "positioning_stage_status": "Not Reviewed",
                        "positioning_labels": "",
                        "fit_summary": "",
                        "positioning_error": "",
                    }
                ]
            ).to_excel(tiktok_positioning, index=False)

            tiktok_data_path = root / "data" / "tiktok" / "tiktok_data.json"
            tiktok_data_path.parent.mkdir(parents=True, exist_ok=True)
            tiktok_data_path.write_text("[]", encoding="utf-8")

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                final_exports={
                    "tiktok": {
                        "final_review": str(tiktok_export),
                        "positioning_card_review": str(tiktok_positioning),
                    },
                },
                task_owner={"responsible_name": "陈俊仁"},
            )

            rows = pd.read_excel(output_path).fillna("")
            tiktok_row = rows.loc[rows["达人ID"] == "timeout_creator"].iloc[0].to_dict()
            self.assertEqual(tiktok_row["ai是否通过"], "转人工")
            self.assertEqual(tiktok_row["ai筛号反馈理由"], "视觉复核异常，需人工确认")
            self.assertNotIn("quan2go", tiktok_row["ai筛号反馈理由"])
            self.assertNotIn("定位卡未完成", tiktok_row["ai筛号反馈理由"])

    def test_manual_review_rows_are_emitted_when_all_platforms_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            exports_dir = root / "exports"
            keep_workbook = root / "upstream" / "exports" / "keep.xlsx"
            keep_workbook.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "Platform": "TikTok",
                        "@username": "beta",
                        "URL": "https://www.tiktok.com/@beta",
                        "last_mail_time": "2026-04-03T05:27:00+08:00",
                        "last_mail_snippet": "Please share more details.",
                    }
                ]
            ).to_excel(keep_workbook, index=False)

            output_path = exports_dir / "all_platforms_final_review.xlsx"
            payload_path = exports_dir / "all_platforms_final_review_payload.json"
            build_all_platforms_final_review_artifacts(
                output_path=output_path,
                payload_json_path=payload_path,
                final_exports={},
                keep_workbook=keep_workbook,
                manual_review_rows=[
                    {
                        "identifier": "beta",
                        "platform": "youtube",
                        "profile_url": "https://www.youtube.com/@beta",
                        "reason": "TikTok / Instagram / YouTube 均未抓取到有效资料，需人工确认。",
                    }
                ],
                task_owner={"responsible_name": "陈俊仁"},
            )

            workbook = pd.read_excel(output_path).fillna("")
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(workbook.loc[0, "达人ID"], "beta")
            self.assertEqual(workbook.loc[0, "平台"], "youtube")
            self.assertEqual(workbook.loc[0, "ai是否通过"], "转人工")
            self.assertIn("需人工确认", workbook.loc[0, "ai筛号反馈理由"])
            self.assertEqual(payload["rows"][0]["ai是否通过"], "转人工")


if __name__ == "__main__":
    unittest.main()
