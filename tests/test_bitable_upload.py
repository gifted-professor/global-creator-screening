from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from feishu_screening_bridge.bitable_export import ResolvedBitableView
from feishu_screening_bridge.bitable_upload import upload_final_review_payload_to_bitable
from feishu_screening_bridge.task_upload_sync import TaskUploadEntry


class _FakeBitableUploadClient:
    def __init__(self) -> None:
        self.created_records: list[dict[str, object]] = []
        self.updated_records: list[dict[str, object]] = []
        self.search_items = [
            {
                "record_id": "rec_existing",
                "fields": {
                    "达人ID": [{"text": "beta", "type": "text"}],
                    "平台": [{"text": "tiktok", "type": "text"}],
                },
            }
        ]

    def get_api_json(self, url_path: str, *, headers: dict[str, str] | None = None) -> dict[str, object]:
        if url_path.endswith("/fields"):
            return {
                "data": {
                    "items": [
                        {"field_id": "fld1", "field_name": "达人ID", "type": 1, "property": None},
                        {"field_id": "fld2", "field_name": "平台", "type": 1, "property": None},
                        {"field_id": "fld3", "field_name": "主页链接", "type": 15, "property": None},
                        {"field_id": "fld4", "field_name": "Followers(K)", "type": 2, "property": {"formatter": "0"}},
                        {"field_id": "fld5", "field_name": "Average Views (K)", "type": 2, "property": {"formatter": "0"}},
                        {"field_id": "fld6", "field_name": "互动率", "type": 1, "property": None},
                        {"field_id": "fld7", "field_name": "当前网红报价", "type": 1, "property": None},
                        {"field_id": "fld8", "field_name": "达人最后一次回复邮件时间", "type": 5, "property": {"date_formatter": "yyyy/MM/dd"}},
                        {"field_id": "fld9", "field_name": "达人回复的最后一封邮件内容", "type": 1, "property": None},
                        {"field_id": "fld10", "field_name": "达人对接人", "type": 11, "property": {"multiple": False}},
                        {
                            "field_id": "fld11",
                            "field_name": "ai 是否通过",
                            "type": 3,
                            "property": {"options": [{"name": "是"}, {"name": "否"}, {"name": "转人工"}]},
                        },
                        {"field_id": "fld12", "field_name": "ai筛号反馈理由", "type": 1, "property": None},
                        {
                            "field_id": "fld13",
                            "field_name": "标签（ai）",
                            "type": 4,
                            "property": {"options": [{"name": "母婴用品-家庭/宝妈"}, {"name": "家庭用品和家电-家庭博主"}]},
                        },
                        {"field_id": "fld14", "field_name": "ai 评价", "type": 1, "property": None},
                    ]
                }
            }
        raise AssertionError(f"unexpected GET {url_path}")

    def post_api_json(
        self,
        url_path: str,
        *,
        body: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        if url_path.endswith("/records/search"):
            return {"data": {"items": self.search_items, "has_more": False}}
        if url_path.endswith("/records"):
            fields = dict((body or {}).get("fields") or {})
            record_id = f"rec_{len(self.created_records) + 1}"
            self.created_records.append({"record_id": record_id, "fields": fields})
            return {"data": {"record": {"record_id": record_id, "fields": fields}}}
        raise AssertionError(f"unexpected POST {url_path}")

    def put_api_json(
        self,
        url_path: str,
        *,
        body: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        if "/records/" not in url_path:
            raise AssertionError(f"unexpected PUT {url_path}")
        fields = dict((body or {}).get("fields") or {})
        record_id = str(url_path.rsplit("/", 1)[-1] or "").strip()
        self.updated_records.append({"record_id": record_id, "fields": fields})
        return {"data": {"record": {"record_id": record_id, "fields": fields}}}


class BitableUploadTests(unittest.TestCase):
    def test_upload_payload_maps_fields_and_updates_existing_rows(self) -> None:
        client = _FakeBitableUploadClient()
        resolved_view = ResolvedBitableView(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            source_kind="base",
            source_token="app_token",
            app_token="app_token",
            table_id="tbl",
            view_id="vew",
            table_name="达人管理",
            view_name="总视图",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload_path = root / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "task_owner": {
                            "linked_bitable_url": "https://stale.example.com",
                            "task_name": "MINISO",
                        },
                        "row_count": 2,
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "# Followers(K)#": 123.4,
                                "Average Views (K)": 56.7,
                                "互动率": "12.3%",
                                "当前网红报价": "$100",
                                "达人最后一次回复邮件时间": "2026/03/30",
                                "达人回复的最后一封邮件内容": "hi",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "ai筛号反馈理由": "ok",
                                "标签(ai)": "家庭用品和家电-家庭博主；美食",
                                "ai评价": "nice",
                                "达人对接人_employee_id": "ou_alpha",
                            },
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "否",
                            },
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

            self.assertEqual(result["to_create_count"], 1)
            self.assertEqual(result["to_update_count"], 1)
            self.assertEqual(result["created_count"], 1)
            self.assertEqual(result["updated_count"], 0)
            self.assertEqual(result["skipped_existing_count"], 1)
            self.assertEqual(result["failed_count"], 0)
            self.assertTrue(Path(result["result_json_path"]).exists())
            self.assertTrue(Path(result["result_xlsx_path"]).exists())

            created_fields = client.created_records[0]["fields"]
            self.assertEqual(created_fields["达人ID"], "alpha")
            self.assertEqual(created_fields["平台"], "instagram")
            self.assertEqual(created_fields["Followers(K)"], 123.4)
            self.assertEqual(created_fields["Average Views (K)"], 56.7)
            self.assertEqual(created_fields["ai 是否通过"], "是")
            self.assertEqual(created_fields["标签（ai）"], ["家庭用品和家电-家庭博主"])
            self.assertEqual(
                created_fields["主页链接"],
                {
                    "link": "https://www.instagram.com/alpha",
                    "text": "https://www.instagram.com/alpha",
                    "type": "url",
                },
            )
            self.assertEqual(created_fields["达人对接人"], [{"id": "ou_alpha"}])
            self.assertIsInstance(created_fields["达人最后一次回复邮件时间"], int)
            self.assertEqual(client.updated_records, [])

    def test_upload_updates_only_mail_fields_for_existing_rows(self) -> None:
        client = _FakeBitableUploadClient()
        resolved_view = ResolvedBitableView(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            source_kind="base",
            source_token="app_token",
            app_token="app_token",
            table_id="tbl",
            view_id="vew",
            table_name="达人管理",
            view_name="总视图",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            payload_path = Path(tmpdir) / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "# Followers(K)#": 999.9,
                                "互动率": "18.5%",
                                "当前网红报价": "$500",
                                "达人最后一次回复邮件时间": "2026/04/03",
                                "达人回复的最后一封邮件内容": "latest reply",
                                "达人对接人_employee_id": "ou_beta",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

            self.assertEqual(result["created_count"], 0)
            self.assertEqual(result["updated_count"], 1)
            self.assertEqual(result["failed_count"], 0)
            self.assertEqual(client.created_records, [])
            self.assertEqual(len(client.updated_records), 1)
            updated_fields = client.updated_records[0]["fields"]
            self.assertEqual(
                set(updated_fields.keys()),
                {"当前网红报价", "达人最后一次回复邮件时间", "达人回复的最后一封邮件内容"},
            )
            self.assertEqual(updated_fields["当前网红报价"], "$500")
            self.assertEqual(updated_fields["达人回复的最后一封邮件内容"], "latest reply")
            self.assertIsInstance(updated_fields["达人最后一次回复邮件时间"], int)

    def test_upload_prefers_task_upload_resolved_target_over_payload_link(self) -> None:
        client = _FakeBitableUploadClient()
        resolved_view = ResolvedBitableView(
            source_url="https://correct.example.com/base/app?table=tbl&view=vew",
            source_kind="base",
            source_token="app_token",
            app_token="app_token",
            table_id="tbl",
            view_id="vew",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            payload_path = Path(tmpdir) / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "task_owner": {
                            "linked_bitable_url": "https://stale.example.com/base/old?table=old&view=old",
                            "task_name": "MINISO",
                        },
                        "rows": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            fake_entry = TaskUploadEntry(
                record_id="rec_task",
                task_name="MINISO",
                employee_id="ou_alpha",
                owner_name="陈俊仁",
                owner_email="chenjunren@amagency.biz",
                owner_email_candidates=("chenjunren@amagency.biz",),
                responsible_name="陈俊仁",
                linked_bitable_url="https://correct.example.com/base/app?table=tbl&view=vew",
                workbook_file_token="box1",
                workbook_file_name="template.xlsx",
                sending_list_file_token="box2",
                sending_list_file_name="sending.xlsx",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_task_upload_entry",
                return_value=fake_entry,
            ), patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    task_name="MINISO",
                    task_upload_url="https://task-upload.example.com",
                    dry_run=True,
                )

            self.assertEqual(result["target_url_source"], "task_upload_entry")
            self.assertEqual(result["target_url"], "https://correct.example.com/base/app?table=tbl&view=vew")
