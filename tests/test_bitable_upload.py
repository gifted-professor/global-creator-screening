from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from urllib import parse
from zoneinfo import ZoneInfo

from feishu_screening_bridge.bitable_export import ResolvedBitableView
from feishu_screening_bridge.bitable_upload import fetch_existing_bitable_record_analysis, upload_final_review_payload_to_bitable
from feishu_screening_bridge.feishu_api import FeishuApiError
from feishu_screening_bridge.task_upload_sync import TaskUploadEntry


class _FakeBitableUploadClient:
    def __init__(self) -> None:
        self.created_records: list[dict[str, object]] = []
        self.updated_records: list[dict[str, object]] = []
        self.uploaded_local_files: list[dict[str, str]] = []
        self.deleted_records: list[str] = []
        self.record_create_attempt_count = 0
        self.record_update_attempt_count = 0
        self.search_attempt_count = 0
        self.create_record_side_effects: list[object] = []
        self.update_record_side_effects: list[object] = []
        self.search_side_effects: list[object] = []
        self.upload_side_effects: list[object] = []
        self.include_task_name_field = False
        self.record_pages: list[dict[str, object]] | None = None
        self.search_items = [
            {
                "record_id": "rec_existing",
                "fields": {
                    "达人ID": "beta",
                    "平台": "tiktok",
                    "达人对接人": [{"id": "ou_beta", "name": "陈俊仁"}],
                    "当前网红报价": "$50",
                    "ai 是否通过": "是",
                },
            },
            {
                "record_id": "rec_unscreened",
                "fields": {
                    "达人ID": "gamma",
                    "平台": "instagram",
                    "达人对接人": [{"id": "ou_gamma", "name": "陈俊仁"}],
                    "ai 是否通过": "",
                },
            }
        ]

    def get_api_json(self, url_path: str, *, headers: dict[str, str] | None = None) -> dict[str, object]:
        if url_path == "/bitable/v1/apps/app_token/tables":
            return {
                "data": {
                    "items": [
                        {"table_id": "tbl_ai", "name": "AI回信管理"},
                        {"table_id": "tbl_creator", "name": "达人管理"},
                    ]
                }
            }
        if url_path == "/bitable/v1/apps/app_token/tables/tbl_ai/views":
            return {"data": {"items": [{"view_id": "vew_ai", "view_name": "表格"}]}}
        if url_path == "/bitable/v1/apps/app_token/tables/tbl/views":
            return {"data": {"items": [{"view_id": "vew", "view_name": "总视图"}]}}
        if "/records?" in url_path:
            parsed = parse.urlparse(url_path)
            query = parse.parse_qs(parsed.query)
            page_token = str(query.get("page_token", [""])[0] or "")
            if self.record_pages is not None:
                if not page_token:
                    page_index = 0
                else:
                    try:
                        page_index = int(page_token.replace("page-", ""))
                    except ValueError as exc:  # pragma: no cover - defensive only
                        raise AssertionError(f"unexpected page token {page_token}") from exc
                if page_index >= len(self.record_pages):
                    raise AssertionError(f"unexpected page index {page_index}")
                return {"data": dict(self.record_pages[page_index])}
            return {"data": {"items": self.search_items, "has_more": False, "page_token": ""}}
        if url_path.endswith("/fields"):
            items = [
                {"field_id": "fld1", "field_name": "达人ID", "type": 1, "property": None},
                {"field_id": "fld2", "field_name": "平台", "type": 1, "property": None},
                {"field_id": "fld3", "field_name": "主页链接", "type": 15, "property": None},
                {"field_id": "fld4", "field_name": "Followers(K)", "type": 2, "property": {"formatter": "0"}},
                {"field_id": "fld5", "field_name": "Following", "type": 2, "property": {"formatter": "0"}},
                {"field_id": "fld6", "field_name": "Median Views (K)", "type": 2, "property": {"formatter": "0"}},
                {"field_id": "fld7", "field_name": "互动率", "type": 1, "property": None},
                {"field_id": "fld8", "field_name": "当前网红报价", "type": 1, "property": None},
                {"field_id": "fld9", "field_name": "达人最后一次回复邮件时间", "type": 5, "property": {"date_formatter": "yyyy/MM/dd"}},
                {"field_id": "fld10", "field_name": "full body", "type": 1, "property": None},
                {"field_id": "fld11", "field_name": "达人对接人", "type": 11, "property": {"multiple": False}},
                {
                    "field_id": "fld12",
                    "field_name": "ai 是否通过",
                    "type": 3,
                    "property": {"options": [{"name": "是"}, {"name": "否"}, {"name": "转人工"}]},
                },
                {"field_id": "fld13", "field_name": "ai筛号反馈理由", "type": 1, "property": None},
                {
                    "field_id": "fld14",
                    "field_name": "标签（ai）",
                    "type": 4,
                    "property": {"options": [{"name": "母婴用品-家庭/宝妈"}, {"name": "家庭用品和家电-家庭博主"}]},
                },
                {"field_id": "fld15", "field_name": "ai 评价", "type": 1, "property": None},
                {"field_id": "fld16", "field_name": "文本 12", "type": 17, "property": None},
            ]
            if self.include_task_name_field:
                items.insert(0, {"field_id": "fld0", "field_name": "任务名", "type": 1, "property": None})
            return {"data": {"items": items}}
        raise AssertionError(f"unexpected GET {url_path}")

    def post_api_json(
        self,
        url_path: str,
        *,
        body: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        if url_path.endswith("/records/search"):
            self.search_attempt_count += 1
            if self.search_side_effects:
                effect = self.search_side_effects.pop(0)
                if isinstance(effect, Exception):
                    raise effect
            return {"data": {"items": self.search_items, "has_more": False}}
        if url_path.endswith("/records"):
            self.record_create_attempt_count += 1
            if self.create_record_side_effects:
                effect = self.create_record_side_effects.pop(0)
                if isinstance(effect, Exception):
                    raise effect
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
        self.record_update_attempt_count += 1
        if self.update_record_side_effects:
            effect = self.update_record_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        record_id = url_path.rsplit("/", 1)[-1]
        fields = dict((body or {}).get("fields") or {})
        self.updated_records.append({"record_id": record_id, "fields": fields})
        return {"data": {"record": {"record_id": record_id, "fields": fields}}}

    def upload_local_file(self, local_path: str | Path, *, parent_type: str = "bitable_file", parent_node: str = "", file_name: str | None = None):  # type: ignore[override]
        from feishu_screening_bridge.feishu_api import UploadedFeishuFile

        path = Path(str(local_path))
        if self.upload_side_effects:
            effect = self.upload_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        self.uploaded_local_files.append(
            {
                "local_path": str(path),
                "parent_type": str(parent_type),
                "parent_node": str(parent_node),
                "file_name": str(file_name or path.name),
            }
        )
        index = len(self.uploaded_local_files)
        return UploadedFeishuFile(
            file_token=f"boxcn-upload-{index}",
            file_name=str(file_name or path.name),
            size_bytes=len(path.read_bytes()),
            source_url=f"https://unit-test.feishu.mock/upload/{index}",
        )

    def delete_api_json(
        self,
        url_path: str,
        *,
        body: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        parsed = parse.urlparse(url_path)
        self.deleted_records.append(parsed.path.rsplit("/", 1)[-1])
        return {"data": {}}


class BitableUploadTests(unittest.TestCase):
    def test_fetch_existing_record_analysis_supports_multi_page_records_listing(self) -> None:
        client = _FakeBitableUploadClient()
        client.record_pages = [
            {
                "items": [
                    {
                        "record_id": "rec_page1",
                        "fields": {
                            "达人ID": "alpha",
                            "平台": "instagram",
                            "ai 是否通过": "是",
                        },
                    }
                ],
                "has_more": True,
                "page_token": "page-1",
            },
            {
                "items": [
                    {
                        "record_id": "rec_page2",
                        "fields": {
                            "达人ID": "beta",
                            "平台": "tiktok",
                            "ai 是否通过": "",
                        },
                    }
                ],
                "has_more": False,
                "page_token": "",
            },
        ]
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

        with patch(
            "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
            return_value=resolved_view,
        ):
            _resolved, analysis = fetch_existing_bitable_record_analysis(
                client,
                linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
            )

        self.assertEqual(len(analysis.index), 2)
        self.assertEqual(analysis.index["alpha::instagram"]["record_id"], "rec_page1")
        self.assertEqual(analysis.index["beta::tiktok"]["record_id"], "rec_page2")

    def test_upload_payload_maps_fields_and_skips_existing_rows(self) -> None:
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
            raw_mail_path = root / "alpha-last.eml"
            raw_mail_path.write_text("Subject: hello\n\nbody", encoding="utf-8")
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
                                "Following": 321.9,
                                "Median Views (K)": 56.7,
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
                                "__feishu_attachment_local_paths": [str(raw_mail_path)],
                            },
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_beta",
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

            self.assertEqual(result["created_count"], 1)
            self.assertEqual(result["skipped_existing_count"], 1)
            self.assertEqual(result["failed_count"], 0)
            self.assertTrue(Path(result["result_json_path"]).exists())
            self.assertTrue(Path(result["result_xlsx_path"]).exists())

            created_fields = client.created_records[0]["fields"]
            self.assertEqual(created_fields["达人ID"], "alpha")
            self.assertEqual(created_fields["平台"], "instagram")
            self.assertEqual(created_fields["Followers(K)"], 123.4)
            self.assertEqual(created_fields["Following"], 321.9)
            self.assertEqual(created_fields["Median Views (K)"], 56.7)
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
            self.assertNotIn("达人对接人", created_fields)
            self.assertIsInstance(created_fields["达人最后一次回复邮件时间"], int)
            self.assertEqual(created_fields["full body"], "hi")
            self.assertEqual(created_fields["文本 12"], [{"file_token": "boxcn-upload-1", "name": "alpha-last.eml"}])
            self.assertEqual(client.uploaded_local_files[0]["local_path"], str(raw_mail_path))
            self.assertEqual(client.uploaded_local_files[0]["parent_type"], "bitable_file")
            self.assertEqual(client.uploaded_local_files[0]["parent_node"], "app_token")

    def test_upload_payload_normalizes_reply_date_to_shanghai_day_before_feishu_write(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
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
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_alpha",
                                "达人最后一次回复邮件时间": "2026-04-02T15:26:24-06:00",
                            }
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

            self.assertEqual(result["created_count"], 1)
            created_fields = client.created_records[0]["fields"]
            expected = int(datetime(2026, 4, 3, 0, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
            self.assertEqual(created_fields["达人最后一次回复邮件时间"], expected)

    def test_upload_payload_updates_existing_records_for_create_or_update_and_mail_only_modes(self) -> None:
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
            raw_mail_path = root / "beta-last.eml"
            raw_mail_path.write_text("Subject: update\n\nbody", encoding="utf-8")
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "当前网红报价": "$200",
                                "达人最后一次回复邮件时间": "2026/03/31",
                                "达人回复的最后一封邮件内容": "latest follow-up",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_beta",
                                "__feishu_attachment_local_paths": [str(raw_mail_path)],
                                "__feishu_update_mode": "mail_only_update",
                            },
                            {
                                "达人ID": "gamma",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/gamma",
                                "# Followers(K)#": 300,
                                "Median Views (K)": 40,
                                "互动率": "9.1%",
                                "当前网红报价": "$300",
                                "达人最后一次回复邮件时间": "2026/03/31",
                                "达人回复的最后一封邮件内容": "gamma reply",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "ai筛号反馈理由": "ok",
                                "标签(ai)": "母婴用品-家庭/宝妈",
                                "ai评价": "good",
                                "达人对接人_employee_id": "ou_gamma",
                                "__feishu_update_mode": "create_or_update",
                            },
                            {
                                "达人ID": "delta",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/delta",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "否",
                                "__feishu_update_mode": "create_or_update",
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

        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(len(client.updated_records), 2)
        beta_update = next(item for item in client.updated_records if item["record_id"] == "rec_existing")
        gamma_update = next(item for item in client.updated_records if item["record_id"] == "rec_unscreened")
        self.assertEqual(
            set(beta_update["fields"].keys()),
            {"主页链接", "当前网红报价", "达人最后一次回复邮件时间", "full body", "文本 12"},
        )
        self.assertEqual(beta_update["fields"]["当前网红报价"], "$200")
        self.assertEqual(beta_update["fields"]["主页链接"], {"link": "https://www.tiktok.com/@beta", "text": "https://www.tiktok.com/@beta", "type": "url"})
        self.assertEqual(beta_update["fields"]["full body"], "latest follow-up")
        self.assertEqual(beta_update["fields"]["文本 12"], [{"file_token": "boxcn-upload-1", "name": "beta-last.eml"}])
        self.assertEqual(gamma_update["fields"]["达人ID"], "gamma")
        self.assertEqual(gamma_update["fields"]["ai 是否通过"], "是")
        self.assertEqual(client.created_records[0]["fields"]["达人ID"], "delta")

    def test_upload_payload_create_or_mail_only_mode_updates_existing_but_creates_new_rows(self) -> None:
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
            raw_mail_path = root / "beta-last.eml"
            raw_mail_path.write_text("Subject: update\n\nbody", encoding="utf-8")
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "# Followers(K)#": 888,
                                "当前网红报价": "$200",
                                "达人最后一次回复邮件时间": "2026/03/31",
                                "达人回复的最后一封邮件内容": "latest follow-up",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_beta",
                                "__feishu_attachment_local_paths": [str(raw_mail_path)],
                                "__feishu_update_mode": "create_or_mail_only_update",
                            },
                            {
                                "达人ID": "epsilon",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/epsilon",
                                "# Followers(K)#": 301,
                                "Median Views (K)": 42,
                                "互动率": "8.5%",
                                "当前网红报价": "$350",
                                "达人最后一次回复邮件时间": "2026/03/30",
                                "达人回复的最后一封邮件内容": "epsilon reply",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_epsilon",
                                "ai是否通过": "是",
                                "ai筛号反馈理由": "ok",
                                "标签(ai)": "母婴用品-家庭/宝妈",
                                "ai评价": "good",
                                "__feishu_update_mode": "create_or_mail_only_update",
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

        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["updated_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        beta_update = client.updated_records[0]
        self.assertEqual(beta_update["record_id"], "rec_existing")
        self.assertEqual(
            set(beta_update["fields"].keys()),
            {"主页链接", "当前网红报价", "达人最后一次回复邮件时间", "full body", "文本 12"},
        )
        self.assertEqual(beta_update["fields"]["主页链接"], {"link": "https://www.tiktok.com/@beta", "text": "https://www.tiktok.com/@beta", "type": "url"})
        self.assertNotIn("Followers(K)", beta_update["fields"])
        created_fields = client.created_records[0]["fields"]
        self.assertEqual(created_fields["达人ID"], "epsilon")
        self.assertEqual(created_fields["ai 是否通过"], "是")
        self.assertEqual(created_fields["Followers(K)"], 301)

    def test_upload_payload_manual_pool_rows_match_existing_records_by_mail_fingerprint(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = [
            {
                "record_id": "rec_manual_1",
                "fields": {
                    "达人ID": "MINISO4/8转人工1",
                    "平台": "转人工",
                    "达人最后一次回复邮件时间": "2026/04/08",
                    "full body": "body a",
                },
            },
            {
                "record_id": "rec_manual_2",
                "fields": {
                    "达人ID": "MINISO4/8转人工2",
                    "平台": "转人工",
                    "达人最后一次回复邮件时间": "2026/04/08",
                    "full body": "body b",
                },
            },
        ]
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
                        "rows": [
                            {
                                "达人ID": "MINISO4/8转人工1",
                                "平台": "转人工",
                                "达人最后一次回复邮件时间": "2026/04/08",
                                "full body": "body b",
                                "当前网红报价": "$200",
                                "__feishu_update_mode": "create_or_mail_only_update",
                            },
                            {
                                "达人ID": "MINISO4/8转人工2",
                                "平台": "转人工",
                                "达人最后一次回复邮件时间": "2026/04/08",
                                "full body": "body a",
                                "当前网红报价": "$100",
                                "__feishu_update_mode": "create_or_mail_only_update",
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

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["updated_count"], 2)
        update_by_record_id = {item["record_id"]: item["fields"] for item in client.updated_records}
        self.assertEqual(update_by_record_id["rec_manual_1"]["full body"], "body a")
        self.assertEqual(update_by_record_id["rec_manual_2"]["full body"], "body b")

    def test_upload_payload_manual_pool_rows_reassign_colliding_synthetic_ids_before_create(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = [
            {
                "record_id": "rec_manual_1",
                "fields": {
                    "达人ID": "MINISO4/8转人工1",
                    "平台": "转人工",
                    "达人最后一次回复邮件时间": "2026/04/08",
                    "full body": "body a",
                },
            },
            {
                "record_id": "rec_manual_2",
                "fields": {
                    "达人ID": "MINISO4/8转人工2",
                    "平台": "转人工",
                    "达人最后一次回复邮件时间": "2026/04/08",
                    "full body": "body b",
                },
            },
        ]
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
                        "rows": [
                            {
                                "达人ID": "MINISO4/8转人工1",
                                "平台": "转人工",
                                "达人最后一次回复邮件时间": "2026/04/08",
                                "full body": "brand new body",
                                "当前网红报价": "$300",
                                "__feishu_update_mode": "create_or_mail_only_update",
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

        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(client.created_records[0]["fields"]["达人ID"], "MINISO4/8转人工3")

    def test_upload_payload_can_suppress_ai_labels(self) -> None:
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
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_alpha",
                                "ai是否通过": "是",
                                "标签(ai)": "家庭用品和家电-家庭博主；美食",
                                "ai评价": "nice",
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
                    suppress_ai_labels=True,
                )

        self.assertTrue(result["suppress_ai_labels"])
        self.assertEqual(result["created_count"], 1)
        created_fields = client.created_records[0]["fields"]
        self.assertEqual(created_fields["达人ID"], "alpha")
        self.assertEqual(created_fields["ai 是否通过"], "是")
        self.assertEqual(created_fields["ai 评价"], "nice")
        self.assertNotIn("标签（ai）", created_fields)

    def test_upload_payload_matches_existing_records_when_feishu_returns_rich_text_lists(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = [
            {
                "record_id": "rec_rich_existing",
                "fields": {
                    "达人ID": [{"text": "beta", "type": "text"}],
                    "平台": [{"text": "tiktok", "type": "text"}],
                    "达人对接人": [{"id": "ou_beta", "name": "陈俊仁"}],
                    "ai 是否通过": "是",
                },
            }
        ]
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
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_beta",
                                "当前网红报价": "$200",
                                "__feishu_update_mode": "mail_only_update",
                            }
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

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["updated_count"], 1)
        self.assertEqual(client.updated_records[0]["record_id"], "rec_rich_existing")

    def test_upload_skips_cross_project_same_creator_without_owner_scope(self) -> None:
        client = _FakeBitableUploadClient()
        client.include_task_name_field = True
        client.search_items = [
            {
                "record_id": "rec_skg1_existing",
                "fields": {
                    "达人ID": "beta",
                    "平台": "tiktok",
                    "达人对接人": [{"id": "ou_dd8d1d1c79d417255a2846862d8efca6", "name": "唐瑞霞"}],
                    "ai 是否通过": "是",
                },
            }
        ]
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
                                "达人对接人": "Sherry97",
                                "达人对接人_employee_id": "ou_7ed60ea94d265816ffcd02ae262c8030",
                                "ai是否通过": "是",
                            }
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

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(result["skipped_existing_count"], 1)
        self.assertEqual(len(client.created_records), 0)

    def test_upload_allows_existing_records_missing_owner_scope(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = [
            {
                "record_id": "rec_existing_missing_owner",
                "fields": {
                    "达人ID": "beta",
                    "平台": "tiktok",
                    "ai 是否通过": "是",
                },
            }
        ]
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
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                            }
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

        self.assertTrue(result["ok"])
        self.assertNotIn("guard_blocked", result)
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(result["skipped_existing_count"], 1)

    def test_upload_payload_updates_keep_record_when_target_table_contains_duplicate_record_keys(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = [
            {
                "record_id": "rec_dup_1",
                "fields": {
                    "达人ID": [{"text": "beta", "type": "text"}],
                    "平台": [{"text": "tiktok", "type": "text"}],
                    "达人对接人": [{"id": "ou_beta", "name": "陈俊仁"}],
                    "ai 是否通过": "是",
                    "达人最后一次回复邮件时间": "2026/03/30",
                },
            },
            {
                "record_id": "rec_dup_2",
                "fields": {
                    "达人ID": [{"text": "beta", "type": "text"}],
                    "平台": [{"text": "tiktok", "type": "text"}],
                    "达人对接人": [{"id": "ou_beta", "name": "陈俊仁"}],
                    "ai 是否通过": "否",
                    "达人最后一次回复邮件时间": "2026/03/29",
                },
            },
        ]
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
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "__feishu_update_mode": "create_or_update",
                            }
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

        self.assertTrue(result["ok"])
        self.assertEqual(result["duplicate_existing_group_count"], 1)
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["updated_count"], 1)
        self.assertEqual(len(client.created_records), 0)
        self.assertEqual(len(client.updated_records), 1)
        self.assertEqual(client.updated_records[0]["record_id"], "rec_dup_1")

    def test_upload_payload_deduplicates_duplicate_rows_and_keeps_last_version(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
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
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha-old",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "否",
                                "__feishu_update_mode": "create_or_update",
                            },
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha-new",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "__feishu_update_mode": "create_or_update",
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

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(result["deduplicated_row_count"], 1)
        self.assertEqual(len(result["deduplicated_rows"]), 1)
        self.assertEqual(client.created_records[0]["fields"]["ai 是否通过"], "是")
        self.assertEqual(
            client.created_records[0]["fields"]["主页链接"],
            {
                "link": "https://www.instagram.com/alpha-new",
                "text": "https://www.instagram.com/alpha-new",
                "type": "url",
            },
        )

    def test_upload_payload_keeps_success_when_result_xlsx_write_fails(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
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
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "__feishu_update_mode": "create_or_update",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ), patch(
                "feishu_screening_bridge.bitable_upload._write_upload_result_xlsx",
                side_effect=RuntimeError("xlsx write failed"),
            ):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["created_count"], 1)
            self.assertTrue(result["result_json_written"])
            self.assertFalse(result["result_xlsx_written"])
            self.assertEqual(len(result["report_write_warnings"]), 1)
            self.assertEqual(result["report_write_warnings"][0]["artifact"], "result_xlsx")
            self.assertTrue(Path(result["result_json_path"]).exists())
            saved_result = json.loads(Path(result["result_json_path"]).read_text(encoding="utf-8"))
            self.assertFalse(saved_result["result_xlsx_written"])
            self.assertEqual(saved_result["report_write_warnings"][0]["artifact"], "result_xlsx")

    def test_upload_retries_transient_create_failures_and_records_retry_summary(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
        client.create_record_side_effects = [
            FeishuApiError(
                "飞书请求失败: status=429 url=https://open.feishu.cn/open-apis/bitable/v1/apps/app_token/tables/tbl/records",
                status_code=429,
                retry_after_seconds=0.5,
                retryable=True,
            )
        ]
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
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "__feishu_update_mode": "create_or_update",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ), patch("feishu_screening_bridge.bitable_upload.time.sleep", return_value=None):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(client.record_create_attempt_count, 2)
        self.assertEqual(result["retry_summary"]["retried_request_count"], 1)
        self.assertEqual(result["retry_summary"]["retryable_error_count"], 1)
        self.assertGreater(result["retry_summary"]["backoff_sleep_seconds"], 0.0)
        self.assertEqual(result["retry_summary"]["operations"]["create_record"]["retried_request_count"], 1)

    def test_upload_retries_frequency_limit_message_even_when_retryable_flag_is_false(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
        client.create_record_side_effects = [
            FeishuApiError(
                "飞书接口返回错误: code=400 msg=request trigger frequency limit",
                status_code=400,
                retryable=False,
            )
        ]
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
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "__feishu_update_mode": "create_or_update",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ), patch("feishu_screening_bridge.bitable_upload.time.sleep", return_value=None):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(client.record_create_attempt_count, 2)
        self.assertEqual(result["retry_summary"]["retryable_error_count"], 1)

    def test_upload_retries_api_code_99991400_even_when_retryable_flag_is_false(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
        client.create_record_side_effects = [
            FeishuApiError(
                "飞书接口返回错误: code=99991400 msg=frequency limit",
                status_code=400,
                api_code=99991400,
                retryable=False,
            )
        ]
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
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "__feishu_update_mode": "create_or_update",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ), patch("feishu_screening_bridge.bitable_upload.time.sleep", return_value=None):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(client.record_create_attempt_count, 2)
        self.assertEqual(result["retry_summary"]["retryable_error_count"], 1)

    def test_upload_rolls_back_uploaded_attachments_when_later_attachment_fails(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
        client.upload_side_effects = [None, RuntimeError("second attachment failed")]
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
            attachment_a = root / "a.eml"
            attachment_b = root / "b.eml"
            attachment_a.write_text("a", encoding="utf-8")
            attachment_b.write_text("b", encoding="utf-8")
            payload_path = root / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "__feishu_attachment_local_paths": [str(attachment_a), str(attachment_b)],
                                "__feishu_update_mode": "create_or_update",
                            }
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

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(client.deleted_records, ["boxcn-upload-1"])
        self.assertIn("second attachment failed", result["failed_rows"][0]["error"])

    def test_upload_recovers_create_after_retryable_failure_when_record_already_exists_on_server(self) -> None:
        client = _FakeBitableUploadClient()
        client.search_items = []
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

        original_post_api_json = client.post_api_json

        def flaky_post_api_json(
            url_path: str,
            *,
            body: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
        ) -> dict[str, object]:
            if url_path.endswith("/records") and client.record_create_attempt_count == 0:
                client.record_create_attempt_count += 1
                client.search_items = [
                    {
                        "record_id": "rec_server_created",
                        "fields": {
                            "达人ID": "alpha",
                            "平台": "instagram",
                            "ai 是否通过": "是",
                        },
                    }
                ]
                raise FeishuApiError(
                    "飞书请求失败: status=503 url=https://open.feishu.cn/open-apis/bitable/v1/apps/app_token/tables/tbl/records",
                    status_code=503,
                    retryable=True,
                )
            return original_post_api_json(url_path, body=body, headers=headers)

        client.post_api_json = flaky_post_api_json  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tmpdir:
            payload_path = Path(tmpdir) / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "达人对接人": "陈俊仁",
                                "ai是否通过": "是",
                                "__feishu_update_mode": "create_or_update",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                return_value=resolved_view,
            ), patch("feishu_screening_bridge.bitable_upload.time.sleep", return_value=None):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app?table=tbl&view=vew",
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(len(client.created_records), 0)
        self.assertEqual(result["created_rows"][0]["record_id"], "rec_server_created")
        self.assertEqual(result["retry_summary"]["recovered_request_count"], 1)
        self.assertEqual(result["retry_summary"]["operations"]["create_record"]["recovered_request_count"], 1)

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
                workbook_file_url="https://unit-test.feishu.mock/open-apis/drive/v1/medias/box1/download?extra=bitablePerm",
                workbook_file_name="template.xlsx",
                sending_list_file_token="box2",
                sending_list_file_url="https://unit-test.feishu.mock/open-apis/drive/v1/medias/box2/download?extra=bitablePerm",
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

    def test_upload_canonicalizes_short_base_link_to_ai_reply_table(self) -> None:
        client = _FakeBitableUploadClient()
        resolved_view = ResolvedBitableView(
            source_url="https://example.com/base/app_token?table=tbl_ai&view=vew_ai",
            source_kind="base",
            source_token="app_token",
            app_token="app_token",
            table_id="tbl_ai",
            view_id="vew_ai",
            table_name="AI回信管理",
            view_name="表格",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            payload_path = Path(tmpdir) / "payload.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            def _resolve_side_effect(_client, url: str) -> ResolvedBitableView:
                if url == "https://example.com/base/app_token":
                    raise ValueError("飞书多维表格 URL 缺少 table 参数。")
                if url == "https://example.com/base/app_token?table=tbl_ai&view=vew_ai":
                    return resolved_view
                raise AssertionError(f"unexpected resolve url: {url}")

            with patch(
                "feishu_screening_bridge.bitable_upload.resolve_bitable_view_from_url",
                side_effect=_resolve_side_effect,
            ):
                result = upload_final_review_payload_to_bitable(
                    client,
                    payload_json_path=payload_path,
                    linked_bitable_url="https://example.com/base/app_token",
                    dry_run=True,
                )

        self.assertEqual(result["target_url"], "https://example.com/base/app_token?table=tbl_ai&view=vew_ai")
        self.assertEqual(result["target_table_name"], "AI回信管理")
        self.assertEqual(result["target_view_name"], "表格")
