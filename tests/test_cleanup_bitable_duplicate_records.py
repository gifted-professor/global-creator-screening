from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import scripts.cleanup_bitable_duplicate_records as cleanup_script


class _FakeCleanupClient:
    def __init__(self) -> None:
        self.deleted_record_ids: list[str] = []
        self.updated_records: list[dict[str, object]] = []

    def delete_api_json(self, url_path: str, *, body=None, headers=None):  # type: ignore[override]
        self.deleted_record_ids.append(url_path.rsplit("/", 1)[-1])
        return {"data": {}}

    def put_api_json(self, url_path: str, *, body=None, headers=None):  # type: ignore[override]
        self.updated_records.append(
            {
                "record_id": url_path.rsplit("/", 1)[-1],
                "fields": dict((body or {}).get("fields") or {}),
            }
        )
        return {"data": {}}


class CleanupBitableDuplicateRecordsTests(unittest.TestCase):
    def test_infer_platform_from_profile_url_uses_hostname_instead_of_path_substring(self) -> None:
        self.assertEqual(
            cleanup_script._infer_platform_from_profile_url("https://www.instagram.com/username/tiktok.com"),
            "instagram",
        )
        self.assertEqual(
            cleanup_script._infer_platform_from_profile_url("www.tiktok.com/@alpha"),
            "tiktok",
        )

    def test_parser_accepts_linked_bitable_url_alias(self) -> None:
        parser = cleanup_script._build_parser()
        args = parser.parse_args(["--linked-bitable-url", "https://example.com/base/app?table=tbl&view=vew"])
        self.assertEqual(args.url, "https://example.com/base/app?table=tbl&view=vew")
        args = parser.parse_args(
            ["--linked-bitable-url", "https://example.com/base/app?table=tbl&view=vew", "--safe-only"]
        )
        self.assertTrue(args.safe_only)
        args = parser.parse_args(
            [
                "--linked-bitable-url",
                "https://example.com/base/app?table=tbl&view=vew",
                "--key-mode",
                cleanup_script.KEY_MODE_CREATOR_PROFILE_URL,
            ]
        )
        self.assertEqual(args.key_mode, cleanup_script.KEY_MODE_CREATOR_PROFILE_URL)

    def test_cleanup_profile_url_key_mode_treats_polluted_platform_groups_as_safe_duplicates(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        field_schemas = {}
        existing_records = [
            (
                "rec_inst_keep",
                {
                    "达人ID": "alpha",
                    "平台": "🚫重复",
                    "主页链接": "https://www.instagram.com/alpha",
                    "ai 是否通过": "是",
                },
            ),
            (
                "rec_inst_dup",
                {
                    "达人ID": "alpha",
                    "平台": "🚫重复",
                    "主页链接": "https://www.instagram.com/alpha",
                    "ai 是否通过": "否",
                },
            ),
            (
                "rec_tt_keep",
                {
                    "达人ID": "alpha",
                    "平台": "🚫重复",
                    "主页链接": "https://www.tiktok.com/@alpha",
                    "ai 是否通过": "是",
                },
            ),
            (
                "rec_tt_dup",
                {
                    "达人ID": "alpha",
                    "平台": "🚫重复",
                    "主页链接": "https://www.tiktok.com/@alpha",
                    "ai 是否通过": "否",
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.resolve_bitable_view_from_url",
            return_value=resolved_view,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._canonicalize_target_url",
            return_value=resolved_view.source_url,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._fetch_field_schemas",
            return_value=field_schemas,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._fetch_existing_records",
            return_value=existing_records,
        ):
            result = cleanup_script.cleanup_duplicate_records(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=False,
                safe_only=True,
                key_mode=cleanup_script.KEY_MODE_CREATOR_PROFILE_URL,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["key_mode"], cleanup_script.KEY_MODE_CREATOR_PROFILE_URL)
        self.assertEqual(result["key_display_name"], "达人ID+主页链接")
        self.assertEqual(result["duplicate_group_count"], 2)
        self.assertEqual(result["safe_group_count"], 2)
        self.assertEqual(result["risky_group_count"], 0)
        self.assertEqual(result["planned_delete_row_count"], 2)

    def test_cleanup_profile_url_key_mode_surfaces_skipped_counts(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        field_schemas = {"达人对接人": object()}
        existing_records = [
            (
                "rec_missing_owner",
                {
                    "达人ID": "alpha",
                    "主页链接": "https://www.instagram.com/alpha",
                },
            ),
            (
                "rec_missing_profile",
                {
                    "达人ID": "beta",
                    "达人对接人": "owner_1",
                },
            ),
            (
                "rec_missing_creator",
                {
                    "主页链接": "https://www.tiktok.com/@gamma",
                    "达人对接人": "owner_1",
                },
            ),
            (
                "rec_valid_keep",
                {
                    "达人ID": "delta",
                    "主页链接": "https://www.instagram.com/delta",
                    "达人对接人": "owner_1",
                    "ai 是否通过": "是",
                },
            ),
            (
                "rec_valid_dup",
                {
                    "达人ID": "delta",
                    "主页链接": "https://www.instagram.com/delta",
                    "达人对接人": "owner_1",
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.resolve_bitable_view_from_url",
            return_value=resolved_view,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._canonicalize_target_url",
            return_value=resolved_view.source_url,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._fetch_field_schemas",
            return_value=field_schemas,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._fetch_existing_records",
            return_value=existing_records,
        ), patch(
            "scripts.cleanup_bitable_duplicate_records._resolve_owner_scope_field_name",
            return_value="达人对接人",
        ):
            result = cleanup_script.cleanup_duplicate_records(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=False,
                safe_only=True,
                key_mode=cleanup_script.KEY_MODE_CREATOR_PROFILE_URL,
            )

        self.assertEqual(result["skipped_owner_scope_record_count"], 1)
        self.assertEqual(result["skipped_missing_profile_url_record_count"], 1)
        self.assertEqual(result["skipped_missing_creator_id_record_count"], 1)
        self.assertEqual(result["skipped_record_count"], 3)
        self.assertEqual(result["planned_delete_row_count"], 1)

    def test_cleanup_execute_safe_only_deletes_only_single_profile_url_groups(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        analysis = SimpleNamespace(
            duplicate_groups=[
                {
                    "record_key": "alpha::🚫重复",
                    "owner_scope_value": "",
                    "creator_id": "alpha",
                    "platform": "🚫重复",
                    "keep_record": {
                        "record_id": "rec_keep_safe",
                        "fields": {
                            "主页链接": "https://www.tiktok.com/@alpha",
                        },
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup_safe",
                            "fields": {
                                "主页链接": "https://www.tiktok.com/@alpha",
                            },
                        }
                    ],
                },
                {
                    "record_key": "beta::🚫重复",
                    "owner_scope_value": "",
                    "creator_id": "beta",
                    "platform": "🚫重复",
                    "keep_record": {
                        "record_id": "rec_keep_risky",
                        "fields": {
                            "主页链接": "https://www.instagram.com/beta",
                        },
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup_risky",
                            "fields": {
                                "主页链接": "https://www.tiktok.com/@beta",
                            },
                        }
                    ],
                },
            ],
            key_field_names=("达人ID", "平台"),
            key_display_name="达人ID+平台",
            owner_scope_field_name="",
            owner_scope_missing_record_count=0,
            index={"alpha::🚫重复": {"record_id": "rec_keep_safe", "fields": {}}, "beta::🚫重复": {"record_id": "rec_keep_risky", "fields": {}}},
        )

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.fetch_existing_bitable_record_analysis",
            return_value=(resolved_view, analysis),
        ):
            result = cleanup_script.cleanup_duplicate_records(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=True,
                safe_only=True,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["execute_mode"], "safe_only")
        self.assertEqual(result["safe_group_count"], 1)
        self.assertEqual(result["risky_group_count"], 1)
        self.assertEqual(result["planned_delete_group_count"], 1)
        self.assertEqual(result["planned_delete_row_count"], 1)
        self.assertEqual(result["deleted_record_count"], 1)
        self.assertEqual(client.deleted_record_ids, ["rec_dup_safe"])

    def test_platform_repair_updates_only_risky_groups_by_profile_url(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        analysis = SimpleNamespace(
            duplicate_groups=[
                {
                    "record_key": "safe::instagram",
                    "owner_scope_value": "",
                    "creator_id": "safe",
                    "platform": "instagram",
                    "keep_record": {
                        "record_id": "rec_keep_safe",
                        "fields": {"平台": "🚫重复", "主页链接": "https://www.instagram.com/safe"},
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup_safe",
                            "fields": {"平台": "🚫重复", "主页链接": "https://www.instagram.com/safe"},
                        }
                    ],
                },
                {
                    "record_key": "risky::🚫重复",
                    "owner_scope_value": "",
                    "creator_id": "risky",
                    "platform": "🚫重复",
                    "keep_record": {
                        "record_id": "rec_keep_risky",
                        "fields": {"平台": "🚫重复", "主页链接": "https://www.instagram.com/risky"},
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup_tiktok",
                            "fields": {"平台": "🚫重复", "主页链接": "https://www.tiktok.com/@risky"},
                        },
                        {
                            "record_id": "rec_dup_youtube",
                            "fields": {"平台": "🚫重复", "主页链接": "https://www.youtube.com/@risky"},
                        },
                    ],
                },
            ],
            key_field_names=("达人ID", "平台"),
            key_display_name="达人ID+平台",
            owner_scope_field_name="",
            owner_scope_missing_record_count=0,
            index={},
        )

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.fetch_existing_bitable_record_analysis",
            return_value=(resolved_view, analysis),
        ):
            result = cleanup_script.repair_platform_field_from_profile_url(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=True,
            )
            saved_summary = json.loads(Path(result["summary_path"]).read_text(encoding="utf-8"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["repair_strategy"], cleanup_script.PLATFORM_REPAIR_STRATEGY_NAME)
        self.assertEqual(result["safe_group_count"], 1)
        self.assertEqual(result["skipped_safe_group_count"], 1)
        self.assertEqual(result["skipped_safe_platform_pollution_group_count"], 1)
        self.assertEqual(result["skipped_safe_platform_pollution_row_count"], 2)
        self.assertEqual(result["risky_group_count"], 1)
        self.assertEqual(result["risky_platform_pollution_group_count"], 1)
        self.assertEqual(result["risky_platform_pollution_row_count"], 3)
        self.assertEqual(result["repair_group_count"], 1)
        self.assertEqual(result["repair_row_count"], 3)
        self.assertEqual(result["updated_record_count"], 3)
        self.assertEqual(
            client.updated_records,
            [
                {"record_id": "rec_keep_risky", "fields": {"平台": "instagram"}},
                {"record_id": "rec_dup_tiktok", "fields": {"平台": "tiktok"}},
                {"record_id": "rec_dup_youtube", "fields": {"平台": "youtube"}},
            ],
        )
        self.assertEqual(saved_summary["updated_record_count"], 3)

    def test_cleanup_execute_allows_global_scope_when_owner_field_is_absent(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        analysis = SimpleNamespace(
            duplicate_groups=[
                {
                    "record_key": "alpha::instagram",
                    "owner_scope_value": "",
                    "creator_id": "alpha",
                    "platform": "instagram",
                    "keep_record": {
                        "record_id": "rec_keep",
                        "fields": {
                            "ai 是否通过": "是",
                            "达人最后一次回复邮件时间": "2026/04/05",
                            "主页链接": "https://www.instagram.com/alpha",
                        },
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup",
                            "fields": {
                                "ai 是否通过": "否",
                                "达人最后一次回复邮件时间": "2026/04/04",
                            },
                        }
                    ],
                }
            ],
            key_field_names=("达人ID", "平台"),
            key_display_name="达人ID+平台",
            owner_scope_field_name="",
            owner_scope_missing_record_count=0,
            index={"alpha::instagram": {"record_id": "rec_keep", "fields": {}}},
        )

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.fetch_existing_bitable_record_analysis",
            return_value=(resolved_view, analysis),
        ):
            result = cleanup_script.cleanup_duplicate_records(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=True,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["scope_mode"], "global_creator_platform")
        self.assertEqual(result["deleted_record_count"], 1)
        self.assertEqual(client.deleted_record_ids, ["rec_dup"])

    def test_cleanup_summary_includes_keep_strategy_and_executes_deletions(self) -> None:
        client = _FakeCleanupClient()
        resolved_view = SimpleNamespace(
            source_url="https://example.com/base/app?table=tbl&view=vew",
            app_token="app_token",
            table_id="tbl",
            table_name="达人管理",
            view_id="vew",
            view_name="总视图",
        )
        analysis = SimpleNamespace(
            duplicate_groups=[
                {
                    "record_key": "alpha::instagram",
                    "owner_scope_value": "ou_alpha",
                    "creator_id": "alpha",
                    "platform": "instagram",
                    "keep_record": {
                        "record_id": "rec_keep",
                        "fields": {
                            "ai 是否通过": "是",
                            "达人最后一次回复邮件时间": "2026/04/05",
                            "主页链接": "https://www.instagram.com/alpha",
                        },
                    },
                    "duplicate_records": [
                        {
                            "record_id": "rec_dup",
                            "fields": {
                                "ai 是否通过": "否",
                                "达人最后一次回复邮件时间": "2026/04/04",
                            },
                        }
                    ],
                }
            ],
            key_field_names=("达人ID", "平台"),
            key_display_name="达人ID+平台",
            owner_scope_field_name="达人对接人",
            owner_scope_missing_record_count=0,
            index={"alpha::instagram": {"record_id": "rec_keep", "fields": {}}},
        )

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "scripts.cleanup_bitable_duplicate_records.fetch_existing_bitable_record_analysis",
            return_value=(resolved_view, analysis),
        ):
            result = cleanup_script.cleanup_duplicate_records(
                client=client,
                linked_bitable_url=resolved_view.source_url,
                output_root=Path(tmpdir),
                execute=True,
            )
            saved_summary = json.loads(Path(result["summary_path"]).read_text(encoding="utf-8"))
            report_exists = Path(result["report_xlsx_path"]).exists()

        self.assertTrue(result["ok"])
        self.assertEqual(result["keep_strategy"], cleanup_script.KEEP_STRATEGY_NAME)
        self.assertEqual(result["keep_strategy_description"], cleanup_script.KEEP_STRATEGY_DESCRIPTION)
        self.assertEqual(result["scope_mode"], "owner_scoped")
        self.assertEqual(result["deleted_record_count"], 1)
        self.assertEqual(client.deleted_record_ids, ["rec_dup"])
        self.assertTrue(report_exists)
        self.assertEqual(saved_summary["keep_strategy"], cleanup_script.KEEP_STRATEGY_NAME)
