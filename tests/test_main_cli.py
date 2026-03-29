from __future__ import annotations

from contextlib import redirect_stdout
from datetime import date
from io import StringIO
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from email_sync.__main__ import _build_parser
from email_sync.date_windows import default_sync_sent_since, resolve_sync_sent_since, subtract_calendar_months
from feishu_screening_bridge.__main__ import (
    _build_parser as build_feishu_bridge_parser,
    _cmd_import_from_feishu,
    _cmd_sync_task_upload_view,
)
from scripts.run_keep_list_screening_pipeline import (
    build_parser as build_keep_list_parser,
    build_scrape_payload,
    build_visual_payload,
)


class MainCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = _build_parser()
        self.keep_list_parser = build_keep_list_parser()
        self.feishu_bridge_parser = build_feishu_bridge_parser()

    def test_list_folders_uses_default_env_file(self) -> None:
        args = self.parser.parse_args(["list-folders"])
        self.assertEqual(args.command, "list-folders")
        self.assertEqual(args.env_file, ".env")

    def test_sync_parser_supports_folder_limit_reset_state_and_workers(self) -> None:
        args = self.parser.parse_args(
            ["sync", "--folder", "INBOX", "--folder", "Sent", "--limit", "20", "--reset-state", "--workers", "4", "--sent-since", "2026-03-01"]
        )
        self.assertEqual(args.command, "sync")
        self.assertEqual(args.folder, ["INBOX", "Sent"])
        self.assertEqual(args.limit, 20)
        self.assertTrue(args.reset_state)
        self.assertEqual(args.workers, 4)
        self.assertEqual(args.sent_since, "2026-03-01")

    def test_subtract_calendar_months_keeps_same_day_when_possible(self) -> None:
        self.assertEqual(subtract_calendar_months(date(2026, 3, 27), 3), date(2025, 12, 27))

    def test_subtract_calendar_months_clamps_end_of_month(self) -> None:
        self.assertEqual(subtract_calendar_months(date(2026, 5, 31), 3), date(2026, 2, 28))

    def test_resolve_sync_sent_since_defaults_to_recent_three_months(self) -> None:
        self.assertEqual(resolve_sync_sent_since(None, today=date(2026, 3, 27)), date(2025, 12, 27))

    def test_default_sync_sent_since_uses_calendar_month_window(self) -> None:
        self.assertEqual(default_sync_sent_since(today=date(2026, 3, 27)), date(2025, 12, 27))

    def test_query_parser_keeps_filters(self) -> None:
        args = self.parser.parse_args(
            [
                "query",
                "--folder",
                "INBOX",
                "--keyword",
                "报价",
                "--has-attachments",
                "--sent-after",
                "2026-03-01",
                "--limit",
                "5",
                "--json",
            ]
        )
        self.assertEqual(args.command, "query")
        self.assertEqual(args.folder, ["INBOX"])
        self.assertEqual(args.keyword, "报价")
        self.assertTrue(args.has_attachments)
        self.assertEqual(args.sent_after, "2026-03-01")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.json)

    def test_index_parser_uses_default_env_file(self) -> None:
        args = self.parser.parse_args(["index"])
        self.assertEqual(args.command, "index")
        self.assertEqual(args.env_file, ".env")

    def test_enrich_creators_parser_keeps_input_and_output_prefix(self) -> None:
        args = self.parser.parse_args(
            ["enrich-creators", "--input", "/tmp/creators.xlsx", "--output-prefix", "exports/out"]
        )
        self.assertEqual(args.command, "enrich-creators")
        self.assertEqual(args.env_file, ".env")
        self.assertEqual(args.input, "/tmp/creators.xlsx")
        self.assertEqual(args.output_prefix, "exports/out")

    def test_enrich_creators_parser_supports_task_driven_sending_list_mode(self) -> None:
        args = self.parser.parse_args(
            [
                "enrich-creators",
                "--task-name",
                "MINISO",
                "--task-upload-url",
                "https://example.com/task-upload",
                "--task-download-dir",
                "downloads/task_upload_attachments",
                "--db-path",
                "/tmp/miniso.db",
            ]
        )
        self.assertEqual(args.command, "enrich-creators")
        self.assertEqual(args.task_name, "MINISO")
        self.assertEqual(args.task_upload_url, "https://example.com/task-upload")
        self.assertEqual(args.task_download_dir, "downloads/task_upload_attachments")
        self.assertEqual(args.db_path, "/tmp/miniso.db")

    def test_prepare_duplicate_review_parser_keeps_sample_options(self) -> None:
        args = self.parser.parse_args(
            [
                "prepare-duplicate-review",
                "--input",
                "/tmp/high.xlsx",
                "--db-path",
                "/tmp/email_sync.db",
                "--output-prefix",
                "temp/sample",
                "--group-key",
                "last_mail_message_id:2",
                "--sample-limit",
                "2",
            ]
        )
        self.assertEqual(args.command, "prepare-duplicate-review")
        self.assertEqual(args.input, "/tmp/high.xlsx")
        self.assertEqual(args.db_path, "/tmp/email_sync.db")
        self.assertEqual(args.output_prefix, "temp/sample")
        self.assertEqual(args.group_key, ["last_mail_message_id:2"])
        self.assertEqual(args.sample_limit, 2)

    def test_review_duplicate_groups_parser_keeps_llm_options(self) -> None:
        args = self.parser.parse_args(
            [
                "review-duplicate-groups",
                "--input",
                "/tmp/high.xlsx",
                "--output-prefix",
                "temp/review",
                "--sample-limit",
                "2",
                "--base-url",
                "https://example.com/v1",
                "--api-key",
                "sk-test",
                "--model",
                "gpt-test",
            ]
        )
        self.assertEqual(args.command, "review-duplicate-groups")
        self.assertEqual(args.input, "/tmp/high.xlsx")
        self.assertEqual(args.output_prefix, "temp/review")
        self.assertEqual(args.sample_limit, 2)
        self.assertEqual(args.base_url, "https://example.com/v1")
        self.assertEqual(args.api_key, "sk-test")
        self.assertEqual(args.model, "gpt-test")

    def test_prepare_llm_review_candidates_parser_keeps_paths(self) -> None:
        args = self.parser.parse_args(
            [
                "prepare-llm-review-candidates",
                "--input",
                "/tmp/high.xlsx",
                "--db-path",
                "/tmp/email_sync.db",
                "--output-prefix",
                "exports/miniso_review",
            ]
        )
        self.assertEqual(args.command, "prepare-llm-review-candidates")
        self.assertEqual(args.input, "/tmp/high.xlsx")
        self.assertEqual(args.db_path, "/tmp/email_sync.db")
        self.assertEqual(args.output_prefix, "exports/miniso_review")

    def test_run_llm_review_parser_keeps_provider_overrides(self) -> None:
        args = self.parser.parse_args(
            [
                "run-llm-review",
                "--input-prefix",
                "exports/miniso_review",
                "--base-url",
                "https://example.com/v1",
                "--api-key",
                "sk-test",
                "--model",
                "qwen-test",
                "--wire-api",
                "responses",
            ]
        )
        self.assertEqual(args.command, "run-llm-review")
        self.assertEqual(args.input_prefix, "exports/miniso_review")
        self.assertEqual(args.base_url, "https://example.com/v1")
        self.assertEqual(args.api_key, "sk-test")
        self.assertEqual(args.model, "qwen-test")
        self.assertEqual(args.wire_api, "responses")

    def test_match_brand_keyword_parser_keeps_generic_inputs(self) -> None:
        args = self.parser.parse_args(
            [
                "match-brand-keyword",
                "--input",
                "/tmp/input.xlsx",
                "--db-path",
                "/tmp/email_sync.db",
                "--keyword",
                "MINISO",
                "--output-prefix",
                "exports/miniso_fast_path",
                "--message-limit",
                "50",
                "--include-from",
                "--email-column",
                "邮箱地址",
                "--profile-column",
                "IGlink",
            ]
        )
        self.assertEqual(args.command, "match-brand-keyword")
        self.assertEqual(args.input, "/tmp/input.xlsx")
        self.assertEqual(args.db_path, "/tmp/email_sync.db")
        self.assertEqual(args.keyword, "MINISO")
        self.assertEqual(args.output_prefix, "exports/miniso_fast_path")
        self.assertEqual(args.message_limit, 50)
        self.assertTrue(args.include_from)
        self.assertEqual(args.email_column, "邮箱地址")
        self.assertEqual(args.profile_column, "IGlink")

    def test_split_shared_email_parser_keeps_paths(self) -> None:
        args = self.parser.parse_args(
            [
                "split-shared-email",
                "--input",
                "/tmp/deduped.xlsx",
                "--output-prefix",
                "exports/shared_email_split",
            ]
        )
        self.assertEqual(args.command, "split-shared-email")
        self.assertEqual(args.input, "/tmp/deduped.xlsx")
        self.assertEqual(args.output_prefix, "exports/shared_email_split")

    def test_resolve_shared_email_parser_keeps_paths(self) -> None:
        args = self.parser.parse_args(
            [
                "resolve-shared-email",
                "--input",
                "/tmp/shared.xlsx",
                "--db-path",
                "/tmp/email_sync.db",
                "--output-prefix",
                "exports/shared_email_resolution",
            ]
        )
        self.assertEqual(args.command, "resolve-shared-email")
        self.assertEqual(args.input, "/tmp/shared.xlsx")
        self.assertEqual(args.db_path, "/tmp/email_sync.db")
        self.assertEqual(args.output_prefix, "exports/shared_email_resolution")

    def test_llm_final_review_parser_keeps_auto_keep_and_provider_overrides(self) -> None:
        args = self.parser.parse_args(
            [
                "llm-final-review",
                "--input-prefix",
                "exports/shared_email_resolution",
                "--auto-keep-workbook",
                "exports/unique.xlsx",
                "--auto-keep-workbook",
                "exports/resolved.xlsx",
                "--base-url",
                "https://example.com/v1",
                "--api-key",
                "sk-test",
                "--model",
                "gpt-test",
                "--wire-api",
                "responses",
            ]
        )
        self.assertEqual(args.command, "llm-final-review")
        self.assertEqual(args.input_prefix, "exports/shared_email_resolution")
        self.assertEqual(args.auto_keep_workbook, ["exports/unique.xlsx", "exports/resolved.xlsx"])
        self.assertEqual(args.base_url, "https://example.com/v1")
        self.assertEqual(args.api_key, "sk-test")
        self.assertEqual(args.model, "gpt-test")
        self.assertEqual(args.wire_api, "responses")

    def test_keep_list_runner_parser_keeps_bounded_execution_flags(self) -> None:
        args = self.keep_list_parser.parse_args(
            [
                "--keep-workbook",
                "exports/miniso_keep.xlsx",
                "--template-workbook",
                "downloads/miniso_template.xlsx",
                "--task-name",
                "MINISO",
                "--platform",
                "instagram",
                "--platform",
                "tiktok",
                "--vision-provider",
                "openai",
                "--probe-vision-provider-only",
                "--max-identifiers-per-platform",
                "25",
                "--skip-scrape",
                "--skip-visual",
            ]
        )
        self.assertEqual(args.keep_workbook, "exports/miniso_keep.xlsx")
        self.assertEqual(args.template_workbook, "downloads/miniso_template.xlsx")
        self.assertEqual(args.task_name, "MINISO")
        self.assertEqual(args.platform, ["instagram", "tiktok"])
        self.assertEqual(args.vision_provider, "openai")
        self.assertTrue(args.probe_vision_provider_only)
        self.assertEqual(args.max_identifiers_per_platform, 25)
        self.assertTrue(args.skip_scrape)
        self.assertTrue(args.skip_visual)

    def test_keep_list_runner_builds_platform_specific_scrape_payloads(self) -> None:
        self.assertEqual(build_scrape_payload("instagram", ["alpha"]), {"usernames": ["alpha"]})
        self.assertEqual(build_scrape_payload("tiktok", ["beta"]), {"profiles": ["beta"]})
        self.assertEqual(build_scrape_payload("youtube", ["https://youtube.com/@gamma"]), {"urls": ["https://youtube.com/@gamma"]})

    def test_feishu_bridge_import_command_returns_structured_legacy_dependency_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=test_app",
                        "FEISHU_APP_SECRET=test_secret",
                        "FEISHU_FILE_TOKEN=boxcn-test",
                        "PROJECT_CODE=P-001",
                        "PRIMARY_CATEGORY=lifestyle",
                    ]
                ),
                encoding="utf-8",
            )
            args = self.feishu_bridge_parser.parse_args(
                [
                    "import-from-feishu",
                    "--env-file",
                    str(env_path),
                    "--json",
                ]
            )
            captured = StringIO()
            with (
                patch(
                    "feishu_screening_bridge.__main__.inspect_email_project_dependency",
                    return_value={
                        "available": False,
                        "error_code": "EMAIL_PROJECT_ROOT_MISSING",
                        "message": "legacy bridge 依赖的外部 email 项目目录不存在: /tmp/email",
                        "remediation": "set EMAIL_PROJECT_ROOT",
                    },
                ),
                patch(
                    "feishu_screening_bridge.__main__.import_screening_workbook_from_feishu",
                    side_effect=AssertionError("should not reach legacy import"),
                ),
                redirect_stdout(captured),
            ):
                exit_code = _cmd_import_from_feishu(args)

        payload = json.loads(captured.getvalue())
        self.assertEqual(exit_code, 2)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error_code"], "EMAIL_PROJECT_ROOT_MISSING")
        self.assertIn("legacyDependency", payload)

    def test_feishu_bridge_sync_task_upload_view_returns_structured_legacy_dependency_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=test_app",
                        "FEISHU_APP_SECRET=test_secret",
                        "TASK_UPLOAD_URL=https://example.com/task",
                    ]
                ),
                encoding="utf-8",
            )
            args = self.feishu_bridge_parser.parse_args(
                [
                    "sync-task-upload-view",
                    "--env-file",
                    str(env_path),
                    "--json",
                ]
            )
            captured = StringIO()
            with (
                patch(
                    "feishu_screening_bridge.__main__.inspect_email_project_dependency",
                    return_value={
                        "available": False,
                        "error_code": "EMAIL_PROJECT_PACKAGE_MISSING",
                        "message": "legacy bridge 指向的目录缺少 email_sync 包: /tmp/email/email_sync",
                        "remediation": "fix email project root",
                    },
                ),
                patch(
                    "feishu_screening_bridge.__main__.sync_task_upload_view_to_email_project",
                    side_effect=AssertionError("should not reach legacy sync"),
                ),
                redirect_stdout(captured),
            ):
                exit_code = _cmd_sync_task_upload_view(args)

        payload = json.loads(captured.getvalue())
        self.assertEqual(exit_code, 2)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error_code"], "EMAIL_PROJECT_PACKAGE_MISSING")
        self.assertEqual(payload["command"], "sync-task-upload-view")

    def test_keep_list_runner_builds_visual_payloads_from_identifiers(self) -> None:
        self.assertEqual(build_visual_payload("instagram", ["alpha"]), {"identifiers": ["alpha"]})
        self.assertEqual(build_visual_payload("youtube", ["gamma"]), {"identifiers": ["gamma"]})


if __name__ == "__main__":
    unittest.main()
