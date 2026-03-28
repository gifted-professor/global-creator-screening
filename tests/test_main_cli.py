from __future__ import annotations

from datetime import date
import unittest

from email_sync.__main__ import _build_parser
from email_sync.date_windows import default_sync_sent_since, resolve_sync_sent_since, subtract_calendar_months
from scripts.run_keep_list_screening_pipeline import (
    build_parser as build_keep_list_parser,
    build_scrape_payload,
    build_visual_payload,
)


class MainCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = _build_parser()
        self.keep_list_parser = build_keep_list_parser()

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

    def test_keep_list_runner_builds_visual_payloads_from_identifiers(self) -> None:
        self.assertEqual(build_visual_payload("instagram", ["alpha"]), {"identifiers": ["alpha"]})
        self.assertEqual(build_visual_payload("youtube", ["gamma"]), {"identifiers": ["gamma"]})


if __name__ == "__main__":
    unittest.main()
