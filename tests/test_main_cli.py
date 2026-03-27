from __future__ import annotations

import unittest

from email_sync.__main__ import _build_parser


class MainCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = _build_parser()

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


if __name__ == "__main__":
    unittest.main()
