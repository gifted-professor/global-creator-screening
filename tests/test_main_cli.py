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


if __name__ == "__main__":
    unittest.main()
