from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import scripts.run_shared_mailbox_sync as sync_runner
from scripts.run_shared_mailbox_sync import _load_last_successful_sync_date, _resolve_wrapper_sent_since


class SharedMailboxSyncTests(unittest.TestCase):
    def test_load_last_successful_sync_date_reads_completed_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "finished_at": "2026-04-03T08:15:00+08:00",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            self.assertEqual(_load_last_successful_sync_date(summary_path), date(2026, 4, 3))

    def test_resolve_wrapper_sent_since_prefers_explicit_cli_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "finished_at": "2026-04-03T08:15:00+08:00",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            resolved, source = _resolve_wrapper_sent_since("2026-04-01", summary_path, today=date(2026, 4, 5))
            self.assertEqual(resolved, date(2026, 4, 1))
            self.assertEqual(source, "cli_explicit")

    def test_resolve_wrapper_sent_since_backfills_from_last_successful_sync_with_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "finished_at": "2026-04-03T08:15:00+08:00",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            resolved, source = _resolve_wrapper_sent_since("", summary_path, today=date(2026, 4, 5))
            self.assertEqual(resolved, date(2026, 4, 2))
            self.assertEqual(source, "last_successful_sync_overlap_1d")

    def test_resolve_wrapper_sent_since_falls_back_to_today_without_prior_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            resolved, source = _resolve_wrapper_sent_since("", summary_path, today=date(2026, 4, 5))
            self.assertEqual(resolved, date(2026, 4, 5))
            self.assertEqual(source, "default_today_only")

    def test_run_shared_mailbox_sync_rebuilds_relation_index_after_new_mail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = SimpleNamespace(
                account_email="partnerships@amagency.biz",
                mail_folders=["其他文件夹/达人回信"],
                imap_host="imap.qq.com",
                imap_port=993,
                data_dir=root / "data",
                db_path=root / "data" / "email_sync.db",
                raw_dir=root / "data" / "raw",
                ensure_directories=lambda: (root / "data").mkdir(parents=True, exist_ok=True),
            )
            args = SimpleNamespace(
                env_file=".env",
                summary_json="",
                sent_since="",
                limit=0,
                reset_state=False,
                workers=1,
            )
            sync_results = [
                SimpleNamespace(
                    folder_name="其他文件夹/达人回信",
                    fetched=2,
                    skipped_state_advance=False,
                    last_seen_uid=10,
                    uidvalidity=1,
                    message_count_on_server=10,
                )
            ]
            with patch.object(sync_runner, "_build_settings", return_value=settings), patch.object(
                sync_runner,
                "_single_instance_lock",
                return_value=nullcontext(),
            ), patch.object(sync_runner, "sync_mailboxes", return_value=sync_results), patch.object(
                sync_runner,
                "rebuild_relation_index",
                return_value={"messages_indexed": 2, "threads": 2},
            ) as rebuild_mock:
                summary = sync_runner.run_shared_mailbox_sync(args)

        self.assertEqual(summary["status"], "completed")
        self.assertTrue(summary["relation_index_rebuilt"])
        self.assertEqual(summary["relation_index_stats"]["messages_indexed"], 2)
        rebuild_mock.assert_called_once()

    def test_run_shared_mailbox_sync_skips_relation_index_rebuild_when_no_new_mail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = SimpleNamespace(
                account_email="partnerships@amagency.biz",
                mail_folders=["其他文件夹/达人回信"],
                imap_host="imap.qq.com",
                imap_port=993,
                data_dir=root / "data",
                db_path=root / "data" / "email_sync.db",
                raw_dir=root / "data" / "raw",
                ensure_directories=lambda: (root / "data").mkdir(parents=True, exist_ok=True),
            )
            args = SimpleNamespace(
                env_file=".env",
                summary_json="",
                sent_since="",
                limit=0,
                reset_state=False,
                workers=1,
            )
            sync_results = [
                SimpleNamespace(
                    folder_name="其他文件夹/达人回信",
                    fetched=0,
                    skipped_state_advance=False,
                    last_seen_uid=10,
                    uidvalidity=1,
                    message_count_on_server=10,
                )
            ]
            with patch.object(sync_runner, "_build_settings", return_value=settings), patch.object(
                sync_runner,
                "_single_instance_lock",
                return_value=nullcontext(),
            ), patch.object(sync_runner, "sync_mailboxes", return_value=sync_results), patch.object(
                sync_runner,
                "rebuild_relation_index",
                return_value={"messages_indexed": 0},
            ) as rebuild_mock:
                summary = sync_runner.run_shared_mailbox_sync(args)

        self.assertEqual(summary["status"], "completed")
        self.assertFalse(summary["relation_index_rebuilt"])
        self.assertEqual(summary["relation_index_stats"], {})
        rebuild_mock.assert_not_called()
