from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

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

