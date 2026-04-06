from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import scripts.watch_screening_run as watch_screening_run


class WatchScreeningRunTests(unittest.TestCase):
    def test_format_progress_snapshot_marks_stalled_batches(self) -> None:
        now = datetime(2026, 4, 6, 20, 30, 0, tzinfo=timezone.utc)
        payload = {
            "status": "running",
            "stage": "tiktok.visual_running",
            "platform": "tiktok",
            "phase": "visual",
            "current_batch": 2,
            "batch_count": 6,
            "processed": 100,
            "total": 287,
            "last_item": "alpha",
            "last_log_line": "完成视觉 batch 2/6",
            "last_heartbeat_at": (now - timedelta(seconds=180)).isoformat(),
            "finished_at": "",
        }

        line = watch_screening_run.format_progress_snapshot(
            payload,
            now=now,
            stalled_after_seconds=120.0,
        )

        self.assertIn("state=stalled", line)
        self.assertIn("stage=tiktok.visual_running", line)
        self.assertIn("batch=2/6", line)
        self.assertIn("processed=100/287", line)
        self.assertIn("last_item=alpha", line)

    def test_resolve_and_load_progress_snapshot_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "run"
            run_dir.mkdir(parents=True, exist_ok=True)
            progress_path = run_dir / "progress.json"
            progress_path.write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "stage": "completed",
                        "last_heartbeat_at": "2026-04-06T20:30:00+00:00",
                        "finished_at": "2026-04-06T20:30:01+00:00",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            resolved = watch_screening_run.resolve_progress_path(run_dir=run_dir, progress_json=None)
            loaded = watch_screening_run.load_progress_snapshot(resolved)

        self.assertEqual(resolved, progress_path.resolve())
        self.assertEqual(loaded["status"], "completed")
        self.assertEqual(loaded["stage"], "completed")


if __name__ == "__main__":
    unittest.main()
