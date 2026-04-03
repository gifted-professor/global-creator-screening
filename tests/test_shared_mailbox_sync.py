from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import types
import unittest
from unittest.mock import patch

from email_sync.db import Database
import scripts.run_shared_mailbox_sync as shared_sync


class _FakeSettings:
    def __init__(self, data_dir: Path) -> None:
        self.account_email = "partnerships@amagency.biz"
        self.auth_code = "secret"
        self.imap_host = "imap.example.com"
        self.imap_port = 993
        self.data_dir = data_dir
        self.db_path = data_dir / "email_sync.db"
        self.raw_dir = data_dir / "raw"
        self.mail_folders = ["其他文件夹/邮件备份"]
        self.readonly = False

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)


class SharedMailboxSyncTests(unittest.TestCase):
    def test_resolve_shared_mailbox_sent_since_prefers_cli(self) -> None:
        resolved, source = shared_sync.resolve_shared_mailbox_sent_since(
            "2026-04-01",
            today=date(2026, 4, 3),
            last_sync_completed_at="2026-03-31T13:05:05+00:00",
        )
        self.assertEqual(resolved, date(2026, 4, 1))
        self.assertEqual(source, "cli")

    def test_resolve_shared_mailbox_sent_since_backfills_from_last_successful_sync(self) -> None:
        resolved, source = shared_sync.resolve_shared_mailbox_sent_since(
            "",
            today=date(2026, 4, 3),
            last_sync_completed_at="2026-03-31T13:05:05+00:00",
        )
        self.assertEqual(resolved, date(2026, 3, 30))
        self.assertEqual(source, "last_successful_sync_backfill")

    def test_run_shared_mailbox_sync_records_resolved_backfill_window(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "shared_mailbox"
            settings = _FakeSettings(data_dir)
            settings.ensure_directories()

            db = Database(settings.db_path)
            try:
                db.init_schema()
                db.update_sync_state(
                    account_email=settings.account_email,
                    folder_name=settings.mail_folders[0],
                    uidvalidity=1774931298,
                    last_seen_uid=31360,
                    last_run_synced=5,
                    last_sync_started_at="2026-03-31T13:05:03+00:00",
                    last_sync_completed_at="2026-03-31T13:05:05+00:00",
                    last_error=None,
                )
            finally:
                db.close()

            captured: dict[str, object] = {}

            def fake_sync_mailboxes(settings_obj, db_obj, **kwargs):
                captured["sent_since"] = kwargs["sent_since"]
                return []

            args = types.SimpleNamespace(
                env_file=str((Path(temp_dir) / ".env").resolve()),
                account_email="",
                account_auth_code="",
                folder="其他文件夹/邮件备份",
                data_dir=str(data_dir),
                db_path="",
                raw_dir="",
                summary_json="",
                sent_since="",
                limit=0,
                reset_state=False,
                workers=1,
            )

            with patch.object(shared_sync, "_build_settings", return_value=settings), patch.object(
                shared_sync,
                "sync_mailboxes",
                side_effect=fake_sync_mailboxes,
            ):
                summary = shared_sync.run_shared_mailbox_sync(args)

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["resolved_sent_since"], "2026-03-30")
        self.assertEqual(summary["sent_since_source"], "last_successful_sync_backfill")
        self.assertEqual(captured["sent_since"], date(2026, 3, 30))


if __name__ == "__main__":
    unittest.main()
