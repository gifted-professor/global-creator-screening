from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from email_sync.config import Settings
from email_sync.db import Database
from email_sync.imap_sync import FetchedMessage, MailboxInfo, sync_mailboxes
from email_sync.mail_parser import ParsedMessage


class _FakeImapClient:
    def select(self, mailbox_name: str, readonly: bool = True):
        return "OK", [b"4"]

    def response(self, code: str):
        if code == "UIDVALIDITY":
            return "OK", [b"1001"]
        return "OK", [None]

    def close(self) -> None:
        return None

    def logout(self) -> None:
        return None


def _make_parsed(uid: int) -> ParsedMessage:
    return ParsedMessage(
        account_email="demo@qq.com",
        folder_name="INBOX",
        uid=uid,
        uidvalidity=1001,
        message_id=f"<{uid}@example.com>",
        subject=f"subject-{uid}",
        in_reply_to=None,
        references_header=None,
        sent_at=f"2026-03-{10 + uid:02d}T09:00:00+08:00",
        sent_at_raw=f"2026-03-{10 + uid:02d}T09:00:00+08:00",
        internal_date=f"2026-03-{10 + uid:02d}T09:00:00+08:00",
        internal_date_raw=f"2026-03-{10 + uid:02d}T09:00:00+08:00",
        flags=["\\Seen"],
        size_bytes=100 + uid,
        from_addresses=[{"name": "Sender", "address": f"sender{uid}@example.com"}],
        to_addresses=[{"name": "", "address": "demo@qq.com"}],
        cc_addresses=[],
        bcc_addresses=[],
        reply_to_addresses=[],
        sender_addresses=[],
        body_text=f"body-{uid}",
        body_html="",
        snippet=f"body-{uid}",
        headers={"Subject": [f"subject-{uid}"]},
        has_attachments=False,
        attachment_count=0,
        attachments=[],
    )


class SyncWorkersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name)
        self.settings = Settings(
            account_email="demo@qq.com",
            auth_code="auth-code",
            imap_host="imap.qq.com",
            imap_port=993,
            data_dir=self.base_path / "data",
            db_path=self.base_path / "data" / "email_sync.db",
            raw_dir=self.base_path / "data" / "raw",
            mail_folders=None,
        )
        self.settings.ensure_directories()
        self.db = Database(self.settings.db_path)
        self.db.init_schema()
        self.mailbox = MailboxInfo(
            display_name="INBOX",
            imap_name="INBOX",
            delimiter="/",
            flags=["\\HasNoChildren"],
        )

    def tearDown(self) -> None:
        self.db.close()
        self.temp_dir.cleanup()

    def test_parallel_sync_persists_messages_and_records_errors(self) -> None:
        def fake_fetch_uid_batch(settings, mailbox, uidvalidity, uid_batch):
            successes = []
            errors = []
            for uid in uid_batch:
                if uid == 4:
                    errors.append((uid, "mock fetch failure"))
                else:
                    successes.append(FetchedMessage(parsed=_make_parsed(uid), raw_bytes=f"raw-{uid}".encode("utf-8")))
            return successes, errors

        with patch("email_sync.imap_sync.connect", side_effect=lambda settings: _FakeImapClient()):
            with patch("email_sync.imap_sync.discover_mailboxes", return_value=[self.mailbox]):
                with patch("email_sync.imap_sync._search_uids", return_value=[1, 2, 3, 4]):
                    with patch("email_sync.imap_sync.PARALLEL_FETCH_BATCH_SIZE", 2):
                        with patch("email_sync.imap_sync._fetch_uid_batch", side_effect=fake_fetch_uid_batch):
                            results = sync_mailboxes(self.settings, self.db, requested_folders=["INBOX"], workers=2)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].folder_name, "INBOX")
        self.assertEqual(results[0].fetched, 3)
        self.assertEqual(results[0].last_seen_uid, 3)

        stats = self.db.fetch_stats()
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]["message_count"], 3)

        error_count = self.db.conn.execute("SELECT COUNT(*) FROM sync_errors").fetchone()[0]
        self.assertEqual(error_count, 1)

        state = self.db.get_sync_state("demo@qq.com", "INBOX")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["last_run_synced"], 3)
        self.assertEqual(state["last_seen_uid"], 3)

        raw_files = sorted(path.name for path in self.settings.raw_dir.rglob("*.eml"))
        self.assertEqual(raw_files, ["1001_1.eml", "1001_2.eml", "1001_3.eml"])


if __name__ == "__main__":
    unittest.main()
