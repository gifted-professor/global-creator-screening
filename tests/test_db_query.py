from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from email_sync.db import Database, MessageQuery
from email_sync.mail_parser import AttachmentMetadata, ParsedMessage


class DatabaseQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "query.db")
        self.db.init_schema()

        first = ParsedMessage(
            account_email="demo@qq.com",
            folder_name="INBOX",
            uid=1,
            uidvalidity=1001,
            message_id="<1@example.com>",
            subject="订单确认",
            in_reply_to=None,
            references_header=None,
            sent_at="2026-03-15T09:00:00+08:00",
            sent_at_raw="2026-03-15T09:00:00+08:00",
            internal_date="2026-03-15T09:00:00+08:00",
            internal_date_raw="2026-03-15T09:00:00+08:00",
            flags=["\\Seen"],
            size_bytes=100,
            from_addresses=[{"name": "Shop", "address": "shop@example.com"}],
            to_addresses=[{"name": "", "address": "demo@qq.com"}],
            cc_addresses=[],
            bcc_addresses=[],
            reply_to_addresses=[],
            sender_addresses=[],
            body_text="你的订单已经付款成功。",
            body_html="",
            snippet="你的订单已经付款成功。",
            headers={"Subject": ["订单确认"]},
            has_attachments=False,
            attachment_count=0,
            attachments=[],
        )

        second = ParsedMessage(
            account_email="demo@qq.com",
            folder_name="Sent Messages",
            uid=2,
            uidvalidity=1001,
            message_id="<2@example.com>",
            subject="周报附件",
            in_reply_to=None,
            references_header=None,
            sent_at="2026-03-16T11:30:00+08:00",
            sent_at_raw="2026-03-16T11:30:00+08:00",
            internal_date="2026-03-16T11:30:00+08:00",
            internal_date_raw="2026-03-16T11:30:00+08:00",
            flags=["\\Seen"],
            size_bytes=200,
            from_addresses=[{"name": "Me", "address": "demo@qq.com"}],
            to_addresses=[{"name": "Boss", "address": "boss@example.com"}],
            cc_addresses=[],
            bcc_addresses=[],
            reply_to_addresses=[],
            sender_addresses=[],
            body_text="这是周报，请查收。",
            body_html="",
            snippet="这是周报，请查收。",
            headers={"Subject": ["周报附件"]},
            has_attachments=True,
            attachment_count=1,
            attachments=[
                AttachmentMetadata(
                    part_index=1,
                    filename="weekly-report.xlsx",
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    size_bytes=1234,
                    content_id=None,
                    content_disposition="attachment",
                    is_inline=False,
                )
            ],
        )

        third = ParsedMessage(
            account_email="demo@qq.com",
            folder_name="INBOX",
            uid=3,
            uidvalidity=1001,
            message_id="<3@example.com>",
            subject="会议提醒",
            in_reply_to=None,
            references_header=None,
            sent_at="2026-03-17T08:00:00+08:00",
            sent_at_raw="2026-03-17T08:00:00+08:00",
            internal_date="2026-03-17T08:00:00+08:00",
            internal_date_raw="2026-03-17T08:00:00+08:00",
            flags=["\\Seen"],
            size_bytes=150,
            from_addresses=[{"name": "Calendar", "address": "calendar@example.com"}],
            to_addresses=[{"name": "", "address": "demo@qq.com"}],
            cc_addresses=[],
            bcc_addresses=[],
            reply_to_addresses=[],
            sender_addresses=[],
            body_text="今天下午两点开会。",
            body_html="",
            snippet="今天下午两点开会。",
            headers={"Subject": ["会议提醒"]},
            has_attachments=False,
            attachment_count=0,
            attachments=[],
        )

        self.db.upsert_message(first, "raw/a.eml", "sha-a", 100)
        self.db.upsert_message(second, "raw/b.eml", "sha-b", 200)
        self.db.upsert_message(third, "raw/c.eml", "sha-c", 150)

    def tearDown(self) -> None:
        self.db.close()
        self.temp_dir.cleanup()

    def test_keyword_search_matches_subject_and_body(self) -> None:
        by_subject = self.db.search_messages(MessageQuery(keyword="会议", limit=10))
        by_body = self.db.search_messages(MessageQuery(keyword="付款成功", limit=10))

        self.assertEqual(len(by_subject), 1)
        self.assertEqual(by_subject[0]["subject"], "会议提醒")
        self.assertEqual(len(by_body), 1)
        self.assertEqual(by_body[0]["subject"], "订单确认")

    def test_attachment_filters_work(self) -> None:
        with_attachments = self.db.search_messages(MessageQuery(has_attachments=True, limit=10))
        by_attachment_name = self.db.search_messages(MessageQuery(attachment_name="report", limit=10))

        self.assertEqual(len(with_attachments), 1)
        self.assertEqual(with_attachments[0]["folder_name"], "Sent Messages")
        self.assertEqual(len(by_attachment_name), 1)
        self.assertEqual(by_attachment_name[0]["subject"], "周报附件")

    def test_folder_sender_and_time_filters_work_together(self) -> None:
        rows = self.db.search_messages(
            MessageQuery(
                folders=["INBOX"],
                from_contains="calendar@example.com",
                sent_after="2026-03-17T00:00:00+08:00",
                sent_before="2026-03-18T00:00:00+08:00",
                limit=10,
            )
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["subject"], "会议提醒")


if __name__ == "__main__":
    unittest.main()
