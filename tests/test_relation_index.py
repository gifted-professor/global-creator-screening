from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from email_sync.db import Database
from email_sync.mail_parser import ParsedMessage
from email_sync.relation_index import rebuild_relation_index


def _parsed_message(
    *,
    uid: int,
    message_id: str,
    subject: str,
    sent_at: str,
    from_address: str,
    to_address: str,
    in_reply_to: str | None = None,
    references_header: str | None = None,
    account_email: str = "demo@qq.com",
    folder_name: str | None = None,
) -> ParsedMessage:
    resolved_folder_name = folder_name
    if resolved_folder_name is None:
        resolved_folder_name = "INBOX" if from_address != account_email else "Sent Messages"
    return ParsedMessage(
        account_email=account_email,
        folder_name=resolved_folder_name,
        uid=uid,
        uidvalidity=1001,
        message_id=message_id,
        subject=subject,
        in_reply_to=in_reply_to,
        references_header=references_header,
        sent_at=sent_at,
        sent_at_raw=sent_at,
        internal_date=sent_at,
        internal_date_raw=sent_at,
        flags=["\\Seen"],
        size_bytes=100,
        from_addresses=[{"name": "", "address": from_address}],
        to_addresses=[{"name": "", "address": to_address}],
        cc_addresses=[],
        bcc_addresses=[],
        reply_to_addresses=[],
        sender_addresses=[],
        body_text=subject,
        body_html="",
        snippet=subject,
        headers={"Subject": [subject]},
        has_attachments=False,
        attachment_count=0,
        attachments=[],
    )


class RelationIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "relations.db")
        self.db.init_schema()

        root = _parsed_message(
            uid=1,
            message_id="<root-1@example.com>",
            subject="Creator Outreach",
            sent_at="2026-03-15T09:00:00+08:00",
            from_address="demo@qq.com",
            to_address="creator1@example.com",
        )
        reply = _parsed_message(
            uid=2,
            message_id="<reply-1@example.com>",
            subject="Re: Creator Outreach",
            sent_at="2026-03-15T10:00:00+08:00",
            from_address="creator1@example.com",
            to_address="demo@qq.com",
            in_reply_to="<root-1@example.com>",
            references_header="<root-1@example.com>",
        )
        another_root = _parsed_message(
            uid=3,
            message_id="<root-2@example.com>",
            subject="Creator Outreach",
            sent_at="2026-03-16T09:00:00+08:00",
            from_address="demo@qq.com",
            to_address="creator2@example.com",
        )

        self.db.upsert_message(root, "raw/root.eml", "sha-root", 100)
        self.db.upsert_message(reply, "raw/reply.eml", "sha-reply", 100)
        self.db.upsert_message(another_root, "raw/another.eml", "sha-another", 100)

    def tearDown(self) -> None:
        self.db.close()
        self.temp_dir.cleanup()

    def test_rebuild_relation_index_groups_same_reply_chain(self) -> None:
        stats = rebuild_relation_index(self.db)

        self.assertEqual(stats["messages_indexed"], 3)
        self.assertEqual(stats["contacts"], 2)
        self.assertEqual(stats["threads"], 2)

        contacts = self.db.fetch_contacts(limit=10)
        self.assertEqual(contacts[0]["email_normalized"], "creator1@example.com")
        self.assertEqual(contacts[0]["message_count"], 2)
        self.assertEqual(contacts[0]["thread_count"], 1)

        creator1_threads = self.db.fetch_threads(limit=10, contact_email="creator1@example.com")
        self.assertEqual(len(creator1_threads), 1)
        self.assertEqual(creator1_threads[0]["message_count"], 2)
        self.assertEqual(creator1_threads[0]["normalized_subject"], "creator outreach")

        thread_messages = self.db.fetch_thread_messages("mid:<root-1@example.com>")
        self.assertEqual(len(thread_messages), 2)
        self.assertEqual(thread_messages[0]["direction"], "outbound")
        self.assertEqual(thread_messages[1]["direction"], "inbound")
        self.assertEqual(thread_messages[1]["thread_depth"], 1)

    def test_same_subject_but_different_contacts_do_not_merge(self) -> None:
        rebuild_relation_index(self.db)

        threads = self.db.fetch_threads(limit=10, subject_contains="creator outreach")
        thread_keys = {row["thread_key"] for row in threads}

        self.assertEqual(len(threads), 2)
        self.assertEqual(thread_keys, {"mid:<root-1@example.com>", "mid:<root-2@example.com>"})

    def test_rebuild_relation_index_treats_internal_alias_domain_as_self(self) -> None:
        alias_db = Database(Path(self.temp_dir.name) / "alias-relations.db")
        alias_db.init_schema()
        try:
            outbound = _parsed_message(
                uid=1,
                account_email="partnerships@amagency.biz",
                folder_name="其他文件夹/邮件备份",
                message_id="<alias-root@example.com>",
                subject="Duet Outreach",
                sent_at="2026-04-01T09:00:00+08:00",
                from_address="yvette@amagency.biz",
                to_address="creator@example.com",
            )
            inbound = _parsed_message(
                uid=2,
                account_email="partnerships@amagency.biz",
                folder_name="其他文件夹/邮件备份",
                message_id="<alias-reply@example.com>",
                subject="Re: Duet Outreach",
                sent_at="2026-04-01T10:00:00+08:00",
                from_address="creator@example.com",
                to_address="astrid@amagency.biz",
                in_reply_to="<alias-root@example.com>",
                references_header="<alias-root@example.com>",
            )
            alias_db.upsert_message(outbound, "raw/alias-outbound.eml", "sha-alias-outbound", 100)
            alias_db.upsert_message(inbound, "raw/alias-inbound.eml", "sha-alias-inbound", 100)

            rebuild_relation_index(alias_db)

            thread_messages = alias_db.fetch_thread_messages("mid:<alias-root@example.com>")
            self.assertEqual(len(thread_messages), 2)
            self.assertEqual(thread_messages[0]["direction"], "outbound")
            self.assertEqual(thread_messages[1]["direction"], "inbound")
        finally:
            alias_db.close()


if __name__ == "__main__":
    unittest.main()
