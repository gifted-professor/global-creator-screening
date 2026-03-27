from __future__ import annotations

import unittest

from email_sync.mail_parser import parse_email_message


SAMPLE_EMAIL = (
    b"From: =?UTF-8?B?5rWL6K+V?= <sender@example.com>\r\n"
    b"To: receiver@example.com\r\n"
    b"Cc: copy@example.com\r\n"
    b"Subject: =?UTF-8?B?5rWL6K+V5qCH6aKY?=\r\n"
    b"Message-ID: <abc123@example.com>\r\n"
    b"Date: Tue, 17 Mar 2026 09:00:00 +0800\r\n"
    b"MIME-Version: 1.0\r\n"
    b"Content-Type: multipart/mixed; boundary=\"mix\"\r\n"
    b"\r\n"
    b"--mix\r\n"
    b"Content-Type: multipart/alternative; boundary=\"alt\"\r\n"
    b"\r\n"
    b"--alt\r\n"
    b"Content-Type: text/plain; charset=\"utf-8\"\r\n"
    b"\r\n"
    b"hello world\r\n"
    b"--alt\r\n"
    b"Content-Type: text/html; charset=\"utf-8\"\r\n"
    b"\r\n"
    b"<html><body><p>hello <b>world</b></p></body></html>\r\n"
    b"--alt--\r\n"
    b"--mix\r\n"
    b"Content-Type: text/plain; name=\"note.txt\"\r\n"
    b"Content-Disposition: attachment; filename=\"note.txt\"\r\n"
    b"Content-Transfer-Encoding: base64\r\n"
    b"\r\n"
    b"aGVsbG8gYXR0YWNobWVudA==\r\n"
    b"--mix--\r\n"
)

MALFORMED_MESSAGE_ID_EMAIL = (
    b"From: sender@example.com\r\n"
    b"To: receiver@example.com\r\n"
    b"Subject: malformed message id\r\n"
    b"Message-ID: [475d821668ce4b13bbc88d2aa15b35bf@example.com]>\r\n"
    b"Date: Tue, 17 Mar 2026 09:00:00 +0800\r\n"
    b"Content-Type: text/plain; charset=\"utf-8\"\r\n"
    b"\r\n"
    b"body\r\n"
)


class MailParserTests(unittest.TestCase):
    def test_parse_message_core_fields(self) -> None:
        parsed = parse_email_message(
            raw_bytes=SAMPLE_EMAIL,
            account_email="demo@qq.com",
            folder_name="INBOX",
            uid=42,
            uidvalidity=1001,
            flags=["\\Seen"],
            internal_date_raw="17-Mar-2026 09:00:00 +0800",
            size_bytes=len(SAMPLE_EMAIL),
        )

        self.assertEqual(parsed.account_email, "demo@qq.com")
        self.assertEqual(parsed.folder_name, "INBOX")
        self.assertEqual(parsed.uid, 42)
        self.assertEqual(parsed.uidvalidity, 1001)
        self.assertEqual(parsed.message_id, "<abc123@example.com>")
        self.assertEqual(parsed.subject, "测试标题")
        self.assertEqual(parsed.from_addresses[0]["address"], "sender@example.com")
        self.assertEqual(parsed.to_addresses[0]["address"], "receiver@example.com")
        self.assertEqual(parsed.cc_addresses[0]["address"], "copy@example.com")
        self.assertIn("hello world", parsed.body_text)
        self.assertIn("<b>world</b>", parsed.body_html)
        self.assertTrue(parsed.has_attachments)
        self.assertEqual(parsed.attachment_count, 1)
        self.assertEqual(parsed.attachments[0].filename, "note.txt")
        self.assertEqual(parsed.attachments[0].size_bytes, len(b"hello attachment"))
        self.assertIn("Subject", parsed.headers)

    def test_parse_message_tolerates_malformed_message_id(self) -> None:
        parsed = parse_email_message(
            raw_bytes=MALFORMED_MESSAGE_ID_EMAIL,
            account_email="demo@qq.com",
            folder_name="INBOX",
            uid=99,
            uidvalidity=1001,
            flags=[],
            internal_date_raw=None,
            size_bytes=len(MALFORMED_MESSAGE_ID_EMAIL),
        )

        self.assertEqual(parsed.message_id, "[475d821668ce4b13bbc88d2aa15b35bf@example.com]>")
        self.assertEqual(parsed.subject, "malformed message id")
        self.assertEqual(parsed.body_text, "body")


if __name__ == "__main__":
    unittest.main()
