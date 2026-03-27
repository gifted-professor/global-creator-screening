from __future__ import annotations

import unittest

from email_sync.imap_sync import MailboxInfo, _format_imap_date, _quote_mailbox_name, resolve_mailboxes


class ImapSyncTests(unittest.TestCase):
    def test_resolve_mailboxes_skips_noselect_case_insensitively(self) -> None:
        discovered = [
            MailboxInfo(display_name="INBOX", imap_name="INBOX", delimiter="/", flags=["\\HasNoChildren"]),
            MailboxInfo(display_name="其他文件夹", imap_name="&UXZO1mWHTvZZOQ-", delimiter="/", flags=["\\NoSelect", "\\HasChildren"]),
            MailboxInfo(display_name="Sent Messages", imap_name="Sent Messages", delimiter="/", flags=["\\HasNoChildren"]),
        ]

        selectable = resolve_mailboxes(discovered, None)

        self.assertEqual([item.display_name for item in selectable], ["INBOX", "Sent Messages"])

    def test_quote_mailbox_name_handles_spaces_and_quotes(self) -> None:
        self.assertEqual(_quote_mailbox_name("Sent Messages"), '"Sent Messages"')
        self.assertEqual(_quote_mailbox_name('My "Folder"'), '"My \\"Folder\\""')

    def test_format_imap_date(self) -> None:
        from datetime import date

        self.assertEqual(_format_imap_date(date(2025, 12, 27)), "27-Dec-2025")


if __name__ == "__main__":
    unittest.main()
