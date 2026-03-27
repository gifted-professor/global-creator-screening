from __future__ import annotations

import unittest

from email_sync import imap_utf7


class ImapUtf7Tests(unittest.TestCase):
    def test_round_trip_with_ascii_and_chinese(self) -> None:
        original = "收件箱/项目A & Archive"
        encoded = imap_utf7.encode(original)
        decoded = imap_utf7.decode(encoded)

        self.assertEqual(decoded, original)

    def test_ampersand_special_case(self) -> None:
        self.assertEqual(imap_utf7.decode("&-"), "&")
        self.assertEqual(imap_utf7.encode("&"), "&-")


if __name__ == "__main__":
    unittest.main()
