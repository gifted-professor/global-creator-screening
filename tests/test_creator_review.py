from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from openpyxl import Workbook

from email_sync.creator_review import (
    _parse_review_response,
    prepare_duplicate_review,
    review_duplicate_groups,
)
from email_sync.db import Database
from email_sync.relation_index import rebuild_relation_index


def _addresses(*items: tuple[str, str]) -> str:
    return json.dumps([{"name": name, "address": address} for name, address in items], ensure_ascii=False)


class CreatorReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp(prefix="creator_review_test_"))
        self.input_path = self.temp_dir / "input.xlsx"
        self.output_prefix = self.temp_dir / "review_sample"
        self.db_path = self.temp_dir / "email_sync.db"

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _make_workbook(self) -> None:
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "results"
        headers = [
            "nickname",
            "@username",
            "Platform",
            "Email",
            "URL",
            "matched_contact_email",
            "match_confidence",
            "last_mail_message_id",
            "last_mail_subject",
            "last_mail_snippet",
            "last_mail_raw_path",
            "evidence_thread_key",
        ]
        sheet.append(headers)
        sheet.append(
            [
                "Jessica Blair",
                "lovejessicablair",
                "YOUTUBE",
                "kat@lovejessicablair.com",
                "https://youtube.com/@lovejessicablair",
                "kat@lovejessicablair.com",
                "high",
                2,
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "Thank you for thinking of Jess for this opportunity!",
                "raw/2.eml",
                "mid:<m1>",
            ]
        )
        sheet.append(
            [
                "Kat",
                "katbendarez",
                "TIKTOK",
                "",
                "https://tiktok.com/@katbendarez",
                "kat@lovejessicablair.com",
                "high",
                2,
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "Thank you for thinking of Jess for this opportunity!",
                "raw/2.eml",
                "mid:<m1>",
            ]
        )
        sheet.append(
            [
                "Solo Creator",
                "soloone",
                "TIKTOK",
                "solo@example.com",
                "https://tiktok.com/@soloone",
                "solo@example.com",
                "high",
                3,
                "Solo opportunity",
                "Solo creator follow-up",
                "raw/3.eml",
                "mid:<m3>",
            ]
        )
        sheet.append(
            [
                "Eva",
                "evarywhere",
                "TIKTOK",
                "eva@wearetrival.com",
                "https://tiktok.com/@evarywhere",
                "eva@wearetrival.com",
                "high",
                "",
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "Wanted to check in on Eva's rates and media kit.",
                "raw/4.eml",
                "mid:<m3>",
            ]
        )
        sheet.append(
            [
                "Eva Diamond",
                "evadiamondj",
                "TIKTOK",
                "tg@wigginshair.com",
                "https://tiktok.com/@evadiamondj",
                "eva@wearetrival.com",
                "high",
                "",
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "Wanted to check in on Eva's rates and media kit.",
                "raw/4.eml",
                "mid:<m3>",
            ]
        )
        workbook.save(self.input_path)

    def _seed_messages(self) -> Database:
        db = Database(self.db_path)
        db.init_schema()
        rows = [
            (
                "william@amagency.biz",
                "INBOX",
                1,
                1,
                "<m1>",
                "Paid Collab with MINISO – In-Store Check-In Campaign",
                None,
                None,
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T10:00:00+00:00",
                "[]",
                100,
                _addresses(("Eden", "eden@amagency.biz")),
                _addresses(("Kat", "kat@lovejessicablair.com")),
                "[]",
                "[]",
                "[]",
                "[]",
                "Hi Jess team, can you share your rates for MINISO?",
                "",
                "Hi Jess team, can you share your rates for MINISO?",
                "{}",
                "raw/1.eml",
                "sha1",
                100,
                0,
                0,
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T10:00:00+00:00",
            ),
            (
                "william@amagency.biz",
                "INBOX",
                2,
                1,
                "<m2>",
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "<m1>",
                "<m1>",
                "2025-01-02T10:00:00+00:00",
                "2025-01-02T10:00:00+00:00",
                "2025-01-02T10:00:00+00:00",
                "2025-01-02T10:00:00+00:00",
                "[]",
                100,
                _addresses(("Kat", "kat@lovejessicablair.com")),
                _addresses(("Eden", "eden@amagency.biz")),
                "[]",
                "[]",
                "[]",
                "[]",
                "Thank you for thinking of Jess for this opportunity! She is interested.",
                "",
                "Thank you for thinking of Jess for this opportunity!",
                "{}",
                "raw/2.eml",
                "sha2",
                100,
                0,
                0,
                "2025-01-02T10:00:00+00:00",
                "2025-01-02T10:00:00+00:00",
            ),
            (
                "william@amagency.biz",
                "INBOX",
                3,
                1,
                "<m3>",
                "Paid Collab with MINISO – In-Store Check-In Campaign",
                None,
                None,
                "2025-01-03T10:00:00+00:00",
                "2025-01-03T10:00:00+00:00",
                "2025-01-03T10:00:00+00:00",
                "2025-01-03T10:00:00+00:00",
                "[]",
                100,
                _addresses(("Eden", "eden@amagency.biz")),
                _addresses(("Eva", "eva@wearetrival.com")),
                "[]",
                "[]",
                "[]",
                "[]",
                "Hi Eva team, following up on the MINISO brief.",
                "",
                "Hi Eva team, following up on the MINISO brief.",
                "{}",
                "raw/3.eml",
                "sha3",
                100,
                0,
                0,
                "2025-01-03T10:00:00+00:00",
                "2025-01-03T10:00:00+00:00",
            ),
            (
                "william@amagency.biz",
                "INBOX",
                4,
                1,
                "<m4>",
                "Re: Paid Collab with MINISO – In-Store Check-In Campaign",
                "<m3>",
                "<m3>",
                "2025-01-04T10:00:00+00:00",
                "2025-01-04T10:00:00+00:00",
                "2025-01-04T10:00:00+00:00",
                "2025-01-04T10:00:00+00:00",
                "[]",
                100,
                _addresses(("Gian", "gian@wearetrival.com")),
                _addresses(("Eden", "eden@amagency.biz")),
                "[]",
                "[]",
                "[]",
                "[]",
                "Wanted to check in on Eva's rates and media kit.",
                "",
                "Wanted to check in on Eva's rates and media kit.",
                "{}",
                "raw/4.eml",
                "sha4",
                100,
                0,
                0,
                "2025-01-04T10:00:00+00:00",
                "2025-01-04T10:00:00+00:00",
            ),
        ]
        db.conn.executemany(
            """
            INSERT INTO messages (
                account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                has_attachments, attachment_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        db.conn.commit()
        rebuild_relation_index(db)
        return db

    def test_prepare_duplicate_review_groups_by_message_id_and_raw_path(self) -> None:
        self._make_workbook()
        db = self._seed_messages()
        try:
            result = prepare_duplicate_review(db, self.input_path, self.output_prefix, sample_limit=5)
        finally:
            db.close()

        self.assertEqual(result["stats"]["total_rows"], 5)
        self.assertEqual(result["stats"]["group_count"], 3)
        self.assertEqual(result["stats"]["singleton_group_count"], 1)
        self.assertEqual(result["stats"]["duplicate_group_count"], 2)
        self.assertEqual(result["stats"]["duplicate_row_count"], 4)
        self.assertEqual(result["selected_group_count"], 2)
        self.assertEqual(
            result["selected_group_keys"],
            ["last_mail_message_id:2", "last_mail_raw_path:raw/4.eml"],
        )

        groups_payload = json.loads((self.output_prefix.with_name(f"{self.output_prefix.name}_groups.json")).read_text(encoding="utf-8"))
        self.assertEqual(groups_payload["selected_group_count"], 2)
        first_group = groups_payload["groups"][0]
        self.assertEqual(first_group["group_key"], "last_mail_message_id:2")
        self.assertEqual(first_group["candidate_count"], 2)
        self.assertEqual(first_group["evidence"]["message_row_id"], 2)
        self.assertEqual(first_group["evidence"]["thread_message_count"], 2)
        self.assertEqual(first_group["rows"][0]["nickname"], "Jessica Blair")

        second_group = groups_payload["groups"][1]
        self.assertEqual(second_group["group_key"], "last_mail_raw_path:raw/4.eml")
        self.assertEqual(second_group["evidence"]["message_row_id"], 4)
        self.assertEqual(second_group["evidence"]["thread_message_count"], 2)

    def test_prepare_duplicate_review_respects_sample_limit(self) -> None:
        self._make_workbook()
        db = self._seed_messages()
        try:
            result = prepare_duplicate_review(db, self.input_path, self.output_prefix, sample_limit=1)
        finally:
            db.close()

        self.assertEqual(result["selected_group_count"], 1)
        self.assertEqual(result["selected_group_keys"], ["last_mail_message_id:2"])

    def test_prepare_duplicate_review_accepts_explicit_group_keys(self) -> None:
        self._make_workbook()
        db = self._seed_messages()
        try:
            result = prepare_duplicate_review(
                db,
                self.input_path,
                self.output_prefix,
                sample_limit=1,
                group_keys=["last_mail_raw_path:raw/4.eml"],
            )
        finally:
            db.close()

        self.assertEqual(result["selection_mode"], "explicit_group_keys")
        self.assertEqual(result["selected_group_count"], 1)
        self.assertEqual(result["selected_group_keys"], ["last_mail_raw_path:raw/4.eml"])

    def test_parse_review_response_downgrades_invalid_match_one(self) -> None:
        parsed = _parse_review_response(
            json.dumps(
                {
                    "decision": "match_one",
                    "selected_candidate_ids": ["results:2", "results:3"],
                    "reason": "too many selected",
                    "confidence": "high",
                },
                ensure_ascii=False,
            ),
            {"results:2", "results:3"},
        )
        self.assertEqual(parsed["decision"], "uncertain")
        self.assertEqual(parsed["selected_candidate_ids"], [])

    def test_review_duplicate_groups_writes_audit_and_row_annotations(self) -> None:
        self._make_workbook()
        db = self._seed_messages()
        try:
            with mock.patch(
                "email_sync.creator_review._invoke_duplicate_review_llm",
                side_effect=[
                    {
                        "decision": "match_one",
                        "selected_candidate_ids": ["results:2"],
                        "reason": "Jessica Blair is named directly in the mail.",
                        "confidence": "high",
                        "raw_text": '{"decision":"match_one"}',
                    },
                    {
                        "decision": "reject_group",
                        "selected_candidate_ids": [],
                        "reason": "No candidate is specific enough.",
                        "confidence": "low",
                        "raw_text": '{"decision":"reject_group"}',
                    },
                ],
            ):
                result = review_duplicate_groups(
                    db,
                    self.input_path,
                    self.output_prefix,
                    env_path=".env",
                    sample_limit=5,
                    base_url="https://example.com/v1",
                    api_key="sk-test",
                    model="gpt-test",
                )
        finally:
            db.close()

        self.assertEqual(result["selected_group_count"], 2)
        self.assertTrue(Path(result["audit_json_path"]).exists())
        self.assertTrue(Path(result["annotated_csv_path"]).exists())
        self.assertTrue(Path(result["annotated_xlsx_path"]).exists())

        audit_payload = json.loads(Path(result["audit_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(len(audit_payload["groups"]), 2)
        self.assertEqual(audit_payload["groups"][0]["decision"], "match_one")
        self.assertEqual(audit_payload["groups"][1]["decision"], "reject_group")

        with Path(result["annotated_csv_path"]).open("r", encoding="utf-8-sig", newline="") as handle:
            import csv

            rows = list(csv.DictReader(handle))

        by_nickname = {row["nickname"]: row for row in rows}
        self.assertEqual(by_nickname["Jessica Blair"]["review_decision"], "match_one")
        self.assertEqual(by_nickname["Jessica Blair"]["review_selected"], "yes")
        self.assertEqual(by_nickname["Kat"]["review_selected"], "no")
        self.assertEqual(by_nickname["Eva"]["review_decision"], "reject_group")
        self.assertEqual(by_nickname["Eva Diamond"]["review_selected"], "no")
        self.assertEqual(by_nickname["Solo Creator"]["review_decision"], "")


if __name__ == "__main__":
    unittest.main()
