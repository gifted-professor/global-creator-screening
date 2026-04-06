from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from email_sync.db import Database
from email_sync.thread_assignments import (
    lookup_thread_assignment,
    persist_thread_assignments_from_keep_workbook,
)


class ThreadAssignmentsTests(unittest.TestCase):
    def _write_keep_workbook(self, path: Path, rows: list[dict[str, object]]) -> None:
        workbook = Workbook()
        sheet = workbook.active
        headers = [
            "thread_key",
            "final_id_final",
            "Platform",
            "matched_contact_email",
            "subject",
            "resolution_confidence_final",
            "last_mail_message_id",
            "last_mail_time",
        ]
        sheet.append(headers)
        for row in rows:
            sheet.append([row.get(header, "") for header in headers])
        workbook.save(path)

    def test_persist_thread_assignments_scopes_rows_by_owner_and_skips_low_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db = Database(root / "email_sync.db")
            db.init_schema()
            keep_path = root / "final_keep.xlsx"
            self._write_keep_workbook(
                keep_path,
                [
                    {
                        "thread_key": "mid:<alpha-root>",
                        "final_id_final": "alpha",
                        "Platform": "Instagram",
                        "matched_contact_email": "alpha@example.com",
                        "subject": "Re: Alpha outreach",
                        "resolution_confidence_final": "high",
                        "last_mail_message_id": "101",
                        "last_mail_time": "2026-04-05T10:00:00+08:00",
                    },
                    {
                        "thread_key": "mid:<beta-root>",
                        "final_id_final": "beta",
                        "Platform": "TikTok",
                        "matched_contact_email": "beta@example.com",
                        "subject": "Re: Beta outreach",
                        "resolution_confidence_final": "low",
                    },
                ],
            )

            first = persist_thread_assignments_from_keep_workbook(
                db=db,
                keep_workbook_path=keep_path,
                brand="MINISO",
                owner_scope="ou_owner_a",
                source_run_id="run-1",
            )
            second = persist_thread_assignments_from_keep_workbook(
                db=db,
                keep_workbook_path=keep_path,
                brand="MINISO",
                owner_scope="ou_owner_b",
                source_run_id="run-2",
            )
            rows = list(
                db.conn.execute(
                    """
                    SELECT thread_key, owner_scope, creator_id, platform, matched_contact_email, normalized_subject
                    FROM thread_assignments
                    ORDER BY owner_scope, thread_key
                    """
                ).fetchall()
            )
            db.close()

        self.assertEqual(first["upserted_count"], 1)
        self.assertEqual(first["skipped_count"], 1)
        self.assertEqual(second["upserted_count"], 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["thread_key"], "mid:<alpha-root>")
        self.assertEqual(rows[0]["owner_scope"], "ou_owner_a")
        self.assertEqual(rows[1]["owner_scope"], "ou_owner_b")
        self.assertEqual(rows[0]["normalized_subject"], "alpha outreach")

    def test_persist_thread_assignments_increments_revision_when_latest_mail_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db = Database(root / "email_sync.db")
            db.init_schema()
            keep_path = root / "final_keep.xlsx"
            self._write_keep_workbook(
                keep_path,
                [
                    {
                        "thread_key": "mid:<alpha-root>",
                        "final_id_final": "alpha",
                        "Platform": "Instagram",
                        "matched_contact_email": "alpha@example.com",
                        "subject": "Re: Alpha outreach",
                        "resolution_confidence_final": "high",
                        "last_mail_message_id": "101",
                        "last_mail_time": "2026-04-05T10:00:00+08:00",
                    }
                ],
            )
            persist_thread_assignments_from_keep_workbook(
                db=db,
                keep_workbook_path=keep_path,
                brand="MINISO",
                owner_scope="ou_owner_a",
                source_run_id="run-1",
            )
            self._write_keep_workbook(
                keep_path,
                [
                    {
                        "thread_key": "mid:<alpha-root>",
                        "final_id_final": "alpha",
                        "Platform": "Instagram",
                        "matched_contact_email": "alpha@example.com",
                        "subject": "Re: Alpha outreach",
                        "resolution_confidence_final": "high",
                        "last_mail_message_id": "102",
                        "last_mail_time": "2026-04-06T10:00:00+08:00",
                    }
                ],
            )
            persist_thread_assignments_from_keep_workbook(
                db=db,
                keep_workbook_path=keep_path,
                brand="MINISO",
                owner_scope="ou_owner_a",
                source_run_id="run-2",
            )
            row = db.conn.execute(
                """
                SELECT last_mail_message_id, last_mail_sent_at, mail_update_revision
                FROM thread_assignments
                WHERE thread_key = ? AND owner_scope = ?
                """,
                ("mid:<alpha-root>", "ou_owner_a"),
            ).fetchone()
            db.close()

        self.assertEqual(row["last_mail_message_id"], "102")
        self.assertEqual(row["last_mail_sent_at"], "2026-04-06T10:00:00+08:00")
        self.assertEqual(int(row["mail_update_revision"]), 2)

    def test_lookup_thread_assignment_requires_guard_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db_path = root / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            keep_path = root / "final_keep.xlsx"
            self._write_keep_workbook(
                keep_path,
                [
                    {
                        "thread_key": "mid:<alpha-root>",
                        "final_id_final": "alpha",
                        "Platform": "Instagram",
                        "matched_contact_email": "alpha@example.com",
                        "subject": "Re: Alpha outreach",
                        "resolution_confidence_final": "high",
                        "last_mail_message_id": "101",
                        "last_mail_time": "2026-04-05T10:00:00+08:00",
                    }
                ],
            )
            persist_thread_assignments_from_keep_workbook(
                db=db,
                keep_workbook_path=keep_path,
                brand="MINISO",
                owner_scope="ou_owner_a",
                source_run_id="run-1",
            )
            db.close()

            matched = lookup_thread_assignment(
                db_path=db_path,
                owner_scope="ou_owner_a",
                creator_id="alpha",
                platform="instagram",
                brand="MINISO",
                matched_contact_email="alpha@example.com",
            )
            missed = lookup_thread_assignment(
                db_path=db_path,
                owner_scope="ou_owner_a",
                creator_id="alpha",
                platform="instagram",
                brand="MINISO",
                matched_contact_email="wrong@example.com",
                subject="different subject",
            )

        self.assertIsNotNone(matched)
        self.assertEqual(matched["thread_key"], "mid:<alpha-root>")
        self.assertEqual(matched["match_reason"], "contact")
        self.assertIsNone(missed)
