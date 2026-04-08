from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from email_sync.db import Database
import scripts.run_shared_mailbox_post_sync_pipeline as pipeline


class SharedMailboxPostSyncPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_loader = pipeline._load_runtime_dependencies
        self.original_build_client = pipeline._build_feishu_client
        pipeline._read_thread_text_excerpt_cached.cache_clear()
        pipeline._read_thread_reply_snapshot_cached.cache_clear()
        pipeline._lookup_thread_key_for_raw_path_cached.cache_clear()

    def tearDown(self) -> None:
        pipeline._load_runtime_dependencies = self.original_loader
        pipeline._build_feishu_client = self.original_build_client
        pipeline._read_thread_text_excerpt_cached.cache_clear()
        pipeline._read_thread_reply_snapshot_cached.cache_clear()
        pipeline._lookup_thread_key_for_raw_path_cached.cache_clear()

    def test_build_parser_mail_first_only_defaults_enabled(self) -> None:
        parser = pipeline.build_parser()

        args = parser.parse_args(["--shared-mail-db-path", "/tmp/email_sync.db"])
        self.assertTrue(args.mail_first_only)
        self.assertFalse(args.thread_first_mail_resolution)
        self.assertEqual(args.positioning_provider, "")

        args = parser.parse_args(
            ["--shared-mail-db-path", "/tmp/email_sync.db", "--no-mail-first-only"]
        )
        self.assertFalse(args.mail_first_only)

        args = parser.parse_args(
            ["--shared-mail-db-path", "/tmp/email_sync.db", "--thread-first-mail-resolution"]
        )
        self.assertTrue(args.thread_first_mail_resolution)

        args = parser.parse_args(
            ["--shared-mail-db-path", "/tmp/email_sync.db", "--positioning-provider", "reelx"]
        )
        self.assertEqual(args.positioning_provider, "reelx")

    def test_deduplicate_prepared_candidates_by_mail_identity_skips_same_source_same_creator_across_batches(self) -> None:
        seen_identity_keys: set[str] = set()
        first_batch = pipeline._deduplicate_prepared_candidates_by_mail_identity(
            [
                {
                    "keep_row": {
                        "evidence_thread_key": "thread-alpha",
                        "last_mail_message_id": "msg-101",
                        "mail_update_revision": 2,
                    },
                    "creator_id": "alpha",
                    "platform": "instagram",
                    "thread_key": "thread-alpha",
                    "last_mail_message_id": "msg-101",
                    "mail_update_revision": 2,
                }
            ],
            seen_identity_keys=seen_identity_keys,
        )
        second_batch = pipeline._deduplicate_prepared_candidates_by_mail_identity(
            [
                {
                    "keep_row": {
                        "evidence_thread_key": "thread-alpha",
                        "last_mail_message_id": "msg-101",
                        "mail_update_revision": 2,
                    },
                    "creator_id": "alpha",
                    "platform": "instagram",
                    "thread_key": "thread-alpha",
                    "last_mail_message_id": "msg-101",
                    "mail_update_revision": 2,
                },
                {
                    "keep_row": {
                        "evidence_thread_key": "thread-alpha",
                        "last_mail_message_id": "msg-101",
                        "mail_update_revision": 2,
                    },
                    "creator_id": "beta",
                    "platform": "instagram",
                    "thread_key": "thread-alpha",
                    "last_mail_message_id": "msg-101",
                    "mail_update_revision": 2,
                },
            ],
            seen_identity_keys=seen_identity_keys,
        )

        self.assertEqual(first_batch["stats"]["deduplicated_count"], 0)
        self.assertEqual(len(first_batch["candidates"]), 1)
        self.assertEqual(second_batch["stats"]["deduplicated_count"], 1)
        self.assertEqual(len(second_batch["candidates"]), 1)
        self.assertEqual(second_batch["candidates"][0]["creator_id"], "beta")
        self.assertIn("last_mail_message_id=msg-101", second_batch["duplicate_rows"][0]["identity_label"])

    def test_apply_conservative_rescue_layer_upgrades_llm_medium_with_supporting_evidence(self) -> None:
        display_rows = [
            {
                "达人ID": "creatoralpha",
                "平台": "instagram",
                "ai是否通过": "否",
                "ai筛号反馈理由": "资料一般，暂不通过",
                "ai评价": "资料一般，暂不通过",
            }
        ]
        payload_rows = [
            {
                "达人ID": "creatoralpha",
                "平台": "instagram",
                "ai是否通过": "否",
                "ai筛号反馈理由": "资料一般，暂不通过",
                "ai评价": "资料一般，暂不通过",
                "latest_external_from": "creatoralpha@creatoralpha.studio",
                "latest_external_full_body": "Hello creatoralpha,\n\nPaid collab rates attached.\n\n> Hi creatoralpha",
                "resolution_stage_final": "llm",
                "resolution_confidence_final": "medium",
                "candidate_sources": '["greeting_quoted"]',
            }
        ]

        updated_display, updated_payload, stats = pipeline._apply_conservative_rescue_layer(
            display_rows=display_rows,
            payload_rows=payload_rows,
            source_keep_rows=[],
            task_name="Duet",
            task_scope="test-scope",
        )

        self.assertEqual(stats["r4_rescue_count"], 1)
        self.assertEqual(stats["rescued_to_manual_count"], 1)
        self.assertEqual(updated_payload[0]["ai是否通过"], "转人工")
        self.assertEqual(updated_payload[0]["final_decision"], "manual")
        self.assertEqual(updated_payload[0]["review_priority"], "high")
        self.assertEqual(updated_payload[0]["rescue_rule_applied"], "r4_medium_with_supporting_evidence")
        self.assertEqual(updated_payload[0]["hard_reject_blocked_rescue"], "false")
        self.assertEqual(updated_display[0]["ai是否通过"], "转人工")

    def test_apply_conservative_rescue_layer_blocks_hard_reject_rescue(self) -> None:
        display_rows = [
            {
                "达人ID": "creatoralpha",
                "平台": "instagram",
                "ai是否通过": "否",
                "ai筛号反馈理由": "scrape failed and blacklisted",
                "ai评价": "scrape failed and blacklisted",
            }
        ]
        payload_rows = [
            {
                "达人ID": "creatoralpha",
                "平台": "instagram",
                "ai是否通过": "否",
                "ai筛号反馈理由": "scrape failed and blacklisted",
                "ai评价": "scrape failed and blacklisted",
                "latest_external_from": "creatoralpha@studio.example",
                "latest_external_full_body": "Paid collab rates attached.",
                "business_signal_detected": "true",
                "resolution_stage_final": "llm",
                "resolution_confidence_final": "low",
            }
        ]

        updated_display, updated_payload, stats = pipeline._apply_conservative_rescue_layer(
            display_rows=display_rows,
            payload_rows=payload_rows,
            source_keep_rows=[],
            task_name="Duet",
            task_scope="test-scope",
        )

        self.assertEqual(stats["hard_reject_blocked_rescue_count"], 1)
        self.assertEqual(updated_payload[0]["ai是否通过"], "否")
        self.assertEqual(updated_payload[0]["hard_reject_blocked_rescue"], "true")
        self.assertEqual(updated_payload[0]["final_decision"], "reject")
        self.assertEqual(updated_display[0]["ai是否通过"], "否")

    def test_read_thread_text_excerpt_cache_invalidates_when_db_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            db.conn.execute(
                """
                INSERT INTO messages (
                    account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                    sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                    from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                    body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                    has_attachments, attachment_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', '', '', 0, 0, 0, ?, ?)
                """,
                (
                    "partnerships@amagency.biz",
                    "INBOX",
                    1,
                    1,
                    "<msg-1>",
                    "SKG outreach",
                    now,
                    now,
                    now,
                    now,
                    '[{"email":"rhea@amagency.biz","name":"Rhea"}]',
                    '[{"email":"creator@example.com","name":"alpha"}]',
                    "old body",
                    "old snippet",
                    now,
                    now,
                ),
            )
            message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            db.conn.execute(
                """
                INSERT INTO message_index (
                    message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                    thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_row_id,
                    "skg outreach",
                    "outbound",
                    "thread-cache",
                    "<msg-1>",
                    "",
                    0,
                    now,
                    1,
                    now,
                ),
            )
            db.conn.commit()

            first = pipeline._read_thread_text_excerpt(db_path, "thread-cache")
            self.assertIn("old snippet", first)

            db.conn.execute(
                "UPDATE messages SET snippet = ?, body_text = ?, updated_at = ? WHERE id = ?",
                ("new snippet", "new body", "2026-04-02T10:05:00+08:00", message_row_id),
            )
            db.conn.commit()
            future_ns = max(time.time_ns(), db_path.stat().st_mtime_ns + 1_000_000)
            os.utime(db_path, ns=(future_ns, future_ns))

            second = pipeline._read_thread_text_excerpt(db_path, "thread-cache")
            self.assertIn("new snippet", second)
            self.assertNotIn("old snippet", second)

            db.close()

    def test_apply_creator_reply_context_clears_outbound_only_threads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            db.conn.execute(
                """
                INSERT INTO messages (
                    account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                    sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                    from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                    body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                    has_attachments, attachment_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                """,
                (
                    "partnerships@amagency.biz",
                    "INBOX",
                    1,
                    1,
                    "<msg-1>",
                    "Duet outreach",
                    now,
                    now,
                    now,
                    now,
                    '[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                    '[{"email":"creator@example.com","name":"Creator"}]',
                    "Hi @Username",
                    "Hi @Username",
                    "raw/outbound.eml",
                    now,
                    now,
                ),
            )
            message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            db.conn.execute(
                """
                INSERT INTO message_index (
                    message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                    thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_row_id,
                    "duet outreach",
                    "outbound",
                    "thread-outbound",
                    "<msg-1>",
                    "",
                    0,
                    now,
                    1,
                    now,
                ),
            )
            db.conn.commit()
            db.close()

            keep_row, resolution = pipeline._apply_creator_reply_context(
                {
                    "evidence_thread_key": "thread-outbound",
                    "last_mail_message_id": "1",
                    "last_mail_time": "2026-04-02T10:00:00+08:00",
                    "last_mail_subject": "Duet outreach",
                    "last_mail_snippet": "Hi @Username",
                    "last_mail_raw_path": "raw/outbound.eml",
                },
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "outbound_only_or_no_reply")
            self.assertFalse(resolution["creator_replied"])
            self.assertEqual(keep_row["last_mail_message_id"], "")
            self.assertEqual(keep_row["last_mail_time"], "")
            self.assertEqual(keep_row["last_mail_subject"], "")
            self.assertEqual(keep_row["last_mail_snippet"], "")
            self.assertEqual(keep_row["last_mail_raw_path"], "")

    def test_apply_creator_reply_context_prefers_latest_inbound_reply_over_later_outbound_follow_up(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(*, uid: int, message_id: str, sent_at: str, direction: str, subject: str, from_json: str, to_json: str, snippet: str, raw_path: str) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        subject,
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-mixed",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                subject="Duet outreach",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Initial outreach",
                raw_path="raw/outbound-1.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply>",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                subject="Re: Duet outreach",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                snippet="My rate is $300",
                raw_path="raw/inbound-1.eml",
            )
            insert_message(
                uid=3,
                message_id="<msg-followup>",
                sent_at="2026-04-01T11:00:00+08:00",
                direction="outbound",
                subject="Re: Duet outreach",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Can we move to WhatsApp?",
                raw_path="raw/outbound-2.eml",
            )
            db.conn.commit()
            db.close()

            keep_row, resolution = pipeline._apply_creator_reply_context(
                {
                    "evidence_thread_key": "thread-mixed",
                    "last_mail_message_id": "3",
                    "last_mail_time": "2026-04-01T11:00:00+08:00",
                    "last_mail_subject": "Re: Duet outreach",
                    "last_mail_snippet": "Can we move to WhatsApp?",
                    "last_mail_raw_path": "raw/outbound-2.eml",
                },
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "creator_replied")
            self.assertTrue(resolution["creator_replied"])
            self.assertEqual(keep_row["last_mail_message_id"], 2)
            self.assertEqual(keep_row["last_mail_time"], "2026-04-01T10:00:00+08:00")
            self.assertEqual(keep_row["last_mail_subject"], "Re: Duet outreach")
            self.assertEqual(keep_row["last_mail_snippet"], "My rate is $300")
            self.assertEqual(keep_row["last_mail_raw_path"], "raw/inbound-1.eml")

    def test_apply_creator_reply_context_rejects_inbound_messages_not_sent_by_creator(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(*, uid: int, message_id: str, sent_at: str, direction: str, from_json: str, to_json: str, snippet: str, raw_path: str) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-non-creator-inbound",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Initial outreach",
                raw_path="raw/outbound.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply-all>",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                from_json='[{"email":"teammate@amagency.biz","name":"Teammate"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                snippet="Internal follow-up",
                raw_path="raw/inbound-teammate.eml",
            )
            db.conn.commit()
            db.close()

            keep_row, resolution = pipeline._apply_creator_reply_context(
                {
                    "evidence_thread_key": "thread-non-creator-inbound",
                    "last_mail_message_id": "2",
                    "last_mail_time": "2026-04-01T10:00:00+08:00",
                    "last_mail_subject": "Re: Duet outreach",
                    "last_mail_snippet": "Internal follow-up",
                    "last_mail_raw_path": "raw/inbound-teammate.eml",
                },
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "outbound_only_or_no_reply")
            self.assertFalse(resolution["creator_replied"])
            self.assertEqual(keep_row["last_mail_message_id"], "")
            self.assertEqual(keep_row["last_mail_time"], "")
            self.assertEqual(keep_row["last_mail_snippet"], "")
            self.assertEqual(keep_row["last_mail_raw_path"], "")

    def test_apply_thread_assignment_cache_fills_missing_thread_key_when_guard_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            db.conn.execute(
                """
                INSERT INTO thread_assignments (
                    thread_key, owner_scope, creator_id, platform, brand,
                    matched_contact_email, normalized_subject, source_stage, source_run_id,
                    last_mail_message_id, last_mail_sent_at, mail_update_revision, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "mid:<alpha-root>",
                    "ou_owner",
                    "alpha",
                    "instagram",
                    "MINISO",
                    "alpha@example.com",
                    "alpha outreach",
                    "final_keep",
                    "run-1",
                    "101",
                    "2026-04-05T10:00:00+08:00",
                    3,
                    "2026-04-05T10:00:00+08:00",
                    "2026-04-05T10:00:00+08:00",
                ),
            )
            db.conn.commit()
            db.close()

            updated_row, resolution = pipeline._apply_thread_assignment_cache(
                {
                    "Platform": "Instagram",
                    "@username": "alpha",
                    "matched_contact_email": "alpha@example.com",
                    "subject": "Re: Alpha outreach",
                },
                shared_mail_db_path=db_path,
                owner_scope="ou_owner",
                task_name="MINISO",
            )

        self.assertEqual(resolution["status"], "cache_hit")
        self.assertEqual(updated_row["evidence_thread_key"], "mid:<alpha-root>")
        self.assertEqual(updated_row["last_mail_message_id"], "101")
        self.assertEqual(updated_row["last_mail_time"], "2026-04-05T10:00:00+08:00")
        self.assertEqual(updated_row["mail_update_revision"], 3)

    def test_apply_creator_reply_context_to_export_row_uses_explicit_creator_email_for_multi_recipient_threads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(
                *,
                uid: int,
                message_id: str,
                sent_at: str,
                direction: str,
                from_json: str,
                to_json: str,
                cc_json: str,
                snippet: str,
                raw_path: str,
            ) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, ?, '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        cc_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-explicit-creator",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        2,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"manager@example.com","name":"Manager"}]',
                cc_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Initial outreach",
                raw_path="raw/outbound.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply>",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                cc_json="[]",
                snippet="Creator reply",
                raw_path="raw/inbound.eml",
            )
            db.conn.commit()
            db.close()

            updated_row, resolution = pipeline._apply_creator_reply_context_to_export_row(
                {
                    "达人ID": "alpha",
                    "matched_contact_email": "creator@example.com",
                    "__last_mail_raw_path": "raw/outbound.eml",
                },
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "creator_replied")
            self.assertTrue(resolution["creator_replied"])
            self.assertEqual(updated_row["达人最后一次回复邮件时间"], "2026/04/01")
            self.assertEqual(updated_row["达人回复的最后一封邮件内容"], "Creator reply")
            self.assertEqual(updated_row["__last_mail_raw_path"], "raw/inbound.eml")

    def test_apply_creator_reply_context_to_export_row_infers_creator_from_cc_when_cc_is_only_external_recipient(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(
                *,
                uid: int,
                message_id: str,
                sent_at: str,
                direction: str,
                from_json: str,
                to_json: str,
                cc_json: str,
                snippet: str,
                raw_path: str,
            ) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, ?, '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        cc_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-cc-only-external",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                cc_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Initial outreach",
                raw_path="raw/outbound.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply>",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                cc_json="[]",
                snippet="Creator reply",
                raw_path="raw/inbound.eml",
            )
            db.conn.commit()
            db.close()

            updated_row, resolution = pipeline._apply_creator_reply_context_to_export_row(
                {
                    "达人ID": "alpha",
                    "__last_mail_raw_path": "raw/outbound.eml",
                },
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "creator_replied")
            self.assertTrue(resolution["creator_replied"])
            self.assertEqual(updated_row["达人回复的最后一封邮件内容"], "Creator reply")
            self.assertEqual(updated_row["__last_mail_raw_path"], "raw/inbound.eml")

    def test_apply_creator_reply_context_to_export_row_marks_legacy_multi_recipient_threads_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "email_sync.db"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(
                *,
                uid: int,
                message_id: str,
                sent_at: str,
                direction: str,
                from_json: str,
                to_json: str,
                cc_json: str,
                snippet: str,
                raw_path: str,
            ) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, ?, '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        cc_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-ambiguous-creator",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        2,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"manager@example.com","name":"Manager"}]',
                cc_json='[{"email":"creator@example.com","name":"Creator"}]',
                snippet="Initial outreach",
                raw_path="raw/outbound.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply>",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                cc_json="[]",
                snippet="Creator reply",
                raw_path="raw/inbound.eml",
            )
            db.conn.commit()
            db.close()

            original_row = {
                "达人ID": "alpha",
                "达人最后一次回复邮件时间": "2026/04/01",
                "达人回复的最后一封邮件内容": "Original content",
                "__last_mail_raw_path": "raw/outbound.eml",
            }
            updated_row, resolution = pipeline._apply_creator_reply_context_to_export_row(
                original_row,
                shared_mail_db_path=db_path,
            )

            self.assertEqual(resolution["status"], "creator_identity_unresolved")
            self.assertIsNone(resolution["creator_replied"])
            self.assertEqual(updated_row["达人最后一次回复邮件时间"], "2026/04/01")
            self.assertEqual(updated_row["达人回复的最后一封邮件内容"], "Original content")
            self.assertEqual(updated_row["__last_mail_raw_path"], "raw/outbound.eml")

    def test_rewrite_existing_final_payload_filters_outbound_only_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "email_sync.db"
            payload_path = temp_path / "all_platforms_final_review_payload.json"
            output_root = temp_path / "rewrite_output"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(*, uid: int, message_id: str, thread_key: str, sent_at: str, direction: str, snippet: str, raw_path: str) -> None:
                from_json = '[{"email":"yvette@amagency.biz","name":"Yvette"}]' if direction == "outbound" else '[{"email":"creator@example.com","name":"Creator"}]'
                to_json = '[{"email":"creator@example.com","name":"Creator"}]' if direction == "outbound" else '[{"email":"yvette@amagency.biz","name":"Yvette"}]'
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        thread_key,
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-outbound-only>",
                thread_key="thread-outbound-only",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                snippet="Initial outreach only",
                raw_path="raw/outbound-only.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-outbound-1>",
                thread_key="thread-with-reply",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                snippet="Initial outreach",
                raw_path="raw/outbound-1.eml",
            )
            insert_message(
                uid=3,
                message_id="<msg-inbound-1>",
                thread_key="thread-with-reply",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                snippet="Creator reply",
                raw_path="raw/inbound-1.eml",
            )
            insert_message(
                uid=4,
                message_id="<msg-outbound-2>",
                thread_key="thread-with-reply",
                sent_at="2026-04-01T11:00:00+08:00",
                direction="outbound",
                snippet="Outbound follow up",
                raw_path="raw/outbound-2.eml",
            )
            db.conn.commit()
            db.close()

            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@alpha",
                                "达人对接人": "Yvette",
                                "达人对接人_employee_id": "ou_yvette",
                                "达人对接人_employee_email": "yvette@amagency.biz",
                                "linked_bitable_url": "https://bitable.example/duet",
                                "任务名": "Duet1",
                                "达人最后一次回复邮件时间": "2026/04/01",
                                "full body": "Initial outreach only",
                                "达人回复的最后一封邮件内容": "Initial outreach only",
                                "__last_mail_raw_path": "raw/outbound-only.eml",
                            },
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "达人对接人": "Yvette",
                                "达人对接人_employee_id": "ou_yvette",
                                "达人对接人_employee_email": "yvette@amagency.biz",
                                "linked_bitable_url": "https://bitable.example/duet",
                                "任务名": "Duet1",
                                "达人最后一次回复邮件时间": "2026/04/01",
                                "full body": "Outbound follow up",
                                "达人回复的最后一封邮件内容": "Outbound follow up",
                                "__last_mail_raw_path": "raw/outbound-2.eml",
                            },
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            uploaded_payloads: list[dict[str, object]] = []

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                uploaded_payloads.append(json.loads(Path(kwargs["payload_json_path"]).read_text(encoding="utf-8")))
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_json.write_text(json.dumps({"created_count": 1, "updated_count": 0}, ensure_ascii=False), encoding="utf-8")
                return {
                    "created_count": 1,
                    "updated_count": 0,
                    "failed_count": 0,
                    "failed_rows": [],
                    "skipped_existing_count": 0,
                    "skipped_existing_rows": [],
                    "result_json_path": str(result_json),
                }

            pipeline._load_runtime_dependencies = lambda: {
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }
            pipeline._build_feishu_client = lambda **kwargs: (SimpleNamespace(), {}, {})

            summary = pipeline.rewrite_existing_final_payload_from_shared_mailbox(
                shared_mail_db_path=db_path,
                existing_final_payload_json=payload_path,
                env_file=".env",
                output_root=output_root,
                upload_dry_run=False,
            )

            self.assertEqual(summary["input_row_count"], 2)
            self.assertEqual(summary["kept_row_count"], 1)
            self.assertEqual(summary["removed_no_reply_count"], 1)
            self.assertEqual(summary["corrected_reply_count"], 1)
            self.assertEqual(len(uploaded_payloads), 1)
            self.assertEqual(len(uploaded_payloads[0]["rows"]), 1)
            self.assertEqual(uploaded_payloads[0]["rows"][0]["达人ID"], "beta")
            self.assertEqual(uploaded_payloads[0]["rows"][0]["full body"], "Creator reply")
            self.assertEqual(uploaded_payloads[0]["rows"][0]["达人回复的最后一封邮件内容"], "Creator reply")
            self.assertEqual(uploaded_payloads[0]["rows"][0]["__last_mail_raw_path"], "raw/inbound-1.eml")

            removed_rows = json.loads((output_root / "exports" / "removed_no_reply_rows.json").read_text(encoding="utf-8"))
            self.assertEqual(removed_rows["removed_count"], 1)
            self.assertEqual(removed_rows["removed_rows"][0]["达人ID"], "alpha")

    def test_apply_row_owner_overrides_backfills_mail_fields_from_keep_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "email_sync.db"
            db_path.touch()
            raw_dir = temp_path / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "alpha.eml").write_text("Subject: alpha\n\nInterested, my rate is $100", encoding="utf-8")

            keep_row = {
                "Platform": "TikTok",
                "@username": "alpha",
                "URL": "https://www.tiktok.com/@alpha",
                "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                "brand_message_snippet": "Interested, my rate is $100",
                "brand_message_raw_path": "raw/alpha.eml",
            }
            keep_frame = pd.DataFrame([keep_row])
            keep_workbook = temp_path / "keep.xlsx"
            keep_frame.to_excel(keep_workbook, index=False)

            overridden_rows = pipeline._apply_row_owner_overrides(
                [
                    {
                        "达人ID": "alpha",
                        "平台": "tiktok",
                        "主页链接": "https://www.tiktok.com/@alpha",
                        "当前网红报价": "",
                        "达人最后一次回复邮件时间": "",
                        "full body": "",
                        "达人回复的最后一封邮件内容": "",
                        "__last_mail_raw_path": "",
                        "__feishu_attachment_local_paths": [],
                    }
                ],
                keep_frame=keep_frame,
                fallback_owner_context={
                    "task_name": "MINISO",
                    "linked_bitable_url": "https://bitable.example/miniso",
                    "responsible_name": "Rhea",
                    "employee_name": "Rhea",
                    "employee_english_name": "Rhea",
                    "employee_id": "ou_rhea",
                    "employee_record_id": "rec_rhea",
                    "employee_email": "rhea@amagency.biz",
                    "owner_name": "rhea@amagency.biz",
                },
                shared_mail_db_path=db_path,
                shared_mail_raw_dir=raw_dir,
                shared_mail_data_dir=temp_path,
                keep_workbook=keep_workbook,
            )

            self.assertEqual(len(overridden_rows), 1)
            row = overridden_rows[0]
            self.assertEqual(row["当前网红报价"], "$100")
            self.assertEqual(row["达人最后一次回复邮件时间"], "2026/03/31")
            self.assertEqual(row["full body"], "Interested, my rate is $100")
            self.assertEqual(row["达人回复的最后一封邮件内容"], "Interested, my rate is $100")
            self.assertEqual(row["__last_mail_raw_path"], "raw/alpha.eml")
            self.assertTrue(row["__feishu_attachment_local_paths"])

    def test_rewrite_existing_final_payload_re_resolves_row_level_owner_for_group_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "email_sync.db"
            payload_path = temp_path / "all_platforms_final_review_payload.json"
            output_root = temp_path / "rewrite_output"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(
                *,
                uid: int,
                message_id: str,
                thread_key: str,
                sent_at: str,
                direction: str,
                snippet: str,
                raw_path: str,
                from_json: str,
                to_json: str,
            ) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        thread_key,
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-outbound-astrid>",
                thread_key="thread-astrid",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                snippet="Hi creator, Astrid here.",
                raw_path="raw/outbound-astrid.eml",
                from_json='[{"email":"astrid@amagency.biz","name":"Astrid"}]',
                to_json='[{"email":"creator@example.com","name":"Creator"}]',
            )
            insert_message(
                uid=2,
                message_id="<msg-inbound-astrid>",
                thread_key="thread-astrid",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                snippet="Hi Astrid, my rate is $500.",
                raw_path="raw/inbound-astrid.eml",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"astrid@amagency.biz","name":"Astrid"}]',
            )
            insert_message(
                uid=3,
                message_id="<msg-outbound-yvette>",
                thread_key="thread-yvette",
                sent_at="2026-04-01T09:00:00+08:00",
                direction="outbound",
                snippet="Hi creator, Yvette here.",
                raw_path="raw/outbound-yvette.eml",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"creator2@example.com","name":"Creator"}]',
            )
            insert_message(
                uid=4,
                message_id="<msg-inbound-yvette>",
                thread_key="thread-yvette",
                sent_at="2026-04-01T10:00:00+08:00",
                direction="inbound",
                snippet="Hi Yvette, I can do this for $700.",
                raw_path="raw/inbound-yvette.eml",
                from_json='[{"email":"creator2@example.com","name":"Creator"}]',
                to_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
            )
            db.conn.commit()
            db.close()

            payload_path.write_text(
                json.dumps(
                    {
                        "task_owner": {
                            "task_name": "Duet2",
                            "responsible_name": "黄淇",
                            "employee_name": "黄淇",
                            "employee_id": "ou_astrid",
                            "employee_record_id": "rec_astrid",
                            "employee_email": "astrid@amagency.biz",
                            "owner_name": "astrid@amagency.biz",
                            "linked_bitable_url": "https://bitable.example/duet",
                        },
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@alpha",
                                "达人对接人": "黄淇",
                                "达人对接人_employee_id": "ou_astrid",
                                "达人对接人_employee_email": "astrid@amagency.biz",
                                "linked_bitable_url": "https://bitable.example/duet",
                                "任务名": "Duet2",
                                "达人最后一次回复邮件时间": "2026/04/01",
                                "达人回复的最后一封邮件内容": "old",
                                "__last_mail_raw_path": "raw/outbound-astrid.eml",
                            },
                            {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "达人对接人": "黄淇",
                                "达人对接人_employee_id": "ou_astrid",
                                "达人对接人_employee_email": "astrid@amagency.biz",
                                "linked_bitable_url": "https://bitable.example/duet",
                                "任务名": "Duet2",
                                "达人最后一次回复邮件时间": "2026/04/01",
                                "达人回复的最后一封邮件内容": "old",
                                "__last_mail_raw_path": "raw/outbound-yvette.eml",
                            },
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            uploaded_payloads: list[dict[str, object]] = []

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                uploaded_payloads.append(json.loads(Path(kwargs["payload_json_path"]).read_text(encoding="utf-8")))
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_json.write_text(json.dumps({"created_count": 2, "updated_count": 0}, ensure_ascii=False), encoding="utf-8")
                return {
                    "created_count": 2,
                    "updated_count": 0,
                    "failed_count": 0,
                    "failed_rows": [],
                    "skipped_existing_count": 0,
                    "skipped_existing_rows": [],
                    "result_json_path": str(result_json),
                }

            pipeline._load_runtime_dependencies = lambda: {
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_duet_1",
                            "taskName": "Duet1",
                            "employeeMatches": [
                                {
                                    "employeeRecordId": "rec_yvette",
                                    "employeeId": "ou_yvette",
                                    "employeeName": "Yvette",
                                    "employeeEnglishName": "Yvette",
                                    "employeeEmail": "yvette@amagency.biz",
                                }
                            ],
                            "employeeRecordId": "rec_yvette",
                            "employeeId": "ou_yvette",
                            "employeeName": "Yvette",
                            "employeeEnglishName": "Yvette",
                            "employeeEmail": "yvette@amagency.biz",
                        },
                        {
                            "recordId": "rec_duet_2",
                            "taskName": "Duet2",
                            "employeeMatches": [
                                {
                                    "employeeRecordId": "rec_astrid",
                                    "employeeId": "ou_astrid",
                                    "employeeName": "黄淇",
                                    "employeeEnglishName": "Astrid",
                                    "employeeEmail": "astrid@amagency.biz",
                                }
                            ],
                            "employeeRecordId": "rec_astrid",
                            "employeeId": "ou_astrid",
                            "employeeName": "黄淇",
                            "employeeEnglishName": "Astrid",
                            "employeeEmail": "astrid@amagency.biz",
                        },
                    ]
                },
            }
            pipeline._build_feishu_client = lambda **kwargs: (
                SimpleNamespace(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {},
            )

            summary = pipeline.rewrite_existing_final_payload_from_shared_mailbox(
                shared_mail_db_path=db_path,
                existing_final_payload_json=payload_path,
                env_file=".env",
                output_root=output_root,
                upload_dry_run=False,
            )

            self.assertEqual(summary["kept_row_count"], 2)
            self.assertEqual(summary["corrected_owner_count"], 1)
            self.assertEqual(summary["owner_candidate_count"], 2)
            uploaded_rows = uploaded_payloads[0]["rows"]
            owner_by_id = {row["达人ID"]: row for row in uploaded_rows}
            self.assertEqual(owner_by_id["alpha"]["达人对接人_employee_id"], "ou_astrid")
            self.assertEqual(owner_by_id["beta"]["达人对接人_employee_id"], "ou_yvette")
            self.assertEqual(owner_by_id["beta"]["达人对接人"], "Yvette")
            self.assertEqual(owner_by_id["beta"]["达人回复的最后一封邮件内容"], "Hi Yvette, I can do this for $700.")

    def test_rewrite_existing_final_payload_uses_feishu_source_url_fallback_for_owner_inspection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "email_sync.db"
            payload_path = temp_path / "all_platforms_final_review_payload.json"
            output_root = temp_path / "rewrite_output"
            inspection_calls: list[dict[str, object]] = []

            payload_path.write_text(
                json.dumps(
                    {
                        "task_owner": {
                            "task_name": "Duet1",
                        },
                        "rows": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_inspect_task_upload_assignments(**kwargs):
                inspection_calls.append(kwargs)
                return {"items": []}

            pipeline._load_runtime_dependencies = lambda: {
                "upload_final_review_payload_to_bitable": lambda *args, **kwargs: {},
                "inspect_task_upload_assignments": fake_inspect_task_upload_assignments,
            }
            pipeline._build_feishu_client = lambda **kwargs: (
                SimpleNamespace(),
                {
                    "FEISHU_SOURCE_URL": "https://feishu-source.example.com",
                },
                {},
            )

            summary = pipeline.rewrite_existing_final_payload_from_shared_mailbox(
                shared_mail_db_path=db_path,
                existing_final_payload_json=payload_path,
                env_file=".env",
                output_root=output_root,
                upload_dry_run=True,
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(len(inspection_calls), 1)
            self.assertEqual(inspection_calls[0]["task_upload_url"], "https://feishu-source.example.com")
            self.assertEqual(inspection_calls[0]["employee_info_url"], "https://feishu-source.example.com")

    def test_rewrite_existing_final_payload_marks_status_when_upload_reports_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "email_sync.db"
            payload_path = temp_path / "all_platforms_final_review_payload.json"
            output_root = temp_path / "rewrite_output"
            db = Database(db_path)
            db.init_schema()
            now = "2026-04-02T10:00:00+08:00"

            def insert_message(*, uid: int, message_id: str, direction: str, sent_at: str, snippet: str, from_json: str, to_json: str, raw_path: str) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        "Duet outreach",
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        snippet,
                        snippet,
                        raw_path,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "duet outreach",
                        direction,
                        "thread-upload-failure",
                        "<msg-root>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<msg-root>",
                direction="outbound",
                sent_at="2026-04-01T09:00:00+08:00",
                snippet="Initial outreach",
                from_json='[{"email":"yvette@amagency.biz","name":"Yvette"}]',
                to_json='[{"email":"creator@example.com","name":"Creator"}]',
                raw_path="raw/outbound.eml",
            )
            insert_message(
                uid=2,
                message_id="<msg-reply>",
                direction="inbound",
                sent_at="2026-04-01T10:00:00+08:00",
                snippet="Creator reply",
                from_json='[{"email":"creator@example.com","name":"Creator"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                raw_path="raw/inbound.eml",
            )
            db.conn.commit()
            db.close()

            payload_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@alpha",
                                "达人对接人": "Yvette",
                                "达人对接人_employee_id": "ou_yvette",
                                "达人对接人_employee_email": "yvette@amagency.biz",
                                "linked_bitable_url": "https://bitable.example/duet",
                                "任务名": "Duet1",
                                "达人最后一次回复邮件时间": "2026/04/01",
                                "达人回复的最后一封邮件内容": "Initial outreach",
                                "__last_mail_raw_path": "raw/outbound.eml",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_json.write_text(json.dumps({"ok": False, "failed_count": 1}, ensure_ascii=False), encoding="utf-8")
                return {
                    "ok": False,
                    "created_count": 0,
                    "updated_count": 0,
                    "failed_count": 1,
                    "failed_rows": [{"row": {"达人ID": "alpha"}, "error": "owner mismatch"}],
                    "skipped_existing_count": 0,
                    "skipped_existing_rows": [],
                    "result_json_path": str(result_json),
                }

            pipeline._load_runtime_dependencies = lambda: {
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }
            pipeline._build_feishu_client = lambda **kwargs: (SimpleNamespace(), {}, {})

            summary = pipeline.rewrite_existing_final_payload_from_shared_mailbox(
                shared_mail_db_path=db_path,
                existing_final_payload_json=payload_path,
                env_file=".env",
                output_root=output_root,
                upload_dry_run=False,
            )

            self.assertEqual(summary["status"], "completed_with_failures")
            self.assertTrue(summary["upload_failed"])
            self.assertEqual(summary["upload_summary"]["failed_count"], 1)

    def test_task_group_alias_skg_collapses_to_single_group_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "alpha.eml").write_text("Subject: alpha\n\nbody", encoding="utf-8")
            db = Database(shared_db_path)
            db.init_schema()
            now = "2026-04-07T10:00:00+08:00"
            db.conn.execute(
                """
                INSERT INTO messages (
                    account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                    sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                    from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                    body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                    has_attachments, attachment_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', ?, '', 0, 0, 0, ?, ?)
                """,
                (
                    "partnerships@amagency.biz",
                    "其他文件夹/达人回信",
                    1,
                    1,
                    "<alpha-1>",
                    "Re: MINISO",
                    now,
                    now,
                    now,
                    now,
                    '[{"email":"alpha@example.com","name":"Alpha"}]',
                    '[{"email":"rhea@amagency.biz","name":"Rhea"}]',
                    "Latest alpha reply $100",
                    "Latest alpha reply $100",
                    "raw/alpha.eml",
                    now,
                    now,
                ),
            )
            message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            db.conn.execute(
                """
                INSERT INTO message_index (
                    message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                    thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_row_id,
                    "miniso",
                    "inbound",
                    "thread-alpha",
                    "<alpha-1>",
                    "",
                    0,
                    now,
                    1,
                    now,
                ),
            )
            db.conn.commit()
            db.close()

            upstream_calls: list[dict[str, object]] = []
            upload_calls: list[dict[str, object]] = []
            downstream_calls: list[dict[str, object]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                upstream_calls.append(
                    {
                        "task_name": kwargs["task_name"],
                        "brand_keyword": kwargs["brand_keyword"],
                        "brand_match_include_from": kwargs["brand_match_include_from"],
                    }
                )
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / f"{kwargs['task_name']}_keep.xlsx"
                template_workbook = output_root / f"{kwargs['task_name']}_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "alpha",
                            "URL": "https://www.tiktok.com/@alpha",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Hi Rhea, rate is $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": kwargs["task_name"],
                            "linked_bitable_url": f"https://bitable.example/{kwargs['task_name']}",
                            "responsible_name": "唐瑞霞",
                            "employee_name": "唐瑞霞",
                            "employee_id": "ou_rhea",
                            "employee_record_id": f"rec_{kwargs['task_name']}",
                            "employee_email": "rhea@amagency.biz",
                            "owner_name": "rhea@amagency.biz",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_fetch_existing_bitable_record_index(client, *, linked_bitable_url):
                return object(), {}

            def fake_run_keep_list_screening_pipeline(**kwargs):
                downstream_calls.append(
                    {
                        "vision_provider": kwargs["vision_provider"],
                        "positioning_provider": kwargs["positioning_provider"],
                    }
                )
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_final_review_payload.json"
                pd.DataFrame(
                    [
                        {
                            "达人ID": "alpha",
                            "平台": "tiktok",
                            "主页链接": "https://www.tiktok.com/@alpha",
                            "# Followers(K)#": 200,
                            "Average Views (K)": 50,
                            "互动率": "10.0%",
                            "当前网红报价": "$100",
                            "达人最后一次回复邮件时间": "2026/03/31",
                            "达人回复的最后一封邮件内容": "alpha latest",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "否",
                            "ai筛号反馈理由": "screened",
                            "标签(ai)": "",
                            "ai评价": "good fit",
                        }
                    ]
                ).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {
                        "responsible_name": kwargs["task_owner_name"],
                        "employee_id": kwargs["task_owner_employee_id"],
                        "employee_record_id": kwargs["task_owner_employee_record_id"],
                        "employee_email": kwargs["task_owner_employee_email"],
                        "owner_name": kwargs["task_owner_owner_name"],
                        "linked_bitable_url": kwargs["linked_bitable_url"],
                        "task_name": kwargs["task_name"],
                    },
                    "columns": [],
                    "source_row_count": 1,
                    "row_count": 1,
                    "skipped_row_count": 0,
                    "rows": [
                        {
                            "达人ID": "alpha",
                            "平台": "tiktok",
                            "主页链接": "https://www.tiktok.com/@alpha",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "否",
                        }
                    ],
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                upload_calls.append(dict(kwargs))
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 1,
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": fake_fetch_existing_bitable_record_index,
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_skg_1",
                            "taskName": "SKG-1",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_rhea",
                            "employeeRecordId": "rec_rhea",
                            "employeeName": "唐瑞霞",
                            "employeeEnglishName": "Rhea",
                            "employeeEmail": "rhea@amagency.biz",
                            "responsibleName": "唐瑞霞",
                            "ownerName": "rhea@amagency.biz",
                        },
                        {
                            "recordId": "rec_skg_2",
                            "taskName": "SKG-2",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_lilith",
                            "employeeRecordId": "rec_lilith",
                            "employeeName": "Sherry97",
                            "employeeEnglishName": "Lilith",
                            "employeeEmail": "lilith@amagency.biz",
                            "responsibleName": "Sherry97",
                            "ownerName": "lilith@amagency.biz",
                        },
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                task_name_filters=["SKG"],
                matching_strategy="brand-keyword-fast-path",
                vision_provider="openai",
                positioning_provider="reelx",
                upload_dry_run=False,
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["task_names"], ["SKG"])
            self.assertEqual(
                [(item["task_name"], item["brand_keyword"], item["brand_match_include_from"]) for item in upstream_calls],
                [("SKG-1", "SKG", True)],
            )
            self.assertEqual(len(upload_calls), 1)
            self.assertTrue(upload_calls[0]["suppress_ai_labels"])
            self.assertEqual(
                downstream_calls,
                [{"vision_provider": "openai", "positioning_provider": "reelx"}],
            )

    def test_parser_defaults_brand_match_include_from_to_true_for_shared_mailbox(self) -> None:
        parser = pipeline.build_parser()

        default_args = parser.parse_args(["--shared-mail-db-path", "data/shared_mailbox/email_sync.db"])
        self.assertIsNone(default_args.brand_match_include_from)

        enabled_args = parser.parse_args(
            ["--shared-mail-db-path", "data/shared_mailbox/email_sync.db", "--brand-match-include-from"]
        )
        self.assertTrue(enabled_args.brand_match_include_from)

        disabled_args = parser.parse_args(
            ["--shared-mail-db-path", "data/shared_mailbox/email_sync.db", "--no-brand-match-include-from"]
        )
        self.assertFalse(disabled_args.brand_match_include_from)

    def test_concrete_skg_2_filter_does_not_expand_back_to_skg_1(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "beta.eml").write_text("Subject: beta\n\nbody", encoding="utf-8")

            upstream_calls: list[dict[str, object]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                upstream_calls.append(
                    {
                        "task_name": kwargs["task_name"],
                        "brand_keyword": kwargs["brand_keyword"],
                    }
                )
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / f"{kwargs['task_name']}_keep.xlsx"
                template_workbook = output_root / f"{kwargs['task_name']}_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "beta",
                            "URL": "https://www.tiktok.com/@beta",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest beta reply $100",
                            "brand_message_raw_path": "raw/beta.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": kwargs["task_name"],
                            "linked_bitable_url": f"https://bitable.example/{kwargs['task_name']}",
                            "responsible_name": "唐瑞霞",
                            "employee_name": "唐瑞霞",
                            "employee_id": "ou_rhea",
                            "employee_record_id": f"rec_{kwargs['task_name']}",
                            "employee_email": "rhea@amagency.biz",
                            "owner_name": "rhea@amagency.biz",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_fetch_existing_bitable_record_index(client, *, linked_bitable_url):
                return object(), {}

            def fake_run_keep_list_screening_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_upload_payload.json"
                pd.DataFrame(
                    [
                        {
                            "达人ID": "beta",
                            "平台": "tiktok",
                            "主页链接": "https://www.tiktok.com/@beta",
                            "# Followers(K)#": 200,
                            "Average Views (K)": 50,
                            "互动率": "10.0%",
                            "当前网红报价": "$100",
                            "达人最后一次回复邮件时间": "2026/03/31",
                            "达人回复的最后一封邮件内容": "beta latest",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "否",
                            "ai筛号反馈理由": "screened",
                            "标签(ai)": "",
                            "ai评价": "good fit",
                        }
                    ]
                ).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": [],
                    "source_row_count": 1,
                    "row_count": 1,
                    "skipped_row_count": 0,
                    "rows": [{"达人ID": "beta", "平台": "tiktok", "ai是否通过": "否"}],
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 1,
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": fake_fetch_existing_bitable_record_index,
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_skg_1",
                            "taskName": "SKG-1",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_rhea",
                            "employeeRecordId": "rec_rhea",
                            "employeeName": "唐瑞霞",
                            "employeeEnglishName": "Rhea",
                            "employeeEmail": "rhea@amagency.biz",
                            "responsibleName": "唐瑞霞",
                            "ownerName": "rhea@amagency.biz",
                        },
                        {
                            "recordId": "rec_skg_2",
                            "taskName": "SKG-2",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_lilith",
                            "employeeRecordId": "rec_lilith",
                            "employeeName": "Sherry97",
                            "employeeEnglishName": "Lilith",
                            "employeeEmail": "lilith@amagency.biz",
                            "responsibleName": "Sherry97",
                            "ownerName": "lilith@amagency.biz",
                        },
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                task_name_filters=["SKG-2"],
                matching_strategy="brand-keyword-fast-path",
                brand_keyword="SKG",
                upload_dry_run=False,
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["task_names"], ["SKG-2"])
            self.assertEqual(
                [(item["task_name"], item["brand_keyword"]) for item in upstream_calls],
                [("SKG-2", "SKG")],
            )

    def test_grouped_skg_routes_row_owner_from_mail_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "alpha.eml").write_text("Subject: alpha\n\nHi Rhea, rate is $100", encoding="utf-8")
            (shared_raw_dir / "beta.eml").write_text("Subject: beta\n\nHi Lilith, rate is $200", encoding="utf-8")

            uploaded_payloads: list[dict[str, object]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "skg_keep.xlsx"
                template_workbook = output_root / "skg_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "alpha",
                            "URL": "https://www.tiktok.com/@alpha",
                            "brand_message_subject": "SKG alpha outreach",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Hi Rhea, rate is $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        },
                        {
                            "Platform": "TikTok",
                            "@username": "beta",
                            "URL": "https://www.tiktok.com/@beta",
                            "brand_message_subject": "SKG beta outreach",
                            "brand_message_sent_at": "2026-03-31T11:00:00+08:00",
                            "brand_message_snippet": "Hi Lilith, rate is $200",
                            "brand_message_raw_path": "raw/beta.eml",
                        },
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 2}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": kwargs["task_name"],
                            "linked_bitable_url": "https://bitable.example/skg",
                            "responsible_name": "唐瑞霞",
                            "employee_name": "唐瑞霞",
                            "employee_id": "ou_rhea",
                            "employee_record_id": "rec_rhea",
                            "employee_email": "rhea@amagency.biz",
                            "owner_name": "rhea@amagency.biz",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                keep_rows = pd.read_excel(Path(kwargs["keep_workbook"])).to_dict(orient="records")
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                display_rows: list[dict[str, object]] = []
                payload_rows: list[dict[str, object]] = []
                for keep_row in keep_rows:
                    creator_id = keep_row["@username"]
                    display_row = {
                        "达人ID": creator_id,
                        "平台": "tiktok",
                        "主页链接": keep_row["URL"],
                        "# Followers(K)#": 200,
                        "Average Views (K)": 50,
                        "互动率": "10.0%",
                        "当前网红报价": "$100",
                        "达人最后一次回复邮件时间": "2026/03/31",
                        "达人回复的最后一封邮件内容": keep_row["brand_message_snippet"],
                        "达人对接人": kwargs["task_owner_name"],
                        "ai是否通过": "是",
                        "ai筛号反馈理由": "screened",
                        "标签(ai)": "",
                        "ai评价": "good fit",
                    }
                    payload_row = dict(display_row)
                    payload_row.update(
                        {
                            "达人对接人_employee_id": kwargs["task_owner_employee_id"],
                            "达人对接人_employee_record_id": kwargs["task_owner_employee_record_id"],
                            "达人对接人_employee_email": kwargs["task_owner_employee_email"],
                            "达人对接人_owner_name": kwargs["task_owner_owner_name"],
                            "linked_bitable_url": kwargs["linked_bitable_url"],
                            "任务名": kwargs["task_name"],
                        }
                    )
                    display_rows.append(display_row)
                    payload_rows.append(payload_row)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_final_review_payload.json"
                pd.DataFrame(display_rows).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": list(display_rows[0].keys()),
                    "source_row_count": len(display_rows),
                    "row_count": len(payload_rows),
                    "skipped_row_count": 0,
                    "rows": payload_rows,
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                payload = json.loads(Path(kwargs["payload_json_path"]).read_text(encoding="utf-8"))
                uploaded_payloads.append(payload)
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": len(payload.get("rows") or []),
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_skg_1",
                            "taskName": "SKG-1",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_rhea",
                            "employeeRecordId": "rec_rhea",
                            "employeeName": "唐瑞霞",
                            "employeeEnglishName": "Rhea",
                            "employeeEmail": "rhea@amagency.biz",
                            "responsibleName": "唐瑞霞",
                            "ownerName": "rhea@amagency.biz",
                        },
                        {
                            "recordId": "rec_skg_2",
                            "taskName": "SKG-2",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_lilith",
                            "employeeRecordId": "rec_lilith",
                            "employeeName": "Sherry97",
                            "employeeEnglishName": "Lilith",
                            "employeeEmail": "lilith@amagency.biz",
                            "responsibleName": "Sherry97",
                            "ownerName": "lilith@amagency.biz",
                        },
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                task_name_filters=["SKG"],
                output_root=root / "run",
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["task_names"], ["SKG"])
            self.assertEqual(len(uploaded_payloads), 1)
            payload_rows = {row["达人ID"]: row for row in uploaded_payloads[0]["rows"]}
            self.assertEqual(payload_rows["alpha"]["达人对接人"], "唐瑞霞")
            self.assertEqual(payload_rows["alpha"]["达人对接人_employee_id"], "ou_rhea")
            self.assertEqual(payload_rows["beta"]["达人对接人"], "Sherry97")
            self.assertEqual(payload_rows["beta"]["达人对接人_employee_id"], "ou_lilith")
            task_result = summary["task_results"][0]
            workbook_rows = pd.read_excel(task_result["all_platforms_final_review"]).fillna("")
            owner_map = {
                row["达人ID"]: row["达人对接人"]
                for row in workbook_rows.to_dict(orient="records")
            }
            self.assertEqual(owner_map, {"alpha": "唐瑞霞", "beta": "Sherry97"})

    def test_grouped_skg_routes_row_owner_from_thread_history_when_latest_reply_is_generic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "alpha.eml").write_text("Subject: alpha\n\nInterested, my rate is $100", encoding="utf-8")

            db = Database(shared_db_path)
            db.init_schema()
            now = "2026-04-01T10:00:00+08:00"

            def insert_message(
                *,
                uid: int,
                message_id: str,
                subject: str,
                sent_at: str,
                direction: str,
                from_json: str,
                to_json: str,
                snippet: str,
                body_text: str,
            ) -> None:
                db.conn.execute(
                    """
                    INSERT INTO messages (
                        account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                        sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                        from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                        body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                        has_attachments, attachment_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, '[]', 0, ?, ?, '[]', '[]', '[]', '[]', ?, '', ?, '{}', '', '', 0, 0, 0, ?, ?)
                    """,
                    (
                        "partnerships@amagency.biz",
                        "INBOX",
                        uid,
                        1,
                        message_id,
                        subject,
                        sent_at,
                        sent_at,
                        sent_at,
                        sent_at,
                        from_json,
                        to_json,
                        body_text,
                        snippet,
                        now,
                        now,
                    ),
                )
                message_row_id = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                db.conn.execute(
                    """
                    INSERT INTO message_index (
                        message_row_id, normalized_subject, direction, thread_key, thread_root_message_id,
                        thread_parent_message_id, thread_depth, sent_sort_at, external_contact_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_row_id,
                        "skg alpha outreach",
                        direction,
                        "thread-alpha",
                        "<root-alpha>",
                        "",
                        0,
                        sent_at,
                        1,
                        now,
                    ),
                )

            insert_message(
                uid=1,
                message_id="<root-alpha>",
                subject="SKG alpha outreach",
                sent_at="2026-03-31T09:00:00+08:00",
                direction="outbound",
                from_json='[{"email":"rhea@amagency.biz","name":"Rhea"}]',
                to_json='[{"email":"creator@example.com","name":"alpha"}]',
                snippet="Hi alpha, this is Rhea from AM Agency.",
                body_text="Hi alpha, this is Rhea from AM Agency for SKG.",
            )
            insert_message(
                uid=2,
                message_id="<reply-alpha>",
                subject="Re: SKG alpha outreach",
                sent_at="2026-03-31T10:00:00+08:00",
                direction="inbound",
                from_json='[{"email":"creator@example.com","name":"alpha"}]',
                to_json='[{"email":"partnerships@amagency.biz","name":"AM Agency"}]',
                snippet="Interested, my rate is $100",
                body_text="Interested, my rate is $100",
            )
            db.conn.commit()
            db.close()

            uploaded_payloads: list[dict[str, object]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "skg_keep.xlsx"
                template_workbook = output_root / "skg_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "alpha",
                            "URL": "https://www.tiktok.com/@alpha",
                            "brand_message_subject": "Re: SKG alpha outreach",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Interested, my rate is $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                            "last_mail_subject": "Re: SKG alpha outreach",
                            "last_mail_snippet": "Interested, my rate is $100",
                            "matched_email": "creator@example.com",
                            "evidence_thread_key": "thread-alpha",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": kwargs["task_name"],
                            "linked_bitable_url": "https://bitable.example/skg",
                            "responsible_name": "唐瑞霞",
                            "employee_name": "唐瑞霞",
                            "employee_id": "ou_rhea",
                            "employee_record_id": "rec_rhea",
                            "employee_email": "rhea@amagency.biz",
                            "owner_name": "rhea@amagency.biz",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                keep_rows = pd.read_excel(Path(kwargs["keep_workbook"])).to_dict(orient="records")
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                display_rows: list[dict[str, object]] = []
                payload_rows: list[dict[str, object]] = []
                for keep_row in keep_rows:
                    display_row = {
                        "达人ID": keep_row["@username"],
                        "平台": "tiktok",
                        "主页链接": keep_row["URL"],
                        "# Followers(K)#": 200,
                        "Average Views (K)": 50,
                        "互动率": "10.0%",
                        "当前网红报价": "$100",
                        "达人最后一次回复邮件时间": "2026/03/31",
                        "达人回复的最后一封邮件内容": keep_row["brand_message_snippet"],
                        "达人对接人": kwargs["task_owner_name"],
                        "ai是否通过": "是",
                        "ai筛号反馈理由": "screened",
                        "标签(ai)": "",
                        "ai评价": "good fit",
                    }
                    payload_row = dict(display_row)
                    payload_row.update(
                        {
                            "达人对接人_employee_id": kwargs["task_owner_employee_id"],
                            "达人对接人_employee_record_id": kwargs["task_owner_employee_record_id"],
                            "达人对接人_employee_email": kwargs["task_owner_employee_email"],
                            "达人对接人_owner_name": kwargs["task_owner_owner_name"],
                            "linked_bitable_url": kwargs["linked_bitable_url"],
                            "任务名": kwargs["task_name"],
                        }
                    )
                    display_rows.append(display_row)
                    payload_rows.append(payload_row)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_final_review_payload.json"
                pd.DataFrame(display_rows).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": list(display_rows[0].keys()),
                    "source_row_count": len(display_rows),
                    "row_count": len(payload_rows),
                    "skipped_row_count": 0,
                    "rows": payload_rows,
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                payload = json.loads(Path(kwargs["payload_json_path"]).read_text(encoding="utf-8"))
                uploaded_payloads.append(payload)
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": len(payload.get("rows") or []),
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_skg_1",
                            "taskName": "SKG-1",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_rhea",
                            "employeeRecordId": "rec_rhea",
                            "employeeName": "唐瑞霞",
                            "employeeEnglishName": "Rhea",
                            "employeeEmail": "rhea@amagency.biz",
                            "responsibleName": "唐瑞霞",
                            "ownerName": "rhea@amagency.biz",
                        },
                        {
                            "recordId": "rec_skg_2",
                            "taskName": "SKG-2",
                            "linkedBitableUrl": "https://bitable.example/skg",
                            "templateFileToken": "boxcn-skg-template",
                            "sendingListFileToken": "boxcn-skg-list",
                            "employeeId": "ou_lilith",
                            "employeeRecordId": "rec_lilith",
                            "employeeName": "Sherry97",
                            "employeeEnglishName": "Lilith",
                            "employeeEmail": "lilith@amagency.biz",
                            "responsibleName": "Sherry97",
                            "ownerName": "lilith@amagency.biz",
                        },
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                task_name_filters=["SKG"],
                output_root=root / "run",
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(len(uploaded_payloads), 1)
            payload_row = uploaded_payloads[0]["rows"][0]
            self.assertEqual(payload_row["达人对接人"], "唐瑞霞")
            self.assertEqual(payload_row["达人对接人_employee_id"], "ou_rhea")
            self.assertEqual(summary["failed_record_count"], 0)

    def test_pipeline_routes_mail_only_updates_and_full_screening_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            for name in ("alpha.eml", "beta.eml", "gamma.eml", "omega.eml"):
                (shared_raw_dir / name).write_text(f"Subject: {name}\n\nbody", encoding="utf-8")

            upload_payloads: dict[str, dict[str, object]] = {}

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                task_name = kwargs["task_name"]
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / f"{task_name}_keep.xlsx"
                template_workbook = output_root / f"{task_name}_template.xlsx"
                template_workbook.touch()
                if task_name == "MINISO":
                    frame = pd.DataFrame(
                        [
                            {
                                "Platform": "Instagram",
                                "@username": "alpha",
                                "URL": "https://www.instagram.com/alpha",
                                "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                                "brand_message_snippet": "Latest alpha reply $100",
                                "brand_message_raw_path": "raw/alpha.eml",
                            },
                            {
                                "Platform": "TikTok",
                                "@username": "beta",
                                "URL": "https://www.tiktok.com/@beta",
                                "brand_message_sent_at": "2026-03-31T11:00:00+08:00",
                                "brand_message_snippet": "Latest beta reply $200",
                                "brand_message_raw_path": "raw/beta.eml",
                            },
                            {
                                "Platform": "Instagram",
                                "@username": "gamma",
                                "URL": "https://www.instagram.com/gamma",
                                "brand_message_sent_at": "2026-03-31T12:00:00+08:00",
                                "brand_message_snippet": "Latest gamma reply $300",
                                "brand_message_raw_path": "raw/gamma.eml",
                            },
                        ]
                    )
                    linked_bitable_url = "https://bitable.example/miniso"
                    owner_name = "陈俊仁"
                    employee_id = "ou_miniso"
                    message_hit_count = 3
                else:
                    frame = pd.DataFrame(
                        [
                            {
                                "Platform": "Instagram",
                                "@username": "omega",
                                "URL": "https://www.instagram.com/omega",
                                "brand_message_sent_at": "2026-03-31T13:00:00+08:00",
                                "brand_message_snippet": "Latest omega reply $500",
                                "brand_message_raw_path": "raw/omega.eml",
                            }
                        ]
                    )
                    linked_bitable_url = "https://bitable.example/tapo"
                    owner_name = "张翔"
                    employee_id = "ou_tapo"
                    message_hit_count = 1
                frame.to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {
                        "brand_match": {
                            "stats": {
                                "message_hit_count": message_hit_count,
                            }
                        }
                    },
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": task_name,
                            "linked_bitable_url": linked_bitable_url,
                            "responsible_name": owner_name,
                            "employee_name": owner_name,
                            "employee_id": employee_id,
                            "employee_record_id": f"rec_{task_name.lower()}",
                            "employee_email": f"{task_name.lower()}@amagency.biz",
                            "owner_name": owner_name,
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_fetch_existing_bitable_record_index(client, *, linked_bitable_url):
                if linked_bitable_url.endswith("/miniso"):
                    return object(), {
                        "alpha::instagram": {
                            "record_id": "rec_alpha",
                            "fields": {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "主页链接": "https://www.instagram.com/alpha",
                                "# Followers(K)#": "120",
                                "Average Views (K)": "10",
                                "互动率": "8.1%",
                                "当前网红报价": "$90",
                                "达人最后一次回复邮件时间": "2026/03/30",
                                "达人回复的最后一封邮件内容": "old alpha",
                                "ai 是否通过": "是",
                                "ai筛号反馈理由": "existing ok",
                                "标签（ai）": ["家庭用品和家电-家庭博主"],
                                "ai 评价": "already screened",
                            },
                        },
                        "beta::tiktok": {
                            "record_id": "rec_beta",
                            "fields": {
                                "达人ID": "beta",
                                "平台": "tiktok",
                                "主页链接": "https://www.tiktok.com/@beta",
                                "ai 是否通过": "",
                            },
                        },
                    }
                return object(), {}

            def fake_run_keep_list_screening_pipeline(**kwargs):
                keep_workbook = Path(kwargs["keep_workbook"])
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                input_rows = pd.read_excel(keep_workbook).to_dict(orient="records")
                task_name = kwargs["task_name"]
                display_rows: list[dict[str, object]] = []
                payload_rows: list[dict[str, object]] = []
                for row in input_rows:
                    creator_id = row["@username"]
                    platform = str(row["Platform"]).strip().lower()
                    display_row = {
                        "达人ID": creator_id,
                        "平台": platform,
                        "主页链接": row["URL"],
                        "# Followers(K)#": 200,
                        "Average Views (K)": 50,
                        "互动率": "10.0%",
                        "当前网红报价": f"${100 if creator_id == 'beta' else 200}",
                        "达人最后一次回复邮件时间": "2026/03/31",
                        "达人回复的最后一封邮件内容": f"{creator_id} latest",
                        "达人对接人": kwargs["task_owner_name"],
                        "ai是否通过": "是",
                        "ai筛号反馈理由": "screened",
                        "标签(ai)": "母婴用品-家庭/宝妈",
                        "ai评价": "good fit",
                    }
                    payload_row = dict(display_row)
                    payload_row.update(
                        {
                            "达人对接人_employee_id": kwargs["task_owner_employee_id"],
                            "达人对接人_employee_record_id": kwargs["task_owner_employee_record_id"],
                            "达人对接人_employee_email": kwargs["task_owner_employee_email"],
                            "达人对接人_owner_name": kwargs["task_owner_owner_name"],
                            "linked_bitable_url": kwargs["linked_bitable_url"],
                            "任务名": task_name,
                        }
                    )
                    display_rows.append(display_row)
                    payload_rows.append(payload_row)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                pd.DataFrame(display_rows).to_excel(final_review_path, index=False)
                payload_path = exports_dir / "all_platforms_final_review_payload.json"
                payload = {
                    "task_owner": {
                        "responsible_name": kwargs["task_owner_name"],
                        "employee_id": kwargs["task_owner_employee_id"],
                        "employee_record_id": kwargs["task_owner_employee_record_id"],
                        "employee_email": kwargs["task_owner_employee_email"],
                        "owner_name": kwargs["task_owner_owner_name"],
                        "linked_bitable_url": kwargs["linked_bitable_url"],
                        "task_name": task_name,
                    },
                    "columns": list(display_rows[0].keys()) if display_rows else [],
                    "source_row_count": len(display_rows),
                    "row_count": len(payload_rows),
                    "skipped_row_count": 0,
                    "rows": payload_rows,
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, *, payload_json_path, linked_bitable_url="", task_name="", task_upload_url="", result_json_path=None, result_xlsx_path=None, dry_run=False, limit=0, suppress_ai_labels=False):
                payload_path = Path(payload_json_path)
                payload = json.loads(payload_path.read_text(encoding="utf-8"))
                upload_payloads[linked_bitable_url] = payload
                archive_dir = payload_path.parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                row_count = len(payload.get("rows") or [])
                created_count = 0
                updated_count = 0
                for row in payload.get("rows") or []:
                    mode = row.get("__feishu_update_mode")
                    if linked_bitable_url.endswith("/miniso"):
                        if row["达人ID"] == "gamma":
                            created_count += 1
                        else:
                            updated_count += 1
                    else:
                        created_count += 1
                return {
                    "ok": True,
                    "payload_json_path": str(payload_path),
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": created_count,
                    "updated_count": updated_count,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {
                    "feishu_base_url": "https://open.feishu.cn",
                    "timeout_seconds": 30.0,
                },
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(
                        index=fake_fetch_existing_bitable_record_index(client, linked_bitable_url=linked_bitable_url)[1],
                        duplicate_groups=[],
                    ),
                ),
                "fetch_existing_bitable_record_index": fake_fetch_existing_bitable_record_index,
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_miniso",
                            "taskName": "MINISO",
                            "linkedBitableUrl": "https://bitable.example/miniso",
                            "employeeId": "ou_miniso",
                            "employeeRecordId": "rec_miniso_emp",
                            "employeeName": "陈俊仁",
                            "employeeEmail": "chenjunren@amagency.biz",
                            "responsibleName": "陈俊仁",
                            "ownerName": "陈俊仁",
                        },
                        {
                            "recordId": "rec_tapo",
                            "taskName": "TAPO",
                            "linkedBitableUrl": "https://bitable.example/tapo",
                            "employeeId": "ou_tapo",
                            "employeeRecordId": "rec_tapo_emp",
                            "employeeName": "张翔",
                            "employeeEmail": "zhangxiang@amagency.biz",
                            "responsibleName": "张翔",
                            "ownerName": "张翔",
                        },
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                env_file=".env",
                output_root=root / "run",
            )
            aggregate_json_exists = Path(summary["aggregate_archive_json"]).exists()
            aggregate_xlsx_exists = Path(summary["aggregate_archive_xlsx"]).exists()
            miniso_result = next(item for item in summary["task_results"] if item["task_name"] == "MINISO")
            miniso_workbook_exists = Path(miniso_result["all_platforms_final_review"]).exists()
            miniso_payload_exists = Path(miniso_result["all_platforms_upload_payload_json"]).exists()

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["task_count"], 2)
        self.assertEqual(summary["task_names"], ["MINISO", "TAPO"])
        self.assertEqual(summary["matched_mail_count"], 4)
        self.assertEqual(summary["new_creator_count"], 2)
        self.assertEqual(summary["existing_screened_count"], 1)
        self.assertEqual(summary["existing_unscreened_count"], 1)
        self.assertEqual(summary["known_thread_hit_count"], 0)
        self.assertEqual(summary["thread_assignment_cache_hit_count"], 0)
        self.assertEqual(summary["full_screening_count"], 3)
        self.assertEqual(summary["mail_only_update_count"], 1)
        self.assertEqual(summary["created_record_count"], 2)
        self.assertEqual(summary["updated_record_count"], 2)
        self.assertEqual(summary["failed_record_count"], 0)
        self.assertTrue(aggregate_json_exists)
        self.assertTrue(aggregate_xlsx_exists)
        self.assertEqual(len(summary["task_results"]), 2)

        miniso_payload = upload_payloads["https://bitable.example/miniso"]
        miniso_rows = list(miniso_payload["rows"])
        self.assertEqual(len(miniso_rows), 3)
        mode_by_creator = {row["达人ID"]: row["__feishu_update_mode"] for row in miniso_rows}
        self.assertEqual(mode_by_creator["alpha"], "mail_only_update")
        self.assertEqual(mode_by_creator["beta"], "create_or_update")
        self.assertEqual(mode_by_creator["gamma"], "create_or_update")
        alpha_row = next(row for row in miniso_rows if row["达人ID"] == "alpha")
        self.assertEqual(alpha_row["ai是否通过"], "是")
        self.assertEqual(alpha_row["标签(ai)"], "家庭用品和家电-家庭博主")
        self.assertTrue(alpha_row["__feishu_attachment_local_paths"][0].endswith("alpha.eml"))

        miniso_result = next(item for item in summary["task_results"] if item["task_name"] == "MINISO")
        self.assertEqual(miniso_result["known_thread_hit_count"], 0)
        self.assertEqual(miniso_result["thread_assignment_cache_hit_count"], 0)
        self.assertEqual(miniso_result["mail_only_update_count"], 1)
        self.assertEqual(miniso_result["full_screening_count"], 2)
        self.assertEqual(miniso_result["created_count"], 1)
        self.assertEqual(miniso_result["updated_count"], 2)
        self.assertTrue(miniso_workbook_exists)
        self.assertTrue(miniso_payload_exists)

    def test_pipeline_consumes_pre_keep_mail_only_workbook_before_full_screening(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            for name in ("alpha.eml", "beta.eml"):
                (shared_raw_dir / name).write_text(f"Subject: {name}\n\nbody", encoding="utf-8")

            uploaded_payloads: list[dict[str, Any]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "MINISO_keep.xlsx"
                pre_keep_workbook = output_root / "MINISO_pre_keep_mail_only.xlsx"
                template_workbook = output_root / "MINISO_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "beta",
                            "URL": "https://www.tiktok.com/@beta",
                            "brand_message_sent_at": "2026-03-31T11:00:00+08:00",
                            "brand_message_snippet": "Latest beta reply $200",
                            "brand_message_raw_path": "raw/beta.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                pd.DataFrame(
                    [
                        {
                            "Platform": "Instagram",
                            "@username": "alpha",
                            "URL": "https://www.instagram.com/alpha",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest alpha reply $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        }
                    ]
                ).to_excel(pre_keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                            "pre_keep_mail_only_workbook": str(pre_keep_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                        "pre_keep_mail_only_workbook": str(pre_keep_workbook),
                    },
                    "steps": {
                        "brand_match": {"stats": {"message_hit_count": 2}},
                        "pre_keep_short_circuit": {"status": "completed", "mail_only_count": 1, "full_screening_count": 1},
                    },
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": "MINISO",
                            "linked_bitable_url": "https://bitable.example/miniso",
                            "responsible_name": "陈俊仁",
                            "employee_name": "陈俊仁",
                            "employee_id": "ou_miniso",
                            "employee_record_id": "rec_miniso",
                            "employee_email": "miniso@amagency.biz",
                            "owner_name": "陈俊仁",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                keep_rows = pd.read_excel(Path(kwargs["keep_workbook"])).to_dict(orient="records")
                self.assertEqual(len(keep_rows), 1)
                self.assertEqual(keep_rows[0]["@username"], "beta")
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_upload_payload.json"
                pd.DataFrame(
                    [
                        {
                            "达人ID": "beta",
                            "平台": "tiktok",
                            "主页链接": "https://www.tiktok.com/@beta",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "是",
                            "ai筛号反馈理由": "screened",
                            "标签(ai)": "母婴用品-家庭/宝妈",
                            "ai评价": "good fit",
                        }
                    ]
                ).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": [],
                    "source_row_count": 1,
                    "row_count": 1,
                    "skipped_row_count": 0,
                    "rows": [{"达人ID": "beta", "平台": "tiktok", "ai是否通过": "是"}],
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, *, payload_json_path, linked_bitable_url="", **kwargs):
                payload = json.loads(Path(payload_json_path).read_text(encoding="utf-8"))
                uploaded_payloads.append(payload)
                archive_dir = Path(payload_json_path).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": str(payload_json_path),
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 0,
                    "updated_count": 2,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(
                        index={
                            "ou_miniso::alpha::instagram": {
                                "record_id": "rec_alpha",
                                "fields": {
                                    "达人ID": "alpha",
                                    "平台": "instagram",
                                    "主页链接": "https://www.instagram.com/alpha",
                                    "ai 是否通过": "是",
                                    "ai筛号反馈理由": "existing ok",
                                    "标签（ai）": ["家庭用品和家电-家庭博主"],
                                    "ai 评价": "already screened",
                                },
                            },
                            "ou_miniso::beta::tiktok": {
                                "record_id": "rec_beta",
                                "fields": {
                                    "达人ID": "beta",
                                    "平台": "tiktok",
                                    "主页链接": "https://www.tiktok.com/@beta",
                                    "ai 是否通过": "",
                                },
                            },
                        },
                        owner_scope_field_name="达人对接人",
                        duplicate_groups=[],
                    ),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_miniso",
                            "taskName": "MINISO",
                            "linkedBitableUrl": "https://bitable.example/miniso",
                            "employeeId": "ou_miniso",
                            "employeeRecordId": "rec_miniso_emp",
                            "employeeName": "陈俊仁",
                            "employeeEmail": "chenjunren@amagency.biz",
                            "responsibleName": "陈俊仁",
                            "ownerName": "陈俊仁",
                        }
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                env_file=".env",
                output_root=root / "run",
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["pre_keep_mail_only_count"], 1)
        self.assertEqual(summary["mail_only_update_count"], 1)
        self.assertEqual(summary["full_screening_count"], 1)
        payload_rows = uploaded_payloads[0]["rows"]
        self.assertEqual({row["达人ID"] for row in payload_rows}, {"alpha", "beta"})
        mode_by_creator = {row["达人ID"]: row["__feishu_update_mode"] for row in payload_rows}
        self.assertEqual(mode_by_creator["alpha"], "mail_only_update")

    def test_pipeline_mail_first_only_skips_downstream_and_uploads_mail_fields_for_all_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            for name in ("alpha.eml", "beta.eml", "gamma.eml"):
                (shared_raw_dir / name).write_text(f"Subject: {name}\n\nbody", encoding="utf-8")

            uploaded_payloads: list[dict[str, Any]] = []
            downstream_called = {"value": False}

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "MINISO_keep.xlsx"
                pre_keep_workbook = output_root / "MINISO_pre_keep_mail_only.xlsx"
                template_workbook = output_root / "MINISO_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "beta",
                            "URL": "https://www.tiktok.com/@beta",
                            "brand_message_sent_at": "2026-03-31T11:00:00+08:00",
                            "brand_message_snippet": "Latest beta reply $200",
                            "brand_message_raw_path": "raw/beta.eml",
                        },
                        {
                            "Platform": "Instagram",
                            "@username": "gamma",
                            "URL": "https://www.instagram.com/gamma",
                            "brand_message_sent_at": "2026-03-31T12:00:00+08:00",
                            "brand_message_snippet": "Latest gamma reply $300",
                            "brand_message_raw_path": "raw/gamma.eml",
                        },
                    ]
                ).to_excel(keep_workbook, index=False)
                pd.DataFrame(
                    [
                        {
                            "Platform": "Instagram",
                            "@username": "alpha",
                            "URL": "https://www.instagram.com/alpha",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest alpha reply $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        }
                    ]
                ).to_excel(pre_keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                            "pre_keep_mail_only_workbook": str(pre_keep_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                        "pre_keep_mail_only_workbook": str(pre_keep_workbook),
                    },
                    "steps": {
                        "brand_match": {"stats": {"message_hit_count": 3}},
                        "pre_keep_short_circuit": {"status": "completed", "mail_only_count": 1, "full_screening_count": 2},
                    },
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": "MINISO",
                            "linked_bitable_url": "https://bitable.example/miniso",
                            "responsible_name": "陈俊仁",
                            "employee_name": "陈俊仁",
                            "employee_id": "ou_miniso",
                            "employee_record_id": "rec_miniso",
                            "employee_email": "miniso@amagency.biz",
                            "owner_name": "陈俊仁",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                downstream_called["value"] = True
                raise AssertionError("downstream should be skipped in mail-first-only mode")

            def fake_upload_final_review_payload_to_bitable(client, *, payload_json_path, linked_bitable_url="", **kwargs):
                payload = json.loads(Path(payload_json_path).read_text(encoding="utf-8"))
                uploaded_payloads.append(payload)
                archive_dir = Path(payload_json_path).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                created_count = 0
                updated_count = 0
                for row in payload.get("rows") or []:
                    if row["达人ID"] == "gamma":
                        created_count += 1
                    else:
                        updated_count += 1
                return {
                    "ok": True,
                    "payload_json_path": str(payload_json_path),
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": created_count,
                    "updated_count": updated_count,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(
                        index={
                            "alpha::instagram": {
                                "record_id": "rec_alpha",
                                "fields": {
                                    "达人ID": "alpha",
                                    "平台": "instagram",
                                    "主页链接": "https://www.instagram.com/alpha",
                                    "ai 是否通过": "是",
                                    "ai筛号反馈理由": "existing ok",
                                    "标签（ai）": ["家庭用品和家电-家庭博主"],
                                    "ai 评价": "already screened",
                                },
                            },
                            "beta::tiktok": {
                                "record_id": "rec_beta",
                                "fields": {
                                    "达人ID": "beta",
                                    "平台": "tiktok",
                                    "主页链接": "https://www.tiktok.com/@beta",
                                    "ai 是否通过": "",
                                },
                            },
                        },
                        duplicate_groups=[],
                    ),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_miniso",
                            "taskName": "MINISO",
                            "linkedBitableUrl": "https://bitable.example/miniso",
                            "employeeId": "ou_miniso",
                            "employeeRecordId": "rec_miniso_emp",
                            "employeeName": "陈俊仁",
                            "employeeEmail": "chenjunren@amagency.biz",
                            "responsibleName": "陈俊仁",
                            "ownerName": "陈俊仁",
                        }
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                env_file=".env",
                output_root=root / "run",
                mail_first_only=True,
            )

        self.assertFalse(downstream_called["value"])
        self.assertEqual(summary["status"], "completed")
        self.assertTrue(summary["mail_first_only_enabled"])
        self.assertEqual(summary["pre_keep_mail_only_count"], 1)
        self.assertEqual(summary["mail_only_update_count"], 1)
        self.assertEqual(summary["full_screening_count"], 2)
        self.assertEqual(summary["mail_first_only_count"], 3)
        self.assertEqual(summary["created_record_count"], 1)
        self.assertEqual(summary["updated_record_count"], 2)

        task_result = summary["task_results"][0]
        self.assertTrue(task_result["mail_first_only_enabled"])
        self.assertEqual(task_result["mail_first_only_count"], 3)
        self.assertEqual(task_result["full_screening_count"], 2)
        self.assertEqual(task_result["mail_only_update_count"], 1)
        self.assertEqual(task_result["mail_only_candidate_count_before_source_dedup"], 3)
        self.assertEqual(task_result["mail_only_candidate_count_after_source_dedup"], 3)
        self.assertEqual(task_result["mail_only_routed_mail_only_candidate_count"], 1)
        self.assertEqual(task_result["mail_only_routed_full_screening_candidate_count"], 2)
        self.assertTrue(task_result["mail_only_candidate_log_json"].endswith("mail_only_candidate_log.json"))
        self.assertTrue(task_result["mail_only_upload_decision_json"].endswith("mail_only_upload_decisions.json"))

        payload_rows = uploaded_payloads[0]["rows"]
        self.assertEqual({row["达人ID"] for row in payload_rows}, {"alpha", "beta", "gamma"})
        self.assertEqual(
            {row["达人ID"]: row["__feishu_update_mode"] for row in payload_rows},
            {
                "alpha": "create_or_mail_only_update",
                "beta": "create_or_mail_only_update",
                "gamma": "create_or_mail_only_update",
            },
        )
        for row in payload_rows:
            self.assertEqual(row["ai是否通过"], "待补充")
            self.assertEqual(row["ai筛号反馈理由"], "")
            self.assertEqual(row["标签(ai)"], "")
            self.assertEqual(row["ai评价"], "")

        alpha_row = next(row for row in payload_rows if row["达人ID"] == "alpha")
        self.assertEqual(alpha_row["达人最后一次回复邮件时间"], "2026/03/31")
        self.assertEqual(alpha_row["full body"], "Latest alpha reply $100")
        self.assertTrue(alpha_row["__feishu_attachment_local_paths"][0].endswith("alpha.eml"))

    def test_pipeline_thread_first_mail_resolution_writes_unresolved_threads_archive(self) -> None:
        original_apply_creator_reply_context = pipeline._apply_creator_reply_context
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "alpha.eml").write_text("Subject: alpha\n\nbody", encoding="utf-8")

            uploaded_payloads: list[dict[str, Any]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "MINISO_keep.xlsx"
                unresolved_workbook = output_root / "MINISO_unresolved_threads.xlsx"
                template_workbook = output_root / "MINISO_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "Instagram",
                            "@username": "alpha",
                            "URL": "https://www.instagram.com/alpha",
                            "evidence_thread_key": "thread-alpha",
                            "brand_message_sent_at": "2026-04-07T10:00:00+08:00",
                            "brand_message_snippet": "Latest alpha reply $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                            "latest_external_full_body": "Latest alpha reply $100",
                            "matched_contact_email": "alpha@example.com",
                            "final_id_final": "alpha",
                            "final_creator_id": "alpha",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                pd.DataFrame(
                    [
                        {
                            "thread_key": "thread-unresolved",
                            "subject": "Re: MINISO",
                            "thread_resolution_status": "unresolved",
                            "resolution_stage_final": "llm",
                            "resolution_evidence": "ambiguous creator identity",
                        }
                    ]
                ).to_excel(unresolved_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                            "pre_keep_mail_only_workbook": "",
                            "unresolved_threads_xlsx": str(unresolved_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {
                        "brand_match": {"stats": {"message_hit_count": 439, "thread_hit_count": 425}},
                        "mail_funnel": {
                            "stats": {
                                "resolved_thread_count": 1,
                                "keep_row_count": 1,
                                "thread_first_resolution": True,
                            },
                            "artifacts": {
                                "unresolved_threads_xlsx": str(unresolved_workbook),
                            },
                        },
                        "pre_keep_short_circuit": {"status": "skipped", "mail_only_count": 0, "full_screening_count": 1},
                    },
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": "MINISO",
                            "linked_bitable_url": "https://bitable.example/miniso",
                            "responsible_name": "陈俊仁",
                            "employee_name": "陈俊仁",
                            "employee_id": "ou_miniso",
                            "employee_record_id": "rec_miniso",
                            "employee_email": "miniso@amagency.biz",
                            "owner_name": "陈俊仁",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, *, payload_json_path, linked_bitable_url="", **kwargs):
                payload = json.loads(Path(payload_json_path).read_text(encoding="utf-8"))
                uploaded_payloads.append(payload)
                archive_dir = Path(payload_json_path).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": str(payload_json_path),
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 1,
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_miniso",
                            "taskName": "MINISO",
                            "linkedBitableUrl": "https://bitable.example/miniso",
                            "employeeId": "ou_miniso",
                            "employeeRecordId": "rec_miniso_emp",
                            "employeeName": "陈俊仁",
                            "employeeEmail": "chenjunren@amagency.biz",
                            "responsibleName": "陈俊仁",
                            "ownerName": "陈俊仁",
                        }
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": lambda **kwargs: (_ for _ in ()).throw(AssertionError("downstream should not run")),
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }
            pipeline._apply_creator_reply_context = lambda keep_row, *, shared_mail_db_path: (
                dict(keep_row),
                {"status": "creator_replied", "creator_replied": True},
            )

            try:
                summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                    shared_mail_db_path=shared_db_path,
                    shared_mail_raw_dir=shared_raw_dir,
                    shared_mail_data_dir=shared_root,
                    env_file=".env",
                    output_root=root / "run",
                    mail_first_only=True,
                    thread_first_mail_resolution=True,
                )
            finally:
                pipeline._apply_creator_reply_context = original_apply_creator_reply_context

            self.assertTrue(Path(summary["task_results"][0]["unresolved_threads_json"]).exists())
            self.assertTrue(Path(summary["task_results"][0]["unresolved_threads_xlsx"]).exists())
            unresolved_payload = json.loads(
                Path(summary["task_results"][0]["unresolved_threads_json"]).read_text(encoding="utf-8")
            )
            self.assertEqual(unresolved_payload["unresolved_thread_count"], 1)
            self.assertEqual(unresolved_payload["rows"][0]["thread_key"], "thread-unresolved")
            self.assertEqual(uploaded_payloads[0]["row_count"], 1)

        self.assertEqual(summary["status"], "completed")
        self.assertTrue(summary["thread_first_mail_resolution_enabled"])
        self.assertEqual(summary["keyword_hit_thread_count"], 425)
        self.assertEqual(summary["resolved_thread_count"], 1)
        self.assertEqual(summary["written_row_count"], 1)
        task_result = summary["task_results"][0]
        self.assertEqual(task_result["keyword_hit_thread_count"], 425)
        self.assertEqual(task_result["resolved_thread_count"], 1)

    def test_project_name_filter_expands_numbered_tasks_without_hardcoded_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "duet.eml").write_text("Subject: duet\n\nbody", encoding="utf-8")

            upstream_calls: list[dict[str, object]] = []

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                upstream_calls.append(
                    {
                        "task_name": kwargs["task_name"],
                        "brand_keyword": kwargs["brand_keyword"],
                        "matching_strategy": kwargs["matching_strategy"],
                    }
                )
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / f"{kwargs['task_name']}_keep.xlsx"
                template_workbook = output_root / f"{kwargs['task_name']}_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "TikTok",
                            "@username": "duet",
                            "URL": "https://www.tiktok.com/@duet",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest duet reply $100",
                            "brand_message_raw_path": "raw/duet.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": kwargs["task_name"],
                            "linked_bitable_url": f"https://bitable.example/{kwargs['task_name']}",
                            "responsible_name": "唐瑞霞",
                            "employee_name": "唐瑞霞",
                            "employee_id": "ou_rhea",
                            "employee_record_id": f"rec_{kwargs['task_name']}",
                            "employee_email": "rhea@amagency.biz",
                            "owner_name": "rhea@amagency.biz",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_upload_payload.json"
                pd.DataFrame(
                    [
                        {
                            "达人ID": "duet",
                            "平台": "tiktok",
                            "主页链接": "https://www.tiktok.com/@duet",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "否",
                            "ai筛号反馈理由": "screened",
                            "标签(ai)": "",
                            "ai评价": "good fit",
                        }
                    ]
                ).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": [],
                    "source_row_count": 1,
                    "row_count": 1,
                    "skipped_row_count": 0,
                    "rows": [{"达人ID": "duet", "平台": "tiktok", "ai是否通过": "否"}],
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 1,
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 0,
                    "failed_rows": [],
                    "skipped_existing_rows": [],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {"recordId": "rec_duet_1", "taskName": "Duet1", "linkedBitableUrl": "https://bitable.example/duet1"},
                        {"recordId": "rec_duet_2", "taskName": "Duet2", "linkedBitableUrl": "https://bitable.example/duet2"},
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                task_name_filters=["Duet"],
                upload_dry_run=False,
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["task_names"], ["Duet1", "Duet2"])
        self.assertEqual(
            [(item["task_name"], item["brand_keyword"], item["matching_strategy"]) for item in upstream_calls],
            [("Duet1", "Duet", "brand-keyword-fast-path"), ("Duet2", "Duet", "brand-keyword-fast-path")],
        )

    def test_pipeline_blocks_ambiguous_task_owner_before_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)

            upstream_called = False

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                nonlocal upstream_called
                upstream_called = True
                raise AssertionError("ambiguous owner task should be blocked before upstream")

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {
                            "recordId": "rec_miniso",
                            "taskName": "MINISO",
                            "linkedBitableUrl": "https://bitable.example/miniso",
                            "employeeId": "ou_primary",
                            "employeeRecordId": "rec_primary",
                            "employeeName": "陈俊仁",
                            "employeeEmail": "chenjunren@amagency.biz",
                            "responsibleName": "陈俊仁",
                            "ownerName": "chenjunren@amagency.biz,eden@amagency.biz",
                            "ownerEmailCandidates": ["chenjunren@amagency.biz", "eden@amagency.biz"],
                            "ownerMatchAmbiguous": True,
                            "employeeMatches": [
                                {
                                    "employeeRecordId": "rec_primary",
                                    "employeeId": "ou_primary",
                                    "employeeName": "陈俊仁",
                                    "employeeEmail": "chenjunren@amagency.biz",
                                    "imapCode": "imap-chen-123",
                                    "matchedBy": "owner_email",
                                    "matchedValue": "chenjunren@amagency.biz",
                                },
                                {
                                    "employeeRecordId": "rec_eden",
                                    "employeeId": "ou_eden",
                                    "employeeName": "Eden",
                                    "employeeEmail": "eden@amagency.biz",
                                    "imapCode": "imap-eden-456",
                                    "matchedBy": "owner_email",
                                    "matchedValue": "eden@amagency.biz",
                                },
                            ],
                        }
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": lambda **kwargs: {},
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": lambda client, **kwargs: {},
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                shared_mail_data_dir=shared_root,
                env_file=".env",
                output_root=root / "run",
            )

        self.assertFalse(upstream_called)
        self.assertEqual(summary["status"], "completed_with_failures")
        self.assertEqual(summary["failed_record_count"], 1)
        self.assertEqual(summary["task_names"], ["MINISO"])
        task_result = summary["task_results"][0]
        self.assertEqual(task_result["status"], "inspection_failed")
        self.assertEqual(task_result["failure"]["error_code"], "TASK_OWNER_MATCH_AMBIGUOUS")

    def test_skipped_existing_rows_do_not_count_as_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)
            (shared_raw_dir / "alpha.eml").write_text("Subject: alpha\n\nbody", encoding="utf-8")

            class FakeClient:
                pass

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "MINISO_keep.xlsx"
                template_workbook = output_root / "MINISO_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "Instagram",
                            "@username": "alpha",
                            "URL": "https://www.instagram.com/alpha",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest alpha reply $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": "MINISO",
                            "linked_bitable_url": "https://bitable.example/miniso",
                            "responsible_name": "陈俊仁",
                            "employee_name": "陈俊仁",
                            "employee_id": "ou_miniso",
                            "employee_record_id": "rec_miniso",
                            "employee_email": "miniso@amagency.biz",
                            "owner_name": "陈俊仁",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                exports_dir = output_root / "exports"
                exports_dir.mkdir(parents=True, exist_ok=True)
                final_review_path = exports_dir / "all_platforms_final_review.xlsx"
                payload_path = exports_dir / "all_platforms_upload_payload.json"
                pd.DataFrame(
                    [
                        {
                            "达人ID": "alpha",
                            "平台": "instagram",
                            "主页链接": "https://www.instagram.com/alpha",
                            "达人对接人": kwargs["task_owner_name"],
                            "ai是否通过": "是",
                            "ai筛号反馈理由": "screened",
                            "标签(ai)": "家庭用品和家电-家庭博主",
                            "ai评价": "good fit",
                        }
                    ]
                ).to_excel(final_review_path, index=False)
                payload = {
                    "task_owner": {"task_name": kwargs["task_name"]},
                    "columns": [],
                    "source_row_count": 1,
                    "row_count": 1,
                    "skipped_row_count": 0,
                    "rows": [{"达人ID": "alpha", "平台": "instagram", "ai是否通过": "是"}],
                    "skipped_rows": [],
                }
                payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                summary = {
                    "status": "completed",
                    "artifacts": {
                        "all_platforms_final_review": str(final_review_path),
                        "all_platforms_upload_payload_json": str(payload_path),
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_upload_final_review_payload_to_bitable(client, **kwargs):
                archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                result_json = archive_dir / "feishu_bitable_upload_result.json"
                result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
                result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
                pd.DataFrame([{"status": "ok"}]).to_excel(result_xlsx, index=False)
                return {
                    "ok": True,
                    "payload_json_path": kwargs["payload_json_path"],
                    "result_json_path": str(result_json),
                    "result_xlsx_path": str(result_xlsx),
                    "created_count": 0,
                    "updated_count": 0,
                    "failed_count": 0,
                    "skipped_existing_count": 1,
                    "failed_rows": [],
                    "skipped_existing_rows": [
                        {
                            "reason": "飞书表已存在同达人ID+平台记录",
                            "row": {"达人ID": "alpha", "平台": "instagram"},
                        }
                    ],
                }

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(index={}, duplicate_groups=[]),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {"recordId": "rec_miniso", "taskName": "MINISO", "linkedBitableUrl": "https://bitable.example/miniso"},
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                task_name_filters=["MINISO"],
                upload_dry_run=False,
            )

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["failed_record_count"], 0)
            self.assertEqual(summary["skipped_existing_count"], 1)
            self.assertTrue(Path(summary["aggregate_existing_skip_json"]).exists())
            self.assertTrue(Path(summary["aggregate_existing_skip_xlsx"]).exists())
            task_result = summary["task_results"][0]
            self.assertEqual(task_result["status"], "completed")
            self.assertEqual(task_result["failed_count"], 0)
            self.assertEqual(task_result["skipped_existing_count"], 1)

    def test_pipeline_blocks_before_downstream_when_target_table_has_duplicate_existing_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shared_root = root / "shared_mailbox"
            shared_db_path = shared_root / "email_sync.db"
            shared_raw_dir = shared_root / "raw"
            shared_db_path.parent.mkdir(parents=True, exist_ok=True)
            shared_db_path.touch()
            shared_raw_dir.mkdir(parents=True, exist_ok=True)

            class FakeClient:
                pass

            downstream_called = {"value": False}

            def fake_run_task_upload_to_keep_list_pipeline(**kwargs):
                output_root = Path(kwargs["output_root"])
                output_root.mkdir(parents=True, exist_ok=True)
                keep_workbook = output_root / "MINISO_keep.xlsx"
                template_workbook = output_root / "MINISO_template.xlsx"
                template_workbook.touch()
                pd.DataFrame(
                    [
                        {
                            "Platform": "Instagram",
                            "@username": "alpha",
                            "URL": "https://www.instagram.com/alpha",
                            "brand_message_sent_at": "2026-03-31T10:00:00+08:00",
                            "brand_message_snippet": "Latest alpha reply $100",
                            "brand_message_raw_path": "raw/alpha.eml",
                        }
                    ]
                ).to_excel(keep_workbook, index=False)
                summary = {
                    "status": "stopped_after_keep-list",
                    "resume_points": {
                        "keep_list": {
                            "keep_workbook": str(keep_workbook),
                            "template_workbook": str(template_workbook),
                        }
                    },
                    "artifacts": {
                        "keep_workbook": str(keep_workbook),
                        "template_workbook": str(template_workbook),
                    },
                    "steps": {"brand_match": {"stats": {"message_hit_count": 1}}},
                    "downstream_handoff": {
                        "task_owner": {
                            "task_name": "MINISO",
                            "linked_bitable_url": "https://bitable.example/miniso",
                            "responsible_name": "陈俊仁",
                            "employee_name": "陈俊仁",
                            "employee_id": "ou_miniso",
                            "employee_record_id": "rec_miniso",
                            "employee_email": "miniso@amagency.biz",
                            "owner_name": "陈俊仁",
                        }
                    },
                }
                Path(kwargs["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
                return summary

            def fake_run_keep_list_screening_pipeline(**kwargs):
                downstream_called["value"] = True
                raise AssertionError("downstream should not run when duplicate existing records are detected")

            pipeline._build_feishu_client = lambda **kwargs: (
                FakeClient(),
                {
                    "TASK_UPLOAD_URL": "https://task-upload.example.com",
                    "EMPLOYEE_INFO_URL": "https://employee.example.com",
                },
                {"feishu_base_url": "https://open.feishu.cn", "timeout_seconds": 30.0},
            )
            pipeline._load_runtime_dependencies = lambda: {
                "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
                "FeishuOpenClient": FakeClient,
                "fetch_existing_bitable_record_analysis": lambda client, *, linked_bitable_url: (
                    object(),
                    SimpleNamespace(
                        index={"alpha::instagram": {"record_id": "rec_keep", "fields": {"达人ID": "alpha", "平台": "instagram"}}},
                        duplicate_groups=[
                            {
                                "record_key": "alpha::instagram",
                                "creator_id": "alpha",
                                "platform": "instagram",
                                "keep_record": {"record_id": "rec_keep", "fields": {"达人ID": "alpha", "平台": "instagram"}},
                                "duplicate_records": [
                                    {"record_id": "rec_dup", "fields": {"达人ID": "alpha", "平台": "instagram"}}
                                ],
                            }
                        ],
                    ),
                ),
                "fetch_existing_bitable_record_index": lambda client, *, linked_bitable_url: (object(), {}),
                "inspect_task_upload_assignments": lambda **kwargs: {
                    "items": [
                        {"recordId": "rec_miniso", "taskName": "MINISO", "linkedBitableUrl": "https://bitable.example/miniso"},
                    ]
                },
                "load_local_env": lambda env_file: {},
                "run_keep_list_screening_pipeline": fake_run_keep_list_screening_pipeline,
                "run_task_upload_to_keep_list_pipeline": fake_run_task_upload_to_keep_list_pipeline,
                "upload_final_review_payload_to_bitable": lambda client, **kwargs: {"ok": True},
            }

            summary = pipeline.run_shared_mailbox_post_sync_pipeline(
                shared_mail_db_path=shared_db_path,
                shared_mail_raw_dir=shared_raw_dir,
                task_name_filters=["MINISO"],
                upload_dry_run=False,
            )

            self.assertFalse(downstream_called["value"])
            self.assertEqual(summary["status"], "completed_with_failures")
            task_result = summary["task_results"][0]
            self.assertEqual(task_result["status"], "guard_blocked_duplicate_existing")
            self.assertEqual(task_result["duplicate_existing_group_count"], 1)
