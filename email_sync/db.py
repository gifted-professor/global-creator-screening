from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .mail_parser import ParsedMessage


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MessageQuery:
    folders: Optional[List[str]] = None
    from_contains: Optional[str] = None
    subject_contains: Optional[str] = None
    keyword: Optional[str] = None
    attachment_name: Optional[str] = None
    sent_after: Optional[str] = None
    sent_before: Optional[str] = None
    has_attachments: Optional[bool] = None
    limit: int = 20


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS mailboxes (
                account_email TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                imap_name TEXT NOT NULL,
                delimiter TEXT,
                flags_json TEXT NOT NULL,
                uidvalidity INTEGER,
                message_count_on_server INTEGER,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (account_email, folder_name)
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                uid INTEGER NOT NULL,
                uidvalidity INTEGER NOT NULL,
                message_id TEXT,
                subject TEXT,
                in_reply_to TEXT,
                references_header TEXT,
                sent_at TEXT,
                sent_at_raw TEXT,
                internal_date TEXT,
                internal_date_raw TEXT,
                flags_json TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                from_json TEXT NOT NULL,
                to_json TEXT NOT NULL,
                cc_json TEXT NOT NULL,
                bcc_json TEXT NOT NULL,
                reply_to_json TEXT NOT NULL,
                sender_json TEXT NOT NULL,
                body_text TEXT,
                body_html TEXT,
                snippet TEXT,
                headers_json TEXT NOT NULL,
                raw_path TEXT NOT NULL,
                raw_sha256 TEXT NOT NULL,
                raw_size_bytes INTEGER NOT NULL,
                has_attachments INTEGER NOT NULL,
                attachment_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (account_email, folder_name, uidvalidity, uid)
            );

            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_row_id INTEGER NOT NULL,
                part_index INTEGER NOT NULL,
                filename TEXT,
                content_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                content_id TEXT,
                content_disposition TEXT,
                is_inline INTEGER NOT NULL,
                FOREIGN KEY (message_row_id) REFERENCES messages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                account_email TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                uidvalidity INTEGER,
                last_seen_uid INTEGER NOT NULL DEFAULT 0,
                last_run_synced INTEGER NOT NULL DEFAULT 0,
                last_sync_started_at TEXT,
                last_sync_completed_at TEXT,
                last_error TEXT,
                PRIMARY KEY (account_email, folder_name)
            );

            CREATE TABLE IF NOT EXISTS sync_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                uid INTEGER,
                stage TEXT NOT NULL,
                error_message TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_normalized TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL DEFAULT '',
                first_seen_at TEXT,
                last_seen_at TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                inbound_message_count INTEGER NOT NULL DEFAULT 0,
                outbound_message_count INTEGER NOT NULL DEFAULT 0,
                thread_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_index (
                message_row_id INTEGER PRIMARY KEY,
                normalized_subject TEXT NOT NULL,
                direction TEXT NOT NULL,
                thread_key TEXT NOT NULL,
                thread_root_message_id TEXT,
                thread_parent_message_id TEXT,
                thread_depth INTEGER NOT NULL DEFAULT 0,
                sent_sort_at TEXT,
                external_contact_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (message_row_id) REFERENCES messages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS message_contacts (
                message_row_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (message_row_id, contact_id, role),
                FOREIGN KEY (message_row_id) REFERENCES messages(id) ON DELETE CASCADE,
                FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS threads (
                thread_key TEXT PRIMARY KEY,
                normalized_subject TEXT NOT NULL,
                example_subject TEXT NOT NULL DEFAULT '',
                thread_root_message_id TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                external_contact_count INTEGER NOT NULL DEFAULT 0,
                first_sent_at TEXT,
                last_sent_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS thread_contacts (
                thread_key TEXT NOT NULL,
                contact_id INTEGER NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                first_sent_at TEXT,
                last_sent_at TEXT,
                PRIMARY KEY (thread_key, contact_id),
                FOREIGN KEY (thread_key) REFERENCES threads(thread_key) ON DELETE CASCADE,
                FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_messages_folder ON messages(folder_name, sent_at);
            CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id);
            CREATE INDEX IF NOT EXISTS idx_messages_subject ON messages(subject);
            CREATE INDEX IF NOT EXISTS idx_attachments_message_row_id ON attachments(message_row_id);
            CREATE INDEX IF NOT EXISTS idx_contacts_message_count ON contacts(message_count DESC, last_seen_at DESC);
            CREATE INDEX IF NOT EXISTS idx_contacts_last_seen_at ON contacts(last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_message_index_thread_key ON message_index(thread_key);
            CREATE INDEX IF NOT EXISTS idx_message_index_subject ON message_index(normalized_subject);
            CREATE INDEX IF NOT EXISTS idx_threads_last_sent_at ON threads(last_sent_at DESC);
            CREATE INDEX IF NOT EXISTS idx_thread_contacts_contact_id ON thread_contacts(contact_id, last_sent_at DESC);
            CREATE INDEX IF NOT EXISTS idx_message_contacts_contact_id ON message_contacts(contact_id);
            """
        )
        self.conn.commit()

    def record_mailbox(
        self,
        account_email: str,
        folder_name: str,
        imap_name: str,
        delimiter: Optional[str],
        flags: List[str],
        uidvalidity: Optional[int],
        message_count_on_server: Optional[int],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO mailboxes (
                account_email, folder_name, imap_name, delimiter, flags_json, uidvalidity, message_count_on_server, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_email, folder_name)
            DO UPDATE SET
                imap_name = excluded.imap_name,
                delimiter = excluded.delimiter,
                flags_json = excluded.flags_json,
                uidvalidity = excluded.uidvalidity,
                message_count_on_server = excluded.message_count_on_server,
                last_seen_at = excluded.last_seen_at
            """,
            (
                account_email,
                folder_name,
                imap_name,
                delimiter,
                json.dumps(flags, ensure_ascii=False),
                uidvalidity,
                message_count_on_server,
                _utc_now(),
            ),
        )
        self.conn.commit()

    def get_sync_state(self, account_email: str, folder_name: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT *
            FROM sync_state
            WHERE account_email = ? AND folder_name = ?
            """,
            (account_email, folder_name),
        ).fetchone()

    def update_sync_state(
        self,
        account_email: str,
        folder_name: str,
        uidvalidity: Optional[int],
        last_seen_uid: int,
        last_run_synced: int,
        last_sync_started_at: Optional[str],
        last_sync_completed_at: Optional[str],
        last_error: Optional[str] = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_state (
                account_email, folder_name, uidvalidity, last_seen_uid, last_run_synced, last_sync_started_at, last_sync_completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_email, folder_name)
            DO UPDATE SET
                uidvalidity = excluded.uidvalidity,
                last_seen_uid = excluded.last_seen_uid,
                last_run_synced = excluded.last_run_synced,
                last_sync_started_at = excluded.last_sync_started_at,
                last_sync_completed_at = excluded.last_sync_completed_at,
                last_error = excluded.last_error
            """,
            (
                account_email,
                folder_name,
                uidvalidity,
                last_seen_uid,
                last_run_synced,
                last_sync_started_at,
                last_sync_completed_at,
                last_error,
            ),
        )
        self.conn.commit()

    def record_sync_error(
        self,
        account_email: str,
        folder_name: str,
        uid: Optional[int],
        stage: str,
        error_message: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_errors (account_email, folder_name, uid, stage, error_message, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (account_email, folder_name, uid, stage, error_message, _utc_now()),
        )
        self.conn.commit()

    def upsert_message(self, parsed: ParsedMessage, raw_path: str, raw_sha256: str, raw_size_bytes: int) -> int:
        now = _utc_now()
        self.conn.execute(
            """
            INSERT INTO messages (
                account_email, folder_name, uid, uidvalidity, message_id, subject, in_reply_to, references_header,
                sent_at, sent_at_raw, internal_date, internal_date_raw, flags_json, size_bytes,
                from_json, to_json, cc_json, bcc_json, reply_to_json, sender_json,
                body_text, body_html, snippet, headers_json, raw_path, raw_sha256, raw_size_bytes,
                has_attachments, attachment_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_email, folder_name, uidvalidity, uid)
            DO UPDATE SET
                message_id = excluded.message_id,
                subject = excluded.subject,
                in_reply_to = excluded.in_reply_to,
                references_header = excluded.references_header,
                sent_at = excluded.sent_at,
                sent_at_raw = excluded.sent_at_raw,
                internal_date = excluded.internal_date,
                internal_date_raw = excluded.internal_date_raw,
                flags_json = excluded.flags_json,
                size_bytes = excluded.size_bytes,
                from_json = excluded.from_json,
                to_json = excluded.to_json,
                cc_json = excluded.cc_json,
                bcc_json = excluded.bcc_json,
                reply_to_json = excluded.reply_to_json,
                sender_json = excluded.sender_json,
                body_text = excluded.body_text,
                body_html = excluded.body_html,
                snippet = excluded.snippet,
                headers_json = excluded.headers_json,
                raw_path = excluded.raw_path,
                raw_sha256 = excluded.raw_sha256,
                raw_size_bytes = excluded.raw_size_bytes,
                has_attachments = excluded.has_attachments,
                attachment_count = excluded.attachment_count,
                updated_at = excluded.updated_at
            """,
            (
                parsed.account_email,
                parsed.folder_name,
                parsed.uid,
                parsed.uidvalidity,
                parsed.message_id,
                parsed.subject,
                parsed.in_reply_to,
                parsed.references_header,
                parsed.sent_at,
                parsed.sent_at_raw,
                parsed.internal_date,
                parsed.internal_date_raw,
                json.dumps(parsed.flags, ensure_ascii=False),
                parsed.size_bytes,
                json.dumps(parsed.from_addresses, ensure_ascii=False),
                json.dumps(parsed.to_addresses, ensure_ascii=False),
                json.dumps(parsed.cc_addresses, ensure_ascii=False),
                json.dumps(parsed.bcc_addresses, ensure_ascii=False),
                json.dumps(parsed.reply_to_addresses, ensure_ascii=False),
                json.dumps(parsed.sender_addresses, ensure_ascii=False),
                parsed.body_text,
                parsed.body_html,
                parsed.snippet,
                json.dumps(parsed.headers, ensure_ascii=False),
                raw_path,
                raw_sha256,
                raw_size_bytes,
                int(parsed.has_attachments),
                parsed.attachment_count,
                now,
                now,
            ),
        )
        message_row = self.conn.execute(
            """
            SELECT id
            FROM messages
            WHERE account_email = ? AND folder_name = ? AND uidvalidity = ? AND uid = ?
            """,
            (parsed.account_email, parsed.folder_name, parsed.uidvalidity, parsed.uid),
        ).fetchone()
        assert message_row is not None
        message_row_id = int(message_row["id"])

        self.conn.execute("DELETE FROM attachments WHERE message_row_id = ?", (message_row_id,))
        self.conn.executemany(
            """
            INSERT INTO attachments (
                message_row_id, part_index, filename, content_type, size_bytes, content_id, content_disposition, is_inline
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    message_row_id,
                    attachment.part_index,
                    attachment.filename,
                    attachment.content_type,
                    attachment.size_bytes,
                    attachment.content_id,
                    attachment.content_disposition,
                    int(attachment.is_inline),
                )
                for attachment in parsed.attachments
            ],
        )
        self.conn.commit()
        return message_row_id

    def fetch_stats(self) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT
                    folder_name,
                    COUNT(*) AS message_count,
                    SUM(has_attachments) AS messages_with_attachments,
                    MAX(sent_at) AS latest_sent_at,
                    MAX(uid) AS latest_uid
                FROM messages
                GROUP BY folder_name
                ORDER BY folder_name
                """
            ).fetchall()
        )

    def search_messages(self, query: MessageQuery) -> List[sqlite3.Row]:
        clauses, params = self._build_search_filters(query)
        limit = query.limit if query.limit > 0 else 20
        params_with_limit = [*params, limit]

        sql = f"""
            SELECT
                m.id,
                m.account_email,
                m.folder_name,
                m.uid,
                m.uidvalidity,
                m.message_id,
                m.subject,
                m.in_reply_to,
                m.references_header,
                m.sent_at,
                m.sent_at_raw,
                m.internal_date,
                m.internal_date_raw,
                m.flags_json,
                m.size_bytes,
                m.from_json,
                m.to_json,
                m.cc_json,
                m.bcc_json,
                m.reply_to_json,
                m.sender_json,
                m.body_text,
                m.body_html,
                m.snippet,
                m.headers_json,
                m.raw_path,
                m.raw_sha256,
                m.raw_size_bytes,
                m.has_attachments,
                m.attachment_count,
                m.created_at,
                m.updated_at,
                (
                    SELECT GROUP_CONCAT(COALESCE(a.filename, ''), ' | ')
                    FROM attachments a
                    WHERE a.message_row_id = m.id
                ) AS attachment_names
            FROM messages m
            WHERE {" AND ".join(clauses)}
            ORDER BY datetime(m.sent_at) DESC, m.id DESC
            LIMIT ?
        """
        return list(self.conn.execute(sql, params_with_limit).fetchall())

    def fetch_contacts(self, limit: Optional[int] = 20, contains: Optional[str] = None) -> List[sqlite3.Row]:
        clauses = ["1 = 1"]
        params: List[object] = []

        if contains:
            clauses.append(
                """
                (
                    LOWER(COALESCE(c.email_normalized, '')) LIKE ?
                    OR LOWER(COALESCE(c.display_name, '')) LIKE ?
                )
                """
            )
            keyword_like = f"%{contains.lower()}%"
            params.extend([keyword_like, keyword_like])

        sql = f"""
            SELECT
                c.id,
                c.email_normalized,
                c.display_name,
                c.first_seen_at,
                c.last_seen_at,
                c.message_count,
                c.inbound_message_count,
                c.outbound_message_count,
                c.thread_count,
                c.created_at,
                c.updated_at
            FROM contacts c
            WHERE {" AND ".join(clauses)}
            ORDER BY c.message_count DESC, datetime(c.last_seen_at) DESC, c.email_normalized
        """
        if limit is not None:
            safe_limit = limit if limit > 0 else 20
            sql = f"{sql}\nLIMIT ?"
            params.append(safe_limit)
        return list(self.conn.execute(sql, params).fetchall())

    def fetch_threads(
        self,
        limit: Optional[int] = 20,
        contact_email: Optional[str] = None,
        subject_contains: Optional[str] = None,
    ) -> List[sqlite3.Row]:
        clauses = ["1 = 1"]
        params: List[object] = []

        if contact_email:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM thread_contacts tc_filter
                    JOIN contacts c_filter ON c_filter.id = tc_filter.contact_id
                    WHERE tc_filter.thread_key = t.thread_key
                      AND c_filter.email_normalized = ?
                )
                """
            )
            params.append(contact_email)

        if subject_contains:
            clauses.append("LOWER(COALESCE(t.normalized_subject, '')) LIKE ?")
            params.append(f"%{subject_contains.lower()}%")

        sql = f"""
            SELECT
                t.thread_key,
                t.normalized_subject,
                t.example_subject,
                t.thread_root_message_id,
                t.message_count,
                t.external_contact_count,
                t.first_sent_at,
                t.last_sent_at,
                (
                    SELECT GROUP_CONCAT(label, ' | ')
                    FROM (
                        SELECT DISTINCT
                            c2.email_normalized AS email_normalized,
                            CASE
                                WHEN COALESCE(c2.display_name, '') != ''
                                    THEN c2.display_name || ' <' || c2.email_normalized || '>'
                                ELSE c2.email_normalized
                            END AS label
                        FROM thread_contacts tc2
                        JOIN contacts c2 ON c2.id = tc2.contact_id
                        WHERE tc2.thread_key = t.thread_key
                        ORDER BY c2.email_normalized
                    )
                ) AS contact_labels
            FROM threads t
            WHERE {" AND ".join(clauses)}
            ORDER BY datetime(t.last_sent_at) DESC, t.thread_key
        """
        if limit is not None:
            safe_limit = limit if limit > 0 else 20
            sql = f"{sql}\nLIMIT ?"
            params.append(safe_limit)
        return list(self.conn.execute(sql, params).fetchall())

    def fetch_indexed_messages(self) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT
                    m.id,
                    m.account_email,
                    m.folder_name,
                    m.uid,
                    m.uidvalidity,
                    m.message_id,
                    m.subject,
                    m.in_reply_to,
                    m.references_header,
                    m.sent_at,
                    m.sent_at_raw,
                    m.internal_date,
                    m.internal_date_raw,
                    m.flags_json,
                    m.size_bytes,
                    m.from_json,
                    m.to_json,
                    m.cc_json,
                    m.bcc_json,
                    m.reply_to_json,
                    m.sender_json,
                    m.body_text,
                    m.body_html,
                    m.snippet,
                    m.headers_json,
                    m.raw_path,
                    m.raw_sha256,
                    m.raw_size_bytes,
                    m.has_attachments,
                    m.attachment_count,
                    m.created_at,
                    m.updated_at,
                    mi.normalized_subject,
                    mi.direction,
                    mi.thread_key,
                    mi.thread_root_message_id,
                    mi.thread_parent_message_id,
                    mi.thread_depth,
                    mi.sent_sort_at,
                    mi.external_contact_count,
                    (
                        SELECT GROUP_CONCAT(COALESCE(a.filename, ''), ' | ')
                        FROM attachments a
                        WHERE a.message_row_id = m.id
                    ) AS attachment_names
                FROM message_index mi
                JOIN messages m ON m.id = mi.message_row_id
                ORDER BY
                    COALESCE(datetime(mi.sent_sort_at), datetime(m.sent_at), datetime(m.internal_date), datetime(m.created_at)),
                    m.id
                """
            ).fetchall()
        )

    def fetch_thread_messages(self, thread_key: str) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT
                    m.id,
                    m.account_email,
                    m.folder_name,
                    m.uid,
                    m.uidvalidity,
                    m.message_id,
                    m.subject,
                    m.in_reply_to,
                    m.references_header,
                    m.sent_at,
                    m.sent_at_raw,
                    m.internal_date,
                    m.internal_date_raw,
                    m.flags_json,
                    m.size_bytes,
                    m.from_json,
                    m.to_json,
                    m.cc_json,
                    m.bcc_json,
                    m.reply_to_json,
                    m.sender_json,
                    m.body_text,
                    m.body_html,
                    m.snippet,
                    m.headers_json,
                    m.raw_path,
                    m.raw_sha256,
                    m.raw_size_bytes,
                    m.has_attachments,
                    m.attachment_count,
                    m.created_at,
                    m.updated_at,
                    mi.normalized_subject,
                    mi.direction,
                    mi.thread_key,
                    mi.thread_root_message_id,
                    mi.thread_parent_message_id,
                    mi.thread_depth,
                    mi.sent_sort_at,
                    mi.external_contact_count,
                    (
                        SELECT GROUP_CONCAT(COALESCE(a.filename, ''), ' | ')
                        FROM attachments a
                        WHERE a.message_row_id = m.id
                    ) AS attachment_names
                FROM message_index mi
                JOIN messages m ON m.id = mi.message_row_id
                WHERE mi.thread_key = ?
                ORDER BY
                    COALESCE(datetime(mi.sent_sort_at), datetime(m.sent_at), datetime(m.internal_date), datetime(m.created_at)),
                    m.id
                """,
                (thread_key,),
            ).fetchall()
        )

    def _build_search_filters(self, query: MessageQuery) -> tuple[List[str], List[object]]:
        clauses = ["1 = 1"]
        params: List[object] = []

        if query.folders:
            placeholders = ", ".join("?" for _ in query.folders)
            clauses.append(f"m.folder_name IN ({placeholders})")
            params.extend(query.folders)

        if query.from_contains:
            clauses.append("LOWER(COALESCE(m.from_json, '')) LIKE ?")
            params.append(f"%{query.from_contains.lower()}%")

        if query.subject_contains:
            clauses.append("LOWER(COALESCE(m.subject, '')) LIKE ?")
            params.append(f"%{query.subject_contains.lower()}%")

        if query.keyword:
            clauses.append(
                """
                (
                    LOWER(COALESCE(m.subject, '')) LIKE ?
                    OR LOWER(COALESCE(m.snippet, '')) LIKE ?
                    OR LOWER(COALESCE(m.body_text, '')) LIKE ?
                    OR LOWER(COALESCE(m.body_html, '')) LIKE ?
                    OR LOWER(COALESCE(m.from_json, '')) LIKE ?
                    OR LOWER(COALESCE(m.to_json, '')) LIKE ?
                    OR EXISTS (
                        SELECT 1
                        FROM attachments a
                        WHERE a.message_row_id = m.id
                          AND LOWER(COALESCE(a.filename, '')) LIKE ?
                    )
                )
                """
            )
            keyword_like = f"%{query.keyword.lower()}%"
            params.extend([keyword_like] * 7)

        if query.attachment_name:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM attachments a
                    WHERE a.message_row_id = m.id
                      AND LOWER(COALESCE(a.filename, '')) LIKE ?
                )
                """
            )
            params.append(f"%{query.attachment_name.lower()}%")

        if query.sent_after:
            clauses.append("datetime(m.sent_at) >= datetime(?)")
            params.append(query.sent_after)

        if query.sent_before:
            clauses.append("datetime(m.sent_at) < datetime(?)")
            params.append(query.sent_before)

        if query.has_attachments is not None:
            clauses.append("m.has_attachments = ?")
            params.append(int(query.has_attachments))

        return clauses, params
