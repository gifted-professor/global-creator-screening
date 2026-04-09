from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .db import Database


MESSAGE_ID_PATTERN = re.compile(r"<[^<>\s]+>")
SUBJECT_PREFIX_PATTERN = re.compile(
    r"^\s*(?:(?:re|fw|fwd)\s*:\s*|回复\s*:\s*|转发\s*:\s*)",
    re.IGNORECASE,
)
PUBLIC_WEBMAIL_DOMAINS = {
    "163.com",
    "126.com",
    "aliyun.com",
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "outlook.com",
    "proton.me",
    "protonmail.com",
    "qq.com",
    "sina.com",
    "sohu.com",
    "yahoo.com",
    "yahoo.co.jp",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_email(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lower()


def normalize_subject(value: Optional[str]) -> str:
    subject = re.sub(r"\s+", " ", (value or "").strip())
    while subject:
        updated = SUBJECT_PREFIX_PATTERN.sub("", subject).strip()
        if updated == subject:
            break
        subject = updated
    return re.sub(r"\s+", " ", subject).strip().lower()


def normalize_message_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None

    match = MESSAGE_ID_PATTERN.search(stripped)
    if match:
        return match.group(0).lower()

    token = stripped.split()[0].strip("<>")
    if not token:
        return None
    return f"<{token.lower()}>"


def extract_message_ids(value: Optional[str]) -> List[str]:
    if not value:
        return []

    matches = [match.group(0).lower() for match in MESSAGE_ID_PATTERN.finditer(value)]
    if not matches:
        for chunk in re.split(r"[\s,]+", value.strip()):
            normalized = normalize_message_id(chunk)
            if normalized:
                matches.append(normalized)

    seen = set()
    result: List[str] = []
    for item in matches:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _load_addresses(raw_value: str) -> List[dict[str, str]]:
    try:
        items = json.loads(raw_value or "[]")
    except json.JSONDecodeError:
        return []

    result: List[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        address = str(item.get("address", "") or "").strip()
        if not name and not address:
            continue
        result.append({"name": name, "address": address})
    return result


def _choose_display_name(current: str, candidate: str) -> str:
    current_clean = re.sub(r"\s+", " ", current or "").strip()
    candidate_clean = re.sub(r"\s+", " ", candidate or "").strip()

    if not candidate_clean:
        return current_clean
    if not current_clean:
        return candidate_clean

    current_has_email = "@" in current_clean
    candidate_has_email = "@" in candidate_clean
    if current_has_email and not candidate_has_email:
        return candidate_clean
    if candidate_has_email and not current_has_email:
        return current_clean
    if len(candidate_clean) > len(current_clean):
        return candidate_clean
    return current_clean


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pick_earlier(current: Optional[str], candidate: Optional[str]) -> Optional[str]:
    if not candidate:
        return current
    if not current:
        return candidate

    current_dt = _parse_datetime(current)
    candidate_dt = _parse_datetime(candidate)
    if current_dt and candidate_dt:
        return candidate if candidate_dt < current_dt else current
    return candidate if candidate < current else current


def _pick_later(current: Optional[str], candidate: Optional[str]) -> Optional[str]:
    if not candidate:
        return current
    if not current:
        return candidate

    current_dt = _parse_datetime(current)
    candidate_dt = _parse_datetime(candidate)
    if current_dt and candidate_dt:
        return candidate if candidate_dt > current_dt else current
    return candidate if candidate > current else current


def _extract_domain(email: str) -> str:
    normalized = normalize_email(email)
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[-1]


def _expand_self_emails_with_internal_aliases(
    role_map: dict[str, List[dict[str, str]]],
    self_emails: set[str],
) -> set[str]:
    internal_domains = {
        domain
        for domain in (_extract_domain(email) for email in self_emails)
        if domain and domain not in PUBLIC_WEBMAIL_DOMAINS
    }
    if not internal_domains:
        return set(self_emails)

    expanded = set(self_emails)
    for items in role_map.values():
        for item in items:
            email = normalize_email(item.get("address"))
            if _extract_domain(email) in internal_domains:
                expanded.add(email)
    return expanded


def _resolve_direction(
    role_map: dict[str, List[dict[str, str]]],
    self_emails: set[str],
    folder_name: str,
) -> str:
    from_emails = {normalize_email(item["address"]) for item in role_map["from"] if normalize_email(item["address"])}
    to_emails = {
        normalize_email(item["address"])
        for role in ("to", "cc", "bcc")
        for item in role_map[role]
        if normalize_email(item["address"])
    }

    if from_emails & self_emails:
        return "outbound"
    if to_emails & self_emails:
        return "inbound"

    folder_lower = folder_name.lower()
    if "sent" in folder_lower or "draft" in folder_lower:
        return "outbound"
    return "unknown"


def rebuild_relation_index(db: Database) -> dict[str, int]:
    db.init_schema()
    conn = db.conn
    now = _utc_now()
    rows = list(
        conn.execute(
            """
            SELECT
                id,
                account_email,
                folder_name,
                message_id,
                subject,
                in_reply_to,
                references_header,
                sent_at,
                internal_date,
                created_at,
                raw_sha256,
                from_json,
                to_json,
                cc_json,
                bcc_json,
                reply_to_json,
                sender_json
            FROM messages
            ORDER BY
                COALESCE(datetime(sent_at), datetime(internal_date), datetime(created_at)),
                id
            """
        ).fetchall()
    )

    contact_meta: Dict[str, dict[str, object]] = {}
    message_contact_rows: List[tuple[int, str, str, str]] = []
    message_index_rows: List[tuple[int, str, str, str, Optional[str], Optional[str], int, Optional[str], int, str]] = []
    thread_meta: Dict[str, dict[str, object]] = {}
    thread_contact_meta: Dict[tuple[str, str], dict[str, object]] = {}

    for row in rows:
        role_map = {
            "from": _load_addresses(row["from_json"]),
            "to": _load_addresses(row["to_json"]),
            "cc": _load_addresses(row["cc_json"]),
            "bcc": _load_addresses(row["bcc_json"]),
            "reply_to": _load_addresses(row["reply_to_json"]),
            "sender": _load_addresses(row["sender_json"]),
        }
        self_emails = {normalize_email(row["account_email"])} if normalize_email(row["account_email"]) else set()
        self_emails = _expand_self_emails_with_internal_aliases(role_map, self_emails)
        sent_sort_at = row["sent_at"] or row["internal_date"] or row["created_at"]
        direction = _resolve_direction(role_map, self_emails, row["folder_name"] or "")
        normalized_subject = normalize_subject(row["subject"])

        external_emails: set[str] = set()
        for role, items in role_map.items():
            seen_in_role: set[str] = set()
            for item in items:
                email = normalize_email(item.get("address"))
                if not email or email in self_emails or email in seen_in_role:
                    continue
                seen_in_role.add(email)
                external_emails.add(email)
                message_contact_rows.append((int(row["id"]), email, role, now))

                meta = contact_meta.setdefault(
                    email,
                    {
                        "display_name": "",
                        "first_seen_at": None,
                        "last_seen_at": None,
                        "message_ids": set(),
                        "thread_keys": set(),
                        "inbound_ids": set(),
                        "outbound_ids": set(),
                    },
                )
                meta["display_name"] = _choose_display_name(str(meta["display_name"]), item.get("name", ""))

        references = extract_message_ids(row["references_header"])
        message_id = normalize_message_id(row["message_id"])
        in_reply_to = normalize_message_id(row["in_reply_to"])
        thread_parent_message_id = references[-1] if references else in_reply_to
        thread_root_message_id = references[0] if references else (in_reply_to or message_id)
        thread_depth = len(references) if references else (1 if in_reply_to else 0)

        if thread_root_message_id:
            thread_key = f"mid:{thread_root_message_id}"
        else:
            fallback_basis = "|".join(
                [
                    normalized_subject or "(no-subject)",
                    ",".join(sorted(external_emails)) or "(no-contact)",
                    row["raw_sha256"] or "",
                ]
            )
            digest = hashlib.sha1(fallback_basis.encode("utf-8")).hexdigest()[:24]
            thread_key = f"fallback:{digest}"

        message_index_rows.append(
            (
                int(row["id"]),
                normalized_subject,
                direction,
                thread_key,
                thread_root_message_id,
                thread_parent_message_id,
                thread_depth,
                sent_sort_at,
                len(external_emails),
                now,
            )
        )

        thread_entry = thread_meta.setdefault(
            thread_key,
            {
                "normalized_subject": normalized_subject,
                "example_subject": row["subject"] or normalized_subject,
                "thread_root_message_id": thread_root_message_id,
                "message_count": 0,
                "first_sent_at": None,
                "last_sent_at": None,
                "external_emails": set(),
                "updated_at": now,
            },
        )
        thread_entry["message_count"] = int(thread_entry["message_count"]) + 1
        thread_entry["first_sent_at"] = _pick_earlier(thread_entry["first_sent_at"], sent_sort_at)
        previous_last = thread_entry["last_sent_at"]
        thread_entry["last_sent_at"] = _pick_later(previous_last, sent_sort_at)
        if thread_entry["last_sent_at"] != previous_last and row["subject"]:
            thread_entry["example_subject"] = row["subject"]
        if not thread_entry["thread_root_message_id"] and thread_root_message_id:
            thread_entry["thread_root_message_id"] = thread_root_message_id
        if not thread_entry["normalized_subject"] and normalized_subject:
            thread_entry["normalized_subject"] = normalized_subject
        thread_entry["external_emails"].update(external_emails)

        for email in external_emails:
            meta = contact_meta[email]
            meta["first_seen_at"] = _pick_earlier(meta["first_seen_at"], sent_sort_at)
            meta["last_seen_at"] = _pick_later(meta["last_seen_at"], sent_sort_at)
            meta["message_ids"].add(int(row["id"]))
            meta["thread_keys"].add(thread_key)
            if direction == "inbound":
                meta["inbound_ids"].add(int(row["id"]))
            elif direction == "outbound":
                meta["outbound_ids"].add(int(row["id"]))

            thread_contact_entry = thread_contact_meta.setdefault(
                (thread_key, email),
                {
                    "message_ids": set(),
                    "first_sent_at": None,
                    "last_sent_at": None,
                },
            )
            thread_contact_entry["message_ids"].add(int(row["id"]))
            thread_contact_entry["first_sent_at"] = _pick_earlier(thread_contact_entry["first_sent_at"], sent_sort_at)
            thread_contact_entry["last_sent_at"] = _pick_later(thread_contact_entry["last_sent_at"], sent_sort_at)

    with conn:
        conn.execute("DELETE FROM thread_contacts")
        conn.execute("DELETE FROM threads")
        conn.execute("DELETE FROM message_contacts")
        conn.execute("DELETE FROM message_index")
        conn.execute("DELETE FROM contacts")

        conn.executemany(
            """
            INSERT INTO contacts (
                email_normalized,
                display_name,
                first_seen_at,
                last_seen_at,
                message_count,
                inbound_message_count,
                outbound_message_count,
                thread_count,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    email,
                    str(meta["display_name"]),
                    meta["first_seen_at"],
                    meta["last_seen_at"],
                    len(meta["message_ids"]),
                    len(meta["inbound_ids"]),
                    len(meta["outbound_ids"]),
                    len(meta["thread_keys"]),
                    now,
                    now,
                )
                for email, meta in sorted(contact_meta.items())
            ],
        )

        contact_id_rows = conn.execute(
            """
            SELECT id, email_normalized
            FROM contacts
            """
        ).fetchall()
        contact_id_by_email = {str(row["email_normalized"]): int(row["id"]) for row in contact_id_rows}

        conn.executemany(
            """
            INSERT INTO message_contacts (
                message_row_id,
                contact_id,
                role,
                created_at
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (message_row_id, contact_id_by_email[email], role, created_at)
                for message_row_id, email, role, created_at in message_contact_rows
                if email in contact_id_by_email
            ],
        )

        conn.executemany(
            """
            INSERT INTO message_index (
                message_row_id,
                normalized_subject,
                direction,
                thread_key,
                thread_root_message_id,
                thread_parent_message_id,
                thread_depth,
                sent_sort_at,
                external_contact_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            message_index_rows,
        )

        conn.executemany(
            """
            INSERT INTO threads (
                thread_key,
                normalized_subject,
                example_subject,
                thread_root_message_id,
                message_count,
                external_contact_count,
                first_sent_at,
                last_sent_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    thread_key,
                    str(meta["normalized_subject"]),
                    str(meta["example_subject"]),
                    meta["thread_root_message_id"],
                    int(meta["message_count"]),
                    len(meta["external_emails"]),
                    meta["first_sent_at"],
                    meta["last_sent_at"],
                    now,
                    now,
                )
                for thread_key, meta in sorted(thread_meta.items())
            ],
        )

        conn.executemany(
            """
            INSERT INTO thread_contacts (
                thread_key,
                contact_id,
                message_count,
                first_sent_at,
                last_sent_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    thread_key,
                    contact_id_by_email[email],
                    len(meta["message_ids"]),
                    meta["first_sent_at"],
                    meta["last_sent_at"],
                )
                for (thread_key, email), meta in sorted(thread_contact_meta.items())
                if email in contact_id_by_email
            ],
        )

    return {
        "messages_indexed": len(message_index_rows),
        "contacts": len(contact_meta),
        "threads": len(thread_meta),
        "thread_contact_links": len(thread_contact_meta),
    }
