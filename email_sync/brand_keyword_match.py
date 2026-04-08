from __future__ import annotations

import csv
import html
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

from .creator_enrichment import (
    CANONICAL_SOURCE_HEADERS,
    SENDING_LIST_CREATOR_ALIASES,
    SENDING_LIST_COUNTRY_ALIASES,
    SENDING_LIST_EMAIL_ALIASES,
    SENDING_LIST_GENERIC_LINK_ALIASES,
    SENDING_LIST_HANDLE_ALIASES,
    SENDING_LIST_PLATFORM_LINK_ALIASES,
    _clean_text,
    _extract_emails,
    _has_canonical_creator_columns,
    _infer_platform_from_value,
    _iter_sending_list_rows,
    _iter_sheet_rows,
    _load_addresses,
    _normalize_handle,
    _normalize_source_column_name,
    _platform_label,
    _resolve_source_column,
    _source_headers,
    _stringify,
    _timestamp,
    _write_csv,
    _write_xlsx,
)
from .db import Database


PLATFORM_COLUMN_ALIASES = ("platform", "平台")
PROFILE_COLUMN_ALIASES = tuple(SENDING_LIST_GENERIC_LINK_ALIASES) + tuple(
    alias for aliases in SENDING_LIST_PLATFORM_LINK_ALIASES.values() for alias in aliases
)
BASE_HEADERS = [
    "Platform",
    "@username",
    "URL",
    "nickname",
    "Region",
    "Email",
    "sheet_name",
    "source_row_number",
]
MATCH_HEADERS = BASE_HEADERS + [
    "brand_keyword",
    "creator_emails",
    "matched_email",
    "matched_email_role",
    "matched_email_hit_count",
    "brand_message_id",
    "brand_message_sent_at",
    "brand_message_subject",
    "brand_message_folder",
    "brand_message_raw_path",
    "brand_message_snippet",
    "profile_dedupe_key",
    "shared_email_candidate_count",
    "shared_email_distinct_profile_count",
]
THREAD_HEADERS = [
    "thread_key",
    "brand_keyword",
    "keyword_hit_message_count",
    "latest_external_message_id",
    "latest_external_sent_at",
    "latest_external_from",
    "latest_external_sender_emails",
    "latest_external_subject",
    "latest_external_folder",
    "latest_external_raw_path",
    "latest_external_snippet",
    "latest_external_clean_body",
    "latest_external_full_body",
    "thread_has_external_message",
]


@dataclass(frozen=True)
class MessageHit:
    email: str
    role: str
    message_row_id: int
    sent_at: str
    subject: str
    folder_name: str
    raw_path: str
    snippet: str


def _first_sender_email(message_row: sqlite3.Row) -> str:
    for key in ("from_json", "reply_to_json", "sender_json"):
        for item in _load_addresses(str(message_row[key] or "[]")):
            email = _clean_text(item.get("address")).lower()
            if email:
                return email
    return ""


def _all_sender_emails(message_row: sqlite3.Row) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for key in ("from_json", "reply_to_json", "sender_json"):
        for item in _load_addresses(str(message_row[key] or "[]")):
            email = _clean_text(item.get("address")).lower()
            if not email or email in seen:
                continue
            seen.add(email)
            result.append(email)
    return result


def _is_external_sender(message_row: sqlite3.Row) -> bool:
    sender_emails = _all_sender_emails(message_row)
    if not sender_emails:
        return False
    return any(not email.endswith("@amagency.biz") for email in sender_emails)


def _build_full_body(message_row: sqlite3.Row) -> str:
    for value in (message_row["body_text"], message_row["snippet"], message_row["body_html"]):
        if value is None:
            continue
        text = html.unescape(str(value)).replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        if text:
            return text
    return ""


def _build_clean_body(full_body: str) -> str:
    if not full_body:
        return ""
    lines: list[str] = []
    for raw_line in str(full_body or "").splitlines():
        line = raw_line.rstrip()
        normalized = line.strip().lower()
        if normalized.startswith(">") or normalized.startswith("on ") and normalized.endswith(" wrote:"):
            break
        if normalized.startswith("from:") or normalized.startswith("de:"):
            break
        lines.append(line)
    cleaned = _clean_text("\n".join(lines))
    return cleaned or _clean_text(full_body)[:2000]


def _normalize_platform(value: Any) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    if "instagram" in text or text == "ig":
        return "instagram"
    if "tiktok" in text or "douyin" in text or text == "tt":
        return "tiktok"
    if "youtube" in text or text == "yt":
        return "youtube"
    return text


def _resolve_candidate_columns(
    headers: Sequence[str],
    *,
    email_column: str = "",
    creator_column: str = "",
    profile_column: str = "",
    handle_column: str = "",
    platform_column: str = "",
) -> dict[str, str]:
    mapping: dict[str, str] = {
        "email": email_column.strip(),
        "creator": creator_column.strip(),
        "profile": profile_column.strip(),
        "handle": handle_column.strip(),
        "platform": platform_column.strip(),
        "country": "",
    }

    if not mapping["email"]:
        resolved = _resolve_source_column(headers, SENDING_LIST_EMAIL_ALIASES)
        mapping["email"] = _stringify(resolved)
    if not mapping["creator"]:
        resolved = _resolve_source_column(headers, SENDING_LIST_CREATOR_ALIASES)
        mapping["creator"] = _stringify(resolved)
    if not mapping["profile"]:
        resolved = _resolve_source_column(headers, PROFILE_COLUMN_ALIASES)
        mapping["profile"] = _stringify(resolved)
    if not mapping["handle"]:
        resolved = _resolve_source_column(headers, SENDING_LIST_HANDLE_ALIASES)
        mapping["handle"] = _stringify(resolved)
    if not mapping["platform"]:
        resolved = _resolve_source_column(headers, PLATFORM_COLUMN_ALIASES)
        mapping["platform"] = _stringify(resolved)
    if not mapping["country"]:
        resolved = _resolve_source_column(headers, SENDING_LIST_COUNTRY_ALIASES)
        mapping["country"] = _stringify(resolved)
    return mapping


def _load_candidate_rows(
    input_path: Path,
    *,
    email_column: str = "",
    creator_column: str = "",
    profile_column: str = "",
    handle_column: str = "",
    platform_column: str = "",
) -> tuple[str, list[dict[str, Any]]]:
    if not input_path.exists():
        raise FileNotFoundError(f"找不到输入 workbook: {input_path}")

    headers = _source_headers(input_path)
    if _has_canonical_creator_columns(headers):
        rows = []
        for row in _iter_sheet_rows(input_path, CANONICAL_SOURCE_HEADERS):
            rows.append({header: row.get(header, "") for header in BASE_HEADERS})
        return "canonical_upload", rows

    mapping = _resolve_candidate_columns(
        headers,
        email_column=email_column,
        creator_column=creator_column,
        profile_column=profile_column,
        handle_column=handle_column,
        platform_column=platform_column,
    )
    if mapping["email"] and (mapping["profile"] or mapping["handle"]):
        selected_headers = [
            column
            for column in (
                mapping["email"],
                mapping["creator"],
                mapping["profile"],
                mapping["handle"],
                mapping["platform"],
                mapping["country"],
            )
            if column
        ]
        rows = []
        for row in _iter_sheet_rows(input_path, selected_headers):
            profile_value = _stringify(row.get(mapping["profile"], "")) if mapping["profile"] else ""
            handle_value = _stringify(row.get(mapping["handle"], "")) if mapping["handle"] else ""
            platform_value = _stringify(row.get(mapping["platform"], "")) if mapping["platform"] else ""
            region_value = _stringify(row.get(mapping["country"], "")) if mapping["country"] else ""
            normalized_platform = _infer_platform_from_value(profile_value) or _normalize_platform(platform_value)
            normalized_handle = _normalize_handle(handle_value) or _normalize_handle(profile_value) or handle_value
            rows.append(
                {
                    "Platform": _platform_label(normalized_platform) if normalized_platform else platform_value,
                    "@username": normalized_handle,
                    "URL": profile_value,
                    "nickname": _stringify(row.get(mapping["creator"], "")) if mapping["creator"] else "",
                    "Region": region_value,
                    "Email": _stringify(row.get(mapping["email"], "")),
                    "sheet_name": row.get("sheet_name", ""),
                    "source_row_number": row.get("source_row_number", ""),
                }
            )
        return "custom_columns", rows

    rows = list(_iter_sending_list_rows(input_path))
    if rows:
        return "sending_list", [{header: row.get(header, "") for header in BASE_HEADERS} for row in rows]

    raise ValueError("无法识别输入 workbook 的邮箱/主页列；请显式传入 --email-column 与 --profile-column 或 --handle-column。")


def _query_keyword_messages(
    db: Database,
    keyword: str,
    *,
    sent_since: date | None = None,
    limit: int = 0,
) -> list[sqlite3.Row]:
    keyword_like = f"%{keyword.lower()}%"
    params: list[object] = [keyword_like] * 8
    sql = """
        SELECT
            m.id,
            COALESCE(mi.thread_key, '') AS thread_key,
            m.folder_name,
            m.subject,
            m.sent_at,
            m.from_json,
            m.to_json,
            m.cc_json,
            m.bcc_json,
            m.reply_to_json,
            m.sender_json,
            m.snippet,
            m.body_text,
            m.body_html,
            m.raw_path
        FROM messages m
        LEFT JOIN message_index mi ON mi.message_row_id = m.id
        WHERE (
            LOWER(COALESCE(m.subject, '')) LIKE ?
            OR LOWER(COALESCE(m.snippet, '')) LIKE ?
            OR LOWER(COALESCE(m.body_text, '')) LIKE ?
            OR LOWER(COALESCE(m.body_html, '')) LIKE ?
            OR LOWER(COALESCE(m.from_json, '')) LIKE ?
            OR LOWER(COALESCE(m.to_json, '')) LIKE ?
            OR LOWER(COALESCE(m.folder_name, '')) LIKE ?
            OR EXISTS (
                SELECT 1
                FROM attachments a
                WHERE a.message_row_id = m.id
                  AND LOWER(COALESCE(a.filename, '')) LIKE ?
            )
        )
    """
    if sent_since is not None:
        sql += "\n        AND datetime(COALESCE(m.sent_at, '')) >= datetime(?)"
        params.append(f"{sent_since.isoformat()} 00:00:00")
    sql += "\n        ORDER BY datetime(m.sent_at) DESC, m.id DESC"
    if limit > 0:
        sql = f"{sql}\nLIMIT ?"
        params.append(limit)
    return list(db.conn.execute(sql, params).fetchall())


def _collect_address_hits(messages: Iterable[sqlite3.Row], include_from: bool = False) -> dict[str, list[MessageHit]]:
    by_email: dict[str, list[MessageHit]] = defaultdict(list)
    address_fields = [
        ("to", "to_json"),
        ("cc", "cc_json"),
        ("bcc", "bcc_json"),
        ("reply_to", "reply_to_json"),
    ]
    if include_from:
        address_fields.extend([("from", "from_json"), ("sender", "sender_json")])

    for row in messages:
        seen_in_message: set[str] = set()
        for role, key in address_fields:
            for item in _load_addresses(str(row[key] or "[]")):
                email = str(item.get("address", "") or "").strip().lower()
                if not email or email in seen_in_message:
                    continue
                seen_in_message.add(email)
                by_email[email].append(
                    MessageHit(
                        email=email,
                        role=role,
                        message_row_id=int(row["id"]),
                        sent_at=str(row["sent_at"] or ""),
                        subject=str(row["subject"] or ""),
                        folder_name=str(row["folder_name"] or ""),
                        raw_path=str(row["raw_path"] or ""),
                        snippet=str(row["snippet"] or ""),
                    )
                )

    for hits in by_email.values():
        hits.sort(key=lambda item: (_timestamp(item.sent_at), item.message_row_id), reverse=True)
    return by_email


def _profile_dedupe_key(row: dict[str, Any]) -> str:
    url = _stringify(row.get("URL", ""))
    platform = _infer_platform_from_value(url) or _normalize_platform(row.get("Platform", ""))
    handle = _normalize_handle(url) or _normalize_handle(row.get("@username", ""))
    if platform and handle:
        return f"{platform}:{handle}"
    if url:
        return url.lower()
    return f"row:{row.get('sheet_name', '')}:{row.get('source_row_number', '')}"


def _build_match_rows(
    candidate_rows: Sequence[dict[str, Any]],
    *,
    keyword: str,
    address_hits: dict[str, list[MessageHit]],
) -> list[dict[str, Any]]:
    matched_rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        creator_emails = _extract_emails(_stringify(row.get("Email", "")))
        best_email = ""
        best_hit: MessageHit | None = None
        hit_count = 0
        for email in creator_emails:
            hits = address_hits.get(email, [])
            if not hits:
                continue
            candidate_hit = hits[0]
            if best_hit is None or (_timestamp(candidate_hit.sent_at), candidate_hit.message_row_id) > (
                _timestamp(best_hit.sent_at),
                best_hit.message_row_id,
            ):
                best_email = email
                best_hit = candidate_hit
                hit_count = len(hits)

        if best_hit is None:
            continue

        normalized_platform = _infer_platform_from_value(row.get("URL", "")) or _normalize_platform(row.get("Platform", ""))
        matched_rows.append(
            {
                "Platform": row.get("Platform") or (_platform_label(normalized_platform) if normalized_platform else ""),
                "@username": row.get("@username") or _normalize_handle(row.get("URL", "")),
                "URL": row.get("URL", ""),
                "nickname": row.get("nickname", ""),
                "Region": row.get("Region", ""),
                "Email": row.get("Email", ""),
                "sheet_name": row.get("sheet_name", ""),
                "source_row_number": row.get("source_row_number", ""),
                "brand_keyword": keyword,
                "creator_emails": " | ".join(creator_emails),
                "matched_email": best_email,
                "matched_email_role": best_hit.role,
                "matched_email_hit_count": hit_count,
                "brand_message_id": best_hit.message_row_id,
                "brand_message_sent_at": best_hit.sent_at,
                "brand_message_subject": best_hit.subject,
                "brand_message_folder": best_hit.folder_name,
                "brand_message_raw_path": best_hit.raw_path,
                "brand_message_snippet": best_hit.snippet,
                "profile_dedupe_key": _profile_dedupe_key(row),
                "shared_email_candidate_count": 0,
                "shared_email_distinct_profile_count": 0,
            }
        )

    matched_rows.sort(
        key=lambda row: (
            _timestamp(row.get("brand_message_sent_at")),
            int(row.get("brand_message_id") or 0),
            int(row.get("source_row_number") or 0),
        ),
        reverse=True,
    )
    return matched_rows


def dedupe_brand_match_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        dedupe_key = str(row.get("profile_dedupe_key", "") or "").strip()
        if not dedupe_key:
            dedupe_key = f"row:{row.get('sheet_name', '')}:{row.get('source_row_number', '')}"
        existing = by_key.get(dedupe_key)
        if existing is None:
            by_key[dedupe_key] = dict(row)
            continue
        current_rank = (_timestamp(row.get("brand_message_sent_at")), int(row.get("brand_message_id") or 0))
        existing_rank = (_timestamp(existing.get("brand_message_sent_at")), int(existing.get("brand_message_id") or 0))
        if current_rank > existing_rank:
            by_key[dedupe_key] = dict(row)

    deduped = list(by_key.values())
    deduped.sort(
        key=lambda row: (
            _timestamp(row.get("brand_message_sent_at")),
            int(row.get("brand_message_id") or 0),
            int(row.get("source_row_number") or 0),
        ),
        reverse=True,
    )
    return deduped


def split_shared_email_rows(rows: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _stringify(row.get("matched_email", ""))
        groups[key].append(dict(row))

    unique_rows: list[dict[str, Any]] = []
    shared_rows: list[dict[str, Any]] = []
    shared_group_count = 0

    for group_rows in groups.values():
        distinct_profile_count = len(
            {
                _stringify(row.get("profile_dedupe_key", "")) or f"row:{row.get('sheet_name', '')}:{row.get('source_row_number', '')}"
                for row in group_rows
            }
        )
        candidate_count = len(group_rows)
        target = unique_rows if distinct_profile_count <= 1 else shared_rows
        if distinct_profile_count > 1:
            shared_group_count += 1
        for row in group_rows:
            row["shared_email_candidate_count"] = candidate_count
            row["shared_email_distinct_profile_count"] = distinct_profile_count
            target.append(row)

    unique_rows.sort(
        key=lambda row: (
            _timestamp(row.get("brand_message_sent_at")),
            int(row.get("brand_message_id") or 0),
            int(row.get("source_row_number") or 0),
        ),
        reverse=True,
    )
    shared_rows.sort(
        key=lambda row: (
            _timestamp(row.get("brand_message_sent_at")),
            int(row.get("brand_message_id") or 0),
            int(row.get("source_row_number") or 0),
        ),
        reverse=True,
    )
    return unique_rows, shared_rows, shared_group_count


def _write_match_outputs(output_prefix: Path, rows: Sequence[dict[str, Any]]) -> tuple[str, str]:
    csv_path = output_prefix.with_suffix(".csv")
    xlsx_path = output_prefix.with_suffix(".xlsx")
    _write_csv(csv_path, MATCH_HEADERS, rows)
    _write_xlsx(xlsx_path, MATCH_HEADERS, rows)
    return str(csv_path), str(xlsx_path)


def build_keyword_hit_threads(
    messages: Sequence[sqlite3.Row],
    *,
    keyword: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in messages:
        thread_key = _clean_text(row["thread_key"]) or f"message:{int(row['id'])}"
        grouped[thread_key].append(row)

    thread_rows: list[dict[str, Any]] = []
    for thread_key, group_rows in grouped.items():
        sorted_rows = sorted(
            group_rows,
            key=lambda item: (_timestamp(item["sent_at"]), int(item["id"])),
            reverse=True,
        )
        external_rows = [row for row in sorted_rows if _is_external_sender(row)]
        selected_row = external_rows[0] if external_rows else sorted_rows[0]
        full_body = _build_full_body(selected_row)
        sender_emails = _all_sender_emails(selected_row)
        thread_rows.append(
            {
                "thread_key": thread_key,
                "brand_keyword": keyword,
                "keyword_hit_message_count": len(group_rows),
                "latest_external_message_id": int(selected_row["id"]),
                "latest_external_sent_at": _clean_text(selected_row["sent_at"]),
                "latest_external_from": _first_sender_email(selected_row),
                "latest_external_sender_emails": " | ".join(sender_emails),
                "latest_external_subject": _clean_text(selected_row["subject"]),
                "latest_external_folder": _clean_text(selected_row["folder_name"]),
                "latest_external_raw_path": _clean_text(selected_row["raw_path"]),
                "latest_external_snippet": _clean_text(selected_row["snippet"]),
                "latest_external_clean_body": _build_clean_body(full_body),
                "latest_external_full_body": full_body,
                "thread_has_external_message": 1 if external_rows else 0,
            }
        )

    thread_rows.sort(
        key=lambda row: (
            _timestamp(row.get("latest_external_sent_at")),
            int(row.get("latest_external_message_id") or 0),
        ),
        reverse=True,
    )
    return thread_rows


def _write_thread_outputs(output_prefix: Path, rows: Sequence[dict[str, Any]]) -> tuple[str, str]:
    csv_path = output_prefix.with_suffix(".csv")
    xlsx_path = output_prefix.with_suffix(".xlsx")
    _write_csv(csv_path, THREAD_HEADERS, rows)
    _write_xlsx(xlsx_path, THREAD_HEADERS, rows)
    return str(csv_path), str(xlsx_path)


def match_brand_keyword(
    *,
    db: Database,
    input_path: Path,
    output_prefix: Path,
    keyword: str,
    sent_since: date | None = None,
    message_limit: int = 0,
    include_from: bool = False,
    email_column: str = "",
    creator_column: str = "",
    profile_column: str = "",
    handle_column: str = "",
    platform_column: str = "",
) -> dict[str, Any]:
    normalized_keyword = _clean_text(keyword)
    if not normalized_keyword:
        raise ValueError("缺少 keyword。")

    db.init_schema()
    source_kind, candidate_rows = _load_candidate_rows(
        input_path,
        email_column=email_column,
        creator_column=creator_column,
        profile_column=profile_column,
        handle_column=handle_column,
        platform_column=platform_column,
    )
    messages = _query_keyword_messages(
        db,
        normalized_keyword,
        sent_since=sent_since,
        limit=max(0, int(message_limit)),
    )
    address_hits = _collect_address_hits(messages, include_from=include_from)
    matched_rows = _build_match_rows(
        candidate_rows,
        keyword=normalized_keyword,
        address_hits=address_hits,
    )
    thread_rows = build_keyword_hit_threads(messages, keyword=normalized_keyword)
    deduped_rows = dedupe_brand_match_rows(matched_rows)
    unique_rows, shared_rows, shared_group_count = split_shared_email_rows(deduped_rows)

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    all_csv_path, all_xlsx_path = _write_match_outputs(output_prefix, matched_rows)
    thread_csv_path, thread_xlsx_path = _write_thread_outputs(
        output_prefix.with_name(f"{output_prefix.name}_threads"),
        thread_rows,
    )
    deduped_csv_path, deduped_xlsx_path = _write_match_outputs(
        output_prefix.with_name(f"{output_prefix.name}_deduped"),
        deduped_rows,
    )
    unique_csv_path, unique_xlsx_path = _write_match_outputs(
        output_prefix.with_name(f"{output_prefix.name}_unique_email"),
        unique_rows,
    )
    shared_csv_path, shared_xlsx_path = _write_match_outputs(
        output_prefix.with_name(f"{output_prefix.name}_shared_email"),
        shared_rows,
    )

    return {
        "source_kind": source_kind,
        "keyword": normalized_keyword,
        "sent_since": sent_since.isoformat() if sent_since else "",
        "message_hit_count": len(messages),
        "thread_hit_count": len(thread_rows),
        "matched_email_count": len(address_hits),
        "email_direct_match_row_count": len(matched_rows),
        "profile_deduped_row_count": len(deduped_rows),
        "unique_email_row_count": len(unique_rows),
        "shared_email_row_count": len(shared_rows),
        "shared_email_group_count": shared_group_count,
        "csv_path": all_csv_path,
        "xlsx_path": all_xlsx_path,
        "thread_csv_path": thread_csv_path,
        "thread_xlsx_path": thread_xlsx_path,
        "deduped_csv_path": deduped_csv_path,
        "deduped_xlsx_path": deduped_xlsx_path,
        "unique_csv_path": unique_csv_path,
        "unique_xlsx_path": unique_xlsx_path,
        "shared_csv_path": shared_csv_path,
        "shared_xlsx_path": shared_xlsx_path,
    }


def split_shared_email_candidates(*, input_path: Path, output_prefix: Path) -> dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"找不到输入 workbook: {input_path}")

    source_headers = _source_headers(input_path)
    rows = list(_iter_sheet_rows(input_path, source_headers))
    required_columns = {"matched_email", "profile_dedupe_key"}
    missing = required_columns - set(source_headers)
    if missing:
        raise ValueError(f"输入 workbook 缺少必要列: {', '.join(sorted(missing))}")

    headers = list(source_headers)
    for header in ("shared_email_candidate_count", "shared_email_distinct_profile_count"):
        if header not in headers:
            headers.append(header)
    unique_rows, shared_rows, shared_group_count = split_shared_email_rows(rows)

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    unique_csv_path = output_prefix.with_name(f"{output_prefix.name}_unique_email").with_suffix(".csv")
    unique_xlsx_path = output_prefix.with_name(f"{output_prefix.name}_unique_email").with_suffix(".xlsx")
    shared_csv_path = output_prefix.with_name(f"{output_prefix.name}_shared_email").with_suffix(".csv")
    shared_xlsx_path = output_prefix.with_name(f"{output_prefix.name}_shared_email").with_suffix(".xlsx")
    _write_csv(unique_csv_path, headers, unique_rows)
    _write_xlsx(unique_xlsx_path, headers, unique_rows)
    _write_csv(shared_csv_path, headers, shared_rows)
    _write_xlsx(shared_xlsx_path, headers, shared_rows)

    return {
        "input_path": str(input_path),
        "unique_email_row_count": len(unique_rows),
        "shared_email_row_count": len(shared_rows),
        "shared_email_group_count": shared_group_count,
        "unique_csv_path": str(unique_csv_path),
        "unique_xlsx_path": str(unique_xlsx_path),
        "shared_csv_path": str(shared_csv_path),
        "shared_xlsx_path": str(shared_xlsx_path),
    }
