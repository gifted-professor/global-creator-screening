from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from email_sync.brand_keyword_match import _load_candidate_rows  # type: ignore
from email_sync.db import Database  # type: ignore
from email_sync.mail_thread_funnel import (  # type: ignore
    FUNNEL_HEADERS,
    _all_sender_emails,
    _build_candidate_identity_maps,
    _build_full_body,
    _build_row,
    _clean_text,
    _extract_explicit_candidates,
    _extract_greeting_candidates,
    _extract_platform_handle_pairs,
    _is_external_sender,
    _matches_auto_reply,
    _resolve_platform_for_handle,
    _run_default_llm_review,
    _write_xlsx,
)


EXTRA_HEADERS = [
    "message_row_id",
    "llm_reason",
    "llm_provider",
    "llm_model",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the parsed-field recall -> rules -> LLM mail funnel path.",
    )
    parser.add_argument("--env-file", default=".env", help="Env file for LLM config.")
    parser.add_argument(
        "--db-path",
        default="data/shared_mailbox/email_sync.db",
        help="SQLite mail DB path.",
    )
    parser.add_argument(
        "--input-workbook",
        default="",
        help="Optional sending-list workbook path. Leave blank to disable sending-list email matching.",
    )
    parser.add_argument("--keyword", required=True, help="Brand keyword, for example MINISO.")
    parser.add_argument(
        "--local-date",
        required=True,
        help="Local date in YYYY-MM-DD, interpreted in --timezone.",
    )
    parser.add_argument(
        "--timezone",
        default="Asia/Shanghai",
        help="Timezone used for the local-day recall window.",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="Output prefix for review/keep/manual/summary artifacts.",
    )
    parser.add_argument("--base-url", help="Override LLM base URL.")
    parser.add_argument("--api-key", help="Override LLM API key.")
    parser.add_argument("--model", help="Override LLM model.")
    parser.add_argument("--wire-api", help="Override LLM wire API.")
    parser.add_argument(
        "--llm-max-workers",
        type=int,
        default=12,
        help="Max concurrent LLM requests.",
    )
    parser.add_argument(
        "--llm-limit",
        type=int,
        default=0,
        help="Only review the first N LLM candidates; 0 means all.",
    )
    return parser.parse_args()


def _source_headers() -> list[str]:
    return list(FUNNEL_HEADERS) + [header for header in EXTRA_HEADERS if header not in FUNNEL_HEADERS]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _parse_sent_at(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _query_parsed_field_messages(
    db: Database,
    *,
    keyword: str,
    local_date: date,
    tz_name: str,
) -> list[Any]:
    keyword_like = f"%{keyword.lower()}%"
    rows = list(
        db.conn.execute(
            """
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
            )
            ORDER BY datetime(m.sent_at) DESC, m.id DESC
            """,
            [keyword_like, keyword_like, keyword_like, keyword_like],
        ).fetchall()
    )
    local_tz = ZoneInfo(tz_name)
    filtered: list[Any] = []
    for row in rows:
        parsed = _parse_sent_at(row["sent_at"])
        if parsed is None:
            continue
        if parsed.astimezone(local_tz).date() == local_date:
            filtered.append(row)
    return filtered


def _review_candidates(
    llm_candidates: list[dict[str, Any]],
    *,
    env_path: str,
    base_url: str | None,
    api_key: str | None,
    model: str | None,
    wire_api: str | None,
    llm_max_workers: int,
    llm_limit: int,
) -> tuple[list[dict[str, Any]], int]:
    if not llm_candidates:
        return [], 0

    review_limit = max(0, int(llm_limit or 0))
    to_review = llm_candidates if review_limit == 0 else llm_candidates[:review_limit]
    skipped = [] if review_limit == 0 else llm_candidates[review_limit:]

    reviewed_rows = _run_default_llm_review(
        to_review,
        env_path=env_path,
        base_url=base_url,
        api_key=api_key,
        model=model,
        wire_api=wire_api,
        max_workers=llm_max_workers,
    )
    skipped_rows = [
        {
            **row,
            "llm_handle": "",
            "resolution_confidence_final": "low",
            "llm_evidence": "llm_skipped_due_to_limit",
            "llm_reason": "llm_skipped_due_to_limit",
            "llm_provider": "",
            "llm_model": "",
        }
        for row in skipped
    ]
    return reviewed_rows + skipped_rows, len(skipped)


def run(args: argparse.Namespace) -> dict[str, Any]:
    local_date = date.fromisoformat(str(args.local_date).strip())
    db_path = Path(args.db_path).expanduser().resolve()
    input_workbook_raw = str(args.input_workbook or "").strip()
    input_workbook = Path(input_workbook_raw).expanduser().resolve() if input_workbook_raw else None
    output_prefix = Path(args.output_prefix).expanduser().resolve()
    sending_list_matching_enabled = input_workbook is not None

    db = Database(db_path)
    db.init_schema()
    try:
        if sending_list_matching_enabled and input_workbook is not None:
            _, candidate_rows = _load_candidate_rows(input_workbook)
            email_to_handles, known_handles, handle_to_platforms, email_to_profile_pairs = _build_candidate_identity_maps(
                candidate_rows
            )
        else:
            email_to_handles = {}
            known_handles = set()
            handle_to_platforms = {}
            email_to_profile_pairs = {}
        parsed_messages = _query_parsed_field_messages(
            db,
            keyword=args.keyword,
            local_date=local_date,
            tz_name=args.timezone,
        )
    finally:
        db.close()

    external_messages = [row for row in parsed_messages if _is_external_sender(row)]

    review_rows: list[dict[str, Any]] = []
    llm_candidates: list[dict[str, Any]] = []

    pass0_count = 0
    regex_pass1_count = 0
    regex_pass2_count = 0
    filtered_auto_reply_count = 0

    for message_row in external_messages:
        full_body = _build_full_body(message_row)
        body_pairs = _extract_platform_handle_pairs(full_body)
        subject = _clean_text(message_row["subject"])
        if _matches_auto_reply(subject, full_body):
            filtered_auto_reply_count += 1
            review_rows.append(
                {
                    **_build_row(
                        message_row=message_row,
                        keyword=args.keyword,
                        stage="filtered_auto_reply",
                        confidence="low",
                        final_id="",
                        sending_list_match_status="none",
                    ),
                    "message_row_id": int(message_row["id"]),
                    "llm_reason": "",
                    "llm_provider": "",
                    "llm_model": "",
                }
            )
            continue

        sender_handles: set[str] = set()
        sender_profile_pairs: set[tuple[str, str]] = set()
        for sender_email in _all_sender_emails(message_row):
            sender_handles.update(email_to_handles.get(sender_email, set()))
            sender_profile_pairs.update(email_to_profile_pairs.get(sender_email, set()))
        if sending_list_matching_enabled and len(sender_handles) == 1:
            final_id = sorted(sender_handles)[0]
            sender_platforms = {platform for handle, platform in sender_profile_pairs if handle == final_id}
            platform = _resolve_platform_for_handle(
                final_id,
                body_pairs=body_pairs,
                handle_to_platforms=handle_to_platforms,
                preferred_platforms=sender_platforms,
            )
            pass0_count += 1
            review_rows.append(
                {
                    **_build_row(
                        message_row=message_row,
                        keyword=args.keyword,
                        stage="pass0_sending_list_email",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="email_exact",
                        platform=platform,
                    ),
                    "message_row_id": int(message_row["id"]),
                    "llm_reason": "",
                    "llm_provider": "",
                    "llm_model": "",
                }
            )
            continue

        greeting_candidates = _extract_greeting_candidates(full_body)
        if len(greeting_candidates) == 1:
            final_id = greeting_candidates[0]
            platform = _resolve_platform_for_handle(
                final_id,
                body_pairs=body_pairs,
                handle_to_platforms=handle_to_platforms,
            )
            regex_pass1_count += 1
            review_rows.append(
                {
                    **_build_row(
                        message_row=message_row,
                        keyword=args.keyword,
                        stage="regex_pass1",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status=(
                            "in_sending_list"
                            if sending_list_matching_enabled and final_id in known_handles
                            else "out_of_sending_list"
                            if sending_list_matching_enabled
                            else "disabled"
                        ),
                        platform=platform,
                    ),
                    "message_row_id": int(message_row["id"]),
                    "llm_reason": "",
                    "llm_provider": "",
                    "llm_model": "",
                }
            )
            continue

        explicit_candidates = _extract_explicit_candidates(full_body)
        unique_explicit = [candidate for candidate in explicit_candidates if candidate not in set(greeting_candidates)]
        if len(unique_explicit) == 1:
            final_id = unique_explicit[0]
            platform = _resolve_platform_for_handle(
                final_id,
                body_pairs=body_pairs,
                handle_to_platforms=handle_to_platforms,
            )
            regex_pass2_count += 1
            review_rows.append(
                {
                    **_build_row(
                        message_row=message_row,
                        keyword=args.keyword,
                        stage="regex_pass2",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status=(
                            "in_sending_list"
                            if sending_list_matching_enabled and final_id in known_handles
                            else "out_of_sending_list"
                            if sending_list_matching_enabled
                            else "disabled"
                        ),
                        platform=platform,
                    ),
                    "message_row_id": int(message_row["id"]),
                    "llm_reason": "",
                    "llm_provider": "",
                    "llm_model": "",
                }
            )
            continue

        llm_candidates.append(
            {
                **_build_row(
                    message_row=message_row,
                    keyword=args.keyword,
                    stage="llm",
                    confidence="unknown",
                    final_id="",
                    sending_list_match_status="none" if sending_list_matching_enabled else "disabled",
                ),
                "message_row_id": int(message_row["id"]),
                "regex_greeting_candidates": greeting_candidates,
                "regex_explicit_candidates": unique_explicit or explicit_candidates,
                "sending_list_known_handles_hint": sorted(known_handles)[:500] if sending_list_matching_enabled else [],
                "llm_reason": "",
                "llm_provider": "",
                "llm_model": "",
            }
        )

    reviewed_llm_rows, llm_skipped_count = _review_candidates(
        llm_candidates,
        env_path=args.env_file,
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        wire_api=args.wire_api,
        llm_max_workers=args.llm_max_workers,
        llm_limit=args.llm_limit,
    )

    llm_high_count = 0
    llm_medium_count = 0
    llm_low_or_blank_count = 0

    for llm_row in reviewed_llm_rows:
        llm_handle = _clean_text(llm_row.get("llm_handle"))
        confidence = _clean_text(llm_row.get("resolution_confidence_final")).lower()
        platform = _resolve_platform_for_handle(
            llm_handle,
            body_pairs=_extract_platform_handle_pairs(_clean_text(llm_row.get("latest_external_full_body"))),
            handle_to_platforms=handle_to_platforms,
        )
        if confidence == "high" and llm_handle:
            llm_high_count += 1
            review_rows.append(
                {
                    **llm_row,
                    "Platform": llm_row.get("Platform") or "",
                    "final_id_final": llm_handle,
                    "resolution_stage_final": "llm",
                    "resolution_confidence_final": "high",
                }
            )
        elif confidence == "medium" and llm_handle:
            llm_medium_count += 1
            review_rows.append(
                {
                    **llm_row,
                    "Platform": llm_row.get("Platform") or "",
                    "final_id_final": llm_handle,
                    "resolution_stage_final": "llm",
                    "resolution_confidence_final": "medium",
                }
            )
        else:
            llm_low_or_blank_count += 1
            review_rows.append(
                {
                    **llm_row,
                    "Platform": llm_row.get("Platform") or "",
                    "final_id_final": llm_handle,
                    "resolution_stage_final": "uncertain" if not llm_handle else "llm",
                    "resolution_confidence_final": confidence or "low",
                }
            )

    review_rows.sort(
        key=lambda row: (_clean_text(row.get("latest_external_sent_at")), _clean_text(row.get("raw_path"))),
        reverse=True,
    )
    keep_rows = [
        row
        for row in review_rows
        if _clean_text(row.get("resolution_stage_final")) in {"pass0_sending_list_email", "regex_pass1", "regex_pass2", "llm"}
        and (_clean_text(row.get("resolution_stage_final")) != "llm" or _clean_text(row.get("resolution_confidence_final")).lower() == "high")
        and _clean_text(row.get("final_id_final"))
    ]
    manual_rows = [
        row
        for row in review_rows
        if row not in keep_rows and not _clean_text(row.get("resolution_stage_final")).startswith("filtered_")
    ]

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    review_xlsx = output_prefix.with_suffix(".xlsx")
    keep_xlsx = output_prefix.with_name(f"{output_prefix.name}_keep").with_suffix(".xlsx")
    manual_xlsx = output_prefix.with_name(f"{output_prefix.name}_manual_tail").with_suffix(".xlsx")
    llm_jsonl = output_prefix.with_name(f"{output_prefix.name}_llm_review").with_suffix(".jsonl")
    summary_json = output_prefix.with_name(f"{output_prefix.name}_summary").with_suffix(".json")

    headers = _source_headers()
    _write_xlsx(review_xlsx, headers, review_rows)
    _write_xlsx(keep_xlsx, headers, keep_rows)
    _write_xlsx(manual_xlsx, headers, manual_rows)
    _write_jsonl(llm_jsonl, reviewed_llm_rows)

    summary = {
        "keyword": args.keyword,
        "local_date": local_date.isoformat(),
        "timezone": args.timezone,
        "sending_list_matching_enabled": sending_list_matching_enabled,
        "input_workbook": str(input_workbook) if input_workbook is not None else "",
        "db_path": str(db_path),
        "parsed_field_message_count": len(parsed_messages),
        "external_message_count": len(external_messages),
        "pass0_sending_list_email_count": pass0_count,
        "regex_pass1_count": regex_pass1_count,
        "regex_pass2_count": regex_pass2_count,
        "rule_keep_count": pass0_count + regex_pass1_count + regex_pass2_count,
        "llm_candidate_count": len(llm_candidates),
        "llm_reviewed_count": max(0, len(reviewed_llm_rows) - llm_skipped_count),
        "llm_skipped_due_to_limit_count": llm_skipped_count,
        "llm_high_count": llm_high_count,
        "llm_medium_count": llm_medium_count,
        "llm_low_or_blank_count": llm_low_or_blank_count,
        "filtered_auto_reply_count": filtered_auto_reply_count,
        "keep_row_count": len(keep_rows),
        "manual_row_count": len(manual_rows),
        "review_xlsx_path": str(review_xlsx),
        "keep_xlsx_path": str(keep_xlsx),
        "manual_tail_xlsx_path": str(manual_xlsx),
        "llm_review_jsonl_path": str(llm_jsonl),
    }
    _write_json(summary_json, summary)
    summary["summary_json_path"] = str(summary_json)
    return summary


def main() -> int:
    args = _parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
