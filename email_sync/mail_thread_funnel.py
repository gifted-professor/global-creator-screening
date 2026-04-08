from __future__ import annotations

import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import requests

from .brand_keyword_match import _load_candidate_rows, _query_keyword_messages, build_keyword_hit_threads
from .creator_enrichment import (
    _clean_text,
    _extract_emails,
    _extract_handles_from_text,
    _infer_platform_from_value,
    _load_addresses,
    _normalize_email,
    _normalize_handle,
    _normalize_name,
    _platform_label,
    _write_xlsx,
)
from .db import Database
from .llm_review import (
    RETRYABLE_STATUS_CODES,
    _extract_response_text,
    is_retryable_llm_transport_failure,
    resolve_llm_review_config_chain,
)
from .relation_index import rebuild_relation_index


FUNNEL_HEADERS = [
    "thread_key",
    "subject",
    "latest_external_from",
    "latest_external_sent_at",
    "latest_external_clean_body",
    "latest_external_full_body",
    "matched_contact_email",
    "creator_emails",
    "Platform",
    "evidence_thread_key",
    "brand_message_id",
    "brand_message_sent_at",
    "brand_message_subject",
    "brand_message_folder",
    "brand_message_raw_path",
    "brand_message_snippet",
    "sending_list_match_status",
    "thread_resolution_status",
    "resolution_stage_final",
    "resolution_confidence_final",
    "final_creator_id",
    "final_id_final",
    "resolution_evidence",
    "candidate_sources",
    "original_decision",
    "final_decision",
    "original_reject_reason",
    "business_signal_detected",
    "review_priority",
    "rescue_rule_applied",
    "confidence_before_rescue",
    "confidence_after_rescue",
    "hard_reject_blocked_rescue",
    "llm_handle",
    "llm_evidence",
    "raw_path",
    "brand_keyword",
]

_TEAMMATE_NAMES = {
    "william",
    "eden",
    "rhea",
    "elin",
    "yvette",
    "astrid",
    "lilith",
    "lillith",
    "ruby",
}
_GENERIC_HANDLE_STOPS = {
    "hi",
    "hello",
    "hallo",
    "there",
    "team",
    "dear",
    "friend",
    "all",
    "everyone",
    "support",
    "creator",
    "brand",
    "campaign",
    "tapo",
    "skg",
    "miniso",
    "duet",
}
_GREETING_PATTERN = re.compile(
    r"(?im)(?:^|[\n>])\s*(?:hi|hello|hallo)\s*@?\s*\*?([A-Za-z0-9._]{3,40})\*?",
)
_SOCIAL_LABEL_PATTERN = re.compile(
    r"(?im)\b(tiktok|tt|instagram|ig|ins|youtube|yt|douyin)\s*[:：-]?\s*@?\s*([A-Za-z0-9._]{3,40})\b",
)
_GENERIC_AT_HANDLE_PATTERN = re.compile(r"(?<![A-Za-z0-9])@([A-Za-z0-9._]{3,40})\b")
_TIKTOK_URL_PATTERN = re.compile(r"tiktok\.com/@([A-Za-z0-9._]{3,40})", re.I)
_INSTAGRAM_URL_PATTERN = re.compile(r"instagram\.com/([A-Za-z0-9._]{3,40})", re.I)
_YOUTUBE_URL_PATTERN = re.compile(r"youtube\.com/@([A-Za-z0-9._-]{3,80})", re.I)
_BEACONS_LINKTREE_URL_PATTERN = re.compile(r"(?:beacons\.ai|linktr\.ee|linktree\.com)/([A-Za-z0-9._-]{3,80})", re.I)
_AUTO_REPLY_PATTERNS = (
    re.compile(r"(out of office|automatic reply|auto.?reply|ooo\b|vacation|abwesenheitsnotiz)", re.I),
    re.compile(r"(thank you for your email|deine anfrage ist bei uns gelandet|ticket#|\[##gl-\d+##\])", re.I),
    re.compile(r"(delivery has failed|delivery status notification|returned mail|undeliverable)", re.I),
)
_QUOTE_SPLIT_PATTERNS = (
    re.compile(r"(?im)^\s*>"),
    re.compile(r"(?im)^on .+ wrote:\s*$"),
    re.compile(r"(?im)^am .+ schrieb .+:\s*$"),
    re.compile(r"(?im)^from:\s"),
    re.compile(r"(?im)^de:\s"),
    re.compile(r"(?im)^-{2,}\s*forwarded message\s*-{2,}\s*$"),
)
_BUSINESS_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "business_collab": re.compile(r"\b(paid\s+collab(?:oration)?|collab(?:oration)?|partnership|campaign)\b", re.I),
    "business_pricing": re.compile(r"\b(rate\s*card|rates?|pricing|quote|budget|fee|fees|price)\b", re.I),
    "business_deliverables": re.compile(r"\b(deliverables?|usage rights?|whitelisting|media kit|invoice|contract)\b", re.I),
    "business_currency": re.compile(r"(?:[$€£]\s?\d{2,}|usd\s?\d{2,})", re.I),
}


def _normalize_platform(value: Any) -> str:
    inferred = _infer_platform_from_value(value)
    if inferred:
        return inferred
    text = _clean_text(value).lower()
    if not text:
        return ""
    if "instagram" in text or text in {"ig", "ins"}:
        return "instagram"
    if "tiktok" in text or "douyin" in text or text == "tt":
        return "tiktok"
    if "youtube" in text or text == "yt":
        return "youtube"
    return ""


def _build_candidate_identity_maps(
    candidate_rows: Sequence[dict[str, Any]],
) -> tuple[dict[str, set[str]], set[str], dict[str, set[str]], dict[str, set[tuple[str, str]]]]:
    email_to_handles: dict[str, set[str]] = {}
    known_handles: set[str] = set()
    handle_to_platforms: dict[str, set[str]] = {}
    email_to_profile_pairs: dict[str, set[tuple[str, str]]] = {}
    for row in candidate_rows:
        handle = _normalize_handle(row.get("@username")) or _normalize_handle(row.get("URL"))
        platform = _normalize_platform(row.get("Platform")) or _normalize_platform(row.get("URL"))
        if not handle:
            continue
        known_handles.add(handle)
        if platform:
            handle_to_platforms.setdefault(handle, set()).add(platform)
        for email in _extract_emails(row.get("Email")):
            normalized_email = _normalize_email(email)
            if not normalized_email:
                continue
            email_to_handles.setdefault(normalized_email, set()).add(handle)
            if platform:
                email_to_profile_pairs.setdefault(normalized_email, set()).add((handle, platform))
    return email_to_handles, known_handles, handle_to_platforms, email_to_profile_pairs


def _first_sender_email(message_row: Any) -> str:
    for key in ("from_json", "reply_to_json", "sender_json"):
        for item in _load_addresses(str(message_row[key] or "[]")):
            email = _normalize_email(item.get("address"))
            if email:
                return email
    return ""


def _all_sender_emails(message_row: Any) -> list[str]:
    seen: set[str] = set()
    emails: list[str] = []
    for key in ("from_json", "reply_to_json", "sender_json"):
        for item in _load_addresses(str(message_row[key] or "[]")):
            email = _normalize_email(item.get("address"))
            if not email or email in seen:
                continue
            seen.add(email)
            emails.append(email)
    return emails


def _is_external_sender(message_row: Any) -> bool:
    sender_emails = _all_sender_emails(message_row)
    if not sender_emails:
        return False
    return any(not email.endswith("@amagency.biz") for email in sender_emails)


def _build_full_body(message_row: Any) -> str:
    for value in (message_row["body_text"], message_row["snippet"], message_row["body_html"]):
        if value is None:
            continue
        text = html.unescape(str(value)).replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        if text:
            return text
    return ""


def _build_clean_body(full_body: str) -> str:
    text = str(full_body or "")
    if not text:
        return ""
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if any(pattern.search(line) for pattern in _QUOTE_SPLIT_PATTERNS):
            break
        lines.append(line)
    cleaned = _clean_text("\n".join(lines))
    return cleaned or _clean_text(full_body)[:1000]


def _filter_handle(candidate: str) -> str:
    handle = _normalize_handle(candidate)
    if not handle:
        return ""
    if handle in _TEAMMATE_NAMES or handle in _GENERIC_HANDLE_STOPS:
        return ""
    if len(handle) < 3:
        return ""
    return handle


def _extract_greeting_candidates(full_body: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for match in _GREETING_PATTERN.finditer(full_body or ""):
        handle = _filter_handle(match.group(1))
        if not handle or handle in seen:
            continue
        seen.add(handle)
        result.append(handle)
    return result


def _append_unique_string(result: list[str], value: str) -> None:
    if value and value not in result:
        result.append(value)


def _append_candidate_match(result: list[tuple[str, str]], handle: Any, source: str) -> None:
    normalized = _filter_handle(handle)
    if normalized:
        result.append((normalized, source))


def _extract_explicit_candidate_matches(full_body: str) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    body_text = full_body or ""
    for pattern, platform in (
        (_TIKTOK_URL_PATTERN, "tiktok"),
        (_INSTAGRAM_URL_PATTERN, "instagram"),
        (_YOUTUBE_URL_PATTERN, "youtube"),
    ):
        for match in pattern.finditer(body_text):
            _append_candidate_match(result, match.group(1), f"explicit_{platform}_url")
    for match in _BEACONS_LINKTREE_URL_PATTERN.finditer(body_text):
        _append_candidate_match(result, match.group(1), "explicit_beacons_linktree")
    for match in _SOCIAL_LABEL_PATTERN.finditer(body_text):
        platform = _normalize_platform(match.group(1)) or "social"
        _append_candidate_match(result, match.group(2), f"explicit_{platform}_label")
    for handle in _extract_handles_from_text(body_text):
        _append_candidate_match(result, handle, "explicit_handle_text")
    for match in _GENERIC_AT_HANDLE_PATTERN.finditer(body_text):
        _append_candidate_match(result, match.group(1), "explicit_at_handle")
    return result


def _extract_explicit_candidates(full_body: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for handle, _source in _extract_explicit_candidate_matches(full_body):
        if handle in seen:
            continue
        seen.add(handle)
        result.append(handle)
    return result


def _extract_platform_handle_pairs(full_body: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for match in _SOCIAL_LABEL_PATTERN.finditer(full_body or ""):
        platform = _normalize_platform(match.group(1))
        handle = _filter_handle(match.group(2))
        if platform and handle:
            result.setdefault(handle, set()).add(platform)
    for pattern, platform in (
        (_TIKTOK_URL_PATTERN, "tiktok"),
        (_INSTAGRAM_URL_PATTERN, "instagram"),
        (_YOUTUBE_URL_PATTERN, "youtube"),
    ):
        for match in pattern.finditer(full_body or ""):
            handle = _filter_handle(match.group(1))
            if handle:
                result.setdefault(handle, set()).add(platform)
    return result


def _build_source_map(candidates: Sequence[str], source: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for candidate in candidates:
        normalized = _filter_handle(candidate)
        if not normalized:
            continue
        result.setdefault(normalized, [])
        _append_unique_string(result[normalized], source)
    return result


def _build_match_source_map(matches: Sequence[tuple[str, str]], source_suffix: str = "") -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for handle, source in matches:
        normalized = _filter_handle(handle)
        if not normalized:
            continue
        result.setdefault(normalized, [])
        label = f"{source}_{source_suffix}" if source_suffix else source
        _append_unique_string(result[normalized], label)
    return result


def _merge_candidate_sources(*source_maps: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for source_map in source_maps:
        for handle, sources in source_map.items():
            target = merged.setdefault(handle, [])
            for source in sources:
                _append_unique_string(target, source)
    return merged


def _ordered_unique_candidates(*candidate_groups: Sequence[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for candidates in candidate_groups:
        for candidate in candidates:
            normalized = _filter_handle(candidate)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
    return result


def _serialize_sources(sources: Sequence[str] | None) -> str:
    payload = [str(item).strip() for item in (sources or []) if str(item).strip()]
    return json.dumps(payload, ensure_ascii=False) if payload else ""


def _load_serialized_sources(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = _clean_text(value)
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [item.strip() for item in re.split(r"[|,;\n]", text) if item.strip()]
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    return []


def _detect_business_signal_markers(subject: str, full_body: str) -> list[str]:
    haystack = "\n".join(part for part in (_clean_text(subject), _clean_text(full_body)) if part)
    if not haystack:
        return []
    result: list[str] = []
    for label, pattern in _BUSINESS_SIGNAL_PATTERNS.items():
        if pattern.search(haystack):
            result.append(label)
    return result


def _derive_row_decision(stage: str, confidence: str, final_id: str) -> str:
    normalized_stage = _clean_text(stage).lower()
    normalized_confidence = _clean_text(confidence).lower()
    normalized_final_id = _clean_text(final_id)
    if normalized_stage.startswith("filtered_"):
        return "reject"
    if normalized_final_id and (
        normalized_stage in {"pass0_sending_list_email", "regex_pass1", "regex_pass2"}
        or (normalized_stage == "llm" and normalized_confidence == "high")
    ):
        return "keep"
    return "manual"


def _stringify_boolean(value: Any) -> str:
    return "true" if bool(value) else "false"


def _resolve_platform_for_handle(
    handle: str,
    *,
    body_pairs: dict[str, set[str]] | None = None,
    handle_to_platforms: dict[str, set[str]] | None = None,
    preferred_platforms: Iterable[str] | None = None,
) -> str:
    body_platforms = {item for item in (body_pairs or {}).get(handle, set()) if item}
    if len(body_platforms) == 1:
        return next(iter(body_platforms))
    preferred = {item for item in (preferred_platforms or []) if item}
    if len(preferred) == 1:
        return next(iter(preferred))
    known_platforms = {item for item in (handle_to_platforms or {}).get(handle, set()) if item}
    if len(known_platforms) == 1:
        return next(iter(known_platforms))
    merged = body_platforms | preferred | known_platforms
    if len(merged) == 1:
        return next(iter(merged))
    return ""


def _matches_auto_reply(subject: str, body: str) -> bool:
    return any(pattern.search(subject or "") or pattern.search(body or "") for pattern in _AUTO_REPLY_PATTERNS)


def _ensure_relation_index(db: Database) -> None:
    message_count = int(db.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
    indexed_count = int(db.conn.execute("SELECT COUNT(*) FROM message_index").fetchone()[0])
    if message_count == 0:
        raise RuntimeError("本地邮件库为空，先运行 python3 -m email_sync sync")
    if indexed_count != message_count:
        rebuild_relation_index(db)


def _build_row(
    *,
    message_row: Any,
    keyword: str,
    stage: str,
    confidence: str,
    final_id: str,
    sending_list_match_status: str,
    platform: str = "",
    matched_contact_email: str = "",
    creator_emails: str = "",
    brand_message_id: str = "",
    brand_message_sent_at: str = "",
    brand_message_subject: str = "",
    brand_message_folder: str = "",
    brand_message_raw_path: str = "",
    brand_message_snippet: str = "",
    thread_resolution_status: str = "",
    resolution_evidence: str = "",
    candidate_sources: Sequence[str] | None = None,
    original_decision: str = "",
    final_decision: str = "",
    original_reject_reason: str = "",
    business_signal_detected: bool | None = None,
    review_priority: str = "",
    rescue_rule_applied: str = "",
    confidence_before_rescue: str = "",
    confidence_after_rescue: str = "",
    hard_reject_blocked_rescue: bool | None = None,
    llm_handle: str = "",
    llm_evidence: str = "",
) -> dict[str, Any]:
    full_body = _build_full_body(message_row)
    subject = _clean_text(message_row["subject"])
    thread_key = _clean_text(message_row["thread_key"]) or f"message:{int(message_row['id'])}"
    brand_raw_path = _clean_text(message_row["raw_path"])
    derived_decision = _derive_row_decision(stage, confidence, final_id)
    derived_business_signal = bool(_detect_business_signal_markers(subject, full_body))
    normalized_original_decision = _clean_text(original_decision) or derived_decision
    normalized_final_decision = _clean_text(final_decision) or normalized_original_decision
    normalized_reject_reason = _clean_text(original_reject_reason)
    if not normalized_reject_reason and normalized_original_decision == "reject":
        normalized_reject_reason = _clean_text(resolution_evidence) or stage
    return {
        "thread_key": thread_key,
        "subject": subject,
        "latest_external_from": _first_sender_email(message_row),
        "latest_external_sent_at": _clean_text(message_row["sent_at"]),
        "latest_external_clean_body": _build_clean_body(full_body),
        "latest_external_full_body": full_body,
        "matched_contact_email": _clean_text(matched_contact_email),
        "creator_emails": _clean_text(creator_emails),
        "Platform": _platform_label(platform) if platform else "",
        "evidence_thread_key": thread_key,
        "brand_message_id": str(int(message_row["id"])),
        "brand_message_sent_at": _clean_text(brand_message_sent_at or message_row["sent_at"]),
        "brand_message_subject": _clean_text(brand_message_subject or message_row["subject"]),
        "brand_message_folder": _clean_text(brand_message_folder or message_row["folder_name"]),
        "brand_message_raw_path": _clean_text(brand_message_raw_path or brand_raw_path),
        "brand_message_snippet": _clean_text(brand_message_snippet or message_row["snippet"]),
        "sending_list_match_status": sending_list_match_status,
        "thread_resolution_status": _clean_text(thread_resolution_status),
        "resolution_stage_final": stage,
        "resolution_confidence_final": confidence,
        "final_creator_id": final_id,
        "final_id_final": final_id,
        "resolution_evidence": _clean_text(resolution_evidence),
        "candidate_sources": _serialize_sources(candidate_sources),
        "original_decision": normalized_original_decision,
        "final_decision": normalized_final_decision,
        "original_reject_reason": normalized_reject_reason,
        "business_signal_detected": _stringify_boolean(
            derived_business_signal if business_signal_detected is None else business_signal_detected
        ),
        "review_priority": _clean_text(review_priority) or ("normal" if normalized_final_decision == "manual" else ""),
        "rescue_rule_applied": _clean_text(rescue_rule_applied),
        "confidence_before_rescue": _clean_text(confidence_before_rescue) or _clean_text(confidence),
        "confidence_after_rescue": _clean_text(confidence_after_rescue) or _clean_text(confidence),
        "hard_reject_blocked_rescue": _stringify_boolean(bool(hard_reject_blocked_rescue)),
        "llm_handle": llm_handle,
        "llm_evidence": llm_evidence,
        "raw_path": brand_raw_path,
        "brand_keyword": keyword,
    }


def _build_thread_row(
    *,
    thread_record: dict[str, Any],
    stage: str,
    confidence: str,
    final_id: str,
    sending_list_match_status: str,
    platform: str = "",
    matched_contact_email: str = "",
    creator_emails: str = "",
    thread_resolution_status: str = "",
    resolution_evidence: str = "",
    candidate_sources: Sequence[str] | None = None,
    original_decision: str = "",
    final_decision: str = "",
    original_reject_reason: str = "",
    business_signal_detected: bool | None = None,
    review_priority: str = "",
    rescue_rule_applied: str = "",
    confidence_before_rescue: str = "",
    confidence_after_rescue: str = "",
    hard_reject_blocked_rescue: bool | None = None,
    llm_handle: str = "",
    llm_evidence: str = "",
) -> dict[str, Any]:
    thread_key = _clean_text(thread_record.get("thread_key"))
    latest_raw_path = _clean_text(thread_record.get("latest_external_raw_path"))
    subject = _clean_text(thread_record.get("latest_external_subject"))
    full_body = _clean_text(thread_record.get("latest_external_full_body"))
    derived_decision = _derive_row_decision(stage, confidence, final_id)
    derived_business_signal = bool(_detect_business_signal_markers(subject, full_body))
    normalized_original_decision = _clean_text(original_decision) or derived_decision
    normalized_final_decision = _clean_text(final_decision) or normalized_original_decision
    normalized_reject_reason = _clean_text(original_reject_reason)
    if not normalized_reject_reason and normalized_original_decision == "reject":
        normalized_reject_reason = _clean_text(resolution_evidence) or stage
    return {
        "thread_key": thread_key,
        "subject": subject,
        "latest_external_from": _clean_text(thread_record.get("latest_external_from")),
        "latest_external_sent_at": _clean_text(thread_record.get("latest_external_sent_at")),
        "latest_external_clean_body": _clean_text(thread_record.get("latest_external_clean_body")),
        "latest_external_full_body": full_body,
        "matched_contact_email": _clean_text(matched_contact_email),
        "creator_emails": _clean_text(creator_emails),
        "Platform": _platform_label(platform) if platform else "",
        "evidence_thread_key": thread_key,
        "brand_message_id": _clean_text(thread_record.get("latest_external_message_id")),
        "brand_message_sent_at": _clean_text(thread_record.get("latest_external_sent_at")),
        "brand_message_subject": _clean_text(thread_record.get("latest_external_subject")),
        "brand_message_folder": _clean_text(thread_record.get("latest_external_folder")),
        "brand_message_raw_path": latest_raw_path,
        "brand_message_snippet": _clean_text(thread_record.get("latest_external_snippet")),
        "sending_list_match_status": sending_list_match_status,
        "thread_resolution_status": _clean_text(thread_resolution_status),
        "resolution_stage_final": stage,
        "resolution_confidence_final": confidence,
        "final_creator_id": final_id,
        "final_id_final": final_id,
        "resolution_evidence": _clean_text(resolution_evidence),
        "candidate_sources": _serialize_sources(candidate_sources),
        "original_decision": normalized_original_decision,
        "final_decision": normalized_final_decision,
        "original_reject_reason": normalized_reject_reason,
        "business_signal_detected": _stringify_boolean(
            derived_business_signal if business_signal_detected is None else business_signal_detected
        ),
        "review_priority": _clean_text(review_priority) or ("normal" if normalized_final_decision == "manual" else ""),
        "rescue_rule_applied": _clean_text(rescue_rule_applied),
        "confidence_before_rescue": _clean_text(confidence_before_rescue) or _clean_text(confidence),
        "confidence_after_rescue": _clean_text(confidence_after_rescue) or _clean_text(confidence),
        "hard_reject_blocked_rescue": _stringify_boolean(bool(hard_reject_blocked_rescue)),
        "llm_handle": _clean_text(llm_handle),
        "llm_evidence": _clean_text(llm_evidence),
        "raw_path": latest_raw_path,
        "brand_keyword": _clean_text(thread_record.get("brand_keyword")),
    }


def _parse_json_object(raw_text: str) -> dict[str, Any] | None:
    cleaned = str(raw_text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                payload = json.loads(cleaned[start : end + 1])
                return payload if isinstance(payload, dict) else None
            except json.JSONDecodeError:
                return None
    return None


def _parse_llm_handle_response(raw_text: str) -> dict[str, str]:
    payload = _parse_json_object(raw_text) or {}
    handle = _filter_handle(payload.get("handle"))
    confidence = _clean_text(payload.get("confidence")).lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "low"
    return {
        "handle": handle,
        "confidence": confidence,
        "reason": _clean_text(payload.get("reason")) or _clean_text(raw_text)[:500],
        "evidence": _clean_text(payload.get("evidence")) or "",
    }


def _build_llm_messages(record: dict[str, Any]) -> list[dict[str, str]]:
    payload = {
        "brand_keyword": record.get("brand_keyword"),
        "subject": record.get("subject"),
        "latest_external_from": record.get("latest_external_from"),
        "latest_external_sent_at": record.get("latest_external_sent_at"),
        "latest_external_clean_body": record.get("latest_external_clean_body"),
        "latest_external_full_body": record.get("latest_external_full_body"),
        "regex_greeting_candidates": record.get("regex_greeting_candidates"),
        "regex_explicit_candidates": record.get("regex_explicit_candidates"),
        "sending_list_known_handles_hint": list(record.get("sending_list_known_handles_hint") or []),
        "instructions": [
            "Extract the most likely creator social handle from this brand collaboration email thread.",
            "Return JSON only.",
            "Prefer explicit greetings, quoted outreach text, signature handles, or social profile URLs.",
            "Do not return teammate or internal operator names.",
            "If no creator handle is identifiable, return empty handle with low confidence.",
        ],
        "output_schema": {
            "handle": "creator handle without @ when identifiable, else empty string",
            "confidence": "high | medium | low",
            "reason": "brief reason",
            "evidence": "short evidence snippet",
        },
    }
    return [
        {
            "role": "system",
            "content": "You extract creator social handles from brand email threads. Return JSON only.",
        },
        {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=False, indent=2),
        },
    ]


def _invoke_handle_llm(config: Any, record: dict[str, Any]) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    messages = _build_llm_messages(record)
    if config.wire_api == "responses":
        url = f"{config.base_url}/responses"
        body: dict[str, Any] = {
            "model": config.model,
            "input": [
                {"role": item["role"], "content": [{"type": "input_text", "text": item["content"]}]}
                for item in messages
            ],
        }
        if getattr(config, "reasoning_effort", ""):
            body["reasoning"] = {"effort": config.reasoning_effort}
    else:
        url = f"{config.base_url}/chat/completions"
        body = {
            "model": config.model,
            "messages": messages,
            "temperature": 0,
        }

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = requests.post(url, headers=headers, json=body, timeout=config.timeout_seconds)
            if response.status_code >= 400:
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < 3:
                    time.sleep(min(4.0, 1.5 * attempt))
                    continue
                raise RuntimeError(f"LLM HTTP {response.status_code}: {response.text[:400]}")
            payload = response.json()
            parsed = _parse_llm_handle_response(_extract_response_text(payload))
            if not parsed["evidence"]:
                parsed["evidence"] = parsed["reason"]
            return parsed
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if is_retryable_llm_transport_failure(exc) and attempt < 3:
                time.sleep(min(4.0, 1.5 * attempt))
                continue
            break
    raise RuntimeError(str(last_error) if last_error else "mail-thread llm review failed")


def _run_default_llm_review(
    records: Sequence[dict[str, Any]],
    *,
    env_path: str,
    base_url: str | None,
    api_key: str | None,
    model: str | None,
    wire_api: str | None,
    max_workers: int,
) -> list[dict[str, Any]]:
    configs = resolve_llm_review_config_chain(
        env_path,
        base_url=base_url,
        api_key=api_key,
        model=model,
        wire_api=wire_api,
    )

    def review_one(record: dict[str, Any]) -> dict[str, Any]:
        last_error = ""
        for config in configs:
            try:
                result = _invoke_handle_llm(config, record)
                return {
                    **record,
                    "llm_handle": result["handle"],
                    "resolution_confidence_final": result["confidence"],
                    "llm_evidence": result["evidence"] or result["reason"],
                    "llm_reason": result["reason"],
                    "llm_provider": config.provider_name,
                    "llm_model": config.model,
                }
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                continue
        return {
            **record,
            "llm_handle": "",
            "resolution_confidence_final": "low",
            "llm_evidence": last_error[:500],
            "llm_reason": last_error[:500],
            "llm_provider": "",
            "llm_model": "",
        }

    if not records:
        return []
    max_workers = max(1, min(int(max_workers or 1), len(records)))
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(review_one, record) for record in records]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _iter_thread_sender_emails(thread_record: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    raw_value = _clean_text(thread_record.get("latest_external_sender_emails"))
    for part in re.split(r"[|,\n]", raw_value):
        email = _normalize_email(part)
        if not email or email in seen:
            continue
        seen.add(email)
        result.append(email)
    fallback = _normalize_email(thread_record.get("latest_external_from"))
    if fallback and fallback not in seen:
        result.append(fallback)
    return result


def build_mail_thread_funnel_keep_workbook(
    *,
    db: Database,
    input_path: Path,
    output_prefix: Path,
    keyword: str,
    sent_since: date | None = None,
    message_limit: int = 0,
    include_from: bool = False,
    env_path: str = ".env",
    base_url: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    wire_api: str | None = None,
    llm_runner: Callable[[Sequence[dict[str, Any]]], Sequence[dict[str, Any]]] | None = None,
    llm_max_workers: int = 12,
    thread_first_resolution: bool = False,
) -> dict[str, Any]:
    normalized_keyword = _clean_text(keyword)
    if not normalized_keyword:
        raise ValueError("缺少 keyword。")

    db.init_schema()
    _ensure_relation_index(db)
    _, candidate_rows = _load_candidate_rows(input_path)
    messages = _query_keyword_messages(
        db,
        normalized_keyword,
        sent_since=sent_since,
        limit=max(0, int(message_limit)),
    )
    email_to_handles, known_handles, handle_to_platforms, email_to_profile_pairs = _build_candidate_identity_maps(candidate_rows)
    external_messages = [row for row in messages if _is_external_sender(row)]
    thread_records = build_keyword_hit_threads(messages, keyword=normalized_keyword) if thread_first_resolution else []

    review_rows: list[dict[str, Any]] = []
    llm_candidates: list[dict[str, Any]] = []

    pass0_count = 0
    regex_pass1_count = 0
    regex_pass2_count = 0
    regex_pass1_rescued_count = 0
    regex_pass2_rescued_count = 0
    full_body_only_hit_count = 0
    beacons_linktree_hit_count = 0
    llm_high_count = 0
    manual_count = 0
    filtered_auto_reply_count = 0
    no_match_count = 0
    resolved_thread_count = 0

    if thread_first_resolution:
        for thread_record in thread_records:
            full_body = _clean_text(thread_record.get("latest_external_full_body"))
            clean_body = _clean_text(thread_record.get("latest_external_clean_body")) or _build_clean_body(full_body)
            body_pairs = _extract_platform_handle_pairs(full_body)
            subject = _clean_text(thread_record.get("latest_external_subject"))
            sender_emails = _iter_thread_sender_emails(thread_record)
            creator_email_text = " | ".join(sender_emails)
            matched_contact_email = sender_emails[0] if sender_emails else ""

            if _matches_auto_reply(subject, full_body):
                filtered_auto_reply_count += 1
                review_rows.append(
                    _build_thread_row(
                        thread_record=thread_record,
                        stage="filtered_auto_reply",
                        confidence="low",
                        final_id="",
                        sending_list_match_status="none",
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="unresolved",
                        resolution_evidence="auto_reply_detected",
                    )
                )
                continue

            sender_handles: set[str] = set()
            sender_profile_pairs: set[tuple[str, str]] = set()
            for sender_email in sender_emails:
                sender_handles.update(email_to_handles.get(sender_email, set()))
                sender_profile_pairs.update(email_to_profile_pairs.get(sender_email, set()))
            if len(sender_handles) == 1:
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
                    _build_thread_row(
                        thread_record=thread_record,
                        stage="pass0_sending_list_email",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="email_exact",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"sender_email_exact:{matched_contact_email}",
                        candidate_sources=["sender_email_exact"],
                    )
                )
                continue

            clean_greeting_candidates = _extract_greeting_candidates(clean_body)
            full_greeting_candidates = _extract_greeting_candidates(full_body)
            greeting_candidates = _ordered_unique_candidates(clean_greeting_candidates, full_greeting_candidates)
            greeting_source_map = _merge_candidate_sources(
                _build_source_map(clean_greeting_candidates, "greeting_clean"),
                _build_source_map(
                    [candidate for candidate in full_greeting_candidates if candidate not in set(clean_greeting_candidates)],
                    "greeting_quoted",
                ),
            )
            if len(greeting_candidates) == 1:
                final_id = greeting_candidates[0]
                platform = _resolve_platform_for_handle(
                    final_id,
                    body_pairs=body_pairs,
                    handle_to_platforms=handle_to_platforms,
                )
                regex_pass1_count += 1
                sources = list(greeting_source_map.get(final_id, []))
                if "greeting_quoted" in sources:
                    regex_pass1_rescued_count += 1
                    full_body_only_hit_count += 1
                review_rows.append(
                    _build_thread_row(
                        thread_record=thread_record,
                        stage="regex_pass1",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="in_sending_list" if final_id in known_handles else "out_of_sending_list",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"greeting:{final_id}",
                        candidate_sources=sources,
                    )
                )
                continue

            clean_explicit_matches = _extract_explicit_candidate_matches(clean_body)
            full_explicit_matches = _extract_explicit_candidate_matches(full_body)
            explicit_source_map = _merge_candidate_sources(
                _build_match_source_map(clean_explicit_matches, "clean"),
                _build_match_source_map(
                    [
                        (handle, source)
                        for handle, source in full_explicit_matches
                        if handle not in {candidate for candidate, _source in clean_explicit_matches}
                    ],
                    "quoted",
                ),
            )
            explicit_candidates = _ordered_unique_candidates(
                [candidate for candidate, _source in clean_explicit_matches],
                [candidate for candidate, _source in full_explicit_matches],
            )
            unique_explicit = [candidate for candidate in explicit_candidates if candidate not in set(greeting_candidates)]
            if len(unique_explicit) == 1:
                final_id = unique_explicit[0]
                platform = _resolve_platform_for_handle(
                    final_id,
                    body_pairs=body_pairs,
                    handle_to_platforms=handle_to_platforms,
                )
                regex_pass2_count += 1
                sources = list(explicit_source_map.get(final_id, []))
                if any(source.endswith("_quoted") for source in sources):
                    regex_pass2_rescued_count += 1
                    full_body_only_hit_count += 1
                if any("beacons_linktree" in source for source in sources):
                    beacons_linktree_hit_count += 1
                review_rows.append(
                    _build_thread_row(
                        thread_record=thread_record,
                        stage="regex_pass2",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="in_sending_list" if final_id in known_handles else "out_of_sending_list",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"explicit:{final_id}",
                        candidate_sources=sources,
                    )
                )
                continue

            llm_candidate_sources = _ordered_unique_candidates(greeting_candidates, unique_explicit or explicit_candidates)
            llm_source_map = _merge_candidate_sources(greeting_source_map, explicit_source_map)
            serialized_sources = []
            for handle in llm_candidate_sources:
                for source in llm_source_map.get(handle, []):
                    _append_unique_string(serialized_sources, source)
            llm_candidates.append(
                {
                    **_build_thread_row(
                        thread_record=thread_record,
                        stage="llm",
                        confidence="unknown",
                        final_id="",
                        sending_list_match_status="none",
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="unresolved",
                        candidate_sources=serialized_sources,
                    ),
                    "regex_greeting_candidates": greeting_candidates,
                    "regex_explicit_candidates": unique_explicit or explicit_candidates,
                    "sending_list_known_handles_hint": sorted(known_handles)[:500],
                }
            )
    else:
        for message_row in external_messages:
            full_body = _build_full_body(message_row)
            clean_body = _build_clean_body(full_body)
            body_pairs = _extract_platform_handle_pairs(full_body)
            subject = _clean_text(message_row["subject"])
            if _matches_auto_reply(subject, full_body):
                filtered_auto_reply_count += 1
                review_rows.append(
                    _build_row(
                        message_row=message_row,
                        keyword=normalized_keyword,
                        stage="filtered_auto_reply",
                        confidence="low",
                        final_id="",
                        sending_list_match_status="none",
                        thread_resolution_status="unresolved",
                        resolution_evidence="auto_reply_detected",
                    )
                )
                continue

            sender_handles: set[str] = set()
            sender_profile_pairs: set[tuple[str, str]] = set()
            sender_emails = _all_sender_emails(message_row)
            matched_contact_email = sender_emails[0] if sender_emails else ""
            creator_email_text = " | ".join(sender_emails)
            for sender_email in sender_emails:
                sender_handles.update(email_to_handles.get(sender_email, set()))
                sender_profile_pairs.update(email_to_profile_pairs.get(sender_email, set()))
            if len(sender_handles) == 1:
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
                    _build_row(
                        message_row=message_row,
                        keyword=normalized_keyword,
                        stage="pass0_sending_list_email",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="email_exact",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"sender_email_exact:{matched_contact_email}",
                        candidate_sources=["sender_email_exact"],
                    )
                )
                continue

            clean_greeting_candidates = _extract_greeting_candidates(clean_body)
            full_greeting_candidates = _extract_greeting_candidates(full_body)
            greeting_candidates = _ordered_unique_candidates(clean_greeting_candidates, full_greeting_candidates)
            greeting_source_map = _merge_candidate_sources(
                _build_source_map(clean_greeting_candidates, "greeting_clean"),
                _build_source_map(
                    [candidate for candidate in full_greeting_candidates if candidate not in set(clean_greeting_candidates)],
                    "greeting_quoted",
                ),
            )
            if len(greeting_candidates) == 1:
                final_id = greeting_candidates[0]
                platform = _resolve_platform_for_handle(
                    final_id,
                    body_pairs=body_pairs,
                    handle_to_platforms=handle_to_platforms,
                )
                regex_pass1_count += 1
                sources = list(greeting_source_map.get(final_id, []))
                if "greeting_quoted" in sources:
                    regex_pass1_rescued_count += 1
                    full_body_only_hit_count += 1
                review_rows.append(
                    _build_row(
                        message_row=message_row,
                        keyword=normalized_keyword,
                        stage="regex_pass1",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="in_sending_list" if final_id in known_handles else "out_of_sending_list",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"greeting:{final_id}",
                        candidate_sources=sources,
                    )
                )
                continue

            clean_explicit_matches = _extract_explicit_candidate_matches(clean_body)
            full_explicit_matches = _extract_explicit_candidate_matches(full_body)
            explicit_source_map = _merge_candidate_sources(
                _build_match_source_map(clean_explicit_matches, "clean"),
                _build_match_source_map(
                    [
                        (handle, source)
                        for handle, source in full_explicit_matches
                        if handle not in {candidate for candidate, _source in clean_explicit_matches}
                    ],
                    "quoted",
                ),
            )
            explicit_candidates = _ordered_unique_candidates(
                [candidate for candidate, _source in clean_explicit_matches],
                [candidate for candidate, _source in full_explicit_matches],
            )
            unique_explicit = [candidate for candidate in explicit_candidates if candidate not in set(greeting_candidates)]
            if len(unique_explicit) == 1:
                final_id = unique_explicit[0]
                platform = _resolve_platform_for_handle(
                    final_id,
                    body_pairs=body_pairs,
                    handle_to_platforms=handle_to_platforms,
                )
                regex_pass2_count += 1
                sources = list(explicit_source_map.get(final_id, []))
                if any(source.endswith("_quoted") for source in sources):
                    regex_pass2_rescued_count += 1
                    full_body_only_hit_count += 1
                if any("beacons_linktree" in source for source in sources):
                    beacons_linktree_hit_count += 1
                review_rows.append(
                    _build_row(
                        message_row=message_row,
                        keyword=normalized_keyword,
                        stage="regex_pass2",
                        confidence="high",
                        final_id=final_id,
                        sending_list_match_status="in_sending_list" if final_id in known_handles else "out_of_sending_list",
                        platform=platform,
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="resolved",
                        resolution_evidence=f"explicit:{final_id}",
                        candidate_sources=sources,
                    )
                )
                continue

            llm_candidate_sources = _ordered_unique_candidates(greeting_candidates, unique_explicit or explicit_candidates)
            llm_source_map = _merge_candidate_sources(greeting_source_map, explicit_source_map)
            serialized_sources = []
            for handle in llm_candidate_sources:
                for source in llm_source_map.get(handle, []):
                    _append_unique_string(serialized_sources, source)
            llm_candidates.append(
                {
                    **_build_row(
                        message_row=message_row,
                        keyword=normalized_keyword,
                        stage="llm",
                        confidence="unknown",
                        final_id="",
                        sending_list_match_status="none",
                        matched_contact_email=matched_contact_email,
                        creator_emails=creator_email_text,
                        thread_resolution_status="unresolved",
                        candidate_sources=serialized_sources,
                    ),
                    "regex_greeting_candidates": greeting_candidates,
                    "regex_explicit_candidates": unique_explicit or explicit_candidates,
                    "sending_list_known_handles_hint": sorted(known_handles)[:500],
                }
            )

    reviewed_llm_rows: list[dict[str, Any]]
    if llm_candidates:
        reviewed_llm_rows = list(
            llm_runner(llm_candidates)
            if llm_runner is not None
            else _run_default_llm_review(
                llm_candidates,
                env_path=env_path,
                base_url=base_url,
                api_key=api_key,
                model=model,
                wire_api=wire_api,
                max_workers=llm_max_workers,
            )
        )
    else:
        reviewed_llm_rows = []

    for llm_row in reviewed_llm_rows:
        llm_handle = _filter_handle(llm_row.get("llm_handle"))
        confidence = _clean_text(llm_row.get("resolution_confidence_final")).lower()
        candidate_sources = _load_serialized_sources(llm_row.get("candidate_sources"))
        if llm_handle or _clean_text(llm_row.get("llm_evidence")) or _clean_text(llm_row.get("llm_reason")):
            _append_unique_string(candidate_sources, "llm")
        platform = _resolve_platform_for_handle(
            llm_handle,
            body_pairs=_extract_platform_handle_pairs(_clean_text(llm_row.get("latest_external_full_body"))),
            handle_to_platforms=handle_to_platforms,
        )
        if llm_handle and confidence == "high":
            llm_high_count += 1
            review_rows.append(
                {
                    **llm_row,
                    "Platform": _platform_label(platform) if platform else _clean_text(llm_row.get("Platform")),
                    "final_id_final": llm_handle,
                    "final_creator_id": llm_handle,
                    "resolution_stage_final": "llm",
                    "resolution_confidence_final": "high",
                    "thread_resolution_status": "resolved",
                    "resolution_evidence": _clean_text(llm_row.get("llm_evidence")) or _clean_text(llm_row.get("llm_reason")),
                    "candidate_sources": _serialize_sources(candidate_sources),
                }
            )
            continue
        if llm_handle or _clean_text(llm_row.get("llm_evidence")):
            manual_count += 1
            review_rows.append(
                {
                    **llm_row,
                    "Platform": _platform_label(platform) if platform else _clean_text(llm_row.get("Platform")),
                    "final_id_final": llm_handle,
                    "final_creator_id": llm_handle,
                    "resolution_stage_final": "llm",
                    "resolution_confidence_final": confidence or "low",
                    "thread_resolution_status": "unresolved",
                    "resolution_evidence": _clean_text(llm_row.get("llm_evidence")) or _clean_text(llm_row.get("llm_reason")),
                    "candidate_sources": _serialize_sources(candidate_sources),
                }
            )
            continue
        no_match_count += 1
        manual_count += 1
        review_rows.append(
            {
                **llm_row,
                "thread_resolution_status": "unresolved",
                "resolution_stage_final": "uncertain",
                "resolution_confidence_final": "low",
                "final_creator_id": "",
                "final_id_final": "",
                "resolution_evidence": _clean_text(llm_row.get("llm_evidence")) or _clean_text(llm_row.get("llm_reason")),
                "candidate_sources": _serialize_sources(candidate_sources),
            }
        )

    def sort_key(row: dict[str, Any]) -> tuple[str, str]:
        return (_clean_text(row.get("latest_external_sent_at")), _clean_text(row.get("raw_path")))

    review_rows.sort(key=sort_key, reverse=True)
    keep_rows = [
        row
        for row in review_rows
        if _clean_text(row.get("resolution_stage_final"))
        in {"pass0_sending_list_email", "regex_pass1", "regex_pass2", "llm"}
        and (_clean_text(row.get("resolution_stage_final")) != "llm" or _clean_text(row.get("resolution_confidence_final")).lower() == "high")
        and _clean_text(row.get("final_id_final"))
    ]
    manual_rows = [
        row
        for row in review_rows
        if row not in keep_rows and not _clean_text(row.get("resolution_stage_final")).startswith("filtered_")
    ]
    resolved_thread_count = len({(_clean_text(row.get("thread_key")) or _clean_text(row.get("evidence_thread_key"))) for row in keep_rows if _clean_text(row.get("thread_key")) or _clean_text(row.get("evidence_thread_key"))})

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    review_xlsx = output_prefix.with_suffix(".xlsx")
    keep_xlsx = output_prefix.with_name(f"{output_prefix.name}_keep").with_suffix(".xlsx")
    manual_xlsx = output_prefix.with_name(f"{output_prefix.name}_manual_tail").with_suffix(".xlsx")
    unresolved_json = output_prefix.with_name(f"{output_prefix.name}_unresolved_threads").with_suffix(".json")
    unresolved_xlsx = output_prefix.with_name(f"{output_prefix.name}_unresolved_threads").with_suffix(".xlsx")
    _write_xlsx(review_xlsx, FUNNEL_HEADERS, review_rows)
    _write_xlsx(keep_xlsx, FUNNEL_HEADERS, keep_rows)
    _write_xlsx(manual_xlsx, FUNNEL_HEADERS, manual_rows)
    unresolved_json.write_text(
        json.dumps(
            {
                "keyword": normalized_keyword,
                "thread_first_resolution": bool(thread_first_resolution),
                "unresolved_thread_count": len(manual_rows),
                "rows": manual_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_xlsx(unresolved_xlsx, FUNNEL_HEADERS, manual_rows)

    return {
        "keyword": normalized_keyword,
        "sent_since": sent_since.isoformat() if sent_since else "",
        "message_hit_count": len(messages),
        "thread_hit_count": len(thread_records) if thread_first_resolution else len(review_rows),
        "external_message_count": len(external_messages),
        "pass0_sending_list_email_count": pass0_count,
        "regex_pass1_count": regex_pass1_count,
        "regex_pass2_count": regex_pass2_count,
        "regex_pass1_rescued_count": regex_pass1_rescued_count,
        "regex_pass2_rescued_count": regex_pass2_rescued_count,
        "full_body_only_hit_count": full_body_only_hit_count,
        "beacons_linktree_hit_count": beacons_linktree_hit_count,
        "r1_rescue_count": 0,
        "r2_rescue_count": 0,
        "r3_rescue_count": 0,
        "r4_rescue_count": 0,
        "rescued_to_manual_count": 0,
        "rescued_to_keep_count": 0,
        "hard_reject_blocked_rescue_count": 0,
        "llm_invocation_count_before": len(llm_candidates) + regex_pass1_rescued_count + regex_pass2_rescued_count,
        "llm_invocation_count_after": len(llm_candidates),
        "llm_high_count": llm_high_count,
        "manual_row_count": len(manual_rows),
        "filtered_auto_reply_count": filtered_auto_reply_count,
        "no_match_count": no_match_count,
        "keep_row_count": len(keep_rows),
        "resolved_thread_count": resolved_thread_count,
        "thread_first_resolution": bool(thread_first_resolution),
        "review_xlsx_path": str(review_xlsx),
        "keep_xlsx_path": str(keep_xlsx),
        "manual_tail_xlsx_path": str(manual_xlsx),
        "unresolved_threads_json_path": str(unresolved_json),
        "unresolved_threads_xlsx_path": str(unresolved_xlsx),
    }
