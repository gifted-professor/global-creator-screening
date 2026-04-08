from __future__ import annotations

import argparse
from datetime import datetime
from functools import lru_cache
import json
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Sequence

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.final_export_merge import (
    FINAL_UPLOAD_COLUMNS,
    _build_quote_text,
    _clean_text,
    _extract_handle,
    _format_date,
    _normalize_platform,
    _normalize_url,
    _resolve_existing_local_paths,
)
from email_sync.known_thread_update import process_known_thread_updates
from email_sync.thread_assignments import lookup_thread_assignment


SUCCESSFUL_DOWNSTREAM_STATUSES = {"completed", "completed_with_partial_scrape"}
MAIL_ONLY_UPDATE_MODE = "mail_only_update"
CREATE_OR_MAIL_ONLY_UPDATE_MODE = "create_or_mail_only_update"
CREATE_OR_UPDATE_MODE = "create_or_update"
_TASK_GROUP_ALIASES = {
    "skg": {"skg", "skg1", "skg-1", "skg2", "skg-2"},
}
_TASK_GROUP_DEFAULT_BRAND_KEYWORDS = {
    "skg": "SKG",
}
_TASK_GROUP_SUFFIX_PATTERN = re.compile(r"(?:[-_\s]*\d+)$")
_KEEP_OWNER_DISPLAY_FIELD = "达人对接人"
_KEEP_OWNER_ENGLISH_NAME_FIELD = "达人对接人_英文名"
_KEEP_OWNER_EMPLOYEE_ID_FIELD = "达人对接人_employee_id"
_KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD = "达人对接人_employee_record_id"
_KEEP_OWNER_EMPLOYEE_EMAIL_FIELD = "达人对接人_employee_email"
_KEEP_OWNER_OWNER_NAME_FIELD = "达人对接人_owner_name"
_KEEP_OWNER_STATUS_FIELD = "__owner_resolution_status"
_KEEP_OWNER_ALIAS_FIELD = "__owner_resolution_aliases"
_LAST_MAIL_KEYS = (
    "last_mail_message_id",
    "last_mail_time",
    "last_mail_subject",
    "last_mail_snippet",
    "last_mail_raw_path",
)
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "yahoo.com",
    "yahoo.co.uk",
    "live.com",
    "aol.com",
    "gmx.de",
    "web.de",
    "proton.me",
    "protonmail.com",
}
_BUSINESS_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "business_collab": re.compile(r"\b(paid\s+collab(?:oration)?|collab(?:oration)?|partnership|campaign)\b", re.I),
    "business_pricing": re.compile(r"\b(rate\s*card|rates?|pricing|quote|budget|fee|fees|price)\b", re.I),
    "business_deliverables": re.compile(r"\b(deliverables?|usage rights?|whitelisting|media kit|invoice|contract)\b", re.I),
    "business_currency": re.compile(r"(?:[$€£]\s?\d{2,}|usd\s?\d{2,})", re.I),
}
_SCRAPE_FAILED_PATTERNS = (
    re.compile(r"(抓取失败|scrape failed|failed to scrape|unable to scrape|fetch failed|采集失败)", re.I),
    re.compile(r"(profile.*unavailable|资料抓取失败|页面抓取失败)", re.I),
)
_INSUFFICIENT_PROFILE_PATTERNS = (
    re.compile(r"(资料不足|信息不足|insufficient profile|not enough (?:profile )?data|内容太少|样本太少)", re.I),
    re.compile(r"(profile data missing|insufficient content|low data)", re.I),
)
_RECENTLY_INACTIVE_PATTERNS = (
    re.compile(r"(近\s*30\s*天.*无更新|最近\s*30\s*天.*无更新|30\s*days?.*no (?:recent )?posts?)", re.I),
    re.compile(r"(inactive.*30\s*days?|no recent posts?|recently inactive)", re.I),
)
_HARD_REJECT_PATTERNS = (
    re.compile(r"(black\s*list|blacklisted|blacklist|fraud|scam|fake followers?)", re.I),
    re.compile(r"(minor|underage|未成年|adult content|explicit content|赌博|illegal|违法)", re.I),
    re.compile(r"(politics|religion|hate speech|violent|violence)", re.I),
)
_RESCUE_SUMMARY_KEYS = (
    "regex_pass1_rescued_count",
    "regex_pass2_rescued_count",
    "full_body_only_hit_count",
    "beacons_linktree_hit_count",
    "r1_rescue_count",
    "r2_rescue_count",
    "r3_rescue_count",
    "r4_rescue_count",
    "rescued_to_manual_count",
    "rescued_to_keep_count",
    "hard_reject_blocked_rescue_count",
    "llm_invocation_count_before",
    "llm_invocation_count_after",
)
_MAIL_ONLY_BUCKET_SUMMARY_KEYS = (
    "mail_only_creator_replied_count",
    "mail_only_outbound_only_skip_count",
    "mail_only_creator_identity_unresolved_count",
    "mail_only_thread_key_missing_count",
    "mail_only_candidate_count_before_source_dedup",
    "mail_only_candidate_count_after_source_dedup",
    "mail_only_source_identity_deduped_count",
    "mail_only_routed_mail_only_candidate_count",
    "mail_only_routed_full_screening_candidate_count",
    "mail_only_upload_decision_count",
    "mail_only_upload_live_existing_hit_count",
    "mail_only_upload_mail_idempotency_skip_count",
)


def _load_runtime_dependencies() -> dict[str, Any]:
    from feishu_screening_bridge.bitable_upload import (
        fetch_existing_bitable_record_analysis,
        fetch_existing_bitable_record_index,
        upload_final_review_payload_to_bitable,
    )
    from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
    from feishu_screening_bridge.local_env import load_local_env
    from feishu_screening_bridge.task_upload_sync import inspect_task_upload_assignments
    from scripts.run_keep_list_screening_pipeline import run_keep_list_screening_pipeline
    from scripts.run_task_upload_to_keep_list_pipeline import run_task_upload_to_keep_list_pipeline

    return {
        "DEFAULT_FEISHU_BASE_URL": DEFAULT_FEISHU_BASE_URL,
        "FeishuOpenClient": FeishuOpenClient,
        "fetch_existing_bitable_record_analysis": fetch_existing_bitable_record_analysis,
        "fetch_existing_bitable_record_index": fetch_existing_bitable_record_index,
        "inspect_task_upload_assignments": inspect_task_upload_assignments,
        "load_local_env": load_local_env,
        "run_keep_list_screening_pipeline": run_keep_list_screening_pipeline,
        "run_task_upload_to_keep_list_pipeline": run_task_upload_to_keep_list_pipeline,
        "upload_final_review_payload_to_bitable": upload_final_review_payload_to_bitable,
    }


def default_output_root() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "temp" / f"shared_mailbox_post_sync_{timestamp}"


def iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _safe_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "task"
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    normalized = "".join(cleaned).strip("_")
    return normalized or "task"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_runtime_progress(scope: str, message: str) -> None:
    print(f"[{iso_now()}] [{scope}] {message}", flush=True)


def _normalize_email(value: Any) -> str:
    return _clean_text(value).lower()


def _first_non_blank(*values: Any) -> str:
    for value in values:
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return ""


def _extract_emails_from_text(value: Any) -> list[str]:
    matches = []
    seen: set[str] = set()
    for match in _EMAIL_PATTERN.finditer(_clean_text(value)):
        normalized = _normalize_email(match.group(0))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        matches.append(normalized)
    return matches


def _load_address_entries(raw_value: Any) -> list[dict[str, str]]:
    try:
        items = json.loads(_clean_text(raw_value) or "[]")
    except json.JSONDecodeError:
        return []
    result: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        email = _normalize_email(item.get("address") or item.get("email"))
        name = _clean_text(item.get("name"))
        if not email and not name:
            continue
        result.append({"email": email, "name": name})
    return result


def _extract_address_emails(raw_value: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in _load_address_entries(raw_value):
        email = _normalize_email(item.get("email"))
        if not email or email in seen:
            continue
        seen.add(email)
        result.append(email)
    return result


def _extract_external_recipient_emails(*raw_values: Any, excluded_emails: Sequence[str] = ()) -> list[str]:
    result: list[str] = []
    seen = {_normalize_email(email) for email in excluded_emails if _normalize_email(email)}
    for raw_value in raw_values:
        for email in _extract_address_emails(raw_value):
            normalized = _normalize_email(email)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
    return result


def _path_summary(path: Path | None, *, source: str, kind: str) -> dict[str, Any]:
    if path is None:
        return {"kind": kind, "path": "", "exists": False, "source": source}
    expanded = path.expanduser()
    return {
        "kind": kind,
        "path": str(expanded.resolve()),
        "exists": expanded.exists(),
        "source": source,
    }


def _resolve_optional_path(raw_value: Any) -> Path | None:
    text = _clean_text(raw_value)
    if not text:
        return None
    return Path(text).expanduser()


def _build_failure_payload(
    *,
    stage: str,
    error_code: str,
    message: str,
    remediation: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "error_code": error_code,
        "message": message,
        "remediation": remediation,
        "details": details or {},
    }


def _resolve_cli_env_value(
    cli_value: object,
    env_values: dict[str, str],
    env_key: str,
    default: str = "",
) -> tuple[str, str]:
    candidate = str(cli_value or "").strip()
    if candidate:
        return candidate, "cli"
    env_candidate = str(env_values.get(env_key, "") or "").strip()
    if env_candidate:
        return env_candidate, "env_file"
    return str(default or "").strip(), "default"


def _normalize_field_name(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .replace("（", "(")
        .replace("）", ")")
        .replace(" ", "")
        .casefold()
    )


def _flatten_field_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [_flatten_field_value(item) for item in value]
        return "；".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("name", "text", "link", "value", "id"):
            candidate = _clean_text(value.get(key))
            if candidate:
                return candidate
        return ""
    return _clean_text(value)


def _get_field_value(fields: dict[str, Any], *candidates: str) -> Any:
    normalized_candidates = {_normalize_field_name(name) for name in candidates if str(name or "").strip()}
    for key, value in (fields or {}).items():
        if _normalize_field_name(str(key or "")) in normalized_candidates:
            return value
    return ""


def _build_record_key(*parts: Any) -> str:
    normalized_parts = [_clean_text(part).casefold() for part in parts]
    if any(not part for part in normalized_parts):
        return ""
    return "::".join(normalized_parts)


def _extract_creator_id(keep_row: dict[str, Any]) -> str:
    return _extract_handle(
        keep_row.get("@username")
        or keep_row.get("达人ID")
        or keep_row.get("URL")
        or keep_row.get("主页链接")
    )


def _extract_platform(keep_row: dict[str, Any]) -> str:
    return _normalize_platform(keep_row.get("Platform") or keep_row.get("平台"))


def _coerce_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _extract_ai_status(fields: dict[str, Any]) -> str:
    return _flatten_field_value(_get_field_value(fields, "ai是否通过", "ai 是否通过"))


def _empty_rescue_stats() -> dict[str, int]:
    return {key: 0 for key in _RESCUE_SUMMARY_KEYS}


def _empty_mail_only_bucket_summary() -> dict[str, int]:
    return {key: 0 for key in _MAIL_ONLY_BUCKET_SUMMARY_KEYS}


def _normalize_compact_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(value).casefold())


def _parse_string_list(value: Any) -> list[str]:
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


def _serialize_string_list(values: Sequence[str] | None) -> str:
    payload = [str(item).strip() for item in (values or []) if str(item).strip()]
    return json.dumps(payload, ensure_ascii=False) if payload else ""


def _parse_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).casefold()
    return text in {"1", "true", "yes", "y", "是"}


def _normalize_decision(value: Any) -> str:
    text = _clean_text(value).casefold()
    if text in {"keep", "manual", "reject"}:
        return text
    return ""


def _derive_original_decision(row: dict[str, Any]) -> str:
    explicit = _normalize_decision(row.get("original_decision"))
    if explicit:
        return explicit
    ai_status = _clean_text(row.get("ai是否通过")).casefold()
    if ai_status in {"是", "通过", "pass", "keep", "yes"}:
        return "keep"
    if ai_status in {"转人工", "manual", "review", "待补充"}:
        return "manual"
    if ai_status in {"否", "reject", "no"}:
        return "reject"
    explicit_final = _normalize_decision(row.get("final_decision"))
    if explicit_final:
        return explicit_final
    return "manual"


def _build_row_lookup_key(row: dict[str, Any]) -> str:
    platform = _normalize_platform(row.get("平台") or row.get("Platform"))
    creator_id = _extract_handle(
        _first_non_blank(
            row.get("达人ID"),
            row.get("@username"),
            row.get("final_creator_id"),
            row.get("final_id_final"),
            row.get("主页链接"),
            row.get("URL"),
        )
    )
    return _build_record_key(platform, creator_id)


def _build_source_keep_index(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _build_row_lookup_key(row)
        if key:
            result[key] = dict(row)
    return result


def _build_business_signal_markers(*values: Any) -> list[str]:
    haystack = "\n".join(_clean_text(value) for value in values if _clean_text(value))
    if not haystack:
        return []
    result: list[str] = []
    for label, pattern in _BUSINESS_SIGNAL_PATTERNS.items():
        if pattern.search(haystack):
            result.append(label)
    return result


def _matches_any_pattern(patterns: Sequence[re.Pattern[str]], *values: Any) -> bool:
    haystack = "\n".join(_clean_text(value) for value in values if _clean_text(value))
    return bool(haystack) and any(pattern.search(haystack) for pattern in patterns)


def _looks_like_creator_identity(candidate: str, creator_id: str) -> bool:
    normalized_candidate = _normalize_compact_text(candidate)
    normalized_creator = _normalize_compact_text(creator_id)
    if not normalized_candidate or not normalized_creator:
        return False
    if normalized_candidate == normalized_creator:
        return True
    if min(len(normalized_candidate), len(normalized_creator)) < 5:
        return False
    return normalized_candidate in normalized_creator or normalized_creator in normalized_candidate


def _row_has_creator_greeting(body_text: str, creator_id: str) -> bool:
    normalized_creator = _clean_text(creator_id)
    if not body_text or not normalized_creator:
        return False
    pattern = re.compile(
        rf"(?im)(?:^|[\n>])\s*(?:hi|hello|hallo)\s*@?\s*\*?{re.escape(normalized_creator)}\*?\b"
    )
    return bool(pattern.search(body_text))


def _row_domain_looks_personal_brand(email_value: str, creator_id: str) -> bool:
    email_text = _clean_text(email_value).casefold()
    if "@" not in email_text:
        return False
    domain = email_text.split("@", 1)[1].strip()
    if not domain or domain in _FREE_EMAIL_DOMAINS:
        return False
    domain_stem = domain.split(".", 1)[0]
    return _looks_like_creator_identity(domain_stem, creator_id)


def _derive_rescue_confidence(row: dict[str, Any], source_row: dict[str, Any] | None = None) -> str:
    source = dict(source_row or {})
    return _clean_text(
        _first_non_blank(
            row.get("confidence_after_rescue"),
            row.get("confidence_before_rescue"),
            row.get("resolution_confidence_final"),
            row.get("mail_resolution_confidence"),
            source.get("resolution_confidence_final"),
            source.get("mail_resolution_confidence"),
            source.get("confidence_after_rescue"),
            source.get("confidence_before_rescue"),
        )
    ).casefold()


def _merge_source_metadata_into_payload_row(row: dict[str, Any], source_row: dict[str, Any] | None) -> dict[str, Any]:
    updated = dict(row)
    source = dict(source_row or {})

    def copy_if_blank(target_key: str, *source_keys: str) -> None:
        if _clean_text(updated.get(target_key)):
            return
        for source_key in source_keys:
            candidate = source.get(source_key)
            if candidate is None:
                continue
            if isinstance(candidate, list):
                if candidate:
                    updated[target_key] = _serialize_string_list(candidate)
                    return
                continue
            cleaned = _clean_text(candidate)
            if cleaned:
                updated[target_key] = cleaned
                return

    copy_if_blank("candidate_sources", "candidate_sources")
    copy_if_blank("resolution_evidence", "resolution_evidence", "mail_evidence")
    copy_if_blank("business_signal_detected", "business_signal_detected")
    copy_if_blank("review_priority", "review_priority")
    copy_if_blank("rescue_rule_applied", "rescue_rule_applied")
    copy_if_blank("confidence_before_rescue", "confidence_before_rescue", "resolution_confidence_final", "mail_resolution_confidence")
    copy_if_blank("confidence_after_rescue", "confidence_after_rescue", "resolution_confidence_final", "mail_resolution_confidence")
    copy_if_blank("hard_reject_blocked_rescue", "hard_reject_blocked_rescue")
    copy_if_blank("original_decision", "original_decision")
    copy_if_blank("final_decision", "final_decision")
    copy_if_blank("original_reject_reason", "original_reject_reason")
    copy_if_blank("latest_external_from", "latest_external_from")
    copy_if_blank("latest_external_sent_at", "latest_external_sent_at")
    copy_if_blank("subject", "subject")
    copy_if_blank("latest_external_full_body", "latest_external_full_body")
    copy_if_blank("latest_external_clean_body", "latest_external_clean_body")
    copy_if_blank("resolution_stage_final", "resolution_stage_final", "mail_resolution_stage")
    copy_if_blank("resolution_confidence_final", "resolution_confidence_final", "mail_resolution_confidence")
    return updated


def _build_default_row_metadata(row: dict[str, Any], source_row: dict[str, Any] | None = None) -> dict[str, Any]:
    updated = _merge_source_metadata_into_payload_row(row, source_row)
    original_decision = _derive_original_decision(updated)
    final_decision = _normalize_decision(updated.get("final_decision")) or original_decision
    confidence = _derive_rescue_confidence(updated, source_row=source_row)
    candidate_sources = _parse_string_list(updated.get("candidate_sources"))
    business_markers = _build_business_signal_markers(
        updated.get("subject"),
        updated.get("latest_external_full_body"),
        updated.get("full body"),
        updated.get("ai筛号反馈理由"),
        updated.get("ai评价"),
        updated.get("当前网红报价"),
    )
    updated["candidate_sources"] = _serialize_string_list(candidate_sources)
    updated["original_decision"] = original_decision
    updated["final_decision"] = final_decision
    updated["original_reject_reason"] = _clean_text(updated.get("original_reject_reason")) or (
        _clean_text(updated.get("ai筛号反馈理由")) if original_decision == "reject" else ""
    )
    updated["business_signal_detected"] = "true" if (
        _parse_boolish(updated.get("business_signal_detected")) or bool(business_markers)
    ) else "false"
    updated["review_priority"] = _clean_text(updated.get("review_priority")) or ("normal" if final_decision == "manual" else "")
    updated["rescue_rule_applied"] = _clean_text(updated.get("rescue_rule_applied"))
    updated["confidence_before_rescue"] = _clean_text(updated.get("confidence_before_rescue")) or confidence
    updated["confidence_after_rescue"] = _clean_text(updated.get("confidence_after_rescue")) or confidence
    updated["hard_reject_blocked_rescue"] = "true" if _parse_boolish(updated.get("hard_reject_blocked_rescue")) else "false"
    return updated


def _row_has_active_reply(row: dict[str, Any]) -> bool:
    return bool(
        _clean_text(row.get("达人最后一次回复邮件时间"))
        or _clean_text(row.get("full body"))
        or _clean_text(row.get("达人回复的最后一封邮件内容"))
    )


def _build_supporting_evidence(row: dict[str, Any]) -> list[str]:
    creator_id = _extract_handle(_first_non_blank(row.get("达人ID"), row.get("@username"), row.get("final_creator_id")))
    from_email = _clean_text(row.get("latest_external_from")).casefold()
    body_text = _first_non_blank(row.get("latest_external_full_body"), row.get("full body"), row.get("达人回复的最后一封邮件内容"))
    candidate_sources = _parse_string_list(row.get("candidate_sources"))
    signals: list[str] = []
    if creator_id and "@" in from_email and _looks_like_creator_identity(from_email.split("@", 1)[0], creator_id):
        signals.append("email_localpart_like_handle")
    if creator_id and _row_has_creator_greeting(body_text, creator_id):
        signals.append("greeting_hits_single_creator_name")
    if creator_id and _row_domain_looks_personal_brand(from_email, creator_id):
        signals.append("domain_like_personal_brand")
    if any("quoted" in source for source in candidate_sources):
        signals.append("quoted_thread_single_candidate")
    return signals


def _evaluate_rescue_rule(row: dict[str, Any]) -> dict[str, Any]:
    original_decision = _derive_original_decision(row)
    if original_decision != "reject":
        return {"applied": False, "blocked": False}
    reason_text = "\n".join(
        _clean_text(value)
        for value in (
            row.get("ai筛号反馈理由"),
            row.get("ai评价"),
            row.get("resolution_evidence"),
        )
        if _clean_text(value)
    )
    business_signal = _parse_boolish(row.get("business_signal_detected"))
    active_reply = _row_has_active_reply(row)
    supporting_evidence = _build_supporting_evidence(row)
    stage = _clean_text(row.get("resolution_stage_final")).casefold()
    confidence = _clean_text(
        _first_non_blank(
            row.get("resolution_confidence_final"),
            row.get("confidence_before_rescue"),
            row.get("mail_resolution_confidence"),
        )
    ).casefold()

    def blocked(rule_name: str, reason: str) -> dict[str, Any]:
        return {
            "applied": False,
            "blocked": True,
            "rule_name": rule_name,
            "reason": reason,
            "supporting_evidence": supporting_evidence,
        }

    if business_signal and _matches_any_pattern(_SCRAPE_FAILED_PATTERNS, reason_text):
        if _matches_any_pattern(_HARD_REJECT_PATTERNS, reason_text):
            return blocked("r1_scrape_failed_with_business_signal", "hard_reject")
        return {
            "applied": True,
            "blocked": False,
            "rule_name": "r1_scrape_failed_with_business_signal",
            "final_decision": "manual",
            "review_priority": "high",
            "reason": "抓取失败，但邮件线程里有明确商务信号，转高优先人工复核。",
            "supporting_evidence": supporting_evidence,
        }
    if active_reply and _matches_any_pattern(_RECENTLY_INACTIVE_PATTERNS, reason_text):
        if _matches_any_pattern(_HARD_REJECT_PATTERNS, reason_text):
            return blocked("r2_recently_inactive_but_active_reply", "hard_reject")
        return {
            "applied": True,
            "blocked": False,
            "rule_name": "r2_recently_inactive_but_active_reply",
            "final_decision": "manual",
            "review_priority": "high",
            "reason": "近 30 天内容活跃度不足，但邮件线程里有明确活跃回复，转高优先人工复核。",
            "supporting_evidence": supporting_evidence,
        }
    if active_reply and _matches_any_pattern(_INSUFFICIENT_PROFILE_PATTERNS, reason_text):
        if _matches_any_pattern(_HARD_REJECT_PATTERNS, reason_text):
            return blocked("r3_insufficient_profile_data_but_active_reply", "hard_reject")
        return {
            "applied": True,
            "blocked": False,
            "rule_name": "r3_insufficient_profile_data_but_active_reply",
            "final_decision": "manual",
            "review_priority": "high",
            "reason": "资料侧信息不足，但邮件线程里有明确活跃回复，转高优先人工复核。",
            "supporting_evidence": supporting_evidence,
        }
    if stage == "llm" and confidence == "medium" and len(supporting_evidence) >= 2:
        if _matches_any_pattern(_HARD_REJECT_PATTERNS, reason_text):
            return blocked("r4_medium_with_supporting_evidence", "hard_reject")
        return {
            "applied": True,
            "blocked": False,
            "rule_name": "r4_medium_with_supporting_evidence",
            "final_decision": "manual",
            "review_priority": "high",
            "reason": "LLM 仅中等置信度，但弱证据一致，转高优先人工复核。",
            "supporting_evidence": supporting_evidence,
        }
    return {"applied": False, "blocked": False, "supporting_evidence": supporting_evidence}


def _apply_conservative_rescue_layer(
    *,
    display_rows: list[dict[str, Any]],
    payload_rows: list[dict[str, Any]],
    source_keep_rows: Sequence[dict[str, Any]],
    task_name: str,
    task_scope: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    updated_display_rows = [dict(row) for row in display_rows]
    display_index = {_build_row_lookup_key(row): idx for idx, row in enumerate(updated_display_rows)}
    keep_index = _build_source_keep_index(source_keep_rows)
    stats = _empty_rescue_stats()
    updated_payload_rows: list[dict[str, Any]] = []
    for row in payload_rows:
        updated_row = _build_default_row_metadata(row, source_row=keep_index.get(_build_row_lookup_key(row)))
        original_decision = _derive_original_decision(updated_row)
        rescue_result = _evaluate_rescue_rule(updated_row)
        key = _build_row_lookup_key(updated_row)
        if rescue_result.get("blocked"):
            updated_row["hard_reject_blocked_rescue"] = "true"
            stats["hard_reject_blocked_rescue_count"] += 1
            event = {
                "task_name": task_name,
                "thread_id": _clean_text(updated_row.get("thread_key") or updated_row.get("mail_thread_key")),
                "creator_id": _clean_text(updated_row.get("达人ID")),
                "original_stage": _clean_text(updated_row.get("resolution_stage_final")),
                "original_confidence": _clean_text(updated_row.get("confidence_before_rescue") or updated_row.get("resolution_confidence_final")),
                "new_stage": _clean_text(updated_row.get("resolution_stage_final")),
                "new_confidence": _clean_text(updated_row.get("confidence_after_rescue") or updated_row.get("resolution_confidence_final")),
                "rule_name": rescue_result.get("rule_name"),
                "reason": rescue_result.get("reason"),
                "evidence": list(rescue_result.get("supporting_evidence") or []),
                "candidate_sources": _parse_string_list(updated_row.get("candidate_sources")),
            }
            _emit_runtime_progress(task_scope, f"rescue_blocked={json.dumps(event, ensure_ascii=False)}")
        elif rescue_result.get("applied"):
            updated_row["final_decision"] = _clean_text(rescue_result.get("final_decision")) or "manual"
            updated_row["review_priority"] = _clean_text(rescue_result.get("review_priority")) or "high"
            updated_row["rescue_rule_applied"] = _clean_text(rescue_result.get("rule_name"))
            updated_row["hard_reject_blocked_rescue"] = "false"
            if not _clean_text(updated_row.get("confidence_before_rescue")):
                updated_row["confidence_before_rescue"] = _clean_text(updated_row.get("resolution_confidence_final"))
            if not _clean_text(updated_row.get("confidence_after_rescue")):
                updated_row["confidence_after_rescue"] = _clean_text(updated_row.get("confidence_before_rescue"))
            updated_row["ai是否通过"] = "转人工" if updated_row["final_decision"] == "manual" else "是"
            updated_row["ai筛号反馈理由"] = _clean_text(rescue_result.get("reason"))
            updated_row["ai评价"] = _clean_text(rescue_result.get("reason"))
            stats[f"{updated_row['rescue_rule_applied'][:2]}_rescue_count"] += 1
            if updated_row["final_decision"] == "manual":
                stats["rescued_to_manual_count"] += 1
            elif updated_row["final_decision"] == "keep":
                stats["rescued_to_keep_count"] += 1
            event = {
                "task_name": task_name,
                "thread_id": _clean_text(updated_row.get("thread_key") or updated_row.get("mail_thread_key")),
                "creator_id": _clean_text(updated_row.get("达人ID")),
                "original_stage": _clean_text(updated_row.get("resolution_stage_final")),
                "original_confidence": _clean_text(updated_row.get("confidence_before_rescue") or updated_row.get("resolution_confidence_final")),
                "new_stage": _clean_text(updated_row.get("resolution_stage_final")),
                "new_confidence": _clean_text(updated_row.get("confidence_after_rescue") or updated_row.get("resolution_confidence_final")),
                "rule_name": updated_row.get("rescue_rule_applied"),
                "reason": updated_row.get("ai筛号反馈理由"),
                "evidence": list(rescue_result.get("supporting_evidence") or []),
                "candidate_sources": _parse_string_list(updated_row.get("candidate_sources")),
            }
            _emit_runtime_progress(task_scope, f"rescue_applied={json.dumps(event, ensure_ascii=False)}")
        elif original_decision == "manual" and not _clean_text(updated_row.get("review_priority")):
            updated_row["review_priority"] = "normal"
        updated_payload_rows.append(updated_row)
        if key and key in display_index:
            display_row = updated_display_rows[display_index[key]]
            display_row["ai是否通过"] = _clean_text(updated_row.get("ai是否通过")) or _clean_text(display_row.get("ai是否通过"))
            display_row["ai筛号反馈理由"] = _clean_text(updated_row.get("ai筛号反馈理由")) or _clean_text(display_row.get("ai筛号反馈理由"))
            display_row["ai评价"] = _clean_text(updated_row.get("ai评价")) or _clean_text(display_row.get("ai评价"))
    return updated_display_rows, updated_payload_rows, stats


def _build_owner_context_from_upstream(upstream_summary: dict[str, Any], fallback_item: dict[str, Any]) -> dict[str, str]:
    task_owner = (((upstream_summary.get("downstream_handoff") or {}).get("task_owner")) or {})
    if bool(fallback_item.get("rowLevelOwnerRouting")):
        return {
            "task_name": _clean_text(fallback_item.get("taskName")) or _clean_text(task_owner.get("task_name")),
            "linked_bitable_url": _clean_text(fallback_item.get("linkedBitableUrl"))
            or _clean_text(task_owner.get("linked_bitable_url")),
            "responsible_name": "",
            "employee_name": "",
            "employee_english_name": "",
            "employee_id": "",
            "employee_record_id": "",
            "employee_email": "",
            "owner_name": "",
        }
    responsible_name = (
        _clean_text(task_owner.get("responsible_name"))
        or _clean_text(task_owner.get("employee_name"))
        or _clean_text(fallback_item.get("responsibleName"))
    )
    return {
        "task_name": _clean_text(task_owner.get("task_name")) or _clean_text(fallback_item.get("taskName")),
        "linked_bitable_url": _clean_text(task_owner.get("linked_bitable_url"))
        or _clean_text(fallback_item.get("linkedBitableUrl")),
        "responsible_name": responsible_name,
        "employee_name": _clean_text(task_owner.get("employee_name")) or _clean_text(fallback_item.get("employeeName")),
        "employee_english_name": _clean_text(task_owner.get("employee_english_name"))
        or _clean_text(fallback_item.get("employeeEnglishName")),
        "employee_id": _clean_text(task_owner.get("employee_id")) or _clean_text(fallback_item.get("employeeId")),
        "employee_record_id": _clean_text(task_owner.get("employee_record_id"))
        or _clean_text(fallback_item.get("employeeRecordId")),
        "employee_email": _clean_text(task_owner.get("employee_email")) or _clean_text(fallback_item.get("employeeEmail")),
        "owner_name": _clean_text(task_owner.get("owner_name")) or _clean_text(fallback_item.get("ownerName")),
    }


def _build_mail_attachment_paths(
    keep_row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> list[str]:
    base_dirs = [
        shared_mail_raw_dir,
        shared_mail_data_dir,
        shared_mail_db_path.parent,
        keep_workbook.parent,
    ]
    return _resolve_existing_local_paths(
        keep_row.get("brand_message_raw_path"),
        keep_row.get("last_mail_raw_path"),
        base_dirs=base_dirs,
    )


def _resolve_keep_row_mail_fields(
    keep_row: dict[str, Any],
    *,
    existing_row: dict[str, Any] | None = None,
) -> dict[str, str]:
    existing = dict(existing_row or {})
    allow_brand_message_fallback = not _clean_text(keep_row.get("evidence_thread_key"))
    quote_text = _build_quote_text(keep_row) or _clean_text(existing.get("当前网红报价"))
    last_mail_time = _format_date(keep_row.get("last_mail_time")) or (
        _format_date(keep_row.get("brand_message_sent_at")) if allow_brand_message_fallback else ""
    ) or _clean_text(existing.get("达人最后一次回复邮件时间"))
    last_mail_content = _clean_text(keep_row.get("last_mail_snippet")) or (
        _clean_text(keep_row.get("brand_message_snippet")) if allow_brand_message_fallback else ""
    ) or _clean_text(existing.get("full body")) or _clean_text(existing.get("达人回复的最后一封邮件内容"))
    return {
        "quote_text": quote_text,
        "last_mail_time": last_mail_time,
        "last_mail_content": last_mail_content,
    }


def _build_mail_only_rows(
    *,
    keep_row: dict[str, Any],
    existing_fields: dict[str, Any],
    owner_context: dict[str, str],
    linked_bitable_url: str,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
    update_mode: str = MAIL_ONLY_UPDATE_MODE,
    preserve_existing_ai_fields: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    creator_id = _extract_creator_id(keep_row) or _flatten_field_value(_get_field_value(existing_fields, "达人ID"))
    platform = _extract_platform(keep_row) or _flatten_field_value(_get_field_value(existing_fields, "平台"))
    profile_url = _clean_text(keep_row.get("URL")) or _flatten_field_value(_get_field_value(existing_fields, "主页链接"))
    mail_fields = _resolve_keep_row_mail_fields(keep_row, existing_row=existing_fields)
    quote_text = mail_fields["quote_text"]
    last_mail_time = mail_fields["last_mail_time"]
    last_mail_content = mail_fields["last_mail_content"]
    owner_display_name = (
        _clean_text(owner_context.get("responsible_name"))
        or _clean_text(owner_context.get("employee_name"))
        or _flatten_field_value(_get_field_value(existing_fields, "达人对接人"))
    )
    ai_status = _extract_ai_status(existing_fields) if preserve_existing_ai_fields else "待补充"
    ai_reason = (
        _flatten_field_value(_get_field_value(existing_fields, "ai筛号反馈理由"))
        if preserve_existing_ai_fields
        else ""
    )
    ai_label = (
        _flatten_field_value(_get_field_value(existing_fields, "标签(ai)", "标签（ai）"))
        if preserve_existing_ai_fields
        else ""
    )
    ai_evaluation = (
        _flatten_field_value(_get_field_value(existing_fields, "ai评价", "ai 评价"))
        if preserve_existing_ai_fields
        else ""
    )
    display_row = {
        "达人ID": creator_id,
        "平台": platform,
        "主页链接": profile_url,
        "# Followers(K)#": _flatten_field_value(_get_field_value(existing_fields, "# Followers(K)#", "Followers(K)")),
        "Average Views (K)": _flatten_field_value(_get_field_value(existing_fields, "Average Views (K)")),
        "互动率": _flatten_field_value(_get_field_value(existing_fields, "互动率")),
        "当前网红报价": quote_text,
        "达人最后一次回复邮件时间": last_mail_time,
        "full body": last_mail_content,
        "达人回复的最后一封邮件内容": last_mail_content,
        "达人对接人": owner_display_name,
        "ai是否通过": ai_status,
        "ai筛号反馈理由": ai_reason,
        "标签(ai)": ai_label,
        "ai评价": ai_evaluation,
    }
    attachment_paths = _build_mail_attachment_paths(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    payload_row = dict(display_row)
    payload_row.update(
        {
            "达人对接人_employee_id": _normalize_employee_id(owner_context.get("employee_id")).split(",")[0].strip(),
            "达人对接人_employee_record_id": _clean_text(owner_context.get("employee_record_id")),
            "达人对接人_employee_email": _clean_text(owner_context.get("employee_email")),
            "达人对接人_owner_name": _clean_text(owner_context.get("owner_name")),
            "linked_bitable_url": linked_bitable_url,
            "任务名": _clean_text(owner_context.get("task_name")),
            "__last_mail_raw_path": _clean_text(keep_row.get("brand_message_raw_path") or keep_row.get("last_mail_raw_path")),
            "__feishu_attachment_local_paths": attachment_paths,
            "__feishu_update_mode": update_mode,
            "last_mail_message_id": _clean_text(keep_row.get("last_mail_message_id")),
            "last_mail_sent_at": _first_non_blank(keep_row.get("last_mail_time"), keep_row.get("brand_message_sent_at")),
            "mail_update_revision": _coerce_non_negative_int(keep_row.get("mail_update_revision")),
        }
    )
    return display_row, payload_row


def _extract_matched_mail_count(upstream_summary: dict[str, Any], keep_frame: pd.DataFrame) -> int:
    steps = upstream_summary.get("steps") or {}
    brand_match_stats = ((steps.get("brand_match") or {}).get("stats") or {})
    if int(brand_match_stats.get("message_hit_count") or 0) > 0:
        return int(brand_match_stats.get("message_hit_count") or 0)
    enrichment_stats = ((steps.get("enrichment") or {}).get("stats") or {})
    if int(enrichment_stats.get("matched_rows") or 0) > 0:
        return int(enrichment_stats.get("matched_rows") or 0)
    return int(len(keep_frame.index))


def _build_prepared_candidate_mail_identity_key(candidate: dict[str, Any]) -> tuple[str, str]:
    keep_row = dict(candidate.get("keep_row") or {})
    creator_id = _clean_text(candidate.get("creator_id")) or _extract_creator_id(keep_row)
    platform = _clean_text(candidate.get("platform")) or _extract_platform(keep_row)
    record_key = _build_record_key(creator_id, platform)
    last_mail_message_id = _clean_text(candidate.get("last_mail_message_id")) or _clean_text(keep_row.get("last_mail_message_id"))
    thread_key = _clean_text(candidate.get("thread_key")) or _clean_text(keep_row.get("evidence_thread_key"))
    mail_update_revision = _coerce_non_negative_int(candidate.get("mail_update_revision") or keep_row.get("mail_update_revision"))

    source_key = ""
    source_label = ""
    if last_mail_message_id:
        source_key = f"message:{last_mail_message_id}"
        source_label = f"last_mail_message_id={last_mail_message_id}"
    elif thread_key and mail_update_revision > 0:
        source_key = f"thread_revision:{thread_key}:{mail_update_revision}"
        source_label = f"thread_key={thread_key} revision={mail_update_revision}"
    elif thread_key:
        source_key = f"thread:{thread_key}"
        source_label = f"thread_key={thread_key}"

    if source_key and record_key:
        return f"{source_key}::{record_key}", f"{source_label} + 达人ID+平台={creator_id}/{platform}"
    if source_key:
        return source_key, source_label
    if record_key:
        return record_key, f"达人ID+平台={creator_id}/{platform}"
    return "", ""


def _deduplicate_prepared_candidates_by_mail_identity(
    candidates: Sequence[dict[str, Any]],
    *,
    seen_identity_keys: set[str],
    deduplicate_within_batch: bool = True,
) -> dict[str, Any]:
    deduplicated_candidates: list[dict[str, Any]] = []
    duplicate_rows: list[dict[str, Any]] = []
    stats = {
        "candidate_count": 0,
        "deduplicated_count": 0,
    }
    starting_seen_identity_keys = set(seen_identity_keys)
    batch_seen_identity_keys: set[str] = set()
    newly_seen_identity_keys: set[str] = set()

    for raw_candidate in candidates:
        candidate = dict(raw_candidate or {})
        stats["candidate_count"] += 1
        identity_key, identity_label = _build_prepared_candidate_mail_identity_key(candidate)
        already_seen = identity_key in starting_seen_identity_keys
        seen_in_batch = identity_key in batch_seen_identity_keys if deduplicate_within_batch else False
        if identity_key and (already_seen or seen_in_batch):
            stats["deduplicated_count"] += 1
            duplicate_rows.append(
                {
                    "candidate": candidate,
                    "identity_key": identity_key,
                    "identity_label": identity_label,
                }
            )
            continue
        if identity_key:
            batch_seen_identity_keys.add(identity_key)
            newly_seen_identity_keys.add(identity_key)
        deduplicated_candidates.append(candidate)

    seen_identity_keys.update(newly_seen_identity_keys)
    return {
        "candidates": deduplicated_candidates,
        "duplicate_rows": duplicate_rows,
        "stats": stats,
    }


def _build_mail_only_candidate_log_entry(
    *,
    task_name: str,
    keep_row: dict[str, Any],
    owner_context: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
    route_decision: str,
    reason: str = "",
    existing_record: dict[str, Any] | None = None,
    reply_resolution: dict[str, Any] | None = None,
    thread_assignment_resolution: dict[str, Any] | None = None,
    next_update_mode: str = "",
) -> dict[str, Any]:
    resolved_candidate = dict(candidate or {})
    resolved_keep_row = dict(keep_row or {})
    resolved_owner_context = dict(owner_context or resolved_candidate.get("owner_context") or {})
    resolved_existing_record = dict(existing_record or resolved_candidate.get("existing_record") or {})
    resolved_reply = dict(reply_resolution or resolved_candidate.get("reply_resolution") or {})
    resolved_thread_assignment = dict(
        thread_assignment_resolution or resolved_candidate.get("thread_assignment_resolution") or {}
    )
    creator_id = _clean_text(resolved_candidate.get("creator_id")) or _extract_creator_id(resolved_keep_row)
    platform = _clean_text(resolved_candidate.get("platform")) or _extract_platform(resolved_keep_row)
    thread_key = _clean_text(resolved_candidate.get("thread_key")) or _clean_text(resolved_keep_row.get("evidence_thread_key"))
    last_mail_message_id = _clean_text(resolved_candidate.get("last_mail_message_id")) or _clean_text(
        resolved_keep_row.get("last_mail_message_id")
    )
    mail_update_revision = _coerce_non_negative_int(
        resolved_candidate.get("mail_update_revision") or resolved_keep_row.get("mail_update_revision")
    )
    return {
        "task_name": _clean_text(task_name),
        "creator_id": creator_id,
        "platform": platform,
        "record_key": _build_record_key(creator_id, platform),
        "owner_scope": _clean_text(resolved_candidate.get("owner_scope")),
        "owner_employee_id": _clean_text(resolved_owner_context.get("employee_id")),
        "thread_key": thread_key,
        "last_mail_message_id": last_mail_message_id,
        "mail_update_revision": mail_update_revision,
        "reply_status": _clean_text(resolved_reply.get("status")),
        "creator_replied": resolved_reply.get("creator_replied"),
        "thread_assignment_status": _clean_text(resolved_thread_assignment.get("status")),
        "existing_record_id": _clean_text(resolved_existing_record.get("record_id")),
        "existing_ai_status": _extract_ai_status(dict(resolved_existing_record.get("fields") or {})),
        "route_decision": _clean_text(route_decision),
        "next_update_mode": _clean_text(next_update_mode),
        "reason": _clean_text(reason),
        "source_raw_path": _clean_text(
            resolved_keep_row.get("last_mail_raw_path")
            or resolved_keep_row.get("brand_message_raw_path")
            or resolved_keep_row.get("__last_mail_raw_path")
        ),
    }


def _write_combined_workbook(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=FINAL_UPLOAD_COLUMNS)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="总表")


def _write_unresolved_threads_archive(
    output_dir: Path,
    rows: list[dict[str, Any]],
) -> tuple[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "unresolved_threads.json"
    xlsx_path = output_dir / "unresolved_threads.xlsx"
    json_path.write_text(
        json.dumps(
            {
                "unresolved_thread_count": len(rows),
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    frame = pd.DataFrame(rows)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="unresolved_threads")
    return str(json_path), str(xlsx_path)


def _write_rows_archive(
    output_dir: Path,
    *,
    stem: str,
    rows: list[dict[str, Any]],
    sheet_name: str,
) -> tuple[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{stem}.json"
    xlsx_path = output_dir / f"{stem}.xlsx"
    json_path.write_text(
        json.dumps(
            {
                "row_count": len(rows),
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    frame = pd.DataFrame(rows)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
    return str(json_path), str(xlsx_path)


def _write_skipped_archive(archive_dir: Path, *, task_owner: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[str, str]:
    archive_dir.mkdir(parents=True, exist_ok=True)
    json_path = archive_dir / "skipped_from_feishu_upload.json"
    xlsx_path = archive_dir / "skipped_from_feishu_upload.xlsx"
    json_path.write_text(
        json.dumps(
            {
                "task_owner": task_owner,
                "skipped_row_count": len(rows),
                "skipped_rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    flattened: list[dict[str, Any]] = []
    for item in rows:
        row = dict(item.get("row") or {})
        row["本地归档原因"] = "；".join(str(reason).strip() for reason in (item.get("skip_reasons") or []) if str(reason).strip())
        flattened.append(row)
    frame = pd.DataFrame(flattened, columns=("本地归档原因", *FINAL_UPLOAD_COLUMNS))
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="未上传归档")
    return str(json_path), str(xlsx_path)


def _combine_payloads(
    *,
    workbook_path: Path,
    owner_context: dict[str, str],
    display_rows: list[dict[str, Any]],
    payload_rows: list[dict[str, Any]],
    skipped_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    archive_dir = workbook_path.parent / "feishu_upload_local_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    payload_json_path = workbook_path.parent / "all_platforms_final_review_payload.json"
    shared_attachment_paths = [str(workbook_path)]
    payload = {
        "task_owner": {
            "responsible_name": _clean_text(owner_context.get("responsible_name")),
            "employee_name": _clean_text(owner_context.get("employee_name")),
            "employee_id": _clean_text(owner_context.get("employee_id")).split(",")[0].strip(),
            "employee_record_id": _clean_text(owner_context.get("employee_record_id")),
            "employee_email": _clean_text(owner_context.get("employee_email")),
            "owner_name": _clean_text(owner_context.get("owner_name")),
            "linked_bitable_url": _clean_text(owner_context.get("linked_bitable_url")),
            "task_name": _clean_text(owner_context.get("task_name")),
        },
        "columns": list(FINAL_UPLOAD_COLUMNS),
        "source_row_count": len(display_rows),
        "row_count": len(payload_rows),
        "skipped_row_count": len(skipped_rows),
        "__feishu_shared_attachment_local_paths": shared_attachment_paths,
        "rows": payload_rows,
        "skipped_rows": skipped_rows,
    }
    payload_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    skipped_json_path, skipped_xlsx_path = _write_skipped_archive(
        archive_dir,
        task_owner=payload["task_owner"],
        rows=skipped_rows,
    )
    return {
        "payload": payload,
        "payload_json_path": str(payload_json_path),
        "archive_dir": str(archive_dir),
        "skipped_archive_json": skipped_json_path,
        "skipped_archive_xlsx": skipped_xlsx_path,
        "shared_attachment_paths": shared_attachment_paths,
    }


def _filter_keep_frame_for_full_screening(
    keep_frame: pd.DataFrame,
    full_screening_keys: set[str],
    *,
    owner_scope_value: str,
    owner_scope_enabled: bool,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in keep_frame.to_dict(orient="records"):
        key = (
            _build_record_key(owner_scope_value, _extract_creator_id(record), _extract_platform(record))
            if owner_scope_enabled
            else _build_record_key(_extract_creator_id(record), _extract_platform(record))
        )
        if key in full_screening_keys:
            rows.append(dict(record))
    return pd.DataFrame(rows, columns=list(keep_frame.columns))


def _write_keep_subset(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="keep")


def _collect_task_filters(values: Sequence[str] | None) -> set[str]:
    expanded: set[str] = set()
    for value in values or []:
        normalized = _clean_text(value).casefold()
        if not normalized:
            continue
        expanded.add(normalized)
        alias_values = _TASK_GROUP_ALIASES.get(normalized)
        if alias_values:
            expanded.update(alias_values)
    return expanded


def _derive_task_group_key(value: str) -> str:
    normalized = _clean_text(value).casefold()
    if not normalized:
        return ""
    stripped = _TASK_GROUP_SUFFIX_PATTERN.sub("", normalized).rstrip("-_ ")
    compact = re.sub(r"[\s\-_]+", "", stripped)
    if compact:
        return compact
    return re.sub(r"[\s\-_]+", "", normalized)


def _derive_default_brand_keyword(task_name: str) -> str:
    text = _clean_text(task_name)
    if not text:
        return ""
    stripped = _TASK_GROUP_SUFFIX_PATTERN.sub("", text).rstrip("-_ ")
    return stripped or text


def _resolve_requested_task_names(
    inspection_items: Sequence[dict[str, Any]],
    requested_filters: set[str],
) -> set[str]:
    if not requested_filters:
        return {
            _clean_text(item.get("taskName")).casefold()
            for item in inspection_items
            if _clean_text(item.get("taskName"))
        }

    exact_names: set[str] = set()
    grouped_names: dict[str, set[str]] = {}
    for item in inspection_items:
        normalized_name = _clean_text(item.get("taskName")).casefold()
        if not normalized_name:
            continue
        exact_names.add(normalized_name)
        group_key = _derive_task_group_key(normalized_name)
        if group_key:
            grouped_names.setdefault(group_key, set()).add(normalized_name)

    resolved: set[str] = set()
    for task_filter in requested_filters:
        if task_filter in exact_names:
            resolved.add(task_filter)
            continue
        alias_values = _TASK_GROUP_ALIASES.get(task_filter)
        if alias_values:
            alias_matches = {name for name in exact_names if name in alias_values}
            if alias_matches:
                resolved.update(alias_matches)
                continue
        group_key = _derive_task_group_key(task_filter)
        if group_key in grouped_names:
            resolved.update(grouped_names[group_key])
    return resolved


def _resolve_group_brand_keyword(
    *,
    task_name: str,
    explicit_brand_keyword: str,
) -> str:
    if _clean_text(explicit_brand_keyword):
        return _clean_text(explicit_brand_keyword)
    group_key = _derive_task_group_key(task_name)
    if group_key in _TASK_GROUP_DEFAULT_BRAND_KEYWORDS:
        return _TASK_GROUP_DEFAULT_BRAND_KEYWORDS[group_key]
    return _derive_default_brand_keyword(task_name)


def _normalize_employee_id(value: Any) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"[,\n|；;]+", _clean_text(value)):
        candidate = raw.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        parts.append(candidate)
    return ",".join(parts)


def _all_items_share_non_blank_value(items: Sequence[dict[str, Any]], field_name: str) -> bool:
    if not items:
        return False
    values = [_clean_text(item.get(field_name)) for item in items]
    return bool(values) and all(values) and len(set(values)) == 1


def _resolve_group_display_name(group_key: str, members: Sequence[dict[str, Any]]) -> str:
    if group_key in _TASK_GROUP_DEFAULT_BRAND_KEYWORDS:
        return _TASK_GROUP_DEFAULT_BRAND_KEYWORDS[group_key]
    first_name = _clean_text(next((item.get("taskName") for item in members if _clean_text(item.get("taskName"))), ""))
    return _derive_default_brand_keyword(first_name) or first_name


def _build_group_member_employee_match(member: dict[str, Any]) -> dict[str, str]:
    return {
        "employeeRecordId": _clean_text(member.get("employeeRecordId")),
        "employeeId": _clean_text(member.get("employeeId")),
        "employeeName": _clean_text(member.get("employeeName")),
        "employeeEnglishName": _clean_text(member.get("employeeEnglishName")),
        "employeeEmail": _clean_text(member.get("employeeEmail")),
        "imapCode": _clean_text(member.get("imapCode")),
        "matchedBy": "group_member",
        "matchedValue": _clean_text(member.get("taskName")),
    }


def _dedupe_employee_matches(entries: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized = {
            "employeeRecordId": _clean_text(entry.get("employeeRecordId")),
            "employeeId": _clean_text(entry.get("employeeId")),
            "employeeName": _clean_text(entry.get("employeeName")),
            "employeeEnglishName": _clean_text(entry.get("employeeEnglishName")),
            "employeeEmail": _clean_text(entry.get("employeeEmail")),
            "imapCode": _clean_text(entry.get("imapCode")),
            "matchedBy": _clean_text(entry.get("matchedBy")),
            "matchedValue": _clean_text(entry.get("matchedValue")),
        }
        key = (
            normalized["employeeRecordId"]
            or normalized["employeeId"]
            or normalized["employeeEmail"].casefold()
            or normalized["employeeName"].casefold()
        )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _should_collapse_task_group(
    *,
    group_key: str,
    members: Sequence[dict[str, Any]],
    requested_filters: set[str],
) -> bool:
    if group_key not in _TASK_GROUP_ALIASES or len(members) < 2:
        return False
    if not (
        _all_items_share_non_blank_value(members, "linkedBitableUrl")
        and _all_items_share_non_blank_value(members, "sendingListFileToken")
        and _all_items_share_non_blank_value(members, "templateFileToken")
    ):
        return False
    if not requested_filters:
        return True
    member_names = {_clean_text(item.get("taskName")).casefold() for item in members if _clean_text(item.get("taskName"))}
    if group_key in requested_filters:
        return True
    return not bool(requested_filters & member_names)


def _collapse_grouped_inspection_items(
    inspection_items: Sequence[dict[str, Any]],
    requested_filters: set[str],
) -> list[dict[str, Any]]:
    collapsed: list[dict[str, Any]] = []
    consumed_task_names: set[str] = set()
    normalized_items = [dict(item) for item in inspection_items]
    for item in normalized_items:
        task_name = _clean_text(item.get("taskName"))
        normalized_task_name = task_name.casefold()
        if not task_name or normalized_task_name in consumed_task_names:
            continue
        group_key = _derive_task_group_key(task_name)
        alias_values = _TASK_GROUP_ALIASES.get(group_key)
        if not alias_values:
            collapsed.append(item)
            consumed_task_names.add(normalized_task_name)
            continue
        members = [
            dict(candidate)
            for candidate in normalized_items
            if _clean_text(candidate.get("taskName")).casefold() in alias_values
        ]
        if not _should_collapse_task_group(group_key=group_key, members=members, requested_filters=requested_filters):
            collapsed.append(item)
            consumed_task_names.add(normalized_task_name)
            continue
        representative = dict(members[0])
        grouped_employee_matches = _dedupe_employee_matches(
            [
                *(dict(entry) for member in members for entry in (member.get("employeeMatches") or []) if isinstance(entry, dict)),
                *(_build_group_member_employee_match(member) for member in members),
            ]
        )
        member_task_names = [_clean_text(member.get("taskName")) for member in members if _clean_text(member.get("taskName"))]
        grouped_item = dict(representative)
        grouped_item.update(
            {
                "taskName": _resolve_group_display_name(group_key, members),
                "groupKey": group_key,
                "groupedTaskNames": member_task_names,
                "groupedRecordIds": [_clean_text(member.get("recordId")) for member in members if _clean_text(member.get("recordId"))],
                "representativeTaskName": _clean_text(representative.get("taskName")),
                "rowLevelOwnerRouting": True,
                "employeeMatched": bool(grouped_employee_matches),
                "employeeMatches": grouped_employee_matches,
                "ownerMatchCount": len(grouped_employee_matches),
                "ownerMatchAmbiguous": len(grouped_employee_matches) > 1,
                "employeeId": "",
                "employeeRecordId": "",
                "employeeName": "",
                "employeeEnglishName": "",
                "employeeEmail": "",
                "responsibleName": "",
                "ownerName": "",
                "ownerEmail": "",
                "ownerEmailCandidates": [],
                "preferredOwnerEmail": "",
            }
        )
        collapsed.append(grouped_item)
        consumed_task_names.update(
            _clean_text(member.get("taskName")).casefold()
            for member in members
            if _clean_text(member.get("taskName"))
        )
    return collapsed


def _build_empty_owner_context(task_owner_context: dict[str, str]) -> dict[str, str]:
    return {
        "task_name": _clean_text(task_owner_context.get("task_name")),
        "linked_bitable_url": _clean_text(task_owner_context.get("linked_bitable_url")),
        "responsible_name": "",
        "employee_name": "",
        "employee_english_name": "",
        "employee_id": "",
        "employee_record_id": "",
        "employee_email": "",
        "owner_name": "",
    }


def _build_owner_context_from_candidate(candidate: dict[str, Any], task_owner_context: dict[str, str]) -> dict[str, str]:
    employee_name = _clean_text(candidate.get("employeeName"))
    employee_english_name = _clean_text(candidate.get("employeeEnglishName"))
    employee_email = _clean_text(candidate.get("employeeEmail"))
    display_name = employee_name or employee_english_name or employee_email
    return {
        "task_name": _clean_text(task_owner_context.get("task_name")),
        "linked_bitable_url": _clean_text(task_owner_context.get("linked_bitable_url")),
        "responsible_name": display_name,
        "employee_name": employee_name or display_name,
        "employee_english_name": employee_english_name,
        "employee_id": _normalize_employee_id(candidate.get("employeeId")),
        "employee_record_id": _clean_text(candidate.get("employeeRecordId")),
        "employee_email": employee_email,
        "owner_name": employee_email or display_name,
    }


def _build_owner_candidate_aliases(candidate: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()
    for raw in (
        candidate.get("employeeEnglishName"),
        candidate.get("employeeEmail"),
        _clean_text(candidate.get("employeeEmail")).split("@", 1)[0].strip(),
        candidate.get("employeeName"),
    ):
        alias = _clean_text(raw)
        if not alias or len(alias) < 2:
            continue
        normalized = alias.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        aliases.append(alias)
    return aliases


def _read_mail_text_excerpt(paths: Sequence[str]) -> str:
    for raw_path in paths:
        path = Path(str(raw_path or "")).expanduser()
        if not path.exists() or not path.is_file():
            continue
        try:
            return path.read_bytes()[:65536].decode("utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            continue
    return ""


@lru_cache(maxsize=4096)
def _read_thread_text_excerpt_cached(
    db_path_value: str,
    db_inode: int,
    db_size: int,
    db_mtime_ns: int,
    db_ctime_ns: int,
    thread_key: str,
) -> str:
    path = Path(db_path_value).expanduser()
    if not thread_key or not path.exists() or not path.is_file():
        return ""
    try:
        with sqlite3.connect(str(path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = list(
                conn.execute(
                    """
                    SELECT
                        COALESCE(mi.direction, '') AS direction,
                        COALESCE(m.subject, '') AS subject,
                        COALESCE(m.snippet, '') AS snippet,
                        COALESCE(m.body_text, '') AS body_text,
                        COALESCE(m.from_json, '') AS from_json,
                        COALESCE(m.to_json, '') AS to_json,
                        COALESCE(m.cc_json, '') AS cc_json,
                        COALESCE(m.reply_to_json, '') AS reply_to_json,
                        COALESCE(m.sender_json, '') AS sender_json
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
    except sqlite3.DatabaseError:
        return ""
    if not rows:
        return ""
    selected_rows = rows
    if len(selected_rows) > 8:
        selected_rows = [*selected_rows[:4], *selected_rows[-4:]]
    parts: list[str] = []
    total_chars = 0
    max_chars = 65536
    for row in selected_rows:
        chunk = "\n".join(
            part
            for part in (
                f"direction={_clean_text(row['direction'])}",
                f"from={_clean_text(row['from_json'])}",
                f"to={_clean_text(row['to_json'])}",
                f"cc={_clean_text(row['cc_json'])}",
                f"reply_to={_clean_text(row['reply_to_json'])}",
                f"sender={_clean_text(row['sender_json'])}",
                _clean_text(row["subject"]),
                _clean_text(row["snippet"]),
                _clean_text(row["body_text"]),
            )
            if part
        )
        if not chunk:
            continue
        remaining = max_chars - total_chars
        if remaining <= 0:
            break
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        parts.append(chunk)
        total_chars += len(chunk)
    return "\n\n".join(parts)


def _build_thread_excerpt_cache_identity(shared_mail_db_path: Path) -> tuple[str, int, int, int, int]:
    path = Path(str(shared_mail_db_path or "")).expanduser()
    try:
        stat = path.stat()
    except OSError:
        return str(path), 0, 0, 0, 0
    return str(path), int(stat.st_ino), int(stat.st_size), int(stat.st_mtime_ns), int(stat.st_ctime_ns)


def _read_thread_text_excerpt(shared_mail_db_path: Path, thread_key: Any) -> str:
    return _read_thread_text_excerpt_cached(*_build_thread_excerpt_cache_identity(shared_mail_db_path), _clean_text(thread_key))


@lru_cache(maxsize=4096)
def _read_thread_reply_snapshot_cached(
    db_path_value: str,
    db_inode: int,
    db_size: int,
    db_mtime_ns: int,
    db_ctime_ns: int,
    thread_key: str,
) -> dict[str, Any]:
    path = Path(db_path_value).expanduser()
    if not thread_key or not path.exists() or not path.is_file():
        return {"messages": [], "latest_message": {}, "thread_creator_candidate_emails": []}
    try:
        with sqlite3.connect(str(path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = list(
                conn.execute(
                    """
                    SELECT
                        m.id,
                        COALESCE(m.account_email, '') AS account_email,
                        COALESCE(mi.direction, '') AS direction,
                        COALESCE(mi.sent_sort_at, m.sent_at, m.internal_date, m.created_at, '') AS sent_sort_at,
                        COALESCE(m.subject, '') AS subject,
                        COALESCE(m.snippet, '') AS snippet,
                        COALESCE(m.raw_path, '') AS raw_path,
                        COALESCE(m.from_json, '') AS from_json,
                        COALESCE(m.sender_json, '') AS sender_json,
                        COALESCE(m.to_json, '') AS to_json,
                        COALESCE(m.cc_json, '') AS cc_json,
                        COALESCE(m.bcc_json, '') AS bcc_json
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
    except sqlite3.DatabaseError:
        return {"messages": [], "latest_message": {}, "thread_creator_candidate_emails": []}
    latest_message: dict[str, Any] = {}
    message_payloads: list[dict[str, Any]] = []
    thread_creator_candidate_emails: list[str] = []
    seen_creator_candidates: set[str] = set()
    for row in rows:
        self_email = _normalize_email(row["account_email"])
        sender_emails = _extract_address_emails(row["from_json"])
        for email in _extract_address_emails(row["sender_json"]):
            if email not in sender_emails:
                sender_emails.append(email)
        recipient_emails = _extract_external_recipient_emails(
            row["to_json"],
            row["cc_json"],
            row["bcc_json"],
            excluded_emails=[self_email],
        )
        creator_candidate_emails: list[str] = []
        if _clean_text(row["direction"]).lower() == "outbound":
            for email in recipient_emails:
                creator_candidate_emails.append(email)
                if email not in seen_creator_candidates:
                    seen_creator_candidates.add(email)
                    thread_creator_candidate_emails.append(email)
        payload = {
            "row_id": int(row["id"]),
            "direction": _clean_text(row["direction"]).lower(),
            "sent_sort_at": _clean_text(row["sent_sort_at"]),
            "subject": _clean_text(row["subject"]),
            "snippet": _clean_text(row["snippet"]),
            "raw_path": _clean_text(row["raw_path"]),
            "sender_emails": sender_emails,
            "recipient_emails": recipient_emails,
            "creator_candidate_emails": creator_candidate_emails,
        }
        message_payloads.append(payload)
        latest_message = payload
    return {
        "messages": message_payloads,
        "latest_message": latest_message,
        "thread_creator_candidate_emails": thread_creator_candidate_emails,
    }


def _read_thread_reply_snapshot(shared_mail_db_path: Path, thread_key: Any) -> dict[str, Any]:
    return _read_thread_reply_snapshot_cached(
        *_build_thread_excerpt_cache_identity(shared_mail_db_path),
        _clean_text(thread_key),
    )


def _find_snapshot_message_by_raw_path(snapshot: dict[str, Any], raw_path: Any) -> tuple[int, dict[str, Any]]:
    normalized_raw_path = _clean_text(raw_path)
    if not normalized_raw_path:
        return -1, {}
    for index, message in enumerate(snapshot.get("messages") or []):
        payload = dict(message or {})
        if _clean_text(payload.get("raw_path")) == normalized_raw_path:
            return index, payload
    return -1, {}


def _infer_creator_target_emails_from_snapshot(snapshot: dict[str, Any], *, row_raw_path: Any) -> set[str]:
    messages = [dict(message or {}) for message in (snapshot.get("messages") or [])]
    if not messages:
        return set()

    anchor_index, anchor_message = _find_snapshot_message_by_raw_path(snapshot, row_raw_path)
    if anchor_message:
        search_start = anchor_index
        if _clean_text(anchor_message.get("direction")).lower() != "outbound":
            search_start = anchor_index - 1
        for index in range(search_start, -1, -1):
            message = messages[index]
            if _clean_text(message.get("direction")).lower() != "outbound":
                continue
            recipient_emails = {
                _normalize_email(email)
                for email in (message.get("recipient_emails") or [])
                if _normalize_email(email)
            }
            if len(recipient_emails) == 1:
                return recipient_emails
            return set()
        return set()

    thread_creator_candidate_emails = {
        _normalize_email(email)
        for email in (snapshot.get("thread_creator_candidate_emails") or [])
        if _normalize_email(email)
    }
    if len(thread_creator_candidate_emails) == 1:
        return thread_creator_candidate_emails
    return set()


def _extract_creator_reply_target_emails(row: dict[str, Any], snapshot: dict[str, Any]) -> set[str]:
    candidates: set[str] = set()
    for key in ("matched_contact_email", "creator_emails", "Email", "matched_email"):
        for email in _extract_emails_from_text(row.get(key)):
            candidates.add(email)
    if candidates:
        return candidates
    row_raw_path = (
        _clean_text(row.get("__brand_message_raw_path"))
        or _clean_text(row.get("__last_mail_raw_path"))
        or _clean_text(row.get("brand_message_raw_path"))
        or _clean_text(row.get("last_mail_raw_path"))
    )
    return _infer_creator_target_emails_from_snapshot(snapshot, row_raw_path=row_raw_path)


def _resolve_latest_creator_reply(snapshot: dict[str, Any], *, creator_target_emails: set[str]) -> dict[str, Any]:
    if not creator_target_emails:
        return {}
    latest_creator_reply: dict[str, Any] = {}
    for message in snapshot.get("messages") or []:
        if _clean_text((message or {}).get("direction")).lower() != "inbound":
            continue
        sender_emails = {
            _normalize_email(email)
            for email in ((message or {}).get("sender_emails") or [])
            if _normalize_email(email)
        }
        if sender_emails & creator_target_emails:
            latest_creator_reply = dict(message)
    return latest_creator_reply


@lru_cache(maxsize=4096)
def _lookup_thread_key_for_raw_path_cached(
    db_path_value: str,
    db_inode: int,
    db_size: int,
    db_mtime_ns: int,
    db_ctime_ns: int,
    raw_path: str,
) -> str:
    path = Path(db_path_value).expanduser()
    normalized_raw_path = _clean_text(raw_path)
    if not normalized_raw_path or not path.exists() or not path.is_file():
        return ""
    try:
        with sqlite3.connect(str(path)) as conn:
            row = conn.execute(
                """
                SELECT COALESCE(mi.thread_key, '')
                FROM messages m
                JOIN message_index mi ON mi.message_row_id = m.id
                WHERE COALESCE(m.raw_path, '') = ?
                ORDER BY m.id DESC
                LIMIT 1
                """,
                (normalized_raw_path,),
            ).fetchone()
    except sqlite3.DatabaseError:
        return ""
    if not row:
        return ""
    return _clean_text(row[0])


def _lookup_thread_key_for_raw_path(shared_mail_db_path: Path, raw_path: Any) -> str:
    return _lookup_thread_key_for_raw_path_cached(
        *_build_thread_excerpt_cache_identity(shared_mail_db_path),
        _clean_text(raw_path),
    )


def _apply_creator_reply_context(
    keep_row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = dict(keep_row)
    thread_key = _clean_text(updated.get("evidence_thread_key"))
    if not thread_key:
        return updated, {
            "status": "thread_key_missing",
            "creator_replied": None,
        }
    snapshot = _read_thread_reply_snapshot(shared_mail_db_path, thread_key)
    creator_target_emails = _extract_creator_reply_target_emails(updated, snapshot)
    latest_creator_reply = _resolve_latest_creator_reply(snapshot, creator_target_emails=creator_target_emails)
    if latest_creator_reply:
        updated["last_mail_message_id"] = latest_creator_reply.get("row_id") or ""
        updated["last_mail_time"] = latest_creator_reply.get("sent_sort_at") or ""
        updated["last_mail_subject"] = latest_creator_reply.get("subject") or ""
        updated["last_mail_snippet"] = latest_creator_reply.get("snippet") or ""
        updated["last_mail_raw_path"] = latest_creator_reply.get("raw_path") or ""
        return updated, {
            "status": "creator_replied",
            "creator_replied": True,
            "latest_inbound": latest_creator_reply,
            "latest_message": dict(snapshot.get("latest_message") or {}),
            "creator_target_emails": sorted(creator_target_emails),
        }
    if not creator_target_emails and (snapshot.get("messages") or []):
        return updated, {
            "status": "creator_identity_unresolved",
            "creator_replied": None,
            "latest_message": dict(snapshot.get("latest_message") or {}),
            "creator_target_emails": [],
        }
    for key in _LAST_MAIL_KEYS:
        updated[key] = ""
    return updated, {
        "status": "outbound_only_or_no_reply",
        "creator_replied": False,
        "latest_message": dict(snapshot.get("latest_message") or {}),
        "creator_target_emails": sorted(creator_target_emails),
    }


def _apply_thread_assignment_cache(
    keep_row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
    owner_scope: str,
    task_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = dict(keep_row)
    if _clean_text(updated.get("evidence_thread_key")):
        return updated, {"status": "skipped_existing_thread_key"}
    normalized_owner_scope = _clean_text(owner_scope)
    creator_id = _extract_creator_id(updated)
    platform = _extract_platform(updated)
    if not normalized_owner_scope or not creator_id or not platform:
        return updated, {
            "status": "skipped_insufficient_identity",
            "owner_scope": normalized_owner_scope,
            "creator_id": creator_id,
            "platform": platform,
        }
    assignment = lookup_thread_assignment(
        db_path=shared_mail_db_path,
        owner_scope=normalized_owner_scope,
        creator_id=creator_id,
        platform=platform,
        brand=task_name,
        matched_contact_email=_first_non_blank(updated.get("matched_contact_email"), updated.get("matched_email")),
        subject=_first_non_blank(updated.get("evidence_subject"), updated.get("last_mail_subject"), updated.get("subject")),
    )
    if not assignment:
        return updated, {
            "status": "cache_miss",
            "owner_scope": normalized_owner_scope,
            "creator_id": creator_id,
            "platform": platform,
        }
    updated["evidence_thread_key"] = _clean_text(assignment.get("thread_key"))
    if not _clean_text(updated.get("matched_contact_email")) and _clean_text(assignment.get("matched_contact_email")):
        updated["matched_contact_email"] = _clean_text(assignment.get("matched_contact_email"))
    if not _clean_text(updated.get("last_mail_message_id")) and _clean_text(assignment.get("last_mail_message_id")):
        updated["last_mail_message_id"] = _clean_text(assignment.get("last_mail_message_id"))
    if not _clean_text(updated.get("last_mail_time")) and _clean_text(assignment.get("last_mail_sent_at")):
        updated["last_mail_time"] = _clean_text(assignment.get("last_mail_sent_at"))
    if _clean_text(assignment.get("normalized_subject")) and not _clean_text(updated.get("last_mail_subject")):
        updated["last_mail_subject"] = _clean_text(assignment.get("normalized_subject"))
    updated["mail_update_revision"] = _coerce_non_negative_int(assignment.get("mail_update_revision"))
    return updated, {
        "status": "cache_hit",
        "thread_key": _clean_text(assignment.get("thread_key")),
        "match_reason": _clean_text(assignment.get("match_reason")),
    }


def _apply_creator_reply_context_to_export_row(
    row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = dict(row)
    existing_mail_content = _clean_text(updated.get("full body")) or _clean_text(updated.get("达人回复的最后一封邮件内容"))
    if existing_mail_content:
        updated.setdefault("full body", existing_mail_content)
        updated.setdefault("达人回复的最后一封邮件内容", existing_mail_content)
    thread_key = _clean_text(updated.get("evidence_thread_key")) or _lookup_thread_key_for_raw_path(
        shared_mail_db_path,
        updated.get("__last_mail_raw_path"),
    )
    if not thread_key:
        return updated, {
            "status": "thread_key_missing",
            "creator_replied": None,
        }
    snapshot = _read_thread_reply_snapshot(shared_mail_db_path, thread_key)
    creator_target_emails = _extract_creator_reply_target_emails(updated, snapshot)
    latest_creator_reply = _resolve_latest_creator_reply(snapshot, creator_target_emails=creator_target_emails)
    if latest_creator_reply:
        updated["达人最后一次回复邮件时间"] = _format_date(latest_creator_reply.get("sent_sort_at"))
        updated["full body"] = _clean_text(latest_creator_reply.get("snippet"))
        updated["达人回复的最后一封邮件内容"] = _clean_text(latest_creator_reply.get("snippet"))
        updated["__last_mail_raw_path"] = _clean_text(latest_creator_reply.get("raw_path"))
        return updated, {
            "status": "creator_replied",
            "creator_replied": True,
            "thread_key": thread_key,
            "latest_inbound": latest_creator_reply,
            "latest_message": dict(snapshot.get("latest_message") or {}),
            "creator_target_emails": sorted(creator_target_emails),
        }
    if not creator_target_emails and (snapshot.get("messages") or []):
        return updated, {
            "status": "creator_identity_unresolved",
            "creator_replied": None,
            "thread_key": thread_key,
            "latest_message": dict(snapshot.get("latest_message") or {}),
            "creator_target_emails": [],
        }
    updated["达人最后一次回复邮件时间"] = ""
    updated["full body"] = ""
    updated["达人回复的最后一封邮件内容"] = ""
    updated["__last_mail_raw_path"] = ""
    return updated, {
        "status": "outbound_only_or_no_reply",
        "creator_replied": False,
        "thread_key": thread_key,
        "latest_message": dict(snapshot.get("latest_message") or {}),
        "creator_target_emails": sorted(creator_target_emails),
    }


def _build_owner_search_texts(
    keep_row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> dict[str, str]:
    attachment_paths = _build_mail_attachment_paths(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    excerpt = _read_mail_text_excerpt(attachment_paths)
    representative_parts = [
        _clean_text(keep_row.get("brand_message_subject")),
        _clean_text(keep_row.get("brand_message_snippet")),
        _clean_text(keep_row.get("last_mail_subject")),
        _clean_text(keep_row.get("last_mail_snippet")),
        _clean_text(keep_row.get("matched_email")),
        _clean_text(keep_row.get("brand_message_folder")),
        excerpt,
    ]
    return {
        "representative_text": "\n".join(part for part in representative_parts if part),
        "thread_text": _read_thread_text_excerpt(shared_mail_db_path, keep_row.get("evidence_thread_key")),
    }


def _match_owner_aliases(search_text: str, candidate: dict[str, Any]) -> list[str]:
    matched_aliases: list[str] = []
    for alias in _build_owner_candidate_aliases(candidate):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", re.IGNORECASE)
        if pattern.search(search_text):
            matched_aliases.append(alias)
    return matched_aliases


def _find_owner_matches(search_text: str, owner_candidates: Sequence[dict[str, Any]]) -> list[tuple[dict[str, Any], list[str]]]:
    matches: list[tuple[dict[str, Any], list[str]]] = []
    for candidate in owner_candidates:
        aliases = _match_owner_aliases(search_text, candidate)
        if aliases:
            matches.append((candidate, aliases))
    return matches


def _resolve_group_row_owner_context(
    keep_row: dict[str, Any],
    *,
    task_owner_context: dict[str, str],
    owner_candidates: Sequence[dict[str, Any]],
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    evidence = _build_owner_search_texts(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    representative_text = str(evidence.get("representative_text") or "")
    matches = _find_owner_matches(representative_text, owner_candidates)
    if len(matches) == 1:
        candidate, aliases = matches[0]
        return _build_owner_context_from_candidate(candidate, task_owner_context), {
            "status": "resolved_from_mail_content",
            "aliases": aliases,
        }
    if len(matches) > 1:
        aliases = [alias for _, matched_aliases in matches for alias in matched_aliases]
        return _build_empty_owner_context(task_owner_context), {
            "status": "ambiguous_mail_owner",
            "aliases": aliases,
        }
    thread_text = str(evidence.get("thread_text") or "")
    if thread_text:
        thread_matches = _find_owner_matches("\n".join(part for part in (representative_text, thread_text) if part), owner_candidates)
        if len(thread_matches) == 1:
            candidate, aliases = thread_matches[0]
            return _build_owner_context_from_candidate(candidate, task_owner_context), {
                "status": "resolved_from_mail_thread",
                "aliases": aliases,
            }
        if len(thread_matches) > 1:
            aliases = [alias for _, matched_aliases in thread_matches for alias in matched_aliases]
            return _build_empty_owner_context(task_owner_context), {
                "status": "ambiguous_mail_owner",
                "aliases": aliases,
            }
    return _build_empty_owner_context(task_owner_context), {
        "status": "unresolved_mail_owner",
        "aliases": [],
    }


def _annotate_keep_frame_owner_context(
    keep_frame: pd.DataFrame,
    *,
    task_owner_context: dict[str, str],
    enable_row_level_owner_routing: bool,
    owner_candidates: Sequence[dict[str, Any]],
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for keep_row in keep_frame.to_dict(orient="records"):
        if enable_row_level_owner_routing:
            row_owner_context, resolution = _resolve_group_row_owner_context(
                keep_row,
                task_owner_context=task_owner_context,
                owner_candidates=owner_candidates,
                shared_mail_db_path=shared_mail_db_path,
                shared_mail_raw_dir=shared_mail_raw_dir,
                shared_mail_data_dir=shared_mail_data_dir,
                keep_workbook=keep_workbook,
            )
        else:
            row_owner_context = dict(task_owner_context)
            resolution = {"status": "task_owner_default", "aliases": []}
        annotated = dict(keep_row)
        annotated.update(
            {
                _KEEP_OWNER_DISPLAY_FIELD: _clean_text(row_owner_context.get("responsible_name"))
                or _clean_text(row_owner_context.get("employee_name")),
                _KEEP_OWNER_ENGLISH_NAME_FIELD: _clean_text(row_owner_context.get("employee_english_name")),
                _KEEP_OWNER_EMPLOYEE_ID_FIELD: _normalize_employee_id(row_owner_context.get("employee_id")),
                _KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD: _clean_text(row_owner_context.get("employee_record_id")),
                _KEEP_OWNER_EMPLOYEE_EMAIL_FIELD: _clean_text(row_owner_context.get("employee_email")),
                _KEEP_OWNER_OWNER_NAME_FIELD: _clean_text(row_owner_context.get("owner_name")),
                "任务名": _clean_text(row_owner_context.get("task_name")),
                "linked_bitable_url": _clean_text(row_owner_context.get("linked_bitable_url")),
                _KEEP_OWNER_STATUS_FIELD: _clean_text(resolution.get("status")),
                _KEEP_OWNER_ALIAS_FIELD: "；".join(_clean_text(alias) for alias in (resolution.get("aliases") or []) if _clean_text(alias)),
            }
        )
        records.append(annotated)
    columns = list(keep_frame.columns)
    for extra_column in (
        _KEEP_OWNER_DISPLAY_FIELD,
        _KEEP_OWNER_ENGLISH_NAME_FIELD,
        _KEEP_OWNER_EMPLOYEE_ID_FIELD,
        _KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD,
        _KEEP_OWNER_EMPLOYEE_EMAIL_FIELD,
        _KEEP_OWNER_OWNER_NAME_FIELD,
        "任务名",
        "linked_bitable_url",
        _KEEP_OWNER_STATUS_FIELD,
        _KEEP_OWNER_ALIAS_FIELD,
    ):
        if extra_column not in columns:
            columns.append(extra_column)
    return pd.DataFrame(records, columns=columns)


def _build_owner_context_from_keep_row(keep_row: dict[str, Any], fallback_owner_context: dict[str, str]) -> dict[str, str]:
    display_name = _clean_text(keep_row.get(_KEEP_OWNER_DISPLAY_FIELD))
    return {
        "task_name": _clean_text(keep_row.get("任务名")) or _clean_text(fallback_owner_context.get("task_name")),
        "linked_bitable_url": _clean_text(keep_row.get("linked_bitable_url"))
        or _clean_text(fallback_owner_context.get("linked_bitable_url")),
        "responsible_name": display_name
        or _clean_text(fallback_owner_context.get("responsible_name"))
        or _clean_text(fallback_owner_context.get("employee_name")),
        "employee_name": display_name or _clean_text(fallback_owner_context.get("employee_name")),
        "employee_english_name": _clean_text(keep_row.get(_KEEP_OWNER_ENGLISH_NAME_FIELD))
        or _clean_text(fallback_owner_context.get("employee_english_name")),
        "employee_id": _normalize_employee_id(keep_row.get(_KEEP_OWNER_EMPLOYEE_ID_FIELD) or fallback_owner_context.get("employee_id")),
        "employee_record_id": _clean_text(keep_row.get(_KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD))
        or _clean_text(fallback_owner_context.get("employee_record_id")),
        "employee_email": _clean_text(keep_row.get(_KEEP_OWNER_EMPLOYEE_EMAIL_FIELD))
        or _clean_text(fallback_owner_context.get("employee_email")),
        "owner_name": _clean_text(keep_row.get(_KEEP_OWNER_OWNER_NAME_FIELD))
        or _clean_text(fallback_owner_context.get("owner_name")),
    }


def _build_owner_context_from_export_row(row: dict[str, Any], fallback_owner_context: dict[str, str]) -> dict[str, str]:
    display_name = _clean_text(row.get("达人对接人"))
    return {
        "task_name": _clean_text(row.get("任务名")) or _clean_text(fallback_owner_context.get("task_name")),
        "linked_bitable_url": _clean_text(row.get("linked_bitable_url"))
        or _clean_text(fallback_owner_context.get("linked_bitable_url")),
        "responsible_name": display_name
        or _clean_text(fallback_owner_context.get("responsible_name"))
        or _clean_text(fallback_owner_context.get("employee_name")),
        "employee_name": display_name or _clean_text(fallback_owner_context.get("employee_name")),
        "employee_english_name": _clean_text(row.get(_KEEP_OWNER_ENGLISH_NAME_FIELD))
        or _clean_text(fallback_owner_context.get("employee_english_name")),
        "employee_id": _normalize_employee_id(row.get(_KEEP_OWNER_EMPLOYEE_ID_FIELD) or fallback_owner_context.get("employee_id")),
        "employee_record_id": _clean_text(row.get(_KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD))
        or _clean_text(fallback_owner_context.get("employee_record_id")),
        "employee_email": _clean_text(row.get(_KEEP_OWNER_EMPLOYEE_EMAIL_FIELD))
        or _clean_text(fallback_owner_context.get("employee_email")),
        "owner_name": _clean_text(row.get(_KEEP_OWNER_OWNER_NAME_FIELD))
        or _clean_text(fallback_owner_context.get("owner_name")),
    }


def _build_rewrite_owner_candidates(
    payload: dict[str, Any],
    inspection_items: Sequence[dict[str, Any]],
) -> list[dict[str, str]]:
    task_names = {
        _clean_text((payload.get("task_owner") or {}).get("task_name")),
        *(_clean_text(row.get("任务名")) for row in (payload.get("rows") or []) if isinstance(row, dict)),
    }
    task_names = {name for name in task_names if name}
    if not task_names:
        return []
    normalized_names = {name.casefold() for name in task_names}
    group_keys = {_derive_task_group_key(name) for name in task_names if _derive_task_group_key(name)}
    selected_items = [
        dict(item)
        for item in inspection_items
        if isinstance(item, dict)
        and (
            _clean_text(item.get("taskName")).casefold() in normalized_names
            or _derive_task_group_key(_clean_text(item.get("taskName"))) in group_keys
        )
    ]
    return _dedupe_employee_matches(
        [
            *(dict(entry) for item in selected_items for entry in (item.get("employeeMatches") or []) if isinstance(entry, dict)),
            *(_build_group_member_employee_match(item) for item in selected_items),
        ]
    )


def _resolve_export_row_owner_context(
    row: dict[str, Any],
    *,
    fallback_owner_context: dict[str, str],
    owner_candidates: Sequence[dict[str, Any]],
    shared_mail_db_path: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    existing_owner_context = _build_owner_context_from_export_row(row, fallback_owner_context)
    if not owner_candidates:
        return existing_owner_context, {"status": "rewrite_owner_candidates_missing", "aliases": []}
    representative_text = "\n".join(
        part
        for part in (
            _clean_text(row.get("达人回复的最后一封邮件内容")),
            _clean_text(row.get("达人最后一次回复邮件时间")),
        )
        if part
    )
    matches = _find_owner_matches(representative_text, owner_candidates)
    if len(matches) == 1:
        candidate, aliases = matches[0]
        return _build_owner_context_from_candidate(candidate, existing_owner_context), {
            "status": "resolved_from_export_content",
            "aliases": aliases,
        }
    if len(matches) > 1:
        aliases = [alias for _, matched_aliases in matches for alias in matched_aliases]
        return existing_owner_context, {
            "status": "ambiguous_export_owner",
            "aliases": aliases,
        }
    thread_key = _lookup_thread_key_for_raw_path(shared_mail_db_path, row.get("__last_mail_raw_path"))
    if thread_key:
        thread_matches = _find_owner_matches(
            "\n".join(part for part in (representative_text, _read_thread_text_excerpt(shared_mail_db_path, thread_key)) if part),
            owner_candidates,
        )
        if len(thread_matches) == 1:
            candidate, aliases = thread_matches[0]
            return _build_owner_context_from_candidate(candidate, existing_owner_context), {
                "status": "resolved_from_export_thread",
                "aliases": aliases,
            }
        if len(thread_matches) > 1:
            aliases = [alias for _, matched_aliases in thread_matches for alias in matched_aliases]
            return existing_owner_context, {
                "status": "ambiguous_export_owner",
                "aliases": aliases,
            }
    return existing_owner_context, {
        "status": "rewrite_owner_unresolved",
        "aliases": [],
    }


def _build_keep_row_owner_lookup(
    keep_frame: pd.DataFrame,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    handle_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    url_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for keep_row in keep_frame.to_dict(orient="records"):
        platform = _extract_platform(keep_row)
        if not platform:
            continue
        creator_id = _extract_creator_id(keep_row)
        if creator_id:
            handle_lookup.setdefault((platform, creator_id.casefold()), dict(keep_row))
        normalized_url = _normalize_url(keep_row.get("URL"))
        if normalized_url:
            url_lookup.setdefault((platform, normalized_url), dict(keep_row))
    return handle_lookup, url_lookup


def _apply_row_owner_overrides(
    rows: list[dict[str, Any]],
    *,
    keep_frame: pd.DataFrame,
    fallback_owner_context: dict[str, str],
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    handle_lookup, url_lookup = _build_keep_row_owner_lookup(keep_frame)
    overridden_rows: list[dict[str, Any]] = []
    for row in rows:
        platform = _normalize_platform(row.get("平台") or row.get("Platform"))
        creator_id = (
            _extract_handle(row.get("达人ID"))
            or _extract_handle(row.get("主页链接") or row.get("URL"))
        )
        normalized_url = _normalize_url(row.get("主页链接") or row.get("URL"))
        keep_row = handle_lookup.get((platform, creator_id.casefold())) or url_lookup.get((platform, normalized_url)) or {}
        row_owner_context = _build_owner_context_from_keep_row(keep_row, fallback_owner_context)
        updated = dict(row)
        display_name = _clean_text(row_owner_context.get("responsible_name")) or _clean_text(row_owner_context.get("employee_name"))
        if display_name:
            updated["达人对接人"] = display_name
        if keep_row:
            attachment_paths = _build_mail_attachment_paths(
                keep_row,
                shared_mail_db_path=shared_mail_db_path,
                shared_mail_raw_dir=shared_mail_raw_dir,
                shared_mail_data_dir=shared_mail_data_dir,
                keep_workbook=keep_workbook,
            )
            mail_fields = _resolve_keep_row_mail_fields(keep_row, existing_row=updated)
            if mail_fields["quote_text"] and not _clean_text(updated.get("当前网红报价")):
                updated["当前网红报价"] = mail_fields["quote_text"]
            if mail_fields["last_mail_time"] and not _clean_text(updated.get("达人最后一次回复邮件时间")):
                updated["达人最后一次回复邮件时间"] = mail_fields["last_mail_time"]
            resolved_mail_content = mail_fields["last_mail_content"] or _clean_text(updated.get("full body")) or _clean_text(
                updated.get("达人回复的最后一封邮件内容")
            )
            if resolved_mail_content:
                if not _clean_text(updated.get("full body")):
                    updated["full body"] = resolved_mail_content
                if not _clean_text(updated.get("达人回复的最后一封邮件内容")):
                    updated["达人回复的最后一封邮件内容"] = resolved_mail_content
            updated[_KEEP_OWNER_EMPLOYEE_ID_FIELD] = _clean_text(row_owner_context.get("employee_id")).split(",")[0].strip()
            updated[_KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD] = _clean_text(row_owner_context.get("employee_record_id"))
            updated[_KEEP_OWNER_EMPLOYEE_EMAIL_FIELD] = _clean_text(row_owner_context.get("employee_email"))
            updated[_KEEP_OWNER_OWNER_NAME_FIELD] = _clean_text(row_owner_context.get("owner_name"))
            updated["linked_bitable_url"] = _clean_text(row_owner_context.get("linked_bitable_url"))
            updated["任务名"] = _clean_text(row_owner_context.get("task_name"))
            if not _clean_text(updated.get("__last_mail_raw_path")):
                updated["__last_mail_raw_path"] = _clean_text(
                    keep_row.get("brand_message_raw_path") or keep_row.get("last_mail_raw_path")
                )
            if not list(updated.get("__feishu_attachment_local_paths") or []):
                updated["__feishu_attachment_local_paths"] = attachment_paths
        overridden_rows.append(updated)
    return overridden_rows


def _build_skipped_row_from_keep_record(
    keep_row: dict[str, Any],
    *,
    owner_context: dict[str, str],
    reason: str,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> dict[str, Any]:
    attachment_paths = _build_mail_attachment_paths(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    display_name = _clean_text(owner_context.get("responsible_name")) or _clean_text(owner_context.get("employee_name"))
    mail_fields = _resolve_keep_row_mail_fields(keep_row)
    return {
        "达人ID": _extract_creator_id(keep_row),
        "平台": _extract_platform(keep_row),
        "主页链接": _clean_text(keep_row.get("URL") or keep_row.get("主页链接")),
        "当前网红报价": mail_fields["quote_text"],
        "达人最后一次回复邮件时间": mail_fields["last_mail_time"],
        "full body": mail_fields["last_mail_content"],
        "达人回复的最后一封邮件内容": mail_fields["last_mail_content"],
        "达人对接人": display_name,
        "达人对接人_employee_id": _clean_text(owner_context.get("employee_id")).split(",")[0].strip(),
        "达人对接人_employee_record_id": _clean_text(owner_context.get("employee_record_id")),
        "达人对接人_employee_email": _clean_text(owner_context.get("employee_email")),
        "达人对接人_owner_name": _clean_text(owner_context.get("owner_name")),
        "linked_bitable_url": _clean_text(owner_context.get("linked_bitable_url")),
        "任务名": _clean_text(owner_context.get("task_name")),
        "ai是否通过": "",
        "ai筛号反馈理由": reason,
        "标签(ai)": "",
        "ai评价": "",
        "__last_mail_raw_path": _clean_text(keep_row.get("brand_message_raw_path") or keep_row.get("last_mail_raw_path")),
        "__feishu_attachment_local_paths": attachment_paths,
    }


def _build_feishu_client(
    *,
    env_file: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
) -> tuple[Any, dict[str, str], dict[str, Any]]:
    runtime = _load_runtime_dependencies()
    env_values = runtime["load_local_env"](env_file)
    app_id, app_id_source = _resolve_cli_env_value(feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret, app_secret_source = _resolve_cli_env_value(feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或参数里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或参数里填写。")
    resolved_timeout, timeout_source = _resolve_cli_env_value(
        timeout_seconds if timeout_seconds > 0 else "",
        env_values,
        "TIMEOUT_SECONDS",
        "30",
    )
    base_url, base_url_source = _resolve_cli_env_value(
        feishu_base_url,
        env_values,
        "FEISHU_OPEN_BASE_URL",
        runtime["DEFAULT_FEISHU_BASE_URL"],
    )
    client = runtime["FeishuOpenClient"](
        app_id=app_id,
        app_secret=app_secret,
        base_url=base_url,
        timeout_seconds=float(resolved_timeout),
    )
    return client, env_values, {
        "feishu_app_id_source": app_id_source,
        "feishu_app_secret_source": app_secret_source,
        "feishu_base_url": base_url,
        "feishu_base_url_source": base_url_source,
        "timeout_seconds": float(resolved_timeout),
        "timeout_seconds_source": timeout_source,
    }


def run_shared_mailbox_post_sync_pipeline(
    *,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None = None,
    shared_mail_data_dir: Path | None = None,
    env_file: str = ".env",
    sent_since: str = "",
    task_upload_url: str = "",
    employee_info_url: str = "",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    task_name_filters: Sequence[str] | None = None,
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    timeout_seconds: float = 0.0,
    owner_email_overrides: dict[str, str] | None = None,
    folder_prefixes: list[str] | None = None,
    matching_strategy: str = "brand-keyword-fast-path",
    brand_keyword: str = "",
    brand_match_include_from: bool = True,
    platform_filters: list[str] | None = None,
    vision_provider: str = "",
    positioning_provider: str = "",
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    upload_dry_run: bool = False,
    reuse_existing: bool = True,
    mail_first_only: bool = False,
    thread_first_mail_resolution: bool = False,
) -> dict[str, Any]:
    runtime = _load_runtime_dependencies()
    inspect_task_upload_assignments = runtime["inspect_task_upload_assignments"]
    fetch_existing_bitable_record_analysis = runtime["fetch_existing_bitable_record_analysis"]
    run_task_upload_to_keep_list_pipeline = runtime["run_task_upload_to_keep_list_pipeline"]
    run_keep_list_screening_pipeline = runtime["run_keep_list_screening_pipeline"]
    upload_final_review_payload_to_bitable = runtime["upload_final_review_payload_to_bitable"]

    resolved_mail_db_path = shared_mail_db_path.expanduser().resolve()
    if not resolved_mail_db_path.exists():
        raise FileNotFoundError(f"shared_mail_db_path 不存在: {resolved_mail_db_path}")
    resolved_mail_raw_dir = shared_mail_raw_dir.expanduser().resolve() if shared_mail_raw_dir else None
    resolved_mail_data_dir = shared_mail_data_dir.expanduser().resolve() if shared_mail_data_dir else resolved_mail_db_path.parent
    resolved_output_root = (output_root or default_output_root()).expanduser().resolve()
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    run_summary_path = summary_json.expanduser().resolve() if summary_json else resolved_output_root / "summary.json"
    aggregate_archive_dir = resolved_output_root / "local_archive"
    aggregate_archive_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "finished_at": "",
        "status": "running",
        "env_file": str(env_file),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "resolved_inputs": {
            "shared_mail_db_path": _path_summary(resolved_mail_db_path, source="cli", kind="file"),
            "shared_mail_raw_dir": _path_summary(resolved_mail_raw_dir, source="cli_or_inferred", kind="dir"),
            "shared_mail_data_dir": _path_summary(resolved_mail_data_dir, source="cli_or_inferred", kind="dir"),
            "sent_since": _clean_text(sent_since),
        },
        "task_count": 0,
        "task_names": [],
        "matched_mail_count": 0,
        "new_creator_count": 0,
        "existing_screened_count": 0,
        "existing_unscreened_count": 0,
        "pre_keep_mail_only_count": 0,
        "partial_refresh_count": 0,
        "known_thread_hit_count": 0,
        "thread_assignment_cache_hit_count": 0,
        "source_identity_deduped_count": 0,
        "full_screening_count": 0,
        "mail_only_update_count": 0,
        "mail_first_only_count": 0,
        "keyword_hit_thread_count": 0,
        "resolved_thread_count": 0,
        "written_row_count": 0,
        "skipped_existing_count": 0,
        "created_record_count": 0,
        "updated_record_count": 0,
        "failed_record_count": 0,
        **_empty_rescue_stats(),
        **_empty_mail_only_bucket_summary(),
        "mail_first_only_enabled": bool(mail_first_only),
        "thread_first_mail_resolution_enabled": bool(thread_first_mail_resolution),
        "local_archive_path": str(aggregate_archive_dir),
        "task_results": [],
    }
    progress_scope = "shared-mailbox-post-sync"
    _write_json(run_summary_path, summary)
    _emit_runtime_progress(progress_scope, f"starting output_root={resolved_output_root}")

    client, env_values, feishu_resolution = _build_feishu_client(
        env_file=env_file,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
    )
    resolved_task_upload_url, _ = _resolve_cli_env_value(task_upload_url, env_values, "TASK_UPLOAD_URL")
    if not resolved_task_upload_url:
        resolved_task_upload_url, _ = _resolve_cli_env_value(task_upload_url, env_values, "FEISHU_SOURCE_URL")
    if not resolved_task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或参数里填写。")
    resolved_employee_info_url, _ = _resolve_cli_env_value(employee_info_url, env_values, "EMPLOYEE_INFO_URL")
    if not resolved_employee_info_url:
        resolved_employee_info_url, _ = _resolve_cli_env_value(employee_info_url, env_values, "FEISHU_SOURCE_URL")
    if not resolved_employee_info_url:
        raise ValueError("缺少 EMPLOYEE_INFO_URL，请在本地 .env 或参数里填写。")

    _emit_runtime_progress(progress_scope, "inspection=running")
    inspection = inspect_task_upload_assignments(
        client=client,
        task_upload_url=resolved_task_upload_url,
        employee_info_url=resolved_employee_info_url,
        download_dir=resolved_output_root / "inspection_downloads",
        download_templates=False,
        parse_templates=False,
        owner_email_overrides=owner_email_overrides or {},
    )
    requested_task_filters = _collect_task_filters(task_name_filters)
    inspection_items = [
        dict(item)
        for item in (inspection.get("items") or [])
        if isinstance(item, dict) and _clean_text(item.get("taskName"))
    ]
    resolved_task_names = _resolve_requested_task_names(inspection_items, requested_task_filters)
    if requested_task_filters:
        inspection_items = [
            item
            for item in inspection_items
            if _clean_text(item.get("taskName")).casefold() in resolved_task_names
        ]
    inspection_items = _collapse_grouped_inspection_items(inspection_items, requested_task_filters)

    summary["task_count"] = len(inspection_items)
    summary["task_names"] = [_clean_text(item.get("taskName")) for item in inspection_items]
    summary["feishu"] = {
        "task_upload_url": resolved_task_upload_url,
        "employee_info_url": resolved_employee_info_url,
        "feishu_base_url": feishu_resolution["feishu_base_url"],
        "timeout_seconds": feishu_resolution["timeout_seconds"],
    }
    _write_json(run_summary_path, summary)
    _emit_runtime_progress(progress_scope, f"inspection=completed task_count={len(inspection_items)}")

    aggregate_failed_rows: list[dict[str, Any]] = []
    aggregate_existing_skip_rows: list[dict[str, Any]] = []
    any_task_failed = False
    seen_mail_identity_keys_by_target: dict[str, set[str]] = {}

    for item in inspection_items:
        task_name = _clean_text(item.get("taskName"))
        task_scope = f"{progress_scope}:{task_name or 'unknown-task'}"
        task_slug = f"{_safe_name(task_name)}_{_safe_name(_clean_text(item.get('recordId')))}"
        task_root = resolved_output_root / task_slug
        task_root.mkdir(parents=True, exist_ok=True)
        task_summary_path = task_root / "summary.json"
        task_result: dict[str, Any] = {
            "task_name": task_name,
            "source_task_names": list(item.get("groupedTaskNames") or ([task_name] if task_name else [])),
            "representative_task_name": _clean_text(item.get("representativeTaskName")) or task_name,
            "linked_bitable_url": _clean_text(item.get("linkedBitableUrl")),
            "matched_mail_count": 0,
            "pre_keep_mail_only_count": 0,
            "partial_refresh_count": 0,
            "known_thread_hit_count": 0,
            "thread_assignment_cache_hit_count": 0,
            "source_identity_deduped_count": 0,
            "full_screening_count": 0,
            "mail_only_update_count": 0,
            "mail_first_only_count": 0,
            "keyword_hit_thread_count": 0,
            "resolved_thread_count": 0,
            "written_row_count": 0,
            "skipped_existing_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "failed_count": 0,
            **_empty_rescue_stats(),
            **_empty_mail_only_bucket_summary(),
            "thread_first_mail_resolution_enabled": bool(thread_first_mail_resolution),
            "summary_path": str(task_summary_path),
            "all_platforms_final_review": "",
            "all_platforms_upload_payload_json": "",
            "feishu_upload_result_json": "",
            "mail_only_candidate_log_json": "",
            "mail_only_candidate_log_xlsx": "",
            "mail_only_upload_decision_json": "",
            "mail_only_upload_decision_xlsx": "",
            "unresolved_threads_json": "",
            "unresolved_threads_xlsx": "",
            "status": "running",
            "upstream_summary_json": "",
            "downstream_summary_json": "",
            "mail_first_only_enabled": bool(mail_first_only),
        }
        _emit_runtime_progress(task_scope, "task=running")
        try:
            if bool(item.get("ownerMatchAmbiguous")) and not bool(item.get("rowLevelOwnerRouting")):
                matched_entries = [
                    dict(entry)
                    for entry in (item.get("employeeMatches") or [])
                    if isinstance(entry, dict)
                ]
                failure = _build_failure_payload(
                    stage="inspection",
                    error_code="TASK_OWNER_MATCH_AMBIGUOUS",
                    message=f"{task_name} 命中多个负责人，当前无法安全判定唯一负责人。",
                    remediation="请通过 owner_email_override 显式指定该任务负责人后再继续共享邮箱正式主线。",
                    details={
                        "task_name": task_name,
                        "owner_email_candidates": list(item.get("ownerEmailCandidates") or []),
                        "matched_employees": matched_entries,
                    },
                )
                task_result["status"] = "inspection_failed"
                task_result["failed_count"] = 1
                task_result["failure"] = failure
                aggregate_failed_rows.append(
                    {
                        "task_name": task_name,
                        "stage": "inspection",
                        "error_code": "TASK_OWNER_MATCH_AMBIGUOUS",
                        "message": failure["message"],
                        "remediation": failure["remediation"],
                        "details": failure["details"],
                    }
                )
                any_task_failed = True
                summary["failed_record_count"] = int(summary.get("failed_record_count") or 0) + 1
                summary["task_results"].append(task_result)
                _write_json(task_summary_path, task_result)
                _write_json(run_summary_path, summary)
                continue

            upstream_task_name = _clean_text(item.get("representativeTaskName")) or task_name
            upstream_output_root = task_root / "upstream"
            upstream_summary_path = upstream_output_root / "summary.json"
            _emit_runtime_progress(task_scope, "upstream=running")
            upstream_summary = run_task_upload_to_keep_list_pipeline(
                task_name=upstream_task_name,
                env_file=env_file,
                task_upload_url=resolved_task_upload_url,
                employee_info_url=resolved_employee_info_url,
                output_root=upstream_output_root,
                summary_json=upstream_summary_path,
                feishu_app_id=feishu_app_id,
                feishu_app_secret=feishu_app_secret,
                feishu_base_url=feishu_base_url,
                timeout_seconds=float(feishu_resolution["timeout_seconds"]),
                folder_prefixes=folder_prefixes or ["其他文件夹/邮件备份"],
                owner_email_overrides=owner_email_overrides or {},
                existing_mail_db_path=resolved_mail_db_path,
                existing_mail_raw_dir=resolved_mail_raw_dir or "",
                existing_mail_data_dir=resolved_mail_data_dir or "",
                sent_since=_clean_text(sent_since),
                stop_after="keep-list",
                reuse_existing=bool(reuse_existing),
                matching_strategy=matching_strategy,
                brand_keyword=_resolve_group_brand_keyword(
                    task_name=task_name,
                    explicit_brand_keyword=brand_keyword,
                ),
                brand_match_include_from=bool(brand_match_include_from),
                thread_first_mail_resolution=bool(thread_first_mail_resolution and mail_first_only),
            )
            task_result["upstream_summary_json"] = str(upstream_summary_path)
            _emit_runtime_progress(
                task_scope,
                f"upstream=completed status={str(upstream_summary.get('status') or '').strip() or 'unknown'}",
            )
            keep_workbook = _resolve_optional_path(
                ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("keep_workbook")
                or (upstream_summary.get("artifacts") or {}).get("keep_workbook")
            )
            pre_keep_mail_only_workbook = _resolve_optional_path(
                ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("pre_keep_mail_only_workbook")
                or (upstream_summary.get("artifacts") or {}).get("pre_keep_mail_only_workbook")
            )
            template_workbook_value = str(
                ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("template_workbook")
                or (upstream_summary.get("artifacts") or {}).get("template_workbook")
                or ""
            ).strip()
            if str(upstream_summary.get("status") or "") == "failed" or keep_workbook is None or not keep_workbook.is_file():
                failure = _build_failure_payload(
                    stage="upstream",
                    error_code=str(upstream_summary.get("error_code") or "UPSTREAM_KEEP_LIST_FAILED"),
                    message=str(upstream_summary.get("error") or f"{task_name} upstream keep-list 未完成"),
                    remediation="先修复共享邮箱 post-sync 上游 keep-list 阶段，再继续当前任务。",
                    details={"task_name": task_name, "upstream_summary_json": str(upstream_summary_path)},
                )
                task_result["status"] = "upstream_failed"
                task_result["failed_count"] = 1
                task_result["failure"] = failure
                aggregate_failed_rows.append(
                    {
                        "task_name": task_name,
                        "stage": failure["stage"],
                        "reason": failure["message"],
                        "row": {},
                    }
                )
                any_task_failed = True
                summary["task_results"].append(task_result)
                summary["failed_record_count"] += 1
                _write_json(task_summary_path, task_result)
                _write_json(run_summary_path, summary)
                continue

            keep_frame = pd.read_excel(keep_workbook)
            brand_match_stats = dict((((upstream_summary.get("steps") or {}).get("brand_match") or {}).get("stats") or {}))
            mail_funnel_stats = dict((((upstream_summary.get("steps") or {}).get("mail_funnel") or {}).get("stats") or {}))
            unresolved_threads_workbook = _resolve_optional_path(
                ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("unresolved_threads_xlsx")
                or (((upstream_summary.get("steps") or {}).get("mail_funnel") or {}).get("artifacts") or {}).get("unresolved_threads_xlsx")
            )
            unresolved_thread_rows: list[dict[str, Any]] = []
            if bool(thread_first_mail_resolution and mail_first_only) and unresolved_threads_workbook is not None and unresolved_threads_workbook.is_file():
                unresolved_thread_rows = pd.read_excel(unresolved_threads_workbook).to_dict(orient="records")
                unresolved_json, unresolved_xlsx = _write_unresolved_threads_archive(
                    task_root / "exports",
                    unresolved_thread_rows,
                )
                task_result["unresolved_threads_json"] = unresolved_json
                task_result["unresolved_threads_xlsx"] = unresolved_xlsx
            task_result["keyword_hit_thread_count"] = int(brand_match_stats.get("thread_hit_count") or 0)
            task_result["resolved_thread_count"] = int(mail_funnel_stats.get("resolved_thread_count") or len(keep_frame.index))
            for key in (
                "regex_pass1_rescued_count",
                "regex_pass2_rescued_count",
                "full_body_only_hit_count",
                "beacons_linktree_hit_count",
                "llm_invocation_count_before",
                "llm_invocation_count_after",
            ):
                task_result[key] = int(mail_funnel_stats.get(key) or 0)
            owner_context = _build_owner_context_from_upstream(upstream_summary, item)
            linked_bitable_url = _clean_text(owner_context.get("linked_bitable_url")) or _clean_text(item.get("linkedBitableUrl"))
            owner_context["linked_bitable_url"] = linked_bitable_url
            owner_context["task_name"] = _clean_text(owner_context.get("task_name")) or task_name
            owner_candidates = [
                dict(entry)
                for entry in (item.get("employeeMatches") or [])
                if isinstance(entry, dict)
            ]
            keep_frame = _annotate_keep_frame_owner_context(
                keep_frame,
                task_owner_context=owner_context,
                enable_row_level_owner_routing=bool(item.get("rowLevelOwnerRouting")),
                owner_candidates=owner_candidates,
                shared_mail_db_path=resolved_mail_db_path,
                shared_mail_raw_dir=resolved_mail_raw_dir,
                shared_mail_data_dir=resolved_mail_data_dir,
                keep_workbook=keep_workbook,
            )
            pre_keep_mail_only_frame = pd.DataFrame(columns=list(keep_frame.columns))
            if pre_keep_mail_only_workbook is not None and pre_keep_mail_only_workbook.is_file():
                pre_keep_mail_only_frame = _annotate_keep_frame_owner_context(
                    pd.read_excel(pre_keep_mail_only_workbook),
                    task_owner_context=owner_context,
                    enable_row_level_owner_routing=bool(item.get("rowLevelOwnerRouting")),
                    owner_candidates=owner_candidates,
                    shared_mail_db_path=resolved_mail_db_path,
                    shared_mail_raw_dir=resolved_mail_raw_dir,
                    shared_mail_data_dir=resolved_mail_data_dir,
                    keep_workbook=pre_keep_mail_only_workbook,
                )
            resolved_target_view, existing_analysis = fetch_existing_bitable_record_analysis(
                client,
                linked_bitable_url=linked_bitable_url,
            )
            duplicate_existing_groups = list(existing_analysis.duplicate_groups)
            if duplicate_existing_groups:
                failure = _build_failure_payload(
                    stage="feishu_existing_guard",
                    error_code="FEISHU_DUPLICATE_RECORDS_DETECTED",
                    message="目标飞书表存在重复的 达人ID+平台 记录，已阻止继续执行当前任务。",
                    remediation="先清理目标飞书表中的重复记录，再重跑共享邮箱 post-sync 主线。",
                    details={
                        "task_name": task_name,
                        "linked_bitable_url": linked_bitable_url,
                        "duplicate_group_count": len(duplicate_existing_groups),
                    },
                )
                task_result["status"] = "guard_blocked_duplicate_existing"
                task_result["failed_count"] = len(duplicate_existing_groups)
                task_result["failure"] = failure
                task_result["duplicate_existing_group_count"] = len(duplicate_existing_groups)
                summary["failed_record_count"] += task_result["failed_count"]
                any_task_failed = True
                aggregate_failed_rows.append(
                    {
                        "task_name": task_name,
                        "stage": failure["stage"],
                        "reason": failure["message"],
                        "row": {},
                    }
                )
                summary["task_results"].append(task_result)
                _write_json(task_summary_path, task_result)
                _write_json(run_summary_path, summary)
                continue
            existing_index = existing_analysis.index
            target_upload_scope = _build_record_key(
                getattr(resolved_target_view, "app_token", ""),
                getattr(resolved_target_view, "table_id", ""),
            ) or _clean_text(linked_bitable_url)
            seen_mail_identity_keys = seen_mail_identity_keys_by_target.setdefault(target_upload_scope, set())
            owner_scope_enabled = bool(_clean_text(getattr(existing_analysis, "owner_scope_field_name", "")))

            matched_mail_count = _extract_matched_mail_count(upstream_summary, keep_frame)
            mail_only_display_rows: list[dict[str, Any]] = []
            mail_only_payload_rows: list[dict[str, Any]] = []
            combined_skipped_rows: list[dict[str, Any]] = []
            source_dedup_skipped_rows: list[dict[str, Any]] = []
            mail_only_candidate_log_rows: list[dict[str, Any]] = []
            mail_only_bucket_summary = _empty_mail_only_bucket_summary()
            prepared_candidates: list[dict[str, Any]] = []
            pre_keep_mail_only_count = int(len(pre_keep_mail_only_frame.index))
            combined_prepared_rows = [
                *pre_keep_mail_only_frame.to_dict(orient="records"),
                *keep_frame.to_dict(orient="records"),
            ]

            for keep_row in combined_prepared_rows:
                row_owner_context = _build_owner_context_from_keep_row(keep_row, owner_context)
                row_owner_scope_value = _clean_text(row_owner_context.get("employee_id")) or _clean_text(
                    row_owner_context.get("responsible_name")
                )
                keep_row, thread_assignment_resolution = _apply_thread_assignment_cache(
                    keep_row,
                    shared_mail_db_path=resolved_mail_db_path,
                    owner_scope=row_owner_scope_value,
                    task_name=task_name,
                )
                keep_row, reply_resolution = _apply_creator_reply_context(
                    keep_row,
                    shared_mail_db_path=resolved_mail_db_path,
                )
                reply_resolution["thread_assignment_cache"] = thread_assignment_resolution
                reply_status = _clean_text(reply_resolution.get("status"))
                if reply_status == "creator_replied":
                    mail_only_bucket_summary["mail_only_creator_replied_count"] += 1
                elif reply_status == "outbound_only_or_no_reply":
                    mail_only_bucket_summary["mail_only_outbound_only_skip_count"] += 1
                elif reply_status == "creator_identity_unresolved":
                    mail_only_bucket_summary["mail_only_creator_identity_unresolved_count"] += 1
                elif reply_status == "thread_key_missing":
                    mail_only_bucket_summary["mail_only_thread_key_missing_count"] += 1
                if bool(item.get("rowLevelOwnerRouting")) and not row_owner_scope_value:
                    owner_status = _clean_text(keep_row.get(_KEEP_OWNER_STATUS_FIELD))
                    alias_text = _clean_text(keep_row.get(_KEEP_OWNER_ALIAS_FIELD))
                    reason = "无法根据邮件内容匹配唯一负责人，已跳过写回。"
                    if owner_status == "ambiguous_mail_owner" and alias_text:
                        reason = f"邮件内容同时命中多个负责人别名（{alias_text}），已跳过写回。"
                    elif owner_status == "unresolved_mail_owner":
                        reason = "邮件内容未命中任何负责人英文名或邮箱别名，已跳过写回。"
                    combined_skipped_rows.append(
                        {
                            "skip_reasons": [reason],
                            "row": _build_skipped_row_from_keep_record(
                                keep_row,
                                owner_context=row_owner_context,
                                reason=reason,
                                shared_mail_db_path=resolved_mail_db_path,
                                shared_mail_raw_dir=resolved_mail_raw_dir,
                                shared_mail_data_dir=resolved_mail_data_dir,
                                keep_workbook=keep_workbook,
                            ),
                        }
                    )
                    mail_only_candidate_log_rows.append(
                        _build_mail_only_candidate_log_entry(
                            task_name=task_name,
                            keep_row=keep_row,
                            owner_context=row_owner_context,
                            route_decision="skipped_owner_routing",
                            reason=reason,
                            reply_resolution=reply_resolution,
                            thread_assignment_resolution=thread_assignment_resolution,
                        )
                    )
                    continue
                if reply_resolution.get("creator_replied") is False:
                    reason = "仅命中负责人发信，达人未回复，已跳过筛选。"
                    combined_skipped_rows.append(
                        {
                            "skip_reasons": [reason],
                            "row": _build_skipped_row_from_keep_record(
                                keep_row,
                                owner_context=row_owner_context,
                                reason=reason,
                                shared_mail_db_path=resolved_mail_db_path,
                                shared_mail_raw_dir=resolved_mail_raw_dir,
                                shared_mail_data_dir=resolved_mail_data_dir,
                                keep_workbook=keep_workbook,
                            ),
                        }
                    )
                    mail_only_candidate_log_rows.append(
                        _build_mail_only_candidate_log_entry(
                            task_name=task_name,
                            keep_row=keep_row,
                            owner_context=row_owner_context,
                            route_decision="skipped_outbound_only_or_no_reply",
                            reason=reason,
                            reply_resolution=reply_resolution,
                            thread_assignment_resolution=thread_assignment_resolution,
                        )
                    )
                    continue
                prepared_candidates.append(
                    {
                        "keep_row": dict(keep_row),
                        "owner_context": dict(row_owner_context),
                        "owner_scope": row_owner_scope_value,
                        "creator_id": _extract_creator_id(keep_row),
                        "platform": _extract_platform(keep_row),
                        "thread_key": _clean_text(keep_row.get("evidence_thread_key")),
                        "last_mail_message_id": _clean_text(keep_row.get("last_mail_message_id")),
                        "mail_update_revision": _coerce_non_negative_int(keep_row.get("mail_update_revision")),
                        "thread_assignment_resolution": dict(thread_assignment_resolution or {}),
                        "reply_resolution": dict(reply_resolution or {}),
                    }
                )

            mail_only_bucket_summary["mail_only_candidate_count_before_source_dedup"] = len(prepared_candidates)
            source_identity_dedup = _deduplicate_prepared_candidates_by_mail_identity(
                prepared_candidates,
                seen_identity_keys=seen_mail_identity_keys,
                deduplicate_within_batch=False,
            )
            prepared_candidates = list(source_identity_dedup.get("candidates") or [])
            source_identity_deduped_count = int(((source_identity_dedup.get("stats") or {}).get("deduplicated_count")) or 0)
            mail_only_bucket_summary["mail_only_candidate_count_after_source_dedup"] = len(prepared_candidates)
            mail_only_bucket_summary["mail_only_source_identity_deduped_count"] = source_identity_deduped_count
            for duplicate in list(source_identity_dedup.get("duplicate_rows") or []):
                candidate = dict(duplicate.get("candidate") or {})
                keep_row = dict(candidate.get("keep_row") or {})
                row_owner_context = dict(candidate.get("owner_context") or {})
                identity_label = _clean_text(duplicate.get("identity_label")) or _clean_text(duplicate.get("identity_key"))
                reason = f"同一源邮件已在当前 run 命中（{identity_label}），已跳过重复上传候选。"
                source_dedup_skipped_rows.append(
                    {
                        "skip_reasons": [reason],
                        "row": _build_skipped_row_from_keep_record(
                            keep_row,
                            owner_context=row_owner_context,
                            reason=reason,
                            shared_mail_db_path=resolved_mail_db_path,
                            shared_mail_raw_dir=resolved_mail_raw_dir,
                            shared_mail_data_dir=resolved_mail_data_dir,
                            keep_workbook=keep_workbook,
                        ),
                    }
                )
                mail_only_candidate_log_rows.append(
                    _build_mail_only_candidate_log_entry(
                        task_name=task_name,
                        keep_row=keep_row,
                        owner_context=row_owner_context,
                        candidate=candidate,
                        route_decision="skipped_source_identity_duplicate",
                        reason=reason,
                        reply_resolution=dict(candidate.get("reply_resolution") or {}),
                        thread_assignment_resolution=dict(candidate.get("thread_assignment_resolution") or {}),
                    )
                )

            known_thread_routing = process_known_thread_updates(
                prepared_candidates,
                existing_index=existing_index,
                owner_scope_enabled=owner_scope_enabled,
            )
            known_thread_stats = dict(known_thread_routing.get("stats") or {})
            existing_screened_count = int(known_thread_stats.get("existing_screened_count") or 0)
            existing_unscreened_count = int(known_thread_stats.get("existing_unscreened_count") or 0)
            new_creator_count = int(known_thread_stats.get("new_creator_count") or 0)
            mail_only_bucket_summary["mail_only_routed_mail_only_candidate_count"] = len(
                list(known_thread_routing.get("mail_only_candidates") or [])
            )
            mail_only_bucket_summary["mail_only_routed_full_screening_candidate_count"] = len(
                list(known_thread_routing.get("full_screening_candidates") or [])
            )

            for candidate in known_thread_routing.get("mail_only_candidates") or []:
                keep_row = dict((candidate or {}).get("keep_row") or {})
                row_owner_context = dict((candidate or {}).get("owner_context") or {})
                existing_record = dict(((candidate or {}).get("existing_record") or {}))
                mail_only_candidate_log_rows.append(
                    _build_mail_only_candidate_log_entry(
                        task_name=task_name,
                        keep_row=keep_row,
                        owner_context=row_owner_context,
                        candidate=dict(candidate or {}),
                        route_decision="mail_only_candidate",
                        existing_record=existing_record,
                        reply_resolution=dict((candidate or {}).get("reply_resolution") or {}),
                        thread_assignment_resolution=dict((candidate or {}).get("thread_assignment_resolution") or {}),
                        next_update_mode=MAIL_ONLY_UPDATE_MODE if not bool(mail_first_only) else CREATE_OR_MAIL_ONLY_UPDATE_MODE,
                    )
                )
                display_row, payload_row = _build_mail_only_rows(
                    keep_row=keep_row,
                    existing_fields=dict(existing_record.get("fields") or {}),
                    owner_context=row_owner_context,
                    linked_bitable_url=linked_bitable_url,
                    shared_mail_db_path=resolved_mail_db_path,
                    shared_mail_raw_dir=resolved_mail_raw_dir,
                    shared_mail_data_dir=resolved_mail_data_dir,
                    keep_workbook=keep_workbook,
                    update_mode=MAIL_ONLY_UPDATE_MODE,
                    preserve_existing_ai_fields=True,
                )
                mail_only_display_rows.append(display_row)
                mail_only_payload_rows.append(payload_row)

            full_screening_rows = [
                dict((candidate or {}).get("keep_row") or {})
                for candidate in (known_thread_routing.get("full_screening_candidates") or [])
            ]
            for candidate in known_thread_routing.get("full_screening_candidates") or []:
                keep_row = dict((candidate or {}).get("keep_row") or {})
                row_owner_context = dict((candidate or {}).get("owner_context") or {})
                existing_record = dict(((candidate or {}).get("existing_record") or {}))
                mail_only_candidate_log_rows.append(
                    _build_mail_only_candidate_log_entry(
                        task_name=task_name,
                        keep_row=keep_row,
                        owner_context=row_owner_context,
                        candidate=dict(candidate or {}),
                        route_decision="full_screening_candidate",
                        existing_record=existing_record,
                        reply_resolution=dict((candidate or {}).get("reply_resolution") or {}),
                        thread_assignment_resolution=dict((candidate or {}).get("thread_assignment_resolution") or {}),
                        next_update_mode=CREATE_OR_MAIL_ONLY_UPDATE_MODE if bool(mail_first_only) else "",
                    )
                )

            full_screening_frame = pd.DataFrame(full_screening_rows, columns=list(keep_frame.columns))
            full_screening_display_rows: list[dict[str, Any]] = []
            full_screening_payload_rows: list[dict[str, Any]] = []
            mail_first_only_display_rows: list[dict[str, Any]] = []
            mail_first_only_payload_rows: list[dict[str, Any]] = []
            downstream_summary_json = ""
            if bool(mail_first_only):
                routed_candidates = [
                    *list(known_thread_routing.get("mail_only_candidates") or []),
                    *list(known_thread_routing.get("full_screening_candidates") or []),
                ]
                for candidate in routed_candidates:
                    keep_row = dict((candidate or {}).get("keep_row") or {})
                    row_owner_context = dict((candidate or {}).get("owner_context") or {})
                    existing_record = dict(((candidate or {}).get("existing_record") or {}))
                    display_row, payload_row = _build_mail_only_rows(
                        keep_row=keep_row,
                        existing_fields=dict(existing_record.get("fields") or {}),
                        owner_context=row_owner_context,
                        linked_bitable_url=linked_bitable_url,
                        shared_mail_db_path=resolved_mail_db_path,
                        shared_mail_raw_dir=resolved_mail_raw_dir,
                        shared_mail_data_dir=resolved_mail_data_dir,
                        keep_workbook=keep_workbook,
                        update_mode=CREATE_OR_MAIL_ONLY_UPDATE_MODE,
                        preserve_existing_ai_fields=False,
                    )
                    mail_first_only_display_rows.append(display_row)
                    mail_first_only_payload_rows.append(payload_row)
            elif len(full_screening_frame.index) > 0:
                filtered_keep_workbook = task_root / "partition" / f"{task_slug}_full_screening_keep.xlsx"
                _write_keep_subset(filtered_keep_workbook, full_screening_frame)
                downstream_output_root = task_root / "downstream"
                downstream_summary_path = downstream_output_root / "summary.json"
                _emit_runtime_progress(task_scope, f"downstream=running row_count={len(full_screening_frame.index)}")
                downstream_summary = run_keep_list_screening_pipeline(
                    keep_workbook=filtered_keep_workbook,
                    template_workbook=Path(template_workbook_value).expanduser() if template_workbook_value else None,
                    task_name=task_name,
                    task_upload_url=resolved_task_upload_url,
                    env_file=env_file,
                    output_root=downstream_output_root,
                    summary_json=downstream_summary_path,
                    platform_filters=platform_filters,
                    vision_provider=vision_provider,
                    positioning_provider=positioning_provider,
                    max_identifiers_per_platform=max(0, int(max_identifiers_per_platform)),
                    poll_interval=max(1.0, float(poll_interval)),
                    skip_scrape=bool(skip_scrape),
                    skip_visual=bool(skip_visual),
                    skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
                    task_owner_name=_clean_text(owner_context.get("responsible_name")),
                    task_owner_employee_id=_clean_text(owner_context.get("employee_id")),
                    task_owner_employee_record_id=_clean_text(owner_context.get("employee_record_id")),
                    task_owner_employee_email=_clean_text(owner_context.get("employee_email")),
                    task_owner_owner_name=_clean_text(owner_context.get("owner_name")),
                    linked_bitable_url=linked_bitable_url,
                )
                downstream_summary_json = str(downstream_summary_path)
                task_result["downstream_summary_json"] = downstream_summary_json
                downstream_status = str(downstream_summary.get("status") or "")
                _emit_runtime_progress(task_scope, f"downstream=completed status={downstream_status or 'unknown'}")
                if downstream_status in SUCCESSFUL_DOWNSTREAM_STATUSES:
                    final_review_path = Path(
                        str((downstream_summary.get("artifacts") or {}).get("all_platforms_final_review") or "")
                    ).expanduser()
                    payload_path = Path(
                        str((downstream_summary.get("artifacts") or {}).get("all_platforms_upload_payload_json") or "")
                    ).expanduser()
                    if final_review_path.exists():
                        full_screening_display_rows = _apply_row_owner_overrides(
                            pd.read_excel(final_review_path).to_dict(orient="records"),
                            keep_frame=full_screening_frame,
                            fallback_owner_context=owner_context,
                            shared_mail_db_path=resolved_mail_db_path,
                            shared_mail_raw_dir=resolved_mail_raw_dir,
                            shared_mail_data_dir=resolved_mail_data_dir,
                            keep_workbook=filtered_keep_workbook,
                        )
                    if payload_path.exists():
                        downstream_payload = json.loads(payload_path.read_text(encoding="utf-8"))
                        overridden_payload_rows = _apply_row_owner_overrides(
                            [dict(row) for row in list(downstream_payload.get("rows") or []) if isinstance(row, dict)],
                            keep_frame=full_screening_frame,
                            fallback_owner_context=owner_context,
                            shared_mail_db_path=resolved_mail_db_path,
                            shared_mail_raw_dir=resolved_mail_raw_dir,
                            shared_mail_data_dir=resolved_mail_data_dir,
                            keep_workbook=filtered_keep_workbook,
                        )
                        for row in overridden_payload_rows:
                            if isinstance(row, dict):
                                annotated_row = dict(row)
                                annotated_row["__feishu_update_mode"] = CREATE_OR_UPDATE_MODE
                                full_screening_payload_rows.append(annotated_row)
                        combined_skipped_rows.extend(list(downstream_payload.get("skipped_rows") or []))
                else:
                    failure_reason = str(
                        downstream_summary.get("error")
                        or f"{task_name} 下游 full-screening 失败，状态为 {downstream_status or 'unknown'}"
                    )
                    for record in full_screening_frame.to_dict(orient="records"):
                        row_owner_context = _build_owner_context_from_keep_row(record, owner_context)
                        combined_skipped_rows.append(
                            {
                                "skip_reasons": [failure_reason],
                                "row": _build_skipped_row_from_keep_record(
                                    record,
                                    owner_context=row_owner_context,
                                    reason=failure_reason,
                                    shared_mail_db_path=resolved_mail_db_path,
                                    shared_mail_raw_dir=resolved_mail_raw_dir,
                                    shared_mail_data_dir=resolved_mail_data_dir,
                                    keep_workbook=filtered_keep_workbook,
                                ),
                            }
                        )
                    any_task_failed = True

            if bool(mail_first_only):
                combined_display_rows = mail_first_only_display_rows
                combined_payload_rows = mail_first_only_payload_rows
            else:
                combined_display_rows = [*mail_only_display_rows, *full_screening_display_rows]
                combined_payload_rows = [*mail_only_payload_rows, *full_screening_payload_rows]
            combined_display_rows, combined_payload_rows, rescue_stats = _apply_conservative_rescue_layer(
                display_rows=combined_display_rows,
                payload_rows=combined_payload_rows,
                source_keep_rows=combined_prepared_rows,
                task_name=task_name,
                task_scope=task_scope,
            )
            exports_dir = task_root / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            combined_workbook_path = exports_dir / "all_platforms_final_review.xlsx"
            _write_combined_workbook(combined_workbook_path, combined_display_rows)
            combined_payload_artifacts = _combine_payloads(
                workbook_path=combined_workbook_path,
                owner_context=owner_context,
                display_rows=combined_display_rows,
                payload_rows=combined_payload_rows,
                skipped_rows=combined_skipped_rows,
            )
            _emit_runtime_progress(
                task_scope,
                f"feishu_upload=running row_count={len(combined_payload_rows)} skipped={len(combined_skipped_rows)}",
            )
            upload_summary = upload_final_review_payload_to_bitable(
                client,
                payload_json_path=combined_payload_artifacts["payload_json_path"],
                linked_bitable_url=linked_bitable_url,
                dry_run=bool(upload_dry_run),
                suppress_ai_labels=True,
            )
            candidate_log_json, candidate_log_xlsx = _write_rows_archive(
                exports_dir,
                stem="mail_only_candidate_log",
                rows=mail_only_candidate_log_rows,
                sheet_name="mail_only_candidate_log",
            )
            upload_decision_rows = [
                dict(row)
                for row in list(upload_summary.get("mail_only_upload_decision_rows") or [])
                if isinstance(row, dict)
            ]
            upload_decision_json, upload_decision_xlsx = _write_rows_archive(
                exports_dir,
                stem="mail_only_upload_decisions",
                rows=upload_decision_rows,
                sheet_name="mail_only_upload_decisions",
            )
            mail_only_bucket_summary["mail_only_upload_decision_count"] = int(
                upload_summary.get("mail_only_upload_decision_count") or 0
            )
            mail_only_bucket_summary["mail_only_upload_live_existing_hit_count"] = int(
                upload_summary.get("mail_only_upload_live_existing_hit_count") or 0
            )
            mail_only_bucket_summary["mail_only_upload_mail_idempotency_skip_count"] = int(
                upload_summary.get("mail_only_upload_mail_idempotency_skip_count") or 0
            )

            task_failed_count = int(combined_payload_artifacts["payload"]["skipped_row_count"]) + int(
                upload_summary.get("failed_count") or 0
            )
            skipped_existing_count = int(upload_summary.get("skipped_existing_count") or 0)
            task_result.update(
                {
                    "matched_mail_count": matched_mail_count,
                    "pre_keep_mail_only_count": pre_keep_mail_only_count,
                    "partial_refresh_count": 0,
                    "full_screening_count": len(full_screening_frame.index),
                    "mail_only_update_count": len(mail_only_payload_rows),
                    "mail_first_only_count": len(mail_first_only_payload_rows),
                    "written_row_count": len(combined_payload_rows),
                    "skipped_existing_count": skipped_existing_count,
                    "created_count": int(upload_summary.get("created_count") or 0),
                    "updated_count": int(upload_summary.get("updated_count") or 0),
                    "failed_count": task_failed_count,
                    "all_platforms_final_review": str(combined_workbook_path),
                    "all_platforms_upload_payload_json": str(combined_payload_artifacts["payload_json_path"]),
                    "feishu_upload_result_json": str(upload_summary.get("result_json_path") or ""),
                    "mail_only_candidate_log_json": candidate_log_json,
                    "mail_only_candidate_log_xlsx": candidate_log_xlsx,
                    "mail_only_upload_decision_json": upload_decision_json,
                    "mail_only_upload_decision_xlsx": upload_decision_xlsx,
                    "local_archive_path": str(combined_payload_artifacts["archive_dir"]),
                    "status": "completed_with_failures" if task_failed_count > 0 else "completed",
                    "new_creator_count": new_creator_count,
                    "existing_screened_count": existing_screened_count,
                    "existing_unscreened_count": existing_unscreened_count,
                    "known_thread_hit_count": int(known_thread_stats.get("known_thread_hit_count") or 0),
                    "thread_assignment_cache_hit_count": int(
                        known_thread_stats.get("thread_assignment_cache_hit_count") or 0
                    ),
                    "source_identity_deduped_count": source_identity_deduped_count,
                    **mail_only_bucket_summary,
                    **rescue_stats,
                }
            )
            if task_failed_count > 0:
                any_task_failed = True
            for skipped in combined_skipped_rows:
                aggregate_failed_rows.append(
                    {
                        "task_name": task_name,
                        "stage": "pre_upload_validation",
                        "reason": "；".join(str(reason).strip() for reason in (skipped.get("skip_reasons") or []) if str(reason).strip()),
                        "row": dict(skipped.get("row") or {}),
                    }
                )
            for failed in list(upload_summary.get("failed_rows") or []):
                aggregate_failed_rows.append(
                    {
                        "task_name": task_name,
                        "stage": "feishu_upload",
                        "reason": _clean_text(failed.get("error")),
                        "row": dict(failed.get("row") or {}),
                    }
                )
            for skipped in list(upload_summary.get("skipped_existing_rows") or []):
                aggregate_existing_skip_rows.append(
                    {
                        "task_name": task_name,
                        "stage": "feishu_upload",
                        "reason": _clean_text(skipped.get("reason")),
                        "row": dict(skipped.get("row") or {}),
                    }
                )
            for skipped in source_dedup_skipped_rows:
                aggregate_existing_skip_rows.append(
                    {
                        "task_name": task_name,
                        "stage": "source_identity_dedup",
                        "reason": "；".join(
                            str(reason).strip()
                            for reason in (skipped.get("skip_reasons") or [])
                            if str(reason).strip()
                        ),
                        "row": dict(skipped.get("row") or {}),
                    }
                )
            _emit_runtime_progress(
                task_scope,
                "feishu_upload=completed "
                f"created={int(upload_summary.get('created_count') or 0)} "
                f"updated={int(upload_summary.get('updated_count') or 0)} "
                f"failed={int(upload_summary.get('failed_count') or 0)}",
            )

            summary["matched_mail_count"] += int(matched_mail_count)
            summary["pre_keep_mail_only_count"] += int(pre_keep_mail_only_count)
            summary["partial_refresh_count"] += 0
            summary["new_creator_count"] += int(new_creator_count)
            summary["existing_screened_count"] += int(existing_screened_count)
            summary["existing_unscreened_count"] += int(existing_unscreened_count)
            summary["known_thread_hit_count"] += int(known_thread_stats.get("known_thread_hit_count") or 0)
            summary["thread_assignment_cache_hit_count"] += int(
                known_thread_stats.get("thread_assignment_cache_hit_count") or 0
            )
            summary["source_identity_deduped_count"] += int(source_identity_deduped_count)
            summary["full_screening_count"] += int(len(full_screening_frame.index))
            summary["mail_only_update_count"] += int(len(mail_only_payload_rows))
            summary["mail_first_only_count"] += int(len(mail_first_only_payload_rows))
            summary["keyword_hit_thread_count"] += int(task_result.get("keyword_hit_thread_count") or 0)
            summary["resolved_thread_count"] += int(task_result.get("resolved_thread_count") or 0)
            summary["written_row_count"] += int(len(combined_payload_rows))
            summary["skipped_existing_count"] += skipped_existing_count
            summary["created_record_count"] += int(upload_summary.get("created_count") or 0)
            summary["updated_record_count"] += int(upload_summary.get("updated_count") or 0)
            summary["failed_record_count"] += int(task_failed_count)
            for key in _MAIL_ONLY_BUCKET_SUMMARY_KEYS:
                summary[key] += int(task_result.get(key) or 0)
            for key in _RESCUE_SUMMARY_KEYS:
                summary[key] += int(task_result.get(key) or 0)
        except Exception as exc:  # noqa: BLE001
            task_result["status"] = "failed"
            task_result["failed_count"] = max(1, int(task_result.get("failed_count") or 0))
            task_result["error"] = str(exc) or exc.__class__.__name__
            summary["failed_record_count"] += task_result["failed_count"]
            any_task_failed = True
            aggregate_failed_rows.append(
                {
                    "task_name": task_name,
                    "stage": "task_runtime",
                    "reason": str(exc) or exc.__class__.__name__,
                    "row": {},
                }
            )
        summary["task_results"].append(task_result)
        _write_json(task_summary_path, task_result)
        _write_json(run_summary_path, summary)
        _emit_runtime_progress(
            task_scope,
            f"task=completed status={str(task_result.get('status') or '').strip() or 'unknown'} "
            f"failed={int(task_result.get('failed_count') or 0)}",
        )

    aggregate_json_path = aggregate_archive_dir / "failed_or_skipped_records.json"
    aggregate_xlsx_path = aggregate_archive_dir / "failed_or_skipped_records.xlsx"
    aggregate_existing_skip_json_path = aggregate_archive_dir / "existing_record_skips.json"
    aggregate_existing_skip_xlsx_path = aggregate_archive_dir / "existing_record_skips.xlsx"
    aggregate_json_path.write_text(
        json.dumps(
            {
                "failed_record_count": len(aggregate_failed_rows),
                "failed_or_skipped_rows": aggregate_failed_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    flattened_rows: list[dict[str, Any]] = []
    for item in aggregate_failed_rows:
        row = dict(item.get("row") or {})
        row["任务名"] = _clean_text(item.get("task_name"))
        row["失败阶段"] = _clean_text(item.get("stage"))
        row["本地归档原因"] = _clean_text(item.get("reason"))
        flattened_rows.append(row)
    aggregate_columns = ("任务名", "失败阶段", "本地归档原因", *FINAL_UPLOAD_COLUMNS)
    aggregate_frame = pd.DataFrame(flattened_rows, columns=aggregate_columns)
    with pd.ExcelWriter(aggregate_xlsx_path, engine="openpyxl") as writer:
        aggregate_frame.to_excel(writer, index=False, sheet_name="failed_or_skipped")

    aggregate_existing_skip_json_path.write_text(
        json.dumps(
            {
                "skipped_existing_count": len(aggregate_existing_skip_rows),
                "skipped_existing_rows": aggregate_existing_skip_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    flattened_existing_skip_rows: list[dict[str, Any]] = []
    for item in aggregate_existing_skip_rows:
        row = dict(item.get("row") or {})
        row["任务名"] = _clean_text(item.get("task_name"))
        row["跳过阶段"] = _clean_text(item.get("stage"))
        row["跳过原因"] = _clean_text(item.get("reason"))
        flattened_existing_skip_rows.append(row)
    existing_skip_columns = ("任务名", "跳过阶段", "跳过原因", *FINAL_UPLOAD_COLUMNS)
    existing_skip_frame = pd.DataFrame(flattened_existing_skip_rows, columns=existing_skip_columns)
    with pd.ExcelWriter(aggregate_existing_skip_xlsx_path, engine="openpyxl") as writer:
        existing_skip_frame.to_excel(writer, index=False, sheet_name="existing_skips")

    summary["finished_at"] = iso_now()
    summary["status"] = "completed_with_failures" if any_task_failed else "completed"
    summary["aggregate_archive_json"] = str(aggregate_json_path)
    summary["aggregate_archive_xlsx"] = str(aggregate_xlsx_path)
    summary["aggregate_existing_skip_json"] = str(aggregate_existing_skip_json_path)
    summary["aggregate_existing_skip_xlsx"] = str(aggregate_existing_skip_xlsx_path)
    _write_json(run_summary_path, summary)
    _emit_runtime_progress(progress_scope, f"run=completed status={summary['status']}")
    return summary


def rewrite_existing_final_payload_from_shared_mailbox(
    *,
    shared_mail_db_path: Path,
    existing_final_payload_json: Path,
    env_file: str = ".env",
    task_upload_url: str = "",
    employee_info_url: str = "",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    timeout_seconds: float = 0.0,
    upload_dry_run: bool = False,
) -> dict[str, Any]:
    runtime = _load_runtime_dependencies()
    upload_final_review_payload_to_bitable = runtime["upload_final_review_payload_to_bitable"]
    inspect_task_upload_assignments = runtime.get("inspect_task_upload_assignments")

    resolved_output_root = (output_root or (REPO_ROOT / "temp" / f"shared_mailbox_payload_rewrite_{datetime.now().strftime('%Y%m%d_%H%M%S')}")).expanduser().resolve()
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    resolved_summary_json = (summary_json or (resolved_output_root / "summary.json")).expanduser().resolve()
    exports_dir = resolved_output_root / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    payload_path = Path(existing_final_payload_json).expanduser().resolve()
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    rows = [dict(row) for row in (payload.get("rows") or []) if isinstance(row, dict)]

    client, env_values, diagnostics = _build_feishu_client(
        env_file=env_file,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
    )
    resolved_task_upload_url, _ = _resolve_cli_env_value(task_upload_url, env_values, "TASK_UPLOAD_URL")
    resolved_employee_info_url, _ = _resolve_cli_env_value(employee_info_url, env_values, "EMPLOYEE_INFO_URL")
    if not resolved_task_upload_url:
        resolved_task_upload_url, _ = _resolve_cli_env_value(task_upload_url, env_values, "FEISHU_SOURCE_URL")
    if not resolved_employee_info_url:
        resolved_employee_info_url, _ = _resolve_cli_env_value(employee_info_url, env_values, "FEISHU_SOURCE_URL")
    owner_candidates: list[dict[str, str]] = []
    if (
        callable(inspect_task_upload_assignments)
        and resolved_task_upload_url
        and resolved_employee_info_url
    ):
        try:
            inspection = inspect_task_upload_assignments(
                client=client,
                task_upload_url=resolved_task_upload_url,
                employee_info_url=resolved_employee_info_url,
                download_dir=resolved_output_root / "inspection_downloads",
                download_templates=False,
                parse_templates=False,
            )
            owner_candidates = _build_rewrite_owner_candidates(payload, inspection.get("items") or [])
        except Exception:  # noqa: BLE001
            owner_candidates = []

    fallback_owner_context = {
        "task_name": _clean_text((payload.get("task_owner") or {}).get("task_name")),
        "linked_bitable_url": _clean_text((payload.get("task_owner") or {}).get("linked_bitable_url")),
        "responsible_name": _clean_text((payload.get("task_owner") or {}).get("responsible_name")),
        "employee_name": _clean_text((payload.get("task_owner") or {}).get("employee_name")),
        "employee_english_name": _clean_text((payload.get("task_owner") or {}).get("employee_english_name")),
        "employee_id": _clean_text((payload.get("task_owner") or {}).get("employee_id")),
        "employee_record_id": _clean_text((payload.get("task_owner") or {}).get("employee_record_id")),
        "employee_email": _clean_text((payload.get("task_owner") or {}).get("employee_email")),
        "owner_name": _clean_text((payload.get("task_owner") or {}).get("owner_name")),
    }

    filtered_rows: list[dict[str, Any]] = []
    removed_rows: list[dict[str, Any]] = []
    corrected_reply_count = 0
    corrected_owner_count = 0
    unresolved_thread_count = 0
    for row in rows:
        updated_row, reply_resolution = _apply_creator_reply_context_to_export_row(
            row,
            shared_mail_db_path=shared_mail_db_path,
        )
        if reply_resolution.get("creator_replied") is False:
            removal = dict(updated_row)
            removal["移除原因"] = "仅命中负责人发信，达人未回复，已从最终写回结果中移除。"
            removed_rows.append(removal)
            continue
        if reply_resolution.get("creator_replied") is None:
            unresolved_thread_count += 1
        elif (
            _clean_text(updated_row.get("达人最后一次回复邮件时间")) != _clean_text(row.get("达人最后一次回复邮件时间"))
            or _clean_text(updated_row.get("达人回复的最后一封邮件内容")) != _clean_text(row.get("达人回复的最后一封邮件内容"))
            or _clean_text(updated_row.get("__last_mail_raw_path")) != _clean_text(row.get("__last_mail_raw_path"))
        ):
            corrected_reply_count += 1
        resolved_owner_context, _owner_resolution = _resolve_export_row_owner_context(
            updated_row,
            fallback_owner_context=fallback_owner_context,
            owner_candidates=owner_candidates,
            shared_mail_db_path=shared_mail_db_path,
        )
        owner_display_name = _clean_text(resolved_owner_context.get("responsible_name")) or _clean_text(
            resolved_owner_context.get("employee_name")
        )
        if (
            owner_display_name != _clean_text(updated_row.get("达人对接人"))
            or _clean_text(resolved_owner_context.get("employee_id")).split(",")[0].strip()
            != _clean_text(updated_row.get("达人对接人_employee_id")).split(",")[0].strip()
            or _clean_text(resolved_owner_context.get("employee_email"))
            != _clean_text(updated_row.get("达人对接人_employee_email"))
        ):
            corrected_owner_count += 1
        updated_row["达人对接人"] = owner_display_name
        updated_row[_KEEP_OWNER_ENGLISH_NAME_FIELD] = _clean_text(resolved_owner_context.get("employee_english_name"))
        updated_row["达人对接人_employee_id"] = _clean_text(resolved_owner_context.get("employee_id")).split(",")[0].strip()
        updated_row["达人对接人_employee_record_id"] = _clean_text(resolved_owner_context.get("employee_record_id"))
        updated_row["达人对接人_employee_email"] = _clean_text(resolved_owner_context.get("employee_email"))
        updated_row["达人对接人_owner_name"] = _clean_text(resolved_owner_context.get("owner_name"))
        updated_row["任务名"] = _clean_text(updated_row.get("任务名")) or _clean_text(resolved_owner_context.get("task_name"))
        updated_row["linked_bitable_url"] = _clean_text(updated_row.get("linked_bitable_url")) or _clean_text(
            resolved_owner_context.get("linked_bitable_url")
        )
        filtered_rows.append(updated_row)

    filtered_payload = dict(payload)
    filtered_payload["rows"] = filtered_rows
    filtered_payload["row_count"] = len(filtered_rows)
    filtered_payload["source_payload_json_path"] = str(payload_path)
    filtered_payload["reply_filter_applied_at"] = iso_now()

    filtered_payload_json_path = exports_dir / "all_platforms_final_review_payload.json"
    filtered_payload_json_path.write_text(json.dumps(filtered_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    filtered_workbook_path = exports_dir / "all_platforms_final_review.xlsx"
    filtered_frame = pd.DataFrame(filtered_rows, columns=FINAL_UPLOAD_COLUMNS)
    with pd.ExcelWriter(filtered_workbook_path, engine="openpyxl") as writer:
        filtered_frame.to_excel(writer, index=False, sheet_name="final_review")

    removed_json_path = exports_dir / "removed_no_reply_rows.json"
    removed_xlsx_path = exports_dir / "removed_no_reply_rows.xlsx"
    removed_json_path.write_text(
        json.dumps(
            {
                "removed_count": len(removed_rows),
                "removed_rows": removed_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    removed_frame = pd.DataFrame(removed_rows, columns=(*FINAL_UPLOAD_COLUMNS, "移除原因"))
    with pd.ExcelWriter(removed_xlsx_path, engine="openpyxl") as writer:
        removed_frame.to_excel(writer, index=False, sheet_name="removed_no_reply")

    upload_summary = upload_final_review_payload_to_bitable(
        client,
        payload_json_path=filtered_payload_json_path,
        dry_run=bool(upload_dry_run),
        suppress_ai_labels=True,
    )
    upload_failed = (not bool(upload_summary.get("ok", True))) or int(upload_summary.get("failed_count") or 0) > 0

    summary = {
        "started_at": iso_now(),
        "finished_at": iso_now(),
        "status": "completed_with_failures" if upload_failed else "completed",
        "mode": "rewrite_existing_final_payload",
        "shared_mail_db_path": str(Path(shared_mail_db_path).expanduser().resolve()),
        "source_payload_json_path": str(payload_path),
        "filtered_payload_json_path": str(filtered_payload_json_path),
        "filtered_workbook_path": str(filtered_workbook_path),
        "removed_json_path": str(removed_json_path),
        "removed_xlsx_path": str(removed_xlsx_path),
        "input_row_count": len(rows),
        "kept_row_count": len(filtered_rows),
        "removed_no_reply_count": len(removed_rows),
        "corrected_reply_count": corrected_reply_count,
        "corrected_owner_count": corrected_owner_count,
        "owner_candidate_count": len(owner_candidates),
        "unresolved_thread_count": unresolved_thread_count,
        "upload_failed": upload_failed,
        "upload_summary": upload_summary,
        "diagnostics": diagnostics,
    }
    _write_json(resolved_summary_json, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Route a pre-synced shared mailbox db into task-specific keep-list/downstream pipelines and incremental Feishu updates."
    )
    parser.add_argument("--shared-mail-db-path", required=True, help="共享邮箱已经同步好的 email_sync.db。")
    parser.add_argument("--existing-final-payload-json", default="", help="已完成链路的 all_platforms_final_review_payload.json；传入后只做 reply filter + 飞书重写。")
    parser.add_argument("--shared-mail-raw-dir", default="", help="共享邮箱 raw 邮件目录；默认推断为 email_sync.db 同级 raw。")
    parser.add_argument("--shared-mail-data-dir", default="", help="共享邮箱邮件数据根目录；默认推断为 email_sync.db 所在目录。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
    parser.add_argument("--sent-since", default="", help="共享邮箱上游起始日期 YYYY-MM-DD；默认沿用任务开始日期。")
    parser.add_argument("--task-upload-url", default="", help="飞书任务上传 wiki/base 链接。")
    parser.add_argument("--employee-info-url", default="", help="飞书员工信息表 wiki/base 链接。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/shared_mailbox_post_sync_<timestamp>。")
    parser.add_argument("--summary-json", default="", help="最终 summary.json 输出路径。")
    parser.add_argument("--task-name", action="append", help="只跑指定任务名，可重复传入。")
    parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id。")
    parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret。")
    parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL。")
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间；默认读取 .env 或 30 秒。")
    parser.add_argument("--owner-email-override", action="append", help="负责人邮箱覆盖，格式 MINISO:eden@amagency.biz。")
    parser.add_argument("--folder-prefix", action="append", help="任务邮箱目录前缀；共享邮箱模式默认 其他文件夹/邮件备份。")
    parser.add_argument(
        "--matching-strategy",
        default="brand-keyword-fast-path",
        choices=("legacy-enrichment", "brand-keyword-fast-path"),
        help="上游匹配策略。",
    )
    parser.add_argument("--brand-keyword", default="", help="fast path 品牌关键词；默认复用 task-name。")
    parser.add_argument(
        "--brand-match-include-from",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="品牌匹配时是否把发件人地址纳入候选；shared-mailbox 主线默认开启。",
    )
    parser.add_argument("--platform", action="append", help="只跑指定平台，可重复传入：tiktok / instagram / youtube。")
    parser.add_argument("--vision-provider", default="", help="指定视觉 provider。")
    parser.add_argument("--positioning-provider", default="", help="指定定位卡 provider。")
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过 scrape。")
    parser.add_argument("--skip-visual", action="store_true", help="跳过视觉复核。")
    parser.add_argument("--skip-positioning-card-analysis", action="store_true", help="跳过定位卡分析。")
    parser.add_argument(
        "--mail-first-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否只上传邮件字段；默认开启，不进入下游 scrape/visual/positioning，可用 --no-mail-first-only 恢复旧全链路。",
    )
    parser.add_argument(
        "--thread-first-mail-resolution",
        action="store_true",
        help="mail-first 专用：先按命中 thread 全量归属，再写飞书；未解析 thread 单独落人工清单。",
    )
    parser.add_argument("--upload-dry-run", action="store_true", help="只构建 payload，不真正写飞书。")
    parser.add_argument("--no-reuse-existing", action="store_true", help="不要复用当前 output-root 下已存在的 task artifact。")
    return parser


def _parse_mapping_overrides(values: Sequence[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in values or []:
        for item in str(chunk or "").split(","):
            normalized_item = item.strip()
            if not normalized_item or ":" not in normalized_item:
                continue
            key, value = normalized_item.split(":", 1)
            normalized_key = key.strip()
            normalized_value = value.strip()
            if normalized_key and normalized_value:
                result[normalized_key] = normalized_value
    return result


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.existing_final_payload_json:
        summary = rewrite_existing_final_payload_from_shared_mailbox(
            shared_mail_db_path=Path(args.shared_mail_db_path),
            existing_final_payload_json=Path(args.existing_final_payload_json),
            env_file=args.env_file,
            task_upload_url=args.task_upload_url or "",
            employee_info_url=args.employee_info_url or "",
            output_root=Path(args.output_root) if args.output_root else None,
            summary_json=Path(args.summary_json) if args.summary_json else None,
            feishu_app_id=args.feishu_app_id or "",
            feishu_app_secret=args.feishu_app_secret or "",
            feishu_base_url=args.feishu_base_url or "",
            timeout_seconds=float(args.timeout_seconds or 0.0),
            upload_dry_run=bool(args.upload_dry_run),
        )
    else:
        summary = run_shared_mailbox_post_sync_pipeline(
            shared_mail_db_path=Path(args.shared_mail_db_path),
            shared_mail_raw_dir=Path(args.shared_mail_raw_dir) if args.shared_mail_raw_dir else None,
            shared_mail_data_dir=Path(args.shared_mail_data_dir) if args.shared_mail_data_dir else None,
            env_file=args.env_file,
            sent_since=args.sent_since,
            task_upload_url=args.task_upload_url or "",
            employee_info_url=args.employee_info_url or "",
            output_root=Path(args.output_root) if args.output_root else None,
            summary_json=Path(args.summary_json) if args.summary_json else None,
            task_name_filters=args.task_name,
            feishu_app_id=args.feishu_app_id or "",
            feishu_app_secret=args.feishu_app_secret or "",
            feishu_base_url=args.feishu_base_url or "",
            timeout_seconds=float(args.timeout_seconds or 0.0),
            owner_email_overrides=_parse_mapping_overrides(args.owner_email_override),
            folder_prefixes=list(args.folder_prefix or []),
            matching_strategy=args.matching_strategy,
            brand_keyword=args.brand_keyword or "",
            brand_match_include_from=True if args.brand_match_include_from is None else bool(args.brand_match_include_from),
            platform_filters=args.platform,
            vision_provider=args.vision_provider or "",
            positioning_provider=args.positioning_provider or "",
            max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
            poll_interval=max(1.0, float(args.poll_interval)),
            skip_scrape=bool(args.skip_scrape),
            skip_visual=bool(args.skip_visual),
            skip_positioning_card_analysis=bool(args.skip_positioning_card_analysis),
            upload_dry_run=bool(args.upload_dry_run),
            reuse_existing=not bool(args.no_reuse_existing),
            mail_first_only=bool(args.mail_first_only),
            thread_first_mail_resolution=bool(args.thread_first_mail_resolution),
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
