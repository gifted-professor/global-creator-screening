from __future__ import annotations

from typing import Any, Mapping, Sequence


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def _build_record_key(*parts: Any) -> str:
    normalized_parts = [_clean_text(part).casefold() for part in parts]
    if any(not part for part in normalized_parts):
        return ""
    return "::".join(normalized_parts)


def _extract_ai_status(fields: Mapping[str, Any]) -> str:
    for candidate in ("ai 是否通过", "ai是否通过"):
        value = _flatten_field_value((fields or {}).get(candidate))
        if value:
            return value
    return ""


def process_known_thread_updates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    existing_index: Mapping[str, dict[str, Any]],
    owner_scope_enabled: bool,
) -> dict[str, Any]:
    mail_only_candidates: list[dict[str, Any]] = []
    full_screening_candidates: list[dict[str, Any]] = []
    stats = {
        "candidate_count": 0,
        "known_thread_hit_count": 0,
        "thread_assignment_cache_hit_count": 0,
        "mail_only_count": 0,
        "full_screening_count": 0,
        "existing_screened_count": 0,
        "existing_unscreened_count": 0,
        "new_creator_count": 0,
    }

    for raw_candidate in candidates:
        candidate = dict(raw_candidate or {})
        keep_row = dict(candidate.get("keep_row") or {})
        owner_scope = _clean_text(candidate.get("owner_scope"))
        creator_id = _clean_text(candidate.get("creator_id"))
        platform = _clean_text(candidate.get("platform"))
        record_key = (
            _build_record_key(owner_scope, creator_id, platform)
            if bool(owner_scope_enabled)
            else _build_record_key(creator_id, platform)
        )
        existing_record = dict((existing_index or {}).get(record_key) or {}) if record_key else None
        thread_key = _clean_text(candidate.get("thread_key")) or _clean_text(keep_row.get("evidence_thread_key"))
        thread_assignment_resolution = dict(candidate.get("thread_assignment_resolution") or {})

        stats["candidate_count"] += 1
        if thread_key:
            stats["known_thread_hit_count"] += 1
        if _clean_text(thread_assignment_resolution.get("status")) == "cache_hit":
            stats["thread_assignment_cache_hit_count"] += 1

        resolved_candidate = {
            **candidate,
            "keep_row": keep_row,
            "record_key": record_key,
            "existing_record": existing_record,
            "thread_key": thread_key,
        }
        if existing_record and _extract_ai_status(dict(existing_record.get("fields") or {})):
            mail_only_candidates.append(resolved_candidate)
            stats["mail_only_count"] += 1
            stats["existing_screened_count"] += 1
            continue
        full_screening_candidates.append(resolved_candidate)
        stats["full_screening_count"] += 1
        if existing_record:
            stats["existing_unscreened_count"] += 1
        else:
            stats["new_creator_count"] += 1

    return {
        "mail_only_candidates": mail_only_candidates,
        "full_screening_candidates": full_screening_candidates,
        "stats": stats,
    }
