from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import requests

from .creator_enrichment import (
    _clean_text,
    _extract_handles_from_text,
    _normalize_handle,
    _normalize_name,
    _stringify,
)
from .db import Database
from .llm_review import (
    RETRYABLE_STATUS_CODES,
    _extract_response_text,
    _iter_rows,
    _source_headers,
    _write_workbook,
    is_retryable_llm_transport_failure,
    resolve_llm_review_config_chain,
)


RESOLUTION_HEADERS = [
    "shared_group_key",
    "resolution_stage",
    "resolution_status",
    "resolution_keep",
    "resolution_method",
    "resolution_reason",
    "resolution_confidence",
    "resolution_candidate_score",
    "resolution_candidate_reasons",
    "resolution_selected_candidate_keys",
    "resolution_provider",
    "resolution_model",
    "resolution_wire_api",
    "resolution_reviewed_at",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _group_key(row: dict[str, Any]) -> str:
    matched_email = _stringify(row.get("matched_email"))
    if matched_email:
        return f"shared_email:{matched_email.lower()}"
    sheet_name = _stringify(row.get("sheet_name")) or "sheet"
    source_row = _stringify(row.get("source_row_number")) or "0"
    return f"fallback:{sheet_name}:{source_row}"


def _candidate_key(row: dict[str, Any]) -> str:
    profile_key = _stringify(row.get("profile_dedupe_key"))
    if profile_key:
        return profile_key
    platform = _stringify(row.get("Platform")).strip().lower() or "unknown"
    handle = (
        _normalize_handle(row.get("URL"))
        or _normalize_handle(row.get("@username"))
        or _normalize_name(row.get("nickname"))
    )
    if handle:
        return f"{platform}:{handle}"
    return _group_key(row)


def _fetch_message_snapshot(db: Database, message_row_id: int) -> dict[str, Any]:
    row = db.conn.execute(
        """
        SELECT id, subject, snippet, body_text, body_html, sent_at, raw_path
        FROM messages
        WHERE id = ?
        """,
        (int(message_row_id),),
    ).fetchone()
    if row is None:
        return {}
    return {
        "message_row_id": int(row["id"]),
        "subject": _stringify(row["subject"]),
        "snippet": _stringify(row["snippet"]),
        "body_text": _stringify(row["body_text"]),
        "body_html": _stringify(row["body_html"]),
        "sent_at": _stringify(row["sent_at"]),
        "raw_path": _stringify(row["raw_path"]),
    }


def _evidence_snapshot(db: Database, group_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    ranked_rows = sorted(
        group_rows,
        key=lambda row: (
            _stringify(row.get("brand_message_sent_at")),
            int(_stringify(row.get("brand_message_id")) or "0"),
        ),
        reverse=True,
    )
    best_row = ranked_rows[0]
    message_id = int(_stringify(best_row.get("brand_message_id")) or "0")
    snapshot = _fetch_message_snapshot(db, message_id) if message_id > 0 else {}
    return {
        "message_row_id": snapshot.get("message_row_id") or message_id,
        "sent_at": snapshot.get("sent_at") or _stringify(best_row.get("brand_message_sent_at")),
        "subject": snapshot.get("subject") or _stringify(best_row.get("brand_message_subject")),
        "snippet": snapshot.get("snippet") or _stringify(best_row.get("brand_message_snippet")),
        "body_text": snapshot.get("body_text") or "",
        "body_html": snapshot.get("body_html") or "",
        "raw_path": snapshot.get("raw_path") or _stringify(best_row.get("brand_message_raw_path")),
    }


def _build_evidence_text(snapshot: dict[str, Any]) -> str:
    return "\n".join(
        value
        for value in (
            _stringify(snapshot.get("subject")),
            _stringify(snapshot.get("snippet")),
            _stringify(snapshot.get("body_text")),
            _stringify(snapshot.get("body_html")),
        )
        if _stringify(value)
    )


def _score_candidate(row: dict[str, Any], evidence_text: str, extracted_handles: set[str]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    handle = _normalize_handle(row.get("URL")) or _normalize_handle(row.get("@username"))
    normalized_handle = _normalize_name(handle)
    nickname = _stringify(row.get("nickname"))
    normalized_name = _normalize_name(nickname)
    normalized_text = _normalize_name(evidence_text)

    if handle and handle in extracted_handles:
        score += 5
        reasons.append("handle_exact")
    elif normalized_handle and len(normalized_handle) >= 4 and normalized_handle in normalized_text:
        score += 3
        reasons.append("handle_text")

    if normalized_name and len(normalized_name) >= 5 and normalized_name in normalized_text:
        score += 2
        reasons.append("nickname_text")

    return score, reasons


def _annotated_row(
    row: dict[str, Any],
    *,
    group_key: str,
    stage: str,
    status: str,
    keep: bool,
    method: str,
    reason: str,
    confidence: str,
    score: int = 0,
    candidate_reasons: Sequence[str] = (),
    selected_candidate_keys: Sequence[str] = (),
    provider: str = "",
    model: str = "",
    wire_api: str = "",
) -> dict[str, Any]:
    annotated = dict(row)
    annotated["shared_group_key"] = group_key
    annotated["resolution_stage"] = stage
    annotated["resolution_status"] = status
    annotated["resolution_keep"] = 1 if keep else 0
    annotated["resolution_method"] = method
    annotated["resolution_reason"] = reason
    annotated["resolution_confidence"] = confidence
    annotated["resolution_candidate_score"] = score
    annotated["resolution_candidate_reasons"] = " | ".join(candidate_reasons)
    annotated["resolution_selected_candidate_keys"] = " | ".join(selected_candidate_keys)
    annotated["resolution_provider"] = provider
    annotated["resolution_model"] = model
    annotated["resolution_wire_api"] = wire_api
    annotated["resolution_reviewed_at"] = _utc_now()
    return annotated


def _ordered_headers(base_headers: Sequence[str]) -> list[str]:
    headers = list(base_headers)
    for header in RESOLUTION_HEADERS:
        if header not in headers:
            headers.append(header)
    return headers


def _write_jsonl(path: Path, records: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _load_rows_if_exists(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    if not path.exists():
        return [], []
    headers = _source_headers(path)
    return headers, list(_iter_rows(path, headers))


def _build_llm_candidate_record(group_key: str, group_rows: Sequence[dict[str, Any]], snapshot: dict[str, Any]) -> dict[str, Any]:
    ranked_rows = sorted(
        group_rows,
        key=lambda row: (
            int(_stringify(row.get("resolution_candidate_score")) or "0"),
            _stringify(row.get("brand_message_sent_at")),
            int(_stringify(row.get("brand_message_id")) or "0"),
        ),
        reverse=True,
    )
    return {
        "group_key": group_key,
        "shared_email": _stringify(group_rows[0].get("matched_email")),
        "candidate_keys": [_candidate_key(row) for row in group_rows],
        "instructions": [
            "Judge which creator row(s) this shared-email evidence belongs to.",
            "Only choose from candidate_rows.candidate_key values.",
            "Use match_one when exactly one candidate should remain.",
            "Use match_some when multiple rows should remain.",
            "Use match_all when every candidate in the group should remain.",
            "Use reject_group when none of the rows should remain tied to this evidence.",
            "Use uncertain when the evidence is too weak and the rows should stay for manual follow-up.",
            "Return one JSON object only.",
        ],
        "representative_message": {
            "message_row_id": snapshot.get("message_row_id"),
            "sent_at": snapshot.get("sent_at"),
            "subject": snapshot.get("subject"),
            "snippet": snapshot.get("snippet"),
            "body_text": snapshot.get("body_text"),
        },
        "candidate_rows": [
            {
                "candidate_key": _candidate_key(row),
                "platform": row.get("Platform"),
                "username": row.get("@username"),
                "url": row.get("URL"),
                "nickname": row.get("nickname"),
                "matched_email": row.get("matched_email"),
                "candidate_score": row.get("resolution_candidate_score"),
                "candidate_reasons": row.get("resolution_candidate_reasons"),
            }
            for row in ranked_rows
        ],
        "output_schema": {
            "decision": "match_one | match_some | match_all | reject_group | uncertain",
            "matched_candidate_keys": ["candidate_key values from candidate_rows"],
            "confidence": "high | medium | low",
            "reason": "brief justification",
        },
    }


def resolve_shared_email_candidates(
    *,
    db: Database,
    input_path: Path,
    output_prefix: Path,
) -> dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"找不到 shared-email workbook: {input_path}")

    db.init_schema()
    source_headers = _source_headers(input_path)
    input_rows = list(_iter_rows(input_path, source_headers))
    output_headers = _ordered_headers(source_headers)

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in input_rows:
        groups[_group_key(row)].append(row)

    resolved_rows: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []
    llm_candidates: list[dict[str, Any]] = []
    resolved_group_count = 0

    for group_key, group_rows in groups.items():
        snapshot = _evidence_snapshot(db, group_rows)
        evidence_text = _build_evidence_text(snapshot)
        extracted_handles = _extract_handles_from_text(evidence_text.lower())
        scored_rows: list[tuple[dict[str, Any], int, list[str]]] = []
        for row in group_rows:
            score, reasons = _score_candidate(row, evidence_text, extracted_handles)
            scored_rows.append((row, score, reasons))

        ranked_rows = sorted(
            scored_rows,
            key=lambda item: (
                item[1],
                _stringify(item[0].get("brand_message_sent_at")),
                int(_stringify(item[0].get("brand_message_id")) or "0"),
            ),
            reverse=True,
        )
        best_score = ranked_rows[0][1]
        second_score = ranked_rows[1][1] if len(ranked_rows) > 1 else -1
        best_reasons = ranked_rows[0][2]
        can_resolve = best_score >= 3 and best_score > second_score

        if can_resolve:
            resolved_group_count += 1
            winner_key = _candidate_key(ranked_rows[0][0])
            resolved_rows.append(
                _annotated_row(
                    ranked_rows[0][0],
                    group_key=group_key,
                    stage="content_rule",
                    status="resolved",
                    keep=True,
                    method="content_rule",
                    reason=f"content rule selected {winner_key}",
                    confidence="high" if best_score >= 5 else "medium",
                    score=best_score,
                    candidate_reasons=best_reasons,
                    selected_candidate_keys=[winner_key],
                )
            )
            continue

        for row, score, reasons in ranked_rows:
            unresolved_rows.append(
                _annotated_row(
                    row,
                    group_key=group_key,
                    stage="content_rule",
                    status="unresolved",
                    keep=False,
                    method="content_rule",
                    reason="content rule could not select a unique winner",
                    confidence="low",
                    score=score,
                    candidate_reasons=reasons,
                )
            )
        llm_candidates.append(_build_llm_candidate_record(group_key, unresolved_rows[-len(group_rows) :], snapshot))

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    resolved_path = output_prefix.with_name(f"{output_prefix.name}_resolved").with_suffix(".xlsx")
    unresolved_path = output_prefix.with_name(f"{output_prefix.name}_unresolved").with_suffix(".xlsx")
    candidates_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
    _write_workbook(resolved_path, output_headers, resolved_rows)
    _write_workbook(unresolved_path, output_headers, unresolved_rows)
    _write_jsonl(candidates_path, llm_candidates)

    return {
        "input_path": str(input_path),
        "resolved_xlsx_path": str(resolved_path),
        "unresolved_xlsx_path": str(unresolved_path),
        "llm_candidates_jsonl_path": str(candidates_path),
        "resolved_group_count": resolved_group_count,
        "resolved_row_count": len(resolved_rows),
        "unresolved_group_count": len(llm_candidates),
        "unresolved_row_count": len(unresolved_rows),
        "llm_candidate_group_count": len(llm_candidates),
    }


def _build_review_messages(group: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You adjudicate shared-email creator matches. "
                "Return JSON only. Use one of: match_one, match_some, match_all, reject_group, uncertain."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(group, ensure_ascii=False, indent=2),
        },
    ]


def _parse_review_response(raw_text: str, valid_keys: set[str]) -> dict[str, Any]:
    cleaned = str(raw_text or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    payload: dict[str, Any] | None = None
    try:
        maybe_payload = json.loads(cleaned)
        if isinstance(maybe_payload, dict):
            payload = maybe_payload
    except json.JSONDecodeError:
        start_idx = cleaned.find("{")
        end_idx = cleaned.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            try:
                maybe_payload = json.loads(cleaned[start_idx : end_idx + 1])
                if isinstance(maybe_payload, dict):
                    payload = maybe_payload
            except json.JSONDecodeError:
                payload = None

    if payload is None:
        return {
            "decision": "uncertain",
            "matched_candidate_keys": [],
            "confidence": "low",
            "reason": cleaned or "模型未返回可解析 JSON",
            "raw_text": raw_text,
        }

    decision = _stringify(payload.get("decision")).lower()
    if decision not in {"match_one", "match_some", "match_all", "reject_group", "uncertain"}:
        decision = "uncertain"

    selected = payload.get("matched_candidate_keys")
    if not isinstance(selected, list):
        selected = []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in selected:
        candidate_key = _stringify(item)
        if not candidate_key or candidate_key not in valid_keys or candidate_key in seen:
            continue
        seen.add(candidate_key)
        normalized.append(candidate_key)

    if decision == "match_all":
        normalized = sorted(valid_keys)
    elif decision == "match_one":
        if len(normalized) != 1:
            decision = "uncertain"
            normalized = []
    elif decision == "match_some":
        if not normalized:
            decision = "uncertain"
    else:
        normalized = []

    confidence = _stringify(payload.get("confidence")).lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "medium" if decision in {"match_one", "match_some", "match_all"} else "low"

    return {
        "decision": decision,
        "matched_candidate_keys": normalized,
        "confidence": confidence,
        "reason": _stringify(payload.get("reason")) or cleaned or "模型未返回原因",
        "raw_text": raw_text,
    }


def _invoke_shared_email_llm_review(config: Any, group: dict[str, Any]) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    messages = _build_review_messages(group)
    if config.wire_api == "responses":
        url = f"{config.base_url}/responses"
        body: dict[str, Any] = {
            "model": config.model,
            "input": [
                {
                    "role": message["role"],
                    "content": [{"type": "input_text", "text": message["content"]}],
                }
                for message in messages
            ],
        }
        if config.reasoning_effort:
            body["reasoning"] = {"effort": config.reasoning_effort}
    else:
        url = f"{config.base_url}/chat/completions"
        body = {"model": config.model, "messages": messages, "temperature": 0}

    for attempt in range(1, 4):
        try:
            response = requests.post(url, headers=headers, json=body, timeout=config.timeout_seconds)
            if response.status_code >= 400:
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < 3:
                    time.sleep(min(4.0, 1.5 * attempt))
                    continue
                raise RuntimeError(f"LLM HTTP {response.status_code}: {response.text[:400]}")
            payload = response.json()
            parsed = _parse_review_response(
                _extract_response_text(payload),
                set(group.get("candidate_keys") or []),
            )
            parsed["provider_payload"] = payload
            return parsed
        except requests.exceptions.RequestException:
            if attempt < 3:
                time.sleep(min(4.0, 1.5 * attempt))
                continue
            raise
    raise RuntimeError("shared-email LLM review failed after retries")


def _invoke_shared_email_llm_review_with_fallback(configs: Sequence[Any], group: dict[str, Any]) -> tuple[dict[str, Any], Any, list[dict[str, Any]]]:
    failures: list[dict[str, Any]] = []
    last_error: Exception | None = None
    for config in configs:
        try:
            parsed = _invoke_shared_email_llm_review(config, group)
            return parsed, config, failures
        except Exception as exc:  # noqa: BLE001
            retryable = is_retryable_llm_transport_failure(exc)
            failure = {
                "candidate_stage": getattr(config, "candidate_stage", ""),
                "provider": getattr(config, "provider_name", ""),
                "model": getattr(config, "model", ""),
                "wire_api": getattr(config, "wire_api", ""),
                "error": str(exc),
                "retryable": retryable,
            }
            failures.append(failure)
            last_error = exc
            if not retryable:
                break
    raise RuntimeError(str(last_error) if last_error else "shared-email final review failed")


def _merge_headers(paths: Sequence[Path], extra_headers: Sequence[str]) -> list[str]:
    headers: list[str] = []
    seen: set[str] = set()
    for path in paths:
        source_headers, _ = _load_rows_if_exists(path)
        for header in source_headers:
            if header in seen:
                continue
            seen.add(header)
            headers.append(header)
    for header in extra_headers:
        if header in seen:
            continue
        seen.add(header)
        headers.append(header)
    return headers


def _dedupe_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    ordered: list[str] = []
    for row in rows:
        key = _candidate_key(row)
        if key not in deduped:
            ordered.append(key)
        deduped[key] = dict(row)
    return [deduped[key] for key in ordered]


def run_shared_email_final_review(
    *,
    input_prefix: Path,
    env_path: str,
    auto_keep_paths: Sequence[Path] | None = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    wire_api: Optional[str] = None,
) -> dict[str, Any]:
    unresolved_path = input_prefix.with_name(f"{input_prefix.name}_unresolved").with_suffix(".xlsx")
    candidates_path = input_prefix.with_name(f"{input_prefix.name}_llm_candidates").with_suffix(".jsonl")
    review_path = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
    llm_resolved_path = input_prefix.with_name(f"{input_prefix.name}_llm_resolved").with_suffix(".xlsx")
    manual_tail_path = input_prefix.with_name(f"{input_prefix.name}_manual_tail").with_suffix(".xlsx")
    final_keep_path = input_prefix.with_name(f"{input_prefix.name}_final_keep").with_suffix(".xlsx")

    source_headers, unresolved_rows = _load_rows_if_exists(unresolved_path)
    output_headers = _ordered_headers(source_headers)
    rows_by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in unresolved_rows:
        rows_by_group[_stringify(row.get("shared_group_key"))].append(row)
    candidate_records = _load_jsonl(candidates_path)

    review_records: list[dict[str, Any]] = []
    llm_resolved_rows: list[dict[str, Any]] = []
    manual_rows: list[dict[str, Any]] = []
    configs: list[Any] = []
    provider_attempt_stats: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    absorbed_failures: list[dict[str, Any]] = []
    if candidate_records:
        configs = resolve_llm_review_config_chain(
            env_path,
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
        )

    for record in candidate_records:
        parsed = {
            "decision": "uncertain",
            "matched_candidate_keys": [],
            "confidence": "low",
            "reason": "missing config",
            "raw_text": "",
        }
        config = None
        failures: list[dict[str, Any]] = []
        if configs:
            try:
                parsed, config, failures = _invoke_shared_email_llm_review_with_fallback(configs, record)
            except Exception as exc:  # noqa: BLE001
                retryable = is_retryable_llm_transport_failure(exc)
                failures.append(
                    {
                        "candidate_stage": getattr(configs[-1], "candidate_stage", "") if configs else "",
                        "provider": getattr(configs[-1], "provider_name", "") if configs else "",
                        "model": getattr(configs[-1], "model", "") if configs else "",
                        "wire_api": getattr(configs[-1], "wire_api", "") if configs else "",
                        "error": str(exc),
                        "retryable": retryable,
                    }
                )
                parsed = {
                    "decision": "uncertain",
                    "matched_candidate_keys": [],
                    "confidence": "low",
                    "reason": str(exc) or "shared-email final review failed",
                    "raw_text": "",
                }
        parsed["group_key"] = record.get("group_key")
        parsed["provider"] = getattr(config, "provider_name", "")
        parsed["model"] = getattr(config, "model", "")
        parsed["wire_api"] = getattr(config, "wire_api", "")
        parsed["candidate_stage"] = getattr(config, "candidate_stage", "")
        parsed["provider_attempts"] = failures
        parsed["reviewed_at"] = _utc_now()
        review_records.append(parsed)

        for failure in failures:
            key = (
                _stringify(failure.get("candidate_stage")),
                _stringify(failure.get("provider")),
                _stringify(failure.get("model")),
                _stringify(failure.get("wire_api")),
            )
            stat = provider_attempt_stats.setdefault(
                key,
                {
                    "candidate_stage": key[0],
                    "provider": key[1],
                    "model": key[2],
                    "wire_api": key[3],
                    "attempt_count": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "retryable_failure_count": 0,
                    "last_error": "",
                },
            )
            stat["attempt_count"] += 1
            stat["failure_count"] += 1
            if failure.get("retryable"):
                stat["retryable_failure_count"] += 1
            stat["last_error"] = _stringify(failure.get("error"))
            if config is not None:
                absorbed_failures.append(
                    {
                        **failure,
                        "recovered_by_provider": getattr(config, "provider_name", ""),
                        "recovered_by_model": getattr(config, "model", ""),
                    }
                )

        if config is not None:
            key = (
                _stringify(getattr(config, "candidate_stage", "")),
                _stringify(getattr(config, "provider_name", "")),
                _stringify(getattr(config, "model", "")),
                _stringify(getattr(config, "wire_api", "")),
            )
            stat = provider_attempt_stats.setdefault(
                key,
                {
                    "candidate_stage": key[0],
                    "provider": key[1],
                    "model": key[2],
                    "wire_api": key[3],
                    "attempt_count": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "retryable_failure_count": 0,
                    "last_error": "",
                },
            )
            stat["attempt_count"] += 1
            stat["success_count"] += 1

        group_key = _stringify(record.get("group_key"))
        group_rows = rows_by_group.get(group_key, [])
        selected_keys = set(parsed.get("matched_candidate_keys") or [])
        decision = parsed.get("decision")

        if decision in {"match_one", "match_some", "match_all"} and selected_keys:
            for row in group_rows:
                candidate_key = _candidate_key(row)
                if candidate_key not in selected_keys:
                    continue
                llm_resolved_rows.append(
                    _annotated_row(
                        row,
                        group_key=group_key,
                        stage="llm",
                        status="resolved",
                        keep=True,
                        method=f"llm_{decision}",
                        reason=_stringify(parsed.get("reason")),
                        confidence=_stringify(parsed.get("confidence")) or "medium",
                        score=int(_stringify(row.get("resolution_candidate_score")) or "0"),
                        candidate_reasons=_stringify(row.get("resolution_candidate_reasons")).split(" | ")
                        if _stringify(row.get("resolution_candidate_reasons"))
                        else (),
                        selected_candidate_keys=sorted(selected_keys),
                        provider=_stringify(parsed.get("provider")),
                        model=_stringify(parsed.get("model")),
                        wire_api=_stringify(parsed.get("wire_api")),
                    )
                )
            continue

        for row in group_rows:
            manual_rows.append(
                _annotated_row(
                    row,
                    group_key=group_key,
                    stage="llm",
                    status="manual",
                    keep=False,
                    method=f"llm_{decision}",
                    reason=_stringify(parsed.get("reason")) or "LLM left the group for manual review",
                    confidence=_stringify(parsed.get("confidence")) or "low",
                    score=int(_stringify(row.get("resolution_candidate_score")) or "0"),
                    candidate_reasons=_stringify(row.get("resolution_candidate_reasons")).split(" | ")
                    if _stringify(row.get("resolution_candidate_reasons"))
                    else (),
                    selected_candidate_keys=sorted(selected_keys),
                    provider=_stringify(parsed.get("provider")),
                    model=_stringify(parsed.get("model")),
                    wire_api=_stringify(parsed.get("wire_api")),
                )
            )

    review_path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl(review_path, review_records)
    _write_workbook(llm_resolved_path, output_headers, llm_resolved_rows)
    _write_workbook(manual_tail_path, output_headers, manual_rows)

    keep_rows: list[dict[str, Any]] = []
    keep_paths = list(auto_keep_paths or [])
    for path in keep_paths:
        _, rows = _load_rows_if_exists(path)
        keep_rows.extend(rows)
    keep_rows.extend(llm_resolved_rows)
    final_headers = _merge_headers([*keep_paths, llm_resolved_path], RESOLUTION_HEADERS)
    deduped_keep_rows = _dedupe_rows(keep_rows)
    _write_workbook(final_keep_path, final_headers, deduped_keep_rows)

    selected_provider = ""
    selected_model = ""
    selected_wire_api = ""
    selected_candidates = sorted(
        (
            stat
            for stat in provider_attempt_stats.values()
            if int(stat.get("success_count") or 0) > 0
        ),
        key=lambda item: (
            -int(item.get("success_count") or 0),
            item.get("candidate_stage") != "primary",
            item.get("candidate_stage") != "secondary",
        ),
    )
    if selected_candidates:
        selected_provider = _stringify(selected_candidates[0].get("provider"))
        selected_model = _stringify(selected_candidates[0].get("model"))
        selected_wire_api = _stringify(selected_candidates[0].get("wire_api"))

    return {
        "input_prefix": str(input_prefix),
        "llm_candidates_jsonl_path": str(candidates_path),
        "llm_review_jsonl_path": str(review_path),
        "llm_resolved_xlsx_path": str(llm_resolved_path),
        "manual_tail_xlsx_path": str(manual_tail_path),
        "final_keep_xlsx_path": str(final_keep_path),
        "review_group_count": len(candidate_records),
        "llm_resolved_row_count": len(llm_resolved_rows),
        "manual_row_count": len(manual_rows),
        "final_keep_row_count": len(deduped_keep_rows),
        "provider_attempts": list(provider_attempt_stats.values()),
        "absorbed_failures": absorbed_failures,
        "retryable_failure_count": sum(int(item.get("retryable_failure_count") or 0) for item in provider_attempt_stats.values()),
        "selected_provider": selected_provider,
        "selected_model": selected_model,
        "selected_wire_api": selected_wire_api,
    }
