from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend import creator_cache
from harness.contract import attach_run_contract
from harness.config import resolve_keep_list_downstream_config
from harness.failures import attach_failure_to_summary, build_failure_payload as build_harness_failure_payload
from harness.handoff import write_workflow_handoff
from harness.paths import resolve_keep_list_downstream_paths
from harness.preflight import (
    build_preflight_error,
    build_preflight_payload,
    inspect_directory_materialization_target,
)
from harness.setup import materialize_setup
from harness.spec import build_keep_list_downstream_task_spec, write_task_spec


DEFAULT_KEEP_WORKBOOK = (
    REPO_ROOT / "exports" / "测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx"
)
DEFAULT_TEMPLATE_WORKBOOK = (
    REPO_ROOT
    / "downloads"
    / "task_upload_attachments"
    / "recveXGV2i3BS0"
    / "需求上传（excel 格式）"
    / "miniso-星战红人筛号需求模板(1).xlsx"
)
DEFAULT_PLATFORM_ORDER = ("tiktok", "instagram", "youtube")


def _load_runtime_dependencies():
    import backend.app as backend_app
    from backend.final_export_merge import build_all_platforms_final_review_artifacts, collect_final_exports
    from feishu_screening_bridge.bitable_upload import (
        fetch_existing_bitable_record_analysis,
        upload_final_review_payload_to_bitable,
    )
    from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
    from feishu_screening_bridge.local_env import get_preferred_value, load_local_env
    from scripts.prepare_screening_inputs import (
        prepare_screening_inputs,
        restore_backend_runtime_state,
        snapshot_backend_runtime_state,
    )
    from scripts.run_screening_smoke import (
        count_passed_profiles,
        export_platform_artifacts,
        poll_job,
        require_success,
        reset_backend_runtime_state,
    )

    return {
        "backend_app": backend_app,
        "build_all_platforms_final_review_artifacts": build_all_platforms_final_review_artifacts,
        "collect_final_exports": collect_final_exports,
        "DEFAULT_FEISHU_BASE_URL": DEFAULT_FEISHU_BASE_URL,
        "FeishuOpenClient": FeishuOpenClient,
        "get_preferred_value": get_preferred_value,
        "load_local_env": load_local_env,
        "prepare_screening_inputs": prepare_screening_inputs,
        "restore_backend_runtime_state": restore_backend_runtime_state,
        "snapshot_backend_runtime_state": snapshot_backend_runtime_state,
        "count_passed_profiles": count_passed_profiles,
        "export_platform_artifacts": export_platform_artifacts,
        "fetch_existing_bitable_record_analysis": fetch_existing_bitable_record_analysis,
        "poll_job": poll_job,
        "require_success": require_success,
        "reset_backend_runtime_state": reset_backend_runtime_state,
        "upload_final_review_payload_to_bitable": upload_final_review_payload_to_bitable,
    }


def _expand_platforms_for_fallback(requested_platforms: list[str]) -> list[str]:
    normalized_requested = [str(item or "").strip().lower() for item in (requested_platforms or []) if str(item or "").strip()]
    if not normalized_requested:
        return []
    expanded: list[str] = []
    seen: set[str] = set()
    for requested in normalized_requested:
        try:
            start_index = DEFAULT_PLATFORM_ORDER.index(requested)
        except ValueError:
            continue
        for candidate in DEFAULT_PLATFORM_ORDER[start_index:]:
            if candidate in seen:
                continue
            seen.add(candidate)
            expanded.append(candidate)
    return expanded


def default_output_root() -> Path:
    return resolve_keep_list_downstream_paths(task_name="task").run_root


def iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _compute_elapsed_seconds(started_at: Any, finished_at: Any) -> float | None:
    started = _parse_iso_datetime(started_at)
    finished = _parse_iso_datetime(finished_at)
    if started is None or finished is None:
        return None
    return max(0.0, round((finished - started).total_seconds(), 3))


def _path_exists(path_value: Any) -> bool:
    text = str(path_value or "").strip()
    if not text:
        return False
    return Path(text).expanduser().exists()


def _emit_runtime_progress(scope: str, message: str) -> None:
    print(f"[{iso_now()}] [{scope}] {message}", flush=True)


def _path_summary(path: Path | None, *, source: str, kind: str) -> dict[str, Any]:
    if path is None:
        return {
            "kind": kind,
            "path": "",
            "exists": False,
            "source": source,
        }
    expanded = path.expanduser()
    return {
        "kind": kind,
        "path": str(expanded.resolve()),
        "exists": expanded.exists(),
        "source": source,
    }


def _build_failure_payload(
    *,
    stage: str,
    error_code: str,
    message: str,
    remediation: str,
    failure_layer: str = "runtime",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_harness_failure_payload(
        stage=stage,
        error_code=error_code,
        message=message,
        remediation=remediation,
        failure_layer=failure_layer,
        details=details,
    )


def normalize_platforms(values: list[str] | None) -> list[str]:
    if not values:
        return list(DEFAULT_PLATFORM_ORDER)
    supported_platforms = set(DEFAULT_PLATFORM_ORDER)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        platform = str(value or "").strip().lower()
        if platform not in supported_platforms:
            raise ValueError(f"不支持的平台: {value}")
        if platform in seen:
            continue
        seen.add(platform)
        normalized.append(platform)
    return normalized


def _build_platform_scrape_identifier(backend_app, platform: str, raw_identifier: str, metadata: dict[str, Any] | None) -> str:
    normalized_metadata = dict(metadata or {})
    fallback_identifier = str(raw_identifier or "").strip()
    if platform in {"tiktok", "youtube"}:
        for candidate in (normalized_metadata.get("url"), normalized_metadata.get("profile_url")):
            value = str(candidate or "").strip()
            if value:
                return value
        screening_module = getattr(backend_app, "screening", None)
        if fallback_identifier and "://" not in fallback_identifier and hasattr(screening_module, "build_canonical_profile_url"):
            canonical = str(screening_module.build_canonical_profile_url(platform, fallback_identifier) or "").strip()
            if canonical:
                return canonical
    if platform == "instagram":
        for candidate in (normalized_metadata.get("handle"), fallback_identifier, normalized_metadata.get("url")):
            value = str(candidate or "").strip()
            if value:
                return value
    return fallback_identifier


def select_platform_identifiers(platform: str, max_identifiers_per_platform: int) -> list[str]:
    runtime = _load_runtime_dependencies()
    backend_app = runtime["backend_app"]
    metadata_lookup = backend_app.load_upload_metadata(platform)
    identifiers = []
    for identifier, metadata in metadata_lookup.items():
        selected = _build_platform_scrape_identifier(backend_app, platform, str(identifier or "").strip(), metadata)
        if selected:
            identifiers.append(selected)
    if max_identifiers_per_platform > 0:
        return identifiers[:max_identifiers_per_platform]
    return identifiers


def _build_casefold_record_key(*parts: Any) -> str:
    normalized_parts = [str(part or "").strip().casefold() for part in parts]
    if any(not part for part in normalized_parts):
        return ""
    return "::".join(normalized_parts)


def _resolve_staged_creator_id(backend_app, platform: str, metadata_key: str, metadata: dict[str, Any] | None) -> str:
    screening_module = getattr(backend_app, "screening", None)
    for candidate in (
        (metadata or {}).get("creator_id"),
        (metadata or {}).get("达人ID"),
        (metadata or {}).get("handle"),
        metadata_key,
    ):
        value = str(candidate or "").strip()
        if not value:
            continue
        if "://" in value and hasattr(screening_module, "extract_platform_identifier"):
            extracted = str(screening_module.extract_platform_identifier(platform, value) or "").strip()
            if extracted:
                return extracted
        if value.startswith("@"):
            value = value[1:]
        return value.rstrip("/")
    return ""


def _flatten_existing_field_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [_flatten_existing_field_value(item) for item in value]
        return "；".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("name", "text", "link", "value", "id"):
            candidate = str(value.get(key) or "").strip()
            if candidate:
                return candidate
        return ""
    return str(value).strip()


def _extract_existing_ai_status(fields: dict[str, Any]) -> str:
    normalized_candidates = {"ai是否通过", "ai 是否通过"}
    for key, value in (fields or {}).items():
        normalized_key = str(key or "").strip().casefold()
        if normalized_key in normalized_candidates or normalized_key.replace(" ", "") in {"ai是否通过"}:
            return _flatten_existing_field_value(value)
    return ""


def _extract_existing_field_text(fields: dict[str, Any], *field_names: str) -> str:
    normalized_candidates = {
        str(field_name or "").strip().casefold().replace(" ", "")
        for field_name in field_names
        if str(field_name or "").strip()
    }
    for key, value in (fields or {}).items():
        normalized_key = str(key or "").strip().casefold().replace(" ", "")
        if normalized_key in normalized_candidates:
            return _flatten_existing_field_value(value)
    return ""


def _existing_record_has_positioning_payload(fields: dict[str, Any]) -> bool:
    return bool(
        _extract_existing_field_text(fields, "标签(ai)", "标签（ai）", "ai评价", "ai 评价", "ai筛号反馈理由")
    )


def _normalize_cache_identifier(backend_app, platform: str, raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    screening_module = getattr(backend_app, "screening", None)
    if screening_module is not None and hasattr(screening_module, "extract_platform_identifier"):
        normalized = str(screening_module.extract_platform_identifier(platform, value) or "").strip()
        if normalized:
            return normalized
    return value.lstrip("@").rstrip("/").casefold()


def _resolve_visual_cache_context_key(backend_app, platform: str, *, vision_provider: str) -> str:
    context_builder = getattr(backend_app, "build_visual_review_cache_context", None)
    if not callable(context_builder):
        return ""
    routing_strategy = ""
    routing_resolver = getattr(backend_app, "resolve_visual_review_routing_strategy", None)
    if callable(routing_resolver):
        routing_strategy = str(routing_resolver({}) or "").strip()
    try:
        context = context_builder(
            platform,
            requested_provider=str(vision_provider or "").strip().lower(),
            routing_strategy=routing_strategy,
        )
    except Exception:
        return ""
    return str((context or {}).get("context_key") or "").strip()


def _append_unique_identifier(bucket: list[str], seen: set[str], raw_value: Any) -> None:
    value = str(raw_value or "").strip()
    if not value or value in seen:
        return
    seen.add(value)
    bucket.append(value)


def _merge_breakdown_counts(target: dict[str, int], source: dict[str, Any] | None) -> None:
    for key, value in dict(source or {}).items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        try:
            delta = max(0, int(value or 0))
        except (TypeError, ValueError):
            continue
        if delta <= 0:
            continue
        target[normalized_key] = int(target.get(normalized_key) or 0) + delta


def _build_platform_identifier_plan(
    backend_app,
    platform: str,
    *,
    max_identifiers_per_platform: int,
    existing_bitable_analysis: Any | None = None,
    task_owner_employee_id: str = "",
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
    vision_provider: str = "",
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
) -> dict[str, Any]:
    metadata_lookup = dict(backend_app.load_upload_metadata(platform) or {})
    key_field_names = tuple(getattr(existing_bitable_analysis, "key_field_names", ()) or ())
    owner_scope_field_name = str(getattr(existing_bitable_analysis, "owner_scope_field_name", "") or "").strip()
    existing_record_index = dict(getattr(existing_bitable_analysis, "index", {}) or {})
    duplicate_existing_group_count = len(list(getattr(existing_bitable_analysis, "duplicate_groups", []) or []))
    owner_scope_value = str(task_owner_employee_id or "").strip()

    planned_entries: list[dict[str, Any]] = []
    for metadata_key, raw_metadata in metadata_lookup.items():
        metadata = dict(raw_metadata or {})
        creator_id = _resolve_staged_creator_id(backend_app, platform, str(metadata_key or "").strip(), metadata)
        scrape_identifier = _build_platform_scrape_identifier(
            backend_app,
            platform,
            str(metadata_key or "").strip(),
            metadata,
        )
        planned_entries.append(
            {
                "creator_id": creator_id,
                "scrape_identifier": scrape_identifier,
                "profile_url": str(metadata.get("url") or metadata.get("profile_url") or "").strip(),
            }
        )

    new_entries: list[dict[str, Any]] = []
    existing_entries: list[dict[str, Any]] = []
    existing_screened_entries: list[dict[str, Any]] = []
    existing_unscreened_entries: list[dict[str, Any]] = []
    mail_only_update_entries: list[dict[str, Any]] = []
    partial_refresh_entries: list[dict[str, Any]] = []
    full_screening_entries: list[dict[str, Any]] = []
    partial_refresh_breakdown: dict[str, int] = {}
    resolved_creator_cache_db_path = creator_cache.resolve_creator_cache_db_path(
        {"creator_cache_db_path": str(creator_cache_db_path or "").strip()}
    )
    creator_cache_enabled = creator_cache.creator_cache_enabled(
        {"creator_cache_db_path": str(creator_cache_db_path or "").strip()}
    )
    scrape_cache_hits: dict[str, list[dict[str, Any]]] = {}
    visual_cache_hits: dict[str, dict[str, Any]] = {}
    visual_cache_context_key = ""
    normalized_planned_identifiers = [
        _normalize_cache_identifier(backend_app, platform, entry.get("scrape_identifier") or entry.get("creator_id"))
        for entry in planned_entries
    ]
    planned_scrape_identifiers = [
        str(entry.get("scrape_identifier") or "").strip()
        for entry in planned_entries
        if str(entry.get("scrape_identifier") or "").strip()
    ]
    if creator_cache_enabled and not force_refresh_creator_cache and resolved_creator_cache_db_path.exists():
        if planned_scrape_identifiers:
            scrape_cache_hits = creator_cache.load_scrape_cache_entries(
                platform,
                planned_scrape_identifiers,
                resolved_creator_cache_db_path,
            )
        visual_cache_context_key = _resolve_visual_cache_context_key(
            backend_app,
            platform,
            vision_provider=vision_provider,
        )
        if visual_cache_context_key and planned_scrape_identifiers:
            visual_cache_hits = creator_cache.load_visual_cache_entries(
                platform,
                planned_scrape_identifiers,
                resolved_creator_cache_db_path,
                visual_cache_context_key,
            )
    if existing_record_index:
        for entry in planned_entries:
            key_parts: list[str] = []
            for field_name in key_field_names or ("达人ID", "平台"):
                if field_name == "达人对接人":
                    key_parts.append(owner_scope_value)
                elif field_name == "达人ID":
                    key_parts.append(str(entry.get("creator_id") or "").strip())
                elif field_name == "平台":
                    key_parts.append(str(platform or "").strip())
                else:
                    key_parts.append("")
            record_key = _build_casefold_record_key(*key_parts)
            if record_key and record_key in existing_record_index:
                existing_record = dict(existing_record_index.get(record_key) or {})
                classified_entry = {
                    **entry,
                    "record_key": record_key,
                    "record_id": str(existing_record.get("record_id") or "").strip(),
                    "existing_fields": dict(existing_record.get("fields") or {}),
                }
                existing_entries.append(classified_entry)
                if _extract_existing_ai_status(classified_entry.get("existing_fields") or {}):
                    existing_screened_entries.append(classified_entry)
                    normalized_identifier = _normalize_cache_identifier(
                        backend_app,
                        platform,
                        classified_entry.get("scrape_identifier") or classified_entry.get("creator_id"),
                    )
                    scrape_cache_hit = bool(normalized_identifier and list(scrape_cache_hits.get(normalized_identifier) or []))
                    visual_cache_hit = bool(normalized_identifier and dict(visual_cache_hits.get(normalized_identifier) or {}))
                    positioning_ready = _existing_record_has_positioning_payload(
                        classified_entry.get("existing_fields") or {}
                    )
                    partial_reasons: list[str] = []
                    if creator_cache_enabled and not scrape_cache_hit:
                        partial_reasons.append(f"scrape_missing_{platform}")
                    if not skip_visual and creator_cache_enabled and visual_cache_context_key and not visual_cache_hit:
                        partial_reasons.append(f"visual_missing_{platform}")
                    if not skip_positioning_card_analysis and not positioning_ready:
                        partial_reasons.append(f"positioning_missing_{platform}")
                    if partial_reasons:
                        classified_entry["execution_mode"] = "partial_refresh"
                        classified_entry["partial_refresh_reasons"] = list(partial_reasons)
                        classified_entry["needs_scrape"] = not scrape_cache_hit
                        classified_entry["needs_visual"] = bool(
                            not skip_visual and creator_cache_enabled and visual_cache_context_key and not visual_cache_hit
                        )
                        classified_entry["needs_positioning"] = bool(
                            not skip_positioning_card_analysis and not positioning_ready
                        )
                        partial_refresh_entries.append(classified_entry)
                        for reason in partial_reasons:
                            partial_refresh_breakdown[reason] = int(partial_refresh_breakdown.get(reason) or 0) + 1
                    else:
                        classified_entry["execution_mode"] = "mail_only_update"
                        mail_only_update_entries.append(classified_entry)
                else:
                    classified_entry["execution_mode"] = "full_screening_existing"
                    existing_unscreened_entries.append(classified_entry)
                    full_screening_entries.append(classified_entry)
                continue
            classified_entry = {**entry, "record_key": record_key, "execution_mode": "full_screening_new"}
            new_entries.append(classified_entry)
            full_screening_entries.append(classified_entry)
    else:
        new_entries = [{**entry, "execution_mode": "full_screening_new"} for entry in planned_entries]
        full_screening_entries = list(new_entries)

    scrape_requested_identifiers: list[str] = []
    visual_requested_identifiers: list[str] = []
    positioning_requested_identifiers: list[str] = []
    scrape_seen: set[str] = set()
    visual_seen: set[str] = set()
    positioning_seen: set[str] = set()
    for entry in full_screening_entries:
        scrape_identifier = str(entry.get("scrape_identifier") or "").strip()
        _append_unique_identifier(scrape_requested_identifiers, scrape_seen, scrape_identifier)
        _append_unique_identifier(visual_requested_identifiers, visual_seen, scrape_identifier)
        _append_unique_identifier(positioning_requested_identifiers, positioning_seen, scrape_identifier)
    for entry in partial_refresh_entries:
        scrape_identifier = str(entry.get("scrape_identifier") or "").strip()
        if any(bool(entry.get(flag)) for flag in ("needs_scrape", "needs_visual", "needs_positioning")):
            _append_unique_identifier(scrape_requested_identifiers, scrape_seen, scrape_identifier)
        if any(bool(entry.get(flag)) for flag in ("needs_visual", "needs_positioning")):
            # Visual stage is also used to materialize cached review artifacts for positioning.
            _append_unique_identifier(visual_requested_identifiers, visual_seen, scrape_identifier)
        if bool(entry.get("needs_positioning")):
            _append_unique_identifier(positioning_requested_identifiers, positioning_seen, scrape_identifier)

    requested_entries = list(scrape_requested_identifiers)
    if max_identifiers_per_platform > 0:
        limit = int(max_identifiers_per_platform)
        requested_entries = requested_entries[:limit]
        visual_requested_identifiers = visual_requested_identifiers[:limit]
        positioning_requested_identifiers = positioning_requested_identifiers[:limit]

    incremental_prefilter = {
        "enabled": bool(existing_record_index),
        "status": "ready" if existing_record_index else "disabled",
        "key_field_names": list(key_field_names),
        "owner_scope_field_name": owner_scope_field_name,
        "existing_bitable_match_count": len(existing_entries),
        "existing_bitable_match_preview": [
            str(item.get("creator_id") or "").strip()
            for item in existing_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "existing_screened_count": len(existing_screened_entries),
        "existing_screened_preview": [
            str(item.get("creator_id") or "").strip()
            for item in existing_screened_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "existing_unscreened_count": len(existing_unscreened_entries),
        "existing_unscreened_preview": [
            str(item.get("creator_id") or "").strip()
            for item in existing_unscreened_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "incremental_candidate_count": len(new_entries),
        "incremental_candidate_preview": [
            str(item.get("creator_id") or "").strip()
            for item in new_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "full_screening_candidate_count": len(full_screening_entries),
        "full_screening_candidate_preview": [
            str(item.get("creator_id") or "").strip()
            for item in full_screening_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "mail_only_update_count": len(mail_only_update_entries),
        "mail_only_update_preview": [
            str(item.get("creator_id") or "").strip()
            for item in mail_only_update_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "partial_refresh_count": len(partial_refresh_entries),
        "partial_refresh_preview": [
            str(item.get("creator_id") or "").strip()
            for item in partial_refresh_entries[:10]
            if str(item.get("creator_id") or "").strip()
        ],
        "partial_refresh_breakdown": dict(sorted(partial_refresh_breakdown.items())),
        "duplicate_existing_group_count": duplicate_existing_group_count,
        "all_existing": bool(planned_entries) and len(existing_entries) == len(planned_entries),
        "mail_only_update_only": bool(planned_entries)
        and len(existing_entries) == len(planned_entries)
        and bool(existing_screened_entries)
        and not requested_entries
        and not partial_refresh_entries,
        "scrape_requested_identifier_count": len(requested_entries),
        "visual_requested_identifier_count": len(visual_requested_identifiers),
        "positioning_requested_identifier_count": len(positioning_requested_identifiers),
        "creator_cache_enabled": bool(creator_cache_enabled),
        "creator_cache_db_path": str(resolved_creator_cache_db_path),
        "visual_cache_context_key": visual_cache_context_key,
    }
    return {
        "staged_identifier_count": len(planned_entries),
        "requested_identifiers": list(requested_entries),
        "visual_requested_identifiers": list(visual_requested_identifiers),
        "positioning_requested_identifiers": list(positioning_requested_identifiers),
        "mail_only_update_entries": [dict(item) for item in mail_only_update_entries],
        "partial_refresh_entries": [dict(item) for item in partial_refresh_entries],
        "incremental_prefilter": incremental_prefilter,
    }


def build_scrape_payload(
    platform: str,
    identifiers: list[str],
    *,
    exclude_pinned_posts: bool = True,
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
) -> dict[str, Any]:
    values = [str(item).strip() for item in identifiers if str(item).strip()]
    if platform == "tiktok":
        payload = {
            "profiles": values,
            "excludePinnedPosts": bool(exclude_pinned_posts),
        }
    elif platform == "instagram":
        payload = {"usernames": values}
    elif platform == "youtube":
        payload = {"urls": values}
    else:
        raise ValueError(f"不支持的平台: {platform}")
    if str(creator_cache_db_path or "").strip():
        payload["creator_cache_db_path"] = str(creator_cache_db_path).strip()
    if force_refresh_creator_cache:
        payload["force_refresh_creator_cache"] = True
    return payload


def build_visual_payload(
    platform: str,
    identifiers: list[str],
    *,
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
) -> dict[str, Any]:
    values = [str(item).strip() for item in identifiers if str(item).strip()]
    if platform in {"tiktok", "instagram", "youtube"}:
        payload = {"identifiers": values}
        if str(creator_cache_db_path or "").strip():
            payload["creator_cache_db_path"] = str(creator_cache_db_path).strip()
        if force_refresh_creator_cache:
            payload["force_refresh_creator_cache"] = True
        return payload
    raise ValueError(f"不支持的平台: {platform}")


def _extract_available_profile_identifiers(profile_reviews: list[dict[str, Any]]) -> list[str]:
    identifiers: list[str] = []
    seen: set[str] = set()
    for item in profile_reviews:
        status = str((item or {}).get("status") or "").strip()
        if status == "Missing":
            continue
        upload_metadata = dict((item or {}).get("upload_metadata") or {})
        identifier = str(
            (item or {}).get("username")
            or upload_metadata.get("handle")
            or ""
        ).strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        identifiers.append(identifier)
    return identifiers


def _resolve_next_fallback_platform(current_platform: str, requested_platforms: list[str]) -> str:
    normalized_current = str(current_platform or "").strip().lower()
    normalized_requested = [str(item or "").strip().lower() for item in (requested_platforms or []) if str(item or "").strip()]
    try:
        current_index = DEFAULT_PLATFORM_ORDER.index(normalized_current)
    except ValueError:
        return ""
    for candidate in DEFAULT_PLATFORM_ORDER[current_index + 1 :]:
        if candidate in normalized_requested:
            return candidate
    return ""


def _build_fallback_metadata_record(backend_app, metadata: dict[str, Any], next_platform: str) -> dict[str, Any]:
    normalized_platform = str(next_platform or "").strip().lower()
    handle = str(metadata.get("handle") or "").strip()
    if not handle:
        return {}
    explicit_next_url = str(metadata.get(f"{normalized_platform}_url") or "").strip()
    next_url = explicit_next_url
    if not next_url and hasattr(backend_app, "screening") and hasattr(backend_app.screening, "build_canonical_profile_url"):
        next_url = str(backend_app.screening.build_canonical_profile_url(normalized_platform, handle) or "").strip()
    if not next_url:
        return {}
    cloned = dict(metadata or {})
    cloned["platform"] = normalized_platform
    cloned["url"] = next_url
    cloned["handle"] = handle
    return cloned


def _stage_missing_profiles_for_fallback(
    *,
    backend_app,
    current_platform: str,
    next_platform: str,
    missing_profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    current_lookup = dict(backend_app.load_upload_metadata(current_platform) or {})
    staged_payload: dict[str, Any] = {}
    staged_identifiers: list[str] = []
    unresolved_missing: list[dict[str, Any]] = []
    for item in missing_profiles:
        identifier = str(item.get("identifier") or "").strip()
        metadata = dict(current_lookup.get(identifier) or {})
        if not metadata:
            unresolved_missing.append(dict(item))
            continue
        next_record = _build_fallback_metadata_record(backend_app, metadata, next_platform)
        if not next_record:
            unresolved_missing.append(dict(item))
            continue
        handle = str(next_record.get("handle") or "").strip()
        if not handle:
            unresolved_missing.append(dict(item))
            continue
        next_record["fallback_from_platform"] = str(current_platform or "").strip().lower()
        next_record["fallback_reason"] = str(item.get("reason") or "").strip()
        staged_payload[handle] = next_record
        staged_identifiers.append(handle)
    if staged_payload:
        backend_app.save_upload_metadata(next_platform, staged_payload, replace=False)
    return {
        "next_platform": str(next_platform or "").strip().lower(),
        "staged_count": len(staged_identifiers),
        "staged_identifier_preview": staged_identifiers[:10],
        "unresolved_missing": unresolved_missing,
    }


def summarize_platform_statuses(platforms: dict[str, dict[str, Any]]) -> str:
    statuses = [str((payload or {}).get("status") or "").strip() for payload in (platforms or {}).values()]
    failure_statuses = {"failed", "scrape_failed", "missing_profiles_blocked"}
    successful_statuses = {"completed", "completed_with_partial_scrape", "dry_run_only", "fallback_staged", "staged_only"}
    if any(status in failure_statuses for status in statuses) and any(status in successful_statuses for status in statuses):
        return "completed_with_platform_failures"
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "scrape_failed" for status in statuses):
        return "scrape_failed"
    if any(status == "missing_profiles_blocked" for status in statuses):
        return "missing_profiles_blocked"
    if any(status == "completed_with_partial_scrape" for status in statuses):
        return "completed_with_partial_scrape"
    if "dry_run_only" in statuses and all(status in {"dry_run_only", "skipped"} for status in statuses):
        return "dry_run_only"
    if "staged_only" in statuses and all(status in {"staged_only", "skipped"} for status in statuses):
        return "staged_only"
    return "completed"


def _coerce_non_negative_int(value: Any) -> int | None:
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, resolved)


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _apply_explicit_feishu_update_mode(payload_json_path: Path, *, update_mode: str) -> int:
    try:
        payload = json.loads(payload_json_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return 0
    rows = list(payload.get("rows") or [])
    updated_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("__feishu_update_mode") or "") != update_mode:
            row["__feishu_update_mode"] = update_mode
            updated_count += 1
    if updated_count > 0:
        payload["rows"] = rows
        payload_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return updated_count


def _stringify_report_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        values = [_stringify_report_value(item) for item in value]
        return "；".join(item for item in values if item)
    if isinstance(value, dict):
        for key in ("text", "name", "link", "value", "id"):
            candidate = str(value.get(key) or "").strip()
            if candidate:
                return candidate
        return ""
    return str(value).strip()


def _write_report_xlsx(output_path: Path, rows: list[dict[str, Any]], *, columns: tuple[str, ...]) -> str:
    if not rows:
        return ""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=columns)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="report")
    return str(output_path)


def _write_report_xlsx_best_effort(
    output_path: Path,
    rows: list[dict[str, Any]],
    *,
    columns: tuple[str, ...],
    warnings_bucket: dict[str, Any],
    warning_key: str,
    artifact_label: str,
) -> str:
    try:
        return _write_report_xlsx(output_path, rows, columns=columns)
    except Exception as exc:  # noqa: BLE001
        warnings_bucket[warning_key] = {
            "artifact": artifact_label,
            "path": str(output_path),
            "error": str(exc) or exc.__class__.__name__,
        }
        return ""


def _collect_missing_profile_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for platform, platform_summary in (summary.get("platforms") or {}).items():
        for item in list((platform_summary or {}).get("missing_profiles") or []):
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "platform": str(platform or "").strip(),
                    "identifier": _stringify_report_value(item.get("identifier")),
                    "profile_url": _stringify_report_value(item.get("profile_url")),
                    "reason": _stringify_report_value(item.get("reason")),
                }
            )
    return rows


def _build_success_report_rows(upload_summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    payload = dict(upload_summary or {})
    for action, items in (
        ("Created", list(payload.get("created_rows") or [])),
        ("Updated", list(payload.get("updated_rows") or [])),
    ):
        for item in items:
            row = dict((item or {}).get("row") or {})
            rows.append(
                {
                    "达人ID": _stringify_report_value(row.get("达人ID")),
                    "平台": _stringify_report_value(row.get("平台")),
                    "主页链接": _stringify_report_value(row.get("主页链接")),
                    "操作": action,
                    "飞书记录ID": _stringify_report_value((item or {}).get("record_id")),
                }
            )
    return rows


def _build_error_report_rows(
    summary: dict[str, Any],
    upload_summary: dict[str, Any] | None,
    *,
    occurred_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _collect_missing_profile_rows(summary):
        rows.append(
            {
                "严重级别": "warning",
                "异常类别": "Missing",
                "达人ID": item["identifier"],
                "平台": item["platform"],
                "原因详情": item["reason"] or item["profile_url"],
                "飞书记录ID": "",
                "时间": occurred_at,
            }
        )

    for platform, platform_summary in (summary.get("platforms") or {}).items():
        payload = dict(platform_summary or {})
        if str(payload.get("status") or "").strip() not in {"scrape_failed", "failed"}:
            continue
        rows.append(
            {
                "严重级别": "error",
                "异常类别": "Scrape Failed",
                "达人ID": "",
                "平台": str(platform or "").strip(),
                "原因详情": _first_non_empty_text(payload.get("error"), payload.get("error_code")),
                "飞书记录ID": "",
                "时间": occurred_at,
            }
        )

    normalized_upload = dict(upload_summary or {})
    for item in list(normalized_upload.get("failed_rows") or []):
        row = dict((item or {}).get("row") or {})
        rows.append(
            {
                "严重级别": "error",
                "异常类别": "Upload Failed",
                "达人ID": _stringify_report_value(row.get("达人ID")),
                "平台": _stringify_report_value(row.get("平台")),
                "原因详情": _first_non_empty_text((item or {}).get("error"), (item or {}).get("reason")),
                "飞书记录ID": _first_non_empty_text((item or {}).get("record_id"), (item or {}).get("existing_record_id")),
                "时间": occurred_at,
            }
        )

    for item in list(normalized_upload.get("deduplicated_rows") or []):
        row = dict((item or {}).get("row") or {})
        rows.append(
            {
                "严重级别": "warning",
                "异常类别": "Payload Deduplicated",
                "达人ID": _stringify_report_value(row.get("达人ID")),
                "平台": _stringify_report_value(row.get("平台")),
                "原因详情": _first_non_empty_text((item or {}).get("error"), (item or {}).get("reason")),
                "飞书记录ID": "",
                "时间": occurred_at,
            }
        )

    for group in list(normalized_upload.get("duplicate_existing_groups") or []):
        duplicate_count = len(list((group or {}).get("duplicate_records") or [])) + 1
        rows.append(
            {
                "严重级别": "warning",
                "异常类别": "Existing Duplicate Group",
                "达人ID": _first_non_empty_text((group or {}).get("creator_id"), (group or {}).get("record_key")),
                "平台": _stringify_report_value((group or {}).get("platform")),
                "原因详情": f"目标飞书表存在 {duplicate_count} 条重复记录，本次仅更新 keep_record。",
                "飞书记录ID": _stringify_report_value(((group or {}).get("keep_record") or {}).get("record_id")),
                "时间": occurred_at,
            }
        )
    return rows


def _compact_upload_summary(upload_summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(upload_summary or {})
    if not payload:
        return {}
    upload_detail = dict(payload.get("upload_detail") or {})
    duplicate_existing_groups = list(upload_detail.get("duplicate_existing_groups") or [])
    compact_upload_detail = {
        "created_key_count": len(list(upload_detail.get("created_keys") or [])),
        "created_key_preview": list(upload_detail.get("created_keys") or [])[:20],
        "updated_key_count": len(list(upload_detail.get("updated_keys") or [])),
        "updated_key_preview": list(upload_detail.get("updated_keys") or [])[:20],
        "failed_detail": list(upload_detail.get("failed_detail") or [])[:20],
        "deduplicated_detail": list(upload_detail.get("deduplicated_detail") or [])[:20],
        "duplicate_existing_group_count": len(duplicate_existing_groups),
        "duplicate_existing_group_preview": [
            {
                "record_key": str(group.get("record_key") or "").strip(),
                "creator_id": str(group.get("creator_id") or "").strip(),
                "platform": _stringify_report_value(group.get("platform")),
                "keep_record_id": _stringify_report_value((group.get("keep_record") or {}).get("record_id")),
                "duplicate_record_count": len(list(group.get("duplicate_records") or [])) + 1,
            }
            for group in duplicate_existing_groups[:20]
            if isinstance(group, dict)
        ],
    }
    retry_summary = dict(payload.get("retry_summary") or {})
    retry_operations = dict(retry_summary.get("operations") or {})
    return {
        "ok": bool(payload.get("ok", True)),
        "dry_run": bool(payload.get("dry_run")),
        "payload_json_path": str(payload.get("payload_json_path") or "").strip(),
        "result_json_path": str(payload.get("result_json_path") or "").strip(),
        "result_xlsx_path": str(payload.get("result_xlsx_path") or "").strip(),
        "result_json_written": bool(payload.get("result_json_written")),
        "result_xlsx_written": bool(payload.get("result_xlsx_written")),
        "target_url": str(payload.get("target_url") or "").strip(),
        "target_table_id": str(payload.get("target_table_id") or "").strip(),
        "target_table_name": str(payload.get("target_table_name") or "").strip(),
        "created_count": int(payload.get("created_count") or 0),
        "updated_count": int(payload.get("updated_count") or 0),
        "failed_count": int(payload.get("failed_count") or 0),
        "skipped_existing_count": int(payload.get("skipped_existing_count") or 0),
        "duplicate_existing_group_count": int(payload.get("duplicate_existing_group_count") or 0),
        "duplicate_payload_group_count": int(payload.get("duplicate_payload_group_count") or 0),
        "deduplicated_row_count": int(payload.get("deduplicated_row_count") or 0),
        "request_control": dict(payload.get("request_control") or {}),
        "retry_summary": {
            "enabled": bool(retry_summary.get("enabled")),
            "max_retries": int(retry_summary.get("max_retries") or 0),
            "write_min_interval_seconds": float(retry_summary.get("write_min_interval_seconds") or 0.0),
            "request_count": int(retry_summary.get("request_count") or 0),
            "attempt_count": int(retry_summary.get("attempt_count") or 0),
            "retried_request_count": int(retry_summary.get("retried_request_count") or 0),
            "retryable_error_count": int(retry_summary.get("retryable_error_count") or 0),
            "recovered_request_count": int(retry_summary.get("recovered_request_count") or 0),
            "exhausted_request_count": int(retry_summary.get("exhausted_request_count") or 0),
            "rate_limit_sleep_seconds": float(retry_summary.get("rate_limit_sleep_seconds") or 0.0),
            "backoff_sleep_seconds": float(retry_summary.get("backoff_sleep_seconds") or 0.0),
            "operations": {
                operation: {
                    "request_count": int((item or {}).get("request_count") or 0),
                    "attempt_count": int((item or {}).get("attempt_count") or 0),
                    "retried_request_count": int((item or {}).get("retried_request_count") or 0),
                    "retryable_error_count": int((item or {}).get("retryable_error_count") or 0),
                    "recovered_request_count": int((item or {}).get("recovered_request_count") or 0),
                    "exhausted_request_count": int((item or {}).get("exhausted_request_count") or 0),
                }
                for operation, item in list(retry_operations.items())[:20]
                if isinstance(item, dict)
            },
        },
        "upload_detail": compact_upload_detail,
        "report_write_warnings": list(payload.get("report_write_warnings") or []),
    }


def _build_cli_output_summary(summary: dict[str, Any]) -> dict[str, Any]:
    artifacts = dict(summary.get("artifacts") or {})
    platforms: dict[str, Any] = {}
    for platform, payload in (summary.get("platforms") or {}).items():
        if not isinstance(payload, dict):
            continue
        platforms[str(platform)] = {
            "status": str(payload.get("status") or "").strip(),
            "current_stage": str(payload.get("current_stage") or "").strip(),
            "requested_identifier_count": int(payload.get("requested_identifier_count") or 0),
            "mail_only_update_count": int(payload.get("mail_only_update_count") or 0),
            "partial_refresh_count": int(payload.get("partial_refresh_count") or 0),
            "profile_review_count": int(payload.get("profile_review_count") or 0),
            "prescreen_pass_count": int(payload.get("prescreen_pass_count") or 0),
            "missing_profile_count": int(payload.get("missing_profile_count") or 0),
        }
    return {
        "status": str(summary.get("status") or "").strip(),
        "dry_run": bool(summary.get("dry_run")),
        "verdict": dict(summary.get("verdict") or {}),
        "run_root": str(summary.get("run_root") or "").strip(),
        "summary_json": str(summary.get("summary_json") or "").strip(),
        "warnings": dict(summary.get("warnings") or {}),
        "dry_run_report": dict(summary.get("dry_run_report") or {}),
        "quality_report": {
            "status": str(((summary.get("quality_report") or {}).get("status")) or "").strip(),
            "warning_count": int(((summary.get("quality_report") or {}).get("warning_count")) or 0),
        },
        "platforms": platforms,
        "artifacts": {
            "all_platforms_final_review": str(artifacts.get("all_platforms_final_review") or "").strip(),
            "missing_profiles_xlsx": str(artifacts.get("missing_profiles_xlsx") or "").strip(),
            "success_report_xlsx": str(artifacts.get("success_report_xlsx") or "").strip(),
            "error_report_xlsx": str(artifacts.get("error_report_xlsx") or "").strip(),
            "feishu_upload_result_json": str(artifacts.get("feishu_upload_result_json") or "").strip(),
            "feishu_upload_result_xlsx": str(artifacts.get("feishu_upload_result_xlsx") or "").strip(),
            "feishu_upload_created_count": int(artifacts.get("feishu_upload_created_count") or 0),
            "feishu_upload_updated_count": int(artifacts.get("feishu_upload_updated_count") or 0),
            "feishu_upload_failed_count": int(artifacts.get("feishu_upload_failed_count") or 0),
        },
        "upload_summary": dict(summary.get("upload_summary") or {}),
    }


def _build_pending_observability_layer(status: str, *, blocking_reason: str = "") -> dict[str, Any]:
    return {
        "status": str(status or "").strip(),
        "blocking_reason": str(blocking_reason or "").strip(),
    }


def _extract_downstream_blocking_reason(summary: dict[str, Any]) -> dict[str, Any]:
    failure = dict(summary.get("failure") or {})
    if failure:
        return {
            "stage": str(failure.get("stage") or "").strip(),
            "error_code": str(failure.get("error_code") or "").strip(),
            "message": str(failure.get("message") or "").strip(),
        }
    return {
        "stage": "",
        "error_code": str(summary.get("error_code") or "").strip(),
        "message": str(summary.get("error") or "").strip(),
    }


def _derive_downstream_run_stage(summary: dict[str, Any]) -> str:
    explicit_stage = str(summary.get("current_stage") or "").strip()
    if explicit_stage:
        return explicit_stage
    for platform in list(summary.get("execution_platforms") or []):
        payload = dict((summary.get("platforms") or {}).get(platform) or {})
        current_stage = str(payload.get("current_stage") or "").strip()
        if current_stage:
            return current_stage
    return str(summary.get("status") or "").strip() or "preflight"


def _extract_export_mode_counts(platforms: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = {
        "normal": 0,
        "deferred": 0,
        "local_missing_profile_fallback": 0,
        "unknown": 0,
    }
    for platform_summary in (platforms or {}).values():
        if not isinstance(platform_summary, dict):
            continue
        export_mode = str(((platform_summary.get("exports") or {}).get("final_review_export_mode")) or "").strip()
        deferred_status = str(((platform_summary.get("final_review_export") or {}).get("status")) or "").strip()
        if deferred_status == "deferred":
            counts["deferred"] += 1
        elif export_mode == "local_missing_profile_fallback":
            counts["local_missing_profile_fallback"] += 1
        elif export_mode == "api" or str(((platform_summary.get("exports") or {}).get("final_review")) or "").strip():
            counts["normal"] += 1
        elif str(platform_summary.get("status") or "").strip() not in {"skipped", ""}:
            counts["unknown"] += 1
    return counts


def _extract_platform_stage_duration_rows(platforms: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for platform, platform_summary in (platforms or {}).items():
        stage_metrics = dict((platform_summary or {}).get("stage_metrics") or {})
        for stage_name, payload in stage_metrics.items():
            if not isinstance(payload, dict):
                continue
            duration_seconds = payload.get("duration_seconds")
            if duration_seconds is None:
                continue
            rows.append(
                {
                    "platform": str(platform),
                    "stage": str(stage_name),
                    "duration_seconds": float(duration_seconds or 0.0),
                    "started_at": str(payload.get("started_at") or "").strip(),
                    "finished_at": str(payload.get("finished_at") or "").strip(),
                    "status": str(payload.get("status") or "").strip(),
                }
            )
    return rows


def _build_task_assets_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    staging = dict(summary.get("staging") or {})
    task_source = dict(staging.get("taskSource") or {})
    resolved_task_owner = dict(summary.get("resolved_task_owner") or {})
    linked_bitable_url = str(resolved_task_owner.get("linked_bitable_url") or "").strip()
    template_workbook = str(summary.get("template_workbook") or "").strip()
    return {
        "status": "ready" if str(summary.get("keep_workbook") or "").strip() else "failed",
        "task_name": str(summary.get("task_name") or "").strip(),
        "task_upload_url": str(summary.get("task_upload_url") or "").strip(),
        "employee_info_url": "",
        "template_workbook": template_workbook,
        "keep_workbook": str(summary.get("keep_workbook") or "").strip(),
        "linked_bitable_url": linked_bitable_url,
        "task_owner_employee_id": str(resolved_task_owner.get("task_owner_employee_id") or "").strip(),
        "task_owner_name": str(resolved_task_owner.get("task_owner_name") or "").strip(),
        "task_start_date": str(task_source.get("taskStartDate") or "").strip(),
        "checks": {
            "linked_bitable_url_present": bool(linked_bitable_url),
            "template_workbook_exists": _path_exists(template_workbook),
            "task_start_date_resolved": bool(str(task_source.get("taskStartDate") or "").strip()),
            "task_owner_complete": bool(
                str(resolved_task_owner.get("task_owner_name") or "").strip()
                and str(resolved_task_owner.get("task_owner_employee_id") or "").strip()
            ),
        },
        "blocking_reason": "",
    }


def _build_mail_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "not_in_scope",
        "source_mode": "",
        "sent_since": "",
        "db_path": "",
        "message_hit_count": 0,
        "external_message_count": 0,
        "pass0_sending_list_email_count": 0,
        "regex_pass1_count": 0,
        "regex_pass2_count": 0,
        "llm_high_count": 0,
        "manual_row_count": 0,
        "keep_row_count": 0,
        "blocking_reason": "mail layer belongs to upstream keep-list summary; downstream starts from keep workbook input",
    }


def _build_incremental_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    platforms = dict(summary.get("platforms") or {})
    existing_bitable_prefilter = dict(summary.get("existing_bitable_prefilter") or {})
    staged_identifier_count = 0
    existing_bitable_match_count = 0
    incremental_candidate_count = 0
    full_screening_candidate_count = 0
    existing_screened_count = 0
    existing_unscreened_count = 0
    mail_only_update_count = 0
    partial_refresh_count = 0
    all_existing = True
    incremental_candidate_preview: list[str] = []
    mail_only_update_preview: list[str] = []
    partial_refresh_preview: list[str] = []
    partial_refresh_breakdown: dict[str, int] = {}
    duplicate_existing_group_count = int(existing_bitable_prefilter.get("duplicate_existing_group_count") or 0)
    for platform_summary in platforms.values():
        if not isinstance(platform_summary, dict):
            continue
        staged_identifier_count += int(platform_summary.get("staged_identifier_count") or 0)
        platform_prefilter = dict(platform_summary.get("incremental_prefilter") or {})
        existing_bitable_match_count += int(platform_prefilter.get("existing_bitable_match_count") or 0)
        incremental_candidate_count += int(platform_prefilter.get("incremental_candidate_count") or 0)
        full_screening_candidate_count += int(platform_prefilter.get("full_screening_candidate_count") or 0)
        existing_screened_count += int(platform_prefilter.get("existing_screened_count") or 0)
        existing_unscreened_count += int(platform_prefilter.get("existing_unscreened_count") or 0)
        mail_only_update_count += int(platform_prefilter.get("mail_only_update_count") or 0)
        partial_refresh_count += int(platform_prefilter.get("partial_refresh_count") or 0)
        duplicate_existing_group_count = max(
            duplicate_existing_group_count,
            int(platform_prefilter.get("duplicate_existing_group_count") or 0),
        )
        all_existing = all_existing and bool(platform_prefilter.get("all_existing"))
        incremental_candidate_preview.extend(
            [str(item) for item in list(platform_prefilter.get("incremental_candidate_preview") or []) if str(item).strip()]
        )
        mail_only_update_preview.extend(
            [str(item) for item in list(platform_prefilter.get("mail_only_update_preview") or []) if str(item).strip()]
        )
        partial_refresh_preview.extend(
            [str(item) for item in list(platform_prefilter.get("partial_refresh_preview") or []) if str(item).strip()]
        )
        for reason, count in dict(platform_prefilter.get("partial_refresh_breakdown") or {}).items():
            normalized_reason = str(reason or "").strip()
            if not normalized_reason:
                continue
            partial_refresh_breakdown[normalized_reason] = int(partial_refresh_breakdown.get(normalized_reason) or 0) + int(count or 0)
    status = "ready" if existing_bitable_prefilter.get("enabled") else str(existing_bitable_prefilter.get("status") or "disabled")
    return {
        "status": status,
        "linked_bitable_url": str(existing_bitable_prefilter.get("linked_bitable_url") or "").strip(),
        "enabled": bool(existing_bitable_prefilter.get("enabled")),
        "staged_identifier_count": staged_identifier_count,
        "existing_bitable_match_count": existing_bitable_match_count,
        "incremental_candidate_count": incremental_candidate_count,
        "full_screening_candidate_count": full_screening_candidate_count,
        "existing_screened_count": existing_screened_count,
        "existing_unscreened_count": existing_unscreened_count,
        "mail_only_update_count": mail_only_update_count,
        "partial_refresh_count": partial_refresh_count,
        "all_existing": bool(staged_identifier_count > 0 and all_existing),
        "duplicate_existing_group_count": duplicate_existing_group_count,
        "incremental_candidate_preview": incremental_candidate_preview[:10],
        "mail_only_update_preview": mail_only_update_preview[:10],
        "partial_refresh_preview": partial_refresh_preview[:10],
        "partial_refresh_breakdown": dict(sorted(partial_refresh_breakdown.items())),
        "blocking_reason": str(existing_bitable_prefilter.get("error") or "").strip(),
    }


def _build_execution_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    platforms = dict(summary.get("platforms") or {})
    platform_payloads: dict[str, Any] = {}
    status = "ready"
    for platform, payload in platforms.items():
        if not isinstance(payload, dict):
            continue
        visual_gate = dict(payload.get("visual_gate") or {})
        visual_retry = dict(payload.get("visual_retry") or {})
        positioning = dict(payload.get("positioning_card_analysis") or {})
        platform_payloads[str(platform)] = {
            "status": str(payload.get("status") or "").strip(),
            "current_stage": str(payload.get("current_stage") or "").strip(),
            "requested_identifier_count": int(payload.get("requested_identifier_count") or 0),
            "mail_only_update_count": int(payload.get("mail_only_update_count") or 0),
            "partial_refresh_count": int(payload.get("partial_refresh_count") or 0),
            "profile_review_count": int(payload.get("profile_review_count") or 0),
            "prescreen_pass_count": int(payload.get("prescreen_pass_count") or 0),
            "missing_profile_count": int(payload.get("missing_profile_count") or 0),
            "fallback_candidate_count": int(payload.get("fallback_candidate_count") or 0),
            "visual_job_status": str((payload.get("visual_job") or {}).get("status") or "").strip(),
            "visual_gate": {
                "executed": bool(visual_gate.get("executed")),
                "selected_provider": str(visual_gate.get("selected_provider") or "").strip(),
                "reason": str(visual_gate.get("reason") or "").strip(),
            },
            "visual_retry": {
                "status": str(visual_retry.get("status") or "").strip(),
                "round_count": len(list(visual_retry.get("rounds") or [])),
                "final_error_count": int(visual_retry.get("final_error_count") or 0),
            },
            "positioning": {
                "status": str(positioning.get("status") or "").strip(),
                "started_at": str(((payload.get("stage_metrics") or {}).get("positioning") or {}).get("started_at") or "").strip(),
                "finished_at": str(((payload.get("stage_metrics") or {}).get("positioning") or {}).get("finished_at") or "").strip(),
                "duration_seconds": float((((payload.get("stage_metrics") or {}).get("positioning") or {}).get("duration_seconds") or 0.0)),
            },
            "stage_metrics": dict(payload.get("stage_metrics") or {}),
        }
        if str(payload.get("status") or "").strip() in {"failed", "scrape_failed"}:
            status = "failed"
        elif int(payload.get("missing_profile_count") or 0) > 0 and status != "failed":
            status = "warning"
    return {
        "status": status,
        "platforms": platform_payloads,
        "blocking_reason": "",
    }


def _build_dry_run_report(summary: dict[str, Any]) -> dict[str, Any]:
    staging_stats = dict(((summary.get("staging") or {}).get("upload") or {}).get("stats") or {})
    total_keep_row_count = 0
    for value in staging_stats.values():
        try:
            total_keep_row_count += max(0, int(value or 0))
        except (TypeError, ValueError):
            continue

    platforms = dict(summary.get("platforms") or {})
    estimated_execution_platforms: list[str] = []
    all_existing_platforms: list[str] = []
    no_candidate_platforms: list[str] = []
    platform_reports: dict[str, Any] = {}
    staged_identifier_count = 0
    existing_bitable_match_count = 0
    incremental_candidate_count = 0
    full_screening_candidate_count = 0
    mail_only_update_count = 0
    partial_refresh_count = 0
    partial_refresh_preview: list[str] = []
    partial_refresh_breakdown: dict[str, int] = {}

    for platform, payload in platforms.items():
        if not isinstance(payload, dict):
            continue
        platform_name = str(platform or "").strip()
        if not platform_name:
            continue
        platform_prefilter = dict(payload.get("incremental_prefilter") or {})
        platform_staged = int(payload.get("staged_identifier_count") or 0)
        platform_existing = int(platform_prefilter.get("existing_bitable_match_count") or 0)
        platform_incremental = int(platform_prefilter.get("incremental_candidate_count") or 0)
        platform_full_screening = int(platform_prefilter.get("full_screening_candidate_count") or 0)
        platform_mail_only_update = int(platform_prefilter.get("mail_only_update_count") or 0)
        platform_partial_refresh = int(platform_prefilter.get("partial_refresh_count") or 0)
        platform_requested = int(payload.get("requested_identifier_count") or 0)
        staged_identifier_count += platform_staged
        existing_bitable_match_count += platform_existing
        incremental_candidate_count += platform_incremental
        full_screening_candidate_count += platform_full_screening
        mail_only_update_count += platform_mail_only_update
        partial_refresh_count += platform_partial_refresh
        if platform_requested > 0 or platform_mail_only_update > 0 or platform_partial_refresh > 0:
            estimated_execution_platforms.append(platform_name)
        if bool(platform_prefilter.get("all_existing")):
            all_existing_platforms.append(platform_name)
        elif platform_staged <= 0:
            no_candidate_platforms.append(platform_name)
        partial_refresh_preview.extend(
            [str(item) for item in list(platform_prefilter.get("partial_refresh_preview") or []) if str(item).strip()]
        )
        for reason, count in dict(platform_prefilter.get("partial_refresh_breakdown") or {}).items():
            normalized_reason = str(reason or "").strip()
            if not normalized_reason:
                continue
            partial_refresh_breakdown[normalized_reason] = (
                int(partial_refresh_breakdown.get(normalized_reason) or 0) + int(count or 0)
            )
        platform_reports[platform_name] = {
            "status": str(payload.get("status") or "").strip(),
            "staged_identifier_count": platform_staged,
            "existing_bitable_match_count": platform_existing,
            "incremental_candidate_count": platform_incremental,
            "full_screening_candidate_count": platform_full_screening,
            "mail_only_update_count": platform_mail_only_update,
            "partial_refresh_count": platform_partial_refresh,
            "requested_identifier_count": platform_requested,
            "requested_identifier_preview": list(payload.get("requested_identifier_preview") or [])[:10],
            "incremental_candidate_preview": list(platform_prefilter.get("incremental_candidate_preview") or [])[:10],
            "mail_only_update_preview": list(platform_prefilter.get("mail_only_update_preview") or [])[:10],
            "partial_refresh_preview": list(platform_prefilter.get("partial_refresh_preview") or [])[:10],
            "partial_refresh_breakdown": dict(platform_prefilter.get("partial_refresh_breakdown") or {}),
            "all_existing": bool(platform_prefilter.get("all_existing")),
            "would_execute": bool(platform_requested > 0 or platform_mail_only_update > 0 or platform_partial_refresh > 0),
        }

    return {
        "total_keep_row_count": total_keep_row_count,
        "staged_identifier_count": staged_identifier_count,
        "existing_bitable_match_count": existing_bitable_match_count,
        "incremental_candidate_count": incremental_candidate_count,
        "full_screening_candidate_count": full_screening_candidate_count,
        "mail_only_update_count": mail_only_update_count,
        "partial_refresh_count": partial_refresh_count,
        "partial_refresh_preview": partial_refresh_preview[:10],
        "partial_refresh_breakdown": dict(sorted(partial_refresh_breakdown.items())),
        "estimated_execution_platform_count": len(estimated_execution_platforms),
        "estimated_execution_platforms": estimated_execution_platforms,
        "all_existing_platforms": all_existing_platforms,
        "no_candidate_platforms": no_candidate_platforms,
        "duplicate_existing_group_count": int(
            ((summary.get("existing_bitable_prefilter") or {}).get("duplicate_existing_group_count")) or 0
        ),
        "platforms": platform_reports,
    }


def _build_exports_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    artifacts = dict(summary.get("artifacts") or {})
    platforms = dict(summary.get("platforms") or {})
    export_modes = _extract_export_mode_counts(platforms)
    platform_exports: dict[str, Any] = {}
    status = "skipped" if bool(summary.get("dry_run")) else "ready"
    for platform, payload in platforms.items():
        if not isinstance(payload, dict):
            continue
        artifact_status = dict(payload.get("artifact_status") or {})
        exports = dict(payload.get("exports") or {})
        deferred = dict(payload.get("final_review_export") or {})
        export_mode = str(exports.get("final_review_export_mode") or "").strip()
        normalized_mode = (
            "deferred"
            if str(deferred.get("status") or "").strip() == "deferred"
            else ("local_missing_profile_fallback" if export_mode == "local_missing_profile_fallback" else ("normal" if export_mode == "api" or str(exports.get("final_review") or "").strip() else "unknown"))
        )
        platform_exports[str(platform)] = {
            "status": str(payload.get("status") or "").strip(),
            "final_review_export_available": bool(str(exports.get("final_review") or "").strip()),
            "final_review_export_blocked": bool(artifact_status.get("final_review_export_blocked")),
            "final_review_export_mode": normalized_mode,
            "final_review_path": str(exports.get("final_review") or "").strip(),
            "positioning_card_review_path": str(exports.get("positioning_card_review") or "").strip(),
        }
        if normalized_mode in {"local_missing_profile_fallback", "unknown"} and status == "ready":
            status = "warning"
    return {
        "status": status,
        "final_review_export_modes": export_modes,
        "platforms": platform_exports,
        "artifact_paths": {
            "all_platforms_final_review": str(artifacts.get("all_platforms_final_review") or "").strip(),
            "all_platforms_final_review_payload_json": str(artifacts.get("all_platforms_upload_payload_json") or "").strip(),
            "missing_profiles_xlsx": str(artifacts.get("missing_profiles_xlsx") or "").strip(),
            "success_report_xlsx": str(artifacts.get("success_report_xlsx") or "").strip(),
            "error_report_xlsx": str(artifacts.get("error_report_xlsx") or "").strip(),
        },
        "blocking_reason": "",
    }


def _build_upload_observability_layer(summary: dict[str, Any]) -> dict[str, Any]:
    upload_summary = dict(summary.get("upload_summary") or {})
    artifacts = dict(summary.get("artifacts") or {})
    retry_summary = dict(upload_summary.get("retry_summary") or {})
    status = "skipped" if bool(summary.get("dry_run")) else "ready"
    if bool(upload_summary.get("guard_blocked")):
        status = "failed"
    elif int(upload_summary.get("failed_count") or artifacts.get("feishu_upload_failed_count") or 0) > 0:
        status = "warning"
    return {
        "status": status,
        "source_row_count": int(artifacts.get("all_platforms_upload_source_row_count") or 0),
        "selected_row_count": int(upload_summary.get("selected_row_count") or 0),
        "created_count": int(upload_summary.get("created_count") or artifacts.get("feishu_upload_created_count") or 0),
        "updated_count": int(upload_summary.get("updated_count") or artifacts.get("feishu_upload_updated_count") or 0),
        "skipped_existing_count": int(upload_summary.get("skipped_existing_count") or artifacts.get("feishu_upload_skipped_existing_count") or 0),
        "failed_count": int(upload_summary.get("failed_count") or artifacts.get("feishu_upload_failed_count") or 0),
        "guard_blocked": bool(upload_summary.get("guard_blocked")),
        "duplicate_existing_group_count": int(upload_summary.get("duplicate_existing_group_count") or 0),
        "duplicate_payload_group_count": int(upload_summary.get("duplicate_payload_group_count") or 0),
        "result_json_written": bool(upload_summary.get("result_json_written")),
        "result_xlsx_written": bool(upload_summary.get("result_xlsx_written")),
        "retried_request_count": int(retry_summary.get("retried_request_count") or 0),
        "retryable_error_count": int(retry_summary.get("retryable_error_count") or 0),
        "recovered_request_count": int(retry_summary.get("recovered_request_count") or 0),
        "exhausted_request_count": int(retry_summary.get("exhausted_request_count") or 0),
        "rate_limit_sleep_seconds": float(retry_summary.get("rate_limit_sleep_seconds") or 0.0),
        "backoff_sleep_seconds": float(retry_summary.get("backoff_sleep_seconds") or 0.0),
        "target_table_id": str(artifacts.get("feishu_upload_target_table_id") or "").strip(),
        "target_table_name": str(artifacts.get("feishu_upload_target_table_name") or "").strip(),
        "blocking_reason": (
            "dry_run flag set; upload skipped"
            if bool(summary.get("dry_run"))
            else str(upload_summary.get("error") or "").strip()
        ),
    }


def _build_downstream_observability(summary: dict[str, Any]) -> dict[str, Any]:
    platforms = dict(summary.get("platforms") or {})
    task_assets_layer = _build_task_assets_observability_layer(summary)
    mail_layer = _build_mail_observability_layer(summary)
    incremental_layer = _build_incremental_observability_layer(summary)
    execution_layer = _build_execution_observability_layer(summary)
    exports_layer = _build_exports_observability_layer(summary)
    upload_layer = _build_upload_observability_layer(summary)
    blocking_reason = _extract_downstream_blocking_reason(summary)
    return {
        "run_stage": _derive_downstream_run_stage(summary),
        "stage_status": str(summary.get("status") or "").strip(),
        "input_counts": {
            "platform_count": len(list(summary.get("execution_platforms") or [])),
            "staged_identifier_count": sum(
                int((payload or {}).get("staged_identifier_count") or 0)
                for payload in platforms.values()
                if isinstance(payload, dict)
            ),
            "requested_identifier_count": sum(
                int((payload or {}).get("requested_identifier_count") or 0)
                for payload in platforms.values()
                if isinstance(payload, dict)
            ),
            "mail_only_update_count": int(incremental_layer.get("mail_only_update_count") or 0),
            "partial_refresh_count": int(incremental_layer.get("partial_refresh_count") or 0),
            "full_screening_candidate_count": int(incremental_layer.get("full_screening_candidate_count") or 0),
        },
        "output_counts": {
            "profile_review_count": sum(
                int((payload or {}).get("profile_review_count") or 0)
                for payload in platforms.values()
                if isinstance(payload, dict)
            ),
            "missing_profile_count": sum(
                int((payload or {}).get("missing_profile_count") or 0)
                for payload in platforms.values()
                if isinstance(payload, dict)
            ),
            "mail_only_update_count": int(incremental_layer.get("mail_only_update_count") or 0),
            "partial_refresh_count": int(incremental_layer.get("partial_refresh_count") or 0),
            "upload_created_count": int(upload_layer.get("created_count") or 0),
            "upload_failed_count": int(upload_layer.get("failed_count") or 0),
        },
        "fallback_flags": {
            "dry_run": bool(summary.get("dry_run")),
            "skip_scrape": bool(summary.get("skip_scrape")),
            "skip_visual": bool(summary.get("skip_visual")),
            "skip_positioning_card_analysis": bool(summary.get("skip_positioning_card_analysis")),
            "local_missing_profile_fallback_used": int((exports_layer.get("final_review_export_modes") or {}).get("local_missing_profile_fallback") or 0) > 0,
        },
        "blocking_reason": blocking_reason,
        "resource_usage": {
            "elapsed_seconds": _compute_elapsed_seconds(summary.get("started_at"), summary.get("finished_at")),
            "platform_stage_durations": _extract_platform_stage_duration_rows(platforms),
        },
        "artifact_paths": dict(exports_layer.get("artifact_paths") or {}),
        "upload_outcome": dict(upload_layer),
        "layers": {
            "task_assets": task_assets_layer,
            "mail_sync": mail_layer,
            "incremental_creator": incremental_layer,
            "screening_execution": execution_layer,
            "exports": exports_layer,
            "upload": upload_layer,
        },
    }


def _build_downstream_diagnostics(summary: dict[str, Any]) -> dict[str, Any]:
    observability = dict(summary.get("observability") or _build_downstream_observability(summary))
    layers = dict(observability.get("layers") or {})
    incremental_layer = dict(layers.get("incremental_creator") or {})
    exports_layer = dict(layers.get("exports") or {})
    upload_layer = dict(layers.get("upload") or {})
    dry_run_report = dict(summary.get("dry_run_report") or {})
    conclusions: list[dict[str, Any]] = []

    if bool(summary.get("dry_run")):
        conclusions.append(
            {
                "layer": "incremental_creator",
                "code": "dry_run_scope_resolved",
                "severity": "info",
                "message": (
                    f"本次为 dry-run：keep 共 {int(dry_run_report.get('total_keep_row_count') or 0)} 行，"
                    f"staged {int(dry_run_report.get('staged_identifier_count') or 0)} 个达人，"
                    f"已存在 {int(dry_run_report.get('existing_bitable_match_count') or 0)} 个，"
                    f"新增 {int(dry_run_report.get('incremental_candidate_count') or 0)} 个，"
                    f"局部补齐 {int(dry_run_report.get('partial_refresh_count') or 0)} 个，"
                    f"邮件直更 {int(dry_run_report.get('mail_only_update_count') or 0)} 个，"
                    f"预计执行平台 {int(dry_run_report.get('estimated_execution_platform_count') or 0)} 个。"
                ),
            }
        )
        conclusions.append(
            {
                "layer": "upload",
                "code": "dry_run_skipped_execution",
                "severity": "info",
                "message": "本次 dry-run 未真正执行 scrape / visual / positioning / export / 飞书上传。",
            }
        )

    if bool(incremental_layer.get("enabled")):
        staged_count = int(incremental_layer.get("staged_identifier_count") or 0)
        existing_count = int(incremental_layer.get("existing_bitable_match_count") or 0)
        incremental_count = int(incremental_layer.get("incremental_candidate_count") or 0)
        mail_only_update_count = int(incremental_layer.get("mail_only_update_count") or 0)
        partial_refresh_count = int(incremental_layer.get("partial_refresh_count") or 0)
        if bool(incremental_layer.get("all_existing")):
            conclusions.append(
                {
                    "layer": "incremental_creator",
                    "code": "all_existing_creators",
                    "severity": "info",
                    "message": (
                        f"本次 staged {staged_count} 个达人均已存在于目标飞书表，"
                        f"其中 {mail_only_update_count} 个会直接走邮件字段更新，"
                        f"{partial_refresh_count} 个只会补抓缺失环节，无需完整重跑。"
                    ),
                }
            )
        else:
            conclusions.append(
                {
                    "layer": "incremental_creator",
                    "code": "incremental_scope_resolved",
                    "severity": "info",
                    "message": (
                        f"本次 staged {staged_count} 个达人，已存在 {existing_count} 个，"
                        f"新增 {incremental_count} 个，局部补齐 {partial_refresh_count} 个，"
                        f"邮件直更 {mail_only_update_count} 个，"
                        "只对需要的达人继续执行补抓或完整筛号。"
                    ),
                }
            )
        if mail_only_update_count > 0:
            conclusions.append(
                {
                    "layer": "incremental_creator",
                    "code": "mail_only_updates_enabled",
                    "severity": "info",
                    "message": f"已有 {mail_only_update_count} 个已筛达人命中新邮件，本轮会复用飞书已有筛号结果，仅更新邮件字段。",
                }
            )
        if partial_refresh_count > 0:
            conclusions.append(
                {
                    "layer": "incremental_creator",
                    "code": "partial_refresh_enabled",
                    "severity": "info",
                    "message": f"已有 {partial_refresh_count} 个老达人命中本地缓存缺口，本轮只补抓缺失的 scrape / visual / positioning 环节。",
                }
            )
    elif str(incremental_layer.get("linked_bitable_url") or "").strip():
        conclusions.append(
            {
                "layer": "incremental_creator",
                "code": "prefilter_not_available",
                "severity": "warning",
                "message": "linked_bitable_url 已提供，但增量达人预过滤未成功启用，需优先检查飞书读取链路。",
            }
        )

    export_modes = dict(exports_layer.get("final_review_export_modes") or {})
    if not bool(summary.get("dry_run")) and int(export_modes.get("local_missing_profile_fallback") or 0) > 0:
        conclusions.append(
            {
                "layer": "exports",
                "code": "local_final_review_fallback_used",
                "severity": "warning",
                "message": f"本次有 {int(export_modes.get('local_missing_profile_fallback') or 0)} 个平台的 final review 走了本地缺号兜底导出，建议后续持续观察是否频繁触发。",
            }
        )
    elif not bool(summary.get("dry_run")) and int(export_modes.get("deferred") or 0) > 0:
        conclusions.append(
            {
                "layer": "exports",
                "code": "final_review_deferred",
                "severity": "info",
                "message": f"本次有 {int(export_modes.get('deferred') or 0)} 个平台因 fallback staging 延迟导出平台级 final review。",
            }
        )

    if bool(summary.get("dry_run")):
        pass
    elif bool(upload_layer.get("guard_blocked")):
        conclusions.append(
            {
                "layer": "upload",
                "code": "upload_guard_blocked",
                "severity": "warning",
                "message": "飞书上传被 guard_blocked 拦截，需优先检查目标表负责人字段或历史脏数据。",
            }
        )
    else:
        conclusions.append(
            {
                "layer": "upload",
                "code": "upload_outcome",
                "severity": "info" if int(upload_layer.get("failed_count") or 0) == 0 else "warning",
                "message": (
                    f"飞书上传结果：created {int(upload_layer.get('created_count') or 0)}，"
                    f"updated {int(upload_layer.get('updated_count') or 0)}，"
                    f"skipped {int(upload_layer.get('skipped_existing_count') or 0)}，"
                    f"failed {int(upload_layer.get('failed_count') or 0)}。"
                ),
            }
        )
        if int(upload_layer.get("duplicate_existing_group_count") or 0) > 0:
            conclusions.append(
                {
                    "layer": "upload",
                    "code": "duplicate_existing_groups_detected",
                    "severity": "warning",
                    "message": f"目标飞书表检测到 {int(upload_layer.get('duplicate_existing_group_count') or 0)} 组历史重复记录，虽然本次未必阻断上传，但建议持续清理。",
                }
            )
        if int(upload_layer.get("retried_request_count") or 0) > 0:
            conclusions.append(
                {
                    "layer": "upload",
                    "code": "upload_request_retried",
                    "severity": "warning" if int(upload_layer.get("failed_count") or 0) > 0 else "info",
                    "message": (
                        f"飞书上传阶段触发了 {int(upload_layer.get('retried_request_count') or 0)} 次请求重试，"
                        f"累计暂时性错误 {int(upload_layer.get('retryable_error_count') or 0)} 次，"
                        f"主动限流等待约 {float(upload_layer.get('rate_limit_sleep_seconds') or 0.0):.2f} 秒。"
                    ),
                }
            )
        if int(upload_layer.get("exhausted_request_count") or 0) > 0:
            conclusions.append(
                {
                    "layer": "upload",
                    "code": "upload_retry_exhausted",
                    "severity": "warning",
                    "message": f"飞书上传阶段有 {int(upload_layer.get('exhausted_request_count') or 0)} 个请求在重试后仍未恢复，建议优先检查飞书限流或网络稳定性。",
                }
            )

    duration_rows = sorted(
        _extract_platform_stage_duration_rows(dict(summary.get("platforms") or {})),
        key=lambda item: float(item.get("duration_seconds") or 0.0),
        reverse=True,
    )
    if duration_rows and float(duration_rows[0].get("duration_seconds") or 0.0) > 0:
        slowest = duration_rows[0]
        conclusions.append(
            {
                "layer": "screening_execution",
                "code": "slowest_stage_detected",
                "severity": "info",
                "message": (
                    f"当前观测到的主要耗时瓶颈在 {slowest['platform']} / {slowest['stage']}，"
                    f"耗时约 {round(float(slowest.get('duration_seconds') or 0.0), 1)} 秒。"
                ),
            }
        )

    headline = conclusions[0]["message"] if conclusions else "下游 summary 观测口径已就绪。"
    return {
        "headline": headline,
        "conclusions": conclusions,
    }


def _refresh_downstream_observability(summary: dict[str, Any]) -> None:
    summary["observability"] = _build_downstream_observability(summary)
    incremental_layer = dict(((summary.get("observability") or {}).get("layers") or {}).get("incremental_creator") or {})
    summary["partial_refresh_count"] = int(incremental_layer.get("partial_refresh_count") or 0)
    summary["partial_refresh_preview"] = list(incremental_layer.get("partial_refresh_preview") or [])[:10]
    summary["partial_refresh_breakdown"] = dict(incremental_layer.get("partial_refresh_breakdown") or {})
    summary["diagnostics"] = _build_downstream_diagnostics(summary)


def _build_feishu_open_client(
    *,
    runtime: dict[str, Any],
    env_file: str | Path,
):
    load_local_env = runtime["load_local_env"]
    get_preferred_value = runtime["get_preferred_value"]
    FeishuOpenClient = runtime["FeishuOpenClient"]
    default_base_url = runtime["DEFAULT_FEISHU_BASE_URL"]

    env_values = load_local_env(env_file)
    app_id = get_preferred_value("", env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value("", env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 shell 环境里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 shell 环境里填写。")
    timeout_raw = get_preferred_value("", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw or "30")
    base_url = get_preferred_value("", env_values, "FEISHU_OPEN_BASE_URL", default_base_url)
    return FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )


def _prepare_existing_bitable_prefilter(
    *,
    runtime: dict[str, Any],
    env_file: str | Path,
    linked_bitable_url: str,
    warnings_bucket: dict[str, Any],
) -> tuple[dict[str, Any], Any | None]:
    summary = {
        "enabled": False,
        "status": "disabled",
        "linked_bitable_url": str(linked_bitable_url or "").strip(),
        "target_url": "",
        "target_table_id": "",
        "target_table_name": "",
        "key_field_names": [],
        "owner_scope_field_name": "",
        "duplicate_existing_group_count": 0,
        "error": "",
    }
    normalized_linked_bitable_url = str(linked_bitable_url or "").strip()
    if not normalized_linked_bitable_url:
        summary["error"] = "linked_bitable_url is empty"
        return summary, None

    fetch_existing_bitable_record_analysis = runtime.get("fetch_existing_bitable_record_analysis")
    if fetch_existing_bitable_record_analysis is None:
        from feishu_screening_bridge.bitable_upload import fetch_existing_bitable_record_analysis

    build_feishu_open_client = runtime.get("build_feishu_open_client")
    try:
        feishu_client = (
            build_feishu_open_client(runtime=runtime, env_file=env_file)
            if callable(build_feishu_open_client)
            else _build_feishu_open_client(runtime=runtime, env_file=env_file)
        )
        resolved_view, analysis = fetch_existing_bitable_record_analysis(
            feishu_client,
            linked_bitable_url=normalized_linked_bitable_url,
        )
    except Exception as exc:  # noqa: BLE001
        summary["status"] = "failed"
        summary["error"] = str(exc) or exc.__class__.__name__
        warnings_bucket["existing_bitable_prefilter_unavailable"] = {
            "linked_bitable_url": normalized_linked_bitable_url,
            "error": summary["error"],
        }
        return summary, None

    summary.update(
        {
            "enabled": True,
            "status": "ready",
            "target_url": str(getattr(resolved_view, "source_url", "") or "").strip(),
            "target_table_id": str(getattr(resolved_view, "table_id", "") or "").strip(),
            "target_table_name": str(getattr(resolved_view, "table_name", "") or "").strip(),
            "key_field_names": list(getattr(analysis, "key_field_names", ()) or ()),
            "owner_scope_field_name": str(getattr(analysis, "owner_scope_field_name", "") or "").strip(),
            "duplicate_existing_group_count": len(list(getattr(analysis, "duplicate_groups", []) or [])),
        }
    )
    return summary, analysis


def _build_platform_quality_report(platform: str, platform_summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(platform_summary or {})
    artifact_status = dict(payload.get("artifact_status") or {})
    visual_job = dict(payload.get("visual_job") or {})
    visual_gate = dict(payload.get("visual_gate") or {})

    staged_identifier_count = _coerce_non_negative_int(payload.get("staged_identifier_count")) or 0
    requested_identifier_count = _coerce_non_negative_int(payload.get("requested_identifier_count")) or 0
    profile_review_count = _coerce_non_negative_int(payload.get("profile_review_count"))
    if profile_review_count is None:
        profile_review_count = _coerce_non_negative_int(artifact_status.get("profile_review_count"))
    prescreen_pass_count = _coerce_non_negative_int(payload.get("prescreen_pass_count")) or 0
    missing_profile_count = _coerce_non_negative_int(payload.get("missing_profile_count"))
    if missing_profile_count is None:
        missing_profile_count = _coerce_non_negative_int(artifact_status.get("missing_profile_count")) or 0

    has_visual_review_count = artifact_status.get("visual_review_count") is not None
    visual_review_count = _coerce_non_negative_int(artifact_status.get("visual_review_count"))
    if visual_review_count is None and has_visual_review_count:
        visual_review_count = 0

    platform_report: dict[str, Any] = {
        "platform": platform,
        "platform_status": str(payload.get("status") or "").strip(),
        "staged_identifier_count": staged_identifier_count,
        "requested_identifier_count": requested_identifier_count,
        "profile_review_count": profile_review_count,
        "prescreen_pass_count": prescreen_pass_count,
        "missing_profile_count": missing_profile_count,
        "visual_review_count": visual_review_count,
        "visual_job_status": str(visual_job.get("status") or "").strip(),
        "issues": [],
        "status": "ok",
    }

    if missing_profile_count > 0:
        platform_report["issues"].append(
            {
                "code": "missing_profiles",
                "severity": "warning",
                "count": missing_profile_count,
                "message": f"{platform} 抓取结果缺少 {missing_profile_count} 个名单账号。",
                "reason": _first_non_empty_text(
                    payload.get("reason"),
                    visual_gate.get("reason"),
                    (payload.get("scrape_job") or {}).get("message"),
                    (payload.get("scrape_job") or {}).get("error"),
                ),
            }
        )

    visual_required = (
        prescreen_pass_count > 0
        and not bool(visual_gate.get("skip_visual_flag"))
        and missing_profile_count == 0
    )
    if visual_required and has_visual_review_count:
        visual_gap_count = max(0, prescreen_pass_count - (visual_review_count or 0))
        platform_report["visual_expected_count"] = prescreen_pass_count
        platform_report["visual_gap_count"] = visual_gap_count
        if visual_gap_count > 0:
            platform_report["issues"].append(
                {
                    "code": "visual_coverage_gap",
                    "severity": "warning",
                    "count": visual_gap_count,
                    "expected_count": prescreen_pass_count,
                    "actual_count": visual_review_count or 0,
                    "message": (
                        f"{platform} 视觉复核缺少 {visual_gap_count}/{prescreen_pass_count} 个 Pass 账号结果。"
                    ),
                    "reason": _first_non_empty_text(
                        visual_job.get("error"),
                        visual_job.get("message"),
                        visual_job.get("reason"),
                        ((payload.get("visual_retry") or {}).get("reason")),
                    ),
                }
            )

    if platform_report["issues"]:
        platform_report["status"] = "warning"
    return platform_report


def build_quality_report(platforms: dict[str, dict[str, Any]]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "status": "ok",
        "warning_count": 0,
        "warnings": [],
        "platforms": {},
    }
    for platform, platform_summary in (platforms or {}).items():
        platform_report = _build_platform_quality_report(platform, platform_summary)
        report["platforms"][platform] = platform_report
        for issue in platform_report.get("issues") or []:
            report["warnings"].append(
                {
                    "platform": platform,
                    **issue,
                }
            )
    report["warning_count"] = len(report["warnings"])
    if report["warning_count"] > 0:
        report["status"] = "warning"
    return report


def _extract_scrape_partial_result(scrape_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        partial_result = result.get("partial_result")
        if isinstance(partial_result, dict):
            return dict(partial_result)
    partial_result = payload.get("partial_result")
    if isinstance(partial_result, dict):
        return dict(partial_result)
    return {}


def _extract_scrape_profile_reviews(scrape_job: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        profile_reviews = result.get("profile_reviews")
        if isinstance(profile_reviews, list):
            return [dict(item) for item in profile_reviews if isinstance(item, dict)]
    partial_result = _extract_scrape_partial_result(payload)
    profile_reviews = partial_result.get("profile_reviews")
    if isinstance(profile_reviews, list):
        return [dict(item) for item in profile_reviews if isinstance(item, dict)]
    return []


def _scrape_has_partial_result(scrape_job: dict[str, Any] | None) -> bool:
    partial_result = _extract_scrape_partial_result(scrape_job)
    if not partial_result:
        return False
    if partial_result.get("profile_reviews"):
        return True
    if partial_result.get("successful_identifiers"):
        return True
    return bool(int(partial_result.get("raw_count") or 0))


def _resolve_scrape_pass_count(scrape_job: dict[str, Any], count_passed_profiles) -> int:
    profile_reviews = _extract_scrape_profile_reviews(scrape_job)
    if profile_reviews:
        return len(
            [
                item
                for item in profile_reviews
                if str((item or {}).get("status") or "").strip() == "Pass"
            ]
        )
    return int(count_passed_profiles(scrape_job) or 0)


def _extract_missing_profile_reviews(scrape_job: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        item
        for item in _extract_scrape_profile_reviews(scrape_job)
        if str((item or {}).get("status") or "").strip() == "Missing"
    ]


def _extract_fallback_profile_reviews(scrape_job: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        item
        for item in _extract_scrape_profile_reviews(scrape_job)
        if str((item or {}).get("status") or "").strip() in {"Missing", "Reject"}
    ]


def _build_missing_profile_summary(profile_reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for item in profile_reviews:
        upload_metadata = dict(item.get("upload_metadata") or {})
        identifier = str(item.get("username") or upload_metadata.get("handle") or "").strip()
        summaries.append({
            "identifier": identifier,
            "profile_url": str(item.get("profile_url") or upload_metadata.get("url") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
        })
    return summaries


def _build_fallback_profile_summary(profile_reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for item in profile_reviews:
        upload_metadata = dict(item.get("upload_metadata") or {})
        identifier = str(item.get("username") or upload_metadata.get("handle") or "").strip()
        summaries.append(
            {
                "identifier": identifier,
                "profile_url": str(item.get("profile_url") or upload_metadata.get("url") or "").strip(),
                "reason": str(item.get("reason") or "").strip(),
                "status": str(item.get("status") or "").strip(),
            }
        )
    return summaries


def _extract_scrape_failure_stage(scrape_job: dict[str, Any] | None) -> str:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        stage = str(result.get("failure_stage") or "").strip()
        if stage:
            return stage
    return str(payload.get("stage") or "").strip()


def _extract_scrape_apify_metadata(scrape_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        apify = result.get("apify")
        if isinstance(apify, dict):
            return dict(apify)
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
            return dict(metadata)
    return {}


def _resolve_platform_stage_group(current_stage: str) -> str:
    normalized = str(current_stage or "").strip().lower()
    if not normalized:
        return ""
    if normalized in {"platform_preparing", "platform_preparing_failed", "platform_skipped"}:
        return "platform_prepare"
    if normalized == "incremental_filter_completed":
        return "incremental_creator"
    if normalized.startswith("scrape") or normalized == "fallback_staging_failed":
        return "scrape"
    if normalized.startswith("visual"):
        return "visual"
    if normalized.startswith("positioning_card_analysis"):
        return "positioning"
    if normalized in {"exporting_artifacts", "artifact_export_failed", "completed"}:
        return "export"
    return ""


def _platform_stage_is_terminal(current_stage: str) -> bool:
    normalized = str(current_stage or "").strip().lower()
    return normalized in {
        "platform_preparing_failed",
        "platform_skipped",
        "incremental_filter_completed",
        "scrape_skipped",
        "scrape_failed",
        "scrape_completed",
        "scrape_partial_ready",
        "visual_runtime_failed",
        "visual_retry_failed",
        "artifact_export_failed",
        "completed",
    }


def _find_open_platform_stage_group(stage_metrics: dict[str, dict[str, Any]]) -> str:
    open_group = ""
    for group_name, payload in stage_metrics.items():
        if not isinstance(payload, dict):
            continue
        if str(payload.get("started_at") or "").strip() and not str(payload.get("finished_at") or "").strip():
            open_group = str(group_name)
    return open_group


def _close_platform_stage_metric(
    stage_metrics: dict[str, dict[str, Any]],
    *,
    group_name: str,
    observed_at: str,
    status: str = "",
) -> None:
    entry = stage_metrics.setdefault(group_name, {})
    if not str(entry.get("started_at") or "").strip():
        entry["started_at"] = observed_at
    if not str(entry.get("finished_at") or "").strip():
        entry["finished_at"] = observed_at
    duration_seconds = _compute_elapsed_seconds(entry.get("started_at"), entry.get("finished_at"))
    if duration_seconds is not None:
        entry["duration_seconds"] = duration_seconds
    if str(status or "").strip():
        entry["status"] = str(status).strip()


def _update_platform_stage_metrics(
    platform_summary: dict[str, Any],
    *,
    current_stage: str,
    observed_at: str,
    status: str = "",
) -> None:
    stage_group = _resolve_platform_stage_group(current_stage)
    if not stage_group:
        return
    stage_metrics = platform_summary.setdefault("stage_metrics", {})
    open_group = _find_open_platform_stage_group(stage_metrics)
    if open_group and open_group != stage_group:
        _close_platform_stage_metric(
            stage_metrics,
            group_name=open_group,
            observed_at=observed_at,
            status=str(platform_summary.get("status") or "").strip(),
        )

    entry = stage_metrics.setdefault(
        stage_group,
        {
            "started_at": observed_at,
            "finished_at": "",
            "duration_seconds": 0.0,
            "latest_stage": current_stage,
            "status": "running",
        },
    )
    if not str(entry.get("started_at") or "").strip():
        entry["started_at"] = observed_at
    entry["latest_stage"] = current_stage
    if not str(entry.get("status") or "").strip():
        entry["status"] = "running"

    if _platform_stage_is_terminal(current_stage):
        _close_platform_stage_metric(
            stage_metrics,
            group_name=stage_group,
            observed_at=observed_at,
            status=str(status or platform_summary.get("status") or current_stage).strip(),
        )


def _persist_platform_summary(
    *,
    summary: dict[str, Any],
    run_summary_path: Path,
    backend_app,
    platform: str,
    platform_summary: dict[str, Any],
    current_stage: str | None = None,
    status: str | None = None,
    summary_writer: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    observed_at = backend_app.iso_now()
    if current_stage is not None:
        _update_platform_stage_metrics(
            platform_summary,
            current_stage=current_stage,
            observed_at=observed_at,
            status=str(status or platform_summary.get("status") or "").strip(),
        )
        platform_summary["current_stage"] = current_stage
    if status is not None:
        platform_summary["status"] = status
    platform_summary["last_updated_at"] = observed_at
    summary["platforms"][platform] = platform_summary
    if summary_writer is not None:
        summary_writer(summary)
    else:
        _write_summary(run_summary_path, summary)


def _build_positioning_stage_payload(status: str, reason: str = "", **extra: Any) -> dict[str, Any]:
    payload = {
        "status": str(status or "").strip(),
        "reason": str(reason or "").strip(),
    }
    payload.update({key: value for key, value in extra.items() if value not in (None, "")})
    return payload


def _mark_platform_runtime_failure(
    *,
    summary: dict[str, Any],
    run_summary_path: Path,
    backend_app,
    platform: str,
    platform_summary: dict[str, Any],
    exc: Exception,
    current_stage: str,
    summary_writer: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    platform_summary["status"] = "failed"
    platform_summary["error_code"] = "PLATFORM_RUNTIME_FAILED"
    platform_summary["error"] = str(exc) or exc.__class__.__name__
    platform_summary["exception_type"] = exc.__class__.__name__
    _persist_platform_summary(
        summary=summary,
        run_summary_path=run_summary_path,
        backend_app=backend_app,
        platform=platform,
        platform_summary=platform_summary,
        current_stage=current_stage,
        summary_writer=summary_writer,
    )


def _build_resolved_config_sources(
    *,
    env_file: str | Path,
    keep_workbook: Path,
    template_workbook: Path | None,
    task_name: str,
    task_upload_url: str,
    platform_filters: list[str] | None,
    vision_provider: str,
    max_identifiers_per_platform: int,
    poll_interval: float,
    dry_run: bool,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
    output_root_source: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_keep_list_downstream_config(
        env_file=env_file,
        keep_workbook=keep_workbook,
        template_workbook=template_workbook,
        task_name=task_name,
        task_upload_url=task_upload_url,
        platform_filters=platform_filters,
        vision_provider=vision_provider,
        max_identifiers_per_platform=max_identifiers_per_platform,
        poll_interval=poll_interval,
        dry_run=dry_run,
        probe_vision_provider_only=probe_vision_provider_only,
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
        output_root_source=output_root_source,
    )


def _build_downstream_preflight(
    *,
    keep_workbook: Path,
    template_workbook: Path | None,
    env_snapshot: Any,
    run_root: Path,
    screening_data_dir: Path,
    config_dir: Path,
    temp_dir: Path,
    exports_dir: Path,
    downloads_dir: Path,
    requested_platforms: list[str],
    dry_run: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
    visual_postcheck_max_rounds: int,
    probe_vision_provider_only: bool,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    run_root_target = inspect_directory_materialization_target(run_root)
    screening_data_dir_target = inspect_directory_materialization_target(screening_data_dir)
    config_dir_target = inspect_directory_materialization_target(config_dir)
    temp_dir_target = inspect_directory_materialization_target(temp_dir)
    exports_dir_target = inspect_directory_materialization_target(exports_dir)
    downloads_dir_target = inspect_directory_materialization_target(downloads_dir)
    if not keep_workbook.exists():
        errors.append(
            build_preflight_error(
                error_code="KEEP_WORKBOOK_MISSING",
                message=f"keep workbook 不存在: {keep_workbook.resolve()}",
                remediation="先确认上游 keep-list 已生成，或通过 `--keep-workbook` 指向真实存在的 `*_keep.xlsx` 文件。",
                details={"path": str(keep_workbook.resolve())},
            )
        )
    if template_workbook is not None and not template_workbook.exists():
        errors.append(
            build_preflight_error(
                error_code="TEMPLATE_WORKBOOK_MISSING",
                message=f"template workbook 不存在: {template_workbook.resolve()}",
                remediation="通过 `--template-workbook` 指向真实模板文件，或改为传 `--task-name` 让 staging 走任务上传模板下载。",
                details={"path": str(template_workbook.resolve())},
            )
        )
    for error_code, path_value, label, inspection in (
        ("RUN_ROOT_UNAVAILABLE", run_root, "run_root", run_root_target),
        ("SCREENING_DATA_DIR_UNAVAILABLE", screening_data_dir, "screening_data_dir", screening_data_dir_target),
        ("CONFIG_DIR_UNAVAILABLE", config_dir, "config_dir", config_dir_target),
        ("TEMP_DIR_UNAVAILABLE", temp_dir, "temp_dir", temp_dir_target),
        ("EXPORTS_DIR_UNAVAILABLE", exports_dir, "exports_dir", exports_dir_target),
        ("DOWNLOADS_DIR_UNAVAILABLE", downloads_dir, "downloads_dir", downloads_dir_target),
    ):
        if not bool(inspection["materializable"]):
            errors.append(
                build_preflight_error(
                    error_code=error_code,
                    message=f"{label} 无法创建: {path_value}",
                    remediation="检查输出路径权限，或显式传入可写目录后重试。",
                    details={
                        "path": str(path_value),
                        "nearest_existing_parent": str(inspection["nearest_existing_parent"]),
                    },
                )
            )
    return build_preflight_payload(
        checks={
            "scope": "keep-list-screening",
            "lightweight_only": True,
            "env_file_exists": bool(getattr(env_snapshot, "exists", False)),
            "keep_workbook_exists": keep_workbook.exists(),
            "template_input_mode": "template_workbook" if template_workbook else "task_upload_or_none",
            "template_workbook_exists": template_workbook.exists() if template_workbook else False,
            "requested_platforms": requested_platforms,
            "dry_run": bool(dry_run),
            "skip_scrape": bool(skip_scrape),
            "skip_visual": bool(skip_visual),
            "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
            "visual_postcheck_max_rounds": max(0, int(visual_postcheck_max_rounds)),
            "probe_vision_provider_only": bool(probe_vision_provider_only),
            "run_root_materializable": bool(run_root_target["materializable"]),
            "screening_data_dir_materializable": bool(screening_data_dir_target["materializable"]),
            "config_dir_materializable": bool(config_dir_target["materializable"]),
            "temp_dir_materializable": bool(temp_dir_target["materializable"]),
            "exports_dir_materializable": bool(exports_dir_target["materializable"]),
            "downloads_dir_materializable": bool(downloads_dir_target["materializable"]),
        },
        errors=errors,
    )


def _extract_visual_partial_result(visual_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(visual_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        partial_result = result.get("partial_result")
        if isinstance(partial_result, dict):
            return dict(partial_result)
        if result.get("visual_results") or result.get("summary"):
            return dict(result)
    partial_result = payload.get("partial_result")
    if isinstance(partial_result, dict):
        return dict(partial_result)
    return {}


def _extract_visual_results_map(platform: str, visual_job: dict[str, Any] | None, backend_app) -> dict[str, dict[str, Any]]:
    partial_result = _extract_visual_partial_result(visual_job)
    visual_results = partial_result.get("visual_results")
    if isinstance(visual_results, dict) and visual_results:
        return {
            str(key or (value or {}).get("username") or "").strip(): dict(value)
            for key, value in visual_results.items()
            if isinstance(value, dict) and str(key or (value or {}).get("username") or "").strip()
        }
    loader = getattr(backend_app, "load_visual_results", None)
    if callable(loader):
        loaded = loader(platform)
        if isinstance(loaded, dict):
            return {
                str(key or (value or {}).get("username") or "").strip(): dict(value)
                for key, value in loaded.items()
                if isinstance(value, dict) and str(key or (value or {}).get("username") or "").strip()
            }
    return {}


def _extract_failed_visual_identifiers(platform: str, visual_job: dict[str, Any] | None, backend_app) -> list[str]:
    visual_results = _extract_visual_results_map(platform, visual_job, backend_app)
    failed_identifiers: list[str] = []
    seen: set[str] = set()
    for key, item in visual_results.items():
        identifier = str(key or item.get("username") or "").strip()
        if not identifier or identifier in seen:
            continue
        if item.get("success") is False:
            failed_identifiers.append(identifier)
            seen.add(identifier)
    return failed_identifiers


def _run_visual_postcheck_retries(
    *,
    client,
    backend_app,
    platform: str,
    platform_summary: dict[str, Any],
    vision_provider: str,
    creator_cache_db_path: str,
    force_refresh_creator_cache: bool,
    poll_job,
    require_success,
    poll_interval: float,
    max_rounds: int,
) -> dict[str, Any]:
    retry_summary: dict[str, Any] = {
        "enabled": True,
        "max_rounds": max(0, int(max_rounds)),
        "rounds": [],
        "initial_error_count": 0,
        "final_error_count": 0,
        "status": "not_needed",
        "reason": "",
    }
    visual_job = dict(platform_summary.get("visual_job") or {})
    if not visual_job:
        retry_summary["status"] = "skipped"
        retry_summary["reason"] = "visual job was not started"
        return retry_summary

    failed_identifiers = _extract_failed_visual_identifiers(platform, visual_job, backend_app)
    retry_summary["initial_error_count"] = len(failed_identifiers)
    retry_summary["final_error_count"] = len(failed_identifiers)
    if not failed_identifiers:
        return retry_summary
    if retry_summary["max_rounds"] <= 0:
        retry_summary["status"] = "exhausted"
        retry_summary["reason"] = "visual postcheck retry disabled by max rounds"
        return retry_summary

    remaining = list(failed_identifiers)
    for round_index in range(1, retry_summary["max_rounds"] + 1):
        visual_payload_body = build_visual_payload(
            platform,
            remaining,
            creator_cache_db_path=creator_cache_db_path,
            force_refresh_creator_cache=force_refresh_creator_cache,
        )
        if vision_provider:
            visual_payload_body["provider"] = str(vision_provider).strip().lower()
        retry_payload = require_success(
            client.post("/api/jobs/visual-review", json={"platform": platform, "payload": visual_payload_body}),
            f"{platform} visual retry round {round_index} start",
        )
        retry_job = poll_job(
            client,
            retry_payload["job"]["id"],
            f"{platform} visual retry round {round_index} poll",
            max(1.0, float(poll_interval)),
        )
        unresolved = _extract_failed_visual_identifiers(platform, retry_job, backend_app)
        retry_summary["rounds"].append({
            "round": round_index,
            "requested_identifier_count": len(remaining),
            "requested_identifier_preview": remaining[:10],
            "job": retry_job,
            "resolved_count": max(0, len(remaining) - len(unresolved)),
            "remaining_error_count": len(unresolved),
        })
        remaining = unresolved
        if not remaining:
            retry_summary["status"] = "completed"
            retry_summary["final_error_count"] = 0
            retry_summary["reason"] = "all failed visual rows were recovered by postcheck rerun"
            return retry_summary

    retry_summary["status"] = "exhausted"
    retry_summary["final_error_count"] = len(remaining)
    retry_summary["remaining_identifier_preview"] = remaining[:10]
    retry_summary["reason"] = "visual postcheck rerun exhausted max rounds with unresolved failures"
    return retry_summary


def run_keep_list_screening_pipeline(
    *,
    keep_workbook: Path,
    template_workbook: Path | None = None,
    task_name: str = "",
    task_upload_url: str = "",
    env_file: str | Path = ".env",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    platform_filters: list[str] | None = None,
    vision_provider: str = "",
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    dry_run: bool = False,
    probe_vision_provider_only: bool = False,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    visual_postcheck_max_rounds: int = 3,
    include_pinned_posts: bool = False,
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
    task_owner_name: str = "",
    task_owner_employee_id: str = "",
    task_owner_employee_record_id: str = "",
    task_owner_employee_email: str = "",
    task_owner_owner_name: str = "",
    linked_bitable_url: str = "",
) -> dict[str, Any]:
    normalized_task_name = str(task_name or "").strip()
    runner_paths = resolve_keep_list_downstream_paths(
        task_name=normalized_task_name or "task",
        output_root=output_root,
        summary_json=summary_json,
    )
    resolved_output_root = runner_paths.output_root
    run_summary_path = runner_paths.summary_json
    staging_summary_path = runner_paths.staging_summary_json
    screening_data_dir = runner_paths.screening_data_dir
    config_dir = runner_paths.config_dir
    temp_dir = runner_paths.temp_dir
    exports_dir = runner_paths.exports_dir
    downloads_dir = runner_paths.downloads_dir
    requested_platforms = normalize_platforms(platform_filters)
    execution_platforms = _expand_platforms_for_fallback(requested_platforms)
    resolved_config_sources, resolved_config = _build_resolved_config_sources(
        env_file=env_file,
        keep_workbook=keep_workbook,
        template_workbook=template_workbook,
        task_name=normalized_task_name,
        task_upload_url=task_upload_url,
        platform_filters=platform_filters,
        vision_provider=vision_provider,
        max_identifiers_per_platform=max_identifiers_per_platform,
        poll_interval=poll_interval,
        dry_run=dry_run,
        probe_vision_provider_only=probe_vision_provider_only,
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
        output_root_source=runner_paths.output_root_source,
    )
    resolved_keep_workbook = keep_workbook.expanduser()
    resolved_template_workbook = template_workbook.expanduser() if template_workbook else None
    inferred_task_owner = _infer_task_owner_from_adjacent_task_spec(keep_workbook=resolved_keep_workbook.resolve())
    normalized_task_owner_name = str(task_owner_name or inferred_task_owner.get("task_owner_name") or "").strip()
    normalized_task_owner_employee_id = str(
        task_owner_employee_id or inferred_task_owner.get("task_owner_employee_id") or ""
    ).strip()
    normalized_task_owner_employee_record_id = str(
        task_owner_employee_record_id or inferred_task_owner.get("task_owner_employee_record_id") or ""
    ).strip()
    normalized_task_owner_employee_email = str(
        task_owner_employee_email or inferred_task_owner.get("task_owner_employee_email") or ""
    ).strip()
    normalized_task_owner_owner_name = str(
        task_owner_owner_name or inferred_task_owner.get("task_owner_owner_name") or ""
    ).strip()
    normalized_linked_bitable_url = str(linked_bitable_url or inferred_task_owner.get("linked_bitable_url") or "").strip()
    if not normalized_task_name:
        normalized_task_name = str(inferred_task_owner.get("task_name") or "").strip()
    if not task_upload_url:
        task_upload_url = str(inferred_task_owner.get("task_upload_url") or "").strip()
    preflight = _build_downstream_preflight(
        keep_workbook=resolved_keep_workbook,
        template_workbook=resolved_template_workbook,
        env_snapshot=resolved_config["env_snapshot"],
        run_root=runner_paths.run_root,
        screening_data_dir=screening_data_dir,
        config_dir=config_dir,
        temp_dir=temp_dir,
        exports_dir=exports_dir,
        downloads_dir=downloads_dir,
        requested_platforms=requested_platforms,
        dry_run=bool(dry_run),
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
        visual_postcheck_max_rounds=visual_postcheck_max_rounds,
        probe_vision_provider_only=probe_vision_provider_only,
    )

    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "finished_at": "",
        "current_stage": "preflight",
        "run_id": runner_paths.run_id,
        "run_root": str(runner_paths.run_root),
        "keep_workbook": str(resolved_keep_workbook.resolve()),
        "template_workbook": str(resolved_template_workbook.resolve()) if resolved_template_workbook else "",
        "task_name": normalized_task_name,
        "task_upload_url": str(task_upload_url or "").strip(),
        "env_file_raw": str(env_file),
        "env_file": str(resolved_config["env_snapshot"].path),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "task_spec_json": str(runner_paths.task_spec_json),
        "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
        "staging_summary_json": str(staging_summary_path),
        "resolved_config_sources": resolved_config_sources,
        "resolved_inputs": {
            "env_file": {
                "path": str(resolved_config["env_snapshot"].path),
                "exists": resolved_config["env_snapshot"].exists,
                "source": resolved_config["env_snapshot"].source,
            },
            "keep_workbook": _path_summary(resolved_keep_workbook, source="cli_or_default", kind="file"),
            "template_workbook": _path_summary(
                resolved_template_workbook,
                source=("cli_or_default" if resolved_template_workbook else "task_upload_or_none"),
                kind="file",
            ),
            "output_dirs": {
                "output_root": _path_summary(resolved_output_root, source=runner_paths.output_root_source, kind="dir"),
                "screening_data_dir": _path_summary(screening_data_dir, source="output_root_default", kind="dir"),
                "config_dir": _path_summary(config_dir, source="output_root_default", kind="dir"),
                "temp_dir": _path_summary(temp_dir, source="output_root_default", kind="dir"),
                "exports_dir": _path_summary(exports_dir, source="output_root_default", kind="dir"),
                "downloads_dir": _path_summary(downloads_dir, source="output_root_default", kind="dir"),
            },
        },
        "preflight": preflight,
        "requested_platforms": requested_platforms,
        "execution_platforms": execution_platforms,
        "requested_vision_provider": str(vision_provider or "").strip().lower(),
        "max_identifiers_per_platform": int(max_identifiers_per_platform),
        "dry_run": bool(dry_run),
        "skip_scrape": bool(skip_scrape),
        "skip_visual": bool(skip_visual),
        "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
        "visual_postcheck_max_rounds": max(0, int(visual_postcheck_max_rounds)),
        "creator_cache_db_path": str(creator_cache_db_path or "").strip(),
        "force_refresh_creator_cache": bool(force_refresh_creator_cache),
        "resolved_task_owner": {
            "task_owner_name": normalized_task_owner_name,
            "task_owner_employee_id": normalized_task_owner_employee_id,
            "task_owner_employee_record_id": normalized_task_owner_employee_record_id,
            "task_owner_employee_email": normalized_task_owner_employee_email,
            "task_owner_owner_name": normalized_task_owner_owner_name,
            "linked_bitable_url": normalized_linked_bitable_url,
            "inferred_from_task_spec": str(inferred_task_owner.get("task_spec_path") or ""),
        },
        "probe_vision_provider_only": bool(probe_vision_provider_only),
        "vision_providers": [],
        "vision_preflight": {},
        "existing_bitable_prefilter": {},
        "staging": {},
        "platforms": {},
        "manual_review_rows": [],
        "dry_run_report": {},
        "warnings": {},
        "artifacts": {
            "keep_workbook": str(resolved_keep_workbook.resolve()),
            "template_workbook": str(resolved_template_workbook.resolve()) if resolved_template_workbook else "",
            "all_platforms_final_review": "",
            "all_platforms_upload_payload_json": "",
            "final_exports": {},
            "missing_profiles_xlsx": "",
            "success_report_xlsx": "",
            "error_report_xlsx": "",
        },
        "setup": {
            "scope": "keep-list-screening",
            "completed": False,
            "skipped": not preflight["ready"],
            "errors": [],
        },
        "observability": {},
        "diagnostics": {},
    }
    attach_run_contract(summary)
    _refresh_downstream_observability(summary)
    task_spec = build_keep_list_downstream_task_spec(
        generated_at=summary["started_at"],
        runner_paths=runner_paths,
        env_snapshot=resolved_config["env_snapshot"],
        env_file_raw=str(env_file),
        resolved_config_sources=resolved_config_sources,
        keep_workbook=resolved_keep_workbook.resolve(),
        template_workbook=resolved_template_workbook.resolve() if resolved_template_workbook else None,
        task_name=normalized_task_name,
        task_upload_url=resolved_config["task_upload_url"].value,
        requested_platforms=requested_platforms,
        vision_provider=str(vision_provider or "").strip().lower(),
        max_identifiers_per_platform=int(max_identifiers_per_platform),
        poll_interval=float(poll_interval),
        dry_run=bool(dry_run),
        probe_vision_provider_only=bool(probe_vision_provider_only),
        skip_scrape=bool(skip_scrape),
        skip_visual=bool(skip_visual),
        skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
        creator_cache_db_path=str(creator_cache_db_path or "").strip(),
        force_refresh_creator_cache=bool(force_refresh_creator_cache),
        task_owner_name=normalized_task_owner_name,
        task_owner_employee_id=normalized_task_owner_employee_id,
        task_owner_employee_record_id=normalized_task_owner_employee_record_id,
        task_owner_employee_email=normalized_task_owner_employee_email,
        task_owner_owner_name=normalized_task_owner_owner_name,
        linked_bitable_url=normalized_linked_bitable_url,
    )
    progress_scope = f"keep-list:{normalized_task_name or runner_paths.run_id}"
    progress_state: dict[str, Any] = {
        "status": "",
        "vision_probe_signature": "",
        "platform_signatures": {},
    }

    def persist_summary(payload: dict[str, Any]) -> None:
        _refresh_downstream_observability(payload)
        current_status = str(payload.get("status") or "").strip()
        if current_status and current_status != progress_state["status"]:
            progress_state["status"] = current_status
            _emit_runtime_progress(progress_scope, f"run_status={current_status}")

        vision_probe = payload.get("vision_probe")
        if isinstance(vision_probe, dict):
            probe_signature = "|".join(
                [
                    str(vision_probe.get("success")),
                    str(vision_probe.get("provider") or ""),
                    str(vision_probe.get("error_code") or ""),
                ]
            )
            if probe_signature != progress_state["vision_probe_signature"]:
                progress_state["vision_probe_signature"] = probe_signature
                probe_status = "passed" if vision_probe.get("success") else "failed"
                provider_name = str(vision_probe.get("provider") or "").strip()
                detail = f"vision_probe={probe_status}"
                if provider_name:
                    detail += f" provider={provider_name}"
                error_code = str(vision_probe.get("error_code") or "").strip()
                if error_code:
                    detail += f" error_code={error_code}"
                _emit_runtime_progress(progress_scope, detail)

        platform_signatures = progress_state["platform_signatures"]
        for platform, platform_payload in (payload.get("platforms") or {}).items():
            if not isinstance(platform_payload, dict):
                continue
            stage = str(platform_payload.get("current_stage") or "").strip()
            status = str(platform_payload.get("status") or "").strip()
            signature = (
                stage,
                status,
                int(platform_payload.get("profile_review_count") or 0),
                int(platform_payload.get("prescreen_pass_count") or 0),
                int(platform_payload.get("missing_profile_count") or 0),
            )
            if platform_signatures.get(platform) == signature:
                continue
            platform_signatures[platform] = signature
            detail_parts = []
            if stage:
                detail_parts.append(f"stage={stage}")
            if status:
                detail_parts.append(f"status={status}")
            if int(platform_payload.get("requested_identifier_count") or 0) > 0:
                detail_parts.append(f"requested={int(platform_payload.get('requested_identifier_count') or 0)}")
            if int(platform_payload.get("profile_review_count") or 0) > 0:
                detail_parts.append(f"reviewed={int(platform_payload.get('profile_review_count') or 0)}")
            if int(platform_payload.get("prescreen_pass_count") or 0) > 0:
                detail_parts.append(f"pass={int(platform_payload.get('prescreen_pass_count') or 0)}")
            if int(platform_payload.get("missing_profile_count") or 0) > 0:
                detail_parts.append(f"missing={int(platform_payload.get('missing_profile_count') or 0)}")
            _emit_runtime_progress(progress_scope, f"{platform} " + " ".join(detail_parts or ["updated"]))

        _write_summary(run_summary_path, payload)
        write_workflow_handoff(
            runner_paths.workflow_handoff_json,
            summary=payload,
            task_spec=task_spec,
            task_spec_available=bool(payload.get("setup", {}).get("completed")),
        )

    def _finalize_failure(
        *,
        failure: dict[str, Any],
        finished_at: str,
        status: str = "failed",
        expose_top_level: bool = True,
    ) -> dict[str, Any]:
        summary["status"] = status
        summary["finished_at"] = finished_at
        attach_failure_to_summary(summary, failure, expose_top_level=expose_top_level)
        attach_run_contract(summary)
        persist_summary(summary)
        return summary

    if not preflight["ready"]:
        failure = preflight["errors"][0]
        return _finalize_failure(
            failure={**failure, "failure_layer": "preflight"},
            finished_at=iso_now(),
        )

    _emit_runtime_progress(
        progress_scope,
        f"starting keep workbook={resolved_keep_workbook.resolve()} platforms={','.join(execution_platforms) or 'none'}",
    )
    summary["current_stage"] = "setup"

    setup = materialize_setup(
        scope="keep-list-screening",
        directories=[
            {
                "label": "run_root",
                "path": runner_paths.run_root,
                "error_code": "RUN_ROOT_UNAVAILABLE",
                "message": "run_root 无法创建: {path}",
                "remediation": "检查输出路径权限，或显式传入可写目录后重试。",
            },
            {
                "label": "screening_data_dir",
                "path": screening_data_dir,
                "error_code": "SCREENING_DATA_DIR_UNAVAILABLE",
                "message": "screening_data_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
            {
                "label": "config_dir",
                "path": config_dir,
                "error_code": "CONFIG_DIR_UNAVAILABLE",
                "message": "config_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
            {
                "label": "temp_dir",
                "path": temp_dir,
                "error_code": "TEMP_DIR_UNAVAILABLE",
                "message": "temp_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
            {
                "label": "exports_dir",
                "path": exports_dir,
                "error_code": "EXPORTS_DIR_UNAVAILABLE",
                "message": "exports_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
            {
                "label": "downloads_dir",
                "path": downloads_dir,
                "error_code": "DOWNLOADS_DIR_UNAVAILABLE",
                "message": "downloads_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
            {
                "label": "template_output_dir",
                "path": runner_paths.template_output_dir,
                "error_code": "TEMPLATE_OUTPUT_DIR_UNAVAILABLE",
                "message": "template_output_dir 无法创建: {path}",
                "remediation": "检查输出路径权限后重试。",
            },
        ],
        files=[
            {
                "label": "task_spec",
                "path": runner_paths.task_spec_json,
                "writer": lambda path: write_task_spec(path, task_spec),
                "error_code": "TASK_SPEC_WRITE_FAILED",
                "message": "task_spec 无法写入: {path}",
                "remediation": "检查 run root 权限或 task spec 序列化逻辑后重试。",
            }
        ],
    )
    summary["setup"] = {**setup, "skipped": False}
    if not setup["completed"]:
        failure = setup["errors"][0]
        return _finalize_failure(
            failure=failure,
            finished_at=iso_now(),
        )

    summary["resolved_inputs"]["output_dirs"] = {
        "output_root": _path_summary(resolved_output_root, source=runner_paths.output_root_source, kind="dir"),
        "screening_data_dir": _path_summary(screening_data_dir, source="output_root_default", kind="dir"),
        "config_dir": _path_summary(config_dir, source="output_root_default", kind="dir"),
        "temp_dir": _path_summary(temp_dir, source="output_root_default", kind="dir"),
        "exports_dir": _path_summary(exports_dir, source="output_root_default", kind="dir"),
        "downloads_dir": _path_summary(downloads_dir, source="output_root_default", kind="dir"),
    }

    try:
        summary["current_stage"] = "runtime_init"
        runtime = _load_runtime_dependencies()
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="runtime_import",
            error_code="SCREENING_RUNTIME_IMPORT_FAILED",
            message=f"筛号 downstream runtime 加载失败: {exc}",
            remediation="先补齐 `backend` 与筛号相关本地依赖，再重试 keep-list downstream run。",
            details={"exception_type": exc.__class__.__name__},
        )
        return _finalize_failure(
            failure=failure,
            finished_at=iso_now(),
        )

    backend_app = runtime["backend_app"]
    if "build_all_platforms_final_review_artifacts" in runtime and "collect_final_exports" in runtime:
        build_all_platforms_final_review_artifacts = runtime["build_all_platforms_final_review_artifacts"]
        collect_final_exports = runtime["collect_final_exports"]
    else:
        from backend.final_export_merge import build_all_platforms_final_review_artifacts, collect_final_exports

    prepare_screening_inputs = runtime["prepare_screening_inputs"]
    snapshot_backend_runtime_state = runtime.get("snapshot_backend_runtime_state", lambda: {})
    restore_backend_runtime_state = runtime.get("restore_backend_runtime_state", lambda snapshot: None)
    count_passed_profiles = runtime["count_passed_profiles"]
    export_platform_artifacts = runtime["export_platform_artifacts"]
    poll_job = runtime["poll_job"]
    require_success = runtime["require_success"]
    reset_backend_runtime_state = runtime["reset_backend_runtime_state"]
    upload_final_review_payload_to_bitable = runtime.get("upload_final_review_payload_to_bitable")
    if upload_final_review_payload_to_bitable is None:
        from feishu_screening_bridge.bitable_upload import upload_final_review_payload_to_bitable
    runtime_snapshot = snapshot_backend_runtime_state()

    try:
        try:
            summary["current_stage"] = "staging"
            _emit_runtime_progress(progress_scope, "staging_inputs=running")
            reset_backend_runtime_state()
            staging_summary = prepare_screening_inputs(
                creator_workbook=resolved_keep_workbook.resolve(),
                template_workbook=resolved_template_workbook.resolve() if resolved_template_workbook else None,
                task_name=normalized_task_name,
                task_upload_url=str(task_upload_url or "").strip(),
                env_file=env_file,
                task_download_dir=downloads_dir,
                template_output_dir=runner_paths.template_output_dir,
                screening_data_dir=screening_data_dir,
                config_dir=config_dir,
                temp_dir=temp_dir,
                summary_json=staging_summary_path,
            )
        except Exception as exc:  # noqa: BLE001
            failure = _build_failure_payload(
                stage="staging",
                error_code="SCREENING_STAGING_FAILED",
                message=str(exc) or exc.__class__.__name__,
                remediation="检查 keep workbook、模板输入、任务上传相关 env，以及 staging summary 的 resolved_inputs 后重试。",
                details={"exception_type": exc.__class__.__name__},
            )
            return _finalize_failure(
                failure=failure,
                finished_at=backend_app.iso_now(),
            )

        summary["started_at"] = backend_app.iso_now()
        summary["staging"] = staging_summary
        _emit_runtime_progress(progress_scope, "staging_inputs=completed")
        summary["vision_providers"] = backend_app.get_available_vision_provider_names()
        summary["vision_preflight"] = backend_app.build_vision_preflight(vision_provider)
        if dry_run:
            summary["vision_probe"] = {
                "status": "skipped",
                "reason": "dry_run flag set",
            }
        elif skip_scrape and not probe_vision_provider_only:
            summary["vision_probe"] = {
                "status": "skipped",
                "reason": "skip_scrape flag set",
            }
        summary["preflight"]["ready"] = True
        summary["preflight"]["errors"] = []
        persist_summary(summary)

        try:
            client = backend_app.app.test_client()
            resolve_routing_strategy = getattr(backend_app, "resolve_visual_review_routing_strategy", None)
            active_routing_strategy = ""
            if callable(resolve_routing_strategy):
                active_routing_strategy = str(resolve_routing_strategy({}) or "").strip().lower()
            if ((not skip_scrape and not skip_visual) or probe_vision_provider_only) and not dry_run:
                summary["current_stage"] = "vision_probe"
                _emit_runtime_progress(progress_scope, "vision_probe=running")
                if (
                    not str(vision_provider or "").strip()
                    and active_routing_strategy == "probe_ranked"
                    and hasattr(backend_app, "run_probe_ranked_visual_provider_race")
                ):
                    race_payload = backend_app.run_probe_ranked_visual_provider_race(
                        platform=execution_platforms[0] if execution_platforms else "instagram"
                    )
                    probe_payload = {
                        "success": bool(race_payload.get("success")),
                        "provider": race_payload.get("selected_provider") or "",
                        "probe": {
                            "success": bool(race_payload.get("success")),
                            "provider": race_payload.get("selected_provider") or "",
                            "model": race_payload.get("selected_model") or "",
                            "checked_at": race_payload.get("checked_at") or "",
                        },
                        "channel_race": race_payload,
                        "vision_preflight": summary["vision_preflight"],
                    }
                    probe_status_code = 200 if probe_payload.get("success") else 400
                    if not probe_payload.get("success"):
                        probe_payload["error_code"] = "VISION_CHANNEL_RACE_FAILED"
                        probe_payload["error"] = "视觉通道赛马失败：当前优先链路都不可用。"
                else:
                    probe_response = client.post("/api/vision/providers/probe", json={"provider": vision_provider or ""})
                    probe_payload = probe_response.get_json(silent=True) or {
                        "success": False,
                        "error": f"unexpected HTTP {probe_response.status_code}",
                    }
                    probe_status_code = probe_response.status_code
                summary["vision_probe"] = probe_payload
                summary["vision_preflight"] = probe_payload.get("vision_preflight") or summary["vision_preflight"]
                if probe_status_code >= 400 or probe_payload.get("success") is False:
                    failure = _build_failure_payload(
                        stage="vision_probe",
                        error_code=str(
                            probe_payload.get("error_code")
                            or (probe_payload.get("vision_preflight") or {}).get("error_code")
                            or "VISION_PROVIDER_PROBE_FAILED"
                        ),
                        message=str(probe_payload.get("error") or "视觉 provider probe 失败"),
                        remediation="检查 vision preflight、provider 配置和可运行通道后重试。",
                    )
                    return _finalize_failure(
                        failure=failure,
                        finished_at=backend_app.iso_now(),
                        status="vision_probe_failed",
                        expose_top_level=False,
                    )
            if probe_vision_provider_only and not dry_run:
                summary["status"] = "vision_probe_only"
                summary["finished_at"] = backend_app.iso_now()
                attach_run_contract(summary)
                persist_summary(summary)
                return summary

            summary["current_stage"] = "incremental_prefilter"
            existing_bitable_prefilter, existing_bitable_analysis = _prepare_existing_bitable_prefilter(
                runtime=runtime,
                env_file=env_file,
                linked_bitable_url=normalized_linked_bitable_url,
                warnings_bucket=summary.setdefault("warnings", {}),
            )
            summary["existing_bitable_prefilter"] = existing_bitable_prefilter
            persist_summary(summary)

            summary["current_stage"] = "screening_execution"
            mail_only_updates_by_platform: dict[str, list[dict[str, Any]]] = {}
            for platform in execution_platforms:
                platform_summary: dict[str, Any] = {
                    "staged_identifier_count": 0,
                    "requested_identifier_count": 0,
                    "requested_identifier_preview": [],
                    "mail_only_update_count": 0,
                    "mail_only_update_preview": [],
                    "partial_refresh_count": 0,
                    "partial_refresh_preview": [],
                    "partial_refresh_breakdown": {},
                    "requested_vision_provider": str(vision_provider or "").strip().lower(),
                    "vision_preflight": backend_app.build_vision_preflight(vision_provider),
                    "incremental_prefilter": {
                        "enabled": False,
                        "status": "disabled",
                        "existing_bitable_match_count": 0,
                        "existing_bitable_match_preview": [],
                        "existing_screened_count": 0,
                        "existing_screened_preview": [],
                        "existing_unscreened_count": 0,
                        "existing_unscreened_preview": [],
                        "incremental_candidate_count": 0,
                        "incremental_candidate_preview": [],
                        "full_screening_candidate_count": 0,
                        "full_screening_candidate_preview": [],
                        "mail_only_update_count": 0,
                        "mail_only_update_preview": [],
                        "partial_refresh_count": 0,
                        "partial_refresh_preview": [],
                        "partial_refresh_breakdown": {},
                        "duplicate_existing_group_count": 0,
                        "all_existing": False,
                        "mail_only_update_only": False,
                    },
                    "status": "running",
                    "current_stage": "platform_preparing",
                }
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    summary_writer=persist_summary,
                )
                try:
                    identifier_plan = _build_platform_identifier_plan(
                        backend_app,
                        platform,
                        max_identifiers_per_platform=max(0, int(max_identifiers_per_platform)),
                        existing_bitable_analysis=existing_bitable_analysis,
                        task_owner_employee_id=normalized_task_owner_employee_id,
                        creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                        force_refresh_creator_cache=bool(force_refresh_creator_cache),
                        vision_provider=str(vision_provider or "").strip().lower(),
                        skip_visual=bool(skip_visual),
                        skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
                    )
                    requested_identifiers = list(identifier_plan.get("requested_identifiers") or [])
                    visual_requested_identifiers = list(identifier_plan.get("visual_requested_identifiers") or [])
                    positioning_requested_identifiers = list(identifier_plan.get("positioning_requested_identifiers") or [])
                    mail_only_update_entries = [dict(item) for item in list(identifier_plan.get("mail_only_update_entries") or []) if isinstance(item, dict)]
                    partial_refresh_entries = [dict(item) for item in list(identifier_plan.get("partial_refresh_entries") or []) if isinstance(item, dict)]
                    has_stage_work = bool(
                        requested_identifiers
                        or visual_requested_identifiers
                        or positioning_requested_identifiers
                    )
                    platform_summary["staged_identifier_count"] = int(identifier_plan.get("staged_identifier_count") or 0)
                    platform_summary["incremental_prefilter"] = dict(identifier_plan.get("incremental_prefilter") or {})
                    platform_summary["requested_identifier_count"] = len(requested_identifiers)
                    platform_summary["requested_identifier_preview"] = requested_identifiers[:10]
                    platform_summary["mail_only_update_count"] = len(mail_only_update_entries)
                    platform_summary["mail_only_update_preview"] = [
                        str(item.get("creator_id") or "").strip()
                        for item in mail_only_update_entries[:10]
                        if str(item.get("creator_id") or "").strip()
                    ]
                    platform_summary["partial_refresh_count"] = len(partial_refresh_entries)
                    platform_summary["partial_refresh_preview"] = [
                        str(item.get("creator_id") or "").strip()
                        for item in partial_refresh_entries[:10]
                        if str(item.get("creator_id") or "").strip()
                    ]
                    platform_summary["partial_refresh_breakdown"] = dict(
                        (platform_summary.get("incremental_prefilter") or {}).get("partial_refresh_breakdown") or {}
                    )
                    if mail_only_update_entries:
                        mail_only_updates_by_platform[str(platform)] = mail_only_update_entries
                except Exception as exc:  # noqa: BLE001
                    _mark_platform_runtime_failure(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        exc=exc,
                        current_stage="platform_preparing_failed",
                        summary_writer=persist_summary,
                    )
                    continue

                if dry_run:
                    if int(platform_summary.get("mail_only_update_count") or 0) > 0 and not has_stage_work:
                        platform_summary["reason"] = "dry_run planned mail-only updates for already-screened creators"
                    elif bool((platform_summary.get("incremental_prefilter") or {}).get("all_existing")) and not has_stage_work:
                        platform_summary["reason"] = "all staged creators already exist in target bitable"
                    elif not has_stage_work:
                        platform_summary["reason"] = "no staged identifiers for platform"
                    else:
                        platform_summary["reason"] = "dry_run planned incremental execution only"
                    platform_summary["status"] = "dry_run_only"
                    platform_summary["scrape_job"] = {"status": "skipped", "reason": "dry_run flag set"}
                    platform_summary["visual_job"] = {"status": "skipped", "reason": "dry_run flag set"}
                    platform_summary["visual_gate"] = {
                        "executed": False,
                        "reason": "dry_run flag set",
                        "preflight_status": platform_summary["vision_preflight"]["status"],
                        "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                        "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                    }
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "dry_run flag set",
                    )
                    platform_summary["exports"] = {}
                    platform_summary["dry_run"] = {
                        "would_execute": bool(has_stage_work),
                        "reason": str(platform_summary.get("reason") or "").strip(),
                    }
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="incremental_filter_completed",
                        summary_writer=persist_summary,
                    )
                    continue

                if int(platform_summary.get("mail_only_update_count") or 0) > 0 and not has_stage_work:
                    platform_summary["status"] = "completed"
                    platform_summary["reason"] = "screened creators will be updated via mail-only export merge"
                    platform_summary["scrape_job"] = {"status": "skipped", "reason": platform_summary["reason"]}
                    platform_summary["visual_job"] = {"status": "skipped", "reason": platform_summary["reason"]}
                    platform_summary["visual_gate"] = {
                        "executed": False,
                        "reason": platform_summary["reason"],
                        "preflight_status": platform_summary["vision_preflight"]["status"],
                        "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                        "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                    }
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "mail-only updates reuse existing screening result",
                    )
                    platform_summary["exports"] = {}
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="incremental_filter_completed",
                        summary_writer=persist_summary,
                    )
                    continue

                if bool((platform_summary.get("incremental_prefilter") or {}).get("all_existing")) and not has_stage_work:
                    platform_summary["status"] = "completed"
                    platform_summary["reason"] = "all staged creators already exist in target bitable"
                    platform_summary["scrape_job"] = {"status": "skipped", "reason": platform_summary["reason"]}
                    platform_summary["visual_job"] = {"status": "skipped", "reason": platform_summary["reason"]}
                    platform_summary["visual_gate"] = {
                        "executed": False,
                        "reason": platform_summary["reason"],
                        "preflight_status": platform_summary["vision_preflight"]["status"],
                        "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                        "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                    }
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "incremental filter found no new creators",
                    )
                    platform_summary["exports"] = {}
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="incremental_filter_completed",
                        summary_writer=persist_summary,
                    )
                    continue

                if not has_stage_work:
                    platform_summary["status"] = "skipped"
                    platform_summary["reason"] = "no staged identifiers for platform"
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="platform_skipped",
                        summary_writer=persist_summary,
                    )
                    continue

                if skip_scrape:
                    platform_summary["status"] = "staged_only"
                    platform_summary["scrape_job"] = {"status": "skipped", "reason": "skip_scrape flag set"}
                    platform_summary["visual_gate"] = {
                        "executed": False,
                        "reason": "scrape skipped before visual review",
                        "preflight_status": platform_summary["vision_preflight"]["status"],
                        "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                        "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                    }
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "scrape skipped before positioning analysis",
                    )
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="scrape_skipped",
                        summary_writer=persist_summary,
                    )
                    continue

                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="scrape_starting",
                    summary_writer=persist_summary,
                )
                scrape_payload_body = build_scrape_payload(
                    platform,
                    requested_identifiers,
                    exclude_pinned_posts=not bool(include_pinned_posts),
                    creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                    force_refresh_creator_cache=bool(force_refresh_creator_cache),
                )
                try:
                    scrape_payload = require_success(
                        client.post("/api/jobs/scrape", json={"platform": platform, "payload": scrape_payload_body}),
                        f"{platform} scrape start",
                    )
                    platform_summary["scrape_job"] = dict(scrape_payload.get("job") or {})
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="scrape_running",
                        summary_writer=persist_summary,
                    )
                    scrape_job = poll_job(
                        client,
                        scrape_payload["job"]["id"],
                        f"{platform} scrape poll",
                        max(1.0, float(poll_interval)),
                    )
                except Exception as exc:  # noqa: BLE001
                    _mark_platform_runtime_failure(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        exc=exc,
                        current_stage="scrape_runtime_failed",
                        summary_writer=persist_summary,
                    )
                    continue
                platform_summary["scrape_job"] = scrape_job
                scrape_apify = _extract_scrape_apify_metadata(scrape_job)
                if scrape_apify:
                    platform_summary["scrape_job"]["failure_stage"] = _extract_scrape_failure_stage(scrape_job)
                    platform_summary["scrape_job"]["apify_run_id"] = scrape_apify.get("apify_run_id") or ""
                    platform_summary["scrape_job"]["apify_dataset_id"] = scrape_apify.get("apify_dataset_id") or ""
                    platform_summary["scrape_job"]["reused_guard"] = bool(scrape_apify.get("reused_guard"))
                    platform_summary["scrape_job"]["guard_key"] = scrape_apify.get("guard_key") or ""
                scrape_partial_result = _extract_scrape_partial_result(scrape_job)
                if scrape_partial_result:
                    platform_summary["scrape_job"]["partial_result"] = scrape_partial_result
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="scrape_completed" if scrape_job["status"] == "completed" else "scrape_poll_finished",
                    summary_writer=persist_summary,
                )
                scrape_was_salvaged = False
                if scrape_job["status"] != "completed":
                    if _scrape_has_partial_result(scrape_job):
                        scrape_was_salvaged = True
                        platform_summary["scrape_job"]["salvaged"] = True
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="scrape_partial_ready",
                            summary_writer=persist_summary,
                        )
                    else:
                        platform_summary["status"] = "scrape_failed"
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="scrape_failed",
                            summary_writer=persist_summary,
                        )
                        continue

                scrape_profile_reviews = _extract_scrape_profile_reviews(scrape_job)
                pass_count = _resolve_scrape_pass_count(scrape_job, count_passed_profiles)
                missing_reviews = _extract_missing_profile_reviews(scrape_job)
                missing_profiles = _build_missing_profile_summary(missing_reviews)
                fallback_reviews = _extract_fallback_profile_reviews(scrape_job)
                fallback_profiles = _build_fallback_profile_summary(fallback_reviews)
                available_identifiers = _extract_available_profile_identifiers(scrape_profile_reviews)
                visual_target_identifiers = list(visual_requested_identifiers or (available_identifiers or requested_identifiers))
                positioning_target_identifiers = list(
                    positioning_requested_identifiers or (available_identifiers or requested_identifiers)
                )
                platform_summary["profile_review_count"] = len(scrape_profile_reviews)
                platform_summary["prescreen_pass_count"] = pass_count
                platform_summary["missing_profile_count"] = len(missing_profiles)
                if missing_profiles:
                    platform_summary["missing_profiles"] = missing_profiles
                platform_summary["fallback_candidate_count"] = len(fallback_profiles)
                if fallback_profiles and len(fallback_profiles) != len(missing_profiles):
                    platform_summary["fallback_candidates_preview"] = fallback_profiles[:10]
                platform_summary["visual_gate"] = {
                    "executed": False,
                    "skip_visual_flag": bool(skip_visual),
                    "preflight_status": platform_summary["vision_preflight"]["status"],
                    "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                    "configured_provider_names": platform_summary["vision_preflight"]["configured_provider_names"],
                    "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                }
                if fallback_profiles:
                    current_lookup = dict(backend_app.load_upload_metadata(platform) or {})
                    next_platform = _resolve_next_fallback_platform(platform, execution_platforms)
                    current_has_fallback_contract = any(
                        (
                            str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("handle") or "").strip()
                            or str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("url") or "").strip()
                            or
                            str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("platform_attempt_order") or "").strip()
                            or str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("instagram_url") or "").strip()
                            or str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("youtube_url") or "").strip()
                        )
                        for item in fallback_profiles
                    )
                    fallback_supported = bool(
                        next_platform
                        and any(
                            str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("handle") or "").strip()
                            or
                            str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get("platform_attempt_order") or "").strip()
                            or str((current_lookup.get(str(item.get("identifier") or "").strip()) or {}).get(f"{next_platform}_url") or "").strip()
                            for item in fallback_profiles
                        )
                    )
                    if fallback_supported and next_platform:
                        try:
                            fallback_result = _stage_missing_profiles_for_fallback(
                                backend_app=backend_app,
                                current_platform=platform,
                                next_platform=next_platform,
                                missing_profiles=fallback_profiles,
                            )
                        except Exception as exc:  # noqa: BLE001
                            _mark_platform_runtime_failure(
                                summary=summary,
                                run_summary_path=run_summary_path,
                                backend_app=backend_app,
                                platform=platform,
                                platform_summary=platform_summary,
                                exc=exc,
                                current_stage="fallback_staging_failed",
                                summary_writer=persist_summary,
                            )
                            continue
                        platform_summary["fallback"] = {
                            "status": "staged" if int(fallback_result.get("staged_count") or 0) > 0 else "unavailable",
                            "next_platform": next_platform,
                            "staged_count": int(fallback_result.get("staged_count") or 0),
                            "staged_identifier_preview": list(fallback_result.get("staged_identifier_preview") or []),
                            "unresolved_missing_count": len(fallback_result.get("unresolved_missing") or []),
                        }
                        unresolved_missing = list(fallback_result.get("unresolved_missing") or [])
                    else:
                        if not current_has_fallback_contract:
                            unresolved_missing = [] if not missing_profiles else list(fallback_profiles)
                        else:
                            unresolved_missing = list(fallback_profiles)
                    if unresolved_missing:
                        manual_rows = []
                        for item in unresolved_missing:
                            identifier = str(item.get("identifier") or "").strip()
                            metadata = dict(current_lookup.get(identifier) or {})
                            handle = str(metadata.get("handle") or identifier or "").strip()
                            platform_attempt_order = str(metadata.get("platform_attempt_order") or "tiktok,instagram,youtube").strip()
                            profile_url = (
                                str(metadata.get("url") or "").strip()
                                or str(item.get("profile_url") or "").strip()
                            )
                            manual_rows.append(
                                {
                                    "identifier": handle,
                                    "platform": str(platform or "").strip().lower(),
                                    "profile_url": profile_url,
                                    "reason": _first_non_empty_text(
                                        item.get("reason"),
                                        f"{platform_attempt_order} 均未抓取到有效资料，需人工确认",
                                    ),
                                }
                            )
                        summary["manual_review_rows"].extend(manual_rows)
                        platform_summary.setdefault("fallback", {})
                        platform_summary["fallback"]["manual_review_count"] = len(manual_rows)
                        platform_summary["fallback"]["manual_review_identifier_preview"] = [
                            str(item.get("identifier") or "").strip()
                            for item in manual_rows[:10]
                        ]
                    if not available_identifiers:
                        platform_summary["visual_gate"]["blocked"] = True
                        platform_summary["visual_gate"]["reason"] = "no successful scrape rows remain after fallback staging"
                        platform_summary["visual_job"] = {
                            "status": "skipped",
                            "reason": "当前平台没有可继续进入视觉复核的抓取结果",
                        }
                        platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                            "skipped",
                            "no successful scrape rows remain after fallback staging",
                        )
                        platform_summary["fallback_only"] = bool(
                            int(((platform_summary.get("fallback") or {}).get("staged_count") or 0)) > 0
                        )
                if skip_visual:
                    platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
                elif pass_count <= 0:
                    platform_summary["visual_job"] = {"status": "skipped", "reason": "no Prescreen=Pass targets"}
                elif backend_app.get_available_vision_provider_names(vision_provider):
                    visual_payload_body = build_visual_payload(
                        platform,
                        visual_target_identifiers,
                        creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                        force_refresh_creator_cache=bool(force_refresh_creator_cache),
                    )
                    if vision_provider:
                        visual_payload_body["provider"] = str(vision_provider).strip().lower()
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="visual_starting",
                        summary_writer=persist_summary,
                    )
                    try:
                        visual_payload = require_success(
                            client.post("/api/jobs/visual-review", json={"platform": platform, "payload": visual_payload_body}),
                            f"{platform} visual start",
                        )
                        platform_summary["visual_job"] = dict(visual_payload.get("job") or {})
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="visual_running",
                            summary_writer=persist_summary,
                        )
                        platform_summary["visual_job"] = poll_job(
                            client,
                            visual_payload["job"]["id"],
                            f"{platform} visual poll",
                            max(1.0, float(poll_interval)),
                        )
                        platform_summary["visual_gate"]["executed"] = True
                    except Exception as exc:  # noqa: BLE001
                        _mark_platform_runtime_failure(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            exc=exc,
                            current_stage="visual_runtime_failed",
                            summary_writer=persist_summary,
                        )
                        continue
                else:
                    platform_summary["visual_job"] = {
                        "status": "skipped",
                        "reason": platform_summary["vision_preflight"]["message"],
                        "error_code": platform_summary["vision_preflight"]["error_code"],
                        "vision_preflight": platform_summary["vision_preflight"],
                    }

                if not skip_visual and pass_count > 0:
                    try:
                        platform_summary["visual_retry"] = _run_visual_postcheck_retries(
                            client=client,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            vision_provider=vision_provider,
                            creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                            force_refresh_creator_cache=bool(force_refresh_creator_cache),
                            poll_job=poll_job,
                            require_success=require_success,
                            poll_interval=poll_interval,
                            max_rounds=visual_postcheck_max_rounds,
                        )
                    except Exception as exc:  # noqa: BLE001
                        _mark_platform_runtime_failure(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            exc=exc,
                            current_stage="visual_retry_failed",
                            summary_writer=persist_summary,
                        )
                        continue
                else:
                    platform_summary["visual_retry"] = {
                        "enabled": True,
                        "max_rounds": max(0, int(visual_postcheck_max_rounds)),
                        "rounds": [],
                        "initial_error_count": 0,
                        "final_error_count": 0,
                        "status": "skipped",
                        "reason": "visual review was skipped or no prescreen-pass targets",
                    }

                if skip_positioning_card_analysis:
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "skip_positioning_card_analysis flag set",
                    )
                elif skip_visual:
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "visual review skipped",
                    )
                else:
                    resolve_targets = getattr(backend_app, "resolve_positioning_card_analysis_targets", None)
                    eligible_targets = []
                    if callable(resolve_targets):
                        eligible_targets = list(
                            resolve_targets(
                                platform,
                                build_visual_payload(
                                    platform,
                                    positioning_target_identifiers,
                                    creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                                    force_refresh_creator_cache=bool(force_refresh_creator_cache),
                                ),
                            )
                        )
                    if not eligible_targets:
                        platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                            "skipped",
                            "no Visual=Pass targets",
                        )
                    else:
                        positioning_payload_body = build_visual_payload(
                            platform,
                            positioning_target_identifiers,
                            creator_cache_db_path=str(creator_cache_db_path or "").strip(),
                            force_refresh_creator_cache=bool(force_refresh_creator_cache),
                        )
                        if vision_provider:
                            positioning_payload_body["provider"] = str(vision_provider).strip().lower()
                        try:
                            _persist_platform_summary(
                                summary=summary,
                                run_summary_path=run_summary_path,
                                backend_app=backend_app,
                                platform=platform,
                                platform_summary=platform_summary,
                                current_stage="positioning_card_analysis_starting",
                                summary_writer=persist_summary,
                            )
                            positioning_payload = require_success(
                                client.post(
                                    "/api/jobs/positioning-card-analysis",
                                    json={"platform": platform, "payload": positioning_payload_body},
                                ),
                                f"{platform} positioning card start",
                            )
                            platform_summary["positioning_card_analysis"] = dict(positioning_payload.get("job") or {})
                            _persist_platform_summary(
                                summary=summary,
                                run_summary_path=run_summary_path,
                                backend_app=backend_app,
                                platform=platform,
                                platform_summary=platform_summary,
                                current_stage="positioning_card_analysis_running",
                                summary_writer=persist_summary,
                            )
                            platform_summary["positioning_card_analysis"] = poll_job(
                                client,
                                positioning_payload["job"]["id"],
                                f"{platform} positioning card poll",
                                max(1.0, float(poll_interval)),
                            )
                        except Exception as exc:  # noqa: BLE001
                            platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                                "failed",
                                str(exc) or exc.__class__.__name__,
                                error_code="POSITIONING_CARD_ANALYSIS_FAILED",
                                non_blocking=True,
                            )

                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="exporting_artifacts",
                    summary_writer=persist_summary,
                )
                try:
                    platform_summary["artifact_status"] = require_success(
                        client.get(f"/api/artifacts/{platform}/status"),
                        f"{platform} artifact status",
                    )
                    fallback_stage_count = int(((platform_summary.get("fallback") or {}).get("staged_count") or 0))
                    if fallback_stage_count > 0:
                        platform_summary["exports"] = {}
                        platform_summary["final_review_export"] = {
                            "status": "deferred",
                            "reason": "missing profiles already staged to fallback platform; defer final review export until fallback platforms finish",
                            "fallback_staged_count": fallback_stage_count,
                        }
                    else:
                        platform_summary["exports"] = export_platform_artifacts(client, platform, exports_dir / platform)
                    if bool(platform_summary.get("fallback_only")):
                        platform_summary["status"] = "fallback_staged"
                    else:
                        platform_summary["status"] = "completed_with_partial_scrape" if scrape_was_salvaged else "completed"
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="completed",
                        summary_writer=persist_summary,
                    )
                except Exception as exc:  # noqa: BLE001
                    _mark_platform_runtime_failure(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        exc=exc,
                        current_stage="artifact_export_failed",
                        summary_writer=persist_summary,
                    )
                    continue
        except Exception as exc:  # noqa: BLE001
            failure = _build_failure_payload(
                stage="downstream",
                error_code="KEEP_LIST_SCREENING_RUNTIME_FAILED",
                message=str(exc) or exc.__class__.__name__,
                remediation="检查 staging 产物、vision preflight、backend runtime 和对应平台 job 日志后重试。",
                details={"exception_type": exc.__class__.__name__},
            )
            return _finalize_failure(
                failure=failure,
                finished_at=backend_app.iso_now(),
            )

        if dry_run:
            summary["dry_run_report"] = _build_dry_run_report(summary)
            summary["quality_report"] = {
                "status": "ok",
                "warning_count": 0,
                "warnings": [],
                "platforms": {},
            }
            summary["status"] = summarize_platform_statuses(summary["platforms"])
            summary["finished_at"] = backend_app.iso_now()
            summary["current_stage"] = "completed"
            attach_run_contract(summary)
            persist_summary(summary)
            return summary

        combined_exports = collect_final_exports(summary.get("platforms"))
        summary["current_stage"] = "export_merge"
        combined_artifacts = build_all_platforms_final_review_artifacts(
            output_path=exports_dir / "all_platforms_final_review.xlsx",
            payload_json_path=exports_dir / "all_platforms_final_review_payload.json",
            final_exports=combined_exports,
            keep_workbook=resolved_keep_workbook,
            manual_review_rows=summary.get("manual_review_rows") or [],
            mail_only_updates=mail_only_updates_by_platform,
            task_owner={
                "responsible_name": normalized_task_owner_name,
                "employee_name": normalized_task_owner_name,
                "employee_id": normalized_task_owner_employee_id,
                "employee_record_id": normalized_task_owner_employee_record_id,
                "employee_email": normalized_task_owner_employee_email,
                "owner_name": normalized_task_owner_owner_name,
                "linked_bitable_url": normalized_linked_bitable_url,
                "task_name": normalized_task_name,
            },
        )
        summary["artifacts"]["final_exports"] = combined_exports
        summary["artifacts"]["all_platforms_final_review"] = combined_artifacts["all_platforms_final_review"]
        summary["artifacts"]["all_platforms_upload_payload_json"] = combined_artifacts["all_platforms_upload_payload_json"]
        summary["artifacts"]["all_platforms_upload_local_archive_dir"] = combined_artifacts["all_platforms_upload_local_archive_dir"]
        summary["artifacts"]["all_platforms_upload_skipped_archive_json"] = combined_artifacts["all_platforms_upload_skipped_archive_json"]
        summary["artifacts"]["all_platforms_upload_skipped_archive_xlsx"] = combined_artifacts["all_platforms_upload_skipped_archive_xlsx"]
        summary["artifacts"]["all_platforms_upload_row_count"] = combined_artifacts["row_count"]
        summary["artifacts"]["all_platforms_upload_source_row_count"] = combined_artifacts["source_row_count"]
        summary["artifacts"]["all_platforms_upload_skipped_row_count"] = combined_artifacts["skipped_row_count"]
        payload_json_path = Path(str(combined_artifacts["all_platforms_upload_payload_json"])).expanduser()
        summary["artifacts"]["all_platforms_upload_explicit_update_mode_count"] = _apply_explicit_feishu_update_mode(
            payload_json_path,
            update_mode="create_or_update",
        )
        missing_profile_rows = _collect_missing_profile_rows(summary)
        summary["artifacts"]["missing_profiles_xlsx"] = _write_report_xlsx_best_effort(
            exports_dir / "missing_profiles.xlsx",
            missing_profile_rows,
            columns=("platform", "identifier", "profile_url", "reason"),
            warnings_bucket=summary.setdefault("warnings", {}),
            warning_key="missing_profiles_report_write_failed",
            artifact_label="missing_profiles.xlsx",
        )
        summary["artifacts"]["feishu_upload_result_json"] = ""
        summary["artifacts"]["feishu_upload_result_xlsx"] = ""
        summary["artifacts"]["feishu_upload_target_url"] = ""
        summary["artifacts"]["feishu_upload_target_table_id"] = ""
        summary["artifacts"]["feishu_upload_target_table_name"] = ""
        summary["artifacts"]["feishu_upload_created_count"] = 0
        summary["artifacts"]["feishu_upload_updated_count"] = 0
        summary["artifacts"]["feishu_upload_failed_count"] = 0
        summary["artifacts"]["feishu_upload_skipped_existing_count"] = 0
        if combined_artifacts["source_row_count"] > 0 and combined_artifacts["row_count"] <= 0:
            skip_archive = summary["artifacts"]["all_platforms_upload_skipped_archive_json"]
            summary["artifacts"]["error_report_xlsx"] = _write_report_xlsx_best_effort(
                exports_dir / "error_report.xlsx",
                _build_error_report_rows(summary, summary.get("upload_summary"), occurred_at=backend_app.iso_now()),
                columns=("严重级别", "异常类别", "达人ID", "平台", "原因详情", "飞书记录ID", "时间"),
                warnings_bucket=summary.setdefault("warnings", {}),
                warning_key="feishu_error_report_write_failed",
                artifact_label="error_report.xlsx",
            )
            failure = _build_failure_payload(
                stage="feishu_upload",
                error_code="FEISHU_UPLOAD_PAYLOAD_EMPTY",
                message="导出已生成，但所有行都在上传前校验阶段被本地归档，未产生可上传 payload。",
                remediation="先检查 skipped_from_feishu_upload.json 里的缺字段原因，修正后重新生成并上传。",
                details={
                    "source_row_count": int(combined_artifacts["source_row_count"]),
                    "skipped_row_count": int(combined_artifacts["skipped_row_count"]),
                    "skipped_archive_json": str(skip_archive),
                },
            )
            return _finalize_failure(
                failure=failure,
                finished_at=backend_app.iso_now(),
            )
        if combined_artifacts["row_count"] > 0:
            summary["current_stage"] = "upload"
            try:
                feishu_client = _build_feishu_open_client(
                    runtime=runtime,
                    env_file=env_file,
                )
                upload_summary = upload_final_review_payload_to_bitable(
                    feishu_client,
                    payload_json_path=combined_artifacts["all_platforms_upload_payload_json"],
                    linked_bitable_url=normalized_linked_bitable_url,
                    task_name=normalized_task_name,
                    task_upload_url=str(task_upload_url or "").strip(),
                )
            except Exception as exc:  # noqa: BLE001
                failure = _build_failure_payload(
                    stage="feishu_upload",
                    error_code="FEISHU_UPLOAD_RUNTIME_FAILED",
                    message=str(exc) or exc.__class__.__name__,
                    remediation="检查飞书 app 配置、目标表链接以及 payload 内容后重试。",
                    details={"exception_type": exc.__class__.__name__},
                )
                return _finalize_failure(
                    failure=failure,
                    finished_at=backend_app.iso_now(),
                )
            full_upload_summary = dict(upload_summary)
            summary["artifacts"]["feishu_upload_result_json"] = str(upload_summary.get("result_json_path") or "").strip()
            summary["artifacts"]["feishu_upload_result_xlsx"] = str(upload_summary.get("result_xlsx_path") or "").strip()
            summary["artifacts"]["feishu_upload_target_url"] = str(upload_summary.get("target_url") or "").strip()
            summary["artifacts"]["feishu_upload_target_table_id"] = str(upload_summary.get("target_table_id") or "").strip()
            summary["artifacts"]["feishu_upload_target_table_name"] = str(upload_summary.get("target_table_name") or "").strip()
            summary["artifacts"]["feishu_upload_created_count"] = int(upload_summary.get("created_count") or 0)
            summary["artifacts"]["feishu_upload_updated_count"] = int(upload_summary.get("updated_count") or 0)
            summary["artifacts"]["feishu_upload_failed_count"] = int(upload_summary.get("failed_count") or 0)
            summary["artifacts"]["feishu_upload_skipped_existing_count"] = int(upload_summary.get("skipped_existing_count") or 0)
            summary["artifacts"]["success_report_xlsx"] = _write_report_xlsx_best_effort(
                exports_dir / "success_report.xlsx",
                _build_success_report_rows(full_upload_summary),
                columns=("达人ID", "平台", "主页链接", "操作", "飞书记录ID"),
                warnings_bucket=summary.setdefault("warnings", {}),
                warning_key="feishu_success_report_write_failed",
                artifact_label="success_report.xlsx",
            )
            summary["artifacts"]["error_report_xlsx"] = _write_report_xlsx_best_effort(
                exports_dir / "error_report.xlsx",
                _build_error_report_rows(summary, full_upload_summary, occurred_at=backend_app.iso_now()),
                columns=("严重级别", "异常类别", "达人ID", "平台", "原因详情", "飞书记录ID", "时间"),
                warnings_bucket=summary.setdefault("warnings", {}),
                warning_key="feishu_error_report_write_failed",
                artifact_label="error_report.xlsx",
            )
            created_count = int(full_upload_summary.get("created_count") or 0)
            updated_count = int(full_upload_summary.get("updated_count") or 0)
            failed_count = int(full_upload_summary.get("failed_count") or 0)
            if list(full_upload_summary.get("report_write_warnings") or []):
                summary.setdefault("warnings", {})["feishu_upload_result_persistence"] = {
                    "warning_count": len(list(full_upload_summary.get("report_write_warnings") or [])),
                    "warnings": list(full_upload_summary.get("report_write_warnings") or []),
                }
            if created_count + updated_count == 0 and (
                not bool(full_upload_summary.get("ok", True)) or failed_count > 0
            ):
                failure = _build_failure_payload(
                    stage="feishu_upload",
                    error_code="FEISHU_UPLOAD_FAILED",
                    message=_first_non_empty_text(
                        full_upload_summary.get("error"),
                        full_upload_summary.get("message"),
                        "飞书上传未完整成功。",
                    ),
                    remediation="检查飞书返回的 failed_rows、目标表去重状态和负责人字段后重试。",
                    details={
                        "result_json_path": str(full_upload_summary.get("result_json_path") or "").strip(),
                        "created_count": created_count,
                        "updated_count": updated_count,
                        "failed_count": failed_count,
                        "skipped_existing_count": int(full_upload_summary.get("skipped_existing_count") or 0),
                    },
                )
                return _finalize_failure(
                    failure=failure,
                    finished_at=backend_app.iso_now(),
                )
            if failed_count > 0:
                summary.setdefault("warnings", {})["feishu_upload_partial_failure"] = {
                    "failed_count": failed_count,
                    "result_json_path": str(full_upload_summary.get("result_json_path") or "").strip(),
                }
            summary["upload_summary"] = _compact_upload_summary(full_upload_summary)
        if not summary["artifacts"].get("error_report_xlsx"):
            summary["artifacts"]["error_report_xlsx"] = _write_report_xlsx_best_effort(
                exports_dir / "error_report.xlsx",
                _build_error_report_rows(summary, summary.get("upload_summary"), occurred_at=backend_app.iso_now()),
                columns=("严重级别", "异常类别", "达人ID", "平台", "原因详情", "飞书记录ID", "时间"),
                warnings_bucket=summary.setdefault("warnings", {}),
                warning_key="feishu_error_report_write_failed",
                artifact_label="error_report.xlsx",
            )
        summary["quality_report"] = build_quality_report(summary["platforms"])
        summary["status"] = summarize_platform_statuses(summary["platforms"])
        if summary["status"] == "completed" and str((summary.get("quality_report") or {}).get("status") or "") == "warning":
            summary["status"] = "completed_with_quality_warnings"
        summary["finished_at"] = backend_app.iso_now()
        summary["current_stage"] = "completed"
        if summary["status"] == "scrape_failed":
            attach_failure_to_summary(
                summary,
                _build_failure_payload(
                    stage="platform_scrape",
                    error_code="SCRAPE_FAILED",
                    message="至少一个平台 scrape 未完成。",
                    remediation="检查对应平台的 scrape job 状态；若确认为临时失败，可直接重试当前 run。",
                ),
                expose_top_level=False,
            )
        elif summary["status"] == "failed":
            failed_platforms: dict[str, dict[str, Any]] = {}
            skipped_platforms: list[str] = []
            for platform_name, payload in (summary.get("platforms") or {}).items():
                normalized_payload = dict(payload or {})
                platform_status = str(normalized_payload.get("status") or "").strip()
                if platform_status == "failed":
                    failed_platforms[str(platform_name)] = {
                        "status": platform_status,
                        "error_code": str(normalized_payload.get("error_code") or "").strip(),
                        "error": str(normalized_payload.get("error") or "").strip(),
                        "current_stage": str(normalized_payload.get("current_stage") or "").strip(),
                    }
                elif platform_status == "skipped":
                    skipped_platforms.append(str(platform_name))
            attach_failure_to_summary(
                summary,
                _build_failure_payload(
                    stage="platform_runtime",
                    error_code="NO_SUCCESSFUL_PLATFORMS",
                    message="本次 run 没有任何平台成功完成，未产生可消费产物。",
                    remediation="检查各平台 summary 里的 error_code/error 和 backend/runtime 日志后重试。",
                    details={
                        "failed_platform_count": len(failed_platforms),
                        "failed_platforms": failed_platforms,
                        "skipped_platform_count": len(skipped_platforms),
                        "skipped_platforms": skipped_platforms,
                        "all_platforms_upload_source_row_count": int(
                            (summary.get("artifacts") or {}).get("all_platforms_upload_source_row_count") or 0
                        ),
                        "all_platforms_upload_row_count": int(
                            (summary.get("artifacts") or {}).get("all_platforms_upload_row_count") or 0
                        ),
                    },
                ),
                expose_top_level=True,
            )
        elif summary["status"] == "missing_profiles_blocked":
            attach_failure_to_summary(
                summary,
                _build_failure_payload(
                    stage="platform_gate",
                    error_code="MISSING_PROFILES_BLOCKED",
                    message="名单账号未在抓取结果中返回，当前 run 已被阻断。",
                    remediation="检查 keep workbook、上传名单和抓取结果的一致性后重试。",
                ),
                expose_top_level=False,
            )
        attach_run_contract(summary)
        persist_summary(summary)
        return summary
    finally:
        restore_backend_runtime_state(runtime_snapshot)


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        normalized = _clean_text(value)
        if normalized:
            return normalized
    return ""


def _extract_task_owner_context_from_payload(
    payload: dict[str, Any],
    *,
    candidate: Path,
) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}

    intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
    task_owner = payload.get("task_owner") if isinstance(payload.get("task_owner"), dict) else {}
    resolved_task_owner = (
        payload.get("resolved_task_owner") if isinstance(payload.get("resolved_task_owner"), dict) else {}
    )
    downstream_handoff = payload.get("downstream_handoff") if isinstance(payload.get("downstream_handoff"), dict) else {}
    handoff_owner = downstream_handoff.get("task_owner") if isinstance(downstream_handoff.get("task_owner"), dict) else {}
    resolved_inputs = payload.get("resolved_inputs") if isinstance(payload.get("resolved_inputs"), dict) else {}
    resolved_feishu = resolved_inputs.get("feishu") if isinstance(resolved_inputs.get("feishu"), dict) else {}
    steps = payload.get("steps") if isinstance(payload.get("steps"), dict) else {}
    task_assets_step = steps.get("task_assets") if isinstance(steps.get("task_assets"), dict) else {}
    task_assets_raw = task_assets_step.get("raw") if isinstance(task_assets_step.get("raw"), dict) else {}

    employee_id = _first_non_empty(
        task_owner.get("task_owner_employee_id"),
        task_owner.get("employee_id"),
        resolved_task_owner.get("task_owner_employee_id"),
        resolved_task_owner.get("employee_id"),
        handoff_owner.get("task_owner_employee_id"),
        handoff_owner.get("employee_id"),
    )
    linked_url = _first_non_empty(
        task_owner.get("linked_bitable_url"),
        resolved_task_owner.get("linked_bitable_url"),
        handoff_owner.get("linked_bitable_url"),
        downstream_handoff.get("linked_bitable_url"),
        task_assets_step.get("linked_bitable_url"),
        task_assets_step.get("linkedBitableUrl"),
        task_assets_raw.get("linked_bitable_url"),
        task_assets_raw.get("linkedBitableUrl"),
    )
    owner_name = _first_non_empty(
        task_owner.get("task_owner_name"),
        task_owner.get("responsible_name"),
        resolved_task_owner.get("task_owner_name"),
        resolved_task_owner.get("responsible_name"),
        handoff_owner.get("task_owner_name"),
        handoff_owner.get("responsible_name"),
        handoff_owner.get("employee_name"),
    )
    record_id = _first_non_empty(
        task_owner.get("task_owner_employee_record_id"),
        task_owner.get("employee_record_id"),
        resolved_task_owner.get("task_owner_employee_record_id"),
        resolved_task_owner.get("employee_record_id"),
        handoff_owner.get("task_owner_employee_record_id"),
        handoff_owner.get("employee_record_id"),
    )
    employee_email = _first_non_empty(
        task_owner.get("task_owner_employee_email"),
        task_owner.get("employee_email"),
        resolved_task_owner.get("task_owner_employee_email"),
        resolved_task_owner.get("employee_email"),
        handoff_owner.get("task_owner_employee_email"),
        handoff_owner.get("employee_email"),
    )
    owner_login = _first_non_empty(
        task_owner.get("task_owner_owner_name"),
        task_owner.get("owner_name"),
        resolved_task_owner.get("task_owner_owner_name"),
        resolved_task_owner.get("owner_name"),
        handoff_owner.get("task_owner_owner_name"),
        handoff_owner.get("owner_name"),
    )
    task_name = _first_non_empty(
        task_owner.get("task_name"),
        resolved_task_owner.get("task_name"),
        handoff_owner.get("task_name"),
        intent.get("task_name"),
    )
    task_upload_url = _first_non_empty(
        task_owner.get("task_upload_url"),
        handoff_owner.get("task_upload_url"),
        intent.get("task_upload_url"),
        resolved_feishu.get("task_upload_url"),
    )
    if not any([employee_id, linked_url, owner_name, task_name, task_upload_url]):
        return {}

    source_path = str(candidate.resolve())
    return {
        "task_owner_name": owner_name,
        "task_owner_employee_id": employee_id,
        "task_owner_employee_record_id": record_id,
        "task_owner_employee_email": employee_email,
        "task_owner_owner_name": owner_login,
        "linked_bitable_url": linked_url,
        "task_name": task_name,
        "task_upload_url": task_upload_url,
        "task_spec_path": source_path,
    }


def _infer_task_owner_from_adjacent_task_spec(
    *,
    keep_workbook: Path,
) -> dict[str, str]:
    candidate_specs: list[Path] = []
    for ancestor in [keep_workbook.parent, *keep_workbook.parents]:
        candidate_specs.append(ancestor / "downstream" / "task_spec.json")
        candidate_specs.append(ancestor / "task_spec.json")
        candidate_specs.append(ancestor / "downstream" / "summary.json")
        candidate_specs.append(ancestor / "summary.json")

    seen: set[Path] = set()
    merged_context: dict[str, str] = {}
    merged_fields = (
        "task_owner_name",
        "task_owner_employee_id",
        "task_owner_employee_record_id",
        "task_owner_employee_email",
        "task_owner_owner_name",
        "linked_bitable_url",
        "task_name",
        "task_upload_url",
    )
    for candidate in candidate_specs:
        resolved_candidate = candidate.resolve(strict=False)
        if resolved_candidate in seen:
            continue
        seen.add(resolved_candidate)
        payload = _load_json_if_exists(candidate)
        candidate_context = _extract_task_owner_context_from_payload(payload, candidate=candidate)
        if not candidate_context:
            continue
        if not merged_context.get("task_spec_path"):
            merged_context["task_spec_path"] = candidate_context["task_spec_path"]
        for field in merged_fields:
            if not merged_context.get(field) and candidate_context.get(field):
                merged_context[field] = candidate_context[field]
    if any(merged_context.get(field) for field in merged_fields):
        return merged_context
    return {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage and optionally run the screening pipeline from a reviewed keep-list workbook."
    )
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
    parser.add_argument("--keep-workbook", default=str(DEFAULT_KEEP_WORKBOOK), help="`*_llm_reviewed_keep.xlsx` 路径。")
    parser.add_argument("--template-workbook", default=str(DEFAULT_TEMPLATE_WORKBOOK), help="需求模板 xlsx。")
    parser.add_argument("--task-name", default="", help="任务名；如需直接复用任务上传模板解析链可传。")
    parser.add_argument("--task-upload-url", default="", help="飞书任务上传 wiki/base 链接。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/keep_list_screening_<timestamp>。")
    parser.add_argument("--summary-json", default="", help="最终 run summary.json 输出路径。")
    parser.add_argument("--platform", action="append", help="只跑指定平台，可重复传入：tiktok / instagram / youtube。")
    parser.add_argument("--vision-provider", default="", help="指定视觉 provider，例如 openai / reelx。")
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--dry-run", action="store_true", help="只做增量达人与平台执行预估，不真正触发 scrape / visual / export / upload。")
    parser.add_argument("--probe-vision-provider-only", action="store_true", help="只做视觉 provider live probe，不继续 scrape/visual/export。")
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="staging-only / local observation run；不触发 scrape/visual/export，且跳过 vision probe。",
    )
    parser.add_argument("--skip-visual", action="store_true", help="跑 scrape 和导出，但跳过视觉复核。")
    parser.add_argument("--include-pinned-posts", action="store_true", help="TikTok scrape 时保留置顶内容；默认去掉置顶。")
    parser.add_argument("--creator-cache-db-path", default="", help="Creator DB SQLite 路径；默认使用仓库内共享缓存库。")
    parser.add_argument("--force-refresh-creator-cache", action="store_true", help="忽略 Creator DB 历史结果，强制重新抓取和视觉审核。")
    parser.add_argument("--visual-postcheck-max-rounds", type=int, default=3, help="视觉完成后自动补跑失败账号的最大轮数；默认 3。")
    parser.add_argument("--skip-positioning-card-analysis", action="store_true", help="跳过 visual-pass 后的定位卡分析。")
    parser.add_argument("--task-owner-name", default="", help="任务负责人展示名，用于总表 `达人对接人`。")
    parser.add_argument("--task-owner-employee-id", default="", help="任务负责人飞书 employeeId，用于总表 payload。")
    parser.add_argument("--task-owner-employee-record-id", default="", help="任务负责人员工表 record_id。")
    parser.add_argument("--task-owner-employee-email", default="", help="任务负责人邮箱。")
    parser.add_argument("--task-owner-owner-name", default="", help="任务上传 ownerName 原始值。")
    parser.add_argument("--linked-bitable-url", default="", help="任务关联达人管理表链接。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_keep_list_screening_pipeline(
        keep_workbook=Path(args.keep_workbook),
        template_workbook=Path(args.template_workbook) if args.template_workbook else None,
        task_name=args.task_name or "",
        task_upload_url=args.task_upload_url or "",
        env_file=args.env_file,
        output_root=Path(args.output_root) if args.output_root else None,
        summary_json=Path(args.summary_json) if args.summary_json else None,
        platform_filters=args.platform,
        vision_provider=args.vision_provider or "",
        max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
        poll_interval=max(1.0, float(args.poll_interval)),
        dry_run=bool(args.dry_run),
        probe_vision_provider_only=bool(args.probe_vision_provider_only),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
        include_pinned_posts=bool(args.include_pinned_posts),
        creator_cache_db_path=args.creator_cache_db_path or "",
        force_refresh_creator_cache=bool(args.force_refresh_creator_cache),
        visual_postcheck_max_rounds=max(0, int(args.visual_postcheck_max_rounds)),
        skip_positioning_card_analysis=bool(args.skip_positioning_card_analysis),
        task_owner_name=args.task_owner_name or "",
        task_owner_employee_id=args.task_owner_employee_id or "",
        task_owner_employee_record_id=args.task_owner_employee_record_id or "",
        task_owner_employee_email=args.task_owner_employee_email or "",
        task_owner_owner_name=args.task_owner_owner_name or "",
        linked_bitable_url=args.linked_bitable_url or "",
    )
    print(json.dumps(_build_cli_output_summary(summary), ensure_ascii=False, indent=2))
    return 0 if summary.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
