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
    from feishu_screening_bridge.bitable_upload import upload_final_review_payload_to_bitable
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
    successful_statuses = {"completed", "completed_with_partial_scrape", "fallback_staged", "staged_only"}
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
    if statuses and all(status in {"staged_only", "skipped"} for status in statuses):
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
            "profile_review_count": int(payload.get("profile_review_count") or 0),
            "prescreen_pass_count": int(payload.get("prescreen_pass_count") or 0),
            "missing_profile_count": int(payload.get("missing_profile_count") or 0),
        }
    return {
        "status": str(summary.get("status") or "").strip(),
        "verdict": dict(summary.get("verdict") or {}),
        "run_root": str(summary.get("run_root") or "").strip(),
        "summary_json": str(summary.get("summary_json") or "").strip(),
        "warnings": dict(summary.get("warnings") or {}),
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
    if current_stage is not None:
        platform_summary["current_stage"] = current_stage
    if status is not None:
        platform_summary["status"] = status
    platform_summary["last_updated_at"] = backend_app.iso_now()
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
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
        visual_postcheck_max_rounds=visual_postcheck_max_rounds,
        probe_vision_provider_only=probe_vision_provider_only,
    )

    summary: dict[str, Any] = {
        "started_at": iso_now(),
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
        "staging": {},
        "platforms": {},
        "manual_review_rows": [],
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
    }
    attach_run_contract(summary)
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
        if skip_scrape and not probe_vision_provider_only:
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
            if (not skip_scrape and not skip_visual) or probe_vision_provider_only:
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
            if probe_vision_provider_only:
                summary["status"] = "vision_probe_only"
                summary["finished_at"] = backend_app.iso_now()
                attach_run_contract(summary)
                persist_summary(summary)
                return summary

            for platform in execution_platforms:
                platform_summary: dict[str, Any] = {
                    "staged_identifier_count": len(backend_app.load_upload_metadata(platform)),
                    "requested_identifier_count": 0,
                    "requested_identifier_preview": [],
                    "requested_vision_provider": str(vision_provider or "").strip().lower(),
                    "vision_preflight": backend_app.build_vision_preflight(vision_provider),
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
                    requested_identifiers = select_platform_identifiers(platform, max(0, int(max_identifiers_per_platform)))
                    platform_summary["requested_identifier_count"] = len(requested_identifiers)
                    platform_summary["requested_identifier_preview"] = requested_identifiers[:10]
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

                if not requested_identifiers:
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
                            if not missing_profiles:
                                unresolved_missing = []
                            elif available_identifiers:
                                unresolved_missing = list(fallback_profiles)
                            else:
                                platform_summary["visual_gate"]["blocked"] = True
                                platform_summary["visual_gate"]["reason"] = "prescreen contains Missing targets"
                                platform_summary["visual_job"] = {
                                    "status": "skipped",
                                    "reason": "名单账号未在本次抓取结果中返回，已阻断视觉复核和最终导出",
                                }
                                platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                                    "skipped",
                                    "missing profiles blocked downstream stages",
                                )
                                try:
                                    platform_summary["artifact_status"] = require_success(
                                        client.get(f"/api/artifacts/{platform}/status"),
                                        f"{platform} artifact status",
                                    )
                                except Exception as exc:  # noqa: BLE001
                                    _mark_platform_runtime_failure(
                                        summary=summary,
                                        run_summary_path=run_summary_path,
                                        backend_app=backend_app,
                                        platform=platform,
                                        platform_summary=platform_summary,
                                        exc=exc,
                                        current_stage="artifact_status_failed",
                                        summary_writer=persist_summary,
                                    )
                                    continue
                                platform_summary["exports"] = {}
                                platform_summary["status"] = "missing_profiles_blocked"
                                _persist_platform_summary(
                                    summary=summary,
                                    run_summary_path=run_summary_path,
                                    backend_app=backend_app,
                                    platform=platform,
                                    platform_summary=platform_summary,
                                    current_stage="missing_profiles_blocked",
                                    summary_writer=persist_summary,
                                )
                                continue
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
                        try:
                            platform_summary["artifact_status"] = require_success(
                                client.get(f"/api/artifacts/{platform}/status"),
                                f"{platform} artifact status",
                            )
                        except Exception as exc:  # noqa: BLE001
                            _mark_platform_runtime_failure(
                                summary=summary,
                                run_summary_path=run_summary_path,
                                backend_app=backend_app,
                                platform=platform,
                                platform_summary=platform_summary,
                                exc=exc,
                                current_stage="artifact_status_failed",
                                summary_writer=persist_summary,
                            )
                            continue
                        platform_summary["exports"] = {}
                        platform_summary["status"] = "fallback_staged"
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="fallback_staged",
                            summary_writer=persist_summary,
                        )
                        continue
                if skip_visual:
                    platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
                elif pass_count <= 0:
                    platform_summary["visual_job"] = {"status": "skipped", "reason": "no Prescreen=Pass targets"}
                elif backend_app.get_available_vision_provider_names(vision_provider):
                    visual_payload_body = build_visual_payload(
                        platform,
                        available_identifiers or requested_identifiers,
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
                                    available_identifiers or requested_identifiers,
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
                            available_identifiers or requested_identifiers,
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
                    final_review_export_blocked = bool((platform_summary.get("artifact_status") or {}).get("final_review_export_blocked"))
                    if final_review_export_blocked and fallback_stage_count > 0:
                        platform_summary["exports"] = {}
                        platform_summary["final_review_export"] = {
                            "status": "deferred",
                            "reason": "missing profiles already staged to fallback platform; defer final review export until fallback platforms finish",
                            "fallback_staged_count": fallback_stage_count,
                        }
                    else:
                        platform_summary["exports"] = export_platform_artifacts(client, platform, exports_dir / platform)
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

        combined_exports = collect_final_exports(summary.get("platforms"))
        combined_artifacts = build_all_platforms_final_review_artifacts(
            output_path=exports_dir / "all_platforms_final_review.xlsx",
            payload_json_path=exports_dir / "all_platforms_final_review_payload.json",
            final_exports=combined_exports,
            keep_workbook=resolved_keep_workbook,
            manual_review_rows=summary.get("manual_review_rows") or [],
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
