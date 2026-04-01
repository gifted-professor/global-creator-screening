from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
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


SUCCESSFUL_DOWNSTREAM_STATUSES = {"completed", "completed_with_partial_scrape"}
MAIL_ONLY_UPDATE_MODE = "mail_only_update"
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


def _extract_ai_status(fields: dict[str, Any]) -> str:
    return _flatten_field_value(_get_field_value(fields, "ai是否通过", "ai 是否通过"))


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
) -> tuple[dict[str, Any], dict[str, Any]]:
    creator_id = _extract_creator_id(keep_row) or _flatten_field_value(_get_field_value(existing_fields, "达人ID"))
    platform = _extract_platform(keep_row) or _flatten_field_value(_get_field_value(existing_fields, "平台"))
    profile_url = _clean_text(keep_row.get("URL")) or _flatten_field_value(_get_field_value(existing_fields, "主页链接"))
    quote_text = _build_quote_text(keep_row) or _flatten_field_value(_get_field_value(existing_fields, "当前网红报价"))
    last_mail_time = _format_date(keep_row.get("brand_message_sent_at")) or _flatten_field_value(
        _get_field_value(existing_fields, "达人最后一次回复邮件时间")
    )
    last_mail_content = _clean_text(keep_row.get("brand_message_snippet")) or _flatten_field_value(
        _get_field_value(existing_fields, "达人回复的最后一封邮件内容")
    )
    owner_display_name = (
        _clean_text(owner_context.get("responsible_name"))
        or _clean_text(owner_context.get("employee_name"))
        or _flatten_field_value(_get_field_value(existing_fields, "达人对接人"))
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
        "达人回复的最后一封邮件内容": last_mail_content,
        "达人对接人": owner_display_name,
        "ai是否通过": _extract_ai_status(existing_fields),
        "ai筛号反馈理由": _flatten_field_value(_get_field_value(existing_fields, "ai筛号反馈理由")),
        "标签(ai)": _flatten_field_value(_get_field_value(existing_fields, "标签(ai)", "标签（ai）")),
        "ai评价": _flatten_field_value(_get_field_value(existing_fields, "ai评价", "ai 评价")),
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
            "__feishu_update_mode": MAIL_ONLY_UPDATE_MODE,
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


def _write_combined_workbook(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=FINAL_UPLOAD_COLUMNS)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="总表")


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


def _build_owner_search_text(
    keep_row: dict[str, Any],
    *,
    shared_mail_db_path: Path,
    shared_mail_raw_dir: Path | None,
    shared_mail_data_dir: Path | None,
    keep_workbook: Path,
) -> str:
    attachment_paths = _build_mail_attachment_paths(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    excerpt = _read_mail_text_excerpt(attachment_paths)
    parts = [
        _clean_text(keep_row.get("brand_message_subject")),
        _clean_text(keep_row.get("brand_message_snippet")),
        _clean_text(keep_row.get("last_mail_snippet")),
        _clean_text(keep_row.get("matched_email")),
        _clean_text(keep_row.get("brand_message_folder")),
        excerpt,
    ]
    return "\n".join(part for part in parts if part)


def _match_owner_aliases(search_text: str, candidate: dict[str, Any]) -> list[str]:
    matched_aliases: list[str] = []
    for alias in _build_owner_candidate_aliases(candidate):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", re.IGNORECASE)
        if pattern.search(search_text):
            matched_aliases.append(alias)
    return matched_aliases


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
    search_text = _build_owner_search_text(
        keep_row,
        shared_mail_db_path=shared_mail_db_path,
        shared_mail_raw_dir=shared_mail_raw_dir,
        shared_mail_data_dir=shared_mail_data_dir,
        keep_workbook=keep_workbook,
    )
    matches: list[tuple[dict[str, Any], list[str]]] = []
    for candidate in owner_candidates:
        aliases = _match_owner_aliases(search_text, candidate)
        if aliases:
            matches.append((candidate, aliases))
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
            updated[_KEEP_OWNER_EMPLOYEE_ID_FIELD] = _clean_text(row_owner_context.get("employee_id")).split(",")[0].strip()
            updated[_KEEP_OWNER_EMPLOYEE_RECORD_ID_FIELD] = _clean_text(row_owner_context.get("employee_record_id"))
            updated[_KEEP_OWNER_EMPLOYEE_EMAIL_FIELD] = _clean_text(row_owner_context.get("employee_email"))
            updated[_KEEP_OWNER_OWNER_NAME_FIELD] = _clean_text(row_owner_context.get("owner_name"))
            updated["linked_bitable_url"] = _clean_text(row_owner_context.get("linked_bitable_url"))
            updated["任务名"] = _clean_text(row_owner_context.get("task_name"))
            updated.setdefault(
                "__last_mail_raw_path",
                _clean_text(keep_row.get("brand_message_raw_path") or keep_row.get("last_mail_raw_path")),
            )
            updated.setdefault("__feishu_attachment_local_paths", attachment_paths)
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
    return {
        "达人ID": _extract_creator_id(keep_row),
        "平台": _extract_platform(keep_row),
        "主页链接": _clean_text(keep_row.get("URL") or keep_row.get("主页链接")),
        "当前网红报价": _build_quote_text(keep_row),
        "达人最后一次回复邮件时间": _format_date(keep_row.get("brand_message_sent_at")),
        "达人回复的最后一封邮件内容": _clean_text(keep_row.get("brand_message_snippet")),
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
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    upload_dry_run: bool = False,
    reuse_existing: bool = True,
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
        },
        "task_count": 0,
        "task_names": [],
        "matched_mail_count": 0,
        "new_creator_count": 0,
        "existing_screened_count": 0,
        "existing_unscreened_count": 0,
        "full_screening_count": 0,
        "mail_only_update_count": 0,
        "skipped_existing_count": 0,
        "created_record_count": 0,
        "updated_record_count": 0,
        "failed_record_count": 0,
        "local_archive_path": str(aggregate_archive_dir),
        "task_results": [],
    }
    _write_json(run_summary_path, summary)

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

    aggregate_failed_rows: list[dict[str, Any]] = []
    aggregate_existing_skip_rows: list[dict[str, Any]] = []
    any_task_failed = False

    for item in inspection_items:
        task_name = _clean_text(item.get("taskName"))
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
            "full_screening_count": 0,
            "mail_only_update_count": 0,
            "skipped_existing_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "failed_count": 0,
            "summary_path": str(task_summary_path),
            "all_platforms_final_review": "",
            "all_platforms_upload_payload_json": "",
            "feishu_upload_result_json": "",
            "status": "running",
            "upstream_summary_json": "",
            "downstream_summary_json": "",
        }
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
                stop_after="keep-list",
                reuse_existing=bool(reuse_existing),
                matching_strategy=matching_strategy,
                brand_keyword=_resolve_group_brand_keyword(
                    task_name=task_name,
                    explicit_brand_keyword=brand_keyword,
                ),
                brand_match_include_from=bool(brand_match_include_from),
            )
            task_result["upstream_summary_json"] = str(upstream_summary_path)
            keep_workbook = Path(
                str(
                    ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("keep_workbook")
                    or (upstream_summary.get("artifacts") or {}).get("keep_workbook")
                    or ""
                )
            ).expanduser()
            template_workbook_value = str(
                ((upstream_summary.get("resume_points") or {}).get("keep_list") or {}).get("template_workbook")
                or (upstream_summary.get("artifacts") or {}).get("template_workbook")
                or ""
            ).strip()
            if str(upstream_summary.get("status") or "") == "failed" or not keep_workbook.exists():
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
            _, existing_analysis = fetch_existing_bitable_record_analysis(
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
            owner_scope_enabled = bool(_clean_text(getattr(existing_analysis, "owner_scope_field_name", "")))

            matched_mail_count = _extract_matched_mail_count(upstream_summary, keep_frame)
            mail_only_display_rows: list[dict[str, Any]] = []
            mail_only_payload_rows: list[dict[str, Any]] = []
            combined_skipped_rows: list[dict[str, Any]] = []
            full_screening_rows: list[dict[str, Any]] = []
            existing_screened_count = 0
            existing_unscreened_count = 0
            new_creator_count = 0

            for keep_row in keep_frame.to_dict(orient="records"):
                row_owner_context = _build_owner_context_from_keep_row(keep_row, owner_context)
                row_owner_scope_value = _clean_text(row_owner_context.get("employee_id")) or _clean_text(
                    row_owner_context.get("responsible_name")
                )
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
                    continue
                creator_id = _extract_creator_id(keep_row)
                platform = _extract_platform(keep_row)
                record_key = (
                    _build_record_key(row_owner_scope_value, creator_id, platform)
                    if owner_scope_enabled
                    else _build_record_key(creator_id, platform)
                )
                existing_record = existing_index.get(record_key) if record_key else None
                if existing_record is None:
                    new_creator_count += 1
                    full_screening_rows.append(dict(keep_row))
                    continue
                ai_status = _extract_ai_status(existing_record.get("fields") or {})
                if ai_status:
                    existing_screened_count += 1
                    display_row, payload_row = _build_mail_only_rows(
                        keep_row=keep_row,
                        existing_fields=dict(existing_record.get("fields") or {}),
                        owner_context=row_owner_context,
                        linked_bitable_url=linked_bitable_url,
                        shared_mail_db_path=resolved_mail_db_path,
                        shared_mail_raw_dir=resolved_mail_raw_dir,
                        shared_mail_data_dir=resolved_mail_data_dir,
                        keep_workbook=keep_workbook,
                    )
                    mail_only_display_rows.append(display_row)
                    mail_only_payload_rows.append(payload_row)
                    continue
                existing_unscreened_count += 1
                full_screening_rows.append(dict(keep_row))

            full_screening_frame = pd.DataFrame(full_screening_rows, columns=list(keep_frame.columns))
            full_screening_display_rows: list[dict[str, Any]] = []
            full_screening_payload_rows: list[dict[str, Any]] = []
            downstream_summary_json = ""
            if len(full_screening_frame.index) > 0:
                filtered_keep_workbook = task_root / "partition" / f"{task_slug}_full_screening_keep.xlsx"
                _write_keep_subset(filtered_keep_workbook, full_screening_frame)
                downstream_output_root = task_root / "downstream"
                downstream_summary_path = downstream_output_root / "summary.json"
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

            combined_display_rows = [*mail_only_display_rows, *full_screening_display_rows]
            combined_payload_rows = [*mail_only_payload_rows, *full_screening_payload_rows]
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
            upload_summary = upload_final_review_payload_to_bitable(
                client,
                payload_json_path=combined_payload_artifacts["payload_json_path"],
                linked_bitable_url=linked_bitable_url,
                dry_run=bool(upload_dry_run),
                suppress_ai_labels=True,
            )

            task_failed_count = int(combined_payload_artifacts["payload"]["skipped_row_count"]) + int(
                upload_summary.get("failed_count") or 0
            )
            skipped_existing_count = int(upload_summary.get("skipped_existing_count") or 0)
            task_result.update(
                {
                    "matched_mail_count": matched_mail_count,
                    "full_screening_count": len(full_screening_frame.index),
                    "mail_only_update_count": len(mail_only_payload_rows),
                    "skipped_existing_count": skipped_existing_count,
                    "created_count": int(upload_summary.get("created_count") or 0),
                    "updated_count": int(upload_summary.get("updated_count") or 0),
                    "failed_count": task_failed_count,
                    "all_platforms_final_review": str(combined_workbook_path),
                    "all_platforms_upload_payload_json": str(combined_payload_artifacts["payload_json_path"]),
                    "feishu_upload_result_json": str(upload_summary.get("result_json_path") or ""),
                    "local_archive_path": str(combined_payload_artifacts["archive_dir"]),
                    "status": "completed_with_failures" if task_failed_count > 0 else "completed",
                    "new_creator_count": new_creator_count,
                    "existing_screened_count": existing_screened_count,
                    "existing_unscreened_count": existing_unscreened_count,
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

            summary["matched_mail_count"] += int(matched_mail_count)
            summary["new_creator_count"] += int(new_creator_count)
            summary["existing_screened_count"] += int(existing_screened_count)
            summary["existing_unscreened_count"] += int(existing_unscreened_count)
            summary["full_screening_count"] += int(len(full_screening_frame.index))
            summary["mail_only_update_count"] += int(len(mail_only_payload_rows))
            summary["skipped_existing_count"] += skipped_existing_count
            summary["created_record_count"] += int(upload_summary.get("created_count") or 0)
            summary["updated_record_count"] += int(upload_summary.get("updated_count") or 0)
            summary["failed_record_count"] += int(task_failed_count)
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
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Route a pre-synced shared mailbox db into task-specific keep-list/downstream pipelines and incremental Feishu updates."
    )
    parser.add_argument("--shared-mail-db-path", required=True, help="共享邮箱已经同步好的 email_sync.db。")
    parser.add_argument("--shared-mail-raw-dir", default="", help="共享邮箱 raw 邮件目录；默认推断为 email_sync.db 同级 raw。")
    parser.add_argument("--shared-mail-data-dir", default="", help="共享邮箱邮件数据根目录；默认推断为 email_sync.db 所在目录。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
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
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过 scrape。")
    parser.add_argument("--skip-visual", action="store_true", help="跳过视觉复核。")
    parser.add_argument("--skip-positioning-card-analysis", action="store_true", help="跳过定位卡分析。")
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
    summary = run_shared_mailbox_post_sync_pipeline(
        shared_mail_db_path=Path(args.shared_mail_db_path),
        shared_mail_raw_dir=Path(args.shared_mail_raw_dir) if args.shared_mail_raw_dir else None,
        shared_mail_data_dir=Path(args.shared_mail_data_dir) if args.shared_mail_data_dir else None,
        env_file=args.env_file,
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
        max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
        poll_interval=max(1.0, float(args.poll_interval)),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
        skip_positioning_card_analysis=bool(args.skip_positioning_card_analysis),
        upload_dry_run=bool(args.upload_dry_run),
        reuse_existing=not bool(args.no_reuse_existing),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
