from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib import parse

import pandas as pd

from backend.timezone_utils import shanghai_day_start_ms
from .bitable_export import ResolvedBitableView, resolve_bitable_view_from_url
from .feishu_api import FeishuApiError, FeishuOpenClient
from .task_upload_sync import resolve_task_upload_entry


_FIELD_NAME_ALIASES = {
    "# followers(k)#": "Followers(K)",
    "followers(k)": "Followers(K)",
    "following": "Following",
    "average views (k)": "Median Views (K)",
    "averageviews(k)": "Median Views (K)",
    "median views (k)": "Median Views (K)",
    "medianviews(k)": "Median Views (K)",
    "ai是否通过": "ai 是否通过",
    "标签(ai)": "标签（ai）",
    "标签（ai）": "标签（ai）",
    "ai评价": "ai 评价",
    "达人回复的最后一封邮件内容": "full body",
    "full_body": "full body",
    "full-body": "full body",
}
_INTERNAL_PAYLOAD_KEYS = {
    "达人对接人_employee_id",
    "达人对接人_employee_record_id",
    "达人对接人_employee_email",
    "达人对接人_owner_name",
    "linked_bitable_url",
    "__last_mail_raw_path",
    "__feishu_attachment_local_paths",
    "__feishu_shared_attachment_local_paths",
}
_ATTACHMENT_FIELD_PREFERRED_NAMES = ("附件", "附件列", "上传附件", "文本 12", "文本12")
_ROW_UPDATE_MODE_KEY = "__feishu_update_mode"
_UPDATE_MODE_CREATE_ONLY = "create_only"
_UPDATE_MODE_CREATE_OR_UPDATE = "create_or_update"
_UPDATE_MODE_MAIL_ONLY = "mail_only_update"
_UPDATE_MODE_CREATE_OR_MAIL_ONLY = "create_or_mail_only_update"
_MAIL_ONLY_FIELD_NAMES = (
    "当前网红报价",
    "达人最后一次回复邮件时间",
    "达人回复的最后一封邮件内容",
    "full body",
)

_UPLOAD_BASE_KEY_FIELDS = ("达人ID", "平台")
_OWNER_SCOPE_FIELD_CANDIDATES = ("达人对接人",)
_PREFERRED_TARGET_TABLE_NAMES = ("AI回信管理", "达人管理")
_PREFERRED_TARGET_VIEW_NAMES = ("表格", "总视图")

@dataclass(frozen=True)
class FieldSchema:
    field_id: str
    field_name: str
    field_type: int
    property: dict[str, Any]


@dataclass(frozen=True)
class ExistingRecordSnapshot:
    record_id: str
    fields: dict[str, Any]
    owner_scope_value: str
    creator_id: str
    platform: str


@dataclass(frozen=True)
class ExistingRecordAnalysis:
    index: dict[str, dict[str, Any]]
    duplicate_groups: list[dict[str, Any]]
    key_field_names: tuple[str, ...]
    key_display_name: str
    owner_scope_field_name: str
    owner_scope_missing_record_count: int


def upload_final_review_payload_to_bitable(
    client: FeishuOpenClient,
    *,
    payload_json_path: str | Path,
    linked_bitable_url: str = "",
    task_name: str = "",
    task_upload_url: str = "",
    result_json_path: str | Path | None = None,
    result_xlsx_path: str | Path | None = None,
    dry_run: bool = False,
    limit: int = 0,
    suppress_ai_labels: bool = False,
) -> dict[str, Any]:
    payload_path = Path(str(payload_json_path)).expanduser().resolve()
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    archive_dir = payload_path.parent / "feishu_upload_local_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    resolved_result_json = (
        Path(str(result_json_path)).expanduser().resolve()
        if result_json_path
        else archive_dir / "feishu_bitable_upload_result.json"
    )
    resolved_result_xlsx = (
        Path(str(result_xlsx_path)).expanduser().resolve()
        if result_xlsx_path
        else archive_dir / "feishu_bitable_upload_result.xlsx"
    )

    target_url, target_url_source = _resolve_target_url(
        client,
        payload=payload,
        explicit_linked_bitable_url=linked_bitable_url,
        task_name=task_name,
        task_upload_url=task_upload_url,
    )
    resolved_view = resolve_bitable_view_from_url(client, target_url)
    field_schemas = _fetch_field_schemas(client, resolved_view)
    attachment_schema = _select_attachment_field_schema(field_schemas)
    existing_record_analysis = _build_existing_record_analysis(
        _fetch_existing_records(client, resolved_view),
        field_schemas=field_schemas,
    )
    existing_records = existing_record_analysis.index
    key_field_names = existing_record_analysis.key_field_names
    key_display_name = existing_record_analysis.key_display_name or "达人对接人+达人ID+平台"

    rows = list(payload.get("rows") or [])
    if limit > 0:
        rows = rows[: int(limit)]

    created_rows: list[dict[str, Any]] = []
    updated_rows: list[dict[str, Any]] = []
    skipped_existing_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []
    payload_duplicate_groups = _find_payload_duplicate_groups(rows, key_field_names=key_field_names)
    duplicate_existing_groups = list(existing_record_analysis.duplicate_groups)
    payload_owner_scopes = _extract_payload_owner_scopes(rows)
    missing_owner_scope_field = bool(payload_owner_scopes) and not existing_record_analysis.owner_scope_field_name
    has_unscoped_existing_records = bool(payload_owner_scopes) and int(
        existing_record_analysis.owner_scope_missing_record_count or 0
    ) > 0

    if missing_owner_scope_field or has_unscoped_existing_records or duplicate_existing_groups or payload_duplicate_groups:
        if missing_owner_scope_field:
            failed_rows.append(
                {
                    "status": "blocked_missing_owner_scope_field",
                    "record_key": "",
                    "record_id": "",
                    "existing_record_id": "",
                    "row": {},
                    "error": "目标飞书表缺少 `达人对接人` 字段，无法区分不同负责人下重复的达人记录，已阻止继续写入。",
                }
            )
        if has_unscoped_existing_records:
            failed_rows.append(
                {
                    "status": "blocked_missing_owner_scope_existing_records",
                    "record_key": "",
                    "record_id": "",
                    "existing_record_id": "",
                    "row": {},
                    "error": "目标飞书表已存在未填写 `达人对接人` 的历史记录，当前无法安全区分不同负责人下的达人，已阻止继续写入。",
                }
            )
        for group in duplicate_existing_groups:
            keep_record = dict(group.get("keep_record") or {})
            for duplicate_record in list(group.get("duplicate_records") or []):
                failed_rows.append(
                    {
                        "status": "blocked_duplicate_existing",
                        "record_key": group.get("record_key"),
                        "record_id": duplicate_record.get("record_id") or "",
                        "existing_record_id": keep_record.get("record_id") or "",
                        "row": dict(duplicate_record.get("fields") or {}),
                        "error": f"目标飞书表已存在重复的 {key_display_name} 记录，已阻止继续写入。",
                    }
                )
        for group in payload_duplicate_groups:
            first_row = dict(group.get("rows") or [{}])[0]
            for duplicate_row in list(group.get("rows") or [])[1:]:
                failed_rows.append(
                    {
                        "status": "blocked_duplicate_payload",
                        "record_key": group.get("record_key"),
                        "record_id": "",
                        "existing_record_id": "",
                        "row": dict(duplicate_row or {}),
                        "error": f"当前上传 payload 内部存在重复的 {key_display_name} 记录，已阻止继续写入。",
                    }
                )
            if not failed_rows and first_row:
                failed_rows.append(
                    {
                        "status": "blocked_duplicate_payload",
                        "record_key": group.get("record_key"),
                        "record_id": "",
                        "existing_record_id": "",
                        "row": first_row,
                        "error": f"当前上传 payload 内部存在重复的 {key_display_name} 记录，已阻止继续写入。",
                    }
                )
        error_messages: list[str] = []
        if missing_owner_scope_field:
            error_messages.append("目标飞书表缺少 `达人对接人` 字段")
        if has_unscoped_existing_records:
            error_messages.append("目标飞书表存在未填写 `达人对接人` 的历史记录")
        if duplicate_existing_groups or payload_duplicate_groups:
            error_messages.append(f"目标飞书表或当前 payload 存在重复的 {key_display_name} 记录")
        summary = {
            "ok": False,
            "dry_run": bool(dry_run),
            "payload_json_path": str(payload_path),
            "result_json_path": str(resolved_result_json),
            "result_xlsx_path": str(resolved_result_xlsx),
            "target_url": target_url,
            "target_url_source": target_url_source,
            "target_app_token": resolved_view.app_token,
            "target_table_id": resolved_view.table_id,
            "target_table_name": resolved_view.table_name,
            "target_view_id": resolved_view.view_id,
            "target_view_name": resolved_view.view_name,
            "source_row_count": int(payload.get("row_count") or len(payload.get("rows") or [])),
            "selected_row_count": len(rows),
            "suppress_ai_labels": bool(suppress_ai_labels),
            "created_count": 0,
            "updated_count": 0,
            "skipped_existing_count": 0,
            "failed_count": len(failed_rows),
            "guard_blocked": True,
            "error": "；".join(error_messages) + "，已阻止继续写入。",
            "key_field_names": list(key_field_names),
            "key_display_name": key_display_name,
            "owner_scope_field_name": existing_record_analysis.owner_scope_field_name,
            "owner_scope_missing_record_count": existing_record_analysis.owner_scope_missing_record_count,
            "duplicate_existing_group_count": len(duplicate_existing_groups),
            "duplicate_payload_group_count": len(payload_duplicate_groups),
            "duplicate_existing_groups": duplicate_existing_groups,
            "duplicate_payload_groups": payload_duplicate_groups,
            "created_rows": [],
            "updated_rows": [],
            "skipped_existing_rows": [],
            "failed_rows": failed_rows,
        }
        resolved_result_json.parent.mkdir(parents=True, exist_ok=True)
        resolved_result_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _write_upload_result_xlsx(resolved_result_xlsx, [], [], [], failed_rows)
        return summary

    for row in rows:
        if not isinstance(row, dict):
            continue
        record_key = _build_payload_record_key(row, key_field_names=key_field_names)
        existing_record = existing_records.get(record_key) if record_key else None
        update_mode = _resolve_row_update_mode(row)
        should_update_existing = existing_record is not None and update_mode in {
            _UPDATE_MODE_CREATE_OR_UPDATE,
            _UPDATE_MODE_MAIL_ONLY,
            _UPDATE_MODE_CREATE_OR_MAIL_ONLY,
        }
        use_mail_only_fields = existing_record is not None and update_mode in {
            _UPDATE_MODE_MAIL_ONLY,
            _UPDATE_MODE_CREATE_OR_MAIL_ONLY,
        }
        if update_mode == _UPDATE_MODE_MAIL_ONLY and existing_record is None:
            skipped_existing_rows.append(
                {
                    "status": "skipped_missing_existing_for_mail_only_update",
                    "record_key": record_key,
                    "existing_record_id": "",
                    "row": row,
                    "reason": f"邮件字段更新模式要求飞书中已存在同 {key_display_name} 记录",
                }
            )
            continue
        if existing_record is not None and not should_update_existing:
            skipped_existing_rows.append(
                {
                    "status": "skipped_existing",
                    "record_key": record_key,
                    "existing_record_id": existing_record["record_id"],
                    "row": row,
                    "reason": f"飞书表已存在同 {key_display_name} 记录",
                }
            )
            continue

        try:
            if use_mail_only_fields:
                fields = _build_mail_only_feishu_fields(
                    row,
                    field_schemas,
                    suppress_ai_labels=bool(suppress_ai_labels),
                )
            else:
                fields = _build_feishu_fields(
                    row,
                    field_schemas,
                    suppress_ai_labels=bool(suppress_ai_labels),
                )
            _attach_local_files_to_fields(
                client,
                row=row,
                fields=fields,
                attachment_schema=attachment_schema,
                app_token=resolved_view.app_token,
            )
        except Exception as exc:  # noqa: BLE001
            failed_rows.append(
                {
                    "status": "failed",
                    "record_key": record_key,
                    "row": row,
                    "error": str(exc) or exc.__class__.__name__,
                }
            )
            continue

        if dry_run:
            bucket = updated_rows if should_update_existing else created_rows
            bucket.append(
                {
                    "status": "dry_run_ready_update" if should_update_existing else "dry_run_ready",
                    "record_key": record_key,
                    "row": row,
                    "fields": fields,
                    "record_id": existing_record["record_id"] if existing_record else "",
                    "existing_record_id": existing_record["record_id"] if existing_record else "",
                }
            )
            continue

        try:
            if should_update_existing:
                response = client.put_api_json(
                    f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records/{existing_record['record_id']}",
                    body={"fields": fields},
                )
            else:
                response = client.post_api_json(
                    f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records",
                    body={"fields": fields},
                )
        except Exception as exc:  # noqa: BLE001
            if "URLFieldConvFail" in str(exc) and "主页链接" in fields and not use_mail_only_fields:
                fallback_fields = dict(fields)
                fallback_fields.pop("主页链接", None)
                try:
                    if should_update_existing:
                        response = client.put_api_json(
                            f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records/{existing_record['record_id']}",
                            body={"fields": fallback_fields},
                        )
                    else:
                        response = client.post_api_json(
                            f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records",
                            body={"fields": fallback_fields},
                        )
                except Exception as retry_exc:  # noqa: BLE001
                    failed_rows.append(
                        {
                            "status": "failed",
                            "record_key": record_key,
                            "row": row,
                            "fields": fields,
                            "error": str(retry_exc) or retry_exc.__class__.__name__,
                        }
                    )
                    continue
                record_id = str((((response.get("data") or {}).get("record") or {}).get("record_id")) or "").strip()
                bucket = updated_rows if should_update_existing else created_rows
                bucket.append(
                    {
                        "status": "updated" if should_update_existing else "created",
                        "record_key": record_key,
                        "record_id": record_id or (existing_record["record_id"] if existing_record else ""),
                        "existing_record_id": existing_record["record_id"] if existing_record else "",
                        "row": row,
                        "fields": fallback_fields,
                        "warning": "主页链接字段触发 URLFieldConvFail，已自动省略该列后重试成功",
                    }
                )
                if record_key:
                    existing_records[record_key] = {
                        "record_id": record_id or (existing_record["record_id"] if existing_record else ""),
                        "fields": dict(existing_record.get("fields") or {}) if existing_record else {},
                    }
                continue
            failed_rows.append(
                {
                    "status": "failed",
                    "record_key": record_key,
                    "row": row,
                    "fields": fields,
                    "error": str(exc) or exc.__class__.__name__,
                }
            )
            continue

        record_id = str((((response.get("data") or {}).get("record") or {}).get("record_id")) or "").strip()
        bucket = updated_rows if should_update_existing else created_rows
        bucket.append(
            {
                "status": "updated" if should_update_existing else "created",
                "record_key": record_key,
                "record_id": record_id or (existing_record["record_id"] if existing_record else ""),
                "existing_record_id": existing_record["record_id"] if existing_record else "",
                "row": row,
                "fields": fields,
            }
        )
        if record_key:
            existing_records[record_key] = {
                "record_id": record_id or (existing_record["record_id"] if existing_record else ""),
                "fields": dict(existing_record.get("fields") or {}) if existing_record else {},
            }

    summary = {
        "ok": True,
        "dry_run": bool(dry_run),
        "payload_json_path": str(payload_path),
        "result_json_path": str(resolved_result_json),
        "result_xlsx_path": str(resolved_result_xlsx),
        "target_url": target_url,
        "target_url_source": target_url_source,
        "target_app_token": resolved_view.app_token,
        "target_table_id": resolved_view.table_id,
        "target_table_name": resolved_view.table_name,
        "target_view_id": resolved_view.view_id,
        "target_view_name": resolved_view.view_name,
        "source_row_count": int(payload.get("row_count") or len(payload.get("rows") or [])),
        "selected_row_count": len(rows),
        "suppress_ai_labels": bool(suppress_ai_labels),
        "created_count": len(created_rows),
        "updated_count": len(updated_rows),
        "skipped_existing_count": len(skipped_existing_rows),
        "failed_count": len(failed_rows),
        "created_rows": created_rows,
        "updated_rows": updated_rows,
        "skipped_existing_rows": skipped_existing_rows,
        "failed_rows": failed_rows,
    }
    resolved_result_json.parent.mkdir(parents=True, exist_ok=True)
    resolved_result_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_upload_result_xlsx(resolved_result_xlsx, created_rows, updated_rows, skipped_existing_rows, failed_rows)
    return summary


def _resolve_target_url(
    client: FeishuOpenClient,
    *,
    payload: dict[str, Any],
    explicit_linked_bitable_url: str,
    task_name: str,
    task_upload_url: str,
) -> tuple[str, str]:
    explicit = str(explicit_linked_bitable_url or "").strip()
    if explicit:
        return _canonicalize_target_url(client, explicit), "explicit"
    normalized_task_upload_url = str(task_upload_url or "").strip()
    normalized_task_name = str(task_name or "").strip() or str(((payload.get("task_owner") or {}).get("task_name")) or "").strip()
    if normalized_task_upload_url and normalized_task_name:
        entry = resolve_task_upload_entry(
            client=client,
            task_upload_url=normalized_task_upload_url,
            task_name=normalized_task_name,
        )
        if str(entry.linked_bitable_url or "").strip():
            return _canonicalize_target_url(client, str(entry.linked_bitable_url).strip()), "task_upload_entry"
    payload_link = str(((payload.get("task_owner") or {}).get("linked_bitable_url")) or "").strip()
    if payload_link:
        return _canonicalize_target_url(client, payload_link), "payload_task_owner"
    raise ValueError("缺少 linked_bitable_url，且无法从 task upload 任务记录中解析目标飞书表。")


def _canonicalize_target_url(client: FeishuOpenClient, raw_url: str) -> str:
    normalized = str(raw_url or "").strip()
    if not normalized:
        return ""
    try:
        resolved = resolve_bitable_view_from_url(client, normalized)
        return resolved.source_url
    except ValueError as exc:
        if "缺少 table 参数" not in str(exc) and "缺少 view 参数" not in str(exc):
            raise
    resolved = _resolve_short_bitable_target(client, normalized)
    parsed = parse.urlparse(normalized)
    base_url = parse.urlunparse((parsed.scheme, parsed.netloc, f"/base/{resolved.app_token}", "", "", ""))
    query = parse.urlencode({"table": resolved.table_id, "view": resolved.view_id})
    return f"{base_url}?{query}"


def _resolve_short_bitable_target(client: FeishuOpenClient, url: str) -> ResolvedBitableView:
    parsed = parse.urlparse(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("飞书多维表格 URL 不合法。")
    segments = [item for item in parsed.path.split("/") if item]
    source_kind = ""
    source_token = ""
    app_token = ""
    title = ""
    if len(segments) >= 2 and segments[0] == "base":
        source_kind = "base"
        source_token = segments[1]
        app_token = source_token
    elif len(segments) >= 2 and segments[0] == "wiki":
        source_kind = "wiki"
        source_token = segments[1]
        node = client.resolve_wiki_node(source_token)
        obj_type = str(node.get("obj_type") or "").strip()
        if obj_type != "bitable":
            raise FeishuApiError(f"当前 wiki 节点不是 bitable，而是 {obj_type or 'unknown'}。")
        app_token = str(node.get("obj_token") or "").strip()
        title = str(node.get("title") or "").strip()
    else:
        raise ValueError("当前 URL 不是支持的飞书 base/wiki 多维表格链接。")
    if not app_token:
        raise ValueError("无法从飞书多维表格链接解析 app_token。")

    tables_payload = client.get_api_json(f"/bitable/v1/apps/{app_token}/tables")
    table_items = list((tables_payload.get("data") or {}).get("items") or [])
    table_item = _select_named_item(
        table_items,
        id_key="table_id",
        name_key="name",
        preferred_names=_PREFERRED_TARGET_TABLE_NAMES,
        missing_message="无法在目标飞书 base 内定位可用数据表。",
    )
    table_id = str(table_item.get("table_id") or "").strip()
    table_name = str(table_item.get("name") or "").strip()

    views_payload = client.get_api_json(f"/bitable/v1/apps/{app_token}/tables/{table_id}/views")
    view_items = list((views_payload.get("data") or {}).get("items") or [])
    view_item = _select_named_item(
        view_items,
        id_key="view_id",
        name_key="view_name",
        preferred_names=_PREFERRED_TARGET_VIEW_NAMES,
        missing_message=f"无法在飞书表 {table_name or table_id} 内定位可用视图。",
    )
    return ResolvedBitableView(
        source_url=str(url or "").strip(),
        source_kind=source_kind,
        source_token=source_token,
        app_token=app_token,
        table_id=table_id,
        view_id=str(view_item.get("view_id") or "").strip(),
        table_name=table_name,
        view_name=str(view_item.get("view_name") or "").strip(),
        title=title,
    )


def _select_named_item(
    items: list[dict[str, Any]],
    *,
    id_key: str,
    name_key: str,
    preferred_names: tuple[str, ...],
    missing_message: str,
) -> dict[str, Any]:
    normalized_preferences = {str(name or "").strip().casefold() for name in preferred_names if str(name or "").strip()}
    for item in items:
        if str(item.get(name_key) or "").strip().casefold() in normalized_preferences:
            return item
    if len(items) == 1:
        return items[0]
    available = ", ".join(str(item.get(name_key) or item.get(id_key) or "").strip() for item in items if str(item.get(name_key) or item.get(id_key) or "").strip())
    raise ValueError(missing_message + (f" 可用项：{available}" if available else ""))


def _fetch_field_schemas(client: FeishuOpenClient, resolved: ResolvedBitableView) -> dict[str, FieldSchema]:
    payload = client.get_api_json(f"/bitable/v1/apps/{resolved.app_token}/tables/{resolved.table_id}/fields")
    results: dict[str, FieldSchema] = {}
    for item in (payload.get("data") or {}).get("items") or []:
        if not isinstance(item, dict):
            continue
        field_name = str(item.get("field_name") or "").strip()
        if not field_name:
            continue
        results[field_name] = FieldSchema(
            field_id=str(item.get("field_id") or "").strip(),
            field_name=field_name,
            field_type=int(item.get("type") or 0),
            property=dict(item.get("property") or {}),
        )
    return results


def _select_attachment_field_schema(field_schemas: dict[str, FieldSchema]) -> FieldSchema | None:
    candidates = [schema for schema in field_schemas.values() if int(schema.field_type or 0) == 17]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    normalized_preferences = {_normalize_field_key(name) for name in _ATTACHMENT_FIELD_PREFERRED_NAMES}
    for schema in candidates:
        if _normalize_field_key(schema.field_name) in normalized_preferences:
            return schema
    return candidates[0]


def _fetch_existing_records(client: FeishuOpenClient, resolved: ResolvedBitableView) -> list[tuple[str, dict[str, Any]]]:
    collected: list[tuple[str, dict[str, Any]]] = []
    page_token = ""
    while True:
        body: dict[str, Any] = {"view_id": resolved.view_id, "page_size": 500}
        if page_token:
            body["page_token"] = page_token
        payload = client.post_api_json(
            f"/bitable/v1/apps/{resolved.app_token}/tables/{resolved.table_id}/records/search",
            body=body,
        )
        data = payload.get("data") or {}
        for item in data.get("items") or []:
            if not isinstance(item, dict):
                continue
            collected.append((str(item.get("record_id") or "").strip(), dict(item.get("fields") or {})))
        if not bool(data.get("has_more")):
            break
        page_token = str(data.get("page_token") or "").strip()
        if not page_token:
            break
    return collected


def _build_existing_record_index(existing_records: list[tuple[str, dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    return _build_existing_record_analysis(existing_records).index


def _build_existing_record_analysis(
    existing_records: list[tuple[str, dict[str, Any]]],
    *,
    field_schemas: dict[str, FieldSchema] | None = None,
) -> ExistingRecordAnalysis:
    owner_scope_field_name = _resolve_owner_scope_field_name(field_schemas or {})
    key_field_names = _resolve_key_field_names(owner_scope_field_name)
    key_display_name = _format_key_field_names(key_field_names)
    grouped: dict[str, list[ExistingRecordSnapshot]] = {}
    owner_scope_missing_record_count = 0
    for record_id, fields in existing_records:
        creator_id = _flatten_field_value(fields.get("达人ID"))
        platform = _flatten_field_value(fields.get("平台"))
        if not creator_id or not platform:
            continue
        owner_scope_value = _extract_existing_owner_scope(fields, owner_scope_field_name)
        if owner_scope_field_name and not owner_scope_value:
            owner_scope_missing_record_count += 1
            continue
        key = _build_record_key(*([owner_scope_value] if owner_scope_field_name else []), creator_id, platform)
        if not key:
            continue
        grouped.setdefault(key, []).append(
            ExistingRecordSnapshot(
                record_id=record_id,
                fields=dict(fields or {}),
                owner_scope_value=owner_scope_value,
                creator_id=creator_id,
                platform=platform,
            )
        )

    index: dict[str, dict[str, Any]] = {}
    duplicate_groups: list[dict[str, Any]] = []
    for record_key, snapshots in grouped.items():
        ordered = sorted(snapshots, key=_existing_record_sort_key, reverse=True)
        keep = ordered[0]
        index[record_key] = {
            "record_id": keep.record_id,
            "fields": dict(keep.fields or {}),
        }
        if len(ordered) <= 1:
            continue
        duplicate_groups.append(
            {
                "record_key": record_key,
                "owner_scope_value": keep.owner_scope_value,
                "creator_id": keep.creator_id,
                "platform": keep.platform,
                "keep_record": {
                    "record_id": keep.record_id,
                    "fields": dict(keep.fields or {}),
                },
                "duplicate_records": [
                    {
                        "record_id": item.record_id,
                        "fields": dict(item.fields or {}),
                    }
                    for item in ordered[1:]
                ],
            }
        )
    return ExistingRecordAnalysis(
        index=index,
        duplicate_groups=duplicate_groups,
        key_field_names=key_field_names,
        key_display_name=key_display_name,
        owner_scope_field_name=owner_scope_field_name,
        owner_scope_missing_record_count=owner_scope_missing_record_count,
    )


def fetch_existing_bitable_record_index(
    client: FeishuOpenClient,
    *,
    linked_bitable_url: str,
) -> tuple[ResolvedBitableView, dict[str, dict[str, Any]]]:
    resolved_view = resolve_bitable_view_from_url(client, _canonicalize_target_url(client, linked_bitable_url))
    field_schemas = _fetch_field_schemas(client, resolved_view)
    existing_records = _fetch_existing_records(client, resolved_view)
    return resolved_view, _build_existing_record_analysis(existing_records, field_schemas=field_schemas).index


def fetch_existing_bitable_record_analysis(
    client: FeishuOpenClient,
    *,
    linked_bitable_url: str,
) -> tuple[ResolvedBitableView, ExistingRecordAnalysis]:
    resolved_view = resolve_bitable_view_from_url(client, _canonicalize_target_url(client, linked_bitable_url))
    field_schemas = _fetch_field_schemas(client, resolved_view)
    existing_records = _fetch_existing_records(client, resolved_view)
    return resolved_view, _build_existing_record_analysis(existing_records, field_schemas=field_schemas)


def _build_record_key(*parts: Any) -> str:
    normalized_parts = [_flatten_field_value(part).casefold() for part in parts]
    if any(not part for part in normalized_parts):
        return ""
    return "::".join(normalized_parts)


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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _existing_record_sort_key(snapshot: ExistingRecordSnapshot) -> tuple[Any, ...]:
    fields = snapshot.fields or {}
    return (
        bool(_flatten_field_value(fields.get("ai 是否通过") or fields.get("ai是否通过"))),
        bool(_flatten_field_value(fields.get("ai筛号反馈理由"))),
        bool(_flatten_field_value(fields.get("标签(ai)") or fields.get("标签（ai）"))),
        bool(_flatten_field_value(fields.get("ai评价") or fields.get("ai 评价"))),
        bool(_flatten_field_value(fields.get("当前网红报价"))),
        _coerce_date_to_ms(_flatten_field_value(fields.get("达人最后一次回复邮件时间"))) or 0,
        len(_flatten_field_value(fields.get("full body") or fields.get("达人回复的最后一封邮件内容"))),
        len(_flatten_field_value(fields.get("主页链接"))),
        snapshot.record_id,
    )


def _find_payload_duplicate_groups(
    rows: list[dict[str, Any]],
    *,
    key_field_names: tuple[str, ...] = _UPLOAD_BASE_KEY_FIELDS,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        record_key = _build_payload_record_key(row, key_field_names=key_field_names)
        if not record_key:
            continue
        grouped.setdefault(record_key, []).append(dict(row))
    duplicates: list[dict[str, Any]] = []
    for record_key, grouped_rows in grouped.items():
        if len(grouped_rows) <= 1:
            continue
        duplicates.append(
            {
                "record_key": record_key,
                "owner_scope_value": _extract_payload_owner_scope(grouped_rows[0])
                if len(key_field_names) > 2
                else "",
                "creator_id": _flatten_field_value(grouped_rows[0].get("达人ID")),
                "platform": _flatten_field_value(grouped_rows[0].get("平台")),
                "rows": grouped_rows,
            }
        )
    return duplicates


def _resolve_owner_scope_field_name(field_schemas: dict[str, FieldSchema]) -> str:
    for candidate in _OWNER_SCOPE_FIELD_CANDIDATES:
        schema = _lookup_field_schema(field_schemas, candidate)
        if schema is not None:
            return schema.field_name
    return ""


def _resolve_key_field_names(owner_scope_field_name: str) -> tuple[str, ...]:
    if _clean_text(owner_scope_field_name):
        return (owner_scope_field_name, *_UPLOAD_BASE_KEY_FIELDS)
    return _UPLOAD_BASE_KEY_FIELDS


def _format_key_field_names(key_field_names: tuple[str, ...]) -> str:
    return "+".join(str(name).strip() for name in key_field_names if str(name).strip())


def _get_field_value_by_candidates(fields: dict[str, Any], *candidates: str) -> Any:
    normalized_candidates = {_normalize_field_key(name) for name in candidates if _clean_text(name)}
    for key, value in (fields or {}).items():
        if _normalize_field_key(str(key or "")) in normalized_candidates:
            return value
    return ""


def _build_payload_record_key(row: dict[str, Any], *, key_field_names: tuple[str, ...]) -> str:
    values: list[Any] = []
    for field_name in key_field_names:
        if field_name == "达人对接人":
            values.append(_extract_payload_owner_scope(row))
        else:
            values.append(row.get(field_name))
    return _build_record_key(*values)


def _extract_payload_owner_scopes(rows: list[dict[str, Any]]) -> set[str]:
    owner_scopes: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        owner_scope = _extract_payload_owner_scope(row)
        if owner_scope:
            owner_scopes.add(owner_scope)
    return owner_scopes


def _extract_payload_owner_scope(row: dict[str, Any]) -> str:
    return (
        _clean_text(row.get("达人对接人_employee_id"))
        or _clean_text(row.get("达人对接人"))
        or _clean_text(row.get("达人对接人_owner_name"))
        or _clean_text(row.get("达人对接人_employee_email"))
    )


def _extract_existing_owner_scope(fields: dict[str, Any], owner_scope_field_name: str) -> str:
    if not _clean_text(owner_scope_field_name):
        return ""
    value = _get_field_value_by_candidates(fields, owner_scope_field_name)
    if isinstance(value, list):
        parts = [_extract_existing_owner_scope({"达人对接人": item}, "达人对接人") for item in value]
        return "；".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("id", "email", "name", "en_name", "text", "value", "link"):
            candidate = _clean_text(value.get(key))
            if candidate:
                return candidate
        return ""
    return _clean_text(value)


def _normalize_field_key(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        return ""
    alias_key = normalized.casefold()
    aliased = _FIELD_NAME_ALIASES.get(alias_key, normalized)
    return aliased.strip().replace("（", "(").replace("）", ")").replace(" ", "").casefold()


def _lookup_field_schema(field_schemas: dict[str, FieldSchema], desired_name: str) -> FieldSchema | None:
    normalized_desired = _normalize_field_key(desired_name)
    for field_name, schema in field_schemas.items():
        if _normalize_field_key(field_name) == normalized_desired:
            return schema
    return None


def _build_feishu_fields(
    row: dict[str, Any],
    field_schemas: dict[str, FieldSchema],
    *,
    suppress_ai_labels: bool = False,
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for payload_name, raw_value in row.items():
        if payload_name in _INTERNAL_PAYLOAD_KEYS or str(payload_name).startswith("__"):
            continue
        schema = _lookup_field_schema(field_schemas, payload_name)
        if schema is None:
            continue
        if bool(suppress_ai_labels) and schema.field_name == "标签（ai）":
            continue
        converted, include = _convert_field_value(schema, raw_value, row=row)
        if include:
            fields[schema.field_name] = converted

    person_schema = _lookup_field_schema(field_schemas, "达人对接人")
    employee_id = _clean_text(row.get("达人对接人_employee_id")).split(",")[0].strip()
    if person_schema is not None and employee_id:
        fields[person_schema.field_name] = [{"id": employee_id}]

    return fields


def _build_mail_only_feishu_fields(
    row: dict[str, Any],
    field_schemas: dict[str, FieldSchema],
    *,
    suppress_ai_labels: bool = False,
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for payload_name in _MAIL_ONLY_FIELD_NAMES:
        schema = _lookup_field_schema(field_schemas, payload_name)
        if schema is None:
            continue
        if bool(suppress_ai_labels) and schema.field_name == "标签（ai）":
            continue
        converted, include = _convert_field_value(schema, row.get(payload_name), row=row)
        if include:
            fields[schema.field_name] = converted
    return fields


def _attach_local_files_to_fields(
    client: FeishuOpenClient,
    *,
    row: dict[str, Any],
    fields: dict[str, Any],
    attachment_schema: FieldSchema | None,
    app_token: str,
) -> None:
    if attachment_schema is None:
        return
    upload_items: list[dict[str, str]] = []
    for local_path in _normalize_attachment_local_paths(row.get("__feishu_attachment_local_paths")):
        uploaded = client.upload_local_file(
            local_path,
            parent_type="bitable_file",
            parent_node=app_token,
        )
        upload_items.append(
            {
                "file_token": uploaded.file_token,
                "name": uploaded.file_name,
            }
        )
    if upload_items:
        fields[attachment_schema.field_name] = upload_items


def _normalize_attachment_local_paths(raw_value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw_value, (list, tuple, set)):
        iterator = raw_value
    else:
        iterator = [raw_value]
    seen: set[str] = set()
    for item in iterator:
        cleaned = _clean_text(item)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        values.append(cleaned)
    return values


def _resolve_row_update_mode(row: dict[str, Any]) -> str:
    mode = _clean_text(row.get(_ROW_UPDATE_MODE_KEY)).lower()
    if mode in {
        _UPDATE_MODE_CREATE_ONLY,
        _UPDATE_MODE_CREATE_OR_UPDATE,
        _UPDATE_MODE_MAIL_ONLY,
        _UPDATE_MODE_CREATE_OR_MAIL_ONLY,
    }:
        return mode
    return _UPDATE_MODE_CREATE_ONLY


def _convert_field_value(schema: FieldSchema, raw_value: Any, *, row: dict[str, Any]) -> tuple[Any, bool]:
    field_name = schema.field_name
    if field_name == "达人对接人":
        return None, False
    if field_name == "标签（ai）":
        values = _resolve_multiselect_values(schema, raw_value)
        return values, bool(values)
    if schema.field_type == 15:
        url_value = _resolve_url_value(raw_value)
        return url_value, bool(url_value)
    if schema.field_type == 2:
        number = _coerce_number(raw_value)
        return number, number is not None
    if schema.field_type == 5:
        timestamp = _coerce_date_to_ms(raw_value)
        return timestamp, timestamp is not None
    if schema.field_type == 3:
        option_value = _resolve_single_select_value(schema, raw_value)
        return option_value, bool(option_value)
    if schema.field_type == 4:
        values = _resolve_multiselect_values(schema, raw_value)
        return values, bool(values)
    cleaned = _clean_text(raw_value)
    return cleaned, bool(cleaned)


def _coerce_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        if math.isclose(float(value), round(float(value)), rel_tol=0.0, abs_tol=1e-9):
            return int(round(float(value)))
        return float(value)
    cleaned = _clean_text(value).replace(",", "")
    if not cleaned:
        return None
    try:
        numeric = float(cleaned)
    except ValueError:
        return None
    if math.isclose(numeric, round(numeric), rel_tol=0.0, abs_tol=1e-9):
        return int(round(numeric))
    return numeric


def _coerce_date_to_ms(value: Any) -> int | None:
    return shanghai_day_start_ms(value)


def _resolve_single_select_value(schema: FieldSchema, raw_value: Any) -> str:
    cleaned = _clean_text(raw_value)
    if not cleaned:
        return ""
    options = (schema.property or {}).get("options") or []
    normalized_lookup = {_clean_text(option.get("name")): _clean_text(option.get("name")) for option in options if isinstance(option, dict)}
    return normalized_lookup.get(cleaned, "")


def _resolve_multiselect_values(schema: FieldSchema, raw_value: Any) -> list[str]:
    options = (schema.property or {}).get("options") or []
    option_lookup = {
        _clean_text(option.get("name")).replace("（", "(").replace("）", ")").strip(): _clean_text(option.get("name")).strip()
        for option in options
        if isinstance(option, dict) and _clean_text(option.get("name"))
    }
    results: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[；;,/]+", _clean_text(raw_value)):
        candidate = part.replace("（", "(").replace("）", ")").strip()
        if not candidate:
            continue
        matched = option_lookup.get(candidate)
        if not matched or matched in seen:
            continue
        seen.add(matched)
        results.append(matched)
    return results


def _resolve_url_value(raw_value: Any) -> dict[str, str] | None:
    cleaned = _clean_text(raw_value)
    if not cleaned:
        return None
    return {
        "link": cleaned,
        "text": cleaned,
        "type": "url",
    }


def _write_upload_result_xlsx(
    output_path: Path,
    created_rows: list[dict[str, Any]],
    updated_rows: list[dict[str, Any]],
    skipped_existing_rows: list[dict[str, Any]],
    failed_rows: list[dict[str, Any]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    status_rows: list[dict[str, Any]] = []
    for item in created_rows:
        status_rows.append(_flatten_status_row(item, default_reason=""))
    for item in updated_rows:
        status_rows.append(_flatten_status_row(item, default_reason=""))
    for item in skipped_existing_rows:
        status_rows.append(_flatten_status_row(item, default_reason=str(item.get("reason") or "")))
    for item in failed_rows:
        status_rows.append(_flatten_status_row(item, default_reason=str(item.get("error") or "")))
    columns = (
        "status",
        "reason",
        "record_id",
        "existing_record_id",
        "达人ID",
        "平台",
        "主页链接",
        "达人对接人",
        "ai是否通过",
        "标签(ai)",
    )
    frame = pd.DataFrame(status_rows, columns=columns)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="upload_result")


def _flatten_status_row(item: dict[str, Any], *, default_reason: str) -> dict[str, Any]:
    row = dict(item.get("row") or {})
    return {
        "status": _clean_text(item.get("status")),
        "reason": _clean_text(item.get("reason")) or _clean_text(item.get("warning")) or _clean_text(item.get("error")) or default_reason,
        "record_id": _clean_text(item.get("record_id")),
        "existing_record_id": _clean_text(item.get("existing_record_id")),
        "达人ID": _flatten_field_value(row.get("达人ID")),
        "平台": _flatten_field_value(row.get("平台")),
        "主页链接": _flatten_field_value(row.get("主页链接")),
        "达人对接人": _flatten_field_value(row.get("达人对接人")),
        "ai是否通过": _flatten_field_value(row.get("ai是否通过") or row.get("ai 是否通过")),
        "标签(ai)": _flatten_field_value(row.get("标签(ai)") or row.get("标签（ai）")),
    }
