from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .bitable_export import ResolvedBitableView, resolve_bitable_view_from_url
from .feishu_api import FeishuOpenClient
from .task_upload_sync import resolve_task_upload_entry


_FIELD_NAME_ALIASES = {
    "# followers(k)#": "Followers(K)",
    "followers(k)": "Followers(K)",
    "ai是否通过": "ai 是否通过",
    "标签(ai)": "标签（ai）",
    "标签（ai）": "标签（ai）",
    "ai评价": "ai 评价",
}
_INTERNAL_PAYLOAD_KEYS = {
    "达人对接人_employee_id",
    "达人对接人_employee_record_id",
    "达人对接人_employee_email",
    "达人对接人_owner_name",
    "linked_bitable_url",
    "任务名",
    "__last_mail_raw_path",
    "__feishu_attachment_local_paths",
    "__feishu_shared_attachment_local_paths",
}
_ATTACHMENT_FIELD_PREFERRED_NAMES = ("附件", "附件列", "上传附件", "文本 12", "文本12")

_UPLOAD_KEY_FIELDS = ("达人ID", "平台")


@dataclass(frozen=True)
class FieldSchema:
    field_id: str
    field_name: str
    field_type: int
    property: dict[str, Any]


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
    existing_records = _fetch_existing_records(client, resolved_view)
    existing_keys = {
        _build_record_key(fields.get("达人ID"), fields.get("平台")): record_id
        for record_id, fields in existing_records
        if _build_record_key(fields.get("达人ID"), fields.get("平台"))
    }

    rows = list(payload.get("rows") or [])
    if limit > 0:
        rows = rows[: int(limit)]

    created_rows: list[dict[str, Any]] = []
    skipped_existing_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        record_key = _build_record_key(row.get("达人ID"), row.get("平台"))
        if record_key and record_key in existing_keys:
            skipped_existing_rows.append(
                {
                    "status": "skipped_existing",
                    "record_key": record_key,
                    "existing_record_id": existing_keys[record_key],
                    "row": row,
                    "reason": "飞书表已存在同达人ID+平台记录",
                }
            )
            continue

        try:
            fields = _build_feishu_fields(row, field_schemas)
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
            created_rows.append(
                {
                    "status": "dry_run_ready",
                    "record_key": record_key,
                    "row": row,
                    "fields": fields,
                    "record_id": "",
                }
            )
            continue

        try:
            response = client.post_api_json(
                f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records",
                body={"fields": fields},
            )
        except Exception as exc:  # noqa: BLE001
            if "URLFieldConvFail" in str(exc) and "主页链接" in fields:
                fallback_fields = dict(fields)
                fallback_fields.pop("主页链接", None)
                try:
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
                created_rows.append(
                    {
                        "status": "created",
                        "record_key": record_key,
                        "record_id": record_id,
                        "row": row,
                        "fields": fallback_fields,
                        "warning": "主页链接字段触发 URLFieldConvFail，已自动省略该列后重试成功",
                    }
                )
                if record_key:
                    existing_keys[record_key] = record_id
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
        created_rows.append(
            {
                "status": "created",
                "record_key": record_key,
                "record_id": record_id,
                "row": row,
                "fields": fields,
            }
        )
        if record_key:
            existing_keys[record_key] = record_id

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
        "created_count": len(created_rows),
        "skipped_existing_count": len(skipped_existing_rows),
        "failed_count": len(failed_rows),
        "created_rows": created_rows,
        "skipped_existing_rows": skipped_existing_rows,
        "failed_rows": failed_rows,
    }
    resolved_result_json.parent.mkdir(parents=True, exist_ok=True)
    resolved_result_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_upload_result_xlsx(resolved_result_xlsx, created_rows, skipped_existing_rows, failed_rows)
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
        return explicit, "explicit"
    normalized_task_upload_url = str(task_upload_url or "").strip()
    normalized_task_name = str(task_name or "").strip() or str(((payload.get("task_owner") or {}).get("task_name")) or "").strip()
    if normalized_task_upload_url and normalized_task_name:
        entry = resolve_task_upload_entry(
            client=client,
            task_upload_url=normalized_task_upload_url,
            task_name=normalized_task_name,
        )
        if str(entry.linked_bitable_url or "").strip():
            return str(entry.linked_bitable_url).strip(), "task_upload_entry"
    payload_link = str(((payload.get("task_owner") or {}).get("linked_bitable_url")) or "").strip()
    if payload_link:
        return payload_link, "payload_task_owner"
    raise ValueError("缺少 linked_bitable_url，且无法从 task upload 任务记录中解析目标飞书表。")


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


def _build_record_key(creator_id: Any, platform: Any) -> str:
    left = _clean_text(creator_id).casefold()
    right = _clean_text(platform).casefold()
    if not left or not right:
        return ""
    return f"{left}::{right}"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


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


def _build_feishu_fields(row: dict[str, Any], field_schemas: dict[str, FieldSchema]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for payload_name, raw_value in row.items():
        if payload_name in _INTERNAL_PAYLOAD_KEYS or str(payload_name).startswith("__"):
            continue
        schema = _lookup_field_schema(field_schemas, payload_name)
        if schema is None:
            continue
        converted, include = _convert_field_value(schema, raw_value, row=row)
        if include:
            fields[schema.field_name] = converted

    person_schema = _lookup_field_schema(field_schemas, "达人对接人")
    employee_id = _clean_text(row.get("达人对接人_employee_id")).split(",")[0].strip()
    if person_schema is not None and employee_id:
        fields[person_schema.field_name] = [{"id": employee_id}]

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
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    try:
        parsed = pd.to_datetime(cleaned)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    dt = datetime(parsed.year, parsed.month, parsed.day)
    return int(dt.timestamp() * 1000)


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
    skipped_existing_rows: list[dict[str, Any]],
    failed_rows: list[dict[str, Any]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    status_rows: list[dict[str, Any]] = []
    for item in created_rows:
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
        "达人ID": _clean_text(row.get("达人ID")),
        "平台": _clean_text(row.get("平台")),
        "主页链接": _clean_text(row.get("主页链接")),
        "达人对接人": _clean_text(row.get("达人对接人")),
        "ai是否通过": _clean_text(row.get("ai是否通过")),
        "标签(ai)": _clean_text(row.get("标签(ai)")),
    }
