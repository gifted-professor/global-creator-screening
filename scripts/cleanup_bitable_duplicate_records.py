from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feishu_screening_bridge.bitable_export import resolve_bitable_view_from_url
from feishu_screening_bridge.bitable_upload import (
    _canonicalize_target_url,
    _fetch_existing_records,
    _fetch_field_schemas,
    _resolve_owner_scope_field_name,
    _extract_existing_owner_scope,
    _build_record_key,
    fetch_existing_bitable_record_analysis,
)
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env

KEEP_STRATEGY_NAME = "prefer_screened_then_richer_then_latest_mail_then_profile_url_then_record_id"
KEEP_STRATEGY_DESCRIPTION = (
    "优先保留 ai 是否通过 / 筛号反馈 / 标签 / 评价 / 报价 更完整，且最近邮件时间更新、邮件正文更长、主页链接更完整的记录；"
    "若仍无法区分，则按 record_id 稳定排序。"
)
PLATFORM_REPAIR_STRATEGY_NAME = "infer_platform_from_profile_url"
PLATFORM_REPAIR_STRATEGY_DESCRIPTION = "根据主页链接域名反推真实平台，并将污染的 `平台` 字段修复为 instagram / tiktok / youtube 等标准值。"
KEY_MODE_CREATOR_PLATFORM = "creator_platform"
KEY_MODE_CREATOR_PROFILE_URL = "creator_profile_url"


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


def _coerce_datetime(value: Any) -> datetime | None:
    cleaned = _flatten_field_value(value)
    if not cleaned:
        return None
    try:
        parsed = pd.to_datetime(cleaned)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _record_score(fields: dict[str, Any], record_id: str) -> tuple[Any, ...]:
    return (
        bool(_flatten_field_value(fields.get("ai 是否通过") or fields.get("ai是否通过"))),
        bool(_flatten_field_value(fields.get("ai筛号反馈理由"))),
        bool(_flatten_field_value(fields.get("标签(ai)") or fields.get("标签（ai）"))),
        bool(_flatten_field_value(fields.get("ai评价") or fields.get("ai 评价"))),
        bool(_flatten_field_value(fields.get("当前网红报价"))),
        _coerce_datetime(fields.get("达人最后一次回复邮件时间")) or datetime.min,
        len(_flatten_field_value(fields.get("达人回复的最后一封邮件内容"))),
        len(_flatten_field_value(fields.get("主页链接"))),
        record_id,
    )


def _infer_platform_from_profile_url(value: Any) -> str:
    raw_url = _flatten_field_value(value)
    if not raw_url:
        return ""
    parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url.lstrip('/')}")
    hostname = str(parsed.hostname or "").strip().casefold()
    if not hostname and str(parsed.path or "").strip():
        hostname = str(parsed.path).split("/", 1)[0].split(":", 1)[0].strip().casefold()
    if not hostname:
        return ""
    if hostname == "tiktok.com" or hostname.endswith(".tiktok.com"):
        return "tiktok"
    if hostname == "instagram.com" or hostname.endswith(".instagram.com"):
        return "instagram"
    if hostname == "youtube.com" or hostname.endswith(".youtube.com") or hostname == "youtu.be" or hostname.endswith(".youtu.be"):
        return "youtube"
    if hostname == "facebook.com" or hostname.endswith(".facebook.com"):
        return "facebook"
    return ""


def _group_unique_profile_urls(group: dict[str, Any]) -> list[str]:
    urls: set[str] = set()
    records = [dict(group.get("keep_record") or {})] + [dict(item or {}) for item in list(group.get("duplicate_records") or [])]
    for record in records:
        fields = dict(record.get("fields") or {})
        url = _flatten_field_value(fields.get("主页链接"))
        if url:
            urls.add(url)
    return sorted(urls)


def _partition_duplicate_groups(
    duplicate_groups: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    safe_groups: list[dict[str, Any]] = []
    risky_groups: list[dict[str, Any]] = []
    for group in duplicate_groups:
        if len(_group_unique_profile_urls(group)) == 1:
            safe_groups.append(group)
        else:
            risky_groups.append(group)
    return safe_groups, risky_groups


def _count_platform_pollution(groups: list[dict[str, Any]]) -> tuple[int, int]:
    polluted_group_count = 0
    polluted_row_count = 0
    for group in groups:
        group_polluted_rows = 0
        records = [dict(group.get("keep_record") or {})] + [dict(item or {}) for item in list(group.get("duplicate_records") or [])]
        for record in records:
            fields = dict(record.get("fields") or {})
            inferred_platform = _infer_platform_from_profile_url(fields.get("主页链接"))
            if not inferred_platform:
                continue
            current_platform = _flatten_field_value(fields.get("平台"))
            if current_platform.casefold() != inferred_platform.casefold():
                group_polluted_rows += 1
        if group_polluted_rows > 0:
            polluted_group_count += 1
            polluted_row_count += group_polluted_rows
    return polluted_group_count, polluted_row_count


def _build_report_rows(duplicate_groups: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    report_rows: list[dict[str, Any]] = []
    histogram: Counter = Counter()
    for group in duplicate_groups:
        keep_record = dict(group.get("keep_record") or {})
        keep_fields = dict(keep_record.get("fields") or {})
        keep_record_id = _clean_text(keep_record.get("record_id"))
        duplicate_records = list(group.get("duplicate_records") or [])
        histogram[len(duplicate_records) + 1] += 1
        report_rows.append(
            {
                "record_key": _clean_text(group.get("record_key")),
                "owner_scope_value": _clean_text(group.get("owner_scope_value")),
                "creator_id": _clean_text(group.get("creator_id")),
                "platform": _clean_text(group.get("platform")),
                "action": "keep",
                "record_id": keep_record_id,
                "paired_keep_record_id": keep_record_id,
                "ai_status": _flatten_field_value(keep_fields.get("ai 是否通过") or keep_fields.get("ai是否通过")),
                "last_mail_time": _flatten_field_value(keep_fields.get("达人最后一次回复邮件时间")),
                "quote": _flatten_field_value(keep_fields.get("当前网红报价")),
                "profile_url": _flatten_field_value(keep_fields.get("主页链接")),
                "reason": f"按 keep_strategy 保留该分组里信息最完整、邮件时间较新的记录。({KEEP_STRATEGY_NAME})",
            }
        )
        for duplicate_record in duplicate_records:
            duplicate_fields = dict(duplicate_record.get("fields") or {})
            report_rows.append(
                {
                    "record_key": _clean_text(group.get("record_key")),
                    "owner_scope_value": _clean_text(group.get("owner_scope_value")),
                    "creator_id": _clean_text(group.get("creator_id")),
                    "platform": _clean_text(group.get("platform")),
                    "action": "delete",
                    "record_id": _clean_text(duplicate_record.get("record_id")),
                    "paired_keep_record_id": keep_record_id,
                    "ai_status": _flatten_field_value(
                        duplicate_fields.get("ai 是否通过") or duplicate_fields.get("ai是否通过")
                    ),
                    "last_mail_time": _flatten_field_value(duplicate_fields.get("达人最后一次回复邮件时间")),
                    "quote": _flatten_field_value(duplicate_fields.get("当前网红报价")),
                    "profile_url": _flatten_field_value(duplicate_fields.get("主页链接")),
                    "reason": f"与保留记录共享同一去重主键，且排序低于 keep_record，判定为重复脏记录。({KEEP_STRATEGY_NAME})",
                }
            )
    return report_rows, histogram


def _build_platform_repair_rows(duplicate_groups: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    report_rows: list[dict[str, Any]] = []
    histogram: Counter[str] = Counter()
    for group in duplicate_groups:
        records = [dict(group.get("keep_record") or {})] + [dict(item or {}) for item in list(group.get("duplicate_records") or [])]
        for record in records:
            fields = dict(record.get("fields") or {})
            current_platform = _flatten_field_value(fields.get("平台")) or _clean_text(group.get("platform"))
            inferred_platform = _infer_platform_from_profile_url(fields.get("主页链接"))
            if not inferred_platform:
                continue
            histogram[inferred_platform] += 1
            report_rows.append(
                {
                    "record_key": _clean_text(group.get("record_key")),
                    "creator_id": _clean_text(group.get("creator_id")),
                    "record_id": _clean_text(record.get("record_id")),
                    "current_platform": current_platform,
                    "inferred_platform": inferred_platform,
                    "profile_url": _flatten_field_value(fields.get("主页链接")),
                    "action": "update" if current_platform.casefold() != inferred_platform.casefold() else "keep",
                    "reason": f"根据主页链接推断平台，并修复污染的 `平台` 字段。({PLATFORM_REPAIR_STRATEGY_NAME})",
                }
            )
    return report_rows, dict(histogram)


def _resolve_cleanup_record_analysis(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    key_mode: str,
) -> tuple[Any, Any]:
    normalized_key_mode = str(key_mode or KEY_MODE_CREATOR_PLATFORM).strip().lower()
    if normalized_key_mode == KEY_MODE_CREATOR_PLATFORM:
        return fetch_existing_bitable_record_analysis(client, linked_bitable_url=linked_bitable_url)

    if normalized_key_mode != KEY_MODE_CREATOR_PROFILE_URL:
        raise ValueError(f"不支持的 key_mode: {key_mode}")

    resolved_view = resolve_bitable_view_from_url(client, _canonicalize_target_url(client, linked_bitable_url))
    field_schemas = _fetch_field_schemas(client, resolved_view)
    existing_records = _fetch_existing_records(client, resolved_view)
    owner_scope_field_name = _resolve_owner_scope_field_name(field_schemas or {})
    owner_scope_missing_record_count = 0
    missing_creator_id_record_count = 0
    missing_profile_url_record_count = 0
    grouped: dict[str, list[dict[str, Any]]] = {}

    for record_id, fields in existing_records:
        creator_id = _flatten_field_value(fields.get("达人ID"))
        profile_url = _flatten_field_value(fields.get("主页链接"))
        if not creator_id:
            missing_creator_id_record_count += 1
            continue
        if not profile_url:
            missing_profile_url_record_count += 1
            continue
        owner_scope_value = _extract_existing_owner_scope(fields, owner_scope_field_name)
        if owner_scope_field_name and not owner_scope_value:
            owner_scope_missing_record_count += 1
            continue
        key = _build_record_key(*([owner_scope_value] if owner_scope_field_name else []), creator_id, profile_url)
        if not key:
            continue
        grouped.setdefault(key, []).append(
            {
                "record_id": _clean_text(record_id),
                "fields": dict(fields or {}),
                "owner_scope_value": owner_scope_value,
                "creator_id": creator_id,
                "platform": _infer_platform_from_profile_url(profile_url),
                "profile_url": profile_url,
            }
        )

    index: dict[str, dict[str, Any]] = {}
    duplicate_groups: list[dict[str, Any]] = []
    key_field_names = (owner_scope_field_name, "达人ID", "主页链接") if _clean_text(owner_scope_field_name) else ("达人ID", "主页链接")
    key_display_name = "+".join(name for name in key_field_names if _clean_text(name))
    for record_key, snapshots in grouped.items():
        ordered = sorted(
            snapshots,
            key=lambda item: _record_score(dict(item.get("fields") or {}), _clean_text(item.get("record_id"))),
            reverse=True,
        )
        keep = ordered[0]
        index[record_key] = {
            "record_id": _clean_text(keep.get("record_id")),
            "fields": dict(keep.get("fields") or {}),
        }
        if len(ordered) <= 1:
            continue
        duplicate_groups.append(
            {
                "record_key": record_key,
                "owner_scope_value": _clean_text(keep.get("owner_scope_value")),
                "creator_id": _clean_text(keep.get("creator_id")),
                "platform": _clean_text(keep.get("platform")),
                "keep_record": {
                    "record_id": _clean_text(keep.get("record_id")),
                    "fields": dict(keep.get("fields") or {}),
                },
                "duplicate_records": [
                    {
                        "record_id": _clean_text(item.get("record_id")),
                        "fields": dict(item.get("fields") or {}),
                    }
                    for item in ordered[1:]
                ],
            }
        )

    return resolved_view, SimpleNamespace(
        index=index,
        duplicate_groups=duplicate_groups,
        key_field_names=key_field_names,
        key_display_name=key_display_name,
        owner_scope_field_name=owner_scope_field_name,
        owner_scope_missing_record_count=owner_scope_missing_record_count,
        skipped_owner_scope_record_count=owner_scope_missing_record_count,
        skipped_missing_creator_id_record_count=missing_creator_id_record_count,
        skipped_missing_profile_url_record_count=missing_profile_url_record_count,
        skipped_record_count=(
            owner_scope_missing_record_count
            + missing_creator_id_record_count
            + missing_profile_url_record_count
        ),
    )


def _write_report(output_root: Path, *, summary: dict[str, Any], report_rows: list[dict[str, Any]]) -> tuple[Path, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "duplicate_cleanup_summary.json"
    report_path = output_root / "duplicate_cleanup_report.xlsx"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    frame = pd.DataFrame(
        report_rows,
        columns=(
            "record_key",
            "owner_scope_value",
            "creator_id",
            "platform",
            "action",
            "record_id",
            "paired_keep_record_id",
            "ai_status",
            "last_mail_time",
            "quote",
            "profile_url",
            "reason",
        ),
    )
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="duplicate_cleanup")
    return summary_path, report_path


def _write_platform_repair_report(
    output_root: Path,
    *,
    summary: dict[str, Any],
    report_rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "platform_repair_summary.json"
    report_path = output_root / "platform_repair_report.xlsx"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    frame = pd.DataFrame(
        report_rows,
        columns=(
            "record_key",
            "creator_id",
            "record_id",
            "current_platform",
            "inferred_platform",
            "profile_url",
            "action",
            "reason",
        ),
    )
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="platform_repair")
    return summary_path, report_path


def cleanup_duplicate_records(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    output_root: Path,
    execute: bool,
    safe_only: bool = False,
    key_mode: str = KEY_MODE_CREATOR_PLATFORM,
) -> dict[str, Any]:
    resolved_view, analysis = _resolve_cleanup_record_analysis(
        client=client,
        linked_bitable_url=linked_bitable_url,
        key_mode=key_mode,
    )
    report_rows, histogram = _build_report_rows(analysis.duplicate_groups)
    safe_groups, risky_groups = _partition_duplicate_groups(list(analysis.duplicate_groups))
    safe_duplicate_row_count = sum(len(group.get("duplicate_records") or []) for group in safe_groups)
    risky_duplicate_row_count = sum(len(group.get("duplicate_records") or []) for group in risky_groups)
    safe_platform_pollution_group_count, safe_platform_pollution_row_count = _count_platform_pollution(safe_groups)
    risky_platform_pollution_group_count, risky_platform_pollution_row_count = _count_platform_pollution(risky_groups)
    executable_groups = safe_groups if safe_only else list(analysis.duplicate_groups)
    executable_duplicate_row_count = sum(len(group.get("duplicate_records") or []) for group in executable_groups)
    deleted_record_ids: list[str] = []
    blocked_reason = ""
    scope_mode = "owner_scoped" if _clean_text(analysis.owner_scope_field_name) else "global_creator_platform"
    skipped_owner_scope_record_count = int(
        getattr(analysis, "skipped_owner_scope_record_count", getattr(analysis, "owner_scope_missing_record_count", 0)) or 0
    )
    skipped_missing_creator_id_record_count = int(getattr(analysis, "skipped_missing_creator_id_record_count", 0) or 0)
    skipped_missing_profile_url_record_count = int(getattr(analysis, "skipped_missing_profile_url_record_count", 0) or 0)
    skipped_record_count = int(
        getattr(
            analysis,
            "skipped_record_count",
            skipped_owner_scope_record_count + skipped_missing_creator_id_record_count + skipped_missing_profile_url_record_count,
        )
        or 0
    )
    if execute and _clean_text(analysis.owner_scope_field_name) and int(analysis.owner_scope_missing_record_count or 0) > 0:
        blocked_reason = "目标飞书表存在未填写 `达人对接人` 的历史记录，当前不允许执行重复清理，需先补齐负责人维度。"
    if execute:
        if blocked_reason:
            summary = {
                "ok": False,
                "execute": True,
                "guard_blocked": True,
                "error": blocked_reason,
                "keep_strategy": KEEP_STRATEGY_NAME,
                "keep_strategy_description": KEEP_STRATEGY_DESCRIPTION,
                "target_url": resolved_view.source_url,
                "target_table_id": resolved_view.table_id,
                "target_table_name": resolved_view.table_name,
                "target_view_id": resolved_view.view_id,
                "target_view_name": resolved_view.view_name,
                "key_mode": str(key_mode),
                "key_field_names": list(analysis.key_field_names),
                "key_display_name": analysis.key_display_name,
                "scope_mode": scope_mode,
                "owner_scope_field_name": analysis.owner_scope_field_name,
                "owner_scope_missing_record_count": analysis.owner_scope_missing_record_count,
                "skipped_owner_scope_record_count": skipped_owner_scope_record_count,
                "skipped_missing_creator_id_record_count": skipped_missing_creator_id_record_count,
                "skipped_missing_profile_url_record_count": skipped_missing_profile_url_record_count,
                "skipped_record_count": skipped_record_count,
                "duplicate_group_count": len(analysis.duplicate_groups),
                "duplicate_row_count": sum(len(group.get("duplicate_records") or []) for group in analysis.duplicate_groups),
                "safe_group_count": len(safe_groups),
                "safe_duplicate_row_count": safe_duplicate_row_count,
                "safe_platform_pollution_group_count": safe_platform_pollution_group_count,
                "safe_platform_pollution_row_count": safe_platform_pollution_row_count,
                "risky_group_count": len(risky_groups),
                "risky_duplicate_row_count": risky_duplicate_row_count,
                "risky_platform_pollution_group_count": risky_platform_pollution_group_count,
                "risky_platform_pollution_row_count": risky_platform_pollution_row_count,
                "execute_mode": "safe_only" if safe_only else "all_duplicate_groups",
                "planned_delete_group_count": len(executable_groups),
                "planned_delete_row_count": executable_duplicate_row_count,
                "unique_record_count": len(analysis.index),
                "group_histogram": dict(histogram),
                "deleted_record_count": 0,
                "deleted_record_ids": [],
            }
            summary_path, report_path = _write_report(output_root, summary=summary, report_rows=report_rows)
            summary["summary_path"] = str(summary_path)
            summary["report_xlsx_path"] = str(report_path)
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return summary
        for group in executable_groups:
            for duplicate_record in list(group.get("duplicate_records") or []):
                record_id = _clean_text(duplicate_record.get("record_id"))
                if not record_id:
                    continue
                client.delete_api_json(
                    f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records/{record_id}"
                )
                deleted_record_ids.append(record_id)

    summary = {
        "ok": True,
        "execute": bool(execute),
        "keep_strategy": KEEP_STRATEGY_NAME,
        "keep_strategy_description": KEEP_STRATEGY_DESCRIPTION,
        "target_url": resolved_view.source_url,
        "target_table_id": resolved_view.table_id,
        "target_table_name": resolved_view.table_name,
        "target_view_id": resolved_view.view_id,
        "target_view_name": resolved_view.view_name,
        "key_mode": str(key_mode),
        "key_field_names": list(analysis.key_field_names),
        "key_display_name": analysis.key_display_name,
        "scope_mode": scope_mode,
        "owner_scope_field_name": analysis.owner_scope_field_name,
        "owner_scope_missing_record_count": analysis.owner_scope_missing_record_count,
        "skipped_owner_scope_record_count": skipped_owner_scope_record_count,
        "skipped_missing_creator_id_record_count": skipped_missing_creator_id_record_count,
        "skipped_missing_profile_url_record_count": skipped_missing_profile_url_record_count,
        "skipped_record_count": skipped_record_count,
        "duplicate_group_count": len(analysis.duplicate_groups),
        "duplicate_row_count": sum(len(group.get("duplicate_records") or []) for group in analysis.duplicate_groups),
        "safe_group_count": len(safe_groups),
        "safe_duplicate_row_count": safe_duplicate_row_count,
        "safe_platform_pollution_group_count": safe_platform_pollution_group_count,
        "safe_platform_pollution_row_count": safe_platform_pollution_row_count,
        "risky_group_count": len(risky_groups),
        "risky_duplicate_row_count": risky_duplicate_row_count,
        "risky_platform_pollution_group_count": risky_platform_pollution_group_count,
        "risky_platform_pollution_row_count": risky_platform_pollution_row_count,
        "execute_mode": "safe_only" if safe_only else "all_duplicate_groups",
        "planned_delete_group_count": len(executable_groups),
        "planned_delete_row_count": executable_duplicate_row_count,
        "unique_record_count": len(analysis.index),
        "group_histogram": dict(histogram),
        "deleted_record_count": len(deleted_record_ids),
        "deleted_record_ids": deleted_record_ids,
    }
    summary_path, report_path = _write_report(output_root, summary=summary, report_rows=report_rows)
    summary["summary_path"] = str(summary_path)
    summary["report_xlsx_path"] = str(report_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def repair_platform_field_from_profile_url(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    output_root: Path,
    execute: bool,
) -> dict[str, Any]:
    resolved_view, analysis = fetch_existing_bitable_record_analysis(client, linked_bitable_url=linked_bitable_url)
    safe_groups, risky_groups = _partition_duplicate_groups(list(analysis.duplicate_groups))
    safe_platform_pollution_group_count, safe_platform_pollution_row_count = _count_platform_pollution(safe_groups)
    risky_platform_pollution_group_count, risky_platform_pollution_row_count = _count_platform_pollution(risky_groups)
    report_rows, platform_histogram = _build_platform_repair_rows(risky_groups)
    planned_updates = [row for row in report_rows if str(row.get("action")) == "update"]
    updated_record_ids: list[str] = []

    if execute:
        for row in planned_updates:
            record_id = _clean_text(row.get("record_id"))
            inferred_platform = _clean_text(row.get("inferred_platform"))
            if not record_id or not inferred_platform:
                continue
            client.put_api_json(
                f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records/{record_id}",
                body={"fields": {"平台": inferred_platform}},
            )
            updated_record_ids.append(record_id)

    summary = {
        "ok": True,
        "execute": bool(execute),
        "repair_strategy": PLATFORM_REPAIR_STRATEGY_NAME,
        "repair_strategy_description": PLATFORM_REPAIR_STRATEGY_DESCRIPTION,
        "target_url": resolved_view.source_url,
        "target_table_id": resolved_view.table_id,
        "target_table_name": resolved_view.table_name,
        "target_view_id": resolved_view.view_id,
        "target_view_name": resolved_view.view_name,
        "duplicate_group_count": len(analysis.duplicate_groups),
        "safe_group_count": len(safe_groups),
        "skipped_safe_group_count": len(safe_groups),
        "skipped_safe_platform_pollution_group_count": safe_platform_pollution_group_count,
        "skipped_safe_platform_pollution_row_count": safe_platform_pollution_row_count,
        "risky_group_count": len(risky_groups),
        "risky_platform_pollution_group_count": risky_platform_pollution_group_count,
        "risky_platform_pollution_row_count": risky_platform_pollution_row_count,
        "repair_group_count": len({str(row.get('record_key')) for row in planned_updates}),
        "repair_row_count": len(planned_updates),
        "platform_histogram": platform_histogram,
        "updated_record_count": len(updated_record_ids),
        "updated_record_ids": updated_record_ids,
    }
    summary_path, report_path = _write_platform_repair_report(output_root, summary=summary, report_rows=report_rows)
    summary["summary_path"] = str(summary_path)
    summary["report_xlsx_path"] = str(report_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="清理飞书多维表格里重复记录；若存在项目维度字段，则按项目维度分组。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    parser.add_argument("--url", "--linked-bitable-url", dest="url", required=True, help="目标飞书多维表 URL")
    parser.add_argument("--output-root", default="", help="本地清理报告目录，默认 ./temp/bitable_duplicate_cleanup_<timestamp>")
    parser.add_argument("--execute", action="store_true", help="真正删除重复记录；默认只做 dry-run 报告")
    parser.add_argument(
        "--key-mode",
        default=KEY_MODE_CREATOR_PLATFORM,
        choices=(KEY_MODE_CREATOR_PLATFORM, KEY_MODE_CREATOR_PROFILE_URL),
        help="去重主键模式：默认达人ID+平台；若平台字段是公式/脏值，可切到达人ID+主页链接。",
    )
    parser.add_argument(
        "--safe-only",
        action="store_true",
        help="只删除单一主页链接的安全重复组；多主页链接/跨平台污染组只保留在报告里，不执行删除。",
    )
    parser.add_argument(
        "--repair-platforms-from-profile-url",
        action="store_true",
        help="根据主页链接反推真实平台，修复 `平台=🚫重复` 等污染值；默认 dry-run，配合 --execute 才会真正写回。",
    )
    parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value("", env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value("", env_values, "FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        raise ValueError("缺少 FEISHU_APP_ID / FEISHU_APP_SECRET。")

    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value("", env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
    )
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if str(args.output_root or "").strip()
        else Path("./temp") / f"bitable_duplicate_cleanup_{timestamp}"
    )
    if args.repair_platforms_from_profile_url:
        result = repair_platform_field_from_profile_url(
            client=client,
            linked_bitable_url=args.url,
            output_root=output_root,
            execute=bool(args.execute),
        )
    else:
        result = cleanup_duplicate_records(
            client=client,
            linked_bitable_url=args.url,
            output_root=output_root,
            execute=bool(args.execute),
            safe_only=bool(args.safe_only),
            key_mode=str(args.key_mode),
        )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if args.repair_platforms_from_profile_url:
            print(
                f"platform repair {'executed' if args.execute else 'planned'}: "
                f"groups={result['repair_group_count']}/{result['risky_group_count']}  "
                f"update_rows={result['repair_row_count']}  "
                f"summary={result['summary_path']}"
            )
        else:
            print(
                f"duplicate cleanup {'executed' if args.execute else 'planned'}: "
                f"groups={result['planned_delete_group_count']}/{result['duplicate_group_count']}  "
                f"delete_rows={result['planned_delete_row_count']}/{result['duplicate_row_count']}  "
                f"summary={result['summary_path']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
