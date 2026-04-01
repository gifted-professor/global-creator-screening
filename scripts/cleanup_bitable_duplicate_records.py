from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feishu_screening_bridge.bitable_upload import fetch_existing_bitable_record_analysis
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env


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
                "reason": "保留该分组里信息最完整、邮件时间较新的记录。",
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
                    "reason": "与保留记录共享同一去重主键，判定为重复脏记录。",
                }
            )
    return report_rows, histogram


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


def cleanup_duplicate_records(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    output_root: Path,
    execute: bool,
) -> dict[str, Any]:
    resolved_view, analysis = fetch_existing_bitable_record_analysis(client, linked_bitable_url=linked_bitable_url)
    report_rows, histogram = _build_report_rows(analysis.duplicate_groups)
    deleted_record_ids: list[str] = []
    blocked_reason = ""
    if execute and not _clean_text(analysis.owner_scope_field_name):
        blocked_reason = "目标飞书表缺少 `达人对接人` 字段，当前不允许执行重复清理，避免误删不同负责人下的达人记录。"
    elif execute and int(analysis.owner_scope_missing_record_count or 0) > 0:
        blocked_reason = "目标飞书表存在未填写 `达人对接人` 的历史记录，当前不允许执行重复清理，需先补齐负责人维度。"
    if execute:
        if blocked_reason:
            summary = {
                "ok": False,
                "execute": True,
                "guard_blocked": True,
                "error": blocked_reason,
                "target_url": resolved_view.source_url,
                "target_table_id": resolved_view.table_id,
                "target_table_name": resolved_view.table_name,
                "target_view_id": resolved_view.view_id,
                "target_view_name": resolved_view.view_name,
                "key_field_names": list(analysis.key_field_names),
                "key_display_name": analysis.key_display_name,
                "owner_scope_field_name": analysis.owner_scope_field_name,
                "owner_scope_missing_record_count": analysis.owner_scope_missing_record_count,
                "duplicate_group_count": len(analysis.duplicate_groups),
                "duplicate_row_count": sum(len(group.get("duplicate_records") or []) for group in analysis.duplicate_groups),
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
        for group in analysis.duplicate_groups:
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
        "target_url": resolved_view.source_url,
        "target_table_id": resolved_view.table_id,
        "target_table_name": resolved_view.table_name,
        "target_view_id": resolved_view.view_id,
        "target_view_name": resolved_view.view_name,
        "key_field_names": list(analysis.key_field_names),
        "key_display_name": analysis.key_display_name,
        "owner_scope_field_name": analysis.owner_scope_field_name,
        "owner_scope_missing_record_count": analysis.owner_scope_missing_record_count,
        "duplicate_group_count": len(analysis.duplicate_groups),
        "duplicate_row_count": sum(len(group.get("duplicate_records") or []) for group in analysis.duplicate_groups),
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="清理飞书多维表格里重复记录；若存在项目维度字段，则按项目维度分组。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    parser.add_argument("--url", required=True, help="目标飞书多维表 URL")
    parser.add_argument("--output-root", default="", help="本地清理报告目录，默认 ./temp/bitable_duplicate_cleanup_<timestamp>")
    parser.add_argument("--execute", action="store_true", help="真正删除重复记录；默认只做 dry-run 报告")
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
    result = cleanup_duplicate_records(
        client=client,
        linked_bitable_url=args.url,
        output_root=output_root,
        execute=bool(args.execute),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"duplicate cleanup {'executed' if args.execute else 'planned'}: "
            f"groups={result['duplicate_group_count']}  "
            f"delete_rows={result['duplicate_row_count']}  "
            f"summary={result['summary_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
