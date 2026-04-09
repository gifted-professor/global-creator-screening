from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feishu_screening_bridge.bitable_export import resolve_bitable_view_from_url
from feishu_screening_bridge.bitable_upload import (
    _canonicalize_target_url,
    _extract_existing_owner_scope,
    _fetch_existing_records,
    _fetch_field_schemas,
    _resolve_owner_scope_field_name,
)
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env

_MANUAL_SUFFIX_PATTERN = re.compile(r"\d{1,2}/\d{1,2}转人工\d+$")


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


def _safe_name(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return "all"
    cleaned: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    return "".join(cleaned).strip("_") or "all"


def _matches_legacy_manual_suffix(creator_id: Any) -> bool:
    return bool(_MANUAL_SUFFIX_PATTERN.search(_clean_text(creator_id)))


def _matches_task_specific_manual_creator_id(creator_id: Any, task_name: str) -> bool:
    normalized_task_name = _clean_text(task_name)
    normalized_creator_id = _clean_text(creator_id)
    if not normalized_task_name or not normalized_creator_id.startswith(normalized_task_name):
        return False
    suffix = normalized_creator_id[len(normalized_task_name) :]
    return bool(re.fullmatch(r"\d{1,2}/\d{1,2}转人工\d+", suffix))


def _build_signal_list(
    *,
    creator_id: str,
    platform: str,
    ai_status: str,
    task_name: str,
    task_name_filter: str,
) -> list[str]:
    signals: list[str] = []
    if platform == "转人工":
        signals.append("platform_equals_转人工")
    if _matches_legacy_manual_suffix(creator_id):
        signals.append("creator_id_matches_manual_pool_pattern")
    if ai_status == "转人工":
        signals.append("ai_status_equals_转人工")
    if task_name_filter and (
        _clean_text(task_name).casefold() == _clean_text(task_name_filter).casefold()
        or _matches_task_specific_manual_creator_id(creator_id, task_name_filter)
    ):
        signals.append("task_name_matches_filter")
    return signals


def _build_report_row(
    *,
    record_id: str,
    fields: dict[str, Any],
    owner_scope_field_name: str,
    task_name_filter: str,
) -> dict[str, Any]:
    creator_id = _flatten_field_value(fields.get("达人ID"))
    platform = _flatten_field_value(fields.get("平台"))
    ai_status = _flatten_field_value(fields.get("ai 是否通过") or fields.get("ai是否通过"))
    task_name = _flatten_field_value(fields.get("任务名"))
    owner_scope_value = _extract_existing_owner_scope(fields, owner_scope_field_name)
    signals = _build_signal_list(
        creator_id=creator_id,
        platform=platform,
        ai_status=ai_status,
        task_name=task_name,
        task_name_filter=task_name_filter,
    )
    strict_candidate = platform == "转人工" and _matches_legacy_manual_suffix(creator_id)
    if task_name_filter and not any(signal == "task_name_matches_filter" for signal in signals):
        task_filter_status = "filtered_out"
    elif task_name_filter:
        task_filter_status = "matched"
    else:
        task_filter_status = "not_requested"
    if strict_candidate:
        recommended_action = "review_then_delete"
        reason = "平台值和达人ID同时命中 legacy manual-pool 占位模式。"
    elif platform == "转人工":
        recommended_action = "review"
        reason = "平台值命中 legacy manual-pool 占位模式。"
    elif _matches_legacy_manual_suffix(creator_id):
        recommended_action = "review"
        reason = "达人ID命中 legacy manual-pool 占位模式。"
    else:
        recommended_action = ""
        reason = ""
    return {
        "record_id": record_id,
        "creator_id": creator_id,
        "platform": platform,
        "ai_status": ai_status,
        "task_name": task_name,
        "owner_scope_value": owner_scope_value,
        "profile_url": _flatten_field_value(fields.get("主页链接")),
        "last_mail_time": _flatten_field_value(fields.get("达人最后一次回复邮件时间")),
        "candidate_signals": " | ".join(signals),
        "strict_candidate": "true" if strict_candidate else "false",
        "task_filter_status": task_filter_status,
        "recommended_action": recommended_action,
        "reason": reason,
    }


def _write_report(
    output_root: Path,
    *,
    summary: dict[str, Any],
    report_rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "legacy_manual_pool_summary.json"
    report_path = output_root / "legacy_manual_pool_report.xlsx"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    frame = pd.DataFrame(
        report_rows,
        columns=(
            "record_id",
            "creator_id",
            "platform",
            "ai_status",
            "task_name",
            "owner_scope_value",
            "profile_url",
            "last_mail_time",
            "candidate_signals",
            "strict_candidate",
            "task_filter_status",
            "recommended_action",
            "reason",
        ),
    )
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="legacy_manual_pool")
    return summary_path, report_path


def export_legacy_manual_pool_records(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    output_root: Path,
    task_name_filter: str = "",
) -> dict[str, Any]:
    resolved_view = resolve_bitable_view_from_url(client, _canonicalize_target_url(client, linked_bitable_url))
    field_schemas = _fetch_field_schemas(client, resolved_view)
    existing_records = _fetch_existing_records(client, resolved_view)
    owner_scope_field_name = _resolve_owner_scope_field_name(field_schemas or {})

    report_rows: list[dict[str, Any]] = []
    platform_manual_count = 0
    creator_id_pattern_count = 0
    ai_manual_count = 0
    strict_candidate_count = 0
    task_filtered_candidate_count = 0

    for record_id, fields in existing_records:
        fields = dict(fields or {})
        row = _build_report_row(
            record_id=_clean_text(record_id),
            fields=fields,
            owner_scope_field_name=owner_scope_field_name,
            task_name_filter=task_name_filter,
        )
        creator_id = _clean_text(row.get("creator_id"))
        platform = _clean_text(row.get("platform"))
        ai_status = _clean_text(row.get("ai_status"))
        task_filter_status = _clean_text(row.get("task_filter_status"))
        if platform == "转人工":
            platform_manual_count += 1
        if _matches_legacy_manual_suffix(creator_id):
            creator_id_pattern_count += 1
        if ai_status == "转人工":
            ai_manual_count += 1
        if _clean_text(row.get("strict_candidate")) == "true":
            strict_candidate_count += 1
        is_candidate = platform == "转人工" or _matches_legacy_manual_suffix(creator_id)
        if task_name_filter and is_candidate and task_filter_status != "matched":
            continue
        if not is_candidate:
            continue
        if task_filter_status == "matched":
            task_filtered_candidate_count += 1
        report_rows.append(row)

    summary = {
        "ok": True,
        "target_url": resolved_view.source_url,
        "target_table_id": resolved_view.table_id,
        "target_table_name": resolved_view.table_name,
        "target_view_id": resolved_view.view_id,
        "target_view_name": resolved_view.view_name,
        "owner_scope_field_name": owner_scope_field_name,
        "task_name_filter": _clean_text(task_name_filter),
        "existing_record_count": len(existing_records),
        "platform_manual_value_count": platform_manual_count,
        "creator_id_manual_pattern_count": creator_id_pattern_count,
        "ai_manual_status_count": ai_manual_count,
        "strict_candidate_count": strict_candidate_count,
        "candidate_record_count": len(report_rows),
        "task_filtered_candidate_count": task_filtered_candidate_count,
        "candidate_record_id_preview": [str(row.get("record_id")) for row in report_rows[:50]],
    }
    summary_path, report_path = _write_report(output_root, summary=summary, report_rows=report_rows)
    summary["summary_path"] = str(summary_path)
    summary["report_xlsx_path"] = str(report_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导出飞书多维表里 legacy manual-pool 占位记录，默认只生成本地报告。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    parser.add_argument("--url", "--linked-bitable-url", dest="url", required=True, help="目标飞书多维表 URL")
    parser.add_argument("--task-name", default="", help="只导出指定任务名前缀的 legacy manual-pool 记录，例如 Duet")
    parser.add_argument("--output-root", default="", help="本地导出目录，默认 ./temp/legacy_manual_pool_export_<timestamp>_<task>")
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
        if _clean_text(args.output_root)
        else Path("./temp") / f"legacy_manual_pool_export_{timestamp}_{_safe_name(_clean_text(args.task_name) or 'all')}"
    )
    result = export_legacy_manual_pool_records(
        client=client,
        linked_bitable_url=args.url,
        output_root=output_root,
        task_name_filter=_clean_text(args.task_name),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            "legacy manual-pool export completed: "
            f"candidates={result['candidate_record_count']}  "
            f"strict={result['strict_candidate_count']}  "
            f"summary={result['summary_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
